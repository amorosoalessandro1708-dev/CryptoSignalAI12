"""MOMENTUM ONE 4H CONFIRMED + 15M RETEST - ALLEGGERITO 25 COIN.

STESSI FILTRI STRATEGICI DELLA VERSIONE ORIGINALE:
- Breakout 4H chiuso: massimi/minimi precedenti 10 barre 4H +0.03 ATR4H;
  chiusura nel 30% direzionale estremo; nessun filtro volume su 4H.
- Retest su 15m chiusa entro +/-0.15 ATR15 e senza invalidazione profonda.
- Conferma su candela 15m SUCCESSIVA CHIUSA: corpo >=75% del range e
  >=0.50 ATR15; wick per lato <=15%; buffer oltre livello 0.02 ATR15.
- Volume definitivo della candela conferma >=1.35x della media delle
  10 precedenti 15m chiuse; volume live controllato senza segnali anticipati.
- BTC15 sulla conferma concorde o neutro; SL 3x4H +0.10 ATR4H;
  TP1=0.30R, TP2=0.80R, TP3=1.50R; nessun cooldown temporale per crypto.
- Validita' setup effettiva 4 ore (come nel codice originale, non 3).

AGGIORNAMENTI SOLO OPERATIVI: consegna Telegram con retry, log diagnostici
per retest/conferme rifiutate, gestione della coda senza perdere messaggi
alla prima difficolta' di rete. La garanzia di consegna esatta una volta
non e' tecnicamente possibile in caso di timeout ambiguo dell'API Telegram.
Segnali duplicati dello STESSO breakout 4H bloccati con chiave (coin, macro_t).
Solo segnali simulati. Nessun ordine automatico, nessuna modifica dei filtri.
"""

import json
import os
import queue
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import websocket

# Variabili ambiente gia' usate nel progetto Telegram/Railway
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

SYMBOLS = [
    'SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT',
    'DOGEUSDT', 'AVAXUSDT', 'SUIUSDT', 'LINKUSDT', 'ADAUSDT',
    'NEARUSDT', 'UNIUSDT', 'LTCUSDT', 'DOTUSDT', 'APTUSDT',
    'ARBUSDT', 'OPUSDT', 'INJUSDT', 'RENDERUSDT', '1000PEPEUSDT',
    'FILUSDT', 'ETCUSDT', 'AAVEUSDT', 'TIAUSDT', 'SEIUSDT',
]

# STRATEGIA 4H CHIUSO -> RETEST 15M -> CHIUSURA 15M (LONG/SHORT)
SCAN_SECONDS = 1.0
TIMEFRAME_SECONDS = 15 * 60
ATR_PERIOD = 14
BTC_NEUTRAL_BODY_ATR_MAX = 0.20

# Breakout 4H vs massimo/minimo ultime 10 candele 4H chiuse.
FOUR_HOUR_RESISTANCE_LOOKBACK = 10
VOLUME_RATIO_MIN = 1.35             # 15m CONFERMA CHIUSA vs media 10 chiuse
VOLUME_AVG_BARS_15M = 10           # nessun volume obbligatorio sulla 4H
LIVE_VOLUME_PACE_MIN_SECONDS = 60     # evitare proiezioni fuorvianti a inizio candela
LIVE_VOLUME_PROGRESS_LOG_SECONDS = 180  # un log ogni 3 minuti, non ogni secondo
BREAKOUT_BUFFER_ATR4H = 0.03       # distanza oltre il livello in ATR4H
FOUR_HOUR_CLOSE_POSITION_MIN = 0.70 # chiusura nel 30% estremo della 4H
MACRO_CLOSE_GRACE_SECONDS = 600     # 10m di tolleranza per reconnect post-chiusura 4H

# Il retest e la candela 15m di conferma vengono DOPO il breakout 4H.
RETEST_ZONE_ATR15 = 0.15           # tolleranza sopra/sotto il livello
MICRO_CLOSE_BUFFER_ATR15 = 0.02    # chiusura 15m oltre il livello
CONFIRM_BODY_FRACTION_MIN = 0.75  # corpo pieno >=75% del range sulla 15m chiusa
CONFIRM_BODY_ATR_MIN = 0.50       # corpo >=0.50 ATR15 sulla 15m chiusa
CONFIRM_LEADING_WICK_MAX = 0.15  # LONG wick alto; SHORT wick basso
CONFIRM_TRAILING_WICK_MAX = 0.15 # LONG wick basso; SHORT wick alto
SETUP_LIFETIME_SECONDS = 4 * 60 * 60  # massimo 4 ore dopo la chiusura 4H
CLOSE_EVENT_MAX_DELAY_SECONDS = 12    # non inviare segnali vecchi dopo disconnessioni

# SL 4H e TP calcolati in R; nessun tetto alla distanza dello stop
SL_4H_LOOKBACK = 3
SL_BUFFER_ATR4H = 0.10
TP1_R = 0.30
TP2_R = 0.80
TP3_R = 1.50
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
FIFTEEN_MIN_MS = TIMEFRAME_SECONDS * 1000

# Solo affidabilita'/trasporto, NON filtri di strategia
KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines'
WS_BASE_URL = 'wss://fstream.binance.com/market/stream?streams='
WS_STALE_SECONDS = 5.0
WS_MAX_RECONNECT_SECONDS = 30
REST_GAP_SECONDS = 0.30
REST_RETRY_WAIT = (5, 10, 20)
REST_REFRESH_SECONDS = 300
GAP_REFRESH_SECONDS = 15
LOG_INTERVAL_SECONDS = 15
TELEGRAM_RETRY_SECONDS = 30
HISTORY_MAXLEN = 320  # necessario per ricostruire ATR14 sul timeframe 4H
# Parametri trasporto Telegram (NON influiscono sui requisiti di trading)
TELEGRAM_MAX_ATTEMPTS = 8
TELEGRAM_DELIVERY_WINDOW_SECONDS = 300
TELEGRAM_RETRY_INITIAL_SECONDS = 3
TELEGRAM_RETRY_MAX_SECONDS = 40
ITALY = ZoneInfo('Europe/Rome')

cache_lock = threading.RLock()
rest_lock = threading.Lock()
alert_lock = threading.Lock()
market_cache = {}              # {symbol: [candele 15m, ...]}
last_ws_receive = {}          # {symbol: time.monotonic()}
needs_refresh = set()         # buchi nella serie, recuperati REST
websocket_connected = False
last_rest_call = 0.0
sent_alerts = set()           # (symbol, apertura della candela)
pending_alerts = set()
failed_alert_times = {}
telegram_queue = queue.Queue(maxsize=100)
active_setups = {}             # {symbol: setup 4H confermato in attesa di retest 15m}
seen_macro_bars = {}            # {symbol: t della 4H gia' analizzata}


def log(*parts):
    print(*parts, flush=True)


def telegram_send_once(text):
    """Restituisce (consegnato, ritentabile, attesa_suggerita).

    Un timeout puo' essere ambiguo: Telegram potrebbe aver ricevuto il
    messaggio, anche se il client non ha ricevuto la conferma. L'ID stabile
    nel testo rende riconoscibili eventuali copie, senza poterle impedire.
    """
    if not BOT_TOKEN or not CHAT_ID:
        log('TELEGRAM NON CONFIGURATO: mancano TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID')
        return False, False, 0
    try:
        response = requests.post(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            data={'chat_id': CHAT_ID, 'text': text}, timeout=(8, 20),
        )
        if response.status_code == 429:
            retry_after = 5
            try:
                payload = response.json()
                retry_after = max(1, int(payload.get('parameters', {}).get('retry_after', 5)))
            except (ValueError, TypeError, AttributeError):
                pass
            log('Telegram rate limit HTTP 429; riprovo dopo', retry_after, 'secondi')
            return False, True, retry_after
        if response.status_code >= 500:
            log('Telegram errore temporaneo HTTP', response.status_code)
            return False, True, 0
        if response.status_code >= 400:
            log('Telegram rifiuto permanente HTTP', response.status_code, response.text[:200])
            return False, False, 0
        response.raise_for_status()
        data = response.json()
        if not data.get('ok'):
            log('Telegram risposta senza ok:', str(data)[:250])
            return False, True, 0
        return True, False, 0
    except (requests.RequestException, ValueError) as exc:
        log('Telegram errore di rete/risposta:', repr(exc))
        return False, True, 0


def send_telegram(text):
    """Compatibilita' con messaggio di avvio (non segnale operativo)."""
    ok, _, _ = telegram_send_once(text)
    return ok


def request_klines(symbol):
    global last_rest_call
    error = None
    for attempt in range(len(REST_RETRY_WAIT) + 1):
        with rest_lock:
            pause = REST_GAP_SECONDS - (time.monotonic() - last_rest_call)
            if pause > 0:
                time.sleep(pause)
            last_rest_call = time.monotonic()
        try:
            response = requests.get(
                KLINES_URL,
                params={'symbol': symbol, 'interval': '15m', 'limit': HISTORY_MAXLEN},
                timeout=15,
            )
            if response.status_code in (418, 429):
                header = response.headers.get('Retry-After')
                try:
                    wait = max(5.0, float(header))
                except (ValueError, TypeError):
                    wait = REST_RETRY_WAIT[min(attempt, len(REST_RETRY_WAIT)-1)]
                log('Limite API Binance:', response.status_code, symbol, 'pausa', wait)
                time.sleep(wait)
                error = RuntimeError(f'HTTP {response.status_code}')
                continue
            response.raise_for_status()
            raw = response.json()
            candles = [{
                't': int(row[0]), 'o': float(row[1]), 'h': float(row[2]),
                'l': float(row[3]), 'c': float(row[4]), 'v': float(row[5]),
            } for row in raw]
            if len(candles) < ATR_PERIOD + 2:
                raise ValueError(f'{symbol}: storico insufficiente')
            return candles
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            error = exc
        if attempt < len(REST_RETRY_WAIT):
            time.sleep(REST_RETRY_WAIT[attempt])
    raise RuntimeError(f'{symbol}: impossibile recuperare dati 15m ({error})')


def refresh_symbol(symbol):
    candles = request_klines(symbol)
    with cache_lock:
        old = market_cache.get(symbol)
        if old:
            # Non rimpiazzare dati WebSocket piu' recenti con REST in ritardo.
            latest_old, latest_new = old[-1], candles[-1]
            if latest_old['t'] > latest_new['t']:
                return
            if latest_old['t'] == latest_new['t'] and latest_old['v'] > latest_new['v']:
                candles[-1] = latest_old
        market_cache[symbol] = candles[-HISTORY_MAXLEN:]
        needs_refresh.discard(symbol)


def initialize_history():
    log('Caricamento storico 15m per', len(SYMBOLS), 'coppie...')
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(refresh_symbol, s): s for s in SYMBOLS}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                log('Errore storico', futures[future], exc)
    with cache_lock:
        available = len(market_cache)
    if available == 0:
        raise RuntimeError('Nessun dato Binance disponibile')
    log('Storico inizializzato:', available, '/', len(SYMBOLS))


def receive_kline(payload):
    event = payload.get('data', payload)
    if event.get('e') != 'kline':
        return
    k = event.get('k') or {}
    symbol = str(event.get('s', '')).upper()
    if symbol not in SYMBOLS or k.get('i') != '15m':
        return
    try:
        c = {
            't': int(k['t']), 'o': float(k['o']), 'h': float(k['h']),
            'l': float(k['l']), 'c': float(k['c']), 'v': float(k['v']),
        }
        if c['t'] <= 0 or c['v'] < 0 or not (c['l'] <= min(c['o'], c['c']) <= max(c['o'], c['c']) <= c['h']):
            return
    except (KeyError, ValueError, TypeError):
        return

    with cache_lock:
        previous = market_cache.get(symbol)
        if not previous:
            needs_refresh.add(symbol)
            return
        last = previous[-1]
        if c['t'] < last['t']:
            return
        if c['t'] == last['t']:
            if c['v'] < last['v']:  # scarta pacchetti cumulativi vecchi
                return
            market_cache[symbol] = previous[:-1] + [c]
        elif c['t'] == last['t'] + TIMEFRAME_SECONDS * 1000:
            market_cache[symbol] = (previous + [c])[-HISTORY_MAXLEN:]
        else:
            needs_refresh.add(symbol)  # serie incompleta: niente segnali finche' recuperata
            return
        last_ws_receive[symbol] = time.monotonic()


def websocket_worker():
    global websocket_connected
    streams = '/'.join(f'{s.lower()}@kline_15m' for s in SYMBOLS)
    url = WS_BASE_URL + streams
    delay = 1
    while True:
        try:
            def on_open(ws):
                nonlocal delay
                global websocket_connected
                delay = 1
                with cache_lock:
                    websocket_connected = True
                log('WS connesso,', len(SYMBOLS), 'stream 15m')

            def on_message(ws, message):
                try:
                    receive_kline(json.loads(message))
                except (ValueError, TypeError, AttributeError) as exc:
                    log('Messaggio WS non valido:', exc)

            def on_error(ws, error):
                log('WebSocket errore:', error)

            def on_close(ws, code, reason):
                global websocket_connected
                with cache_lock:
                    websocket_connected = False
                log('WS disconnesso:', code, reason)

            ws = websocket.WebSocketApp(
                url, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=60, ping_timeout=20)
        except Exception as exc:
            log('Errore connessione WS:', exc)
        finally:
            with cache_lock:
                websocket_connected = False
        time.sleep(delay)
        delay = min(delay * 2, WS_MAX_RECONNECT_SECONDS)


def history_worker():
    last_full = time.monotonic()
    last_gap = 0.0
    while True:
        now = time.monotonic()
        with cache_lock:
            gaps = list(needs_refresh)
            missing = [s for s in SYMBOLS if s not in market_cache]
        if now - last_gap >= GAP_REFRESH_SECONDS:
            for symbol in set(gaps + missing):
                try:
                    refresh_symbol(symbol)
                except Exception as exc:
                    log('Recupero REST fallito:', symbol, exc)
            last_gap = now
        if now - last_full >= REST_REFRESH_SECONDS:
            for symbol in SYMBOLS:
                try:
                    refresh_symbol(symbol)
                except Exception as exc:
                    log('Refresh REST fallito:', symbol, exc)
            last_full = time.monotonic()
        time.sleep(1)


def true_range_atr(closed_candles):
    """ATR14 su sole candele CHIUSE: non usa mai quella live."""
    if len(closed_candles) < ATR_PERIOD + 1:
        return None
    bars = closed_candles[-(ATR_PERIOD + 1):]
    tr = []
    for i in range(1, len(bars)):
        c, prev = bars[i], bars[i - 1]
        tr.append(max(c['h']-c['l'], abs(c['h']-prev['c']), abs(c['l']-prev['c'])))
    return sum(tr) / ATR_PERIOD


def live_volume_progress(candles, now_ms):
    """Misura il volume cumulativo della candela LIVE senza anticipare il segnale.

    actual_ratio = volume gia' scambiato / media delle 10 precedenti CHIUSE.
    pace_ratio = proiezione lineare a fine 15m, solo INDICATIVA; non e' il
    volume definitivo, ne' un filtro di ingresso. La media e' fissata per
    tutta la durata della candela live e non include mai la live stessa.
    """
    if len(candles) < VOLUME_AVG_BARS_15M + 1:
        return None
    live = candles[-1]
    elapsed_ms = now_ms - live['t']
    if not 0 <= elapsed_ms < FIFTEEN_MIN_MS:
        return None
    previous = candles[-(VOLUME_AVG_BARS_15M + 1):-1]
    avg = sum(max(0.0, c['v']) for c in previous) / VOLUME_AVG_BARS_15M
    if avg <= 0:
        return None
    elapsed = elapsed_ms / 1000.0
    actual_ratio = max(0.0, live['v']) / avg
    pace_ratio = (actual_ratio * TIMEFRAME_SECONDS / elapsed
                  if elapsed >= LIVE_VOLUME_PACE_MIN_SECONDS else None)
    return {
        'candle_t': live['t'],
        'elapsed_seconds': elapsed,
        'actual_ratio': actual_ratio,
        'pace_ratio': pace_ratio,
        'progress_percent': 100.0 * actual_ratio / VOLUME_RATIO_MIN,
    }


def monitor_live_confirmation_volume(setup, candles, now_ms):
    """Aggiorna ogni scan (~1s) il volume LIVE DOPO un retest CHIUSO.

    I log di avanzamento vengono limitati a uno ogni 3 minuti. Il volume
    1.35x raggiunto prima della chiusura e' solo una condizione PRELIMINARE:
    il segnale resta in evaluate_closed_retest_candle sulle 15m CHIUSE.
    """
    if not setup['touched'] or not candles:
        return
    if candles[-1]['t'] <= setup['touch_t']:
        return  # il retest stesso non puo' essere la candela di conferma
    progress = live_volume_progress(candles, now_ms)
    if not progress:
        return
    t = progress['candle_t']
    state = setup.get('live_volume_state')
    if state is None or state['candle_t'] != t:
        state = {
            'candle_t': t,
            'reached_final_volume_live': False,
            'next_log_elapsed': LIVE_VOLUME_PROGRESS_LOG_SECONDS,
        }
        setup['live_volume_state'] = state
    # Il dato viene aggiornato ogni secondo; i log sono meno frequenti.
    state['actual_ratio'] = progress['actual_ratio']
    state['pace_ratio'] = progress['pace_ratio']
    state['progress_percent'] = progress['progress_percent']
    if (progress['actual_ratio'] >= VOLUME_RATIO_MIN and
            not state['reached_final_volume_live']):
        state['reached_final_volume_live'] = True
        log('VOLUME LIVE', setup['symbol'], setup['direction'],
            f"{progress['actual_ratio']:.2f}x su {VOLUME_RATIO_MIN:.2f}x",
            '| SOGLIA RAGGIUNTA: si attende CHIUSURA 15m + altri filtri')
    if (progress['pace_ratio'] is not None and
            progress['elapsed_seconds'] >= state['next_log_elapsed']):
        state['next_log_elapsed'] = ((int(progress['elapsed_seconds']) //
                                      LIVE_VOLUME_PROGRESS_LOG_SECONDS) + 1) * LIVE_VOLUME_PROGRESS_LOG_SECONDS
        log('PROGRESSO VOLUME LIVE', setup['symbol'], setup['direction'],
            f"reale {progress['actual_ratio']:.2f}x",
            f"({progress['progress_percent']:.0f}% della soglia)",
            f"ritmo stimato {progress['pace_ratio']:.2f}x",
            '| conferma SOLO dopo chiusura 15m')


def four_hour_closed_candles(candles_15m):
    """Ricostruisce 4H da 16 candele 15m chiuse, allineate alle 00/04/08 UTC.

    Esclude la candela 15m LIVE e i blocchi 4H parziali/incompleti.
    Con 320 candele 15m in memoria si hanno almeno 15 candele 4H
    complete anche ai margini temporali, sufficienti per ATR14.
    """
    groups = {}
    for candle in candles_15m[:-1]:
        block_t = (candle['t'] // FOUR_HOURS_MS) * FOUR_HOURS_MS
        groups.setdefault(block_t, []).append(candle)
    result = []
    for block_t, group in sorted(groups.items()):
        if len(group) != 16:
            continue
        group.sort(key=lambda c: c['t'])
        if any(c['t'] != block_t + i * FIFTEEN_MIN_MS for i, c in enumerate(group)):
            continue
        result.append({
            't': block_t,
            'o': group[0]['o'],
            'h': max(c['h'] for c in group),
            'l': min(c['l'] for c in group),
            'c': group[-1]['c'],
            'v': sum(c['v'] for c in group),
        })
    return result


def four_hour_structural_stop(candles_15m, direction):
    """Stop oltre 2 candele 4H CHIUSE +0.10 ATR4H, senza stop artificiali.

    Considera anche l'estremo gia' realizzato sulla 15m LIVE per non
    collocare lo stop dentro il range della candela di ingresso.
    """
    closed_4h = four_hour_closed_candles(candles_15m)
    if len(closed_4h) < max(SL_4H_LOOKBACK, ATR_PERIOD + 1):
        return None
    atr4h = true_range_atr(closed_4h)
    if atr4h is None or atr4h <= 0:
        return None
    structural = closed_4h[-SL_4H_LOOKBACK:]
    live = candles_15m[-1]
    if direction == 'LONG':
        extreme = min(min(c['l'] for c in structural), live['l'])
        sl = extreme - SL_BUFFER_ATR4H * atr4h
    else:
        extreme = max(max(c['h'] for c in structural), live['h'])
        sl = extreme + SL_BUFFER_ATR4H * atr4h
    return {'sl': sl, 'atr4h': atr4h, 'structure_extreme': extreme}


def btc15_direction(btc_candles):
    """BTC concorde oppure neutro; BTC opposto non neutro blocca il segnale.

    BTC e' neutro se il corpo live e' <0.20 ATR15 sulle sole chiuse.
    Se l'ATR non e' disponibile, restituisce None: niente segnali.
    Non richiede volume BTC, medie o conferme 1H.
    """
    if len(btc_candles) < ATR_PERIOD + 2:
        return None
    atr15 = true_range_atr(btc_candles[:-1])
    if atr15 is None or atr15 <= 0:
        return None
    live = btc_candles[-1]
    displacement = live['c'] - live['o']
    if abs(displacement) / atr15 < BTC_NEUTRAL_BODY_ATR_MAX:
        return 'NEUTRAL'
    return 'LONG' if displacement > 0 else 'SHORT'


def beyond_breakout(price, direction, level, buffer):
    return price >= level + buffer if direction == 'LONG' else price <= level - buffer


def macro_4h_breakout(symbol, candles, now_ms):
    """Considera SOLO la 4H appena CHIUSA e le 10 4H CHIUSE precedenti.

    La resistenza e' reale sul timeframe 4H, non max di 16 x 15m live.
    Se un deploy riparte molto dopo la chiusura, non simula il passato.
    """
    four = four_hour_closed_candles(candles)
    required = max(ATR_PERIOD + 2, FOUR_HOUR_RESISTANCE_LOOKBACK + 1)
    if len(four) < required:
        return None
    macro = four[-1]
    close_ms = macro['t'] + FOUR_HOURS_MS
    if not (0 <= now_ms - close_ms <= MACRO_CLOSE_GRACE_SECONDS * 1000):
        return None
    prev = four[:-1]
    if any(prev[i]['t'] + FOUR_HOURS_MS != prev[i + 1]['t']
           for i in range(len(prev) - 1)):
        return None
    if prev[-1]['t'] + FOUR_HOURS_MS != macro['t']:
        return None
    direction = ('LONG' if macro['c'] > macro['o'] else
                 'SHORT' if macro['c'] < macro['o'] else None)
    if direction is None:
        return None
    atr4h = true_range_atr(prev)
    if not atr4h or atr4h <= 0:
        return None
    level_bars = prev[-FOUR_HOUR_RESISTANCE_LOOKBACK:]
    level = max(c['h'] for c in level_bars) if direction == 'LONG' else min(c['l'] for c in level_bars)
    buffer = BREAKOUT_BUFFER_ATR4H * atr4h
    if not beyond_breakout(macro['c'], direction, level, buffer):
        return None
    full_range = macro['h'] - macro['l']
    if full_range <= 0:
        return None
    close_pos = ((macro['c']-macro['l']) / full_range if direction == 'LONG'
                 else (macro['h']-macro['c']) / full_range)
    if close_pos < FOUR_HOUR_CLOSE_POSITION_MIN:
        return None
    if not four_hour_structural_stop(candles, direction):
        return None
    return {
        'symbol': symbol, 'direction': direction,
        'macro_t': macro['t'], 'macro_end_t': close_ms,
        'level': level, 'macro_buffer': buffer,
        'atr4h_at_breakout': atr4h,
        'macro_close_pos': close_pos,
        'last_checked_15m_t': None,
        'touched': False, 'touch_t': None,
        'touched_price': None,
    }


def btc15_closed_direction(btc_candles, confirmation_t):
    """BTC15 sulla candela appena CHIUSA, non sulla nuova LIVE quasi neutra."""
    if len(btc_candles) < ATR_PERIOD + 3 or btc_candles[-2]['t'] != confirmation_t:
        return None
    atr = true_range_atr(btc_candles[:-2])
    if not atr or atr <= 0:
        return None
    candle = btc_candles[-2]
    movement = candle['c'] - candle['o']
    if abs(movement) / atr < BTC_NEUTRAL_BODY_ATR_MAX:
        return 'NEUTRAL'
    return 'LONG' if movement > 0 else 'SHORT'


def confirmation_15m_volume_ratio(candles):
    """Volume della 15m appena CHIUSA vs media delle 10 CHIUSE precedenti.

    candles[-1] e' la 15m LIVE appena iniziata, candles[-2] la conferma chiusa.
    Nessuna proiezione: si richiede volume 1.35x reale sulla conferma.
    """
    if len(candles) < VOLUME_AVG_BARS_15M + 2:
        return 0.0
    confirmation = candles[-2]
    previous = candles[-(VOLUME_AVG_BARS_15M + 2):-2]
    avg = sum(max(0.0, c['v']) for c in previous) / VOLUME_AVG_BARS_15M
    if avg <= 0:
        return 0.0
    return max(0.0, confirmation['v']) / avg


def confirmation_15m_valid(candle, direction, level, atr15):
    """Richiede candela 15m CHIUSA, lunga, piena e con ombre limitate.

    La candela del retest NON usa queste soglie: valgono solo per la candela
    successiva che conferma la ripartenza nella direzione della rottura 4H.
    """
    if atr15 is None or atr15 <= 0:
        return False
    span = candle['h'] - candle['l']
    if span <= 0:
        return False
    opening, closing = candle['o'], candle['c']
    if direction == 'LONG' and closing <= opening:
        return False
    if direction == 'SHORT' and closing >= opening:
        return False
    if direction not in ('LONG', 'SHORT'):
        return False
    if not beyond_breakout(closing, direction, level,
                           MICRO_CLOSE_BUFFER_ATR15 * atr15):
        return False
    body = abs(closing - opening)
    if body / span < CONFIRM_BODY_FRACTION_MIN:
        return False
    if body / atr15 < CONFIRM_BODY_ATR_MIN:
        return False
    upper_wick = max(0.0, candle['h'] - max(opening, closing)) / span
    lower_wick = max(0.0, min(opening, closing) - candle['l']) / span
    leading_wick = upper_wick if direction == 'LONG' else lower_wick
    trailing_wick = lower_wick if direction == 'LONG' else upper_wick
    return (leading_wick <= CONFIRM_LEADING_WICK_MAX and
            trailing_wick <= CONFIRM_TRAILING_WICK_MAX)

def build_confirmed_signal(setup, candles, btc_direction, candle_15m, now_ms):
    """Entry indicativa dal nuovo tick disponibile DOPO chiusura 15m."""
    if btc_direction not in (setup['direction'], 'NEUTRAL'):
        return None
    live = candles[-1]
    price = live['c']
    if price <= 0 or not beyond_breakout(
            price, setup['direction'], setup['level'],
            MICRO_CLOSE_BUFFER_ATR15 * setup['atr15_retest']):
        return None
    info = four_hour_structural_stop(candles, setup['direction'])
    if not info:
        return None
    risk = (price - info['sl'] if setup['direction'] == 'LONG'
            else info['sl'] - price)
    if risk <= 0:
        return None
    return {
        'symbol': setup['symbol'], 'direction': setup['direction'],
        'entry': price, 'sl': info['sl'], 'atr4h': info['atr4h'],
        'stop_distance_pct': risk/price,
        'macro_t': setup['macro_t'], 'macro_end_t': setup['macro_end_t'],
        'confirm_15m_t': candle_15m['t'],
        'retest_t': setup['touch_t'], 'breakout_level': setup['level'],
        'breakout_buffer': setup['macro_buffer'],
        'confirm_volume_ratio': setup['confirm_volume_ratio'],
        'macro_close_pos': setup['macro_close_pos'],
        'btc_direction': btc_direction,
    }


def evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms):
    """Stessi filtri dell'originale; aggiunge SOLO motivazioni nei log."""
    if len(candles) < ATR_PERIOD + 3:
        return 'WAIT', None
    live = candles[-1]
    closed = candles[-2]
    closed_end = closed['t'] + FIFTEEN_MIN_MS
    sym = setup['symbol']
    if live['t'] != closed_end:
        log('SETUP ANNULLATO', sym, '| dati 15m non consecutivi')
        return 'EXPIRED', None
    if closed['t'] < setup['macro_end_t']:
        return 'WAIT', None
    if now_ms - setup['macro_end_t'] > SETUP_LIFETIME_SECONDS * 1000:
        log('SETUP SCADUTO', sym, '| durata 4H superata')
        return 'EXPIRED', None
    if setup['last_checked_15m_t'] == closed['t']:
        return 'WAIT', None
    if (setup['last_checked_15m_t'] is not None and
            closed['t'] != setup['last_checked_15m_t'] + FIFTEEN_MIN_MS):
        log('SETUP ANNULLATO', sym, '| mancante una chiusura 15m')
        return 'EXPIRED', None
    setup['last_checked_15m_t'] = closed['t']
    if now_ms - closed_end > CLOSE_EVENT_MAX_DELAY_SECONDS * 1000:
        log('CONFERMA SCARTATA', sym, '| dato chiusura arrivato dopo',
            CLOSE_EVENT_MAX_DELAY_SECONDS, 'secondi')
        return 'WAIT', None

    atr15 = true_range_atr(candles[:-2])
    if not atr15 or atr15 <= 0:
        log('CONFERMA SCARTATA', sym, '| ATR15 indisponibile')
        return 'WAIT', None
    level = setup['level']
    tol = RETEST_ZONE_ATR15 * atr15
    d = setup['direction']
    touched = (closed['l'] <= level + tol and closed['h'] >= level - tol)
    if d == 'SHORT':
        touched = (closed['h'] >= level - tol and closed['l'] <= level + tol)

    # RETEST e invalidazione esattamente come nel codice inviato.
    if (d == 'LONG' and closed['l'] < level - tol) or (d == 'SHORT' and closed['h'] > level + tol):
        log('SETUP ANNULLATO', sym, '| wick retest troppo profondo',
            '| livello', price_format(level))
        return 'EXPIRED', None
    if touched and not setup['touched']:
        setup['touched'] = True
        setup['touch_t'] = closed['t']
        setup['touched_price'] = closed['l'] if d == 'LONG' else closed['h']
        log('RETEST 15M', sym, d, 'livello', price_format(level))

    if not setup['touched']:
        log('ATTESA RETEST', sym, d, '| candela 15m chiusa', closed['t'])
        return 'WAIT', None
    # Richiesta conferma 15m su candela successiva (non la stessa del retest).
    if closed['t'] <= setup['touch_t']:
        return 'WAIT', None
    if not confirmation_15m_valid(closed, d, level, atr15):
        span = closed['h'] - closed['l']
        body = abs(closed['c'] - closed['o'])
        log('CONFERMA SCARTATA', sym, '| prezzo/corpo/wick',
            '| corpo%', f'{100 * body / span:.1f}' if span > 0 else 'n/d',
            '| corpoATR', f'{body / atr15:.2f}')
        return 'WAIT', None
    ratio15 = confirmation_15m_volume_ratio(candles)
    if ratio15 < VOLUME_RATIO_MIN:
        log('CONFERMA SCARTATA', sym, '| volume', f'{ratio15:.2f}x',
            '< soglia', f'{VOLUME_RATIO_MIN:.2f}x')
        return 'WAIT', None
    btc_direction = btc15_closed_direction(btc_candles, closed['t'])
    if btc_direction not in (d, 'NEUTRAL'):
        log('CONFERMA SCARTATA', sym, '| BTC15', btc_direction, 'contro', d)
        return 'WAIT', None
    setup['atr15_retest'] = atr15
    setup['confirm_volume_ratio'] = ratio15
    signal = build_confirmed_signal(setup, candles, btc_direction, closed, now_ms)
    if signal:
        return 'SIGNAL', signal
    log('CONFERMA SCARTATA', sym, '| primo prezzo post-chiusura o SL non valido')
    return 'WAIT', None


def price_format(price):
    if price >= 1000:
        return f'{price:.2f}'
    if price >= 1:
        return f'{price:.4f}'
    if price >= 0.01:
        return f'{price:.6f}'
    return f'{price:.9f}'


def message_for(s):
    """Messaggio della stessa strategia alleggerita; aggiunge ID diagnostico."""
    d = 1 if s['direction'] == 'LONG' else -1
    risk = abs(s['entry'] - s['sl'])
    tp1 = s['entry'] + d * TP1_R * risk
    tp2 = s['entry'] + d * TP2_R * risk
    tp3 = s['entry'] + d * TP3_R * risk
    now_italy = datetime.now(ITALY).strftime('%d/%m/%Y %H:%M:%S')
    macro_italy = datetime.fromtimestamp(s['macro_t']/1000, ITALY).strftime('%d/%m %H:%M')
    retest_italy = datetime.fromtimestamp(s['retest_t']/1000, ITALY).strftime('%d/%m %H:%M')
    confirm_italy = datetime.fromtimestamp(s['confirm_15m_t']/1000, ITALY).strftime('%d/%m %H:%M')
    signal_id = f"{s['symbol']}-{s['macro_t']}-{s['confirm_15m_t']}"
    return (
        f"MOMENTUM ONE ALLEGGERITO 25 COIN - {s['direction']} [SIMULAZIONE]\n"
        f"{s['symbol'].replace('USDT', '/USDT')}\n"
        f"ID segnale: {signal_id}\n"
        f"Orario Italia: {now_italy}\n"
        f"Breakout 4H: {macro_italy} (10 barre, +0.03 ATR4H)\n"
        f"Retest 15m: {retest_italy} (+/-0.15 ATR15)\n"
        f"Conferma 15m: {confirm_italy}\n"
        f"Livello: {price_format(s['breakout_level'])}\n"
        f"Volume: {s['confirm_volume_ratio']:.2f}x (min {VOLUME_RATIO_MIN:.2f}x)\n"
        f"BTC15: {s['btc_direction']}\n"
        f"ENTRY: {price_format(s['entry'])}\n"
        f"SL 4H (3 chiuse +0.10 ATR4H): {price_format(s['sl'])}\n"
        f"TP1 0.30R: {price_format(tp1)} / TP2 0.80R: {price_format(tp2)} / TP3 1.50R: {price_format(tp3)}\n"
        "Solo avviso in simulazione, nessun ordine automatico."
    )


def enqueue_alert(signal):
    """Inserisce in una coda protetta: un solo job per (coin, breakout)."""
    key = (signal['symbol'], signal['macro_t'])
    now = time.monotonic()
    with alert_lock:
        if key in sent_alerts or key in pending_alerts:
            return False
        if now - failed_alert_times.get(key, -1e9) < TELEGRAM_RETRY_SECONDS:
            log('TELEGRAM: job in pausa per precedente errore', key)
            return False
        pending_alerts.add(key)
    try:
        telegram_queue.put_nowait((key, message_for(signal)))
        log('TELEGRAM IN CODA', key[0], 'macro', key[1])
        return True
    except Exception as exc:
        with alert_lock:
            pending_alerts.discard(key)
        log('TELEGRAM: impossibile accodare', key, repr(exc))
        return False


def telegram_worker():
    """Ritenta invii falliti e conferma solo con risposta positiva API.

    Finche' il job e' in retry resta in pending_alerts, quindi non viene
    accodato due volte. Nessun blocco temporale tra breakout diversi.
    """
    while True:
        key, message = telegram_queue.get()
        delivered = False
        try:
            started = time.monotonic()
            for attempt in range(1, TELEGRAM_MAX_ATTEMPTS + 1):
                try:
                    ok, retryable, retry_after = telegram_send_once(message)
                except Exception as exc:
                    log('TELEGRAM WORKER errore inatteso', repr(exc))
                    ok, retryable, retry_after = False, True, 0
                if ok:
                    delivered = True
                    with alert_lock:
                        sent_alerts.add(key)
                        failed_alert_times.pop(key, None)
                    log('TELEGRAM CONSEGNATO', key[0], '| macro', key[1], '| tentativo', attempt)
                    break
                elapsed = time.monotonic() - started
                if not retryable or attempt >= TELEGRAM_MAX_ATTEMPTS:
                    break
                delay = max(float(retry_after), min(
                    TELEGRAM_RETRY_MAX_SECONDS,
                    TELEGRAM_RETRY_INITIAL_SECONDS * 2 ** (attempt - 1),
                ))
                if elapsed + delay > TELEGRAM_DELIVERY_WINDOW_SECONDS:
                    break
                log('TELEGRAM RETRY', key[0], '| tentativo', attempt,
                    '| prossimo in', int(delay), 'secondi')
                time.sleep(delay)
            if not delivered:
                with alert_lock:
                    failed_alert_times[key] = time.monotonic()
                log('TELEGRAM NON CONSEGNATO', key[0], '| macro', key[1],
                    '| verificare token/chat/rete; segnale non marcato inviato')
        finally:
            with alert_lock:
                pending_alerts.discard(key)
            telegram_queue.task_done()


def current_snapshot():
    """Solo serie con WebSocket corrente e senza buchi; niente REST a ogni scan."""
    now = time.monotonic()
    with cache_lock:
        if not websocket_connected:
            return {}
        return {
            symbol: list(market_cache[symbol])
            for symbol in SYMBOLS
            if symbol in market_cache
            and symbol not in needs_refresh
            and now - last_ws_receive.get(symbol, -1e9) <= WS_STALE_SECONDS
        }


def scan_market():
    """Avvia un setup solo dopo chiusura 4H; valuta SOLO 15m chiuse."""
    snapshot = current_snapshot()
    if 'BTCUSDT' not in snapshot:
        return 0
    btc_candles = snapshot['BTCUSDT']
    now_ms = int(time.time() * 1000)
    if not (btc_candles[-1]['t'] <= now_ms < btc_candles[-1]['t'] + FIFTEEN_MIN_MS):
        return 0
    queued = 0
    for symbol, candles in snapshot.items():
        if candles[-1]['t'] != btc_candles[-1]['t']:
            continue
        four = four_hour_closed_candles(candles)
        if not four:
            continue
        macro_t = four[-1]['t']
        if seen_macro_bars.get(symbol) != macro_t:
            seen_macro_bars[symbol] = macro_t
            candidate = macro_4h_breakout(symbol, candles, now_ms)
            if candidate:
                active_setups[symbol] = candidate
                log('BREAKOUT 4H CHIUSO', symbol, candidate['direction'],
                    '| livello', price_format(candidate['level']),
                    '| volume 4H non richiesto')
            else:
                active_setups.pop(symbol, None)  # nuovo blocco 4H invalida il vecchio

        setup = active_setups.get(symbol)
        if not setup:
            continue
        if now_ms - setup['macro_end_t'] > SETUP_LIFETIME_SECONDS * 1000:
            log('SETUP SCADUTO', symbol, '| 4 ore senza segnale')
            del active_setups[symbol]
            continue
        status, signal = evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms)
        if status == 'EXPIRED':
            del active_setups[symbol]
        elif status == 'SIGNAL' and signal:
            if enqueue_alert(signal):
                queued += 1
                log('SEGNALE 4H + RETEST 15M', symbol, signal['direction'],
                    '| volume 15m CHIUSO', f"{signal['confirm_volume_ratio']:.2f}x",
                    '| BTC15', signal['btc_direction'])
                del active_setups[symbol]
            else:
                log('SEGNALE NON ACCODATO', symbol, '| duplicato, errore precedente o coda piena')
        # La scansione continua a monitorare il volume della 15m ancora LIVE,
        # ma NON puo' produrre segnali prima della chiusura di quella candela.
        if symbol in active_setups:
            monitor_live_confirmation_volume(setup, candles, now_ms)
    # Protezione ANTI-DUPLICATO per (coin, macro_t), non cooldown per crypto.
    # Le chiavi obsolete vengono rimosse dopo 8h: il setup scade gia' dopo 4h.
    oldest_allowed = now_ms - 2 * FOUR_HOURS_MS
    with alert_lock:
        sent_alerts.intersection_update({k for k in sent_alerts if k[1] >= oldest_allowed})
        for key in list(failed_alert_times):
            if key[1] < oldest_allowed:
                del failed_alert_times[key]
    return queued


def main():
    log('MOMENTUM ONE ALLEGGERITO | 25 crypto | scan 1s | 4H 10 barre +0.03 ATR + RETEST 0.15 ATR')
    log('TELEGRAM RETRY ATTIVO | NO COOLDOWN 8H | anti-duplicati per breakout 4H | log diagnostici attivi')
    if not BOT_TOKEN or not CHAT_ID:
        log('ATTENZIONE: mancano variabili Telegram')
    while True:
        try:
            initialize_history()
            break
        except Exception as exc:
            log('Storico Binance non disponibile:', exc, 'riprovo 20s')
            time.sleep(20)
    threading.Thread(target=telegram_worker, daemon=True).start()
    threading.Thread(target=history_worker, daemon=True).start()
    threading.Thread(target=websocket_worker, daemon=True).start()
    send_telegram('MOMENTUM ONE ALLEGGERITO 25 COIN AVVIATO - TELEGRAM RETRY ATTIVO - '
                  'Scan 1s - 4H 10 barre +0.03 ATR - Retest 0.15 ATR - '
                  'Vol 1.35x - Corpo 75%/0.50 ATR Wick 15% - SL 3x4H - '
                  'TP 0.30R/0.80R/1.50R - NO COOLDOWN 8H; duplicati stesso breakout bloccati. SOLO SIMULAZIONE.')
    next_scan = time.monotonic()
    last_log = next_scan
    while True:
        next_scan += SCAN_SECONDS
        try:
            scan_market()
        except Exception as exc:
            log('ERRORE SCANSIONE:', repr(exc))
        now = time.monotonic()
        if now - last_log >= LOG_INTERVAL_SECONDS:
            snap = current_snapshot()
            b = btc15_direction(snap['BTCUSDT']) if 'BTCUSDT' in snap else 'DATI NON FRESCHI'
            with alert_lock:
                in_delivery = len(pending_alerts)
            log('Scansione 1s | feed attivi:', len(snap), '/25 | BTC15:', b,
                '| setup 4H:', len(active_setups), '| Telegram in coda:', in_delivery)
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()


if __name__ == '__main__':
    main()
