"""MOMENTUM ONE UNIFICATO - 40 COIN | UN SOLO SERVIZIO RAILWAY.

Gruppo 1H: BTCUSDT, ETHUSDT, SOLUSDT.
- Breakout 1H CHIUSO sopra/sotto le 20 precedenti 1H, buffer 0.04 ATR1H,
  chiusura nel 35% direzionale; volume >=1.20x delle 20 precedenti 1H.
- Retest su 15m CHIUSA: tocco del livello (senza tolleranza ATR e senza limite wick); conferma 15m successiva CHIUSA
  con corpo >=70%, >=0.60 ATR15, wick <=12% per lato e volume >=0.60x
  media delle 20 precedenti 15m. SL ultime 5 1H +0.25 ATR1H. Setup 4h.
- BTC15 sul segnale BTC e' NEUTRAL (non filtra); su ETH/SOL concorde/neutro o eccezione forza relativa 4H >=5%.

Gruppo 4H: le altre 37 criptovalute (PARAMETRI PRECEDENTI INVARIATI).
- Breakout 4H CHIUSO su 10 precedenti 4H, buffer 0.03 ATR4H,
  chiusura nel 30% direzionale; volume >=1.10x delle 20 precedenti 4H.
- Retest 15m CHIUSA: tocco del livello (senza tolleranza ATR e senza limite wick); conferma successiva 15m CHIUSA con
  corpo >=70%, >=0.50 ATR15, wick <=15%, volume >=0.60x media 20 15m.
- SL ultime 3 4H +0.25 ATR4H. Setup 8h. BTC15 concorde/neutro o forza relativa 4H >=5% nella direzione.

Comune: 40 stream 15m WS, scan ~1s, storico 480 candele 15m,
ATR14 solo su chiuse, TP1/TP2/TP3=0.30R/0.80R/1.50R,
Telegram retry con anti-duplicato (coin, macro_t), nessun cooldown;
BTC neutro se corpo <0.35 ATR15; deroga direzione contraria solo se forza relativa 4H >=5%;
nessuna esecuzione ordini (solo avvisi in simulazione).
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
    # 15 nuove altcoin: SOLO strategia 4H; nessun filtro modificato.
    'HBARUSDT', 'XLMUSDT', 'TRXUSDT', 'ONDOUSDT', 'ENAUSDT',
    'WIFUSDT', 'TAOUSDT', '1000SHIBUSDT', '1000BONKUSDT', 'WLDUSDT',
    'FETUSDT', 'ATOMUSDT', 'BCHUSDT', 'JUPUSDT', 'PENDLEUSDT',
]

# Le 40 coppie restano monitorate in un solo processo.
ONE_HOUR_SYMBOLS = frozenset(('BTCUSDT', 'ETHUSDT', 'SOLUSDT'))
FOUR_HOUR_SYMBOLS = frozenset(s for s in SYMBOLS if s not in ONE_HOUR_SYMBOLS)
SCAN_SECONDS = 1.0
TIMEFRAME_SECONDS = 15 * 60
FIFTEEN_MIN_MS = 15 * 60 * 1000
ATR_PERIOD = 14
BTC_NEUTRAL_BODY_ATR_MAX = 0.35
VOLUME_AVG_BARS_15M = 20
LIVE_VOLUME_PACE_MIN_SECONDS = 60
LIVE_VOLUME_PROGRESS_LOG_SECONDS = 180
# Retest: deve toccare il livello di breakout; nessuna soglia ATR sul retest.
MICRO_CLOSE_BUFFER_ATR15 = 0.02
MACRO_CLOSE_GRACE_SECONDS = 1800
CLOSE_EVENT_MAX_DELAY_SECONDS = 30
TP1_R, TP2_R, TP3_R = 0.30, 0.80, 1.50
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
RELATIVE_BTC_OVERRIDE_PCT = 5.0  # 5 punti percentuali rispetto a BTC in 4H

# Parametri indipendenti per i due gruppi: nessuna sovrapposizione di segnali.
STRATEGIES = {
    '1H': {
        'name': '1H', 'macro_ms': ONE_HOUR_MS,
        'lookback': 20, 'macro_volume_min': 1.20,
        'volume_bars_macro': 20, 'breakout_buffer_atr': 0.04,
        'macro_close_position_min': 0.65,
        'confirm_volume_min': 0.60, 'body_fraction': 0.70,
        'body_atr_min': 0.60, 'wick_directional': 0.12,
        'wick_opposite': 0.12, 'sl_lookback': 5,
        'sl_buffer_atr': 0.25, 'lifetime_seconds': 4 * 60 * 60,
    },
    '4H': {
        'name': '4H', 'macro_ms': FOUR_HOURS_MS,
        'lookback': 10, 'macro_volume_min': 1.10,
        'volume_bars_macro': 20, 'breakout_buffer_atr': 0.03,
        'macro_close_position_min': 0.70,
        'confirm_volume_min': 0.60, 'body_fraction': 0.70,
        'body_atr_min': 0.50, 'wick_directional': 0.15,
        'wick_opposite': 0.15, 'sl_lookback': 3,
        'sl_buffer_atr': 0.25, 'lifetime_seconds': 8 * 60 * 60,
    },
}

def strategy_for(symbol):
    return STRATEGIES['1H'] if symbol in ONE_HOUR_SYMBOLS else STRATEGIES['4H']


# Configurazione affidabilita'/trasporto e storico sufficiente ai filtri 4H
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
HISTORY_MAXLEN = 480  # almeno 20 4H precedenti + breakout per le altcoin
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


def live_volume_progress(candles, now_ms, threshold):
    """Misura il volume cumulativo della candela LIVE senza anticipare il segnale.

    actual_ratio = volume gia' scambiato / media delle 20 precedenti CHIUSE.
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
        'progress_percent': 100.0 * actual_ratio / threshold,
    }


def monitor_live_confirmation_volume(setup, candles, now_ms):
    """Aggiorna ogni scan (~1s) il volume LIVE DOPO un retest CHIUSO.

    I log di avanzamento vengono limitati a uno ogni 3 minuti. Il volume
    La soglia raggiunta prima della chiusura e' solo PRELIMINARE:
    il segnale resta in evaluate_closed_retest_candle sulle 15m CHIUSE.
    """
    if not setup['touched'] or not candles:
        return
    if candles[-1]['t'] <= setup['touch_t']:
        return  # il retest stesso non puo' essere la candela di conferma
    threshold = strategy_for(setup['symbol'])['confirm_volume_min']
    progress = live_volume_progress(candles, now_ms, threshold)
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
    if (progress['actual_ratio'] >= threshold and
            not state['reached_final_volume_live']):
        state['reached_final_volume_live'] = True
        log('VOLUME LIVE', setup['symbol'], setup['direction'],
            f"{progress['actual_ratio']:.2f}x su {threshold:.2f}x",
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


def macro_closed_candles(candles_15m, macro_ms):
    """Ricostruisce candele 1H/4H solo da blocchi completi di 15m CHIUSE."""
    required_count = macro_ms // FIFTEEN_MIN_MS
    groups = {}
    for candle in candles_15m[:-1]:  # l'ultima 15m e' LIVE
        block_t = (candle['t'] // macro_ms) * macro_ms
        groups.setdefault(block_t, []).append(candle)
    result = []
    for block_t, group in sorted(groups.items()):
        if len(group) != required_count:
            continue
        group.sort(key=lambda c: c['t'])
        if any(c['t'] != block_t + i * FIFTEEN_MIN_MS for i, c in enumerate(group)):
            continue
        result.append({
            't': block_t, 'o': group[0]['o'],
            'h': max(c['h'] for c in group), 'l': min(c['l'] for c in group),
            'c': group[-1]['c'], 'v': sum(c['v'] for c in group),
        })
    return result


def macro_structural_stop(candles_15m, direction, cfg):
    """SL strutturale del gruppo 1H/4H e ATR solo su macro candele chiuse."""
    bars = macro_closed_candles(candles_15m, cfg['macro_ms'])
    if len(bars) < max(cfg['sl_lookback'], ATR_PERIOD + 1):
        return None
    atr = true_range_atr(bars)
    if atr is None or atr <= 0:
        return None
    structural = bars[-cfg['sl_lookback']:]
    live = candles_15m[-1]
    if direction == 'LONG':
        extreme = min(min(c['l'] for c in structural), live['l'])
        sl = extreme - cfg['sl_buffer_atr'] * atr
    else:
        extreme = max(max(c['h'] for c in structural), live['h'])
        sl = extreme + cfg['sl_buffer_atr'] * atr
    return {'sl': sl, 'atr_macro': atr, 'structure_extreme': extreme}


def btc15_direction(btc_candles):
    """BTC concorde oppure neutro; BTC opposto non neutro blocca il segnale.

    BTC e' neutro se il corpo live e' <0.35 ATR15 sulle sole chiuse.
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


def macro_breakout(symbol, candles, now_ms, cfg):
    """Breakout 1H sulle 3 maggiori crypto, 4H sulle altre 37, su candele CHIUSE."""
    macro_bars = macro_closed_candles(candles, cfg['macro_ms'])
    required = max(ATR_PERIOD + 2, cfg['lookback'] + 1, cfg['volume_bars_macro'] + 1)
    if len(macro_bars) < required:
        return None
    macro = macro_bars[-1]
    close_ms = macro['t'] + cfg['macro_ms']
    if not (0 <= now_ms - close_ms <= MACRO_CLOSE_GRACE_SECONDS * 1000):
        return None
    prev = macro_bars[:-1]
    if any(prev[i]['t'] + cfg['macro_ms'] != prev[i + 1]['t'] for i in range(len(prev)-1)):
        return None
    if prev[-1]['t'] + cfg['macro_ms'] != macro['t']:
        return None
    direction = ('LONG' if macro['c'] > macro['o'] else
                 'SHORT' if macro['c'] < macro['o'] else None)
    if direction is None:
        return None
    atr_macro = true_range_atr(prev)
    if not atr_macro or atr_macro <= 0:
        return None
    level_bars = prev[-cfg['lookback']:]
    level = max(c['h'] for c in level_bars) if direction == 'LONG' else min(c['l'] for c in level_bars)
    buffer = cfg['breakout_buffer_atr'] * atr_macro
    if not beyond_breakout(macro['c'], direction, level, buffer):
        return None
    span = macro['h'] - macro['l']
    if span <= 0:
        return None
    close_pos = ((macro['c']-macro['l']) / span if direction == 'LONG'
                 else (macro['h']-macro['c']) / span)
    if close_pos < cfg['macro_close_position_min']:
        return None
    vols = prev[-cfg['volume_bars_macro']:]
    avg_vol = sum(max(0.0, c['v']) for c in vols) / cfg['volume_bars_macro']
    if avg_vol <= 0:
        return None
    vol_ratio = max(0.0, macro['v']) / avg_vol
    if vol_ratio < cfg['macro_volume_min']:
        log('BREAKOUT', cfg['name'], 'SCARTATO VOLUME', symbol,
            f'{vol_ratio:.2f}x', '| minimo', f"{cfg['macro_volume_min']:.2f}x")
        return None
    if not macro_structural_stop(candles, direction, cfg):
        return None
    return {
        'symbol': symbol, 'direction': direction,
        'strategy': cfg['name'], 'macro_t': macro['t'],
        'macro_end_t': close_ms, 'level': level, 'macro_buffer': buffer,
        'atr_macro_at_breakout': atr_macro, 'macro_close_pos': close_pos,
        'macro_volume_ratio': vol_ratio, 'last_checked_15m_t': None,
        'touched': False, 'touch_t': None, 'touched_price': None,
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


def relative_performance_4h(alt_candles, btc_candles, confirmation_t):
    """Rendimento relativo percentuale su 4 ore esatte, senza usare candele live.

    Prende la chiusura 15m di conferma e la chiusura di 16 barre 15m prima,
    con 17 chiusure consecutive. Confronta altcoin e BTC allo stesso orario.
    Se mancano dati sincronizzati non concede l'eccezione BTC.
    """
    bars_4h = FOUR_HOURS_MS // FIFTEEN_MIN_MS
    needed = bars_4h + 2  # -1 live; -2 conferma chiusa; -18 quattro ore prima
    if len(alt_candles) < needed or len(btc_candles) < needed:
        return None
    alt_window = alt_candles[-needed:-1]
    btc_window = btc_candles[-needed:-1]
    if (len(alt_window) != bars_4h + 1 or
            alt_window[-1]['t'] != confirmation_t or
            btc_window[-1]['t'] != confirmation_t or
            alt_window[0]['t'] != btc_window[0]['t'] or
            any(a['t'] != b['t'] for a, b in zip(alt_window, btc_window))):
        return None
    if any(alt_window[i+1]['t'] - alt_window[i]['t'] != FIFTEEN_MIN_MS
           for i in range(bars_4h)):
        return None
    a0, a1 = alt_window[0]['c'], alt_window[-1]['c']
    b0, b1 = btc_window[0]['c'], btc_window[-1]['c']
    if min(a0, a1, b0, b1) <= 0:
        return None
    alt_perf = (a1 / a0 - 1.0) * 100.0
    btc_perf = (b1 / b0 - 1.0) * 100.0
    return {'alt_pct': alt_perf, 'btc_pct': btc_perf,
            'relative_pct': alt_perf - btc_perf}


def btc_direction_with_relative_exception(symbol, direction, btc_direction,
                                          alt_candles, btc_candles, confirmation_t):
    """Deroga BTC15 opposto solo con sovra/sottoperformance relativa >=5 pp.

    BTCUSDT mantiene la propria eccezione neutra, gestita dal chiamante.
    In assenza di dati affidabili resta attivo il filtro BTC15 originale.
    """
    if not ((direction == 'LONG' and btc_direction == 'SHORT') or
            (direction == 'SHORT' and btc_direction == 'LONG')):
        return btc_direction, None, False
    perf = relative_performance_4h(alt_candles, btc_candles, confirmation_t)
    if perf is None:
        return btc_direction, None, False
    relative = perf['relative_pct']
    if ((direction == 'LONG' and relative >= RELATIVE_BTC_OVERRIDE_PCT) or
            (direction == 'SHORT' and relative <= -RELATIVE_BTC_OVERRIDE_PCT)):
        log('ECCEZIONE BTC15', symbol, direction,
            '| BTC15 originale:', btc_direction,
            '| alt 4H:', f"{perf['alt_pct']:+.2f}%",
            '| BTC 4H:', f"{perf['btc_pct']:+.2f}%",
            '| forza relativa:', f'{relative:+.2f} pp',
            '| filtro BTC impostato NEUTRAL')
        return 'NEUTRAL', perf, True
    return btc_direction, perf, False


def confirmation_15m_volume_ratio(candles):
    """Volume della 15m appena CHIUSA vs media delle 20 CHIUSE precedenti.

    candles[-1] e' la 15m LIVE appena iniziata, candles[-2] la conferma chiusa.
    Nessuna proiezione: si richiede volume reale sulla conferma.
    """
    if len(candles) < VOLUME_AVG_BARS_15M + 2:
        return 0.0
    confirmation = candles[-2]
    previous = candles[-(VOLUME_AVG_BARS_15M + 2):-2]
    avg = sum(max(0.0, c['v']) for c in previous) / VOLUME_AVG_BARS_15M
    if avg <= 0:
        return 0.0
    return max(0.0, confirmation['v']) / avg


def confirmation_15m_valid(candle, direction, level, atr15, cfg):
    """Conferma 15m CHIUSA secondo i parametri specifici 1H oppure 4H."""
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
    if not beyond_breakout(closing, direction, level, MICRO_CLOSE_BUFFER_ATR15 * atr15):
        return False
    body = abs(closing - opening)
    if body / span < cfg['body_fraction']:
        return False
    if body / atr15 < cfg['body_atr_min']:
        return False
    upper_wick = max(0.0, candle['h'] - max(opening, closing)) / span
    lower_wick = max(0.0, min(opening, closing) - candle['l']) / span
    leading = upper_wick if direction == 'LONG' else lower_wick
    trailing = lower_wick if direction == 'LONG' else upper_wick
    return leading <= cfg['wick_directional'] and trailing <= cfg['wick_opposite']


def build_confirmed_signal(setup, candles, btc_direction, candle_15m, now_ms):
    """Entry sul primo tick dopo la chiusura 15m e stop strutturale per gruppo."""
    if btc_direction not in (setup['direction'], 'NEUTRAL'):
        return None
    live = candles[-1]
    price = live['c']
    if price <= 0 or not beyond_breakout(
            price, setup['direction'], setup['level'],
            MICRO_CLOSE_BUFFER_ATR15 * setup['atr15_retest']):
        return None
    cfg = strategy_for(setup['symbol'])
    info = macro_structural_stop(candles, setup['direction'], cfg)
    if not info:
        return None
    risk = price - info['sl'] if setup['direction'] == 'LONG' else info['sl'] - price
    if risk <= 0:
        return None
    return {
        'symbol': setup['symbol'], 'direction': setup['direction'],
        'strategy': setup['strategy'], 'entry': price, 'sl': info['sl'],
        'atr_macro': info['atr_macro'], 'stop_distance_pct': risk / price,
        'macro_t': setup['macro_t'], 'macro_end_t': setup['macro_end_t'],
        'confirm_15m_t': candle_15m['t'], 'retest_t': setup['touch_t'],
        'breakout_level': setup['level'], 'breakout_buffer': setup['macro_buffer'],
        'confirm_volume_ratio': setup['confirm_volume_ratio'],
        'macro_close_pos': setup['macro_close_pos'],
        'macro_volume_ratio': setup['macro_volume_ratio'],
        'btc_direction': btc_direction,
        'btc_override': setup.get('btc_override', False),
        'btc_relative_4h': setup.get('btc_relative_4h'),
    }


def evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms):
    """Retest senza ATR; conferma a chiusura e deroga BTC su forza relativa 4H."""
    if len(candles) < ATR_PERIOD + 3:
        return 'WAIT', None
    live = candles[-1]
    closed = candles[-2]
    closed_end = closed['t'] + FIFTEEN_MIN_MS
    sym = setup['symbol']
    cfg = strategy_for(sym)
    if live['t'] != closed_end:
        log('SETUP ANNULLATO', sym, '| dati 15m non consecutivi')
        return 'EXPIRED', None
    if closed['t'] < setup['macro_end_t']:
        return 'WAIT', None
    if now_ms - setup['macro_end_t'] > cfg['lifetime_seconds'] * 1000:
        log('SETUP SCADUTO', sym, '| durata', cfg['name'], 'superata')
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
    d = setup['direction']
    # Retest: la candela 15m CHIUSA deve toccare/attraversare il livello reale.
    # Nessuna tolleranza ATR15 e nessuna invalidazione per wick troppo profondo.
    # La conferma resta obbligatoria su una candela 15m SUCCESSIVA e chiusa.
    touched = closed['l'] <= level <= closed['h']
    if touched and not setup['touched']:
        setup['touched'] = True
        setup['touch_t'] = closed['t']
        setup['touched_price'] = closed['l'] if d == 'LONG' else closed['h']
        log('RETEST 15M', sym, d, '| tocco livello (nessun limite ATR)', price_format(level))

    if not setup['touched']:
        log('ATTESA RETEST', sym, d, '| candela 15m chiusa', closed['t'])
        return 'WAIT', None
    # Richiesta conferma 15m su candela successiva (non la stessa del retest).
    if closed['t'] <= setup['touch_t']:
        return 'WAIT', None
    if not confirmation_15m_valid(closed, d, level, atr15, cfg):
        span = closed['h'] - closed['l']
        body = abs(closed['c'] - closed['o'])
        log('CONFERMA SCARTATA', sym, '| prezzo/corpo/wick',
            '| corpo%', f'{100 * body / span:.1f}' if span > 0 else 'n/d',
            '| corpoATR', f'{body / atr15:.2f}')
        return 'WAIT', None
    ratio15 = confirmation_15m_volume_ratio(candles)
    if ratio15 < cfg['confirm_volume_min']:
        log('CONFERMA SCARTATA', sym, '| volume', f'{ratio15:.2f}x',
            '< soglia', f"{cfg['confirm_volume_min']:.2f}x")
        return 'WAIT', None
    btc_direction = ('NEUTRAL' if sym == 'BTCUSDT' and cfg['name'] == '1H'
                     else btc15_closed_direction(btc_candles, closed['t']))
    original_btc_direction = btc_direction
    btc_perf = None
    btc_override = False
    if sym != 'BTCUSDT':
        btc_direction, btc_perf, btc_override = btc_direction_with_relative_exception(
            sym, d, btc_direction, candles, btc_candles, closed['t'])
    if btc_direction not in (d, 'NEUTRAL'):
        relative_note = (f"| forza relativa 4H {btc_perf['relative_pct']:+.2f} pp"
                         if btc_perf is not None else '| forza relativa 4H non disponibile')
        log('CONFERMA SCARTATA', sym, '| BTC15', original_btc_direction,
            'contro', d, relative_note)
        return 'WAIT', None
    setup['btc_override'] = btc_override
    setup['btc_relative_4h'] = btc_perf
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
    """Segnale identificato come 1H/4H; nessun ordine automatico."""
    cfg = strategy_for(s['symbol'])
    d = 1 if s['direction'] == 'LONG' else -1
    risk = abs(s['entry'] - s['sl'])
    tp1 = s['entry'] + d * TP1_R * risk
    tp2 = s['entry'] + d * TP2_R * risk
    tp3 = s['entry'] + d * TP3_R * risk
    now_italy = datetime.now(ITALY).strftime('%d/%m/%Y %H:%M:%S')
    macro_italy = datetime.fromtimestamp(s['macro_t']/1000, ITALY).strftime('%d/%m %H:%M')
    retest_italy = datetime.fromtimestamp(s['retest_t']/1000, ITALY).strftime('%d/%m %H:%M')
    confirm_italy = datetime.fromtimestamp(s['confirm_15m_t']/1000, ITALY).strftime('%d/%m %H:%M')
    title = ('MOMENTUM ONE 1H - BTC ETH SOL' if cfg['name'] == '1H'
             else 'MOMENTUM ONE 4H - 37 ALTCOIN')
    signal_id = f"{s['symbol']}-{s['macro_t']}-{s['confirm_15m_t']}"
    return (
        f"{title} - {s['direction']} [SIMULAZIONE]\n"
        f"{s['symbol'].replace('USDT', '/USDT')}\n"
        f"ID segnale: {signal_id}\n"
        f"Orario Italia: {now_italy}\n"
        f"Breakout {cfg['name']}: {macro_italy} ({cfg['lookback']} barre, +{cfg['breakout_buffer_atr']:.2f} ATR{cfg['name']})\n"
        f"Volume breakout {cfg['name']}: {s['macro_volume_ratio']:.2f}x (min {cfg['macro_volume_min']:.2f}x su 20 {cfg['name']})\n"
        f"Retest 15m: {retest_italy} (tocco livello, nessun filtro ATR)\n"
        f"Conferma 15m: {confirm_italy}\n"
        f"Livello: {price_format(s['breakout_level'])}\n"
        f"Volume rimbalzo 15m: {s['confirm_volume_ratio']:.2f}x (min {cfg['confirm_volume_min']:.2f}x su 20 15m)\n"
        f"BTC15: {s['btc_direction']}" +
        (f" (DEROGA forza relativa 4H {s['btc_relative_4h']['relative_pct']:+.2f} pp)"
         if s.get('btc_override') and s.get('btc_relative_4h') else '') + "\n"
        f"ENTRY: {price_format(s['entry'])}\n"
        f"SL {cfg['name']} ({cfg['sl_lookback']} chiuse +{cfg['sl_buffer_atr']:.2f} ATR{cfg['name']}): {price_format(s['sl'])}\n"
        f"TP1 0.30R: {price_format(tp1)} / TP2 0.80R: {price_format(tp2)} / TP3 1.50R: {price_format(tp3)}\n"
        'Solo avviso in simulazione, nessun ordine automatico.'
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
    """Un'unica scansione: 1H per BTC/ETH/SOL, 4H per le altre 37."""
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
        cfg = strategy_for(symbol)
        bars = macro_closed_candles(candles, cfg['macro_ms'])
        if not bars:
            continue
        macro_t = bars[-1]['t']
        if seen_macro_bars.get(symbol) != macro_t:
            seen_macro_bars[symbol] = macro_t
            candidate = macro_breakout(symbol, candles, now_ms, cfg)
            if candidate:
                active_setups[symbol] = candidate
                log('BREAKOUT', cfg['name'], 'CHIUSO', symbol,
                    candidate['direction'], '| livello', price_format(candidate['level']),
                    '| volume', f"{candidate['macro_volume_ratio']:.2f}x", '| media 20 precedenti')
            # Non cancellare i setup ancora validi quando la candela macro
            # successiva non crea un nuovo breakout: durate 4H (gruppo 1H)
            # e 8H (gruppo 4H) realmente rispettate.
        setup = active_setups.get(symbol)
        if not setup:
            continue
        if now_ms - setup['macro_end_t'] > cfg['lifetime_seconds'] * 1000:
            log('SETUP SCADUTO', cfg['name'], symbol, '| tempo massimo superato')
            del active_setups[symbol]
            continue
        status, signal = evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms)
        if status == 'EXPIRED':
            del active_setups[symbol]
        elif status == 'SIGNAL' and signal:
            if enqueue_alert(signal):
                queued += 1
                log('SEGNALE', cfg['name'], '+ RETEST 15M', symbol, signal['direction'],
                    '| volume 15m', f"{signal['confirm_volume_ratio']:.2f}x",
                    '| BTC15', signal['btc_direction'])
                del active_setups[symbol]
            else:
                log('SEGNALE NON ACCODATO', cfg['name'], symbol, '| duplicato o errore coda')
        if symbol in active_setups:
            monitor_live_confirmation_volume(setup, candles, now_ms)
    oldest_allowed = now_ms - 3 * FOUR_HOURS_MS  # memoria duplicati oltre 8h di setup
    with alert_lock:
        sent_alerts.intersection_update({k for k in sent_alerts if k[1] >= oldest_allowed})
        for key in list(failed_alert_times):
            if key[1] < oldest_allowed:
                del failed_alert_times[key]
    return queued


def main():
    log('MOMENTUM ONE UNIFICATO | 40 crypto | 3 strategia 1H + 37 strategia 4H | scan ~1s')
    log('1H BTC/ETH/SOL: volume breakout >=1.20x 20 1H; conferma 15m >=0.60x 20 15m; setup 4h')
    log('4H altre 37: volume breakout >=1.10x 20 4H; conferma 15m >=0.60x 20 15m; setup 8h')
    log('RETEST 15m: tocco livello senza ATR; BTC neutro <0.35 ATR15; eccezione alt-BTC +/-5pp su 4H')
    log('TELEGRAM RETRY ATTIVO | NO COOLDOWN | anti-duplicati per breakout')
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
    send_telegram('MOMENTUM ONE UNIFICATO AVVIATO [SIMULAZIONE] - 40 crypto, scan ~1s. '
                  'BTC ETH SOL: BREAKOUT 1H volume >=1.20x su 20 1H; '
                  'RETEST 15m senza filtro ATR; CONFERMA 15m volume >=0.60x su 20 15m; '
                  'corpo >=70%, >=0.60 ATR15, wick <=12%; SL 5x1H +0.25 ATR1H; setup 4h. '
                  'ALTRE 37: BREAKOUT 4H volume >=1.10x su 20 4H; '
                  'RETEST 15m senza filtro ATR; CONFERMA 15m volume >=0.60x su 20 15m; '
                  'corpo >=70%, >=0.50 ATR15, wick <=15%; SL 3x4H +0.25 ATR4H; setup 8h. '
                  'BTC neutro se corpo <0.35 ATR15; altcoin con forza relativa +/-5pp su 4H '
                  'possono superare BTC15 opposto. BTC neutro solo su BTC. '
                  'TP1 0.30R / TP2 0.80R / TP3 1.50R. NO COOLDOWN | RETRY TELEGRAM | '
                  'Nessun ordine automatico.')
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
            s1 = sum(1 for s in active_setups if s in ONE_HOUR_SYMBOLS)
            s4 = sum(1 for s in active_setups if s in FOUR_HOUR_SYMBOLS)
            log('Scansione ~1s | feed attivi:', len(snap), '/40 | BTC15:', b,
                '| setup 1H:', s1, '| setup 4H:', s4, '| Telegram in coda:', in_delivery)
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()


if __name__ == '__main__':
    main()
