"""CryptoSignalAI MOMENTUM ONE - BREAKOUT 4 ORE | DUE CONFERME ALTERNATIVE.

SOLO SIMULAZIONE: un tipo di alert LONG/SHORT, nessun ordine automatico.
25 coppie Binance USD-M Futures, candele 15m live via WebSocket, scan ~1s.

TRIGGER: candela LIVE >=60s, corpo >=82% e >=0.65 ATR15, wick 8%/10%,
volume progressivo >=1.80x; prezzo oltre massimo/minimo delle 16 candele 15m
precedenti chiuse (4 ore di mercato) di >=0.05 ATR15. BTC15 concorde/neutro.

DUE POSSIBILITA' (una sola basta):
A) Il prezzo rimane oltre il livello congelato di rottura + buffer per 600s
   CONTINUI: se rientra, si azzera il timer. Se il WebSocket manca, il setup
   viene annullato per non dare false conferme.
B) La candela di breakout CHIUDE oltre il livello con corpo/volume validi;
   una candela SUCCESSIVA torna entro +/-0.10 ATR15 dal livello (retest),
   senza oltrepassarlo dalla parte sbagliata di piu' di 0.10 ATR15. Dopo
   il ritorno oltre il livello + buffer e un rimbalzo di 0.05 ATR15 dal
   prezzo di tocco, conferma per 20s consecutivi. Retest non obbligato ad
   avere corpo 82%/volume 1.80x, gia' richiesti sulla rottura.

Una sola notifica per setup; livello fisso anche se cambia la candela 15m.
Setup scade dopo 60 minuti per non inseguire vecchie rotture.
SL oltre estremo di 2 candele 4H CHIUSE +0.10 ATR4H, senza limite percentuale;
TP1=0.30R, TP2=0.80R, TP3=1.50R calcolati sull'entry effettiva.
SL e TP SOLO INFORMATIVI, non sono ordini inseriti sull'exchange.
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

# Soli filtri di INGRESSO (uguali per LONG e SHORT)
SCAN_SECONDS = 1.0
TIMEFRAME_SECONDS = 15 * 60
BODY_FRACTION_MIN = 0.82
LEADING_WICK_MAX = 0.08     # LONG wick alto; SHORT wick basso
TRAILING_WICK_MAX = 0.10    # LONG wick basso; SHORT wick alto
BODY_ATR_MIN = 0.65
VOLUME_RATIO_MIN = 1.80
MIN_CANDLE_AGE_SECONDS = 60.0
HOLD_SECONDS = 600.0           # permanenza oltre il livello per 10 minuti
RETEST_CONFIRM_SECONDS = 20.0  # conferma rimbalzo
RETEST_ZONE_ATR15 = 0.10       # distanza consentita dal livello nel retest
RETEST_REBOUND_ATR15 = 0.05    # spinta positiva dal prezzo di tocco
SETUP_LIFETIME_SECONDS = 3600 # setup valido massimo 1 ora
BTC_NEUTRAL_BODY_ATR_MAX = 0.20  # neutralita' di BTC 15m live
ATR_PERIOD = 14
VOLUME_AVG_BARS = 10
BREAKOUT_15M_LOOKBACK = 16     # 4 ore = 16 candele 15m CHIUSE
BREAKOUT_BUFFER_ATR15 = 0.05  # distanza minima oltre il livello
# SL/TP calcolati sul timeframe 4H, senza filtri direzionali su 4H.
SL_4H_LOOKBACK = 2          # ultime 2 candele 4H COMPLETE
SL_BUFFER_ATR4H = 0.10      # buffer oltre struttura 4H
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
active_setups = {}              # {symbol: stato di breakout/retest, con livello congelato}
used_breakout_bars = set()       # impedisce di ricreare setup sulla stessa candela


def log(*parts):
    print(*parts, flush=True)


def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        log('TELEGRAM NON CONFIGURATO: impostare TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID')
        return False
    try:
        response = requests.post(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            data={'chat_id': CHAT_ID, 'text': text}, timeout=20,
        )
        response.raise_for_status()
        if not response.json().get('ok'):
            log('Telegram ha rifiutato il messaggio:', response.text[:250])
            return False
        return True
    except (requests.RequestException, ValueError) as exc:
        log('Errore Telegram:', exc)
        return False


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


def live_volume_pace(candles, now_ms):
    """Volume proiettato a 15m / volume medio delle 10 precedenti chiuse."""
    if len(candles) < VOLUME_AVG_BARS + 1:
        return 0.0
    live = candles[-1]
    previous = candles[-(VOLUME_AVG_BARS + 1):-1]
    avg = sum(c['v'] for c in previous) / len(previous)
    if avg <= 0:
        return 0.0
    elapsed = min(float(TIMEFRAME_SECONDS), max(1.0, (now_ms - live['t']) / 1000.0))
    return max(0.0, live['v']) / (avg * elapsed / TIMEFRAME_SECONDS)



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


def breakout_candidate(symbol, candles, btc_direction, now_ms):
    """Crea un candidato esclusivamente sulla candela di rottura 15m live."""
    required = max(ATR_PERIOD + 2, VOLUME_AVG_BARS + 1, BREAKOUT_15M_LOOKBACK + 1)
    if len(candles) < required:
        return None
    current = candles[-1]
    if not (current['t'] <= now_ms < current['t'] + FIFTEEN_MIN_MS):
        return None
    opening, closing = current['o'], current['c']
    if opening == closing:
        return None
    direction = 'LONG' if closing > opening else 'SHORT'
    if btc_direction not in (direction, 'NEUTRAL'):
        return None
    elapsed = (now_ms - current['t']) / 1000.0
    if elapsed < MIN_CANDLE_AGE_SECONDS:
        return None
    previous = candles[-(BREAKOUT_15M_LOOKBACK + 1):-1]
    if any(previous[i]['t'] + FIFTEEN_MIN_MS != previous[i+1]['t'] for i in range(len(previous)-1)):
        return None
    if previous[-1]['t'] + FIFTEEN_MIN_MS != current['t']:
        return None
    span = current['h'] - current['l']
    if span <= 0:
        return None
    body = abs(closing - opening)
    fraction = body / span
    leading = ((current['h'] - closing) if direction == 'LONG' else (closing - current['l'])) / span
    trailing = ((opening - current['l']) if direction == 'LONG' else (current['h'] - opening)) / span
    if fraction < BODY_FRACTION_MIN or leading > LEADING_WICK_MAX or trailing > TRAILING_WICK_MAX:
        return None
    atr = true_range_atr(candles[:-1])
    if not atr or body / atr < BODY_ATR_MIN:
        return None
    buffer = BREAKOUT_BUFFER_ATR15 * atr
    level = max(c['h'] for c in previous) if direction == 'LONG' else min(c['l'] for c in previous)
    if not beyond_breakout(closing, direction, level, buffer):
        return None
    pace = live_volume_pace(candles, now_ms)
    if pace < VOLUME_RATIO_MIN:
        return None
    # Manteniamo lo stop 4H anche per i trigger, cosi' non si avvia un setup senza storico.
    if not four_hour_structural_stop(candles, direction):
        return None
    return {
        'symbol': symbol, 'direction': direction, 'breakout_candle_start': current['t'],
        'level': level, 'buffer': buffer, 'atr15': atr,
        'breakout_volume_pace': pace, 'breakout_body_fraction': fraction,
        'breakout_body_atr': body / atr, 'breakout_leading_wick': leading,
        'breakout_trailing_wick': trailing,
    }


def beyond_breakout(price, direction, level, buffer):
    return price >= level + buffer if direction == 'LONG' else price <= level - buffer


def breakout_candle_closed_valid(candles, setup):
    """Retest attivabile SOLO se la candela di rottura chiude forte oltre il livello."""
    ix = next((i for i, c in enumerate(candles[:-1]) if c['t'] == setup['breakout_candle_start']), None)
    if ix is None or ix < max(ATR_PERIOD + 1, VOLUME_AVG_BARS):
        return False
    candle = candles[ix]
    d = setup['direction']
    span = candle['h'] - candle['l']
    if span <= 0 or not beyond_breakout(candle['c'], d, setup['level'], setup['buffer']):
        return False
    body = abs(candle['c'] - candle['o'])
    if (candle['c'] <= candle['o']) if d == 'LONG' else (candle['c'] >= candle['o']):
        return False
    lead = (candle['h'] - candle['c']) / span if d == 'LONG' else (candle['c'] - candle['l']) / span
    trail = (candle['o'] - candle['l']) / span if d == 'LONG' else (candle['h'] - candle['o']) / span
    if body/span < BODY_FRACTION_MIN or lead > LEADING_WICK_MAX or trail > TRAILING_WICK_MAX:
        return False
    if body / setup['atr15'] < BODY_ATR_MIN:
        return False
    avg = sum(c['v'] for c in candles[ix - VOLUME_AVG_BARS:ix]) / VOLUME_AVG_BARS
    if avg <= 0 or candle['v'] / avg < VOLUME_RATIO_MIN:
        return False
    return True


def build_signal(setup, candles, btc_direction, mode, now_ms):
    """Ricalcola SL4H e target R sul PREZZO DEL SEGNALE, non sul breakout vecchio."""
    live = candles[-1]
    sl_info = four_hour_structural_stop(candles, setup['direction'])
    if not sl_info:
        return None
    price = live['c']
    risk = (price - sl_info['sl']) if setup['direction'] == 'LONG' else (sl_info['sl'] - price)
    if price <= 0 or risk <= 0:
        return None
    # Controllo coerenza con la direzione prima dell'invio.
    if not beyond_breakout(price, setup['direction'], setup['level'], setup['buffer']):
        return None
    s = {
        'symbol': setup['symbol'], 'direction': setup['direction'], 'entry': price,
        'candle_start': setup['breakout_candle_start'], 'entry_candle_start': live['t'],
        'confirmation_mode': mode,
        'body_fraction': setup['breakout_body_fraction'], 'body_atr': setup['breakout_body_atr'],
        'leading_wick': setup['breakout_leading_wick'], 'trailing_wick': setup['breakout_trailing_wick'],
        'volume_pace': setup['breakout_volume_pace'], 'btc_direction': btc_direction,
        'atr15': setup['atr15'], 'sl': sl_info['sl'], 'atr4h': sl_info['atr4h'],
        'stop_distance_pct': risk / price,
        'breakout_level': setup['level'], 'breakout_buffer': setup['buffer'],
        'elapsed': (now_ms - setup['breakout_candle_start'])/1000.0,
    }
    return s


def create_setup(candidate, mono):
    return {
        **candidate,
        'created': mono,
        'hold_started': mono,
        'breakout_closed_checked': False,
        'retest_armed': False,
        'retest_touched': False,
        'touch_price': None,
        'rebound_started': None,
    }


def update_setup(setup, candles, btc_direction, now_ms, mono):
    """Processa A (10 min continui) o B (chiusura + retest + rimbalzo 20s)."""
    live = candles[-1]
    d = setup['direction']
    price = live['c']
    level = setup['level']
    buffer = setup['buffer']
    atr = setup['atr15']
    if mono - setup['created'] >= SETUP_LIFETIME_SECONDS:
        return 'EXPIRED', None
    if now_ms < live['t'] or now_ms >= live['t'] + FIFTEEN_MIN_MS:
        return 'EXPIRED', None

    above = beyond_breakout(price, d, level, buffer)
    if above:
        if setup['hold_started'] is None:
            setup['hold_started'] = mono
    else:
        setup['hold_started'] = None

    if not setup['breakout_closed_checked'] and live['t'] > setup['breakout_candle_start']:
        setup['breakout_closed_checked'] = True
        initial_closed = next((c for c in candles[:-1]
                               if c['t'] == setup['breakout_candle_start']), None)
        # Una chiusura dalla parte sbagliata invalida entrambe le alternative.
        if initial_closed is None or not beyond_breakout(
                initial_closed['c'], d, level, buffer):
            return 'EXPIRED', None
        # Il retest richiede un breakout CHIUSO forte; la tenuta 10m richiede
        # invece la forza al trigger e 600s sopra/sotto il livello congelato.
        setup['retest_armed'] = breakout_candle_closed_valid(candles, setup)

    btc_ok = btc_direction in (d, 'NEUTRAL')
    if above and setup['hold_started'] is not None and mono - setup['hold_started'] >= HOLD_SECONDS:
        # Finche' siamo sulla candela di rottura, anche il volume live deve essere alto.
        if live['t'] != setup['breakout_candle_start'] or live_volume_pace(candles, now_ms) >= VOLUME_RATIO_MIN:
            if btc_ok:
                s = build_signal(setup, candles, btc_direction, 'TENUTA_10_MIN', now_ms)
                if s:
                    return 'SIGNAL', s

    if not setup['retest_armed'] or live['t'] <= setup['breakout_candle_start']:
        return 'WAIT', None

    tol = RETEST_ZONE_ATR15 * atr
    # Uno sfondamento profondo invalida il retest; si evita di scambiarlo per un rimbalzo.
    deep_break = live['l'] < level - tol if d == 'LONG' else live['h'] > level + tol
    if deep_break:
        return 'EXPIRED', None

    if not setup['retest_touched']:
        # Il PREZZO LIVE entra nella zona +/-0.10 ATR attorno al livello.
        if abs(price - level) <= tol:
            setup['retest_touched'] = True
            setup['touch_price'] = price
            setup['rebound_started'] = None
            log('RETEST', setup['symbol'], d, 'livello', price_format(level))
    if setup['retest_touched']:
        if d == 'LONG':
            bounce_target = max(level + buffer, setup['touch_price'] + RETEST_REBOUND_ATR15 * atr)
            bounced = price >= bounce_target
        else:
            bounce_target = min(level - buffer, setup['touch_price'] - RETEST_REBOUND_ATR15 * atr)
            bounced = price <= bounce_target
        if bounced:
            if setup['rebound_started'] is None:
                setup['rebound_started'] = mono
            if mono - setup['rebound_started'] >= RETEST_CONFIRM_SECONDS and btc_ok:
                s = build_signal(setup, candles, btc_direction, 'RETEST_RIMBALZO', now_ms)
                if s:
                    return 'SIGNAL', s
        else:
            setup['rebound_started'] = None
    return 'WAIT', None

def price_format(price):
    if price >= 1000:
        return f'{price:.2f}'
    if price >= 1:
        return f'{price:.4f}'
    if price >= 0.01:
        return f'{price:.6f}'
    return f'{price:.9f}'


def message_for(signal):
    """Invia SL 4H + TP in R, distinguendo il breakout dal retest."""
    s = signal
    d = 1 if s['direction'] == 'LONG' else -1
    risk = abs(s['entry'] - s['sl'])
    tp1 = s['entry'] + d * TP1_R * risk
    tp2 = s['entry'] + d * TP2_R * risk
    tp3 = s['entry'] + d * TP3_R * risk
    now_italy = datetime.now(ITALY).strftime('%d/%m/%Y %H:%M:%S')
    candle_italy = datetime.fromtimestamp(s['candle_start']/1000, ITALY).strftime('%d/%m %H:%M')
    entry_italy = datetime.fromtimestamp(s['entry_candle_start']/1000, ITALY).strftime('%d/%m %H:%M')
    confirmation = 'Permanenza di 10 minuti' if s['confirmation_mode'] == 'TENUTA_10_MIN' else 'Retest + rimbalzo 20 secondi'
    return (
        f"MOMENTUM ONE 4H - {s['direction']} [SIMULAZIONE]\n"
        f"{s['symbol'].replace('USDT', '/USDT')}\n"
        f"Conferma: {confirmation}\n"
        f"Orario Italia: {now_italy}\n"
        f"Candela breakout: {candle_italy} | candela entry: {entry_italy}\n"
        f"ENTRY indicativa: {price_format(s['entry'])}\n"
        f"SL 4H (2 chiuse + 0.10 ATR4H): {price_format(s['sl'])}\n"
        f"Distanza SL: {s['stop_distance_pct']*100:.2f}% (nessun tetto)\n"
        f"TP1 (0.30R): {price_format(tp1)}\n"
        f"TP2 (0.80R): {price_format(tp2)}\n"
        f"TP3 (1.50R): {price_format(tp3)}\n\n"
        f"Corpo BREAKOUT: {s['body_fraction']*100:.1f}% | {s['body_atr']:.2f} ATR15\n"
        f"Wick breakout direzionale/opposto: {s['leading_wick']*100:.1f}% / {s['trailing_wick']*100:.1f}%\n"
        f"Volume progressivo al breakout: {s['volume_pace']:.2f}x\n"
        f"Livello ultime 4h: {price_format(s['breakout_level'])}, buffer {price_format(s['breakout_buffer'])}\n"
        f"BTC15: {s['btc_direction']} (neutro <0.20 ATR15)\n"
        f"SOLO AVVISO INFORMATIVO. SL/TP non sono ordini.\n"
        f"Trading sperimentale; gestione rischio e liquidazione restano a carico dell'utente."
    )

def enqueue_alert(signal):
    key = (signal['symbol'], signal['candle_start'])
    now = time.monotonic()
    with alert_lock:
        if key in sent_alerts or key in pending_alerts:
            return False
        if now - failed_alert_times.get(key, -1e9) < TELEGRAM_RETRY_SECONDS:
            return False
        pending_alerts.add(key)
    try:
        telegram_queue.put_nowait((key, message_for(signal)))
        return True
    except queue.Full:
        with alert_lock:
            pending_alerts.discard(key)
        log('Coda Telegram piena:', key)
        return False


def telegram_worker():
    while True:
        key, message = telegram_queue.get()
        try:
            ok = send_telegram(message)
            with alert_lock:
                if ok:
                    sent_alerts.add(key)
                else:
                    failed_alert_times[key] = time.monotonic()
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
    """Due strade indipendenti: tenuta 600s oppure breakout chiuso + retest."""
    snapshot = current_snapshot()
    if 'BTCUSDT' not in snapshot:
        active_setups.clear()  # feed assente: non si puo' certificare permanenza continua
        return 0
    now_ms = int(time.time() * 1000)
    btc_candles = snapshot['BTCUSDT']
    btc_live = btc_candles[-1]
    if not (btc_live['t'] <= now_ms < btc_live['t'] + FIFTEEN_MIN_MS):
        active_setups.clear()
        return 0
    btc_direction = btc15_direction(btc_candles)
    if btc_direction is None:
        active_setups.clear()
        return 0
    mono = time.monotonic()
    queued = 0
    for symbol in list(active_setups):
        if symbol not in snapshot or snapshot[symbol][-1]['t'] != btc_live['t']:
            del active_setups[symbol]  # un buco nel feed annulla conferma e setup
    for symbol, candles in snapshot.items():
        if candles[-1]['t'] != btc_live['t']:
            continue
        setup = active_setups.get(symbol)
        if setup is None:
            key = (symbol, candles[-1]['t'])
            if key in used_breakout_bars:
                continue
            with alert_lock:
                if key in sent_alerts or key in pending_alerts:
                    continue
            candidate = breakout_candidate(symbol, candles, btc_direction, now_ms)
            if candidate is None:
                continue
            used_breakout_bars.add(key)
            setup = create_setup(candidate, mono)
            active_setups[symbol] = setup
            log('BREAKOUT CANDIDATO', symbol, setup['direction'],
                '| livello', price_format(setup['level']), '| volume', f"{setup['breakout_volume_pace']:.2f}x")
        status, signal = update_setup(setup, candles, btc_direction, now_ms, mono)
        if status == 'EXPIRED':
            del active_setups[symbol]
            continue
        if status == 'SIGNAL' and signal:
            if enqueue_alert(signal):
                queued += 1
                log('SEGNALE', signal['direction'], symbol, '|', signal['confirmation_mode'],
                    '| BTC15', btc_direction)
                del active_setups[symbol]
    # Storico deduplica limitato a 4 ore; nessun doppio avviso per medesimo setup.
    oldest_allowed = btc_live['t'] - FOUR_HOURS_MS
    used_breakout_bars.intersection_update({k for k in used_breakout_bars if k[1] >= oldest_allowed})
    with alert_lock:
        sent_alerts.intersection_update({k for k in sent_alerts if k[1] >= oldest_allowed})
        for key in list(failed_alert_times):
            if key[1] < oldest_allowed:
                del failed_alert_times[key]
    return queued

def main():
    log('MOMENTUM ONE | 25 crypto | scan ~1s | 4H breakout + TENUTA 10 MIN / RETEST')
    log('Corpo 82% / 0.65 ATR15, wick 8%/10%, volume 1.80x, buffer 0.05 ATR15, BTC15 favorevole/neutro')
    log('SL 2 candele 4H chiuse +0.10 ATR4H senza limite; TP 0.30R / 0.80R / 1.50R')
    if not BOT_TOKEN or not CHAT_ID:
        log('ATTENZIONE: mancano le variabili ambiente Telegram; niente notifiche.')
    while True:
        try:
            initialize_history()
            break
        except Exception as exc:
            log('Storico Binance non disponibile:', exc, 'riprovo fra 20s')
            time.sleep(20)
    threading.Thread(target=telegram_worker, daemon=True).start()
    threading.Thread(target=history_worker, daemon=True).start()
    threading.Thread(target=websocket_worker, daemon=True).start()
    send_telegram('MOMENTUM ONE 4H AVVIATO [SIMULAZIONE]. 25 crypto, scan ~1s. '
                  'Breakout 4 ore (16x15m chiuse) +0.05 ATR15 con candela forte: '
                  'corpo 82%, >=0.65 ATR15, wick 8%/10%, volume progressivo >=1.80x. '
                  'Conferma ALTERNATIVA A: 10 minuti CONTINUI sopra/sotto il livello; '
                  'OPPURE B: candela di breakout chiusa forte, retest +/-0.10 ATR15 '
                  'e rimbalzo di 0.05 ATR15 per 20 secondi. BTC15 concorde/neutro. '
                  'SL ultime 2 candele 4H chiuse +0.10 ATR4H, senza limite percentuale. '
                  'TP1 0.30R / TP2 0.80R / TP3 1.50R sullo stop 4H. '
                  'Solo segnali, nessun ordine automatico.')
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
            log('Scansione ~1s | feed attivi:', len(snap), '/25 | BTC15:', b,
                '| setup in attesa:', len(active_setups))
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()


if __name__ == '__main__':
    main()
