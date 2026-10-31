"""MOMENTUM ONE UNIFICATO - 40 COIN | UN SOLO SERVIZIO RAILWAY.
Gruppo 1H: BTCUSDT, ETHUSDT, SOLUSDT.
Gruppo 4H: le altre 37 criptovalute.
FIX FEED: tollera 1 candela di ritardo, feed attivi 40/40 stabile
"""

import json
import os
import queue
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from statistics import median
from zoneinfo import ZoneInfo
import requests
import websocket

BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

SYMBOLS = [
    'SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT',
    'DOGEUSDT', 'AVAXUSDT', 'SUIUSDT', 'LINKUSDT', 'ADAUSDT',
    'NEARUSDT', 'UNIUSDT', 'LTCUSDT', 'DOTUSDT', 'APTUSDT',
    'ARBUSDT', 'OPUSDT', 'INJUSDT', 'RENDERUSDT', '1000PEPEUSDT',
    'FILUSDT', 'ETCUSDT', 'AAVEUSDT', 'TIAUSDT', 'SEIUSDT',
    'HBARUSDT', 'XLMUSDT', 'TRXUSDT', 'ONDOUSDT', 'ENAUSDT',
    'WIFUSDT', 'TAOUSDT', '1000SHIBUSDT', '1000BONKUSDT', 'WLDUSDT',
    'FETUSDT', 'ATOMUSDT', 'BCHUSDT', 'JUPUSDT', 'PENDLEUSDT',
]

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
MICRO_CLOSE_BUFFER_ATR15 = 0.02
MACRO_CLOSE_GRACE_SECONDS = 1800
CLOSE_EVENT_MAX_DELAY_SECONDS = 30
TP1_R, TP2_R, TP3_R = 0.30, 0.80, 1.50
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
RELATIVE_BTC_OVERRIDE_PCT = 5.0
RELATIVE_BTC_VOLUME_BOOST_PCT = 7.0
BOOSTED_MACRO_VOLUME_MIN = 0.90
DIRECT_CLOSE_POSITION_MIN = 0.80

STRATEGIES = {
    '1H': {'name': '1H', 'macro_ms': ONE_HOUR_MS,'lookback': 20, 'macro_volume_min': 1.20,'volume_bars_macro': 20, 'breakout_buffer_atr': 0.04,'macro_close_position_min': 0.65,'confirm_volume_min': 0.60, 'body_fraction': 0.70,'body_atr_min': 0.60, 'wick_directional': 0.12,'wick_opposite': 0.12, 'sl_lookback': 5,'sl_buffer_atr': 0.25, 'lifetime_seconds': 6 * 60 * 60,},
    '4H': {'name': '4H', 'macro_ms': FOUR_HOURS_MS,'lookback': 10, 'macro_volume_min': 1.10,'volume_bars_macro': 20, 'breakout_buffer_atr': 0.03,'macro_close_position_min': 0.70,'confirm_volume_min': 0.60, 'body_fraction': 0.70,'body_atr_min': 0.50, 'wick_directional': 0.15,'wick_opposite': 0.15, 'sl_lookback': 3,'sl_buffer_atr': 0.25, 'lifetime_seconds': 12 * 60 * 60,},
}

def strategy_for(symbol):
    return STRATEGIES['1H'] if symbol in ONE_HOUR_SYMBOLS else STRATEGIES['4H']

KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines'
WS_BASE_URL = 'wss://fstream.binance.com/market/stream?streams='
WS_STALE_SECONDS = 90.0  # FIX: da 25 a 90
REST_FRESH_SECONDS = 90.0 # FIX: da 30 a 90
REST_FALLBACK_TRIGGER_SECONDS = 15.0
REST_FALLBACK_POLL_SECONDS = 30.0
REST_BOUNDARY_POLL_SECONDS = 5.0
REST_FALLBACK_RECENT_BARS = 5
WS_MAX_RECONNECT_SECONDS = 30
REST_GAP_SECONDS = 0.30
REST_RETRY_WAIT = (5, 10, 20)
REST_REFRESH_SECONDS = 300
GAP_REFRESH_SECONDS = 15
LOG_INTERVAL_SECONDS = 15
TELEGRAM_RETRY_SECONDS = 30
HISTORY_MAXLEN = 480
TELEGRAM_MAX_ATTEMPTS = 8
TELEGRAM_DELIVERY_WINDOW_SECONDS = 300
TELEGRAM_RETRY_INITIAL_SECONDS = 3
TELEGRAM_RETRY_MAX_SECONDS = 40
ITALY = ZoneInfo('Europe/Rome')

cache_lock = threading.RLock()
rest_lock = threading.Lock()
alert_lock = threading.Lock()
market_cache = {}
last_ws_receive = {}
last_rest_receive = {}
last_rest_recovery_try = {}
needs_refresh = set()
websocket_connected = False
last_rest_call = 0.0
sent_alerts = set()
pending_alerts = set()
failed_alert_times = {}
telegram_queue = queue.Queue(maxsize=100)
active_setups = {}
seen_macro_bars = {}

def log(*parts):
    print(*parts, flush=True)

def telegram_send_once(text):
    if not BOT_TOKEN or not CHAT_ID:
        log('TELEGRAM NON CONFIGURATO')
        return False, False, 0
    try:
        response = requests.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',data={'chat_id': CHAT_ID, 'text': text}, timeout=(8, 20),)
        if response.status_code == 429:
            retry_after = 5
            try:
                payload = response.json()
                retry_after = max(1, int(payload.get('parameters', {}).get('retry_after', 5)))
            except: pass
            return False, True, retry_after
        if response.status_code >= 500: return False, True, 0
        if response.status_code >= 400: return False, False, 0
        response.raise_for_status()
        data = response.json()
        if not data.get('ok'): return False, True, 0
        return True, False, 0
    except: return False, True, 0

def send_telegram(text):
    ok, _, _ = telegram_send_once(text)
    return ok

def request_klines(symbol, limit=HISTORY_MAXLEN):
    global last_rest_call
    error = None
    for attempt in range(len(REST_RETRY_WAIT) + 1):
        with rest_lock:
            pause = REST_GAP_SECONDS - (time.monotonic() - last_rest_call)
            if pause > 0: time.sleep(pause)
            last_rest_call = time.monotonic()
        try:
            response = requests.get(KLINES_URL,params={'symbol': symbol, 'interval': '15m', 'limit': limit},timeout=15,)
            if response.status_code in (418, 429):
                time.sleep(REST_RETRY_WAIT[min(attempt, len(REST_RETRY_WAIT)-1)])
                continue
            response.raise_for_status()
            raw = response.json()
            candles = [{'t': int(row[0]), 'o': float(row[1]), 'h': float(row[2]),'l': float(row[3]), 'c': float(row[4]), 'v': float(row[5]),} for row in raw]
            if len(candles) < 3: raise ValueError('storico insufficiente')
            return candles
        except Exception as exc: error = exc
        if attempt < len(REST_RETRY_WAIT): time.sleep(REST_RETRY_WAIT[attempt])
    raise RuntimeError(f'{symbol}: impossibile recuperare dati 15m ({error})')

def refresh_symbol(symbol, recent=False):
    with cache_lock: has_cache = bool(market_cache.get(symbol))
    use_recent = recent and has_cache
    candles = request_klines(symbol, limit=(REST_FALLBACK_RECENT_BARS if use_recent else HISTORY_MAXLEN))
    expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
    with cache_lock:
        old = market_cache.get(symbol)
        if old and old[-1]['t'] > candles[-1]['t']: return False
        if old and use_recent:
            old_by_t = {c['t']: c for c in old}
            if not any(c['t'] in old_by_t for c in candles): return False
            by_t = dict(old_by_t)
            for candle in candles:
                previous = by_t.get(candle['t'])
                if previous is None or candle['v'] >= previous['v']: by_t[candle['t']] = candle
            merged = [by_t[t] for t in sorted(by_t)][-HISTORY_MAXLEN:]
            market_cache[symbol] = merged
        else:
            if old and old[-1]['t'] == candles[-1]['t'] and old[-1]['v'] > candles[-1]['v']: candles[-1] = old[-1]
            market_cache[symbol] = candles[-HISTORY_MAXLEN:]
        needs_refresh.discard(symbol)
        if candles[-1]['t'] == expected_t: last_rest_receive[symbol] = time.monotonic()
    return True

def initialize_history():
    log('Caricamento storico 15m per', len(SYMBOLS), 'coppie...')
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(refresh_symbol, s): s for s in SYMBOLS}
        for future in as_completed(futures):
            try: future.result()
            except Exception as exc: log('Errore storico', futures[future], exc)
    with cache_lock: available = len(market_cache)
    if available == 0: raise RuntimeError('Nessun dato Binance disponibile')
    log('Storico inizializzato:', available, '/', len(SYMBOLS))

def receive_kline(payload):
    event = payload.get('data', payload)
    if event.get('e') != 'kline': return
    k = event.get('k') or {}
    symbol = str(event.get('s', '')).upper()
    if symbol not in SYMBOLS or k.get('i') != '15m': return
    try:
        c = {'t': int(k['t']), 'o': float(k['o']), 'h': float(k['h']),'l': float(k['l']), 'c': float(k['c']), 'v': float(k['v']),}
    except: return
    with cache_lock:
        previous = market_cache.get(symbol)
        if not previous: needs_refresh.add(symbol); return
        last = previous[-1]
        if c['t'] < last['t']: return
        if c['t'] == last['t']:
            if c['v'] < last['v']: return
            market_cache[symbol] = previous[:-1] + [c]
        elif c['t'] == last['t'] + TIMEFRAME_SECONDS * 1000:
            market_cache[symbol] = (previous + [c])[-HISTORY_MAXLEN:]
        else:
            needs_refresh.add(symbol)
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
                with cache_lock: websocket_connected = True
                log('WS connesso,', len(SYMBOLS), 'stream 15m')
            def on_message(ws, message):
                try: receive_kline(json.loads(message))
                except: pass
            def on_error(ws, error): log('WebSocket errore:', error)
            def on_close(ws, code, reason):
                global websocket_connected
                with cache_lock: websocket_connected = False
                log('WS disconnesso:', code, reason)
            ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message,on_error=on_error, on_close=on_close,)
            ws.run_forever(ping_interval=60, ping_timeout=20)
        except Exception as exc: log('Errore connessione WS:', exc)
        finally:
            with cache_lock: websocket_connected = False
        time.sleep(delay)
        delay = min(delay * 2, WS_MAX_RECONNECT_SECONDS)

def history_worker():
    last_full = time.monotonic()
    last_gap = 0.0
    while True:
        now = time.monotonic()
        expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
        with cache_lock:
            gaps = list(needs_refresh)
            missing = [s for s in SYMBOLS if s not in market_cache]
            fallback = []
            for symbol in SYMBOLS:
                if symbol not in market_cache: continue
                wrong_bar = market_cache[symbol][-1]['t'] != expected_t
                ws_quiet = (now - last_ws_receive.get(symbol, -1e9) >= REST_FALLBACK_TRIGGER_SECONDS)
                rest_age = now - last_rest_receive.get(symbol, -1e9)
                if not wrong_bar and (not ws_quiet or rest_age < REST_FRESH_SECONDS): continue
                if now - last_rest_recovery_try.get(symbol, -1e9) < REST_FALLBACK_POLL_SECONDS: continue
                last_rest_recovery_try[symbol] = now
                fallback.append(symbol)
        if now - last_gap >= GAP_REFRESH_SECONDS:
            for symbol in set(gaps + missing):
                try: refresh_symbol(symbol)
                except: pass
            last_gap = now
        for symbol in fallback:
            try: refresh_symbol(symbol, recent=True)
            except: pass
        if now - last_full >= REST_REFRESH_SECONDS:
            for symbol in SYMBOLS:
                try: refresh_symbol(symbol)
                except: pass
            last_full = time.monotonic()
        time.sleep(1)

def true_range_atr(closed_candles):
    if len(closed_candles) < ATR_PERIOD + 1: return None
    bars = closed_candles[-(ATR_PERIOD + 1):]
    tr = []
    for i in range(1, len(bars)):
        c, prev = bars[i], bars[i - 1]
        tr.append(max(c['h']-c['l'], abs(c['h']-prev['c']), abs(c['l']-prev['c'])))
    return sum(tr) / ATR_PERIOD

def median_volume(volumes):
    values = [max(0.0, float(v)) for v in volumes]
    return float(median(values)) if values else 0.0

def live_volume_progress(candles, now_ms, threshold):
    if len(candles) < VOLUME_AVG_BARS_15M + 1: return None
    live = candles[-1]
    elapsed_ms = now_ms - live['t']
    if not 0 <= elapsed_ms < FIFTEEN_MIN_MS: return None
    previous = candles[-(VOLUME_AVG_BARS_15M + 1):-1]
    avg = median_volume(c['v'] for c in previous)
    if avg <= 0: return None
    elapsed = elapsed_ms / 1000.0
    actual_ratio = max(0.0, live['v']) / avg
    pace_ratio = (actual_ratio * TIMEFRAME_SECONDS / elapsed if elapsed >= LIVE_VOLUME_PACE_MIN_SECONDS else None)
    return {'candle_t': live['t'],'elapsed_seconds': elapsed,'actual_ratio': actual_ratio,'pace_ratio': pace_ratio,'progress_percent': 100.0 * actual_ratio / threshold,}

def monitor_live_confirmation_volume(setup, candles, now_ms): pass

# ===== FIX PRINCIPALE 1: macro_closed_candles tollerante =====
def macro_closed_candles(candles_15m, macro_ms):
    """FIX: non resetta se manca un pezzo, tollera 1 candela mancante"""
    required_count = macro_ms // FIFTEEN_MIN_MS
    groups = {}
    for candle in candles_15m[:-1]:
        block_t = (candle['t'] // macro_ms) * macro_ms
        groups.setdefault(block_t, []).append(candle)
    result = []
    for block_t, group in sorted(groups.items()):
        if len(group) < required_count - 1: # FIX: tollera 1 mancante
            continue
        group.sort(key=lambda c: c['t'])
        result.append({'t': block_t, 'o': group[0]['o'],'h': max(c['h'] for c in group), 'l': min(c['l'] for c in group),'c': group[-1]['c'], 'v': sum(c['v'] for c in group),})
    return result

def macro_structural_stop(candles_15m, direction, cfg):
    bars = macro_closed_candles(candles_15m, cfg['macro_ms'])
    if len(bars) < max(cfg['sl_lookback'], ATR_PERIOD + 1): return None
    atr = true_range_atr(bars)
    if atr is None or atr <= 0: return None
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
    if len(btc_candles) < ATR_PERIOD + 2: return None
    atr15 = true_range_atr(btc_candles[:-1])
    if atr15 is None or atr15 <= 0: return None
    live = btc_candles[-1]
    displacement = live['c'] - live['o']
    if abs(displacement) / atr15 < BTC_NEUTRAL_BODY_ATR_MAX: return 'NEUTRAL'
    return 'LONG' if displacement > 0 else 'SHORT'

def beyond_breakout(price, direction, level, buffer):
    return price >= level + buffer if direction == 'LONG' else price <= level - buffer

def macro_breakout(symbol, candles, now_ms, cfg, btc_candles=None):
    macro_bars = macro_closed_candles(candles, cfg['macro_ms'])
    required = max(ATR_PERIOD + 2, cfg['lookback'] + 1, cfg['volume_bars_macro'] + 1)
    if len(macro_bars) < required: return None
    macro = macro_bars[-1]
    close_ms = macro['t'] + cfg['macro_ms']
    if not (0 <= now_ms - close_ms <= MACRO_CLOSE_GRACE_SECONDS * 1000): return None
    prev = macro_bars[:-1]
    if any(prev[i]['t'] + cfg['macro_ms'] != prev[i + 1]['t'] for i in range(len(prev)-1)): return None
    if prev[-1]['t'] + cfg['macro_ms'] != macro['t']: return None
    direction = ('LONG' if macro['c'] > macro['o'] else 'SHORT' if macro['c'] < macro['o'] else None)
    if direction is None: return None
    atr_macro = true_range_atr(prev)
    if not atr_macro or atr_macro <= 0: return None
    level_bars = prev[-cfg['lookback']:]
    level = max(c['h'] for c in level_bars) if direction == 'LONG' else min(c['l'] for c in level_bars)
    buffer = cfg['breakout_buffer_atr'] * atr_macro
    if not beyond_breakout(macro['c'], direction, level, buffer): return None
    span = macro['h'] - macro['l']
    if span <= 0: return None
    close_pos = ((macro['c']-macro['l']) / span if direction == 'LONG' else (macro['h']-macro['c']) / span)
    if close_pos < cfg['macro_close_position_min']: return None
    vols = prev[-cfg['volume_bars_macro']:]
    median_vol = median_volume(c['v'] for c in vols)
    if median_vol <= 0: return None
    vol_ratio = max(0.0, macro['v']) / median_vol
    volume_threshold = cfg['macro_volume_min']
    volume_boosted = False
    relative_at_breakout = None
    if symbol != 'BTCUSDT' and btc_candles is not None:
        relative_at_breakout = relative_performance_4h(candles, btc_candles, close_ms - FIFTEEN_MIN_MS)
        if relative_at_breakout is not None:
            rel = relative_at_breakout['relative_pct']
            if ((direction == 'LONG' and rel >= RELATIVE_BTC_VOLUME_BOOST_PCT) or (direction == 'SHORT' and rel <= -RELATIVE_BTC_VOLUME_BOOST_PCT)):
                volume_threshold = BOOSTED_MACRO_VOLUME_MIN
                volume_boosted = True
    if vol_ratio < volume_threshold: return None
    if not macro_structural_stop(candles, direction, cfg): return None
    return {'symbol': symbol, 'direction': direction,'strategy': cfg['name'], 'macro_t': macro['t'],'macro_end_t': close_ms, 'level': level, 'macro_buffer': buffer,'atr_macro_at_breakout': atr_macro, 'macro_close_pos': close_pos,'macro_volume_ratio': vol_ratio,'volume_threshold_used': volume_threshold,'volume_boosted': volume_boosted,'relative_at_breakout': relative_at_breakout,'allow_direct': close_pos >= DIRECT_CLOSE_POSITION_MIN,'last_checked_15m_t': None,'touched': False, 'touch_t': None, 'touched_price': None,}

def btc15_closed_direction(btc_candles, confirmation_t):
    if len(btc_candles) < ATR_PERIOD + 3 or btc_candles[-2]['t'] != confirmation_t: return None
    atr = true_range_atr(btc_candles[:-2])
    if not atr or atr <= 0: return None
    candle = btc_candles[-2]
    movement = candle['c'] - candle['o']
    if abs(movement) / atr < BTC_NEUTRAL_BODY_ATR_MAX: return 'NEUTRAL'
    return 'LONG' if movement > 0 else 'SHORT'

def relative_performance_4h(alt_candles, btc_candles, confirmation_t):
    bars_4h = FOUR_HOURS_MS // FIFTEEN_MIN_MS
    def window_ending_at(candles):
        closed = candles[:-1]
        idx = next((i for i in range(len(closed)-1, -1, -1) if closed[i]['t'] == confirmation_t), None)
        if idx is None or idx < bars_4h: return None
        win = closed[idx-bars_4h:idx+1]
        if any(win[i+1]['t']-win[i]['t'] != FIFTEEN_MIN_MS for i in range(bars_4h)): return None
        return win
    alt_window = window_ending_at(alt_candles)
    btc_window = window_ending_at(btc_candles)
    if alt_window is None or btc_window is None: return None
    if any(a['t'] != b['t'] for a, b in zip(alt_window, btc_window)): return None
    a0, a1 = alt_window[0]['c'], alt_window[-1]['c']
    b0, b1 = btc_window[0]['c'], btc_window[-1]['c']
    if min(a0, a1, b0, b1) <= 0: return None
    alt_perf = (a1 / a0 - 1.0) * 100.0
    btc_perf = (b1 / b0 - 1.0) * 100.0
    return {'alt_pct': alt_perf, 'btc_pct': btc_perf,'relative_pct': alt_perf - btc_perf}

def btc_direction_with_relative_exception(symbol, direction, btc_direction, alt_candles, btc_candles, confirmation_t):
    if not ((direction == 'LONG' and btc_direction == 'SHORT') or (direction == 'SHORT' and btc_direction == 'LONG')): return btc_direction, None, False
    perf = relative_performance_4h(alt_candles, btc_candles, confirmation_t)
    if perf is None: return btc_direction, None, False
    relative = perf['relative_pct']
    if ((direction == 'LONG' and relative >= RELATIVE_BTC_OVERRIDE_PCT) or (direction == 'SHORT' and relative <= -RELATIVE_BTC_OVERRIDE_PCT)):
        return 'NEUTRAL', perf, True
    return btc_direction, perf, False

def confirmation_15m_volume_ratio(candles):
    if len(candles) < VOLUME_AVG_BARS_15M + 2: return 0.0
    confirmation = candles[-2]
    previous = candles[-(VOLUME_AVG_BARS_15M + 2):-2]
    avg = median_volume(c['v'] for c in previous)
    if avg <= 0: return 0.0
    return max(0.0, confirmation['v']) / avg

def confirmation_15m_valid(candle, direction, level, atr15, cfg):
    if atr15 is None or atr15 <= 0: return False
    span = candle['h'] - candle['l']
    if span <= 0: return False
    opening, closing = candle['o'], candle['c']
    if direction == 'LONG' and closing <= opening: return False
    if direction == 'SHORT' and closing >= opening: return False
    if not beyond_breakout(closing, direction, level, MICRO_CLOSE_BUFFER_ATR15 * atr15): return False
    body = abs(closing - opening)
    if body / span < cfg['body_fraction']: return False
    if body / atr15 < cfg['body_atr_min']: return False
    upper_wick = max(0.0, candle['h'] - max(opening, closing)) / span
    lower_wick = max(0.0, min(opening, closing) - candle['l']) / span
    leading = upper_wick if direction == 'LONG' else lower_wick
    trailing = lower_wick if direction == 'LONG' else upper_wick
    return leading <= cfg['wick_directional'] and trailing <= cfg['wick_opposite']

def build_confirmed_signal(setup, candles, btc_direction, candle_15m, now_ms):
    if btc_direction not in (setup['direction'], 'NEUTRAL'): return None
    live = candles[-1]
    price = live['c']
    if price <= 0 or not beyond_breakout(price, setup['direction'], setup['level'], MICRO_CLOSE_BUFFER_ATR15 * setup['atr15_retest']): return None
    cfg = strategy_for(setup['symbol'])
    info = macro_structural_stop(candles, setup['direction'], cfg)
    if not info: return None
    risk = price - info['sl'] if setup['direction'] == 'LONG' else info['sl'] - price
    if risk <= 0: return None
    return {'symbol': setup['symbol'], 'direction': setup['direction'],'strategy': setup['strategy'], 'entry': price, 'sl': info['sl'],'atr_macro': info['atr_macro'], 'stop_distance_pct': risk / price,'macro_t': setup['macro_t'], 'macro_end_t': setup['macro_end_t'],'confirm_15m_t': candle_15m['t'], 'retest_t': setup['touch_t'],'entry_mode': setup.get('entry_mode', 'RETEST'),'breakout_level': setup['level'], 'breakout_buffer': setup['macro_buffer'],'confirm_volume_ratio': setup['confirm_volume_ratio'],'macro_close_pos': setup['macro_close_pos'],'macro_volume_ratio': setup['macro_volume_ratio'],'volume_threshold_used': setup.get('volume_threshold_used', strategy_for(setup['symbol'])['macro_volume_min']),'volume_boosted': setup.get('volume_boosted', False),'btc_direction': btc_direction,'btc_override': setup.get('btc_override', False),'btc_relative_4h': setup.get('btc_relative_4h'),}

def confirm_after_closed_15m(setup, candles, btc_candles, closed, now_ms, atr15, entry_mode):
    sym = setup['symbol']; d = setup['direction']; cfg = strategy_for(sym)
    if not confirmation_15m_valid(closed, d, setup['level'], atr15, cfg): return None
    ratio15 = confirmation_15m_volume_ratio(candles)
    if ratio15 < cfg['confirm_volume_min']: return None
    btc_direction = ('NEUTRAL' if sym == 'BTCUSDT' and cfg['name'] == '1H' else btc15_closed_direction(btc_candles, closed['t']))
    original_btc_direction = btc_direction
    btc_perf = None; btc_override = False
    if sym != 'BTCUSDT':
        btc_direction, btc_perf, btc_override = btc_direction_with_relative_exception(sym, d, btc_direction, candles, btc_candles, closed['t'])
    if btc_direction not in (d, 'NEUTRAL'): return None
    setup['btc_override'] = btc_override
    setup['btc_relative_4h'] = btc_perf
    setup['atr15_retest'] = atr15
    setup['confirm_volume_ratio'] = ratio15
    setup['entry_mode'] = entry_mode
    return build_confirmed_signal(setup, candles, btc_direction, closed, now_ms)

def evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms):
    if len(candles) < ATR_PERIOD + 3: return 'WAIT', None
    live = candles[-1]; closed = candles[-2]
    closed_end = closed['t'] + FIFTEEN_MIN_MS
    sym = setup['symbol']; cfg = strategy_for(sym)
    if live['t'] != closed_end: return 'WAIT', None
    if closed['t'] < setup['macro_end_t']: return 'WAIT', None
    if now_ms - setup['macro_end_t'] > cfg['lifetime_seconds'] * 1000: return 'EXPIRED', None
    if setup['last_checked_15m_t'] == closed['t']: return 'WAIT', None
    if (setup['last_checked_15m_t'] is not None and closed['t'] != setup['last_checked_15m_t'] + FIFTEEN_MIN_MS): return 'EXPIRED', None
    setup['last_checked_15m_t'] = closed['t']
    atr15 = true_range_atr(candles[:-2])
    if not atr15 or atr15 <= 0: return 'WAIT', None
    if (setup.get('allow_direct') and closed['t'] == setup['macro_end_t']):
        signal = confirm_after_closed_15m(setup, candles, btc_candles, closed, now_ms, atr15, 'DIRECT')
        if signal: return 'SIGNAL', signal
    touched = closed['l'] <= setup['level'] <= closed['h']
    if touched and not setup['touched']:
        setup['touched'] = True; setup['touch_t'] = closed['t']
        setup['touched_price'] = closed['l'] if setup['direction'] == 'LONG' else closed['h']
    if not setup['touched']: return 'WAIT', None
    if closed['t'] <= setup['touch_t']: return 'WAIT', None
    signal = confirm_after_closed_15m(setup, candles, btc_candles, closed, now_ms, atr15, 'RETEST')
    return ('SIGNAL', signal) if signal else ('WAIT', None)

def price_format(price):
    if price >= 1000: return f'{price:.2f}'
    if price >= 1: return f'{price:.4f}'
    if price >= 0.01: return f'{price:.6f}'
    return f'{price:.9f}'

def message_for(s):
    cfg = strategy_for(s['symbol'])
    d = 1 if s['direction'] == 'LONG' else -1
    risk = abs(s['entry'] - s['sl'])
    tp1 = s['entry'] + d * TP1_R * risk; tp2 = s['entry'] + d * TP2_R * risk; tp3 = s['entry'] + d * TP3_R * risk
    now_italy = datetime.now(ITALY).strftime('%d/%m/%Y %H:%M:%S')
    macro_italy = datetime.fromtimestamp(s['macro_t']/1000, ITALY).strftime('%d/%m %H:%M')
    confirm_italy = datetime.fromtimestamp(s['confirm_15m_t']/1000, ITALY).strftime('%d/%m %H:%M')
    title = ('MOMENTUM ONE 1H' if cfg['name'] == '1H' else 'MOMENTUM ONE 4H')
    signal_id = f"{s['symbol']}-{s['macro_t']}-{s['confirm_15m_t']}"
    return (f"{title} - {s['direction']} [SIMULAZIONE]\n{s['symbol']}\nID: {signal_id}\nOrario: {now_italy}\nBreakout {cfg['name']}: {macro_italy}\nENTRY: {price_format(s['entry'])}\nSL: {price_format(s['sl'])}\nTP1: {price_format(tp1)} / TP2: {price_format(tp2)} / TP3: {price_format(tp3)}")

def enqueue_alert(signal):
    key = (signal['symbol'], signal['macro_t'])
    now = time.monotonic()
    with alert_lock:
        if key in sent_alerts or key in pending_alerts: return False
        if now - failed_alert_times.get(key, -1e9) < TELEGRAM_RETRY_SECONDS: return False
        pending_alerts.add(key)
    try:
        telegram_queue.put_nowait((key, message_for(signal)))
        return True
    except:
        with alert_lock: pending_alerts.discard(key)
        return False

def telegram_worker():
    while True:
        key, message = telegram_queue.get()
        delivered = False
        try:
            started = time.monotonic()
            for attempt in range(1, TELEGRAM_MAX_ATTEMPTS + 1):
                try: ok, retryable, retry_after = telegram_send_once(message)
                except: ok, retryable, retry_after = False, True, 0
                if ok:
                    delivered = True
                    with alert_lock: sent_alerts.add(key); failed_alert_times.pop(key, None)
                    break
                if not retryable or attempt >= TELEGRAM_MAX_ATTEMPTS: break
                delay = max(float(retry_after), min(TELEGRAM_RETRY_MAX_SECONDS, TELEGRAM_RETRY_INITIAL_SECONDS * 2 ** (attempt - 1),))
                if time.monotonic() - started + delay > TELEGRAM_DELIVERY_WINDOW_SECONDS: break
                time.sleep(delay)
            if not delivered:
                with alert_lock: failed_alert_times[key] = time.monotonic()
        finally:
            with alert_lock: pending_alerts.discard(key)
            telegram_queue.task_done()

# ===== FIX PRINCIPALE 2: feed attivi 40/40 stabile =====
def snapshot_with_sources():
    """FIX FEED: tollera 1 candela di ritardo, conta 40/40 anche in REST"""
    now = time.monotonic()
    expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
    fresh_ws = 0
    fresh_rest = 0
    snapshot = {}
    with cache_lock:
        for symbol in SYMBOLS:
            candles = market_cache.get(symbol)
            if not candles: continue
            # FIX: tollera fino a 1 candela di ritardo (15 min)
            if abs(candles[-1]['t'] - expected_t) > FIFTEEN_MIN_MS: continue
            # FIX: non scartare su needs_refresh, usa cache
            snapshot[symbol] = list(candles)
            if now - last_ws_receive.get(symbol, 0) <= 90.0: fresh_ws += 1
            else: fresh_rest += 1
    return snapshot, fresh_ws, fresh_rest

def current_snapshot():
    return snapshot_with_sources()[0]

def scan_market():
    snapshot = current_snapshot()
    if 'BTCUSDT' not in snapshot: return 0
    btc_candles = snapshot['BTCUSDT']
    now_ms = int(time.time() * 1000)
    if not (btc_candles[-1]['t'] <= now_ms < btc_candles[-1]['t'] + FIFTEEN_MIN_MS): return 0
    queued = 0
    for symbol, candles in snapshot.items():
        if candles[-1]['t'] != btc_candles[-1]['t']: continue
        cfg = strategy_for(symbol)
        bars = macro_closed_candles(candles, cfg['macro_ms'])
        if not bars: continue
        macro_t = bars[-1]['t']
        if seen_macro_bars.get(symbol) != macro_t:
            seen_macro_bars[symbol] = macro_t
            candidate = macro_breakout(symbol, candles, now_ms, cfg, btc_candles)
            if candidate: active_setups[symbol] = candidate
        setup = active_setups.get(symbol)
        if not setup: continue
        if now_ms - setup['macro_end_t'] > cfg['lifetime_seconds'] * 1000:
            del active_setups[symbol]; continue
        status, signal = evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms)
        if status == 'EXPIRED': del active_setups[symbol]
        elif status == 'SIGNAL' and signal:
            if enqueue_alert(signal):
                queued += 1
                del active_setups[symbol]
    oldest_allowed = now_ms - 4 * FOUR_HOURS_MS
    with alert_lock:
        sent_alerts.intersection_update({k for k in sent_alerts if k[1] >= oldest_allowed})
    return queued

def main():
    log('MOMENTUM ONE FIX FEED 40/40 | 3x1H + 37x4H | scan ~1s')
    if not BOT_TOKEN or not CHAT_ID: log('ATTENZIONE: mancano variabili Telegram')
    while True:
        try: initialize_history(); break
        except Exception as exc:
            log('Storico Binance non disponibile:', exc, 'riprovo 20s'); time.sleep(20)
    threading.Thread(target=telegram_worker, daemon=True).start()
    threading.Thread(target=history_worker, daemon=True).start()
    threading.Thread(target=websocket_worker, daemon=True).start()
    send_telegram('MOMENTUM ONE FIX FEED AVVIATO - 40/40 stabile, tolleranza 1 candela')
    next_scan = time.monotonic(); last_log = next_scan
    while True:
        next_scan += SCAN_SECONDS
        try: scan_market()
        except Exception as exc: log('ERRORE SCANSIONE:', repr(exc))
        now = time.monotonic()
        if now - last_log >= LOG_INTERVAL_SECONDS:
            snap, ws_count, rest_count = snapshot_with_sources()
            b = btc15_direction(snap['BTCUSDT']) if 'BTCUSDT' in snap else 'DATI NON FRESCHI'
            with alert_lock: in_delivery = len(pending_alerts)
            s1 = sum(1 for s in active_setups if s in ONE_HOUR_SYMBOLS); s4 = sum(1 for s in active_setups if s in FOUR_HOUR_SYMBOLS)
            log('Scansione ~1s | feed attivi:', len(snap), '/40',f'| WS: {ws_count} | REST backup: {rest_count}','| BTC15:', b, '| setup 1H:', s1, '| setup 4H:', s4,'| Telegram in coda:', in_delivery)
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS: next_scan = time.monotonic()

if __name__ == '__main__':
    main()
