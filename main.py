# V10.1.1 SNELLA: Stocastico in rafforzamento obbligatorio solo su TREND
import os
import time
import json
import queue
import threading
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import websocket
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
SYMBOLS = ['SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'DOGEUSDT', 'AVAXUSDT', 'SUIUSDT', 'LINKUSDT', 'ADAUSDT', 'NEARUSDT', 'UNIUSDT', 'LTCUSDT', 'DOTUSDT', 'APTUSDT', 'ARBUSDT', 'OPUSDT', 'INJUSDT', 'RENDERUSDT', '1000PEPEUSDT', 'FILUSDT', 'ETCUSDT', 'AAVEUSDT', 'TIAUSDT', 'SEIUSDT']
KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines'
SCAN_SECONDS = 1
SCAN_WORKERS = 6
WS_URL = 'wss://fstream.binance.com/market/stream?streams='
WS_STALE_SECONDS = 5.0
WS_RECONNECT_MAX_SECONDS = 30
SCAN_LOG_EVERY_SECONDS = 10
CACHE_1H_SECONDS = 60
CACHE_4H_SECONDS = 300
MIN_REST_GAP_SECONDS = 0.3
RETRY_FALLBACK_SECONDS = (5, 10, 20, 30)
MAX_RETRIES = 4
DIAGNOSTIC_EVERY_SECONDS = 60
LIVE_VOLUME_MIN_ELAPSED_SECONDS = 15
LIVE_VOLUME_PACE_CAP = 20.0
CLOSED_VOLUME_PREFERRED = 1.1
MIN_CLOSED_VOLUME_RATIO = 0.8
STRUCTURE_BARS = 6
RESISTANCE_LOOKBACK_BARS = 48
MIN_ROOM_R = 1.2
MIN_QUALITY = 9
STOCH_K_PERIOD = 14
STOCH_K_SMOOTH = 3
STOCH_D_PERIOD = 3
STOCH_TREND_MIN_STRENGTHENING = 0.5  # incremento minimo punti K-D vs seconda candela chiusa
BTC15_DIRECTIONAL_BODY_ATR_MIN = 0.2
BTC30_DIRECTIONAL_BODY_ATR_MIN = 0.15
SL_STRUCTURE_LOOKBACK_BARS = 12
SL_1H_LOOKBACK_BARS = 5
SL_BUFFER_STRONG_TREND_ATR15 = 0.5
SL_BUFFER_NORMAL_TREND_ATR15 = 0.45
SL_BUFFER_NEUTRAL_4H_ATR15 = 0.3
SL_MIN_DISTANCE_ATR15 = 1.2
SL_MAX_DISTANCE_ATR15 = 4.5
VOLUME_SAMPLE_MIN_GAP_SECONDS = 4.0
VOLUME_SAMPLE_MAX_GAP_SECONDS = 60.0
VOLUME_SAMPLE_LOOKBACK_SECONDS = 120.0
VOLUME_ACCEL_MIN_GROWTH = 0.12
VOLUME_RECENT_RATE_FLOOR_PART = 0.75
FAST_SUSTAINED_VOLUME_MULTIPLIER = 2.5
FAST_SUSTAINED_MAX_RATE_DROP = 0.15
FAST_BODY_ATR_MIN = 0.7
FAST_BODY_FRACTION_MIN = 0.8
FAST_WICK_MAX = 0.35
FAST_MAX_EXTENSION_ATR = 1.4
FAST_BTC15_BODY_ATR_MIN = 0.3
TREND_FIRST_BODY_ATR_MIN = 0.2
TREND_SECOND_BODY_ATR_MIN = 0.25
TREND_BODY_ATR_MIN = 0.35
TREND_BODY_FRACTION_MIN = 0.8
TREND_UPPER_WICK_MAX_RANGE = 0.15
TREND_LOWER_WICK_MAX_RANGE = 0.15
TREND_PROTECTED_EXTREME_TOL_ATR = 0.08
TREND_LIVE_BREAKOUT_ATR = 0.05
TREND_MAX_EXTENSION_ATR = 2.2
TREND_BTC15_BODY_ATR_MIN = 0.2
TP_MIN_NET_PCT = 0.0015
LEVERAGE_SAFETY = 0.35
LEVERAGE_STEPS = (1, 2, 3, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100)
request_lock = threading.Lock()
cache_lock = threading.Lock()
last_rest_request = 0.0
market_cache = {}
ws_last_seen = {}
ws_gap_keys = set()
websocket_status = {'connected': False, 'since': 0.0}
last_scan_log = 0.0
signals_queue = queue.Queue(maxsize=200)
signals_lock = threading.Lock()
signals_in_flight = set()
signal_state = {}
last_diag = 0.0
diagnostic_totals = Counter()
live_volume_samples = {}
volume_samples_lock = threading.Lock()
def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print('Telegram non configurato')
        return False
    try:
        r = requests.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage', data={'chat_id': CHAT_ID, 'text': message}, timeout=20)
        r.raise_for_status()
        return bool(r.json().get('ok'))
    except Exception as exc:
        print('Errore Telegram:', exc)
        return False
def get_json(url, params):
    global last_rest_request
    error = None
    for attempt in range(MAX_RETRIES):
        with request_lock:
            wait = MIN_REST_GAP_SECONDS - (time.monotonic() - last_rest_request)
            if wait > 0:
                time.sleep(wait)
            last_rest_request = time.monotonic()
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code in (418, 429):
                try:
                    delay = max(5.0, float(r.headers.get('Retry-After')))
                except (TypeError, ValueError):
                    delay = RETRY_FALLBACK_SECONDS[min(attempt, 3)]
                print('Rate limit Binance', r.status_code, 'attesa', delay)
                time.sleep(delay)
                error = RuntimeError(f'HTTP {r.status_code}')
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            error = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_FALLBACK_SECONDS[min(attempt, 3)])
    raise error or RuntimeError('Binance non disponibile')
def parse_candles(raw):
    return [{'o': float(x[1]), 'h': float(x[2]), 'l': float(x[3]), 'c': float(x[4]), 'v': float(x[5]), 't': int(x[0])} for x in raw]
def ws_stream_names():
    return [f'{symbol.lower()}@kline_15m' for symbol in SYMBOLS] + ['btcusdt@kline_30m']
def ws_update(payload):
    evt = payload.get('data', payload)
    if evt.get('e') != 'kline':
        return
    k = evt.get('k') or {}
    symbol = str(evt.get('s', k.get('s', ''))).upper()
    interval = k.get('i')
    if symbol not in SYMBOLS or (interval != '15m' and (not (symbol == 'BTCUSDT' and interval == '30m'))):
        return
    try:
        candle = {'o': float(k['o']), 'h': float(k['h']), 'l': float(k['l']), 'c': float(k['c']), 'v': float(k['v']), 't': int(k['t'])}
    except (KeyError, TypeError, ValueError):
        return
    if not candle['l'] <= min(candle['o'], candle['c']) <= max(candle['o'], candle['c']) <= candle['h']:
        return
    key = (symbol, interval)
    interval_ms = 900000 if interval == '15m' else 1800000
    now = time.monotonic()
    with cache_lock:
        entry = market_cache.get(key)
        if not entry or not entry['data']:
            ws_gap_keys.add(key)
            return
        old = entry['data']
        last = old[-1]
        if candle['t'] < last['t']:
            return
        if candle['t'] == last['t']:
            if candle['v'] + 1e-10 < last['v']:
                return
            updated = old[:-1] + [candle]
        elif candle['t'] == last['t'] + interval_ms:
            updated = (old + [candle])[-120:]
        else:
            ws_gap_keys.add(key)
            return
        market_cache[key] = {'time': time.time(), 'data': updated}
        ws_last_seen[key] = now
        ws_gap_keys.discard(key)
def ws_connection_worker():
    streams = '/'.join(ws_stream_names())
    reconnect_delay = 1
    while True:
        try:
            def on_open(ws):
                nonlocal reconnect_delay
                reconnect_delay = 1
                with cache_lock:
                    websocket_status['connected'] = True
                    websocket_status['since'] = time.monotonic()
                print('WEBSOCKET CONNESSO: 25x15m + BTC30m (26 stream)', flush=True)
            def on_message(ws, message):
                try:
                    ws_update(json.loads(message))
                except (ValueError, TypeError) as exc:
                    print('WS messaggio non valido:', exc, flush=True)
            def on_error(ws, error):
                print('ERRORE WebSocket:', error, flush=True)
            def on_close(ws, code, reason):
                with cache_lock:
                    websocket_status['connected'] = False
                print('WEBSOCKET DISCONNESSO:', code, reason, flush=True)
            client = websocket.WebSocketApp(WS_URL + streams, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
            client.run_forever(ping_interval=120, ping_timeout=20)
        except Exception as exc:
            print('ERRORE CONNESSIONE WebSocket:', exc, flush=True)
        finally:
            with cache_lock:
                websocket_status['connected'] = False
        time.sleep(reconnect_delay)
        reconnect_delay = min(WS_RECONNECT_MAX_SECONDS, reconnect_delay * 2)
def hydrate_cache(key, only_if_needed=False):
    symbol, interval = key
    rows = parse_candles(get_json(KLINES_URL, {'symbol': symbol, 'interval': interval, 'limit': 120}))
    if len(rows) < 55:
        raise ValueError(f'{symbol}/{interval}: storico troppo breve')
    with cache_lock:
        current = market_cache.get(key)
        if only_if_needed and current:
            latest = current['data'][-1]
            candidate = rows[-1]
            if latest['t'] > candidate['t'] or (latest['t'] == candidate['t'] and latest['v'] > candidate['v']):
                if key not in ws_gap_keys:
                    return
        market_cache[key] = {'time': time.time(), 'data': rows}
        ws_gap_keys.discard(key)
    return True
def bootstrap_history():
    keys = [(symbol, interval) for symbol in SYMBOLS for interval in ('15m', '1h', '4h')]
    keys.append(('BTCUSDT', '30m'))
    failures = []
    print('Caricamento storico Binance Futures:', len(keys), 'serie...', flush=True)
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = {pool.submit(hydrate_cache, key): key for key in keys}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                failures.append(futures[future])
                print('ERRORE inizializzazione', futures[future], exc, flush=True)
    if failures:
        raise RuntimeError(f'Storico non disponibile per {len(failures)} timeframe; ritento all avvio')
    print('STORICO PRONTO: 25 coppie e BTC30', flush=True)
def rest_maintenance_worker():
    last_1h = time.monotonic()
    last_4h = time.monotonic()
    last_gap_retry = 0.0
    while True:
        now = time.monotonic()
        keys = []
        if now - last_1h >= CACHE_1H_SECONDS:
            keys.extend(((symbol, '1h') for symbol in SYMBOLS))
            last_1h = now
        if now - last_4h >= CACHE_4H_SECONDS:
            keys.extend(((symbol, '4h') for symbol in SYMBOLS))
            last_4h = now
        if now - last_gap_retry >= 15:
            with cache_lock:
                keys.extend(ws_gap_keys)
            last_gap_retry = now
        for key in dict.fromkeys(keys):
            try:
                hydrate_cache(key, only_if_needed=True)
            except Exception as exc:
                print('REST refresh FALLITO', key, exc, flush=True)
        time.sleep(1)
def market_snapshot():
    now = time.monotonic()
    market = {}
    with cache_lock:
        online = websocket_status['connected']
        btc30_item = market_cache.get(('BTCUSDT', '30m'))
        btc30_fresh = now - ws_last_seen.get(('BTCUSDT', '30m'), -1000000000.0) <= WS_STALE_SECONDS
        btc30 = btc30_item['data'] if btc30_item and btc30_fresh and (('BTCUSDT', '30m') not in ws_gap_keys) else None
        for symbol in SYMBOLS:
            keys = ((symbol, '15m'), (symbol, '1h'), (symbol, '4h'))
            items = [market_cache.get(key) for key in keys]
            if not online or any((x is None for x in items)):
                continue
            if keys[0] in ws_gap_keys or now - ws_last_seen.get(keys[0], -1000000000.0) > WS_STALE_SECONDS:
                continue
            if time.time() - items[1]['time'] > 180 or time.time() - items[2]['time'] > 900:
                continue
            market[symbol] = {'15m': items[0]['data'], '1h': items[1]['data'], '4h': items[2]['data']}
    return (market, btc30)
def telegram_sender_worker():
    while True:
        key, message = signals_queue.get()
        try:
            if send_telegram(message):
                with signals_lock:
                    signal_state[key] = time.time()
            else:
                print('Invio Telegram fallito:', key, flush=True)
        finally:
            with signals_lock:
                signals_in_flight.discard(key)
            signals_queue.task_done()
def queue_signal(symbol, signal):
    key = (symbol, signal['direction'], signal['candle_time'])
    with signals_lock:
        if key in signals_in_flight or key in signal_state:
            return False
        signals_in_flight.add(key)
    try:
        signals_queue.put_nowait((key, build_message(symbol, signal)))
        return True
    except queue.Full:
        with signals_lock:
            signals_in_flight.discard(key)
        print('CODA TELEGRAM PIENA: segnale non accodato', key, flush=True)
        return False
def ema(values, period):
    if len(values) < period:
        return None
    result = sum(values[:period]) / period
    alpha = 2 / (period + 1)
    for value in values[period:]:
        result = alpha * value + (1 - alpha) * result
    return result
def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    ranges = [max(candles[i]['h'] - candles[i]['l'], abs(candles[i]['h'] - candles[i - 1]['c']), abs(candles[i]['l'] - candles[i - 1]['c'])) for i in range(1, len(candles))]
    return sum(ranges[-period:]) / period
def live_volume_ratio(candles):
    if len(candles) < 22:
        return (0.0, 0.0)
    avg = sum((x['v'] for x in candles[-21:-1])) / 20
    live = candles[-1]
    elapsed = max(0.0, min((time.time() * 1000 - live['t']) / 1000, 900.0))
    if avg <= 0 or elapsed < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return (0.0, elapsed)
    return (min(live['v'] / (avg * max(elapsed / 900.0, 0.01)), LIVE_VOLUME_PACE_CAP), elapsed)
def progressive_volume_metrics(symbol, candles, now):
    result = {'ready': False, 'accel_ok': False, 'intensity_ok': False, 'sustained_ok': False, 'pace': 0.0, 'relative_pace': 0.0, 'dynamic_floor': 0.0, 'growth': 0.0, 'recent_rate': 0.0, 'prior_rate': 0.0, 'projected': 0.0}
    if len(candles) < 22:
        return result
    live = candles[-1]
    elapsed = max(0.0, min((time.time() * 1000 - live['t']) / 1000.0, 900.0))
    prior = [max(0.0, x['v']) for x in candles[-21:-1]]
    avg = sum(prior) / len(prior)
    sorted_prior = sorted(prior)
    median = (sorted_prior[9] + sorted_prior[10]) / 2.0
    reference = (avg + median) / 2.0
    with volume_samples_lock:
        state = live_volume_samples.get(symbol)
        if state is None or state['candle_time'] != live['t'] or (state['samples'] and live['v'] < state['samples'][-1][1]):
            state = {'candle_time': live['t'], 'samples': deque(maxlen=16)}
            live_volume_samples[symbol] = state
        samples = state['samples']
        while samples and now - samples[0][0] > VOLUME_SAMPLE_LOOKBACK_SECONDS:
            samples.popleft()
        if not samples or now - samples[-1][0] >= VOLUME_SAMPLE_MIN_GAP_SECONDS:
            samples.append((now, max(0.0, live['v'])))
        points = tuple(samples)
    if avg <= 0 or reference <= 0 or elapsed < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return result
    projected = max(0.0, live['v']) * 900.0 / elapsed
    pace = min(projected / avg, LIVE_VOLUME_PACE_CAP)
    dyn_floor = reference / avg
    relative = projected / reference
    result.update(pace=pace, dynamic_floor=dyn_floor, relative_pace=relative, projected=projected, intensity_ok=projected >= reference)
    if len(points) < 3:
        return result
    t0, v0 = points[-3]
    t1, v1 = points[-2]
    t2, v2 = points[-1]
    dt_prev, dt_new = (t1 - t0, t2 - t1)
    if not VOLUME_SAMPLE_MIN_GAP_SECONDS <= dt_prev <= VOLUME_SAMPLE_MAX_GAP_SECONDS or not VOLUME_SAMPLE_MIN_GAP_SECONDS <= dt_new <= VOLUME_SAMPLE_MAX_GAP_SECONDS:
        return result
    prev_rate = max(0.0, v1 - v0) / dt_prev
    new_rate = max(0.0, v2 - v1) / dt_new
    historical_rate = reference / 900.0
    growth = min(3.0, new_rate / max(prev_rate, historical_rate * 0.1) - 1.0) if new_rate > 0 else 0.0
    accel_ok = new_rate > 0 and new_rate >= prev_rate * (1.0 + VOLUME_ACCEL_MIN_GROWTH) and (new_rate >= historical_rate * VOLUME_RECENT_RATE_FLOOR_PART)
    sustained_min_rate = historical_rate * FAST_SUSTAINED_VOLUME_MULTIPLIER
    sustained_ok = relative >= FAST_SUSTAINED_VOLUME_MULTIPLIER and prev_rate >= sustained_min_rate and (new_rate >= sustained_min_rate) and (new_rate >= prev_rate * (1.0 - FAST_SUSTAINED_MAX_RATE_DROP))
    result.update(ready=True, accel_ok=accel_ok, sustained_ok=sustained_ok, growth=growth, recent_rate=new_rate, prior_rate=prev_rate)
    return result
def closed_volume_ratio(candles):
    if len(candles) < 22:
        return 0.0
    avg = sum((x['v'] for x in candles[-22:-2])) / 20
    return candles[-2]['v'] / avg if avg > 0 else 0.0
def candle_metrics(c, direction, a15):
    body = abs(c['c'] - c['o'])
    span = c['h'] - c['l']
    if body <= 0 or span <= 0 or a15 <= 0:
        return None
    if direction == 'LONG':
        if c['c'] <= c['o']:
            return None
        pos = (c['c'] - c['l']) / span
        wick = max(0.0, c['h'] - c['c']) / body
    else:
        if c['c'] >= c['o']:
            return None
        pos = (c['h'] - c['c']) / span
        wick = max(0.0, c['c'] - c['l']) / body
    upper_wick = max(0.0, c['h'] - max(c['o'], c['c'])) / span
    lower_wick = max(0.0, min(c['o'], c['c']) - c['l']) / span
    return {'body': body / a15, 'close': pos, 'wick': wick, 'body_fraction': body / span, 'upper_wick_range': upper_wick, 'lower_wick_range': lower_wick}
def trend_shape_ok(m):
    return m is not None and m['body_fraction'] >= TREND_BODY_FRACTION_MIN and (m['upper_wick_range'] <= TREND_UPPER_WICK_MAX_RANGE) and (m['lower_wick_range'] <= TREND_LOWER_WICK_MAX_RANGE)
def trend_body_regime(first_body, second_body, live_body):
    if live_body > second_body > first_body:
        return 'ACCELERAZIONE'
    if live_body >= (first_body + second_body) / 2.0:
        return 'REGOLARE'
    return 'RALLENTAMENTO'
def trend_structure_reason(direction, first, second, live, a15):
    tolerance = TREND_PROTECTED_EXTREME_TOL_ATR * a15
    breakout = TREND_LIVE_BREAKOUT_ATR * a15
    if direction == 'LONG':
        if second['h'] <= first['h']:
            return 'seconda: massimo non crescente'
        if second['l'] < first['l'] - tolerance:
            return 'seconda: minimo non protetto'
        if live['l'] < second['l'] - tolerance:
            return 'terza LIVE: minimo non protetto'
        if live['c'] < second['h'] + breakout:
            return 'terza LIVE: prezzo sotto breakout +0.05 ATR'
    elif direction == 'SHORT':
        if second['l'] >= first['l']:
            return 'seconda: minimo non decrescente'
        if second['h'] > first['h'] + tolerance:
            return 'seconda: massimo non protetto'
        if live['h'] > second['h'] + tolerance:
            return 'terza LIVE: massimo non protetto'
        if live['c'] > second['l'] - breakout:
            return 'terza LIVE: prezzo sopra breakout -0.05 ATR'
    else:
        return 'direzione non valida'
    return None
def trend_1h(candles):
    closes = [x['c'] for x in candles[:-1]]
    e20, e50, prev = (ema(closes, 20), ema(closes, 50), ema(closes[:-1], 20))
    if None in (e20, e50, prev):
        return 'NEUTRAL'
    price = candles[-2]['c']
    if price > e20 > e50 and e20 >= prev:
        return 'LONG'
    if price < e20 < e50 and e20 <= prev:
        return 'SHORT'
    return 'NEUTRAL'
def context_4h(candles):
    closes = [x['c'] for x in candles[:-1]]
    e20, e50 = (ema(closes, 20), ema(closes, 50))
    if e20 is None or e50 is None:
        return 'NEUTRAL'
    if candles[-2]['c'] > e20 > e50:
        return 'LONG'
    if candles[-2]['c'] < e20 < e50:
        return 'SHORT'
    return 'NEUTRAL'
def btc_bias(data):
    one, four = (data['1h'], data['4h'])
    close1 = [x['c'] for x in one[:-1]]
    close4 = [x['c'] for x in four[:-1]]
    a, b = (ema(close1, 20), ema(close1, 50))
    c, d = (ema(close4, 20), ema(close4, 50))
    prev = ema(close1[:-1], 20)
    if None in (a, b, c, d, prev):
        return 'NEUTRAL_STABLE'
    if one[-2]['c'] > a > b and a > prev and (four[-2]['c'] > c):
        return 'LONG_STRONG'
    if one[-2]['c'] < a < b and a < prev and (four[-2]['c'] < c):
        return 'SHORT_STRONG'
    if one[-2]['c'] > a and a > prev:
        return 'LONG_LIGHT'
    if one[-2]['c'] < a and a < prev:
        return 'SHORT_LIGHT'
    return 'NEUTRAL_STABLE'
def btc_30m_direction(candles):
    if len(candles) < 22:
        return 'NEUTRAL'
    a = atr(candles[:-1])
    if not a:
        return 'NEUTRAL'
    live = candles[-1]
    direction = 'LONG' if live['c'] > live['o'] else 'SHORT'
    metrics = candle_metrics(live, direction, a)
    return direction if metrics and metrics['body'] >= BTC30_DIRECTIONAL_BODY_ATR_MIN else 'NEUTRAL'
def btc15_direction(data):
    c = data['15m']
    a = atr(c[:-1])
    pace, _ = live_volume_ratio(c)
    result = {'directional': 'NEUTRAL', 'body_atr': 0.0, 'volume_pace': pace}
    if not a:
        return result
    live = c[-1]
    direction = 'LONG' if live['c'] > live['o'] else 'SHORT'
    m = candle_metrics(live, direction, a)
    if m:
        result['body_atr'] = m['body']
        if m['body'] >= BTC15_DIRECTIONAL_BODY_ATR_MIN:
            result['directional'] = direction
    return result
def stochastic(candles):
    if len(candles) < STOCH_K_PERIOD + STOCH_K_SMOOTH + STOCH_D_PERIOD - 2:
        return None
    fast = []
    for end in range(STOCH_K_PERIOD - 1, len(candles)):
        window = candles[end - STOCH_K_PERIOD + 1:end + 1]
        high = max((x['h'] for x in window))
        low = min((x['l'] for x in window))
        fast.append(50.0 if high <= low else 100 * (candles[end]['c'] - low) / (high - low))
    slow = [sum(fast[i - STOCH_K_SMOOTH + 1:i + 1]) / STOCH_K_SMOOTH for i in range(STOCH_K_SMOOTH - 1, len(fast))]
    if len(slow) < STOCH_D_PERIOD:
        return None
    return (slow[-1], sum(slow[-STOCH_D_PERIOD:]) / STOCH_D_PERIOD)
def signed(value, direction):
    return value if direction == 'LONG' else -value
def intelligent_stop(direction, entry, level, live, a15, recent, ctx, bias, one_hour):
    if ctx == direction and bias.startswith(direction):
        buffer = SL_BUFFER_STRONG_TREND_ATR15
    elif ctx == 'NEUTRAL':
        buffer = SL_BUFFER_NEUTRAL_4H_ATR15
    else:
        buffer = SL_BUFFER_NORMAL_TREND_ATR15
    closed15 = recent[-SL_STRUCTURE_LOOKBACK_BARS:]
    closed1h = one_hour[:-1][-SL_1H_LOOKBACK_BARS:]
    if not closed15:
        return None
    margin = a15 * buffer
    maximum = a15 * SL_MAX_DISTANCE_ATR15
    if direction == 'LONG':
        support15 = min([live['l'], level] + [x['l'] for x in closed15])
        structural = support15 - margin
        if closed1h:
            support1h = min((x['l'] for x in closed1h))
            candidate = support1h - margin
            if 0 < entry - candidate <= maximum:
                structural = min(structural, candidate)
        raw = entry - structural
    else:
        resistance15 = max([live['h'], level] + [x['h'] for x in closed15])
        structural = resistance15 + margin
        if closed1h:
            resistance1h = max((x['h'] for x in closed1h))
            candidate = resistance1h + margin
            if 0 < candidate - entry <= maximum:
                structural = max(structural, candidate)
        raw = structural - entry
    if raw <= 0 or raw > maximum:
        return None
    distance = max(raw, a15 * SL_MIN_DISTANCE_ATR15)
    if distance > maximum:
        return None
    return entry - distance if direction == 'LONG' else entry + distance
def available_room(c15, direction, entry, risk):
    history = c15[:-1]
    older = history[-(RESISTANCE_LOOKBACK_BARS + STRUCTURE_BARS):-STRUCTURE_BARS]
    if direction == 'LONG':
        levels = [x['h'] for x in older if x['h'] > entry]
        return min(levels) - entry if levels else float('inf')
    levels = [x['l'] for x in older if x['l'] < entry]
    return entry - max(levels) if levels else float('inf')
def clamp(v, lo, hi):
    return max(lo, min(hi, v))
def adaptive_targets(direction, entry, stop, a15, quality, pace, body, follow, ctx, bias):
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    strength = clamp((pace - 1.2) / 2.5, 0, 1) * 0.3
    strength += clamp((body - 0.16) / 0.7, 0, 1) * 0.25
    strength += clamp((follow - 0.06) / 0.6, 0, 1) * 0.2
    strength += clamp((quality - 7) / 6, 0, 1) * 0.15
    strength += 0.05 * int(ctx == direction) + 0.05 * int(bias.startswith(direction))
    strength = clamp(strength, 0, 1)
    adjusted = clamp(strength * (1 - 0.35 * clamp((body - 1.2) / 0.35, 0, 1)), 0, 1)
    d1 = max(risk * 0.3, entry * TP_MIN_NET_PCT)
    previous_tp1_distance = max(risk * 0.35, entry * TP_MIN_NET_PCT)
    d2 = max(risk * 0.45, previous_tp1_distance * 1.15)
    d3 = max(risk * 0.6, d2 * 1.15)
    sign = 1 if direction == 'LONG' else -1
    return (entry + sign * d1, entry + sign * d2, entry + sign * d3, strength)
def leverage_cap(q):
    if q >= 13:
        return 100
    if q >= 12:
        return 75
    if q >= 11:
        return 50
    if q >= 10:
        return 30
    if q >= 9:
        return 20
    return 15
def calculate_leverage(entry, stop, quality, aggressive_ok):
    stop_pct = abs(entry - stop) / entry
    if stop_pct <= 0:
        return 1
    cap = leverage_cap(quality)
    if not aggressive_ok:
        cap = min(cap, 15)
    allowed = min(int(LEVERAGE_SAFETY / stop_pct), cap, 100)
    return max((x for x in LEVERAGE_STEPS if x <= allowed), default=1)
def trend_15m(candles, direction):
    closes = [x['c'] for x in candles]
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    prev20 = ema(closes[:-1], 20)
    if None in (e20, e50, prev20):
        return False
    if direction == 'LONG':
        return closes[-1] > e20 > e50 and e20 > prev20
    return closes[-1] < e20 < e50 and e20 < prev20
def candle_color(c):
    if c['c'] > c['o']:
        return 'LONG'
    if c['c'] < c['o']:
        return 'SHORT'
    return 'NEUTRAL'
def analyze_symbol(symbol, data, bias, btc15, btc30, btc1h, now, counters, btc_volume=None, volume_override=None):
    c15, c1h, c4h = (data['15m'], data['1h'], data['4h'])
    volume = volume_override if volume_override is not None else progressive_volume_metrics(symbol, c15, now)
    if min(len(c15), len(c1h), len(c4h)) < 55:
        counters['dati insufficienti'] += 1
        return None
    direction = trend_1h(c1h)
    if direction == 'NEUTRAL':
        counters['trend 1H neutro'] += 1
        return None
    sign = 1 if direction == 'LONG' else -1
    live = c15[-1]
    prev1, prev2 = (c15[-2], c15[-3])
    a15 = atr(c15[:-1])
    if not a15 or a15 <= 0:
        counters['ATR mancante'] += 1
        return None
    metrics = candle_metrics(live, direction, a15)
    if metrics is None:
        counters['candela live contraria'] += 1
        return None
    pace, elapsed = live_volume_ratio(c15)
    if elapsed < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        counters['volume live in formazione'] += 1
        return None
    fast = candle_color(prev1) != direction and metrics['body'] >= FAST_BODY_ATR_MIN and (metrics['body_fraction'] >= FAST_BODY_FRACTION_MIN) and (metrics['wick'] <= FAST_WICK_MAX)
    prev2_metrics = candle_metrics(prev2, direction, a15)
    prev1_metrics = candle_metrics(prev1, direction, a15)
    structure_ok = trend_structure_reason(direction, prev2, prev1, live, a15) is None
    trend = prev2_metrics is not None and prev1_metrics is not None and (prev2_metrics['body'] >= TREND_FIRST_BODY_ATR_MIN) and (prev1_metrics['body'] >= TREND_SECOND_BODY_ATR_MIN) and trend_shape_ok(prev2_metrics) and trend_shape_ok(prev1_metrics) and (sign * (prev1['c'] - prev2['c']) > 0) and (sign * (live['c'] - prev1['c']) > 0) and (metrics['body'] >= TREND_BODY_ATR_MIN) and trend_shape_ok(metrics) and structure_ok
    volume_confirmation = 'PROGRESSIVO (A)'
    if fast or trend:
        if not volume['ready']:
            counters['volume progressivo: campioni insufficienti'] += 1
            return None
        if not volume['intensity_ok']:
            counters['volume progressivo: intensita insufficiente'] += 1
            return None
        fast_volume_ok = volume['accel_ok'] or volume['sustained_ok']
        if fast and (not fast_volume_ok):
            counters['volume FAST: ne progressivo ne alto sostenuto'] += 1
            return None
        if not fast and trend and (not volume['accel_ok']):
            counters['volume progressivo: accelerazione insufficiente'] += 1
            return None
        if fast and (not volume['accel_ok']) and volume['sustained_ok']:
            volume_confirmation = 'ALTO SOSTENUTO (B >=2.50x)'
    if fast:
        mode = 'FAST ENTRY - PRIMA CANDELA'
        reference = live['o']
        max_extension = FAST_MAX_EXTENSION_ATR
        btc_min = FAST_BTC15_BODY_ATR_MIN
    elif trend:
        mode = 'TREND ENTRY - TERZA CANDELA'
        reference = prev2['o']
        max_extension = TREND_MAX_EXTENSION_ATR
        btc_min = TREND_BTC15_BODY_ATR_MIN
    else:
        counters['setup FAST/TREND non valido'] += 1
        return None
    total_move = sign * (live['c'] - reference) / a15
    if total_move > max_extension:
        counters['impulso troppo esteso'] += 1
        return None
    if btc15['directional'] != direction or btc15['body_atr'] < btc_min:
        counters['BTC15 non forte/concorde'] += 1
        return None
    if mode == 'TREND ENTRY - TERZA CANDELA':
        if not btc_volume or not btc_volume['ready']:
            counters['BTC15 TREND: campioni volume insufficienti'] += 1
            return None
        if not btc_volume['intensity_ok']:
            counters['BTC15 TREND: volume sotto intensita minima'] += 1
            return None
        if not btc_volume['accel_ok']:
            counters['BTC15 TREND: volume non progressivo'] += 1
            return None
    if btc1h not in (direction, 'NEUTRAL') or not bias.startswith(direction + '_'):
        counters['BTC 1H/bias contrario'] += 1
        return None
    if not trend_15m(c15, direction):
        counters['EMA20/50 15m contrarie'] += 1
        return None
    sto = stochastic(c15)
    if sto is None or signed(sto[0] - sto[1], direction) <= 0:
        counters['stocastico contrario'] += 1
        return None
    stoch_strength_delta = None
    if mode == 'TREND ENTRY - TERZA CANDELA':
        # Confronta la terza candela LIVE con la seconda gia chiusa.
        previous_sto = stochastic(c15[:-1])
        if previous_sto is None:
            counters['stocastico TREND: storico insufficiente'] += 1
            return None
        previous_gap = signed(previous_sto[0] - previous_sto[1], direction)
        live_gap = signed(sto[0] - sto[1], direction)
        stoch_strength_delta = live_gap - previous_gap
        if stoch_strength_delta < STOCH_TREND_MIN_STRENGTHENING:
            counters['stocastico TREND: non si rafforza'] += 1
            return None
    ctx = context_4h(c4h)
    entry = live['c']
    level = reference
    stop = intelligent_stop(direction, entry, level, live, a15, c15[:-1], ctx, bias, c1h)
    if stop is None:
        counters['stop strutturale fuori range'] += 1
        return None
    risk = abs(entry - stop)
    room = available_room(c15, direction, entry, risk)
    if room < MIN_ROOM_R * risk:
        counters['spazio tecnico insufficiente'] += 1
        return None
    closed_vol = closed_volume_ratio(c15)
    if fast and closed_vol < MIN_CLOSED_VOLUME_RATIO:
        counters['volume candela precedente insufficiente (FAST)'] += 1
        return None
    body = metrics['body']
    volume_quality_bonus = volume['growth'] >= 0.25 or (fast and volume_confirmation.startswith('ALTO SOSTENUTO'))
    quality = 5 + int(volume['relative_pace'] >= 1.1) + int(volume_quality_bonus) + int(body >= 0.4) + int(body >= 0.65)
    quality += int(total_move >= 0.35) + (int(closed_vol >= CLOSED_VOLUME_PREFERRED) if fast else 1)
    quality += int(ctx == direction) + int(signed(sto[0] - sto[1], direction) >= 2.0)
    quality = min(quality, 13)
    if quality < MIN_QUALITY:
        counters['qualita insufficiente'] += 1
        return None
    leverage = calculate_leverage(entry, stop, quality, False)
    targets = adaptive_targets(direction, entry, stop, a15, quality, pace, body, total_move, ctx, bias)
    if targets is None:
        counters['target non valido'] += 1
        return None
    tp1, tp2, tp3, strength = targets
    counters['segnali idonei ' + mode] += 1
    return {'direction': direction, 'mode': mode, 'price': entry, 'entry_low': entry - a15 * 0.06, 'entry_high': entry + a15 * 0.06, 'sl': stop, 'tp1': tp1, 'tp2': tp2, 'tp3': tp3, 'leverage': leverage, 'quality': quality, 'level': level, 'candle_time': live['t'], 'live_volume_pace': pace, 'closed_volume_ratio': closed_vol, 'volume_dynamic_floor': volume['dynamic_floor'], 'volume_acceleration': volume['growth'], 'volume_confirmation': volume_confirmation, 'volume_relative_strength': volume['relative_pace'], 'live_body_atr': body, 'follow_through_atr': total_move, 'trend_bodies_atr': (round(prev2_metrics['body'], 3), round(prev1_metrics['body'], 3), round(body, 3)) if trend and (not fast) else None, 'trend_body_regime': trend_body_regime(prev2_metrics['body'], prev1_metrics['body'], body) if trend and (not fast) else None, 'rejection_wick_ratio': metrics['wick'], 'price_progress_atr': total_move, 'stoch_k': sto[0], 'stoch_d': sto[1], 'stoch_strength_delta': stoch_strength_delta, 'context1h': direction, 'context4h': ctx, 'btc': bias, 'btc15_direction': btc15['directional'], 'btc15_body_atr': btc15['body_atr'], 'btc15_volume_pace': btc15['volume_pace'], 'btc15_volume_progressive': bool(btc_volume and btc_volume['ready'] and btc_volume['intensity_ok'] and btc_volume['accel_ok']), 'btc15_volume_growth': btc_volume['growth'] if btc_volume else 0.0, 'btc30_direction': btc30, 'btc1h_direction': btc1h, 'target_strength': strength, 'check_seconds': int(elapsed), 'room_r': room / risk if room != float('inf') else None}
def fmt_price(v):
    if v >= 1000:
        return f'{v:.2f}'
    if v >= 1:
        return f'{v:.4f}'
    if v >= 0.01:
        return f'{v:.6f}'
    return f'{v:.9f}'
def build_message(symbol, s):
    pair = symbol.replace('USDT', '/USDT')
    italian_time = datetime.now(ZoneInfo('Europe/Rome')).strftime('%d/%m/%Y %H:%M:%S')
    room = f"{s['room_r']:.2f}R" if s['room_r'] is not None else 'nessun ostacolo rilevato'
    adapt = s.get('adaptive')
    adaptive_note = f"V10 PROGRESSIVO (solo osservazione): {adapt['score']}/100, \nregime {adapt['regime']}, fattori in miglioramento {adapt['improving']}/6\n" if adapt else ''
    trend_bodies = s.get('trend_bodies_atr')
    strength_note = f"Stoch rafforzamento TREND: +{s['stoch_strength_delta']:.2f} punti K-D\n" if s.get('stoch_strength_delta') is not None else ''
    body_note = f"Corpi 1/2/3: {trend_bodies[0]:.2f} / {trend_bodies[1]:.2f} / {trend_bodies[2]:.2f} ATR | {s['trend_body_regime']}\n" if trend_bodies else ''
    return f"🚀 FAST/TREND V10.1.1 (SPERIMENTALE)\n{pair} — {s['direction']}\nOrario Italia: {italian_time}\nSetup: {s['mode']} | Qualita': {s['quality']}/13\n\nENTRY: {fmt_price(s['entry_low'])} - {fmt_price(s['entry_high'])}\nSL strutturale 15m/1H: {fmt_price(s['sl'])}\n\nTP1: {fmt_price(s['tp1'])}\nTP2: {fmt_price(s['tp2'])}\nTP3: {fmt_price(s['tp3'])}\n\nLeva indicativa: {s['leverage']}x\nCandela live in corso da: {s['check_seconds']}s\nLivello: {fmt_price(s['level'])}\nVolume LIVE: {s['live_volume_pace']:.2f}x\nVolume soglia dinamica: {s['volume_dynamic_floor']:.2f}x\nConferma volume: {s['volume_confirmation']}\nVariazione velocita' volume: {s['volume_acceleration'] * 100:+.0f}%\nVolume chiuso: {s['closed_volume_ratio']:.2f}x\nProgressione: {s['price_progress_atr']:+.3f} ATR15\n{body_note}Estensione: {s['follow_through_atr']:.2f} ATR15\nStoch K/D: {s['stoch_k']:.1f}/{s['stoch_d']:.1f}\n{strength_note}Spazio tecnico storico: {room}\n\nTrend 1H: {s['context1h']} | 4H: {s['context4h']}\nBTC bias: {s['btc']}\nBTC15: {s['btc15_direction']} (corpo {s['btc15_body_atr']:.2f} ATR) | BTC1H: {s['btc1h_direction']}\nBTC15 volume progressivo: {('SI' if s['btc15_volume_progressive'] else 'NO')} (+{s['btc15_volume_growth'] * 100:.0f}% ritmo; obbligatorio solo TREND)\nBTC30 informativo: {s['btc30_direction']}\n{adaptive_note}Segnale anticipato non validato. Stop e leva non garantiscono protezione da liquidazione."
def scan_market():
    global last_scan_log, last_diag
    now = time.monotonic()
    market, btc30_candles = market_snapshot()
    btc_data = market.get('BTCUSDT')
    if btc_data is None:
        if now - last_scan_log > SCAN_LOG_EVERY_SECONDS:
            print('BTC15 assente o WebSocket non aggiornato; invii sospesi', flush=True)
            last_scan_log = now
        return
    btc_volume = progressive_volume_metrics('BTCUSDT', btc_data['15m'], now)
    bias = btc_bias(btc_data)
    btc15 = btc15_direction(btc_data)
    btc30 = btc_30m_direction(btc30_candles) if btc30_candles else 'NEUTRAL'
    btc1h = trend_1h(btc_data['1h'])
    counters = Counter()
    for symbol, data in market.items():
        try:
            vol = btc_volume if symbol == 'BTCUSDT' else progressive_volume_metrics(symbol, data['15m'], now)
            signal = analyze_symbol(symbol, data, bias, btc15, btc30, btc1h, now, counters, btc_volume=btc_volume, volume_override=vol)
            if signal and queue_signal(symbol, signal):
                print('SEGNALE', symbol, signal['direction'], signal['mode'], flush=True)
        except Exception as exc:
            print('ERRORE', symbol, str(exc), flush=True)
    diagnostic_totals.update(counters)
    if now - last_scan_log >= SCAN_LOG_EVERY_SECONDS:
        print('Scansione', len(market), '/25 | BTC15', btc15['directional'], '| BTC1H', btc1h, flush=True)
        last_scan_log = now
    if now - last_diag >= DIAGNOSTIC_EVERY_SECONDS:
        print('DIAGNOSTICA SINTETICA:', diagnostic_totals.most_common(8), flush=True)
        diagnostic_totals.clear()
        last_diag = now
def main():
    print('CryptoSignalAI V10.1.1 SNELLA - Stoch TREND progressivo | FAST/TREND 25 coppie | WebSocket 1s', flush=True)
    while True:
        try:
            bootstrap_history()
            break
        except Exception as exc:
            print('ERRORE storico:', exc, 'riprovo fra 20s', flush=True)
            time.sleep(20)
    threading.Thread(target=telegram_sender_worker, daemon=True).start()
    threading.Thread(target=rest_maintenance_worker, daemon=True).start()
    threading.Thread(target=ws_connection_worker, daemon=True).start()
    send_telegram('Bot V10.1.1 SNELLA avviato. Stocastico in rafforzamento su TREND (+0.5 punti K-D); FAST invariato. 25 crypto, WebSocket 1s. Usare in simulazione.')
    next_scan = time.monotonic()
    while True:
        next_scan += SCAN_SECONDS
        try:
            scan_market()
        except Exception as exc:
            print('Errore scansione:', exc, flush=True)
        time.sleep(max(0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()
if __name__ == '__main__':
    main()
