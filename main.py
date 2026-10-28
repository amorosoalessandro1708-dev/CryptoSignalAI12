"""MOMENTUM ONE 4H CONFIRMED + 15M RETEST - VERSIONE ALLEGGERITA 25 COIN.

MODIFICHE APPLICATE (ottimizzato per continuazione trend + 25 coin):
1. Breakout 4H: 10 candele precedenti (non 3), buffer 0.03 ATR4H (non 0.05), ATR 14 periodi
2. Retest 15m: tolleranza 0.15 ATR15 (non 0.10), validità max 12 candele 15m (3h)
3. Volume: soglia finale 1.35x (non 1.60x) su media 10 chiuse
4. Candela conferma 15m: corpo >=75% (non 82%), >=0.50 ATR15 (non 0.65), wick <=15% (non 8%/10%), chiusura +0.02 ATR15 (non 0.05)
5. SL: 3 candele 4H (non 2) +0.10 ATR4H
6. Scan: 1 secondo fisso + cooldown 8h per coin
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

BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

SYMBOLS = [
    'SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT',
    'DOGEUSDT', 'AVAXUSDT', 'SUIUSDT', 'LINKUSDT', 'ADAUSDT',
    'NEARUSDT', 'UNIUSDT', 'LTCUSDT', 'DOTUSDT', 'APTUSDT',
    'ARBUSDT', 'OPUSDT', 'INJUSDT', 'RENDERUSDT', '1000PEPEUSDT',
    'FILUSDT', 'ETCUSDT', 'AAVEUSDT', 'TIAUSDT', 'SEIUSDT',
]

SCAN_SECONDS = 1.0
TIMEFRAME_SECONDS = 15 * 60
ATR_PERIOD = 14
BTC_NEUTRAL_BODY_ATR_MAX = 0.20

FOUR_HOUR_RESISTANCE_LOOKBACK = 10
BREAKOUT_BUFFER_ATR4H = 0.03
FOUR_HOUR_CLOSE_POSITION_MIN = 0.70
MACRO_CLOSE_GRACE_SECONDS = 600

VOLUME_RATIO_MIN = 1.35
VOLUME_AVG_BARS_15M = 10
LIVE_VOLUME_PACE_MIN_SECONDS = 60
LIVE_VOLUME_PROGRESS_LOG_SECONDS = 180

RETEST_ZONE_ATR15 = 0.15
MICRO_CLOSE_BUFFER_ATR15 = 0.02
CONFIRM_BODY_FRACTION_MIN = 0.75
CONFIRM_BODY_ATR_MIN = 0.50
CONFIRM_LEADING_WICK_MAX = 0.15
CONFIRM_TRAILING_WICK_MAX = 0.15
SETUP_LIFETIME_SECONDS = 4 * 60 * 60
CLOSE_EVENT_MAX_DELAY_SECONDS = 12
COOLDOWN_AFTER_SIGNAL_SECONDS = 8 * 60 * 60

SL_4H_LOOKBACK = 3
SL_BUFFER_ATR4H = 0.10
TP1_R = 0.30
TP2_R = 0.80
TP3_R = 1.50
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
FIFTEEN_MIN_MS = TIMEFRAME_SECONDS * 1000

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
HISTORY_MAXLEN = 320
ITALY = ZoneInfo('Europe/Rome')

cache_lock = threading.RLock()
rest_lock = threading.Lock()
alert_lock = threading.Lock()
market_cache = {}
last_ws_receive = {}
needs_refresh = set()
websocket_connected = False
last_rest_call = 0.0
sent_alerts = set()
pending_alerts = set()
failed_alert_times = {}
telegram_queue = queue.Queue(maxsize=100)
active_setups = {}
seen_macro_bars = {}
last_signal_time = {}

def log(*parts):
    print(*parts, flush=True)

def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        log('TELEGRAM NON CONFIGURATO')
        return False
    try:
        response = requests.post(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            data={'chat_id': CHAT_ID, 'text': text}, timeout=20,
        )
        response.raise_for_status()
        if not response.json().get('ok'):
            log('Telegram ha rifiutato:', response.text[:250])
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
                log('Limite API:', response.status_code, symbol, 'pausa', wait)
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
            if c['v'] < last['v']:
                return
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
    if len(closed_candles) < ATR_PERIOD + 1:
        return None
    bars = closed_candles[-(ATR_PERIOD + 1):]
    tr = []
    for i in range(1, len(bars)):
        c, prev = bars[i], bars[i - 1]
        tr.append(max(c['h']-c['l'], abs(c['h']-prev['c']), abs(c['l']-prev['c'])))
    return sum(tr) / ATR_PERIOD

def live_volume_progress(candles, now_ms):
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
    pace_ratio = (actual_ratio * TIMEFRAME_SECONDS / elapsed if elapsed >= LIVE_VOLUME_PACE_MIN_SECONDS else None)
    return {
        'candle_t': live['t'],
        'elapsed_seconds': elapsed,
        'actual_ratio': actual_ratio,
        'pace_ratio': pace_ratio,
        'progress_percent': 100.0 * actual_ratio / VOLUME_RATIO_MIN,
    }

def monitor_live_confirmation_volume(setup, candles, now_ms):
    if not setup['touched'] or not candles:
        return
    if candles[-1]['t'] <= setup['touch_t']:
        return
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
    state['actual_ratio'] = progress['actual_ratio']
    state['pace_ratio'] = progress['pace_ratio']
    state['progress_percent'] = progress['progress_percent']
    if (progress['actual_ratio'] >= VOLUME_RATIO_MIN and not state['reached_final_volume_live']):
        state['reached_final_volume_live'] = True
        log('VOLUME LIVE', setup['symbol'], setup['direction'], f"{progress['actual_ratio']:.2f}x su {VOLUME_RATIO_MIN:.2f}x")

def four_hour_closed_candles(candles_15m):
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
    four = four_hour_closed_candles(candles)
    required = max(ATR_PERIOD + 2, FOUR_HOUR_RESISTANCE_LOOKBACK + 1)
    if len(four) < required:
        return None
    macro = four[-1]
    close_ms = macro['t'] + FOUR_HOURS_MS
    if not (0 <= now_ms - close_ms <= MACRO_CLOSE_GRACE_SECONDS * 1000):
        return None
    prev = four[:-1]
    if any(prev[i]['t'] + FOUR_HOURS_MS != prev[i + 1]['t'] for i in range(len(prev) - 1)):
        return None
    if prev[-1]['t'] + FOUR_HOURS_MS != macro['t']:
        return None
    direction = ('LONG' if macro['c'] > macro['o'] else 'SHORT' if macro['c'] < macro['o'] else None)
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
    close_pos = ((macro['c']-macro['l']) / full_range if direction == 'LONG' else (macro['h']-macro['c']) / full_range)
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
    if len(candles) < VOLUME_AVG_BARS_15M + 2:
        return 0.0
    confirmation = candles[-2]
    previous = candles[-(VOLUME_AVG_BARS_15M + 2):-2]
    avg = sum(max(0.0, c['v']) for c in previous) / VOLUME_AVG_BARS_15M
    if avg <= 0:
        return 0.0
    return max(0.0, confirmation['v']) / avg

def confirmation_15m_valid(candle, direction, level, atr15):
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
    if not beyond_breakout(closing, direction, level, MICRO_CLOSE_BUFFER_ATR15 * atr15):
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
    return (leading_wick <= CONFIRM_LEADING_WICK_MAX and trailing_wick <= CONFIRM_TRAILING_WICK_MAX)

def build_confirmed_signal(setup, candles, btc_direction, candle_15m, now_ms):
    if btc_direction not in (setup['direction'], 'NEUTRAL'):
        return None
    live = candles[-1]
    price = live['c']
    if price <= 0 or not beyond_breakout(price, setup['direction'], setup['level'], MICRO_CLOSE_BUFFER_ATR15 * setup['atr15_retest']):
        return None
    info = four_hour_structural_stop(candles, setup['direction'])
    if not info:
        return None
    risk = (price - info['sl'] if setup['direction'] == 'LONG' else info['sl'] - price)
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
    if len(candles) < ATR_PERIOD + 3:
        return 'WAIT', None
    live = candles[-1]
    closed = candles[-2]
    closed_end = closed['t'] + FIFTEEN_MIN_MS
    if live['t'] != closed_end:
        return 'EXPIRED', None
    if closed['t'] < setup['macro_end_t']:
        return 'WAIT', None
    if now_ms - setup['macro_end_t'] > SETUP_LIFETIME_SECONDS * 1000:
        return 'EXPIRED', None
    if setup['last_checked_15m_t'] == closed['t']:
        return 'WAIT', None
    if (setup['last_checked_15m_t'] is not None and closed['t'] != setup['last_checked_15m_t'] + FIFTEEN_MIN_MS):
        return 'EXPIRED', None
    setup['last_checked_15m_t'] = closed['t']
    if now_ms - closed_end > CLOSE_EVENT_MAX_DELAY_SECONDS * 1000:
        return 'WAIT', None
    atr15 = true_range_atr(candles[:-2])
    if not atr15 or atr15 <= 0:
        return 'WAIT', None
    level = setup['level']
    tol = RETEST_ZONE_ATR15 * atr15
    d = setup['direction']
    touched = (closed['l'] <= level + tol and closed['h'] >= level - tol)
    if (d == 'LONG' and closed['l'] < level - tol) or (d == 'SHORT' and closed['h'] > level + tol):
        return 'EXPIRED', None
    if touched and not setup['touched']:
        setup['touched'] = True
        setup['touch_t'] = closed['t']
        setup['touched_price'] = closed['l'] if d == 'LONG' else closed['h']
        log('RETEST 15M', setup['symbol'], d, 'livello', price_format(level))
    if not setup['touched']:
        return 'WAIT', None
    if closed['t'] <= setup['touch_t']:
        return 'WAIT', None
    if not confirmation_15m_valid(closed, d, level, atr15):
        return 'WAIT', None
    ratio15 = confirmation_15m_volume_ratio(candles)
    if ratio15 < VOLUME_RATIO_MIN:
        return 'WAIT', None
    btc_direction = btc15_closed_direction(btc_candles, closed['t'])
    if btc_direction not in (d, 'NEUTRAL'):
        return 'WAIT', None
    setup['atr15_retest'] = atr15
    setup['confirm_volume_ratio'] = ratio15
    signal = build_confirmed_signal(setup, candles, btc_direction, closed, now_ms)
    if signal:
        return 'SIGNAL', signal
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
    d = 1 if s['direction'] == 'LONG' else -1
    risk = abs(s['entry'] - s['sl'])
    tp1 = s['entry'] + d * TP1_R * risk
    tp2 = s['entry'] + d * TP2_R * risk
    tp3 = s['entry'] + d * TP3_R * risk
    now_italy = datetime.now(ITALY).strftime('%d/%m/%Y %H:%M:%S')
    macro_italy = datetime.fromtimestamp(s['macro_t']/1000, ITALY).strftime('%d/%m %H:%M')
    retest_italy = datetime.fromtimestamp(s['retest_t']/1000, ITALY).strftime('%d/%m %H:%M')
    confirm_italy = datetime.fromtimestamp(s['confirm_15m_t']/1000, ITALY).strftime('%d/%m %H:%M')
    return (
        f"MOMENTUM ONE ALLEGGERITO 25 COIN - {s['direction']} [SIMULAZIONE]\n"
        f"{s['symbol'].replace('USDT', '/USDT')}\n"
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
    )

def enqueue_alert(signal):
    key = (signal['symbol'], signal['macro_t'])
    now = time.monotonic()
    if now - last_signal_time.get(signal['symbol'], -1e9) < COOLDOWN_AFTER_SIGNAL_SECONDS:
        log('Cooldown attivo per', signal['symbol'])
        return False
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
                    last_signal_time[key[0]] = time.monotonic()
                else:
                    failed_alert_times[key] = time.monotonic()
        finally:
            with alert_lock:
                pending_alerts.discard(key)
            telegram_queue.task_done()

def current_snapshot():
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
                log('BREAKOUT 4H CHIUSO', symbol, candidate['direction'], '| livello', price_format(candidate['level']))
            else:
                active_setups.pop(symbol, None)
        setup = active_setups.get(symbol)
        if not setup:
            continue
        if now_ms - setup['macro_end_t'] > SETUP_LIFETIME_SECONDS * 1000:
            del active_setups[symbol]
            continue
        status, signal = evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms)
        if status == 'EXPIRED':
            del active_setups[symbol]
        elif status == 'SIGNAL' and signal:
            if enqueue_alert(signal):
                queued += 1
                log('SEGNALE ALLEGGERITO', symbol, signal['direction'], '| vol', f"{signal['confirm_volume_ratio']:.2f}x")
                del active_setups[symbol]
        if symbol in active_setups:
            monitor_live_confirmation_volume(setup, candles, now_ms)
    oldest_allowed = now_ms - 2 * FOUR_HOURS_MS
    with alert_lock:
        sent_alerts.intersection_update({k for k in sent_alerts if k[1] >= oldest_allowed})
        for key in list(failed_alert_times):
            if key[1] < oldest_allowed:
                del failed_alert_times[key]
    return queued

def main():
    log('MOMENTUM ONE ALLEGGERITO | 25 crypto | scan 1s | 4H 10 barre +0.03 ATR + RETEST 0.15 ATR')
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
    send_telegram('MOMENTUM ONE ALLEGGERITO 25 COIN AVVIATO - Scan 1s - 4H 10 barre +0.03 ATR - Retest 0.15 ATR - Vol 1.35x - Corpo 75%/0.50 ATR Wick 15% - SL 3x4H')
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
            log('Scansione 1s | feed attivi:', len(snap), '/25 | BTC15:', b, '| setup 4H:', len(active_setups))
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()

if __name__ == '__main__':
    main()
