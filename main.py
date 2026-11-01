"""MOMENTUM ONE UNIFICATO - 40 COIN | UN SOLO SERVIZIO RAILWAY.
Gruppo 1H: 18 COIN - BTC,ETH,SOL + PEPE,BONK,SHIB,WIF,TAO,FET,RENDER,SUI,SEI,JUP,INJ,ARB,OP,PENDLE,ONDO
Gruppo 4H: le altre 22 criptovalute lente
FIX FEED: tollera 1 candela di ritardo, feed attivi 40/40 stabile
CONFIG OPZIONE B: 15c 1H / 6c 4H / vol 1.10x / retest+direct
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

ONE_HOUR_SYMBOLS = frozenset((
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT',
    '1000PEPEUSDT', '1000BONKUSDT', '1000SHIBUSDT', 'WIFUSDT',
    'TAOUSDT', 'FETUSDT', 'RENDERUSDT',
    'SUIUSDT', 'SEIUSDT', 'JUPUSDT', 'INJUSDT',
    'ARBUSDT', 'OPUSDT', 'PENDLEUSDT', 'ONDOUSDT'
))
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
    '1H': {'name': '1H', 'macro_ms': ONE_HOUR_MS,'lookback': 15, 'macro_volume_min': 1.10,'volume_bars_macro': 20, 'breakout_buffer_atr': 0.04,'macro_close_position_min': 0.65,'confirm_volume_min': 0.60, 'body_fraction': 0.70,'body_atr_min': 0.60, 'wick_directional': 0.12,'wick_opposite': 0.12, 'sl_lookback': 5,'sl_buffer_atr': 0.25, 'lifetime_seconds': 6 * 60 * 60,},
    '4H': {'name': '4H', 'macro_ms': FOUR_HOURS_MS,'lookback': 6, 'macro_volume_min': 1.10,'volume_bars_macro': 20, 'breakout_buffer_atr': 0.03,'macro_close_position_min': 0.70,'confirm_volume_min': 0.60, 'body_fraction': 0.70,'body_atr_min': 0.50, 'wick_directional': 0.15,'wick_opposite': 0.15, 'sl_lookback': 3,'sl_buffer_atr': 0.25, 'lifetime_seconds': 12 * 60 * 60,},
}

def strategy_for(symbol):
    return STRATEGIES['1H'] if symbol in ONE_HOUR_SYMBOLS else STRATEGIES['4H']

KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines'
WS_BASE_URL = 'wss://fstream.binance.com/market/stream?streams='
WS_STALE_SECONDS = 90.0
REST_FRESH_SECONDS = 90.0
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
                if
