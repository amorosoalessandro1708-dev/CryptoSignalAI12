import json
import os
import queue
import threading
import time
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
    'HYPEUSDT', 'ZECUSDT', 'XTZUSDT', 'CRVUSDT', 'STXUSDT',
    'ICPUSDT', 'SANDUSDT', 'POLUSDT', 'ALGOUSDT', 'LDOUSDT',
]
ONE_HOUR_SYMBOLS = frozenset(SYMBOLS)
FOUR_HOUR_SYMBOLS = frozenset()  # Non piu usato: tutte le 50 coin seguono la 1H
SCAN_SECONDS = 1.0
TIMEFRAME_SECONDS = 15 * 60
FIFTEEN_MIN_MS = 15 * 60 * 1000
ATR_PERIOD = 14
BTC_NEUTRAL_BODY_ATR_MAX = 0.35
BTC_TP1_VOLUME_MEDIAN_BARS_1H = 5
BTC_TP1_BLOCK_VOLUME_RATIO = 0.60
ENTRY_MAX_DISTANCE_ATR1H = 0.50
VOLUME_AVG_BARS_15M = 20
RETEST_VOLUME_RATIO_MIN = 1.50
RETEST_COUNTER_BTC1H_VOLUME_RATIO_MIN = 1.80  # Solo RETEST contro direzione BTC 1H chiusa
CONT_VOLUME_RATIO_MIN = 1.80
CONT_BREAKOUT_LOOKBACK_1H = 4  # Resistenza/supporto delle 4 candele 1H precedenti
CONT_SL_BUFFER_ATR1H = 0.15     # Buffer del livello 1H rotto, non delle 15m
RETEST_SL_BUFFER_ATR1H = CONT_SL_BUFFER_ATR1H  # RETEST: stesso SL su livello 1H
CONT_WICK_MAX_BODY = 0.15  # Ciascuna wick <= 15% del corpo: solo CONTINUAZIONE
LIVE_VOLUME_PACE_MIN_SECONDS = 60
LIVE_VOLUME_PROGRESS_LOG_SECONDS = 180
MICRO_CLOSE_BUFFER_ATR15 = 0.02
MACRO_CLOSE_GRACE_SECONDS = 1800
CLOSE_EVENT_MAX_DELAY_SECONDS = 30
TP1_R, TP2_R, TP3_R = 0.30, 0.80, 1.50
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
STRATEGIES = {
    '1H': {
        'name': '1H', 'macro_ms': ONE_HOUR_MS,
        'lookback': 4, 'breakout_buffer_atr': 0.04,
        'macro_close_position_min': 0.65,
        'confirm_volume_min': 0.60, 'body_fraction': 0.60,
        'body_atr_min': 0.60, 'wick_directional': 0.12,
        'wick_opposite': 0.25,
        'lifetime_seconds': 6 * 60 * 60,
    },

}
def strategy_for(symbol):
    return STRATEGIES['1H']
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
TELEGRAM_OUTBOX_FILE = os.getenv('TELEGRAM_OUTBOX_FILE', '/tmp/momentum_one_telegram_outbox.json')
TELEGRAM_RETRY_ROUND_SECONDS = 45
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
telegram_queue = queue.Queue()
persisted_pending = {}
dead_letters = {}
btc1h_timing = {'active_signal': None, 'blocked': False, 'blocked_at_ms': None,
                'last_unblock_macro_t': None,
                'tp1_wait_hour_t': None, 'tp1_last_checked_hour_t': None}
active_setups = {}
continuation_setups = {}  # Breakout 1H a 4 candele, indipendente dal RETEST
continuation_initialized = set()  # Ricostruzione dello storico una volta per simbolo
continuation_consumed = {}  # Ultimo breakout CONTINUAZIONE gia' utilizzato, per deduplica riavvii
seen_macro_bars = {}
last_mode_alert_15m = {}  # {symbol: 15m trigger already used for an alert}
def log(*parts):
    print(*parts, flush=True)
def _outbox_save_locked():
    try:
        folder = os.path.dirname(os.path.abspath(TELEGRAM_OUTBOX_FILE))
        os.makedirs(folder, exist_ok=True)
        payload = {
            'pending': list(persisted_pending.values()),
            'failed': list(dead_letters.values()),
            'sent': [[k[0], k[1]] for k in sent_alerts],
            'btc1h_timing': btc1h_timing,
            'continuation_consumed': continuation_consumed,
        }
        temp_file = TELEGRAM_OUTBOX_FILE + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as file:
            json.dump(payload, file, ensure_ascii=False)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_file, TELEGRAM_OUTBOX_FILE)
    except Exception as exc:
        log('TELEGRAM OUTBOX: scrittura non riuscita', repr(exc))
def load_telegram_outbox():
    try:
        with open(TELEGRAM_OUTBOX_FILE, 'r', encoding='utf-8') as file:
            payload = json.load(file)
    except FileNotFoundError:
        return
    except Exception as exc:
        log('TELEGRAM OUTBOX: lettura non riuscita', repr(exc))
        return
    now = time.time()
    with alert_lock:
        consumed = payload.get('continuation_consumed')
        if isinstance(consumed, dict):
            for symbol, breakout_t in consumed.items():
                if symbol in SYMBOLS and isinstance(breakout_t, int) and breakout_t > 0:
                    continuation_consumed[symbol] = breakout_t
        timing = payload.get('btc1h_timing')
        if isinstance(timing, dict):
            btc1h_timing['blocked'] = timing.get('blocked') is True
            btc1h_timing['blocked_at_ms'] = timing.get('blocked_at_ms')
            btc1h_timing['last_unblock_macro_t'] = timing.get('last_unblock_macro_t')
            wait_t = timing.get('tp1_wait_hour_t')
            checked_t = timing.get('tp1_last_checked_hour_t')
            btc1h_timing['tp1_wait_hour_t'] = wait_t if isinstance(wait_t, int) else None
            btc1h_timing['tp1_last_checked_hour_t'] = (
                checked_t if isinstance(checked_t, int) else None)
            active = timing.get('active_signal')
            if (isinstance(active, dict) and
                    active.get('direction') in ('LONG', 'SHORT') and
                    isinstance(active.get('tp1'), (int, float)) and
                    isinstance(active.get('sl'), (int, float)) and
                    isinstance(active.get('initial_high'), (int, float)) and
                    isinstance(active.get('initial_low'), (int, float)) and
                    isinstance(active.get('entry_bar_t'), int)):
                btc1h_timing['active_signal'] = active
            if btc1h_timing['blocked']:
                log('BTC1H TIMING: BLOCCO ALT RIPRISTINATO DALLA OUTBOX')
        for record in payload.get('sent', []):
            if isinstance(record, list) and len(record) == 2:
                key = (str(record[0]), int(record[1]))
                sent_alerts.add(key)
        for record in payload.get('failed', []):
            try:
                key = (str(record['symbol']), int(record['macro_t']))
                dead_letters[key] = record
            except (KeyError, ValueError, TypeError):
                continue
        for record in payload.get('pending', []):
            try:
                key = (str(record['symbol']), int(record['macro_t']))
                created = float(record['created_at'])
                msg = str(record['message'])
                if key in sent_alerts:
                    continue
                if now - created > TELEGRAM_DELIVERY_WINDOW_SECONDS:
                    old = dict(record)
                    old['reason'] = 'alert scaduto durante il riavvio; NON inviato in ritardo'
                    dead_letters[key] = old
                    log('TELEGRAM SCADUTO AL RIAVVIO', key[0], key[1])
                    continue
                persisted_pending[key] = record
                pending_alerts.add(key)
                telegram_queue.put_nowait((key, msg, created))
                log('TELEGRAM OUTBOX RIPRISTINATO', key[0], key[1])
            except (KeyError, ValueError, TypeError):
                continue
        _outbox_save_locked()
def telegram_send_once(text):
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
    ok, _, _ = telegram_send_once(text)
    return ok
def request_klines(symbol, limit=HISTORY_MAXLEN):
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
                params={'symbol': symbol, 'interval': '15m', 'limit': limit},
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
            min_bars = 3 if limit < ATR_PERIOD + 2 else ATR_PERIOD + 2
            if len(candles) < min_bars:
                raise ValueError(f'{symbol}: storico insufficiente')
            return candles
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            error = exc
        if attempt < len(REST_RETRY_WAIT):
            time.sleep(REST_RETRY_WAIT[attempt])
    raise RuntimeError(f'{symbol}: impossibile recuperare dati 15m ({error})')
def refresh_symbol(symbol, recent=False):
    with cache_lock:
        has_cache = bool(market_cache.get(symbol))
    use_recent = recent and has_cache
    candles = request_klines(symbol, limit=(REST_FALLBACK_RECENT_BARS
                                            if use_recent else HISTORY_MAXLEN))
    expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
    with cache_lock:
        old = market_cache.get(symbol)
        if old and old[-1]['t'] > candles[-1]['t']:
            return False
        if old and use_recent:
            old_by_t = {c['t']: c for c in old}
            if not any(c['t'] in old_by_t for c in candles):
                needs_refresh.add(symbol)
                return False
            by_t = dict(old_by_t)
            for candle in candles:
                previous = by_t.get(candle['t'])
                if previous is None or candle['v'] >= previous['v']:
                    by_t[candle['t']] = candle
            merged = [by_t[t] for t in sorted(by_t)][-HISTORY_MAXLEN:]
            if any(merged[i+1]['t'] - merged[i]['t'] != FIFTEEN_MIN_MS
                   for i in range(len(merged)-1)):
                needs_refresh.add(symbol)
                return False
            market_cache[symbol] = merged
        else:
            if old and old[-1]['t'] == candles[-1]['t'] and old[-1]['v'] > candles[-1]['v']:
                candles[-1] = old[-1]
            market_cache[symbol] = candles[-HISTORY_MAXLEN:]
        needs_refresh.discard(symbol)
        if candles[-1]['t'] == expected_t:
            last_rest_receive[symbol] = time.monotonic()
    return True
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
        expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
        with cache_lock:
            gaps = list(needs_refresh)
            missing = [s for s in SYMBOLS if s not in market_cache]
            fallback = []
            for symbol in SYMBOLS:
                if symbol not in market_cache or symbol in needs_refresh:
                    continue
                wrong_bar = market_cache[symbol][-1]['t'] != expected_t
                ws_quiet = (now - last_ws_receive.get(symbol, -1e9)
                            >= REST_FALLBACK_TRIGGER_SECONDS)
                rest_age = now - last_rest_receive.get(symbol, -1e9)
                if not wrong_bar and (not ws_quiet or rest_age < REST_FRESH_SECONDS):
                    continue
                min_wait = (REST_BOUNDARY_POLL_SECONDS if wrong_bar
                            else REST_FALLBACK_POLL_SECONDS)
                if now - last_rest_recovery_try.get(symbol, -1e9) < min_wait:
                    continue
                last_rest_recovery_try[symbol] = now
                fallback.append(symbol)
        if now - last_gap >= GAP_REFRESH_SECONDS:
            for symbol in set(gaps + missing):
                try:
                    refresh_symbol(symbol)
                except Exception as exc:
                    log('Recupero REST fallito:', symbol, exc)
            last_gap = now
        for symbol in fallback:
            try:
                if not refresh_symbol(symbol, recent=True):
                    pass
            except Exception as exc:
                log('Fallback feed REST fallito:', symbol, exc)
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
def median_volume(volumes):
    values = [max(0.0, float(v)) for v in volumes]
    return float(median(values)) if values else 0.0
def live_volume_progress(candles, now_ms, threshold):
    if len(candles) < VOLUME_AVG_BARS_15M + 1:
        return None
    live = candles[-1]
    elapsed_ms = now_ms - live['t']
    if not 0 <= elapsed_ms < FIFTEEN_MIN_MS:
        return None
    previous = candles[-(VOLUME_AVG_BARS_15M + 1):-1]
    avg = median_volume(c['v'] for c in previous)
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
def macro_closed_candles(candles_15m, macro_ms):
    required_count = macro_ms // FIFTEEN_MIN_MS
    groups = {}
    for candle in candles_15m[:-1]:
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
def macro_tail_complete_and_consecutive(bars, count, macro_ms):
    if len(bars) < count:
        return False
    tail = bars[-count:]
    return all(tail[i]['t'] + macro_ms == tail[i + 1]['t']
               for i in range(count - 1))
def entry_extension_1h(signal, candles):
    bars = macro_closed_candles(candles, ONE_HOUR_MS)
    if not macro_tail_complete_and_consecutive(bars, ATR_PERIOD + 1, ONE_HOUR_MS):
        return None
    atr1h = true_range_atr(bars)
    if atr1h is None or atr1h <= 0:
        return None
    sign = 1 if signal['direction'] == 'LONG' else -1
    extension = sign * (signal['entry'] - signal['breakout_level'])
    limit = ENTRY_MAX_DISTANCE_ATR1H * atr1h
    return {'allowed': extension <= limit + 1e-12,
            'extension': extension, 'limit': limit, 'atr1h': atr1h}
def record_btc1h_signal(signal, btc_candles):
    if signal['symbol'] != 'BTCUSDT' or signal['strategy'] != '1H':
        return
    direction = signal['direction']
    risk = abs(signal['entry'] - signal['sl'])
    tp1 = signal['entry'] + (1 if direction == 'LONG' else -1) * TP1_R * risk
    current = btc_candles[-1]
    with alert_lock:
        if btc1h_timing['blocked']:
            return
        if btc1h_timing['tp1_wait_hour_t'] is not None:
            return
        btc1h_timing['active_signal'] = {
            'direction': direction, 'tp1': tp1, 'sl': signal['sl'],
            'entry_bar_t': current['t'],
            'initial_high': current['h'], 'initial_low': current['l'],
            'macro_t': signal['macro_t'],
        }
        _outbox_save_locked()
    log('BTC1H TIMING ARMATO', direction, '| TP1', price_format(tp1))
def btc1h_first_target(signal, btc_candles, now_ms):
    entry_bar_t = signal['entry_bar_t']
    direction = signal['direction']
    tp1, sl = signal['tp1'], signal['sl']
    for bar in btc_candles:
        if bar['t'] < entry_bar_t:
            continue
        is_entry_bar = bar['t'] == entry_bar_t
        is_live = bar is btc_candles[-1]
        high, low, price = bar['h'], bar['l'], bar['c']
        if direction == 'LONG':
            tp_touch = (high >= tp1 and
                        (not is_entry_bar or signal['initial_high'] < tp1))
            sl_touch = (low <= sl and
                        (not is_entry_bar or signal['initial_low'] > sl))
            tp_touch |= is_live and price >= tp1
            sl_touch |= is_live and price <= sl
        else:
            tp_touch = (low <= tp1 and
                        (not is_entry_bar or signal['initial_low'] > tp1))
            sl_touch = (high >= sl and
                        (not is_entry_bar or signal['initial_high'] < sl))
            tp_touch |= is_live and price <= tp1
            sl_touch |= is_live and price >= sl
        if tp_touch or sl_touch:
            outcome = ('AMBIGUO' if tp_touch and sl_touch else
                       'TP1' if tp_touch else 'SL')
            return (outcome, now_ms if is_live else bar['t'] + FIFTEEN_MIN_MS,
                    bar['t'])
    return None
def btc1h_new_breakout_with_volume(btc_candles, blocked_at_ms):
    bars = macro_closed_candles(btc_candles, ONE_HOUR_MS)
    n = STRATEGIES['1H']['lookback']
    if not macro_tail_complete_and_consecutive(bars, n + 1, ONE_HOUR_MS):
        return None
    curr = bars[-1]
    close_t = curr['t'] + ONE_HOUR_MS
    if close_t <= blocked_at_ms:
        return None
    prev = bars[-(n + 1):-1]
    avg_volume = sum(bar['v'] for bar in prev) / n
    if curr['v'] <= avg_volume:
        return None
    if curr['c'] > max(c['h'] for c in prev):
        return curr['t'], 'LONG'
    if curr['c'] < min(c['l'] for c in prev):
        return curr['t'], 'SHORT'
    return None
def btc1h_breakout_during_tp1_wait(macro_bars, hour_index):
    n = STRATEGIES['1H']['lookback']
    if hour_index < n:
        return None
    window = macro_bars[hour_index - n:hour_index + 1]
    if not macro_tail_complete_and_consecutive(window, n + 1, ONE_HOUR_MS):
        return None
    current = window[-1]
    previous = window[:-1]
    avg_volume = sum(bar['v'] for bar in previous) / n
    if current['v'] <= avg_volume:
        return None
    if current['c'] > max(bar['h'] for bar in previous):
        return 'LONG'
    if current['c'] < min(bar['l'] for bar in previous):
        return 'SHORT'
    return None
def update_btc1h_timing(btc_candles, now_ms):
    with alert_lock:
        active = btc1h_timing['active_signal']
        pending_hour_t = btc1h_timing['tp1_wait_hour_t']
        if active and not btc1h_timing['blocked'] and pending_hour_t is None:
            outcome = btc1h_first_target(active, btc_candles, now_ms)
            if outcome is not None:
                result, hit_time, hit_bar_t = outcome
                if result == 'TP1':
                    pending_hour_t = (hit_bar_t // ONE_HOUR_MS) * ONE_HOUR_MS
                    btc1h_timing['tp1_wait_hour_t'] = pending_hour_t
                    btc1h_timing['tp1_last_checked_hour_t'] = None
                    log('BTC1H TP1 RAGGIUNTO | ALT 1H ANCORA LIBERE',
                        active['direction'], '| TP1', price_format(active['tp1']),
                        '| attendo chiusura 1H', pending_hour_t)
                else:
                    btc1h_timing['active_signal'] = None
                    log('BTC1H TIMING: TP1 NON CONFERMATO', result,
                        '| nessun nuovo blocco ALT')
                _outbox_save_locked()
        pending_hour_t = btc1h_timing['tp1_wait_hour_t']
        if not btc1h_timing['blocked'] and isinstance(pending_hour_t, int):
            bars = macro_closed_candles(btc_candles, ONE_HOUR_MS)
            n = BTC_TP1_VOLUME_MEDIAN_BARS_1H
            last_checked_t = btc1h_timing['tp1_last_checked_hour_t']
            for i in range(n, len(bars)):
                closed_hour = bars[i]
                hour_t = closed_hour['t']
                if hour_t < pending_hour_t or (last_checked_t is not None and
                                              hour_t <= last_checked_t):
                    continue
                breakout_direction = btc1h_breakout_during_tp1_wait(bars, i)
                if breakout_direction is not None:
                    btc1h_timing['active_signal'] = None
                    btc1h_timing['tp1_wait_hour_t'] = None
                    btc1h_timing['tp1_last_checked_hour_t'] = None
                    btc1h_timing['blocked'] = False
                    btc1h_timing['blocked_at_ms'] = None
                    _outbox_save_locked()
                    log('BTC1H BREAKOUT VALIDO DURANTE ATTESA TP1 | ATTESA ANNULLATA',
                        breakout_direction, '| candela 1H', hour_t,
                        '| ALT 1H LIBERE; vecchio TP1 invalidato')
                    break
                window = bars[i-n:i+1]
                if not macro_tail_complete_and_consecutive(window, n+1,
                                                           ONE_HOUR_MS):
                    continue
                base_volume = median_volume(c['v'] for c in window[:-1])
                if base_volume <= 0:
                    log('BTC1H TP1 VOLUME NON VALUTABILE', '| 1H', hour_t,
                        '| mediana 5 volumi non positiva')
                    continue
                ratio = closed_hour['v'] / base_volume
                btc1h_timing['tp1_last_checked_hour_t'] = hour_t
                last_checked_t = hour_t
                if ratio < BTC_TP1_BLOCK_VOLUME_RATIO:
                    btc1h_timing['blocked'] = True
                    btc1h_timing['blocked_at_ms'] = hour_t + ONE_HOUR_MS
                    btc1h_timing['tp1_wait_hour_t'] = None
                    btc1h_timing['tp1_last_checked_hour_t'] = None
                    log('BTC1H VOLUME BASSO DOPO TP1 | ALT 1H BLOCCATE',
                        '| volume', f'{ratio:.2f}x',
                        '| soglia', f'{BTC_TP1_BLOCK_VOLUME_RATIO:.2f}x',
                        '| candela 1H', hour_t)
                else:
                    log('BTC1H TP1 | ALT 1H LIBERE',
                        '| volume', f'{ratio:.2f}x',
                        '| soglia', f'{BTC_TP1_BLOCK_VOLUME_RATIO:.2f}x',
                        '| ricontrollo prossima 1H')
                _outbox_save_locked()
                if btc1h_timing['blocked']:
                    break
        if btc1h_timing['blocked']:
            blocked_at = btc1h_timing['blocked_at_ms']
            if not isinstance(blocked_at, (int, float)):
                return
            new_breakout = btc1h_new_breakout_with_volume(btc_candles, blocked_at)
            if new_breakout is not None:
                macro_t, direction = new_breakout
                if macro_t != btc1h_timing['last_unblock_macro_t']:
                    btc1h_timing['blocked'] = False
                    btc1h_timing['blocked_at_ms'] = None
                    btc1h_timing['last_unblock_macro_t'] = macro_t
                    btc1h_timing['active_signal'] = None
                    btc1h_timing['tp1_wait_hour_t'] = None
                    btc1h_timing['tp1_last_checked_hour_t'] = None
                    _outbox_save_locked()
                    log('BTC1H NUOVO BREAKOUT + VOLUME | ALT SBLOCCATE', direction,
                        '| candela 1H', macro_t)
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
def macro_breakout(symbol, candles, now_ms, cfg, btc_candles=None):
    macro_bars = macro_closed_candles(candles, cfg['macro_ms'])
    required = max(ATR_PERIOD + 2, cfg['lookback'] + 1)
    if len(macro_bars) < required:
        return None
    macro = macro_bars[-1]
    close_ms = macro['t'] + cfg['macro_ms']
    if not (0 <= now_ms - close_ms <= MACRO_CLOSE_GRACE_SECONDS * 1000):
        return None
    if not macro_tail_complete_and_consecutive(macro_bars, required, cfg['macro_ms']):
        return None
    prev = macro_bars[:-1]
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
    # RETEST: stesso SL della CONTINUAZIONE, ancorato al livello 1H rotto.
    # Il buffer ATR1H(14) viene fissato alla chiusura della candela breakout 1H.
    stop = (level - RETEST_SL_BUFFER_ATR1H * atr_macro if direction == 'LONG'
            else level + RETEST_SL_BUFFER_ATR1H * atr_macro)
    return {
        'symbol': symbol, 'direction': direction,
        'strategy': cfg['name'], 'macro_t': macro['t'],
        'macro_end_t': close_ms, 'level': level, 'macro_buffer': buffer,
        'atr_macro_at_breakout': atr_macro, 'macro_close_pos': close_pos, 'sl': stop,
        'last_checked_15m_t': None,
        'touched': False, 'touch_t': None, 'touched_price': None,
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
    avg = median_volume(c['v'] for c in previous)
    if avg <= 0:
        return 0.0
    return max(0.0, confirmation['v']) / avg
def confirmation_15m_valid(candle, direction, level, atr15, cfg, check_wicks=True):
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
    if not check_wicks:
        # Solo CONTINUAZIONE: non applicare il vecchio filtro wick 12%/25%.
        # Il limite autonomo 15% di ciascuna wick rispetto al CORPO resta obbligatorio.
        return True
    upper_wick = max(0.0, candle['h'] - max(opening, closing)) / span
    lower_wick = max(0.0, min(opening, closing) - candle['l']) / span
    leading = upper_wick if direction == 'LONG' else lower_wick
    trailing = lower_wick if direction == 'LONG' else upper_wick
    return leading <= cfg['wick_directional'] and trailing <= cfg['wick_opposite']
def btc_guard_on_closed15(btc_candles, confirmation_t, direction, symbol):
    """ALT: ultima BTC15 chiusa deve essere NEUTRA oppure CONCORDE.
    Per BTC stesso nessun confronto con se stesso.
    """
    if symbol == 'BTCUSDT':
        return 'NEUTRAL'
    bias = btc15_closed_direction(btc_candles, confirmation_t)
    if bias not in (direction, 'NEUTRAL'):
        return None
    return bias


def retest_btc1h_15m_filter(btc_candles, confirmation_t, direction):
    """Solo RETEST: BTC 1H e 15m dall'ultima candela CHIUSA (colore esatto).

    Con BTC1H o BTC1H neutro: BTC15 neutro/concorde, volume >= 1.50x.
    Contro BTC1H: BTC15 obbligatoriamente concorde, volume >= 1.80x.
    None = dati BTC 15m/1H ancora non sincronizzati o non completi.
    """
    if direction not in ('LONG', 'SHORT') or len(btc_candles) < 2:
        return None
    btc_15m = btc_candles[-2]
    if btc_15m['t'] != confirmation_t:
        return None
    close_t = confirmation_t + FIFTEEN_MIN_MS
    hourly = macro_closed_candles(btc_candles, ONE_HOUR_MS)
    if not hourly:
        return None
    btc_1h = hourly[-1]
    # Esige l'ultima ora COMPLETAMENTE chiusa, non un'ora parziale o vecchia.
    expected_1h_end_t = (close_t // ONE_HOUR_MS) * ONE_HOUR_MS
    if btc_1h['t'] + ONE_HOUR_MS != expected_1h_end_t:
        return None

    def candle_direction(candle):
        if candle['c'] > candle['o']:
            return 'LONG'
        if candle['c'] < candle['o']:
            return 'SHORT'
        return 'NEUTRAL'

    btc_1h_dir = candle_direction(btc_1h)
    btc_15m_dir = candle_direction(btc_15m)
    against_1h = btc_1h_dir in ('LONG', 'SHORT') and btc_1h_dir != direction
    threshold = (RETEST_COUNTER_BTC1H_VOLUME_RATIO_MIN if against_1h
                 else RETEST_VOLUME_RATIO_MIN)
    permitted_15m = (btc_15m_dir == direction if against_1h else
                     btc_15m_dir in (direction, 'NEUTRAL'))
    return {'allowed': permitted_15m, 'threshold': threshold,
            'btc1h': btc_1h_dir, 'btc15m': btc_15m_dir,
            'against_1h': against_1h}


def retest_confirmation(setup, candles, btc_candles, closed, now_ms, atr15):
    direction, level = setup['direction'], setup['level']
    cfg = STRATEGIES['1H']
    if not confirmation_15m_valid(closed, direction, level, atr15, cfg):
        log('RETEST SCARTATO', setup['symbol'], '| struttura 15m/corpo/ATR/wick')
        return None
    symbol = setup['symbol']
    btc_filter = retest_btc1h_15m_filter(btc_candles, closed['t'], direction)
    if btc_filter is None:
        return 'BTC_PENDING'
    if not btc_filter['allowed']:
        log('RETEST SCARTATO', symbol, '| BTC1H', btc_filter['btc1h'],
            '| BTC15m', btc_filter['btc15m'], '| direzione', direction)
        return None
    ratio = confirmation_15m_volume_ratio(candles)
    minimum_volume = btc_filter['threshold']
    if ratio < minimum_volume:
        log('RETEST SCARTATO', symbol, '| volume', f'{ratio:.2f}x',
            '| minimo', f'{minimum_volume:.2f}x',
            '| BTC1H', btc_filter['btc1h'], '| BTC15m', btc_filter['btc15m'])
        return None
    if symbol != 'BTCUSDT' and btc1h_timing['blocked']:
        log('RETEST BLOCCATO', symbol, '| BTC TP1 1H: aspetto sblocco')
        return None
    live = candles[-1]
    # Nessun ingresso se il prezzo e gia tornato nel range dopo la chiusura 15m.
    if not beyond_breakout(live['c'], direction, level, MICRO_CLOSE_BUFFER_ATR15 * atr15):
        return None
    sl = setup['sl']  # Livello 1H rotto +/- 0.15 ATR1H, fissato al breakout.
    price = live['c']
    risk = price - sl if direction == 'LONG' else sl - price
    if risk <= 0:
        return None
    signal = {
        'symbol': symbol, 'direction': direction, 'strategy': '1H',
        'entry_mode': 'RETEST', 'entry': price, 'sl': sl,
        'breakout_level': level, 'macro_t': setup['macro_t'],
        'confirm_15m_t': closed['t'], 'confirm_volume_ratio': ratio,
    }
    distance = entry_extension_1h(signal, candles)
    if not distance or not distance['allowed']:
        log('RETEST SCARTATO', symbol, '| ingresso oltre 0.50 ATR1H')
        return None
    return signal


def evaluate_retest(setup, candles, btc_candles, now_ms):
    if len(candles) < VOLUME_AVG_BARS_15M + 3:
        return 'WAIT', None
    live, closed = candles[-1], candles[-2]
    closed_end = closed['t'] + FIFTEEN_MIN_MS
    symbol, level, direction = setup['symbol'], setup['level'], setup['direction']
    if live['t'] != closed_end or closed['t'] < setup['macro_end_t']:
        return 'WAIT', None
    if now_ms - setup['macro_end_t'] > STRATEGIES['1H']['lifetime_seconds'] * 1000:
        return 'EXPIRED', None
    if setup.get('last_checked_15m_t') == closed['t']:
        return 'WAIT', None
    if now_ms < closed_end:
        return 'WAIT', None
    # Il tocco puo verificarsi nella stessa 15m che chiude con rimbalzo.
    touched_now = closed['l'] <= level <= closed['h']
    if touched_now and not setup['touched']:
        setup['touched'] = True
        setup['touch_t'] = closed['t']
        log('RETEST 15M', symbol, direction, '| contatto livello', price_format(level))
    if now_ms - closed_end > CLOSE_EVENT_MAX_DELAY_SECONDS * 1000:
        setup['last_checked_15m_t'] = closed['t']
        return 'WAIT', None
    if not setup['touched']:
        setup['last_checked_15m_t'] = closed['t']
        return 'WAIT', None
    atr15 = true_range_atr(candles[:-2])
    if atr15 is None or atr15 <= 0:
        return 'WAIT', None
    signal = retest_confirmation(setup, candles, btc_candles, closed, now_ms, atr15)
    if signal == 'BTC_PENDING':
        return 'WAIT', None
    setup['last_checked_15m_t'] = closed['t']
    return ('SIGNAL', signal) if signal else ('WAIT', None)


def trend_1h_direction(candles):
    """La 1H CHIUSA indica solo direzione (verde LONG, rossa SHORT).
    Nessun breakout macro e nessun volume macro richiesti.
    """
    bars = macro_closed_candles(candles, ONE_HOUR_MS)
    if len(bars) < ATR_PERIOD + 2 or not macro_tail_complete_and_consecutive(
            bars, ATR_PERIOD + 2, ONE_HOUR_MS):
        return None, None
    bar = bars[-1]
    return ('LONG' if bar['c'] > bar['o'] else
            'SHORT' if bar['c'] < bar['o'] else None), bar['t']


def continuation_wicks_ok(candle, direction):
    """SOLO prima 15m CHIUSA dopo breakout 1H: ogni wick <= 15% del CORPO.
    Il filtro NON si applica alle candele 15m successive o alla live d'ingresso.
    """
    if direction not in ('LONG', 'SHORT'):
        return False
    opening, closing = candle['o'], candle['c']
    if direction == 'LONG' and closing <= opening:
        return False
    if direction == 'SHORT' and closing >= opening:
        return False
    body = abs(closing - opening)
    if body <= 0:
        return False
    upper = max(0.0, candle['h'] - max(opening, closing))
    lower = max(0.0, min(opening, closing) - candle['l'])
    epsilon = max(1e-12, body * 1e-10)
    return (upper <= CONT_WICK_MAX_BODY * body + epsilon and
            lower <= CONT_WICK_MAX_BODY * body + epsilon)


def continuation_progresses(candle, direction, reference):
    """HH+HL per LONG; LH+LL per SHORT. Confronto con ultima candela VALIDA."""
    if direction == 'LONG':
        return candle['h'] > reference['h'] and candle['l'] > reference['l']
    return candle['h'] < reference['h'] and candle['l'] < reference['l']


def continuation_hour_breakout(candles):
    """Breakout CONTINUAZIONE della 1H chiusa sulle 4 precedenti.

    La rottura e' valida solo dopo la CHIUSURA della candela 1H:
    LONG sopra il massimo delle ultime quattro 1H precedenti;
    SHORT sotto il minimo delle ultime quattro 1H precedenti.
    Si conservano buffer breakout 0.04 ATR1H e close position 65% della V7.2.
    ATR1H(14) e' calcolato su candele 1H chiuse precedenti alla rottura.
    """
    bars = macro_closed_candles(candles, ONE_HOUR_MS)
    cfg = STRATEGIES['1H']
    required = max(ATR_PERIOD + 2, CONT_BREAKOUT_LOOKBACK_1H + 1)
    if not macro_tail_complete_and_consecutive(bars, required, ONE_HOUR_MS):
        return None
    bar = bars[-1]
    previous = bars[:-1]
    direction = ('LONG' if bar['c'] > bar['o'] else
                 'SHORT' if bar['c'] < bar['o'] else None)
    if direction is None:
        return None
    atr1h = true_range_atr(previous)
    if atr1h is None or atr1h <= 0:
        return None
    previous_four = previous[-CONT_BREAKOUT_LOOKBACK_1H:]
    level = (max(x['h'] for x in previous_four) if direction == 'LONG'
             else min(x['l'] for x in previous_four))
    if not beyond_breakout(bar['c'], direction, level,
                           cfg['breakout_buffer_atr'] * atr1h):
        return None
    span = bar['h'] - bar['l']
    if span <= 0:
        return None
    close_position = ((bar['c'] - bar['l']) / span if direction == 'LONG'
                      else (bar['h'] - bar['c']) / span)
    if close_position < cfg['macro_close_position_min']:
        return None
    # SL LONG sotto la resistenza rotta; SL SHORT sopra il supporto rotto.
    stop = (level - CONT_SL_BUFFER_ATR1H * atr1h if direction == 'LONG'
            else level + CONT_SL_BUFFER_ATR1H * atr1h)
    return {
        'direction': direction,
        'macro_t': bar['t'], 'macro_end_t': bar['t'] + ONE_HOUR_MS,
        'level': level, 'sl': stop, 'atr1h': atr1h,
    }


def continuation_first_15m_closed(candles, direction):
    """Solo la prima 15m CHIUSA dopo il breakout 1H puo' confermarlo.

    La sua chiusura resta oltre il livello 1H rotto e le sue due wick
    rispettano 15% del CORPO. La candela che invia il segnale piu' tardi
    NON ha un nuovo filtro wick.
    """
    if len(candles) < ATR_PERIOD + 3:
        return None
    live, closed = candles[-1], candles[-2]
    if live['t'] != closed['t'] + FIFTEEN_MIN_MS:
        return None
    breakout = continuation_hour_breakout(candles)
    if breakout is None or breakout['direction'] != direction:
        return None
    # La prima 15m successiva inizia esattamente alla chiusura breakout 1H.
    if closed['t'] != breakout['macro_end_t']:
        return None
    atr15 = true_range_atr(candles[:-2])
    if atr15 is None or atr15 <= 0:
        return None
    if not confirmation_15m_valid(closed, direction, breakout['level'],
                                   atr15, STRATEGIES['1H'], check_wicks=False):
        return None
    if not continuation_wicks_ok(closed, direction):
        return None
    breakout['breakout_t'] = closed['t']
    breakout['atr15'] = atr15
    return breakout


def create_continuation_setup(candles, direction, now_ms):
    """Ancora la continuazione al livello 1H rotto e alla prima 15m valida."""
    confirmation = continuation_first_15m_closed(candles, direction)
    if confirmation is None:
        return None
    initial = candles[-2]
    return {
        'direction': direction, 'level': confirmation['level'],
        'macro_breakout_t': confirmation['macro_t'],
        'breakout_t': confirmation['breakout_t'],
        'atr15': confirmation['atr15'], 'sl': confirmation['sl'],
        'last_processed_t': confirmation['breakout_t'],
        'last_good': {'t': initial['t'], 'h': initial['h'], 'l': initial['l']},
        'pending_recovery': False, 'sent': False,
    }


def advance_continuation_setup(setup, closed):
    """Processa una nuova candela CHIUSA dopo il breakout.

    - progressione: aggiorna l'ultima candela valida;
    - prima interruzione: NON cancella, mantiene la candela valida come riferimento;
    - candela successiva: deve recuperare HH+HL / LH+LL rispetto a quel
      riferimento, altrimenti setup annullato.
    """
    if closed['t'] != setup['last_processed_t'] + FIFTEEN_MIN_MS:
        return False
    direction = setup['direction']
    if not beyond_breakout(closed['c'], direction, setup['level'],
                           MICRO_CLOSE_BUFFER_ATR15 * setup['atr15']):
        return False
    if continuation_progresses(closed, direction, setup['last_good']):
        setup['last_good'] = {'t': closed['t'], 'h': closed['h'], 'l': closed['l']}
        setup['pending_recovery'] = False
    elif setup['pending_recovery']:
        return False
    else:
        setup['pending_recovery'] = True
    setup['last_processed_t'] = closed['t']
    return True


def rebuild_continuation_setup(symbol, candles, direction, now_ms):
    """Ricostruisce una continuazione nata da breakout 1H + prima 15m valida.
    Non invia alert retroattivi: controlla solo la candela live corrente.
    """
    setup = None
    if len(candles) < max(VOLUME_AVG_BARS_15M + 3, ATR_PERIOD + 3):
        return None
    for pos in range(max(VOLUME_AVG_BARS_15M + 1, ATR_PERIOD + 1), len(candles)-1):
        # pos = indice della candela 15m chiusa da considerare
        closed = candles[pos]
        if setup is not None:
            if not advance_continuation_setup(setup, closed):
                setup = None
        if setup is None:
            # Possibile 1H chiusa in breakout + prima 15m successiva valida.
            prefix = candles[:pos+2]
            candidate = create_continuation_setup(prefix, direction, now_ms)
            if candidate:
                trigger_trend, _ = trend_1h_direction(prefix)
                if trigger_trend == direction:
                    setup = candidate
                    setup['sent'] = (continuation_consumed.get(symbol) == setup['breakout_t'])
    return setup


def update_continuation_setup(symbol, candles, direction, now_ms):
    """Mantiene il breakout attivo per qualunque numero di 15m, finche' valido."""
    closed_t = candles[-2]['t']
    setup = continuation_setups.get(symbol)
    if symbol not in continuation_initialized:
        continuation_initialized.add(symbol)
        setup = rebuild_continuation_setup(symbol, candles, direction, now_ms)
        if setup:
            continuation_setups[symbol] = setup
            log('CONTINUAZIONE RIPRISTINATA', symbol, direction,
                '| breakout', setup['breakout_t'])
        else:
            continuation_setups.pop(symbol, None)
        return setup
    if setup and setup['direction'] != direction:
        continuation_setups.pop(symbol, None)
        setup = None
    if setup and setup['last_processed_t'] < closed_t:
        previous_t = setup['last_processed_t']
        new_closed = [c for c in candles[:-1] if previous_t < c['t'] <= closed_t]
        for closed in new_closed:
            if not advance_continuation_setup(setup, closed):
                log('CONTINUAZIONE ANNULLATA', symbol, direction,
                    '| progressione non recuperata o prezzo rientrato', closed['t'])
                continuation_setups.pop(symbol, None)
                setup = None
                break
    if setup is None:
        setup = create_continuation_setup(candles, direction, now_ms)
        if setup:
            setup['sent'] = (continuation_consumed.get(symbol) == setup['breakout_t'])
            continuation_setups[symbol] = setup
            log('BREAKOUT 1H CONTINUAZIONE', symbol, direction,
                '| 1H', setup['macro_breakout_t'], '| prima 15m', setup['breakout_t'],
                '| livello', price_format(setup['level']), '| stop', price_format(setup['sl']))
    return setup


def evaluate_continuation(symbol, candles, btc_candles, now_ms):
    """Qualunque 15m LIVE dopo breakout 1H e prima 15m confermata; 1 pausa."""
    if len(candles) < VOLUME_AVG_BARS_15M + 3:
        return None
    trend, hour_t = trend_1h_direction(candles)
    if trend is None:
        continuation_setups.pop(symbol, None)
        return None
    setup = update_continuation_setup(symbol, candles, trend, now_ms)
    if setup is None or setup['sent']:
        return None
    live = candles[-1]
    if live['t'] <= setup['breakout_t'] or live['t'] != setup['last_processed_t'] + FIFTEEN_MIN_MS:
        return None
    direction = setup['direction']
    price = live['c']
    if not beyond_breakout(price, direction, setup['level'],
                           MICRO_CLOSE_BUFFER_ATR15 * setup['atr15']):
        return None
    # Ogni 15m successiva deve riprendere rispetto all'ultima candela valida.
    # Quando la precedente ha interrotto la progressione, il confronto
    # rimane con la candela PRIMA dell'interruzione.
    if not continuation_progresses(live, direction, setup['last_good']):
        return None
    vol_info = live_volume_progress(candles, now_ms, CONT_VOLUME_RATIO_MIN)
    if not vol_info or vol_info['actual_ratio'] < CONT_VOLUME_RATIO_MIN:
        return None
    if symbol != 'BTCUSDT' and btc_candles[-2]['t'] != candles[-2]['t']:
        return None
    btc_dir = btc_guard_on_closed15(btc_candles, btc_candles[-2]['t'],
                                    direction, symbol)
    if btc_dir is None:
        return None
    if symbol != 'BTCUSDT' and btc1h_timing['blocked']:
        return None
    sl = setup['sl']
    risk = price - sl if direction == 'LONG' else sl - price
    if risk <= 0:
        return None
    # Manteniamo la deduplica V7: un alert per coppia per ora 1H.
    signal = {
        'symbol': symbol, 'direction': direction, 'strategy': '1H',
        'entry_mode': 'CONTINUAZIONE', 'entry': price, 'sl': sl,
        'breakout_level': setup['level'], 'macro_t': hour_t,
        'confirm_15m_t': live['t'], 'confirm_volume_ratio': vol_info['actual_ratio'],
        'micro_break_t': setup['breakout_t'],  # Identificatore 15m, deduplica conservata
        'macro_breakout_1h_t': setup['macro_breakout_t'],
    }
    distance = entry_extension_1h(signal, candles)
    if not distance or not distance['allowed']:
        return None
    return signal

def price_format(price):
    if price >= 1000:
        return f'{price:.2f}'
    if price >= 1:
        return f'{price:.4f}'
    if price >= 0.01:
        return f'{price:.6f}'
    return f'{price:.9f}'
def message_for(s):
    cfg = strategy_for(s['symbol'])
    direction = s['direction']
    icon = '🟢' if direction == 'LONG' else '🔴'
    d = 1 if direction == 'LONG' else -1
    risk = abs(s['entry'] - s['sl'])
    tp1 = s['entry'] + d * TP1_R * risk
    tp2 = s['entry'] + d * TP2_R * risk
    tp3 = s['entry'] + d * TP3_R * risk
    pair = s['symbol'].replace('USDT', '/USDT')
    return (
        f"{icon} {direction} | {pair} | {cfg['name']} | {s['entry_mode']}\n\n"
        f"Entry: {price_format(s['entry'])}\n"
        f"Stop Loss: {price_format(s['sl'])}\n"
        f"TP1: {price_format(tp1)}\n"
        f"TP2: {price_format(tp2)}\n"
        f"TP3: {price_format(tp3)}"
    )
def enqueue_alert(signal):
    key = (signal['symbol'], signal['macro_t'])
    message = message_for(signal)
    with alert_lock:
        if key in sent_alerts or key in pending_alerts or key in dead_letters:
            log('TELEGRAM DUPLICATO/ARCHIVIATO', key[0], key[1])
            return False
        created_at = time.time()
        record = {'symbol': key[0], 'macro_t': key[1],
                  'message': message, 'created_at': created_at}
        pending_alerts.add(key)
        persisted_pending[key] = record
        _outbox_save_locked()
        telegram_queue.put_nowait((key, message, created_at))
    log('TELEGRAM IN CODA', key[0], '| breakout', key[1])
    return True
def telegram_worker():
    while True:
        key, message, created_at = telegram_queue.get()
        delivered = False
        last_error = 'nessuna risposta positiva Telegram'
        try:
            attempt = 0
            while time.time() - created_at <= TELEGRAM_DELIVERY_WINDOW_SECONDS:
                attempt += 1
                try:
                    ok, retryable, retry_after = telegram_send_once(message)
                except Exception as exc:
                    log('TELEGRAM WORKER ECCEZIONE', key[0], repr(exc))
                    ok, retryable, retry_after = False, True, 0
                if ok:
                    delivered = True
                    with alert_lock:
                        sent_alerts.add(key)
                        persisted_pending.pop(key, None)
                        pending_alerts.discard(key)
                        dead_letters.pop(key, None)
                        _outbox_save_locked()
                    log('TELEGRAM OK', key[0], '| breakout', key[1],
                        '| tentativo', attempt)
                    break
                if not retryable:
                    last_error = 'Telegram ha rifiutato definitivamente la richiesta (HTTP 4xx)'
                    break
                if attempt >= TELEGRAM_MAX_ATTEMPTS:
                    delay = TELEGRAM_RETRY_ROUND_SECONDS
                    attempt = 0
                else:
                    delay = min(TELEGRAM_RETRY_MAX_SECONDS,
                                TELEGRAM_RETRY_INITIAL_SECONDS * (2 ** (attempt - 1)))
                delay = max(float(retry_after), delay)
                remaining = TELEGRAM_DELIVERY_WINDOW_SECONDS - (time.time() - created_at)
                if remaining <= 0 or delay >= remaining:
                    last_error = 'finestra consegna 5 minuti esaurita'
                    break
                log('TELEGRAM RETRY', key[0], '| prossimo tentativo fra',
                    round(delay), 'sec')
                time.sleep(delay)
            if not delivered:
                record = {'symbol': key[0], 'macro_t': key[1],
                          'message': message, 'created_at': created_at,
                          'reason': last_error, 'failed_at': time.time()}
                with alert_lock:
                    persisted_pending.pop(key, None)
                    pending_alerts.discard(key)
                    dead_letters[key] = record
                    failed_alert_times[key] = time.monotonic()
                    _outbox_save_locked()
                log('TELEGRAM NON CONSEGNATO - SALVATO IN OUTBOX FALLITI',
                    key[0], key[1], '|', last_error)
        finally:
            telegram_queue.task_done()
def snapshot_with_sources():
    now = time.monotonic()
    expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
    fresh_ws, fresh_rest, obsolete = 0, 0, 0
    snapshot = {}
    with cache_lock:
        for symbol in SYMBOLS:
            candles = market_cache.get(symbol)
            if (not candles or symbol in needs_refresh or
                    candles[-1]['t'] != expected_t):
                obsolete += 1
                continue
            ws_ok = (websocket_connected and
                     now - last_ws_receive.get(symbol, -1e9) <= WS_STALE_SECONDS)
            rest_ok = now - last_rest_receive.get(symbol, -1e9) <= REST_FRESH_SECONDS
            if ws_ok:
                fresh_ws += 1
            elif rest_ok:
                fresh_rest += 1
            else:
                obsolete += 1
                continue
            snapshot[symbol] = list(candles)
    return snapshot, fresh_ws, fresh_rest, obsolete
def current_snapshot():
    return snapshot_with_sources()[0]
def scan_market():
    snapshot, _, _, _ = snapshot_with_sources()
    if 'BTCUSDT' not in snapshot:
        return 0
    btc_candles = snapshot['BTCUSDT']
    now_ms = int(time.time() * 1000)
    if not (btc_candles[-1]['t'] <= now_ms < btc_candles[-1]['t'] + FIFTEEN_MIN_MS):
        return 0
    update_btc1h_timing(btc_candles, now_ms)
    queued = 0
    cfg = STRATEGIES['1H']
    for symbol, candles in snapshot.items():
        if candles[-1]['t'] != btc_candles[-1]['t']:
            continue
        bars = macro_closed_candles(candles, ONE_HOUR_MS)
        if not bars:
            continue
        macro_t = bars[-1]['t']
        setup = active_setups.get(symbol)
        if setup and now_ms - setup['macro_end_t'] > cfg['lifetime_seconds'] * 1000:
            del active_setups[symbol]
            setup = None
        if seen_macro_bars.get(symbol) != macro_t:
            required = max(ATR_PERIOD + 2, cfg['lookback'] + 1)
            if macro_tail_complete_and_consecutive(bars, required, ONE_HOUR_MS):
                if not setup:
                    candidate = macro_breakout(symbol, candles, now_ms, cfg, btc_candles)
                    if candidate:
                        active_setups[symbol] = candidate
                        setup = candidate
                        log('BREAKOUT 1H RETEST', symbol, candidate['direction'])
                seen_macro_bars[symbol] = macro_t
        # RETEST: tocco del livello e conferma della 15m a volume 1.50x.
        setup = active_setups.get(symbol)
        retest_signal = None
        if setup:
            status, retest_signal = evaluate_retest(setup, candles, btc_candles, now_ms)
            if status == 'EXPIRED':
                del active_setups[symbol]
            elif status == 'SIGNAL' and retest_signal:
                if enqueue_alert(retest_signal):
                    queued += 1
                    last_mode_alert_15m[symbol] = retest_signal['confirm_15m_t']
                    if symbol == 'BTCUSDT':
                        record_btc1h_signal(retest_signal, btc_candles)
                    del active_setups[symbol]
                    log('SEGNALE RETEST IN CODA', symbol, retest_signal['direction'])
                else:
                    log('RETEST NON ACCODATO', symbol)
        # CONTINUAZIONE: breakout della 1H chiusa sulle 4 precedenti,
        # prima 15m successiva chiusa oltre il livello con wick <=15% corpo;
        # ogni 15m live successiva puo' entrare a volume reale >=1.80x,
        # HH+HL / LH+LL e una interruzione con recupero immediato.
        cont_signal = evaluate_continuation(symbol, candles, btc_candles, now_ms)
        if cont_signal:
            accepted = enqueue_alert(cont_signal)
            # Anche se la deduplica blocca una seconda modalita' nella stessa
            # ora, non riproponiamo a ogni secondo questo medesimo breakout.
            if symbol in continuation_setups:
                continuation_setups[symbol]['sent'] = True
            continuation_consumed[symbol] = cont_signal['micro_break_t']
            with alert_lock:
                _outbox_save_locked()
            if accepted:
                queued += 1
                last_mode_alert_15m[symbol] = cont_signal['confirm_15m_t']
                if symbol == 'BTCUSDT':
                    record_btc1h_signal(cont_signal, btc_candles)
                log('SEGNALE CONTINUAZIONE IN CODA', symbol, cont_signal['direction'])
    oldest_allowed = now_ms - 4 * FOUR_HOURS_MS
    with alert_lock:
        recent = {k for k in sent_alerts if k[1] >= oldest_allowed}
        if recent != sent_alerts:
            sent_alerts.intersection_update(recent)
            _outbox_save_locked()
    return queued

def main():
    log('MOMENTUM ONE V7.4.1 | 50 crypto tutte 1H | RETEST BTC1H 1.50x/1.80x | CONTINUAZIONE live 1.80x')
    log('1H RETEST: breakout 4x1H + rimbalzo 15m chiuso; volume 1.50x concorde/neutro BTC1H, 1.80x contro BTC1H; SL livello 1H +/-0.15 ATR1H')
    log('CONTINUAZIONE: breakout 1H sulle 4 precedenti + prima 15m chiusa oltre livello; volume live 1.80x su qualunque successiva')
    log('STRUTTURA CONTINUAZIONE: HH+HL o LH+LL; una pausa con recupero immediato; wick <=15% corpo SOLO prima 15m chiusa, senza vecchio filtro 12%/25%')
    log('SL CONTINUAZIONE: livello di resistenza/supporto 1H rotto +/- 0.15 ATR1H | TP 0.30R/0.80R/1.50R')
    log('BTC RETEST: BTC1H/15m da colore ultime chiuse; contro BTC1H volume 1.80x e BTC15 direzionale; gate TP1 invariato')
    if not BOT_TOKEN or not CHAT_ID:
        log('ATTENZIONE: mancano variabili Telegram')
    load_telegram_outbox()
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
    if send_telegram('MOMENTUM ONE V7.4.1 AVVIATO [SIMULAZIONE] - '
                     '50 crypto tutte 1H | RETEST 4x1H BTC1H con/contro volume 1.50x/1.80x | '
                     'CONTINUAZIONE breakout 4x1H + 15m e volume live 1.80x | '
                     'BTC neutro/concorde | TP invariati | nessun ordine automatico'):
        log('TELEGRAM AVVIO OK')
    else:
        log('TELEGRAM AVVIO NON CONSEGNATO - controllare token/chat/rete')
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
            snap, ws_count, rest_count, obsolete = snapshot_with_sources()
            b = btc15_direction(snap['BTCUSDT']) if 'BTCUSDT' in snap else 'DATI NON FRESCHI'
            with alert_lock:
                in_delivery = len(pending_alerts)
                failures = len(dead_letters)
            log('Scansione ~1s | feed attivi:', len(snap), '/50',
                f'| WS: {ws_count} | REST: {rest_count} | obsoleti: {obsolete}',
                '| BTC15 live:', b, '| setup RETEST 1H:', len(active_setups),
                '| Telegram in coda:', in_delivery, '| Telegram falliti:', failures)
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()

if __name__ == '__main__':
    main()
