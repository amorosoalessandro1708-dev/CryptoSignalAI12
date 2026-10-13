import os
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

# ==========================================================
# V8 - MOMENTUM DIREZIONALE, HOLD 5 MINUTI
# 25 futures USDT; solo breakout della struttura 15m.
# BTC15 direzionale + BTC1H + bias BTC + altcoin 1H concordi.
# EMA20 15m: prezzo concorde e EMA20 inclinata nella direzione.
# EMA50 15m informativa, NON bloccante. EMA20/50 1H INVARIATE.
# Stocastico 15m favorevole all'avvio e MIGLIORATO dopo 300s.
# NESSUN ingresso anticipato / breakout esplosivo.
# BTC30 e 4H informativi. Un solo messaggio Telegram.
# ==========================================================
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
SYMBOLS = [
    'SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT',
    'BNBUSDT', 'DOGEUSDT', 'AVAXUSDT', 'SUIUSDT',
    'LINKUSDT', 'ADAUSDT', 'NEARUSDT', 'UNIUSDT',
    'LTCUSDT', 'DOTUSDT', 'APTUSDT', 'ARBUSDT',
    'OPUSDT', 'INJUSDT', 'RENDERUSDT', '1000PEPEUSDT',
    'FILUSDT', 'ETCUSDT', 'AAVEUSDT', 'TIAUSDT', 'SEIUSDT'
]
REST = 'https://fapi.binance.com'
KLINES_URL = REST + '/fapi/v1/klines'
SCAN_SECONDS = 5  # Target; una scansione puo' durare piu' di 5s.
CACHE_30M_SECONDS = 30
CACHE_1H_SECONDS = 60  # Solo candele CHIUSE; aggiorniamo dopo nuova chiusura.
CACHE_4H_SECONDS = 300
MIN_REST_GAP_SECONDS = 0.30
MAX_RETRIES = 4
RETRY_FALLBACK_SECONDS = (5, 10, 20, 30)
SCAN_WORKERS = 6
DIAGNOSTIC_LOGS = True
DIAGNOSTIC_EVERY_SECONDS = 60

# V8: tutte le condizioni devono restare valide dopo 5 minuti.
HOLD_SECONDS = 300
HOLD_LEVEL_TOLERANCE_ATR = 0.00  # Rientro osservato sotto/sopra livello => annulla.
HOLD_MAX_GAP_SECONDS = 40  # Se non abbiamo osservazioni sufficienti, annulla.
HOLD_PRICE_PROGRESS_MIN_ATR = 0.025
HOLD_EMA_IMPROVEMENT_MIN_ATR = 0.005
HOLD_STOCH_SPREAD_IMPROVEMENT_MIN = 0.5  # Punti su scala 0..100.
HOLD_MAX_EXTENSION_ATR = 0.90
BREAKOUT_MIN_ATR = 0.06
BREAKOUT_START_MAX_EXTENSION_ATR = 0.45
BREAKOUT_VOLUME_PACE_MIN = 1.20
BREAKOUT_BODY_ATR_MIN = 0.25
BREAKOUT_CLOSE_POSITION_MIN = 0.68
BREAKOUT_WICK_MAX = 0.55
MIN_CANDLE_ELAPSED_SECONDS = 30
LIVE_VOLUME_MIN_ELAPSED_SECONDS = 15
LIVE_VOLUME_PACE_CAP = 20.0
CLOSED_VOLUME_PREFERRED = 1.10
STRUCTURE_BARS = 6
STOCH_K_PERIOD = 14
STOCH_K_SMOOTH = 3
STOCH_D_PERIOD = 3
STOCH_MIN_SPREAD = 0.0
# I filtri di trend 1H usano EMA20 e EMA50 chiuse (INVARIATI).
BTC15_DIRECTIONAL_BODY_ATR_MIN = 0.15

# Gestione rischio V7 invariata (con prezzi di ingresso aggiornati).
SL_STRUCTURE_LOOKBACK_BARS = 4
SL_BUFFER_STRONG_TREND_ATR15 = 0.18
SL_BUFFER_NORMAL_TREND_ATR15 = 0.14
SL_BUFFER_NEUTRAL_4H_ATR15 = 0.10
SL_MIN_DISTANCE_ATR15 = 0.55
SL_MAX_DISTANCE_ATR15 = 1.80
TP_MIN_NET_PCT = 0.0015
LEVERAGE_SAFETY = 0.35
LEVERAGE_STEPS = (1, 2, 3, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100)

request_lock = threading.Lock()
cache_lock = threading.Lock()
last_rest_request = 0.0
market_cache = {}
pending = {}  # In memoria: riavviare Railway azzera gli HOLD.
signal_state = {}
last_diag = 0.0


def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print('Telegram non configurato')
        return False
    try:
        response = requests.post(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            data={'chat_id': CHAT_ID, 'text': message}, timeout=20)
        response.raise_for_status()
        result = response.json()
        if not result.get('ok'):
            raise RuntimeError(str(result))
        return True
    except Exception as exc:
        print('Errore Telegram:', exc)
        return False


def get_json(url, params):
    global last_rest_request
    last_error = None
    for attempt in range(MAX_RETRIES):
        with request_lock:
            remaining = MIN_REST_GAP_SECONDS - (time.monotonic() - last_rest_request)
            if remaining > 0:
                time.sleep(remaining)
            last_rest_request = time.monotonic()
        try:
            # Sessione per richiesta: evita accesso concorrente a Session condivisa.
            response = requests.get(url, params=params, timeout=15)
            if response.status_code in (418, 429):
                retry = response.headers.get('Retry-After')
                try:
                    wait = max(float(retry), 5.0)
                except (TypeError, ValueError):
                    wait = RETRY_FALLBACK_SECONDS[min(attempt, 3)]
                print('Binance rate limit:', response.status_code, 'attesa', wait)
                time.sleep(wait)
                last_error = RuntimeError(f'HTTP {response.status_code}')
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_FALLBACK_SECONDS[min(attempt, 3)])
    raise last_error or RuntimeError('Binance non disponibile')


def parse_candles(raw):
    return [{'o': float(x[1]), 'h': float(x[2]), 'l': float(x[3]),
             'c': float(x[4]), 'v': float(x[5]), 't': int(x[0])}
            for x in raw]


def cached_candles(symbol, interval, lifetime):
    key = (symbol, interval)
    now = time.time()
    with cache_lock:
        item = market_cache.get(key)
        if item and now - item['time'] < lifetime:
            return item['data']
    data = parse_candles(get_json(KLINES_URL, {
        'symbol': symbol, 'interval': interval, 'limit': 120}))
    with cache_lock:
        market_cache[key] = {'time': time.time(), 'data': data}
    return data


def market_data(symbol):
    return {
        '15m': parse_candles(get_json(KLINES_URL, {
            'symbol': symbol, 'interval': '15m', 'limit': 120})),
        '1h': cached_candles(symbol, '1h', CACHE_1H_SECONDS),
        '4h': cached_candles(symbol, '4h', CACHE_4H_SECONDS),
    }


def ema(values, period):
    if len(values) < period:
        return None
    result = sum(values[:period]) / period
    k = 2 / (period + 1)
    for value in values[period:]:
        result = (value - result) * k + result
    return result


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    ranges = [max(candles[i]['h'] - candles[i]['l'],
                  abs(candles[i]['h'] - candles[i - 1]['c']),
                  abs(candles[i]['l'] - candles[i - 1]['c']))
              for i in range(1, len(candles))]
    return sum(ranges[-period:]) / period


def live_volume_ratio(candles):
    if len(candles) < 22:
        return 0.0, 0.0
    previous = candles[-21:-1]
    average = sum(c['v'] for c in previous) / len(previous)
    live = candles[-1]
    elapsed = max(0.0, min((time.time() * 1000 - live['t']) / 1000, 900.0))
    if average <= 0 or elapsed < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return 0.0, elapsed
    expected = average * max(elapsed / 900.0, 0.01)
    return min(live['v'] / expected, LIVE_VOLUME_PACE_CAP), elapsed


def closed_volume_ratio(candles):
    if len(candles) < 22:
        return 0.0
    average = sum(c['v'] for c in candles[-22:-2]) / 20
    return candles[-2]['v'] / average if average > 0 else 0.0


def candle_metrics(candle, direction, a15):
    body = abs(candle['c'] - candle['o'])
    span = candle['h'] - candle['l']
    if body <= 0 or span <= 0 or a15 <= 0:
        return None
    if direction == 'LONG':
        if candle['c'] <= candle['o']:
            return None
        close_pos = (candle['c'] - candle['l']) / span
        wick = max(0.0, candle['h'] - candle['c']) / body
    else:
        if candle['c'] >= candle['o']:
            return None
        close_pos = (candle['h'] - candle['c']) / span
        wick = max(0.0, candle['c'] - candle['l']) / body
    return {'body': body / a15, 'close': close_pos, 'wick': wick}


def trend_1h(candles):
    closes = [c['c'] for c in candles[:-1]]
    e20, e50 = ema(closes, 20), ema(closes, 50)
    previous = ema(closes[:-1], 20)
    if None in (e20, e50, previous):
        return 'NEUTRAL'
    price = candles[-2]['c']
    if price > e20 > e50 and e20 >= previous:
        return 'LONG'
    if price < e20 < e50 and e20 <= previous:
        return 'SHORT'
    return 'NEUTRAL'


def context_4h(candles):
    closes = [c['c'] for c in candles[:-1]]
    e20, e50 = ema(closes, 20), ema(closes, 50)
    if e20 is None or e50 is None:
        return 'NEUTRAL'
    price = candles[-2]['c']
    if price > e20 > e50:
        return 'LONG'
    if price < e20 < e50:
        return 'SHORT'
    return 'NEUTRAL'


def btc_bias(data):
    one, four = data['1h'], data['4h']
    close1 = [c['c'] for c in one[:-1]]
    close4 = [c['c'] for c in four[:-1]]
    a, b = ema(close1, 20), ema(close1, 50)
    c, d = ema(close4, 20), ema(close4, 50)
    previous = ema(close1[:-1], 20)
    if None in (a, b, c, d, previous):
        return 'NEUTRAL_STABLE'
    if one[-2]['c'] > a > b and a > previous and four[-2]['c'] > c:
        return 'LONG_STRONG'
    if one[-2]['c'] < a < b and a < previous and four[-2]['c'] < c:
        return 'SHORT_STRONG'
    if one[-2]['c'] > a and a > previous:
        return 'LONG_LIGHT'
    if one[-2]['c'] < a and a < previous:
        return 'SHORT_LIGHT'
    return 'NEUTRAL_STABLE'


def btc_30m_direction(candles):
    if len(candles) < 22:
        return 'NEUTRAL'
    a30 = atr(candles[:-1], 14)
    if not a30:
        return 'NEUTRAL'
    live = candles[-1]
    direction = 'LONG' if live['c'] > live['o'] else 'SHORT'
    metrics = candle_metrics(live, direction, a30)
    if not metrics or metrics['body'] < BTC15_DIRECTIONAL_BODY_ATR_MIN:
        return 'NEUTRAL'
    return direction


def btc15_direction(btc_data):
    candles = btc_data['15m']
    a15 = atr(candles[:-1], 14)
    pace, _ = live_volume_ratio(candles)
    result = {'directional': 'NEUTRAL', 'body_atr': 0.0, 'volume_pace': pace}
    if not a15:
        return result
    live = candles[-1]
    direction = 'LONG' if live['c'] > live['o'] else 'SHORT'
    metrics = candle_metrics(live, direction, a15)
    if metrics:
        result['body_atr'] = metrics['body']
        if metrics['body'] >= BTC15_DIRECTIONAL_BODY_ATR_MIN:
            result['directional'] = direction
    return result


def stochastic(candles):
    """Slow stochastic 14,3,3, incluso il prezzo LIVE 15m.

    Usa finestre OHLC disponibili al momento: nessun dato futuro.
    """
    if len(candles) < STOCH_K_PERIOD + STOCH_K_SMOOTH + STOCH_D_PERIOD - 2:
        return None
    fast = []
    for end in range(STOCH_K_PERIOD - 1, len(candles)):
        window = candles[end - STOCH_K_PERIOD + 1:end + 1]
        highest = max(c['h'] for c in window)
        lowest = min(c['l'] for c in window)
        if highest <= lowest:
            fast.append(50.0)
        else:
            fast.append(100 * (candles[end]['c'] - lowest) / (highest - lowest))
    slow_k = [sum(fast[i - STOCH_K_SMOOTH + 1:i + 1]) / STOCH_K_SMOOTH
              for i in range(STOCH_K_SMOOTH - 1, len(fast))]
    if len(slow_k) < STOCH_D_PERIOD:
        return None
    k = slow_k[-1]
    d = sum(slow_k[-STOCH_D_PERIOD:]) / STOCH_D_PERIOD
    return k, d


def live_ema15(c15):
    return ema([c['c'] for c in c15], 20)


def aligned_ema(c15, direction):
    # MODIFICA UNICA AL FILTRO EMA 15m:
    # - prezzo sopra/sotto EMA20 LIVE;
    # - EMA20 LIVE inclinata nella stessa direzione rispetto alla barra precedente;
    # - EMA50 15m NON blocca piu' (resta obbligatoria nel trend 1H).
    values = [c['c'] for c in c15]
    e20 = ema(values, 20)
    previous_e20 = ema(values[:-1], 20)
    if e20 is None or previous_e20 is None:
        return False, None
    price = values[-1]
    if direction == 'LONG':
        return price > e20 and e20 > previous_e20, e20
    return price < e20 and e20 < previous_e20, e20


def signed(value, direction):
    return value if direction == 'LONG' else -value


def intelligent_stop(direction, entry, level, live, a15, recent, ctx, bias):
    trend_strong = ctx == direction and bias.startswith(direction)
    if trend_strong:
        buffer_atr = SL_BUFFER_STRONG_TREND_ATR15
    elif ctx == 'NEUTRAL':
        buffer_atr = SL_BUFFER_NEUTRAL_4H_ATR15
    else:
        buffer_atr = SL_BUFFER_NORMAL_TREND_ATR15
    recent = recent[-SL_STRUCTURE_LOOKBACK_BARS:]
    if direction == 'LONG':
        structural = min([live['l'], level] + [c['l'] for c in recent])
        raw_distance = entry - (structural - a15 * buffer_atr)
    else:
        structural = max([live['h'], level] + [c['h'] for c in recent])
        raw_distance = (structural + a15 * buffer_atr) - entry
    if raw_distance <= 0 or raw_distance > a15 * SL_MAX_DISTANCE_ATR15:
        return None
    distance = max(raw_distance, a15 * SL_MIN_DISTANCE_ATR15)
    return entry - distance if direction == 'LONG' else entry + distance


def clamp(value, low, high):
    return max(low, min(value, high))


def adaptive_targets(direction, entry, stop, a15, quality, pace, body, follow, ctx, bias):
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    strength = 0.0
    strength += clamp((pace - 1.20) / 2.50, 0, 1) * 0.30
    strength += clamp((body - 0.16) / 0.70, 0, 1) * 0.25
    strength += clamp((follow - 0.06) / 0.60, 0, 1) * 0.20
    strength += clamp((quality - 7) / 6.0, 0, 1) * 0.15
    if ctx == direction:
        strength += 0.05
    if bias.startswith(direction):
        strength += 0.05
    strength = clamp(strength, 0, 1)
    exhaustion = clamp((body - 1.20) / 0.35, 0, 1)
    adjusted = clamp(strength * (1 - 0.35 * exhaustion), 0, 1)
    r1 = 1.00 + 0.40 * adjusted
    r2 = 1.60 + 0.60 * adjusted
    r3 = 2.40 + 0.90 * adjusted
    d1 = max(risk * r1, entry * TP_MIN_NET_PCT)
    d2 = max(risk * r2, d1 * 1.15)
    d3 = max(risk * r3, d2 * 1.15)
    sign = 1 if direction == 'LONG' else -1
    return entry + sign * d1, entry + sign * d2, entry + sign * d3, strength


def leverage_cap(quality):
    if quality >= 13:
        return 100
    if quality >= 12:
        return 75
    if quality >= 11:
        return 50
    if quality >= 10:
        return 30
    if quality >= 9:
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


def snapshot(data, direction):
    c15 = data['15m']
    live = c15[-1]
    a15 = atr(c15[:-1], 14)
    if not a15 or a15 <= 0:
        return None, 'ATR'
    pace, elapsed = live_volume_ratio(c15)
    if elapsed < MIN_CANDLE_ELAPSED_SECONDS:
        return None, 'inizio candela'
    metrics = candle_metrics(live, direction, a15)
    if metrics is None:
        return None, 'candela contraria'
    aligned, e20 = aligned_ema(c15, direction)
    if not aligned:
        return None, 'EMA 15m'
    sto = stochastic(c15)
    if sto is None:
        return None, 'stocastico mancante'
    k, d = sto
    spread = signed(k - d, direction)
    if spread <= STOCH_MIN_SPREAD:
        return None, 'stocastico contrario'
    if pace < BREAKOUT_VOLUME_PACE_MIN:
        return None, 'volume LIVE'
    return {
        'price': live['c'], 'a15': a15, 'pace': pace,
        'body': metrics['body'], 'close': metrics['close'],
        'wick': metrics['wick'], 'ema20': e20,
        'ema_gap_atr': signed(live['c'] - e20, direction) / a15,
        'stoch_k': k, 'stoch_d': d, 'stoch_spread': spread,
        'candle_time': live['t'],
        'closed_volume': closed_volume_ratio(c15),
    }, None


def invalidate(symbol, reason, counters):
    if symbol in pending:
        item = pending.pop(symbol)
        print(f'{symbol}: HOLD ANNULLATO ({reason}) | {item["direction"]}')
    counters[reason] += 1


def analyze_symbol(symbol, data, bias, btc15, btc30, btc1h, now, counters):
    c15, c1h, c4h = data['15m'], data['1h'], data['4h']
    if min(len(c15), len(c1h), len(c4h)) < 55:
        invalidate(symbol, 'dati insufficienti', counters)
        return None
    trend = trend_1h(c1h)
    if trend == 'NEUTRAL':
        invalidate(symbol, 'trend 1H neutro', counters)
        return None
    direction = trend
    if btc15['directional'] != direction:
        invalidate(symbol, 'BTC15 non concorde', counters)
        return None
    if btc1h != direction:
        invalidate(symbol, 'BTC1H non concorde', counters)
        return None
    if not bias.startswith(direction + '_'):
        invalidate(symbol, 'bias BTC non concorde', counters)
        return None
    current, reason = snapshot(data, direction)
    if current is None:
        invalidate(symbol, reason, counters)
        return None

    live = c15[-1]
    a15 = current['a15']
    ctx = context_4h(c4h)
    sign = 1 if direction == 'LONG' else -1
    item = pending.get(symbol)

    if item is None:
        # Struttura fissata SOLO al primo attraversamento osservato.
        closed = c15[:-1]
        structure = closed[-STRUCTURE_BARS:]
        level = max(c['h'] for c in structure) if direction == 'LONG' else min(c['l'] for c in structure)
        extension = sign * (live['c'] - level) / a15
        if not (BREAKOUT_MIN_ATR <= extension <= BREAKOUT_START_MAX_EXTENSION_ATR):
            counters['nessun breakout iniziale'] += 1
            return None
        if (current['body'] < BREAKOUT_BODY_ATR_MIN or
                current['close'] < BREAKOUT_CLOSE_POSITION_MIN or
                current['wick'] > BREAKOUT_WICK_MAX):
            counters['candela breakout debole'] += 1
            return None
        # Non si avvia un hold quando il prezzo era gia' oltre il livello
        # nella scansione precedente: osserviamo il vero attraversamento.
        previous = last_observed.get(symbol)
        if previous is None or previous['direction'] != direction or previous['price'] * sign > level * sign:
            counters['breakout gia avvenuto/non osservato'] += 1
            return None
        pending[symbol] = {
            'direction': direction, 'level': level, 'started': now,
            'last_seen': now, 'start': current,
            'start_candle_time': current['candle_time'],
        }
        print(f'{symbol}: HOLD AVVIATO {direction} | livello {level:.9g} | 300s')
        counters['hold avviati'] += 1
        return None

    if item['direction'] != direction:
        invalidate(symbol, 'cambio direzione', counters)
        return None
    if now - item['last_seen'] > HOLD_MAX_GAP_SECONDS:
        invalidate(symbol, 'gap dati durante HOLD', counters)
        return None
    item['last_seen'] = now
    level = item['level']
    if sign * (current['price'] - level) <= HOLD_LEVEL_TOLERANCE_ATR * a15:
        invalidate(symbol, 'rientro nel livello', counters)
        return None
    if now - item['started'] < HOLD_SECONDS:
        counters['hold in corso'] += 1
        return None

    start = item['start']
    progress = sign * (current['price'] - start['price']) / a15
    ema_improvement = current['ema_gap_atr'] - start['ema_gap_atr']
    stoch_improvement = current['stoch_spread'] - start['stoch_spread']
    extension = sign * (current['price'] - level) / a15
    if progress < HOLD_PRICE_PROGRESS_MIN_ATR:
        invalidate(symbol, 'prezzo non migliorato in 5m', counters)
        return None
    if ema_improvement < HOLD_EMA_IMPROVEMENT_MIN_ATR:
        invalidate(symbol, 'EMA non migliorata in 5m', counters)
        return None
    if stoch_improvement < HOLD_STOCH_SPREAD_IMPROVEMENT_MIN:
        invalidate(symbol, 'stocastico non migliorato in 5m', counters)
        return None
    if extension < BREAKOUT_MIN_ATR or extension > HOLD_MAX_EXTENSION_ATR:
        invalidate(symbol, 'estensione ATR fuori range', counters)
        return None
    if (current['body'] < BREAKOUT_BODY_ATR_MIN or
            current['close'] < BREAKOUT_CLOSE_POSITION_MIN or
            current['wick'] > BREAKOUT_WICK_MAX):
        invalidate(symbol, 'candela finale debole', counters)
        return None

    entry = current['price']
    stop = intelligent_stop(direction, entry, level, live, a15, c15[:-1], ctx, bias)
    if stop is None:
        invalidate(symbol, 'stop strutturale fuori range', counters)
        return None
    quality = 5
    quality += int(current['pace'] >= 1.8) + int(current['pace'] >= 2.5)
    quality += int(current['body'] >= 0.40) + int(current['body'] >= 0.65)
    quality += int(progress >= 0.06)
    quality += int(current['closed_volume'] >= CLOSED_VOLUME_PREFERRED)
    quality += int(ctx == direction)
    quality += int(stoch_improvement >= 2.0)
    quality = min(quality, 13)
    aggressive_ok = (quality >= 11 and current['pace'] >= 2.5 and
                     current['body'] >= 0.65 and progress >= 0.06 and
                     ctx == direction and stoch_improvement >= 2.0)
    leverage = calculate_leverage(entry, stop, quality, aggressive_ok)
    targets = adaptive_targets(direction, entry, stop, a15, quality,
                               current['pace'], current['body'], extension, ctx, bias)
    if targets is None:
        invalidate(symbol, 'target non valido', counters)
        return None
    tp1, tp2, tp3, strength = targets
    signal = {
        'direction': direction, 'mode': 'BREAKOUT CONFERMATO 5 MIN',
        'price': entry, 'entry_low': entry - a15 * 0.06,
        'entry_high': entry + a15 * 0.06,
        'sl': stop, 'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
        'leverage': leverage, 'quality': quality, 'level': level,
        'candle_time': item['start_candle_time'],
        'live_volume_pace': current['pace'],
        'closed_volume_ratio': current['closed_volume'],
        'live_body_atr': current['body'],
        'follow_through_atr': extension,
        'rejection_wick_ratio': current['wick'],
        'price_progress_atr': progress,
        'ema_improvement': ema_improvement,
        'stoch_improvement': stoch_improvement,
        'stoch_k': current['stoch_k'], 'stoch_d': current['stoch_d'],
        'context1h': trend, 'context4h': ctx, 'btc': bias,
        'btc15_direction': btc15['directional'],
        'btc15_body_atr': btc15['body_atr'],
        'btc15_volume_pace': btc15['volume_pace'],
        'btc30_direction': btc30, 'btc1h_direction': btc1h,
        'target_strength': strength,
        'hold_seconds': int(now - item['started']),
    }
    pending.pop(symbol, None)
    counters['hold confermati'] += 1
    return signal


def fmt_price(value):
    if value >= 1000:
        return f'{value:.2f}'
    if value >= 1:
        return f'{value:.4f}'
    if value >= 0.01:
        return f'{value:.6f}'
    return f'{value:.9f}'


def build_message(symbol, signal):
    pair = symbol.replace('USDT', '/USDT')
    italian_time = datetime.now(ZoneInfo('Europe/Rome')).strftime('%d/%m/%Y %H:%M:%S')
    return (
        f"🚀 MOMENTUM CONFERMATO V8\n{pair} — {signal['direction']}\n"
        f"Orario Italia: {italian_time}\n"
        f"Ingresso: {signal['mode']} | Qualita': {signal['quality']}/13\n\n"
        f"ENTRY: {fmt_price(signal['entry_low'])} - {fmt_price(signal['entry_high'])}\n"
        f"SL strutturale: {fmt_price(signal['sl'])}\n\n"
        f"TP1 dinamico: {fmt_price(signal['tp1'])}\n"
        f"TP2 dinamico: {fmt_price(signal['tp2'])}\n"
        f"TP3 dinamico: {fmt_price(signal['tp3'])}\n\n"
        f"Leva indicativa: {signal['leverage']}x\n\n"
        f"HOLD verificato: {signal['hold_seconds']}s\n"
        f"Livello rotto: {fmt_price(signal['level'])}\n"
        f"Volume LIVE: {signal['live_volume_pace']:.2f}x\n"
        f"Volume 15m chiuso: {signal['closed_volume_ratio']:.2f}x\n"
        f"Body LIVE: {signal['live_body_atr']:.2f} ATR15\n"
        f"Distanza dal livello: {signal['follow_through_atr']:.2f} ATR15\n"
        f"Progressione 5m: {signal['price_progress_atr']:+.3f} ATR15\n"
        f"Miglioramento EMA 5m: {signal['ema_improvement']:+.3f} ATR15\n"
        f"Stoch 15m K/D: {signal['stoch_k']:.1f}/{signal['stoch_d']:.1f}\n"
        f"Miglioramento spread Stoch: {signal['stoch_improvement']:+.2f} punti\n\n"
        f"Altcoin 1H: {signal['context1h']}\n"
        f"4H informativo: {signal['context4h']}\n"
        f"BTC bias: {signal['btc']}\n"
        f"BTC15: {signal['btc15_direction']} (direzionale)\n"
        f"BTC30 informativo: {signal['btc30_direction']}\n"
        f"BTC1H: {signal['btc1h_direction']}\n"
        f"Forza target: {signal['target_strength']:.2f}\n\n"
        'Nessun ingresso anticipato. Leva indicativa, non garanzia.'
    )


def should_send(symbol, signal):
    now = time.time()
    key = (symbol, signal['direction'], signal['candle_time'])
    for old_key, old_time in list(signal_state.items()):
        if now - old_time > 12 * 3600:
            del signal_state[old_key]
    return key not in signal_state


last_observed = {}


def scan_market():
    global last_diag
    started = time.monotonic()
    successful = 0
    counters = Counter()
    btc_data = market_data('BTCUSDT')
    bias = btc_bias(btc_data)
    btc15 = btc15_direction(btc_data)
    btc30 = btc_30m_direction(cached_candles('BTCUSDT', '30m', CACHE_30M_SECONDS))
    btc1h = trend_1h(btc_data['1h'])
    print(f"BTC: {bias} | BTC15: {btc15['directional']} | BTC30: {btc30} | BTC1H: {btc1h}")
    market = {'BTCUSDT': btc_data}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as executor:
        futures = {executor.submit(market_data, symbol): symbol
                   for symbol in SYMBOLS if symbol != 'BTCUSDT'}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                market[symbol] = future.result()
            except Exception as exc:
                print(symbol, 'ERRORE download:', exc)
                counters['download fallito'] += 1
    now = time.monotonic()
    for symbol in SYMBOLS:
        if symbol not in market:
            # Non considerare un HOLD valido se mancano dati.
            if symbol in pending:
                invalidate(symbol, 'download mancante', counters)
            continue
        try:
            successful += 1
            data = market[symbol]
            signal = analyze_symbol(symbol, data, bias, btc15, btc30, btc1h, now, counters)
            if signal and should_send(symbol, signal):
                print(symbol, 'MOMENTUM CONFERMATO V8', signal['direction'],
                      signal['mode'], 'leva', signal['leverage'])
                if send_telegram(build_message(symbol, signal)):
                    signal_state[(symbol, signal['direction'], signal['candle_time'])] = time.time()
            # La rilevazione precedente serve a identificare attraversamenti
            # reali, non un prezzo gia' sopra la resistenza.
            trend = trend_1h(data['1h'])
            last_observed[symbol] = {'price': data['15m'][-1]['c'], 'direction': trend}
        except Exception as exc:
            print(symbol, 'ERRORE analisi:', exc)
            if symbol in pending:
                invalidate(symbol, 'errore analisi', counters)
    elapsed = time.monotonic() - started
    print(f'Scansione completata: {successful}/{len(SYMBOLS)} coppie | '
          f'durata {elapsed:.1f}s | target {SCAN_SECONDS}s | HOLD attivi {len(pending)}')
    if DIAGNOSTIC_LOGS and time.monotonic() - last_diag >= DIAGNOSTIC_EVERY_SECONDS:
        print('DIAGNOSTICA V8 (questa scansione):', dict(counters))
        last_diag = time.monotonic()


def main():
    print('CryptoSignalAI V8 - HOLD 5 MINUTI + EMA20 15m DIREZIONALE')
    send_telegram(
        'CryptoSignalAI V8 ONLINE\n'
        '25 coppie. Solo breakout 15m, HOLD 300 secondi obbligatorio.\n'
        'BTC15 direzionale + BTC1H + bias concordi; altcoin 1H direzionale.\n'
        'EMA20 15m: prezzo e inclinazione concordi; EMA50 15m informativa.\n'
        'EMA20/EMA50 1H INVARIATE. Stocastico 15m favorevole e migliorato dopo 5 minuti.\n'
        'Volume LIVE >=1.20x, prezzo in progresso, anti-inseguimento ATR.\n'
        'BTC30/4H informativi. SL/TP e leva dinamici. NESSUN ingresso anticipato.')
    while True:
        started = time.monotonic()
        try:
            scan_market()
        except Exception as exc:
            print('Errore scansione generale:', exc)
            # In caso di errore generale, non fidarsi di HOLD senza osservazioni.
            pending.clear()
        time.sleep(max(1.0, SCAN_SECONDS - (time.monotonic() - started)))


if __name__ == '__main__':
    main()
