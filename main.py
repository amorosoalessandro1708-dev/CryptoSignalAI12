import os
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

# ==========================================================
# MOMENTUM CONFERMATO - UN SOLO TIPO DI SEGNALE
# Nessun nuovo indicatore: EMA, ATR, volume, prezzo e BTC15.
# ==========================================================
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
SYMBOLS = [
    'SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT',
    'BNBUSDT', 'DOGEUSDT', 'AVAXUSDT', 'SUIUSDT',
    'LINKUSDT', 'ADAUSDT', 'NEARUSDT', 'UNIUSDT'
]
REST = 'https://fapi.binance.com'
KLINES_URL = REST + '/fapi/v1/klines'
SCAN_SECONDS = 15
CACHE_1H_SECONDS = 15 * 60
CACHE_4H_SECONDS = 60 * 60
LIVE_VOLUME_MIN_ELAPSED_SECONDS = 15
LIVE_VOLUME_PACE_CAP = 20.0
MIN_REST_GAP_SECONDS = 0.30
MAX_RETRIES = 4
RETRY_FALLBACK_SECONDS = (5, 10, 20, 30)
DIAGNOSTIC_LOGS = True

# Un solo set di filtri per LONG e SHORT, in trend o controtrend 1H.
MOMENTUM_VOLUME_MIN = 2.50
MOMENTUM_BODY_ATR_MIN = 0.80
MOMENTUM_BREAKOUT_ATR_MIN = 0.70
MOMENTUM_WICK_MAX = 0.20
MOMENTUM_VOLUME_GROWTH_MIN = 1.10  # +10% rispetto alla scansione precedente
MOMENTUM_CLOSE_POSITION_MIN = 0.80
PRICE_PROGRESS_MIN_ATR15 = 0.03
BREAKOUT_PROGRESS_MIN_ATR15 = 0.02
LIVE_POWER_STRUCTURE_BARS = 2
# Via eccezionale gia presente nel bot originale: permette la prima lettura.
EARLY_REVERSAL_EXCEPTIONAL_PACE_MIN = 3.80
EARLY_REVERSAL_EXCEPTIONAL_BODY_ATR_MIN = 0.85
EARLY_REVERSAL_EXCEPTIONAL_WICK_MAX = 0.10
EARLY_REVERSAL_EXCEPTIONAL_CLOSE_POSITION_MIN = 0.90
EARLY_REVERSAL_MAX_PRIOR_DIRECTIONAL_BARS = 1
# Controlli anti-inseguimento invariati.
LATE_ENTRY_BODY_ATR_HARD = 1.55
LATE_ENTRY_BREAKOUT_ATR_HARD = 1.20

# Il contesto 1H deve essere riconoscibile; 4H e' informativo, mai bloccante.
BTC15_DIRECTIONAL_BODY_ATR_MIN = 0.15
BTC15_STRONG_BODY_ATR_MIN = 0.45
BTC15_STRONG_CLOSE_POSITION_MIN = 0.80
BTC15_STRONG_VOLUME_PACE_MIN = 1.50
BTC15_STRONG_MAX_ADVERSE_WICK_BODY = 0.25

# Stop, target e leva: stessi parametri del codice fornito.
SL_STRUCTURE_BUFFER_ATR15 = 0.10
SL_BUFFER_STRONG_TREND_ATR15 = 0.18
SL_BUFFER_NORMAL_TREND_ATR15 = 0.14
SL_BUFFER_NEUTRAL_4H_ATR15 = 0.10
SL_MIN_DISTANCE_ATR15 = 0.55
SL_MAX_DISTANCE_ATR15 = 1.80
SL_STRUCTURE_LOOKBACK_BARS = 4
TP1_MIN_ATR15, TP1_MAX_ATR15 = 0.30, 0.75
TP2_MIN_ATR15, TP2_MAX_ATR15 = 0.65, 1.35
TP3_MIN_ATR15, TP3_MAX_ATR15 = 1.00, 2.10
TP_MIN_NET_PCT = 0.0015
LEVERAGE_SAFETY = 0.35
LEVERAGE_STEPS = (1, 2, 3, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100)

session = requests.Session()
request_lock = threading.Lock()
cache_lock = threading.Lock()
last_rest_request = 0.0
market_cache = {}
live_volume_state = {}
live_momentum_state = {}
signal_state = {}


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
            response = session.get(url, params=params, timeout=15)
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
             'c': float(x[4]), 'v': float(x[5]), 't': int(x[0])} for x in raw]


def cached_candles(symbol, interval, lifetime):
    key = (symbol, interval)
    now = time.time()
    with cache_lock:
        item = market_cache.get(key)
        if item and now - item['time'] < lifetime:
            return item['data']
    data = parse_candles(get_json(KLINES_URL, {'symbol': symbol, 'interval': interval, 'limit': 120}))
    with cache_lock:
        market_cache[key] = {'time': now, 'data': data}
    return data


def market_data(symbol):
    return {
        '15m': parse_candles(get_json(KLINES_URL, {'symbol': symbol, 'interval': '15m', 'limit': 120})),
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


def candle_metrics(candle, direction, atr15):
    body = abs(candle['c'] - candle['o'])
    span = candle['h'] - candle['l']
    if body <= 0 or span <= 0 or atr15 <= 0:
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
    return {'body': body / atr15, 'close': close_pos, 'wick': wick}


def trend_1h(candles):
    closes = [c['c'] for c in candles[:-1]]
    e20 = ema(closes, 20)
    previous = ema(closes[:-1], 20)
    if e20 is None or previous is None:
        return 'NEUTRAL'
    price = candles[-2]['c']
    if price > e20 and e20 >= previous:
        return 'LONG'
    if price < e20 and e20 <= previous:
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


def get_btc15_live_strength(btc_data):
    c15 = btc_data['15m']
    a15 = atr(c15[:-1], 14)
    pace, _ = live_volume_ratio(c15)
    neutral = {'direction': 'NEUTRAL', 'directional': 'NEUTRAL',
               'strong': False, 'body_atr': 0.0, 'volume_pace': pace}
    if not a15:
        return neutral
    live = c15[-1]
    direction = 'LONG' if live['c'] > live['o'] else 'SHORT'
    metrics = candle_metrics(live, direction, a15)
    if not metrics:
        return neutral
    directional = direction if metrics['body'] >= BTC15_DIRECTIONAL_BODY_ATR_MIN else 'NEUTRAL'
    strong = (metrics['body'] >= BTC15_STRONG_BODY_ATR_MIN and
              metrics['close'] >= BTC15_STRONG_CLOSE_POSITION_MIN and
              pace >= BTC15_STRONG_VOLUME_PACE_MIN and
              metrics['wick'] <= BTC15_STRONG_MAX_ADVERSE_WICK_BODY)
    return {'direction': direction if strong else 'NEUTRAL',
            'directional': directional, 'strong': strong,
            'body_atr': metrics['body'], 'volume_pace': pace}


def update_volume(symbol, live, pace):
    previous = live_volume_state.get(symbol)
    same = previous is not None and previous['candle_time'] == live['t']
    growth = 0.0
    if same and previous['pace'] > 0:
        growth = pace / previous['pace']
    volume_rising = same and live['v'] > previous['volume']
    live_volume_state[symbol] = {'candle_time': live['t'], 'pace': pace, 'volume': live['v']}
    return growth, bool(volume_rising)


def update_progress(symbol, direction, live, level, a15):
    key = (symbol, direction)
    depth = ((live['c'] - level) if direction == 'LONG' else (level - live['c'])) / a15
    previous = live_momentum_state.get(key)
    same = previous is not None and previous['candle_time'] == live['t']
    price_progress = 0.0
    depth_progress = 0.0
    if same:
        price_progress = ((live['c'] - previous['price']) if direction == 'LONG'
                          else (previous['price'] - live['c'])) / a15
        depth_progress = depth - previous['depth']
    live_momentum_state[key] = {'candle_time': live['t'], 'price': live['c'], 'depth': depth}
    return same, price_progress, depth_progress


def prior_directional_bars(c15, direction, a15):
    count = 0
    for candle in reversed(c15[-3:-1]):
        directional = candle['c'] > candle['o'] if direction == 'LONG' else candle['c'] < candle['o']
        if directional and abs(candle['c'] - candle['o']) / a15 >= 0.35:
            count += 1
        else:
            break
    return count


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
        raw_distance = structural + a15 * buffer_atr - entry
    distance = max(a15 * SL_MIN_DISTANCE_ATR15,
                   min(raw_distance, a15 * SL_MAX_DISTANCE_ATR15))
    return entry - distance if direction == 'LONG' else entry + distance


def clamp(value, low, high):
    return max(low, min(value, high))


def adaptive_targets(direction, entry, stop, a15, quality, pace, body, follow, ctx, bias):
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    strength = 0.0
    strength += clamp((pace - 2.50) / 2.50, 0, 1) * 0.30
    strength += clamp((body - 0.60) / 0.70, 0, 1) * 0.25
    strength += clamp((follow - 0.15) / 0.60, 0, 1) * 0.20
    strength += clamp((quality - 10) / 3.0, 0, 1) * 0.15
    if ctx == direction:
        strength += 0.05
    if bias.startswith(direction):
        strength += 0.05
    strength = clamp(strength, 0, 1)
    exhaustion = clamp((body - 1.20) / 0.35, 0, 1)
    adjusted = clamp(strength * (1 - 0.35 * exhaustion), 0, 1)
    a1 = TP1_MIN_ATR15 + (TP1_MAX_ATR15 - TP1_MIN_ATR15) * adjusted
    a2 = TP2_MIN_ATR15 + (TP2_MAX_ATR15 - TP2_MIN_ATR15) * adjusted
    a3 = TP3_MIN_ATR15 + (TP3_MAX_ATR15 - TP3_MIN_ATR15) * adjusted
    d1 = max(a15 * a1, entry * TP_MIN_NET_PCT)
    d2 = max(a15 * a2, d1 * 1.45)
    d3 = max(a15 * a3, d2 * 1.35)
    sign = 1 if direction == 'LONG' else -1
    return entry + sign * d1, entry + sign * d2, entry + sign * d3, strength


def leverage_cap(quality):
    if quality >= 13: return 100
    if quality >= 12: return 75
    if quality >= 11: return 50
    if quality >= 10: return 30
    if quality >= 9: return 20
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


def analyze_symbol(symbol, data, bias, btc15):
    c15, c1h, c4h = data['15m'], data['1h'], data['4h']
    if min(len(c15), len(c1h), len(c4h)) < 55:
        return None
    live = c15[-1]
    a15 = atr(c15[:-1], 14)
    if not a15 or a15 <= 0:
        return None
    trend = trend_1h(c1h)
    ctx = context_4h(c4h)
    pace, elapsed = live_volume_ratio(c15)
    growth, volume_rising = update_volume(symbol, live, pace)
    structure = c15[-(LIVE_POWER_STRUCTURE_BARS + 1):-1]
    resistance = max(max(c['o'], c['c']) for c in structure)
    support = min(min(c['o'], c['c']) for c in structure)
    # Aggiorniamo entrambe le progressioni a ogni scansione, anche senza segnale.
    progression = {
        'LONG': update_progress(symbol, 'LONG', live, resistance, a15),
        'SHORT': update_progress(symbol, 'SHORT', live, support, a15),
    }
    if trend == 'NEUTRAL' or elapsed < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return None
    if pace < MOMENTUM_VOLUME_MIN:
        return None
    for direction, level in (('LONG', resistance), ('SHORT', support)):
        metrics = candle_metrics(live, direction, a15)
        if not metrics:
            continue
        body, close_pos, wick = metrics['body'], metrics['close'], metrics['wick']
        follow = ((live['c'] - level) if direction == 'LONG' else (level - live['c'])) / a15
        if not (body >= MOMENTUM_BODY_ATR_MIN and
                close_pos >= MOMENTUM_CLOSE_POSITION_MIN and
                wick <= MOMENTUM_WICK_MAX and
                follow >= MOMENTUM_BREAKOUT_ATR_MIN):
            continue
        if body >= LATE_ENTRY_BODY_ATR_HARD or follow >= LATE_ENTRY_BREAKOUT_ATR_HARD:
            continue
        if prior_directional_bars(c15, direction, a15) > EARLY_REVERSAL_MAX_PRIOR_DIRECTIONAL_BARS:
            continue
        countertrend = direction != trend
        # BTC15 direzionale per tutte le altcoin; FORTE se controtrend 1H.
        if symbol != 'BTCUSDT':
            if btc15['directional'] != direction:
                continue
            if countertrend and not (btc15['strong'] and btc15['direction'] == direction):
                continue
        same, pp, dp = progression[direction]
        progress_ok = (same and pp >= PRICE_PROGRESS_MIN_ATR15 and
                       dp >= BREAKOUT_PROGRESS_MIN_ATR15)
        growth_ok = volume_rising and growth >= MOMENTUM_VOLUME_GROWTH_MIN
        # La via eccezionale evita di perdere la prima scansione di un impulso.
        exceptional = (pace >= EARLY_REVERSAL_EXCEPTIONAL_PACE_MIN and
                       body >= EARLY_REVERSAL_EXCEPTIONAL_BODY_ATR_MIN and
                       wick <= EARLY_REVERSAL_EXCEPTIONAL_WICK_MAX and
                       close_pos >= EARLY_REVERSAL_EXCEPTIONAL_CLOSE_POSITION_MIN)
        if not ((growth_ok and progress_ok) or exceptional):
            continue
        entry = live['c']
        stop = intelligent_stop(direction, entry, level, live, a15, c15[:-1], ctx, bias)
        quality = 10 + int(pace >= 3) + int(body >= 0.70) + int(pp >= 0.08) + int(countertrend)
        # Unico tipo di segnale; leva dinamica, nessuna etichetta aggressiva.
        aggressive_ok = (not countertrend and pace >= 3.50 and
                         body >= 0.80 and quality >= 11)
        leverage = calculate_leverage(entry, stop, quality, aggressive_ok)
        targets = adaptive_targets(direction, entry, stop, a15, quality, pace,
                                   body, follow, ctx, bias)
        if targets is None:
            continue
        tp1, tp2, tp3, strength = targets
        return {
            'type': 'MOMENTUM_CONFERMATO', 'direction': direction,
            'price': entry, 'entry_low': entry - a15 * 0.06,
            'entry_high': entry + a15 * 0.06,
            'sl': stop, 'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
            'leverage': leverage, 'level': level, 'candle_time': live['t'],
            'live_volume_pace': pace, 'volume_growth_pct': (growth - 1) * 100 if growth else None,
            'live_body_atr': body, 'follow_through_atr': follow,
            'rejection_wick_ratio': wick, 'price_progress_atr': pp,
            'depth_progress_atr': dp, 'context1h': trend, 'context4h': ctx,
            'btc': bias, 'btc15_direction': btc15['directional'],
            'btc15_strong': btc15['strong'], 'btc15_body_atr': btc15['body_atr'],
            'btc15_volume_pace': btc15['volume_pace'], 'target_strength': strength,
            'exceptional': exceptional,
        }
    if DIAGNOSTIC_LOGS and pace >= MOMENTUM_VOLUME_MIN:
        print(f'{symbol} filtro momentum non superato: volume {pace:.2f}x, 1H {trend}, 4H {ctx}')
    return None


def fmt_price(value):
    if value >= 1000: return f'{value:.2f}'
    if value >= 1: return f'{value:.4f}'
    return f'{value:.6f}'


def build_message(symbol, signal):
    pair = symbol.replace('USDT', '/USDT')
    growth = signal['volume_growth_pct']
    growth_text = 'prima lettura eccezionale' if growth is None else f'{growth:+.1f}%'
    italian_time = datetime.now(ZoneInfo('Europe/Rome')).strftime('%d/%m/%Y %H:%M:%S')
    return (
        f"🚀 MOMENTUM CONFERMATO\n{pair} — {signal['direction']}\n"
        f"Orario Italia: {italian_time}\n\n"
        f"ENTRY: {fmt_price(signal['entry_low'])} - {fmt_price(signal['entry_high'])}\n"
        f"SL dinamico: {fmt_price(signal['sl'])}\n\n"
        f"TP1 dinamico: {fmt_price(signal['tp1'])}\n"
        f"TP2 dinamico: {fmt_price(signal['tp2'])}\n"
        f"TP3 dinamico: {fmt_price(signal['tp3'])}\n\n"
        f"Leva indicativa: {signal['leverage']}x\n\n"
        f"Volume LIVE: {signal['live_volume_pace']:.2f}x\n"
        f"Accelerazione volume: {growth_text}\n"
        f"Body LIVE: {signal['live_body_atr']:.2f} ATR15\n"
        f"Breakout totale: {signal['follow_through_atr']:.2f} ATR15\n"
        f"Rejection Wick: {signal['rejection_wick_ratio']:.2f}x body\n"
        f"Progressione prezzo: {signal['price_progress_atr']:+.2f} ATR15\n"
        f"Progressione breakout: {signal['depth_progress_atr']:+.2f} ATR15\n\n"
        f"Contesto 1H: {signal['context1h']}\n"
        f"Contesto 4H (informativo): {signal['context4h']}\n"
        f"BTC contesto: {signal['btc']}\n"
        f"BTC15 LIVE: {signal['btc15_direction']} "
        f"{'FORTE' if signal['btc15_strong'] else 'direzionale'} | "
        f"body {signal['btc15_body_atr']:.2f} ATR | "
        f"vol {signal['btc15_volume_pace']:.2f}x\n"
        f"Forza target: {signal['target_strength']:.2f}\n\n"
        'Candela 15m ancora in formazione | HOLD non richiesto'
    )


def should_send(symbol, signal):
    # Un solo messaggio per simbolo/direzione/candela, senza duplicati al riavvio
    # soltanto finche' il processo resta attivo (stato in memoria).
    now = time.time()
    key = (symbol, signal['direction'], signal['candle_time'])
    for old_key, old_time in list(signal_state.items()):
        if now - old_time > 12 * 3600:
            del signal_state[old_key]
    return key not in signal_state


def scan_market():
    successful = 0
    btc_data = market_data('BTCUSDT')
    bias = btc_bias(btc_data)
    btc15 = get_btc15_live_strength(btc_data)
    print(f"BTC: {bias} | BTC15: {btc15['directional']} forte={btc15['strong']}")
    for symbol in SYMBOLS:
        try:
            data = btc_data if symbol == 'BTCUSDT' else market_data(symbol)
            successful += 1
            signal = analyze_symbol(symbol, data, bias, btc15)
            if signal and should_send(symbol, signal):
                print(symbol, 'MOMENTUM CONFERMATO', signal['direction'],
                      'leva', signal['leverage'])
                if send_telegram(build_message(symbol, signal)):
                    signal_state[(symbol, signal['direction'], signal['candle_time'])] = time.time()
        except Exception as exc:
            print(symbol, 'ERRORE:', exc)
        time.sleep(0.20)
    print(f'Scansione completata: {successful}/{len(SYMBOLS)} coppie')


def main():
    print('CryptoSignalAI12 - MOMENTUM CONFERMATO UNICO')
    send_telegram(
        'CryptoSignalAI12 ONLINE\n'
        'Unico segnale: 🚀 MOMENTUM CONFERMATO (LONG/SHORT).\n'
        '12 coppie, scanner target 15 secondi.\n'
        'Volume >=2.50x | body >=0.80 ATR15 | breakout >=0.70 ATR15 | '
        'wick <=0.20x | accelerazione >=10% (salvo impulso eccezionale).\n'
        '1H obbligatorio (trend o controtrend), 4H informativo.\n'
        'BTC15 direzionale per altcoin, FORTE se controtrend 1H.\n'
        'SL, TP e leva dinamici. Nessun HOLD. Prima candela LIVE.')
    while True:
        started = time.monotonic()
        try:
            scan_market()
        except Exception as exc:
            print('Errore scansione generale:', exc)
        time.sleep(max(1.0, SCAN_SECONDS - (time.monotonic() - started)))


if __name__ == '__main__':
    main()
