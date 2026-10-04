import os
import time
import json
import threading
from collections import deque

import requests
import websocket


# ==========================================================
# CONFIGURAZIONE
# ==========================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SYMBOLS = [
    "SOLUSDT", "BTCUSDT", "ETHUSDT", "XRPUSDT",
    "BNBUSDT", "DOGEUSDT", "AVAXUSDT", "SUIUSDT",
    "LINKUSDT", "ADAUSDT", "NEARUSDT", "UNIUSDT"
]

REST = "https://fapi.binance.com"
KLINES_URL = f"{REST}/fapi/v1/klines"
OI_URL = f"{REST}/futures/data/openInterestHist"
PREMIUM_URL = f"{REST}/fapi/v1/premiumIndex"

LIQ_WS_URLS = [
    "wss://fstream.binance.com/market/ws/!forceOrder@arr",
    "wss://fstream.binance.com/public/ws/!forceOrder@arr",
]

SCAN_SECONDS = 30
LIQ_WINDOW_SECONDS = 15 * 60

CACHE_1H_SECONDS = 15 * 60
CACHE_4H_SECONDS = 60 * 60
market_cache = {}
market_cache_lock = threading.Lock()

PRE_STRUCTURE_BARS = 6
PRE_VOL_15M_MIN = 0.90
PRE_NEAR_ATR15 = 0.30
CONFIRM_VOL_15M_FLOOR = 1.15
CONFIRM_VOL_1H_FLOOR = 0.30
CONFIRM_BODY_ATR_FLOOR = 0.15
FOLLOW_THROUGH_MIN_ATR15 = 0.08
CONFIRM_SCORE_MIN = 7
OI_HARD_FLOOR = -0.10
OI_SCORE_POSITIVE = 0.05
OI_SCORE_STRONG = 0.20
FUNDING_BLOCK = 0.0008
FUNDING_GOOD = 0.0005

VOL15_SCORE_MEDIUM = 1.30
VOL15_SCORE_STRONG = 1.50
VOL15_SCORE_VERY_STRONG = 2.00
VOL1H_SCORE_MEDIUM = 0.60
VOL1H_SCORE_STRONG = 1.00

BREAKOUT_HOLD_SECONDS = 2 * 60
BREAKOUT_RETEST_TOLERANCE_ATR15 = 0.05
REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD = True

REQUIRE_LIVE_DIRECTION_AFTER_HOLD = True
LIVE_BODY_ATR_MIN = 0.05
FINAL_BREAKOUT_MARGIN_ATR15 = 0.06
LIVE_CLOSE_POSITION_MIN = 0.60
LIVE_VOLUME_PACE_MIN = 1.00
LIVE_VOLUME_MIN_ELAPSED_SECONDS = 30
LIVE_VOLUME_PACE_CAP = 20.00

REQUIRE_BTC_NOT_OPPOSITE = True
REQUIRE_STRONG_BTC_FOR_AGGRESSIVE = True
DIAGNOSTIC_LOGS = True

AGGRESSIVE_VOL_15M_MIN = 2.00
AGGRESSIVE_VOL_1H_MIN = 1.00
OI_AGGRESSIVE_MIN = 0.20
FUNDING_AGGRESSIVE = 0.0005
ATR_AGGRESSIVE_MAX_PCT = 3.00
AGGRESSIVE_SCORE_MIN = 11

ATR_MIN_PCT = 0.10
ATR_MAX_PCT = 6.00
MAX_LIVE_RETRACE = 0.45
MAX_EXTENSION_ATR15 = 1.20
NORMAL_MAX_POST_HOLD_EXTENSION_ATR15 = 0.90
NORMAL_MAX_LIVE_RANGE_ATR15 = 1.60

# V6.6.2 MOMENTUM CONTINUATION 30S
# Logica di mercato semplificata:
# 15m = ingresso sulla PRIMA candela ancora aperta
# 1H = direzione principale
# 4H = conferma del contesto
# BTC = stessa direzione per le altcoin
# Nessun filtro OI/funding/liquidazioni/score/swing/dynamic impulse per confermare.
LIVE_POWER_ENABLED = True
LIVE_POWER_STRUCTURE_BARS = 2
LIVE_POWER_VOL_PACE_MIN = 2.50
LIVE_POWER_BODY_ATR_MIN = 0.45
LIVE_POWER_FOLLOW_THROUGH_ATR15 = 0.15
LIVE_POWER_CLOSE_POSITION_MIN = 0.70
LIVE_POWER_MIN_ELAPSED_SECONDS = 30

# V6.6: il ritmo deve accelerare rispetto alla lettura precedente.
LIVE_POWER_MIN_PACE_GROWTH = 1.20
# Via immediata: nessuna seconda scansione se l'impulso e' eccezionale.
LIVE_POWER_EXCEPTIONAL_PACE_MIN = 3.50
LIVE_POWER_EXCEPTIONAL_BODY_ATR_MIN = 0.80
LIVE_POWER_EXCEPTIONAL_WICK_MAX = 0.20
LIVE_POWER_EXCEPTIONAL_CLOSE_POSITION_MIN = 0.85

# V6.5 REJECTION WICK GUARD
REJECTION_WICK_MAX_BODY_RATIO = 0.40

# Swing guard: non si azzera per piccole pause/rimbalzi.
SWING_LOOKBACK_BARS = 12
NORMAL_MAX_SWING_ATR15 = 4.00

SL_STRUCTURE_BUFFER_ATR15 = 0.10
SL_MIN_DISTANCE_ATR15 = 0.45
SL_MAX_DISTANCE_ATR15 = 1.35

TP1_R_BASE = 0.60
TP2_R_BASE = 1.20
TP3_R_BASE = 2.00
TP1_R_HIGH = 0.70
TP2_R_HIGH = 1.40
TP3_R_HIGH = 2.40
TP1_R_AGGRESSIVE = 0.75
TP2_R_AGGRESSIVE = 1.50
TP3_R_AGGRESSIVE = 2.70

LEVERAGE_SAFETY = 0.35
LEVERAGE_STEPS = [1, 2, 3, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100]

MIN_REST_GAP_SECONDS = 0.30
MAX_RETRIES = 4
RETRY_FALLBACK_SECONDS = [5, 10, 20, 30]

session = requests.Session()
request_lock = threading.Lock()
last_rest_request = 0.0
signal_state = {}
breakout_hold_state = {}
# Memoria breve del volume live per verificare che il ritmo resti alto/crescente
# durante la stessa candela 15m.
live_power_volume_state = {}
liquidation_events = deque()
liq_lock = threading.Lock()


def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram non configurato")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": text},
            timeout=20,
        )
        r.raise_for_status()
    except Exception as e:
        print("Errore Telegram:", e)


def wait_rest_slot():
    global last_rest_request
    with request_lock:
        now = time.monotonic()
        wait = MIN_REST_GAP_SECONDS - (now - last_rest_request)
        if wait > 0:
            time.sleep(wait)
        last_rest_request = time.monotonic()


def get_json(url, params=None, timeout=15):
    last_error = None
    for attempt in range(MAX_RETRIES):
        wait_rest_slot()
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (418, 429):
                retry_after = r.headers.get("Retry-After")
                try:
                    wait = float(retry_after)
                except (TypeError, ValueError):
                    wait = RETRY_FALLBACK_SECONDS[min(attempt, len(RETRY_FALLBACK_SECONDS) - 1)]
                wait = max(wait, 5.0)
                print(f"Binance {r.status_code}: attendo {wait:.0f}s ({attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
                last_error = requests.HTTPError(f"HTTP {r.status_code}")
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last_error = e
            if attempt >= MAX_RETRIES - 1:
                break
            wait = min(RETRY_FALLBACK_SECONDS[min(attempt, len(RETRY_FALLBACK_SECONDS) - 1)], 30)
            print("Errore rete Binance:", e, f"- retry tra {wait}s")
            time.sleep(wait)
    raise last_error or RuntimeError("Errore Binance sconosciuto")


def get_klines(symbol, interval, limit=120):
    return get_json(KLINES_URL, {"symbol": symbol, "interval": interval, "limit": limit})


def parse_candles(raw):
    return [
        {"o": float(c[1]), "h": float(c[2]), "l": float(c[3]), "c": float(c[4]), "v": float(c[5]), "t": int(c[0])}
        for c in raw
    ]


def get_cached_klines(symbol, interval, cache_seconds):
    now = time.time()
    key = f"{symbol}:{interval}"
    with market_cache_lock:
        cached = market_cache.get(key)
        if cached and now - cached["time"] < cache_seconds:
            return cached["data"]
    data = parse_candles(get_klines(symbol, interval))
    with market_cache_lock:
        market_cache[key] = {"time": now, "data": data}
    return data


def market_data(symbol):
    return {
        "15m": parse_candles(get_klines(symbol, "15m")),
        "1h": get_cached_klines(symbol, "1h", CACHE_1H_SECONDS),
        "4h": get_cached_klines(symbol, "4h", CACHE_4H_SECONDS),
    }


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    value = sum(values[:period]) / period
    for price in values[period:]:
        value = (price - value) * k + value
    return value


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    ranges = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        ranges.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(ranges[-period:]) / period


def volume_ratio_closed(candles, period=20):
    if len(candles) < period + 2:
        return 0.0
    current_volume = candles[-2]["v"]
    previous = [c["v"] for c in candles[-(period + 2):-2]]
    avg = sum(previous) / len(previous)
    return current_volume / avg if avg > 0 else 0.0


def live_volume_ratio(candles, period=20):
    if len(candles) < period + 2:
        return 0.0, 0.0
    live = candles[-1]
    previous = [c["v"] for c in candles[-(period + 1):-1]]
    if not previous:
        return 0.0, 0.0
    avg_closed_volume = sum(previous) / len(previous)
    if avg_closed_volume <= 0:
        return 0.0, 0.0
    now_ms = int(time.time() * 1000)
    elapsed_seconds = max(0.0, min((now_ms - live["t"]) / 1000.0, 15 * 60))
    if elapsed_seconds < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return 0.0, elapsed_seconds
    fraction = max(elapsed_seconds / (15 * 60), 0.01)
    expected_volume_now = avg_closed_volume * fraction
    if expected_volume_now <= 0:
        return 0.0, elapsed_seconds
    return min(live["v"] / expected_volume_now, LIVE_VOLUME_PACE_CAP), elapsed_seconds


def candle_strength(candle, direction):
    rng = candle["h"] - candle["l"]
    if rng <= 0:
        return False
    body = abs(candle["c"] - candle["o"]) / rng
    close_pos = (candle["c"] - candle["l"]) / rng
    if direction == "LONG":
        return candle["c"] > candle["o"] and body >= 0.50 and close_pos >= 0.65
    return candle["c"] < candle["o"] and body >= 0.50 and close_pos <= 0.35


def live_reversal(candle, direction):
    rng = candle["h"] - candle["l"]
    if rng <= 0:
        return False
    if direction == "LONG":
        return candle["c"] < candle["o"] and (candle["h"] - candle["c"]) / rng >= MAX_LIVE_RETRACE
    return candle["c"] > candle["o"] and (candle["c"] - candle["l"]) / rng >= MAX_LIVE_RETRACE


def log_no_confirm(symbol, direction, reasons):
    if DIAGNOSTIC_LOGS and reasons:
        print(f"{symbol} NO CONFIRM {direction}: " + ", ".join(reasons))


def final_live_confirmation(symbol, direction, live15, level, atr15, live_volume_pace, live_elapsed_seconds):
    reasons = []
    if atr15 <= 0:
        return False, ["ATR15 non valido"]
    if REQUIRE_LIVE_DIRECTION_AFTER_HOLD:
        if direction == "LONG" and live15["c"] <= live15["o"]:
            reasons.append("candela live non verde")
        if direction == "SHORT" and live15["c"] >= live15["o"]:
            reasons.append("candela live non rossa")
    live_body_atr = abs(live15["c"] - live15["o"]) / atr15
    if live_body_atr < LIVE_BODY_ATR_MIN:
        reasons.append(f"body live {live_body_atr:.2f} ATR < {LIVE_BODY_ATR_MIN:.2f}")
    required_margin = atr15 * FINAL_BREAKOUT_MARGIN_ATR15
    if direction == "LONG" and live15["c"] < level + required_margin:
        reasons.append("continuazione LONG insufficiente")
    if direction == "SHORT" and live15["c"] > level - required_margin:
        reasons.append("continuazione SHORT insufficiente")
    live_range = live15["h"] - live15["l"]
    if live_range <= 0:
        reasons.append("range candela live non valido")
    else:
        close_position = (live15["c"] - live15["l"]) / live_range
        if direction == "LONG" and close_position < LIVE_CLOSE_POSITION_MIN:
            reasons.append(f"chiusura LONG debole ({close_position:.2f})")
        if direction == "SHORT" and close_position > 1.0 - LIVE_CLOSE_POSITION_MIN:
            reasons.append(f"chiusura SHORT debole ({close_position:.2f})")
    if live_reversal(live15, direction):
        reasons.append("inversione live post-HOLD")
    if live_elapsed_seconds < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        reasons.append("volume live ancora troppo precoce")
    elif live_volume_pace < LIVE_VOLUME_PACE_MIN:
        reasons.append(f"volume live pace {live_volume_pace:.2f}x < {LIVE_VOLUME_PACE_MIN:.2f}x")
    if reasons:
        log_no_confirm(symbol, direction, reasons)
        return False, reasons
    return True, []


def get_derivatives(symbol):
    oi_change = None
    funding = None
    try:
        data = get_json(OI_URL, {"symbol": symbol, "period": "15m", "limit": 3})
        if len(data) >= 2:
            prev = float(data[-2]["sumOpenInterestValue"])
            curr = float(data[-1]["sumOpenInterestValue"])
            if prev > 0:
                oi_change = (curr - prev) / prev * 100
    except Exception as e:
        print(symbol, "OI ERRORE:", e)
    try:
        data = get_json(PREMIUM_URL, {"symbol": symbol})
        funding = float(data["lastFundingRate"])
    except Exception as e:
        print(symbol, "FUNDING ERRORE:", e)
    return oi_change, funding


def store_liquidation(payload):
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    items = payload if isinstance(payload, list) else [payload]
    for item in items:
        if not isinstance(item, dict):
            continue
        order = item.get("o", item)
        symbol = order.get("s")
        if symbol not in SYMBOLS:
            continue
        qty = float(order.get("z") or order.get("q") or 0)
        price = float(order.get("ap") or order.get("p") or 0)
        notional = qty * price
        if notional <= 0:
            continue
        kind = "LONG_LIQ" if order.get("S") == "SELL" else "SHORT_LIQ"
        ts = int(order.get("T") or item.get("E") or time.time() * 1000) / 1000
        with liq_lock:
            liquidation_events.append({"symbol": symbol, "kind": kind, "notional": notional, "time": ts})


def ws_message(ws, message):
    try:
        store_liquidation(json.loads(message))
    except Exception as e:
        print("LIQ parse error:", e)


def ws_error(ws, error):
    print("LIQ WS error:", error)


def ws_open(ws):
    print("LIQ WS connesso")


def ws_loop():
    index = 0
    while True:
        url = LIQ_WS_URLS[index % len(LIQ_WS_URLS)]
        print("LIQ WS connessione:", url)
        try:
            ws = websocket.WebSocketApp(url, on_open=ws_open, on_message=ws_message, on_error=ws_error)
            ws.run_forever(ping_interval=120, ping_timeout=30)
        except Exception as e:
            print("LIQ WS restart:", e)
        index += 1
        time.sleep(5)


def liquidation_metrics(symbol):
    cutoff = time.time() - LIQ_WINDOW_SECONDS
    long_liq = short_liq = 0.0
    with liq_lock:
        while liquidation_events and liquidation_events[0]["time"] < cutoff:
            liquidation_events.popleft()
        for event in liquidation_events:
            if event["symbol"] != symbol:
                continue
            if event["kind"] == "LONG_LIQ":
                long_liq += event["notional"]
            else:
                short_liq += event["notional"]
    return long_liq, short_liq


def fmt_price(value):
    if value >= 1000:
        return f"{value:.2f}"
    if value >= 1:
        return f"{value:.4f}"
    return f"{value:.6f}"


def fmt_money(value):
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.0f}"


def leverage_cap(quality):
    if quality >= 13: return 100
    if quality >= 12: return 75
    if quality >= 11: return 50
    if quality >= 10: return 30
    if quality >= 9: return 20
    return 15


def calculate_leverage(entry, stop, quality, aggressive_ok):
    if entry <= 0:
        return 1
    stop_pct = abs(entry - stop) / entry
    if stop_pct <= 0:
        return 1
    raw = int(LEVERAGE_SAFETY / stop_pct)
    cap = leverage_cap(quality)
    if not aggressive_ok:
        cap = min(cap, 15)
    allowed = min(raw, cap, 100)
    selected = 1
    for step in LEVERAGE_STEPS:
        if step <= allowed:
            selected = step
        else:
            break
    return selected


def get_btc_bias(data):
    c15, c1h, c4h = data["15m"], data["1h"], data["4h"]
    closes1h = [c["c"] for c in c1h[:-1]]
    closes4h = [c["c"] for c in c4h[:-1]]
    if len(closes1h) < 51 or len(closes4h) < 51:
        return "NEUTRAL_STABLE"
    e20_1h, e50_1h = ema(closes1h, 20), ema(closes1h, 50)
    e20_4h, e50_4h = ema(closes4h, 20), ema(closes4h, 50)
    e20_prev = ema(closes1h[:-1], 20)
    atr15, atr1h = atr(c15[:-1], 14), atr(c1h[:-1], 14)
    if None in (e20_1h, e50_1h, e20_4h, e50_4h, e20_prev, atr15, atr1h):
        return "NEUTRAL_STABLE"
    last1h, last4h, live15 = c1h[-2], c4h[-2], c15[-1]
    price1h = last1h["c"]
    ema20_rising, ema20_falling = e20_1h > e20_prev, e20_1h < e20_prev
    long_1h, short_1h = price1h > e20_1h, price1h < e20_1h
    strong_long_1h = price1h > e20_1h > e50_1h and ema20_rising
    strong_short_1h = price1h < e20_1h < e50_1h and ema20_falling
    long_4h, short_4h = last4h["c"] > e20_4h, last4h["c"] < e20_4h
    volatile_live = atr15 > 0 and (live15["h"] - live15["l"]) >= atr15 * 1.40
    if strong_long_1h and long_4h: return "LONG_STRONG"
    if strong_short_1h and short_4h: return "SHORT_STRONG"
    if long_1h and ema20_rising: return "LONG_LIGHT"
    if short_1h and ema20_falling: return "SHORT_LIGHT"
    return "NEUTRAL_VOLATILE" if volatile_live else "NEUTRAL_STABLE"


def btc_direction(btc_bias):
    if btc_bias in ("LONG_LIGHT", "LONG_STRONG"): return "LONG"
    if btc_bias in ("SHORT_LIGHT", "SHORT_STRONG"): return "SHORT"
    return "NEUTRAL"


def btc_is_strong(btc_bias, direction):
    return btc_bias == ("LONG_STRONG" if direction == "LONG" else "SHORT_STRONG")


def btc_is_aligned(btc_bias, direction):
    return btc_direction(btc_bias) == direction


def btc_allows_confirmed(symbol, direction, btc_bias):
    if symbol == "BTCUSDT" or not REQUIRE_BTC_NOT_OPPOSITE:
        return True
    direction_btc = btc_direction(btc_bias)
    return direction_btc == "NEUTRAL" or direction_btc == direction


def btc_allows_aggressive(symbol, direction, btc_bias):
    if symbol == "BTCUSDT":
        return True
    if not REQUIRE_STRONG_BTC_FOR_AGGRESSIVE:
        return btc_allows_confirmed(symbol, direction, btc_bias)
    return btc_is_strong(btc_bias, direction)


def confirmation_grade(quality):
    if quality >= 12: return "MOLTO ALTO"
    if quality >= 10: return "ALTO"
    if quality >= 8: return "MEDIO-ALTO"
    return "MEDIO"


def volume_score(vol15, vol1h):
    score = 0
    if vol15 >= VOL15_SCORE_VERY_STRONG: score += 3
    elif vol15 >= VOL15_SCORE_STRONG: score += 2
    elif vol15 >= VOL15_SCORE_MEDIUM: score += 1
    if vol1h >= VOL1H_SCORE_STRONG: score += 2
    elif vol1h >= VOL1H_SCORE_MEDIUM: score += 1
    return score


def breakout_hold_check(symbol, direction, level, price, atr15, breakout_candle_time):
    now = time.time()
    key = f"{symbol}:{direction}"
    tolerance = atr15 * BREAKOUT_RETEST_TOLERANCE_ATR15
    if direction == "LONG":
        beyond_level, deeply_invalidated = price >= level, price < level - tolerance
    else:
        beyond_level, deeply_invalidated = price <= level, price > level + tolerance
    if deeply_invalidated:
        breakout_hold_state.pop(key, None)
        if DIAGNOSTIC_LOGS: print(f"{symbol} HOLD {direction} ANNULLATO: breakout riassorbito")
        return False
    state = breakout_hold_state.get(key)
    same_breakout = state is not None and state["candle_time"] == breakout_candle_time and abs(state["level"] - level) <= max(atr15 * 0.02, 1e-12)
    if not same_breakout:
        if not beyond_level:
            if DIAGNOSTIC_LOGS: print(f"{symbol} HOLD {direction} NON AVVIATO: prezzo rientrato nel livello")
            return False
        breakout_hold_state[key] = {"start": now, "level": level, "candle_time": breakout_candle_time, "passed": False}
        if DIAGNOSTIC_LOGS: print(f"{symbol} HOLD {direction} AVVIATO: attesa {BREAKOUT_HOLD_SECONDS // 60} minuti")
        return False
    if state.get("passed", False):
        if REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD and not beyond_level:
            return False
        return True
    elapsed = now - state["start"]
    if elapsed < BREAKOUT_HOLD_SECONDS:
        if DIAGNOSTIC_LOGS: print(f"{symbol} HOLD {direction}: {max(0, BREAKOUT_HOLD_SECONDS-elapsed):.0f}s rimanenti")
        return False
    if REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD and not beyond_level:
        return False
    state["passed"] = True
    if DIAGNOSTIC_LOGS: print(f"{symbol} HOLD {direction} SUPERATO")
    return True


def intelligent_stop(direction, entry, level, breakout_candle, atr15):
    if atr15 <= 0:
        return None
    min_distance, max_distance = atr15 * SL_MIN_DISTANCE_ATR15, atr15 * SL_MAX_DISTANCE_ATR15
    buffer_value = atr15 * SL_STRUCTURE_BUFFER_ATR15
    if direction == "LONG":
        technical_stop = min(breakout_candle["l"], level - buffer_value)
        distance = min(max(entry - technical_stop, min_distance), max_distance)
        stop = entry - distance
        return stop if stop < entry else None
    technical_stop = max(breakout_candle["h"], level + buffer_value)
    distance = min(max(technical_stop - entry, min_distance), max_distance)
    stop = entry + distance
    return stop if stop > entry else None


def intelligent_targets(direction, entry, stop, quality, aggressive_ok):
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    if aggressive_ok and quality >= AGGRESSIVE_SCORE_MIN:
        r1, r2, r3 = TP1_R_AGGRESSIVE, TP2_R_AGGRESSIVE, TP3_R_AGGRESSIVE
    elif quality >= 10:
        r1, r2, r3 = TP1_R_HIGH, TP2_R_HIGH, TP3_R_HIGH
    else:
        r1, r2, r3 = TP1_R_BASE, TP2_R_BASE, TP3_R_BASE
    if direction == "LONG":
        return entry+risk*r1, entry+risk*r2, entry+risk*r3, r1, r2, r3
    return entry-risk*r1, entry-risk*r2, entry-risk*r3, r1, r2, r3



def live_power_volume_ok(symbol, live15, live_volume_pace, elapsed_seconds):
    """
    V6.6.2 MOMENTUM CONTINUATION 30S
    Registra ogni lettura della candela live e distingue:
    - volume minimo >= 2.50x;
    - accelerazione vera: pace corrente >= pace precedente * 1.20;
    - prima lettura: il volume viene memorizzato, ma la via normale aspetta
      una lettura successiva per dimostrare accelerazione.
    La via eccezionale viene valutata in analyze_symbol e puo' entrare subito.
    """
    if elapsed_seconds < LIVE_POWER_MIN_ELAPSED_SECONDS:
        return False, "volume live troppo precoce", None, 0.0

    candle_time = live15["t"]
    previous = live_power_volume_state.get(symbol)

    previous_pace = 0.0
    same_candle = previous is not None and previous.get("candle_time") == candle_time
    if same_candle:
        previous_pace = float(previous.get("pace", 0.0) or 0.0)

    live_power_volume_state[symbol] = {
        "candle_time": candle_time,
        "volume": live15["v"],
        "pace": live_volume_pace,
        "price": live15["c"],
        "time": time.time(),
    }

    if live_volume_pace < LIVE_POWER_VOL_PACE_MIN:
        return False, (
            f"volume live pace {live_volume_pace:.2f}x < "
            f"{LIVE_POWER_VOL_PACE_MIN:.2f}x"
        ), previous_pace if same_candle else None, 0.0

    if not same_candle or previous_pace <= 0:
        return False, "prima lettura: attendo accelerazione", None, 0.0

    growth = live_volume_pace / previous_pace
    growth_pct = (growth - 1.0) * 100.0

    if live15["v"] <= float(previous.get("volume", 0.0) or 0.0):
        return False, "volume assoluto non in crescita", previous_pace, growth_pct

    if growth < LIVE_POWER_MIN_PACE_GROWTH:
        return False, (
            f"volume non accelera abbastanza "
            f"({previous_pace:.2f}x -> {live_volume_pace:.2f}x, {growth_pct:+.1f}%)"
        ), previous_pace, growth_pct

    return True, (
        f"ACCELERAZIONE {previous_pace:.2f}x -> "
        f"{live_volume_pace:.2f}x ({growth_pct:+.1f}%)"
    ), previous_pace, growth_pct

def live_power_candle_strength(candle, direction, atr15):
    if atr15 is None or atr15 <= 0:
        return False, 0.0, 0.0

    rng = candle["h"] - candle["l"]
    if rng <= 0:
        return False, 0.0, 0.0

    body_atr = abs(candle["c"] - candle["o"]) / atr15
    close_position = (candle["c"] - candle["l"]) / rng

    if direction == "LONG":
        ok = (
            candle["c"] > candle["o"]
            and body_atr >= LIVE_POWER_BODY_ATR_MIN
            and close_position >= LIVE_POWER_CLOSE_POSITION_MIN
        )
    else:
        ok = (
            candle["c"] < candle["o"]
            and body_atr >= LIVE_POWER_BODY_ATR_MIN
            and close_position <= 1.0 - LIVE_POWER_CLOSE_POSITION_MIN
        )

    return ok, body_atr, close_position



def rejection_wick_ok(candle, direction):
    """Wick contrario massimo = 50% del body live."""
    body = abs(candle["c"] - candle["o"])
    if body <= 0:
        return False, float("inf")
    if direction == "LONG":
        adverse_wick = max(0.0, candle["h"] - max(candle["o"], candle["c"]))
    else:
        adverse_wick = max(0.0, min(candle["o"], candle["c"]) - candle["l"])
    ratio = adverse_wick / body
    return ratio <= REJECTION_WICK_MAX_BODY_RATIO, ratio


def candle_body_high(candle):
    return max(candle["o"], candle["c"])


def candle_body_low(candle):
    return min(candle["o"], candle["c"])


def analyze_symbol(symbol, data, btc_bias):
    c15, c1h, c4h = data["15m"], data["1h"], data["4h"]
    if len(c15) < 25 or len(c1h) < 55 or len(c4h) < 55:
        return None

    live15, last1h, last4h = c15[-1], c1h[-2], c4h[-2]
    atr15, atr1h = atr(c15[:-1], 14), atr(c1h[:-1], 14)
    if atr15 is None or atr15 <= 0 or atr1h is None or atr1h <= 0:
        return None

    closes1h = [c["c"] for c in c1h[:-1]]
    closes4h = [c["c"] for c in c4h[:-1]]
    e20_1h = ema(closes1h, 20)
    e20_1h_prev = ema(closes1h[:-1], 20)
    e20_4h = ema(closes4h, 20)
    e50_4h = ema(closes4h, 50)
    if None in (e20_1h, e20_1h_prev, e20_4h, e50_4h):
        return None

    # 1H obbligatoriamente direzionale.
    trend1h_long = last1h["c"] > e20_1h and e20_1h >= e20_1h_prev
    trend1h_short = last1h["c"] < e20_1h and e20_1h <= e20_1h_prev

    # 4H: concorde o neutro ammesso; blocca solo se chiaramente opposto.
    clear_4h_long = last4h["c"] > e20_4h > e50_4h
    clear_4h_short = last4h["c"] < e20_4h < e50_4h
    four_h_allows_long = not clear_4h_short
    four_h_allows_short = not clear_4h_long
    context4h = "LONG" if clear_4h_long else "SHORT" if clear_4h_short else "NEUTRAL"

    # Breakout sui CORPI delle 2 candele chiuse precedenti, non sugli stoppini.
    structure = c15[-(LIVE_POWER_STRUCTURE_BARS + 1):-1]
    resistance = max(candle_body_high(c) for c in structure)
    support = min(candle_body_low(c) for c in structure)

    price = live15["c"]
    live_volume_pace, live_volume_elapsed = live_volume_ratio(c15)
    live_depth_long = price - resistance
    live_depth_short = support - price

    candle_long_ok, body_long_atr, close_pos_long = live_power_candle_strength(live15, "LONG", atr15)
    candle_short_ok, body_short_atr, close_pos_short = live_power_candle_strength(live15, "SHORT", atr15)
    wick_long_ok, wick_long_ratio = rejection_wick_ok(live15, "LONG")
    wick_short_ok, wick_short_ratio = rejection_wick_ok(live15, "SHORT")

    volume_ok, volume_reason, previous_volume_pace, volume_growth_pct = live_power_volume_ok(
        symbol, live15, live_volume_pace, live_volume_elapsed
    )

    # BTC stessa direzione per le altcoin.
    btc_long_ok = symbol == "BTCUSDT" or btc_direction(btc_bias) == "LONG"
    btc_short_ok = symbol == "BTCUSDT" or btc_direction(btc_bias) == "SHORT"

    # Via immediata per impulsi eccezionali: non aspetta una seconda scansione.
    exceptional_long = (
        live_volume_pace >= LIVE_POWER_EXCEPTIONAL_PACE_MIN
        and body_long_atr >= LIVE_POWER_EXCEPTIONAL_BODY_ATR_MIN
        and wick_long_ratio <= LIVE_POWER_EXCEPTIONAL_WICK_MAX
        and close_pos_long >= LIVE_POWER_EXCEPTIONAL_CLOSE_POSITION_MIN
    )
    exceptional_short = (
        live_volume_pace >= LIVE_POWER_EXCEPTIONAL_PACE_MIN
        and body_short_atr >= LIVE_POWER_EXCEPTIONAL_BODY_ATR_MIN
        and wick_short_ratio <= LIVE_POWER_EXCEPTIONAL_WICK_MAX
        and close_pos_short <= 1.0 - LIVE_POWER_EXCEPTIONAL_CLOSE_POSITION_MIN
    )

    long_ok = (
        LIVE_POWER_ENABLED and trend1h_long and four_h_allows_long and btc_long_ok
        and live_depth_long >= atr15 * LIVE_POWER_FOLLOW_THROUGH_ATR15
        and candle_long_ok and wick_long_ok and (volume_ok or exceptional_long)
    )
    short_ok = (
        LIVE_POWER_ENABLED and trend1h_short and four_h_allows_short and btc_short_ok
        and live_depth_short >= atr15 * LIVE_POWER_FOLLOW_THROUGH_ATR15
        and candle_short_ok and wick_short_ok and (volume_ok or exceptional_short)
    )

    if not long_ok and not short_ok:
        if DIAGNOSTIC_LOGS and live_volume_pace >= LIVE_POWER_VOL_PACE_MIN:
            print(
                f"{symbol} NO LIVE POWER: pace={live_volume_pace:.2f}x, "
                f"1H={'LONG' if trend1h_long else 'SHORT' if trend1h_short else 'NEUTRAL'}, "
                f"4H={context4h}, BTC={btc_bias}, "
                f"wickL={wick_long_ratio:.2f}, wickS={wick_short_ratio:.2f}"
            )
        return None

    if long_ok:
        direction, level = "LONG", resistance
        body_atr, close_position = body_long_atr, close_pos_long
        follow_atr, wick_ratio = live_depth_long / atr15, wick_long_ratio
    else:
        direction, level = "SHORT", support
        body_atr, close_position = body_short_atr, 1.0 - close_pos_short
        follow_atr, wick_ratio = live_depth_short / atr15, wick_short_ratio

    exceptional_entry = exceptional_long if direction == "LONG" else exceptional_short
    continuation_mode = "IMPULSO ECCEZIONALE" if exceptional_entry and not volume_ok else "ACCELERAZIONE"
    if exceptional_entry and not volume_ok:
        volume_reason = "impulso eccezionale: ingresso immediato"

    entry = price
    stop = intelligent_stop(direction, entry, level, live15, atr15)
    if stop is None:
        return None

    quality = 10
    if live_volume_pace >= 3.00:
        quality += 1
    if body_atr >= 0.70:
        quality += 1
    if live_volume_pace >= 3.50 and body_atr >= 0.80:
        quality += 1

    aggressive_ok = (
        live_volume_pace >= 3.50 and body_atr >= 0.80
        and quality >= AGGRESSIVE_SCORE_MIN
    )
    leverage = calculate_leverage(entry, stop, quality, aggressive_ok)
    target_data = intelligent_targets(direction, entry, stop, quality, aggressive_ok)
    if target_data is None:
        return None
    tp1, tp2, tp3, tp1_r, tp2_r, tp3_r = target_data

    return {
        "type": "CONFIRMED", "direction": direction, "price": entry,
        "entry_low": entry - atr15 * 0.08, "entry_high": entry + atr15 * 0.08,
        "sl": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "tp1_r": tp1_r, "tp2_r": tp2_r, "tp3_r": tp3_r,
        "level": level, "volume1h": volume_ratio_closed(c1h),
        "volume15": live_volume_pace, "live_volume_pace": live_volume_pace,
        "live_volume_elapsed": live_volume_elapsed,
        "atr_pct": atr1h / last1h["c"] * 100,
        "quality": quality, "leverage": leverage, "btc": btc_bias,
        "context4h": context4h, "follow_through_atr": follow_atr,
        "aggressive": leverage >= 20 and aggressive_ok,
        "entry_mode": "LIVE_POWER_WICK_GUARD", "live_power": True,
        "live_body_atr": body_atr, "live_close_position": close_position,
        "live_volume_reason": volume_reason, "rejection_wick_ratio": wick_ratio,
        "previous_volume_pace": previous_volume_pace,
        "volume_growth_pct": volume_growth_pct,
        "continuation_mode": continuation_mode,
        "score_breakdown": [
            f"VOL LIVE {live_volume_pace:.2f}x",
            f"BODY {body_atr:.2f} ATR15",
            f"WICK {wick_ratio:.2f}x BODY",
            "TREND 1H OK", f"4H {context4h} NON OPPOSTO", f"BTC {btc_bias}"
        ],
    }


def build_message(symbol, signal):
    pair = symbol.replace("USDT", "/USDT")
    aggressive = signal.get("aggressive", False)

    title = (
        "🔥 LIVE POWER AGGRESSIVO"
        if aggressive
        else "⚡ LIVE POWER CONFERMATO"
    )

    return (
        f"{title}\n"
        f"{pair} — {signal['direction']}\n\n"
        f"ENTRY: {fmt_price(signal['entry_low'])} - {fmt_price(signal['entry_high'])}\n"
        f"SL intelligente: {fmt_price(signal['sl'])}\n\n"
        f"TP1: {fmt_price(signal['tp1'])}\n"
        f"TP2: {fmt_price(signal['tp2'])}\n"
        f"TP3: {fmt_price(signal['tp3'])}\n\n"
        f"Leva indicativa: {signal['leverage']}x\n\n"
        f"Volume LIVE: {signal['live_volume_pace']:.2f}x\n"
        f"Momentum volume: {signal['continuation_mode']}\n"
        f"Accelerazione: {signal['live_volume_reason']}\n"
        f"Body LIVE: {signal['live_body_atr']:.2f} ATR15\n"
        f"Breakout: CONFERMATO\n"
        f"Rejection Wick: OK ({signal['rejection_wick_ratio']:.2f}x body)\n\n"
        f"Trend 1H: CONCORDE\n"
        f"Contesto 4H: {signal['context4h']} — NON OPPOSTO\n"
        f"BTC: {signal['btc']}\n\n"
        "Modalità ingresso: LIVE POWER\n"
        "Prima candela ancora in formazione\n"
        "Seconda candela: NON ATTESA\n"
        "HOLD: NON RICHIESTO"
    )


def should_send(symbol,signal):
    now=time.time()
    key=f"{symbol}:{signal['direction']}:{signal['type']}"
    signature=(signal["type"],signal["direction"],round(signal["level"],8))
    previous=signal_state.get(key)
    if previous and previous["signature"]==signature and now-previous["time"]<6*3600:
        return False
    signal_state[key]={"signature":signature,"time":now}
    for old_key in list(signal_state):
        if now-signal_state[old_key]["time"]>12*3600:
            del signal_state[old_key]
    return True


def cleanup_hold_state():
    now=time.time()
    for key in list(breakout_hold_state):
        if now-breakout_hold_state[key]["start"]>2*3600:
            del breakout_hold_state[key]


def scan_market():
    successful=0
    cleanup_hold_state()
    btc_data=market_data("BTCUSDT")
    btc_bias=get_btc_bias(btc_data)
    print(f"BTC regime corrente: {btc_bias}")
    for symbol in SYMBOLS:
        try:
            data=btc_data if symbol=="BTCUSDT" else market_data(symbol)
            successful+=1
            signal=analyze_symbol(symbol,data,btc_bias)
            if signal:
                print(symbol,signal["type"],signal["direction"],"quality=",signal["quality"],"leverage=",signal["leverage"],"mode=",signal.get("entry_mode","PRE"))
                if signal["type"]=="CONFIRMED" and should_send(symbol,signal):
                    send_telegram(build_message(symbol,signal))
        except Exception as e:
            print(symbol,"ERRORE:",e)
        time.sleep(0.20)
    print(f"Scansione completata: {successful}/{len(SYMBOLS)} coppie")


threading.Thread(target=ws_loop,daemon=True).start()

print("CryptoSignalAI12 avviato - V6.6.2 MOMENTUM CONTINUATION 30S")

send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "V6.6.2 MOMENTUM CONTINUATION 30S attiva.\n\n"
    "--- LOGICA MERCATO ---\n"
    "15m: ingresso sulla PRIMA candela ancora aperta.\n"
    "Volume LIVE minimo: 2.50x ritmo atteso.\n"
    "Continuazione normale: accelerazione volume >= +20% tra letture.\n"
    "Via immediata: >=3.50x + body >=0.80 ATR + wick <=20% + close >=85%.\n"
    "Body LIVE minimo: 0.45 ATR15.\n"
    "Breakout BODY minimo: 0.15 ATR15.\n"
    "Rejection Wick Guard: wick contrario max 40% del body.\n"
    "Breakout calcolato sui CORPI delle 2 candele precedenti.\n"
    "1H: direzione obbligatoria.\n"
    "4H: concorde o neutro; blocca solo se chiaramente opposto.\n"
    "BTC: stessa direzione per le altcoin.\n\n"
    "HOLD: NON RICHIESTO\n"
    "Chiusura prima candela: NON ATTESA\n"
    "Seconda candela: NON ATTESA\n"
    "Scanner: 12 coppie / ciclo ogni 30 secondi."
)

while True:
    cycle_start=time.monotonic()
    try:
        scan_market()
    except Exception as e:
        print("Errore scansione generale:",e)
    elapsed=time.monotonic()-cycle_start
    time.sleep(max(1.0,SCAN_SECONDS-elapsed))
