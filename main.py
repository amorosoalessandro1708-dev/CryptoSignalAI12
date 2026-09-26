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

SCAN_SECONDS = 60
LIQ_WINDOW_SECONDS = 15 * 60


# ==========================================================
# V6.3 SCORE - STRUTTURA
# ==========================================================

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


# ==========================================================
# VOLUME SCORE
# ==========================================================

VOL15_SCORE_MEDIUM = 1.30
VOL15_SCORE_STRONG = 1.50
VOL15_SCORE_VERY_STRONG = 2.00

VOL1H_SCORE_MEDIUM = 0.60
VOL1H_SCORE_STRONG = 1.00


# ==========================================================
# HOLD ANTI FALSE-BREAKOUT
# ==========================================================

BREAKOUT_HOLD_SECONDS = 3 * 60
BREAKOUT_RETEST_TOLERANCE_ATR15 = 0.05
REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD = True


# ==========================================================
# V6.3 - CONTINUAZIONE LIVE POST-HOLD
# ==========================================================

REQUIRE_LIVE_DIRECTION_AFTER_HOLD = True

# Prima era 0.03.
# Richiediamo una candela live leggermente più consistente.
LIVE_BODY_ATR_MIN = 0.05

# Prima era 0.02.
# Il prezzo deve avere realmente progredito oltre il breakout.
FINAL_BREAKOUT_MARGIN_ATR15 = 0.06

# NUOVO:
# LONG: chiusura almeno nel 60% superiore del range live.
# SHORT: chiusura almeno nel 40% inferiore.
LIVE_CLOSE_POSITION_MIN = 0.60

LIVE_VOLUME_PACE_MIN = 0.80
LIVE_VOLUME_MIN_ELAPSED_SECONDS = 120
LIVE_VOLUME_PACE_CAP = 4.00


# ==========================================================
# BTC
# ==========================================================

REQUIRE_BTC_NOT_OPPOSITE = True
REQUIRE_STRONG_BTC_FOR_AGGRESSIVE = True


# ==========================================================
# DIAGNOSTICA
# ==========================================================

DIAGNOSTIC_LOGS = True


# ==========================================================
# AGGRESSIVI 20X+
# ==========================================================

AGGRESSIVE_VOL_15M_MIN = 2.00
AGGRESSIVE_VOL_1H_MIN = 1.00
OI_AGGRESSIVE_MIN = 0.20

FUNDING_AGGRESSIVE = 0.0005
ATR_AGGRESSIVE_MAX_PCT = 3.00

AGGRESSIVE_SCORE_MIN = 11


# ==========================================================
# VOLATILITA
# ==========================================================

ATR_MIN_PCT = 0.10
ATR_MAX_PCT = 6.00


# ==========================================================
# ANTI-INVERSIONE / ANTI-INSEGUIMENTO
# ==========================================================

MAX_LIVE_RETRACE = 0.45
MAX_EXTENSION_ATR15 = 1.20


# ==========================================================
# STOP LOSS
# ==========================================================

SL_STRUCTURE_BUFFER_ATR15 = 0.10
SL_MIN_DISTANCE_ATR15 = 0.45
SL_MAX_DISTANCE_ATR15 = 1.35


# ==========================================================
# TAKE PROFIT
# ==========================================================

TP1_R_BASE = 0.60
TP2_R_BASE = 1.20
TP3_R_BASE = 2.00

TP1_R_HIGH = 0.70
TP2_R_HIGH = 1.40
TP3_R_HIGH = 2.40

TP1_R_AGGRESSIVE = 0.75
TP2_R_AGGRESSIVE = 1.50
TP3_R_AGGRESSIVE = 2.70


# ==========================================================
# LEVA
# ==========================================================

LEVERAGE_SAFETY = 0.35

LEVERAGE_STEPS = [
    1, 2, 3, 5, 10, 15,
    20, 25, 30, 40, 50, 75, 100
]


# ==========================================================
# PROTEZIONE BINANCE
# ==========================================================

MIN_REST_GAP_SECONDS = 0.18
MAX_RETRIES = 4
RETRY_FALLBACK_SECONDS = [3, 6, 12, 20]

session = requests.Session()

request_lock = threading.Lock()
last_rest_request = 0.0

signal_state = {}
breakout_hold_state = {}

liquidation_events = deque()
liq_lock = threading.Lock()


# ==========================================================
# TELEGRAM
# ==========================================================

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


# ==========================================================
# REST
# ==========================================================

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
                    wait = RETRY_FALLBACK_SECONDS[
                        min(attempt, len(RETRY_FALLBACK_SECONDS) - 1)
                    ]

                wait = max(wait, 2.0)

                print(
                    f"Binance {r.status_code}: "
                    f"attendo {wait:.0f}s "
                    f"({attempt + 1}/{MAX_RETRIES})"
                )

                time.sleep(wait)
                last_error = requests.HTTPError(f"HTTP {r.status_code}")
                continue

            r.raise_for_status()
            return r.json()

        except requests.RequestException as e:
            last_error = e

            if attempt >= MAX_RETRIES - 1:
                break

            wait = min(2 ** attempt, 8)

            print(
                "Errore rete Binance:",
                e,
                f"- retry tra {wait}s"
            )

            time.sleep(wait)

    raise last_error or RuntimeError("Errore Binance sconosciuto")


# ==========================================================
# MARKET DATA
# ==========================================================

def get_klines(symbol, interval, limit=120):
    return get_json(
        KLINES_URL,
        {"symbol": symbol, "interval": interval, "limit": limit}
    )


def parse_candles(raw):
    return [
        {
            "o": float(c[1]),
            "h": float(c[2]),
            "l": float(c[3]),
            "c": float(c[4]),
            "v": float(c[5]),
            "t": int(c[0]),
        }
        for c in raw
    ]


def market_data(symbol):
    return {
        "15m": parse_candles(get_klines(symbol, "15m")),
        "1h": parse_candles(get_klines(symbol, "1h")),
        "4h": parse_candles(get_klines(symbol, "4h")),
    }


# ==========================================================
# INDICATORI
# ==========================================================

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
        h = candles[i]["h"]
        l = candles[i]["l"]
        pc = candles[i - 1]["c"]

        ranges.append(max(h - l, abs(h - pc), abs(l - pc)))

    return sum(ranges[-period:]) / period


def volume_ratio_closed(candles, period=20):
    if len(candles) < period + 2:
        return 0.0

    current_volume = candles[-2]["v"]
    previous = [c["v"] for c in candles[-(period + 2):-2]]
    avg = sum(previous) / len(previous)

    if avg <= 0:
        return 0.0

    return current_volume / avg


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
    elapsed_seconds = (now_ms - live["t"]) / 1000.0
    elapsed_seconds = max(0.0, min(elapsed_seconds, 15 * 60))

    if elapsed_seconds < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return 0.0, elapsed_seconds

    fraction = elapsed_seconds / (15 * 60)
    fraction = max(fraction, 0.01)

    expected_volume_now = avg_closed_volume * fraction

    if expected_volume_now <= 0:
        return 0.0, elapsed_seconds

    ratio = live["v"] / expected_volume_now
    ratio = min(ratio, LIVE_VOLUME_PACE_CAP)

    return ratio, elapsed_seconds


def candle_strength(candle, direction):
    rng = candle["h"] - candle["l"]

    if rng <= 0:
        return False

    body = abs(candle["c"] - candle["o"]) / rng
    close_pos = (candle["c"] - candle["l"]) / rng

    if direction == "LONG":
        return (
            candle["c"] > candle["o"]
            and body >= 0.50
            and close_pos >= 0.65
        )

    return (
        candle["c"] < candle["o"]
        and body >= 0.50
        and close_pos <= 0.35
    )


# ==========================================================
# ANTI-INVERSIONE LIVE
# ==========================================================

def live_reversal(candle, direction):
    rng = candle["h"] - candle["l"]

    if rng <= 0:
        return False

    if direction == "LONG":
        retrace = (candle["h"] - candle["c"]) / rng
        bearish = candle["c"] < candle["o"]
        return bearish and retrace >= MAX_LIVE_RETRACE

    retrace = (candle["c"] - candle["l"]) / rng
    bullish = candle["c"] > candle["o"]

    return bullish and retrace >= MAX_LIVE_RETRACE


# ==========================================================
# V6.3 - CONFERMA FINALE MOMENTUM / CONTINUAZIONE LIVE
# ==========================================================

def final_live_confirmation(
    symbol,
    direction,
    live15,
    level,
    atr15,
    live_volume_pace,
    live_elapsed_seconds
):
    reasons = []

    if atr15 <= 0:
        return False, ["ATR15 non valido"]

    # ------------------------------------------------------
    # 1. DIREZIONE CANDELA LIVE
    # ------------------------------------------------------

    if REQUIRE_LIVE_DIRECTION_AFTER_HOLD:
        if direction == "LONG" and live15["c"] <= live15["o"]:
            reasons.append("candela live non verde")

        if direction == "SHORT" and live15["c"] >= live15["o"]:
            reasons.append("candela live non rossa")

    # ------------------------------------------------------
    # 2. BODY LIVE
    # ------------------------------------------------------

    live_body = abs(live15["c"] - live15["o"])
    live_body_atr = live_body / atr15

    if live_body_atr < LIVE_BODY_ATR_MIN:
        reasons.append(
            f"body live {live_body_atr:.2f} ATR "
            f"< {LIVE_BODY_ATR_MIN:.2f}"
        )

    # ------------------------------------------------------
    # 3. PROGRESSIONE REALE OLTRE BREAKOUT
    # ------------------------------------------------------

    required_margin = atr15 * FINAL_BREAKOUT_MARGIN_ATR15

    if direction == "LONG":
        if live15["c"] < level + required_margin:
            reasons.append(
                f"continuazione LONG insufficiente "
                f"(< {FINAL_BREAKOUT_MARGIN_ATR15:.2f} ATR oltre breakout)"
            )
    else:
        if live15["c"] > level - required_margin:
            reasons.append(
                f"continuazione SHORT insufficiente "
                f"(< {FINAL_BREAKOUT_MARGIN_ATR15:.2f} ATR oltre breakout)"
            )

    # ------------------------------------------------------
    # 4. POSIZIONE CHIUSURA NEL RANGE LIVE
    # ------------------------------------------------------

    live_range = live15["h"] - live15["l"]

    if live_range <= 0:
        reasons.append("range candela live non valido")
    else:
        close_position = (
            live15["c"] - live15["l"]
        ) / live_range

        if direction == "LONG":
            if close_position < LIVE_CLOSE_POSITION_MIN:
                reasons.append(
                    f"chiusura LONG debole nel range "
                    f"({close_position:.2f} < "
                    f"{LIVE_CLOSE_POSITION_MIN:.2f})"
                )
        else:
            short_max_position = 1.0 - LIVE_CLOSE_POSITION_MIN

            if close_position > short_max_position:
                reasons.append(
                    f"chiusura SHORT debole nel range "
                    f"({close_position:.2f} > "
                    f"{short_max_position:.2f})"
                )

    # ------------------------------------------------------
    # 5. ANTI-INVERSIONE
    # ------------------------------------------------------

    if live_reversal(live15, direction):
        reasons.append("inversione live post-HOLD")

    # ------------------------------------------------------
    # 6. VOLUME LIVE
    # ------------------------------------------------------

    if live_elapsed_seconds < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        reasons.append("volume live ancora troppo precoce")

    elif live_volume_pace < LIVE_VOLUME_PACE_MIN:
        reasons.append(
            f"volume live pace {live_volume_pace:.2f}x "
            f"< {LIVE_VOLUME_PACE_MIN:.2f}x"
        )

    if reasons:
        log_no_confirm(symbol, direction, reasons)
        return False, reasons

    return True, []


# ==========================================================
# DERIVATI
# ==========================================================

def get_derivatives(symbol):
    oi_change = None
    funding = None

    try:
        data = get_json(
            OI_URL,
            {"symbol": symbol, "period": "15m", "limit": 3}
        )

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


# ==========================================================
# LIQUIDAZIONI
# ==========================================================

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

        ts = int(
            order.get("T")
            or item.get("E")
            or time.time() * 1000
        ) / 1000

        with liq_lock:
            liquidation_events.append(
                {
                    "symbol": symbol,
                    "kind": kind,
                    "notional": notional,
                    "time": ts
                }
            )


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
            ws = websocket.WebSocketApp(
                url,
                on_open=ws_open,
                on_message=ws_message,
                on_error=ws_error,
            )

            ws.run_forever(
                ping_interval=120,
                ping_timeout=30
            )

        except Exception as e:
            print("LIQ WS restart:", e)

        index += 1
        time.sleep(5)


def liquidation_metrics(symbol):
    cutoff = time.time() - LIQ_WINDOW_SECONDS
    long_liq = 0.0
    short_liq = 0.0

    with liq_lock:
        while (
            liquidation_events
            and liquidation_events[0]["time"] < cutoff
        ):
            liquidation_events.popleft()

        for event in liquidation_events:
            if event["symbol"] != symbol:
                continue

            if event["kind"] == "LONG_LIQ":
                long_liq += event["notional"]
            else:
                short_liq += event["notional"]

    return long_liq, short_liq


# ==========================================================
# FORMATTAZIONE
# ==========================================================

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


# ==========================================================
# LEVA
# ==========================================================

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


# ==========================================================
# BTC REGIME V6.3
# ==========================================================

def get_btc_bias(data):
    c15 = data["15m"]
    c1h = data["1h"]
    c4h = data["4h"]

    closes1h = [c["c"] for c in c1h[:-1]]
    closes4h = [c["c"] for c in c4h[:-1]]

    if len(closes1h) < 51 or len(closes4h) < 51:
        return "NEUTRAL_STABLE"

    e20_1h = ema(closes1h, 20)
    e50_1h = ema(closes1h, 50)
    e20_4h = ema(closes4h, 20)
    e50_4h = ema(closes4h, 50)

    previous_closes = closes1h[:-1]
    e20_prev = ema(previous_closes, 20)

    atr15 = atr(c15[:-1], 14)
    atr1h = atr(c1h[:-1], 14)

    if None in (
        e20_1h,
        e50_1h,
        e20_4h,
        e50_4h,
        e20_prev,
        atr15,
        atr1h
    ):
        return "NEUTRAL_STABLE"

    last1h = c1h[-2]
    last4h = c4h[-2]
    live15 = c15[-1]

    price1h = last1h["c"]

    ema20_rising = e20_1h > e20_prev
    ema20_falling = e20_1h < e20_prev

    long_1h = price1h > e20_1h
    short_1h = price1h < e20_1h

    strong_long_1h = (
        price1h > e20_1h > e50_1h
        and ema20_rising
    )

    strong_short_1h = (
        price1h < e20_1h < e50_1h
        and ema20_falling
    )

    long_4h = last4h["c"] > e20_4h
    short_4h = last4h["c"] < e20_4h

    live_range = live15["h"] - live15["l"]

    volatile_live = (
        atr15 > 0
        and live_range >= atr15 * 1.40
    )

    if strong_long_1h and long_4h:
        return "LONG_STRONG"

    if strong_short_1h and short_4h:
        return "SHORT_STRONG"

    if long_1h and ema20_rising:
        return "LONG_LIGHT"

    if short_1h and ema20_falling:
        return "SHORT_LIGHT"

    if volatile_live:
        return "NEUTRAL_VOLATILE"

    return "NEUTRAL_STABLE"


def btc_direction(btc_bias):
    if btc_bias in ("LONG_LIGHT", "LONG_STRONG"):
        return "LONG"

    if btc_bias in ("SHORT_LIGHT", "SHORT_STRONG"):
        return "SHORT"

    return "NEUTRAL"


def btc_is_strong(btc_bias, direction):
    if direction == "LONG":
        return btc_bias == "LONG_STRONG"

    return btc_bias == "SHORT_STRONG"


def btc_is_aligned(btc_bias, direction):
    return btc_direction(btc_bias) == direction


def btc_allows_confirmed(symbol, direction, btc_bias):
    if symbol == "BTCUSDT":
        return True

    if not REQUIRE_BTC_NOT_OPPOSITE:
        return True

    direction_btc = btc_direction(btc_bias)

    if direction_btc == "NEUTRAL":
        return True

    return direction_btc == direction


def btc_allows_aggressive(symbol, direction, btc_bias):
    if symbol == "BTCUSDT":
        return True

    if not REQUIRE_STRONG_BTC_FOR_AGGRESSIVE:
        return btc_allows_confirmed(symbol, direction, btc_bias)

    return btc_is_strong(btc_bias, direction)


# ==========================================================
# GRADO CONFERMA
# ==========================================================

def confirmation_grade(quality):
    if quality >= 12:
        return "MOLTO ALTO"

    if quality >= 10:
        return "ALTO"

    if quality >= 8:
        return "MEDIO-ALTO"

    return "MEDIO"


# ==========================================================
# DIAGNOSTICA
# ==========================================================

def log_no_confirm(symbol, direction, reasons):
    if not DIAGNOSTIC_LOGS or not reasons:
        return

    print(
        f"{symbol} NO CONFIRM {direction}: "
        + ", ".join(reasons)
    )


# ==========================================================
# SCORE VOLUME
# ==========================================================

def volume_score(vol15, vol1h):
    score = 0

    if vol15 >= VOL15_SCORE_VERY_STRONG:
        score += 3
    elif vol15 >= VOL15_SCORE_STRONG:
        score += 2
    elif vol15 >= VOL15_SCORE_MEDIUM:
        score += 1

    if vol1h >= VOL1H_SCORE_STRONG:
        score += 2
    elif vol1h >= VOL1H_SCORE_MEDIUM:
        score += 1

    return score


# ==========================================================
# HOLD V6.3
# ==========================================================

def breakout_hold_check(
    symbol,
    direction,
    level,
    price,
    atr15,
    breakout_candle_time
):
    now = time.time()
    key = f"{symbol}:{direction}"
    tolerance = atr15 * BREAKOUT_RETEST_TOLERANCE_ATR15

    if direction == "LONG":
        beyond_level = price >= level
        deeply_invalidated = price < level - tolerance
    else:
        beyond_level = price <= level
        deeply_invalidated = price > level + tolerance

    if deeply_invalidated:
        if key in breakout_hold_state:
            del breakout_hold_state[key]

        if DIAGNOSTIC_LOGS:
            print(
                f"{symbol} HOLD {direction} "
                "ANNULLATO: breakout riassorbito"
            )

        return False

    state = breakout_hold_state.get(key)

    same_breakout = (
        state is not None
        and state["candle_time"] == breakout_candle_time
        and abs(state["level"] - level)
        <= max(atr15 * 0.02, 1e-12)
    )

    if not same_breakout:
        if not beyond_level:
            if DIAGNOSTIC_LOGS:
                print(
                    f"{symbol} HOLD {direction} "
                    "NON AVVIATO: prezzo rientrato nel livello"
                )

            return False

        breakout_hold_state[key] = {
            "start": now,
            "level": level,
            "candle_time": breakout_candle_time,
            "passed": False
        }

        if DIAGNOSTIC_LOGS:
            print(
                f"{symbol} HOLD {direction} AVVIATO: "
                f"attesa {BREAKOUT_HOLD_SECONDS // 60} minuti"
            )

        return False

    if state.get("passed", False):
        if REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD and not beyond_level:
            if DIAGNOSTIC_LOGS:
                print(
                    f"{symbol} HOLD {direction} "
                    "SUPERATO MA PREZZO NON OLTRE LIVELLO"
                )

            return False

        return True

    elapsed = now - state["start"]

    if elapsed < BREAKOUT_HOLD_SECONDS:
        if DIAGNOSTIC_LOGS:
            remaining = max(0, BREAKOUT_HOLD_SECONDS - elapsed)

            print(
                f"{symbol} HOLD {direction}: "
                f"{remaining:.0f}s rimanenti"
            )

        return False

    if REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD and not beyond_level:
        if DIAGNOSTIC_LOGS:
            print(
                f"{symbol} HOLD {direction} "
                "NON CONFERMATO: prezzo dentro struttura"
            )

        return False

    state["passed"] = True

    if DIAGNOSTIC_LOGS:
        print(f"{symbol} HOLD {direction} SUPERATO")

    return True


# ==========================================================
# STOP LOSS
# ==========================================================

def intelligent_stop(direction, entry, level, breakout_candle, atr15):
    if atr15 <= 0:
        return None

    min_distance = atr15 * SL_MIN_DISTANCE_ATR15
    max_distance = atr15 * SL_MAX_DISTANCE_ATR15
    buffer_value = atr15 * SL_STRUCTURE_BUFFER_ATR15

    if direction == "LONG":
        technical_stop = min(
            breakout_candle["l"],
            level - buffer_value
        )

        distance = entry - technical_stop
        distance = max(distance, min_distance)
        distance = min(distance, max_distance)
        stop = entry - distance

        if stop >= entry:
            return None

        return stop

    technical_stop = max(
        breakout_candle["h"],
        level + buffer_value
    )

    distance = technical_stop - entry
    distance = max(distance, min_distance)
    distance = min(distance, max_distance)
    stop = entry + distance

    if stop <= entry:
        return None

    return stop


# ==========================================================
# TARGET
# ==========================================================

def intelligent_targets(direction, entry, stop, quality, aggressive_ok):
    risk = abs(entry - stop)

    if risk <= 0:
        return None

    if aggressive_ok and quality >= AGGRESSIVE_SCORE_MIN:
        r1 = TP1_R_AGGRESSIVE
        r2 = TP2_R_AGGRESSIVE
        r3 = TP3_R_AGGRESSIVE
    elif quality >= 10:
        r1 = TP1_R_HIGH
        r2 = TP2_R_HIGH
        r3 = TP3_R_HIGH
    else:
        r1 = TP1_R_BASE
        r2 = TP2_R_BASE
        r3 = TP3_R_BASE

    if direction == "LONG":
        return (
            entry + risk * r1,
            entry + risk * r2,
            entry + risk * r3,
            r1, r2, r3
        )

    return (
        entry - risk * r1,
        entry - risk * r2,
        entry - risk * r3,
        r1, r2, r3
    )


# ==========================================================
# ANALISI PRINCIPALE
# ==========================================================

def analyze_symbol(symbol, data, btc_bias):
    c15 = data["15m"]
    c1h = data["1h"]
    c4h = data["4h"]

    last15 = c15[-2]
    live15 = c15[-1]

    last1h = c1h[-2]
    last4h = c4h[-2]

    closes1h = [c["c"] for c in c1h[:-1]]
    closes4h = [c["c"] for c in c4h[:-1]]

    e20_1h = ema(closes1h, 20)
    e50_1h = ema(closes1h, 50)
    e20_4h = ema(closes4h, 20)
    e50_4h = ema(closes4h, 50)

    atr15 = atr(c15[:-1], 14)
    atr1h = atr(c1h[:-1], 14)

    if None in (
        e20_1h,
        e50_1h,
        e20_4h,
        e50_4h,
        atr15,
        atr1h
    ):
        return None

    structure = c15[-(PRE_STRUCTURE_BARS + 2):-2]

    resistance = max(c["h"] for c in structure)
    support = min(c["l"] for c in structure)

    price = live15["c"]
    closed_price15 = last15["c"]
    price1h = last1h["c"]

    # ------------------------------------------------------
    # TREND
    # ------------------------------------------------------

    trend1h_long = price1h > e20_1h
    trend1h_short = price1h < e20_1h

    strong_trend1h_long = price1h > e20_1h > e50_1h
    strong_trend1h_short = price1h < e20_1h < e50_1h

    trend4h_long = last4h["c"] > e20_4h
    trend4h_short = last4h["c"] < e20_4h

    # ------------------------------------------------------
    # VOLUME / VOLATILITA
    # ------------------------------------------------------

    vol15 = volume_ratio_closed(c15)
    vol1h = volume_ratio_closed(c1h)

    live_volume_pace, live_volume_elapsed = live_volume_ratio(c15)

    atr_pct = atr1h / price1h * 100

    volatility_ok = ATR_MIN_PCT <= atr_pct <= ATR_MAX_PCT

    # ------------------------------------------------------
    # EARLY / PRE
    # ------------------------------------------------------

    distance_long = resistance - price
    distance_short = price - support

    near_long = (
        distance_long >= 0
        and distance_long <= atr15 * PRE_NEAR_ATR15
    )

    near_short = (
        distance_short >= 0
        and distance_short <= atr15 * PRE_NEAR_ATR15
    )

    early_break_long = (
        price > resistance
        and price <= resistance + atr15 * MAX_EXTENSION_ATR15
    )

    early_break_short = (
        price < support
        and price >= support - atr15 * MAX_EXTENSION_ATR15
    )

    bullish15 = last15["c"] > last15["o"]
    bearish15 = last15["c"] < last15["o"]

    pre_long = (
        trend1h_long
        and bullish15
        and (near_long or early_break_long)
        and vol15 >= PRE_VOL_15M_MIN
    )

    pre_short = (
        trend1h_short
        and bearish15
        and (near_short or early_break_short)
        and vol15 >= PRE_VOL_15M_MIN
    )

    # ------------------------------------------------------
    # BREAKOUT CHIUSO
    # ------------------------------------------------------

    breakout_long = closed_price15 > resistance
    breakout_short = closed_price15 < support

    body15 = abs(last15["c"] - last15["o"])
    body_atr = body15 / atr15 if atr15 > 0 else 0.0

    strong15_long = candle_strength(last15, "LONG")
    strong15_short = candle_strength(last15, "SHORT")

    reversing_long = live_reversal(live15, "LONG")
    reversing_short = live_reversal(live15, "SHORT")

    extension_long = price - resistance
    extension_short = support - price

    not_extended_long = extension_long <= atr15 * MAX_EXTENSION_ATR15
    not_extended_short = extension_short <= atr15 * MAX_EXTENSION_ATR15

    # ------------------------------------------------------
    # FOLLOW THROUGH
    # ------------------------------------------------------

    breakout_depth_long = closed_price15 - resistance
    breakout_depth_short = support - closed_price15

    follow_through_long = (
        bullish15
        and breakout_depth_long >= atr15 * FOLLOW_THROUGH_MIN_ATR15
    )

    follow_through_short = (
        bearish15
        and breakout_depth_short >= atr15 * FOLLOW_THROUGH_MIN_ATR15
    )

    # ------------------------------------------------------
    # REQUISITI STRUTTURALI
    # ------------------------------------------------------

    candidate_long = (
        breakout_long
        and follow_through_long
        and trend1h_long
        and vol15 >= CONFIRM_VOL_15M_FLOOR
        and vol1h >= CONFIRM_VOL_1H_FLOOR
        and body_atr >= CONFIRM_BODY_ATR_FLOOR
        and volatility_ok
        and not reversing_long
        and not_extended_long
    )

    candidate_short = (
        breakout_short
        and follow_through_short
        and trend1h_short
        and vol15 >= CONFIRM_VOL_15M_FLOOR
        and vol1h >= CONFIRM_VOL_1H_FLOOR
        and body_atr >= CONFIRM_BODY_ATR_FLOOR
        and volatility_ok
        and not reversing_short
        and not_extended_short
    )

    attempt_long = breakout_long or early_break_long
    attempt_short = breakout_short or early_break_short

    # ------------------------------------------------------
    # DIAGNOSTICA BASE
    # ------------------------------------------------------

    if attempt_long and not candidate_long:
        reasons = []

        if not breakout_long:
            reasons.append("breakout15m non ancora chiuso")

        if breakout_long and not bullish15:
            reasons.append("candela breakout non rialzista")

        if breakout_long and bullish15 and not follow_through_long:
            reasons.append(
                f"follow-through {breakout_depth_long / atr15:.2f} ATR"
            )

        if not trend1h_long:
            reasons.append("trend1H non LONG")

        if vol15 < CONFIRM_VOL_15M_FLOOR:
            reasons.append(
                f"volume15 {vol15:.2f}x "
                f"< floor {CONFIRM_VOL_15M_FLOOR:.2f}x"
            )

        if vol1h < CONFIRM_VOL_1H_FLOOR:
            reasons.append(f"volume1H {vol1h:.2f}x")

        if body_atr < CONFIRM_BODY_ATR_FLOOR:
            reasons.append(f"body/ATR {body_atr:.2f}")

        if not volatility_ok:
            reasons.append(f"ATR1H {atr_pct:.2f}%")

        if reversing_long:
            reasons.append("inversione live")

        if not not_extended_long:
            reasons.append("prezzo troppo esteso")

        log_no_confirm(symbol, "LONG", reasons)

    if attempt_short and not candidate_short:
        reasons = []

        if not breakout_short:
            reasons.append("breakout15m non ancora chiuso")

        if breakout_short and not bearish15:
            reasons.append("candela breakout non ribassista")

        if breakout_short and bearish15 and not follow_through_short:
            reasons.append(
                f"follow-through {breakout_depth_short / atr15:.2f} ATR"
            )

        if not trend1h_short:
            reasons.append("trend1H non SHORT")

        if vol15 < CONFIRM_VOL_15M_FLOOR:
            reasons.append(
                f"volume15 {vol15:.2f}x "
                f"< floor {CONFIRM_VOL_15M_FLOOR:.2f}x"
            )

        if vol1h < CONFIRM_VOL_1H_FLOOR:
            reasons.append(f"volume1H {vol1h:.2f}x")

        if body_atr < CONFIRM_BODY_ATR_FLOOR:
            reasons.append(f"body/ATR {body_atr:.2f}")

        if not volatility_ok:
            reasons.append(f"ATR1H {atr_pct:.2f}%")

        if reversing_short:
            reasons.append("inversione live")

        if not not_extended_short:
            reasons.append("prezzo troppo esteso")

        log_no_confirm(symbol, "SHORT", reasons)

    # ======================================================
    # PRE INTERNI
    # ======================================================

    if not candidate_long and not candidate_short:
        if pre_long:
            invalidation = (
                support
                if support < price
                else resistance - atr15 * 0.80
            )

            quality = 5
            quality += int(strong_trend1h_long)
            quality += int(trend4h_long)
            quality += int(vol15 >= 1.10)
            quality += int(early_break_long)

            leverage = calculate_leverage(
                price,
                invalidation,
                quality,
                False
            )

            return {
                "type": "PRE",
                "direction": "LONG",
                "price": price,
                "level": resistance,
                "invalidation": invalidation,
                "volume1h": vol1h,
                "volume15": vol15,
                "atr_pct": atr_pct,
                "quality": quality,
                "leverage": leverage,
                "early_break": early_break_long,
            }

        if pre_short:
            invalidation = (
                resistance
                if resistance > price
                else support + atr15 * 0.80
            )

            quality = 5
            quality += int(strong_trend1h_short)
            quality += int(trend4h_short)
            quality += int(vol15 >= 1.10)
            quality += int(early_break_short)

            leverage = calculate_leverage(
                price,
                invalidation,
                quality,
                False
            )

            return {
                "type": "PRE",
                "direction": "SHORT",
                "price": price,
                "level": support,
                "invalidation": invalidation,
                "volume1h": vol1h,
                "volume15": vol15,
                "atr_pct": atr_pct,
                "quality": quality,
                "leverage": leverage,
                "early_break": early_break_short,
            }

        return None

    # ======================================================
    # HOLD 3 MINUTI
    # ======================================================

    if candidate_long:
        hold_ok = breakout_hold_check(
            symbol=symbol,
            direction="LONG",
            level=resistance,
            price=price,
            atr15=atr15,
            breakout_candle_time=last15["t"]
        )
    else:
        hold_ok = breakout_hold_check(
            symbol=symbol,
            direction="SHORT",
            level=support,
            price=price,
            atr15=atr15,
            breakout_candle_time=last15["t"]
        )

    if not hold_ok:
        return None

    # ======================================================
    # DIREZIONE
    # ======================================================

    direction = "LONG" if candidate_long else "SHORT"

    # ======================================================
    # V6.3 - CONFERMA FINALE CONTINUAZIONE
    # ======================================================

    level = resistance if direction == "LONG" else support

    live_confirmation_ok, _ = final_live_confirmation(
        symbol=symbol,
        direction=direction,
        live15=live15,
        level=level,
        atr15=atr15,
        live_volume_pace=live_volume_pace,
        live_elapsed_seconds=live_volume_elapsed
    )

    if not live_confirmation_ok:
        return None

    # ======================================================
    # ANTI-INSEGUIMENTO DOPO HOLD
    # ======================================================

    post_hold_extension = (
        price - level
        if direction == "LONG"
        else level - price
    )

    if post_hold_extension > atr15 * MAX_EXTENSION_ATR15:
        log_no_confirm(
            symbol,
            direction,
            [
                f"prezzo troppo esteso dopo HOLD "
                f"({post_hold_extension / atr15:.2f} ATR15 > "
                f"{MAX_EXTENSION_ATR15:.2f} ATR15)"
            ]
        )
        return None

    # ======================================================
    # BTC
    # ======================================================

    if not btc_allows_confirmed(symbol, direction, btc_bias):
        log_no_confirm(
            symbol,
            direction,
            [f"BTC contrario ({btc_bias})"]
        )
        return None

    # ======================================================
    # DERIVATI
    # ======================================================

    oi_change, funding = get_derivatives(symbol)

    if oi_change is None or funding is None:
        reasons = []

        if oi_change is None:
            reasons.append("OI non disponibile")

        if funding is None:
            reasons.append("funding non disponibile")

        log_no_confirm(symbol, direction, reasons)
        return None

    if oi_change < OI_HARD_FLOOR:
        log_no_confirm(
            symbol,
            direction,
            [f"OI troppo debole {oi_change:+.2f}%"]
        )
        return None

    # ------------------------------------------------------
    # FUNDING HARD BLOCK
    # ------------------------------------------------------

    if direction == "LONG":
        funding_ok = funding <= FUNDING_BLOCK
    else:
        funding_ok = funding >= -FUNDING_BLOCK

    if not funding_ok:
        log_no_confirm(
            symbol,
            direction,
            [f"funding estremo {funding * 100:+.4f}%"]
        )
        return None

    # ======================================================
    # LIQUIDAZIONI
    # ======================================================

    long_liq, short_liq = liquidation_metrics(symbol)

    liq_available = long_liq + short_liq > 0

    if direction == "LONG":
        liq_support = (
            liq_available
            and short_liq >= max(long_liq * 1.25, 10000)
        )

        severe_liq_against = (
            liq_available
            and long_liq >= max(short_liq * 3.0, 50000)
        )
    else:
        liq_support = (
            liq_available
            and long_liq >= max(short_liq * 1.25, 10000)
        )

        severe_liq_against = (
            liq_available
            and short_liq >= max(long_liq * 3.0, 50000)
        )

    if severe_liq_against:
        log_no_confirm(
            symbol,
            direction,
            ["liquidazioni fortemente contrarie"]
        )
        return None

    # ======================================================
    # SCORE V6.3
    # ======================================================

    score = 0
    score_breakdown = []

    # 1) VOLUME BREAKOUT CHIUSO
    vol_score = volume_score(vol15, vol1h)
    score += vol_score

    if vol_score > 0:
        score_breakdown.append(f"VOL +{vol_score}")

    # 2) TREND 1H
    strong_trend = (
        strong_trend1h_long
        if direction == "LONG"
        else strong_trend1h_short
    )

    if strong_trend:
        score += 2
        score_breakdown.append("TREND1H +2")
    else:
        score += 1
        score_breakdown.append("TREND1H +1")

    # 3) TREND 4H
    trend4h_aligned = (
        trend4h_long
        if direction == "LONG"
        else trend4h_short
    )

    if trend4h_aligned:
        score += 1
        score_breakdown.append("4H +1")

    # 4) OI
    if oi_change >= OI_SCORE_STRONG:
        score += 2
        score_breakdown.append("OI +2")
    elif oi_change >= OI_SCORE_POSITIVE:
        score += 1
        score_breakdown.append("OI +1")

    # 5) FUNDING
    if direction == "LONG":
        funding_good = funding <= FUNDING_GOOD
    else:
        funding_good = funding >= -FUNDING_GOOD

    if funding_good:
        score += 1
        score_breakdown.append("FUNDING +1")

    # 6) BTC
    btc_aligned = btc_is_aligned(btc_bias, direction)
    btc_strong = btc_is_strong(btc_bias, direction)

    if btc_strong:
        score += 2
        score_breakdown.append("BTC +2")
    elif btc_aligned:
        score += 1
        score_breakdown.append("BTC +1")

    # 7) CANDELA BREAKOUT
    strong15 = (
        strong15_long
        if direction == "LONG"
        else strong15_short
    )

    if strong15:
        score += 1
        score_breakdown.append("CANDLE +1")

    # 8) FOLLOW-THROUGH
    follow_atr = (
        breakout_depth_long / atr15
        if direction == "LONG"
        else breakout_depth_short / atr15
    )

    if follow_atr >= 0.15:
        score += 1
        score_breakdown.append("FOLLOW +1")

    # 9) LIQUIDAZIONI
    if liq_support:
        score += 1
        score_breakdown.append("LIQ +1")

    if score < CONFIRM_SCORE_MIN:
        log_no_confirm(
            symbol,
            direction,
            [
                f"score {score}/{CONFIRM_SCORE_MIN}",
                " | ".join(score_breakdown)
            ]
        )
        return None

    # ======================================================
    # AGGRESSIVO
    # ======================================================

    btc_aggressive_ok = btc_allows_aggressive(
        symbol,
        direction,
        btc_bias
    )

    funding_aggressive = (
        funding <= FUNDING_AGGRESSIVE
        if direction == "LONG"
        else funding >= -FUNDING_AGGRESSIVE
    )

    oi_aggressive = oi_change >= OI_AGGRESSIVE_MIN

    aggressive_ok = (
        score >= AGGRESSIVE_SCORE_MIN
        and vol15 >= AGGRESSIVE_VOL_15M_MIN
        and vol1h >= AGGRESSIVE_VOL_1H_MIN
        and oi_aggressive
        and funding_aggressive
        and atr_pct <= ATR_AGGRESSIVE_MAX_PCT
        and strong15
        and btc_aggressive_ok
        and not severe_liq_against
    )

    # ======================================================
    # STOP LOSS
    # ======================================================

    entry = price

    stop = intelligent_stop(
        direction=direction,
        entry=entry,
        level=level,
        breakout_candle=last15,
        atr15=atr15
    )

    if stop is None:
        log_no_confirm(
            symbol,
            direction,
            ["stop intelligente non valido"]
        )
        return None

    risk = abs(entry - stop)

    if risk <= 0:
        return None

    # ======================================================
    # LEVA
    # ======================================================

    leverage = calculate_leverage(
        entry,
        stop,
        score,
        aggressive_ok
    )

    # ======================================================
    # TARGET
    # ======================================================

    target_data = intelligent_targets(
        direction=direction,
        entry=entry,
        stop=stop,
        quality=score,
        aggressive_ok=aggressive_ok
    )

    if target_data is None:
        return None

    tp1, tp2, tp3, tp1_r, tp2_r, tp3_r = target_data

    entry_low = entry - atr15 * 0.08
    entry_high = entry + atr15 * 0.08

    return {
        "type": "CONFIRMED",
        "direction": direction,
        "price": entry,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "sl": stop,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp1_r": tp1_r,
        "tp2_r": tp2_r,
        "tp3_r": tp3_r,
        "level": level,
        "volume1h": vol1h,
        "volume15": vol15,
        "live_volume_pace": live_volume_pace,
        "live_volume_elapsed": live_volume_elapsed,
        "atr_pct": atr_pct,
        "quality": score,
        "score_breakdown": score_breakdown,
        "leverage": leverage,
        "oi": oi_change,
        "funding": funding,
        "btc": btc_bias,
        "long_liq": long_liq,
        "short_liq": short_liq,
        "liq_available": liq_available,
        "follow_through_atr": follow_atr,
        "aggressive": leverage >= 20 and aggressive_ok,
    }


# ==========================================================
# MESSAGGIO
# ==========================================================

def build_message(symbol, signal):
    pair = symbol.replace("USDT", "/USDT")
    grade = confirmation_grade(signal["quality"])

    if signal["type"] == "PRE":
        setup = (
            "prima rottura struttura 15m"
            if signal["early_break"]
            else "avvicinamento struttura 15m"
        )

        return (
            "🟠 PRE-SEGNALE\n"
            f"{pair} — {signal['direction']}\n\n"
            f"Livello chiave: {fmt_price(signal['level'])}\n"
            f"Prezzo attuale: {fmt_price(signal['price'])}\n"
            f"Setup: {setup}\n"
            "Possibile ENTRY: solo dopo conferma\n"
            f"Invalidazione: {fmt_price(signal['invalidation'])}\n"
            f"Volume 15m: {signal['volume15']:.2f}x media\n"
            f"Volume 1H: {signal['volume1h']:.2f}x media\n"
            f"ATR 1H: {signal['atr_pct']:.2f}%\n"
            f"Leva potenziale: {signal['leverage']}x\n"
            "Timeframe: 15m / 1H\n"
            f"Grado conferma: {grade}"
        )

    title = (
        "🔥 SEGNALE CONFERMATO AGGRESSIVO"
        if signal["aggressive"]
        else "🟢 SEGNALE CONFERMATO"
    )

    funding_pct = signal["funding"] * 100

    if signal["direction"] == "LONG":
        favorable_liq = signal["short_liq"]
        adverse_liq = signal["long_liq"]
    else:
        favorable_liq = signal["long_liq"]
        adverse_liq = signal["short_liq"]

    if signal["liq_available"]:
        liq_text = (
            f"Favorevoli {fmt_money(favorable_liq)}"
            " | "
            f"Contrarie {fmt_money(adverse_liq)}"
        )
    else:
        liq_text = "In raccolta / nessun evento recente"

    score_text = " | ".join(signal["score_breakdown"])

    return (
        f"{title}\n"
        f"{pair} — {signal['direction']}\n\n"
        f"ENTRY: {fmt_price(signal['entry_low'])}"
        " - "
        f"{fmt_price(signal['entry_high'])}\n"
        f"SL intelligente: {fmt_price(signal['sl'])}\n"
        f"TP1: {fmt_price(signal['tp1'])} ({signal['tp1_r']:.2f}R)\n"
        f"TP2: {fmt_price(signal['tp2'])} ({signal['tp2_r']:.2f}R)\n"
        f"TP3: {fmt_price(signal['tp3'])} ({signal['tp3_r']:.2f}R)\n"
        f"Leva indicativa: {signal['leverage']}x\n\n"
        f"Volume breakout 15m: {signal['volume15']:.2f}x media\n"
        f"Volume 1H: {signal['volume1h']:.2f}x media\n"
        f"Volume live post-HOLD: "
        f"{signal['live_volume_pace']:.2f}x ritmo atteso\n"
        f"Open Interest 15m: {signal['oi']:+.2f}%\n"
        f"Funding: {funding_pct:+.4f}%\n"
        f"ATR 1H: {signal['atr_pct']:.2f}%\n"
        f"Follow-through: {signal['follow_through_atr']:.2f} ATR15\n"
        f"BTC: {signal['btc']}\n"
        f"Liquidazioni 15m: {liq_text}\n"
        f"Score V6.3: {signal['quality']}\n"
        f"Componenti: {score_text}\n"
        f"Grado conferma: {grade}\n"
        "HOLD breakout: SUPERATO\n"
        "Candela live successiva: CONFERMATA\n"
        "Continuazione post-HOLD: CONFERMATA\n"
        "Momentum post-HOLD: CONFERMATO\n"
        "Volume live post-HOLD: CONFERMATO\n"
        "Timeframe: 15m / 1H / 4H\n\n"
        "Nota: SL, TP e leva sono calcolati "
        "dal modello tecnico; non garantiscono "
        "l'esito dell'operazione."
    )


# ==========================================================
# ANTI-SPAM
# ==========================================================

def should_send(symbol, signal):
    now = time.time()

    key = (
        f"{symbol}:"
        f"{signal['direction']}:"
        f"{signal['type']}"
    )

    signature = (
        signal["type"],
        signal["direction"],
        round(signal["level"], 8)
    )

    previous = signal_state.get(key)

    if (
        previous
        and previous["signature"] == signature
        and now - previous["time"] < 6 * 3600
    ):
        return False

    signal_state[key] = {
        "signature": signature,
        "time": now
    }

    for old_key in list(signal_state.keys()):
        if now - signal_state[old_key]["time"] > 12 * 3600:
            del signal_state[old_key]

    return True


# ==========================================================
# PULIZIA HOLD OBSOLETI
# ==========================================================

def cleanup_hold_state():
    now = time.time()

    for key in list(breakout_hold_state.keys()):
        state = breakout_hold_state[key]

        if now - state["start"] > 2 * 3600:
            del breakout_hold_state[key]


# ==========================================================
# SCANNER
# ==========================================================

def scan_market():
    successful = 0

    cleanup_hold_state()

    btc_data = market_data("BTCUSDT")
    btc_bias = get_btc_bias(btc_data)

    print(f"BTC regime corrente: {btc_bias}")

    for symbol in SYMBOLS:
        try:
            data = (
                btc_data
                if symbol == "BTCUSDT"
                else market_data(symbol)
            )

            successful += 1

            signal = analyze_symbol(
                symbol,
                data,
                btc_bias
            )

            if signal:
                print(
                    symbol,
                    signal["type"],
                    signal["direction"],
                    "quality=",
                    signal["quality"],
                    "leverage=",
                    signal["leverage"],
                )

                if (
                    signal["type"] == "CONFIRMED"
                    and should_send(symbol, signal)
                ):
                    send_telegram(
                        build_message(symbol, signal)
                    )

        except Exception as e:
            print(symbol, "ERRORE:", e)

        time.sleep(0.20)

    print(
        f"Scansione completata: "
        f"{successful}/{len(SYMBOLS)} coppie"
    )


# ==========================================================
# AVVIO WEBSOCKET
# ==========================================================

threading.Thread(
    target=ws_loop,
    daemon=True
).start()


# ==========================================================
# STARTUP
# ==========================================================

print(
    "CryptoSignalAI12 avviato - "
    "modalita V6.3 CONTINUATION FILTER attiva"
)

send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "V6.3 CONTINUATION FILTER attiva.\n"
    "Telegram invia solo SEGNALI CONFERMATI.\n"
    "Volume breakout 15m: floor 1.15x, poi scoring.\n"
    "Volume 1H: floor 0.30x, poi scoring.\n"
    "Breakout: candela 15m chiusa obbligatoria.\n"
    "Follow-through minimo: 0.08 ATR15.\n"
    "Body breakout minimo: 0.15 ATR15.\n"
    "HOLD anti falso-breakout: 3 minuti.\n"
    "Dopo HOLD il prezzo deve restare oltre il livello.\n"
    "LONG: candela live successiva VERDE.\n"
    "SHORT: candela live successiva ROSSA.\n"
    "Body live minimo: 0.05 ATR15.\n"
    "Continuazione minima oltre breakout: 0.06 ATR15.\n"
    "Chiusura live direzionale: minimo 60% del range.\n"
    "Volume live: normalizzato per il tempo trascorso.\n"
    "Volume live pace minimo: 0.80x.\n"
    "Anti-inversione ricontrollata dopo HOLD.\n"
    "Anti-inseguimento post-HOLD: max 1.20 ATR15 dal breakout.\n"
    "Score minimo confermato: 7.\n"
    "BTC LIGHT allineato: +1 | BTC STRONG allineato: +2.\n"
    "OI hard floor: -0.10%.\n"
    "BTC contrario blocca il confermato altcoin.\n"
    "Aggressivo 20x+: filtri severi separati.\n"
    "Aggressivo: volume15 2.00x | volume1H 1.00x | OI +0.20%.\n"
    "SL intelligente: struttura + candela breakout + ATR15.\n"
    "TP intelligenti: adattati allo score e al rischio R.\n"
    "Diagnostica NO CONFIRM attiva.\n"
    "Protezione Binance 429 attiva."
)


# ==========================================================
# LOOP
# ==========================================================

while True:
    cycle_start = time.monotonic()

    try:
        scan_market()

    except Exception as e:
        print("Errore scansione generale:", e)

    elapsed = time.monotonic() - cycle_start
    sleep_time = max(1.0, SCAN_SECONDS - elapsed)

    time.sleep(sleep_time)
