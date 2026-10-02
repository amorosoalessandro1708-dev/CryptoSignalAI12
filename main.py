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

# ==========================================================
# HOLD NORMALE
# ==========================================================

BREAKOUT_HOLD_SECONDS = 2 * 60
BREAKOUT_RETEST_TOLERANCE_ATR15 = 0.05
REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD = True

REQUIRE_LIVE_DIRECTION_AFTER_HOLD = True
LIVE_BODY_ATR_MIN = 0.05
FINAL_BREAKOUT_MARGIN_ATR15 = 0.06
LIVE_CLOSE_POSITION_MIN = 0.60

LIVE_VOLUME_PACE_MIN = 1.00
LIVE_VOLUME_MIN_ELAPSED_SECONDS = 120
LIVE_VOLUME_PACE_CAP = 4.00

REQUIRE_BTC_NOT_OPPOSITE = True
REQUIRE_STRONG_BTC_FOR_AGGRESSIVE = True

DIAGNOSTIC_LOGS = True


# ==========================================================
# AGGRESSIVE 20X+
# ==========================================================

AGGRESSIVE_VOL_15M_MIN = 2.00
AGGRESSIVE_VOL_1H_MIN = 1.00
OI_AGGRESSIVE_MIN = 0.20
FUNDING_AGGRESSIVE = 0.0005
ATR_AGGRESSIVE_MAX_PCT = 3.00
AGGRESSIVE_SCORE_MIN = 11


# ==========================================================
# ATR / ANTI-CHASING
# ==========================================================

ATR_MIN_PCT = 0.10
ATR_MAX_PCT = 6.00

MAX_LIVE_RETRACE = 0.45
MAX_EXTENSION_ATR15 = 1.20

NORMAL_MAX_POST_HOLD_EXTENSION_ATR15 = 0.90
NORMAL_MAX_LIVE_RANGE_ATR15 = 1.60


# ==========================================================
# DYNAMIC IMPULSE
# ==========================================================

IMPULSE_MAX_LOOKBACK_BARS = 6
IMPULSE_MIN_BARS = 2
IMPULSE_PULLBACK_TOLERANCE_ATR15 = 0.20

NORMAL_MAX_DYNAMIC_IMPULSE_ATR15 = 3.00
NORMAL_MAX_TOTAL_MOVE_ATR15 = 3.50


# ==========================================================
# EARLY POWER V6.3.6
# ==========================================================

EARLY_STRUCTURE_BARS = 2
EARLY_VOL_15M_MIN = 1.50
EARLY_VOL_1H_MIN = 0.80
EARLY_BODY_ATR_MIN = 0.35
EARLY_FOLLOW_THROUGH_ATR15 = 0.10
EARLY_MAX_SWING_ATR15 = 2.25

EARLY_REQUIRE_4H = True
EARLY_REQUIRE_STRONG_BTC = True


# ==========================================================
# V6.3.7 FAST POWER ENTRY
#
# Percorso eccezionale:
# se LA PRIMA CANDELA CHIUSA di breakout e' molto potente,
# il segnale puo' essere confermato senza HOLD e senza
# aspettare la candela live successiva.
#
# BTC DEVE essere STRONG nella stessa direzione.
# ==========================================================

FAST_POWER_ENABLED = True

FAST_STRUCTURE_BARS = 2

# Volume della prima candela molto superiore alla media
FAST_VOL_15M_MIN = 2.00

# Anche il contesto 1H deve avere partecipazione sufficiente
FAST_VOL_1H_MIN = 0.80

# Corpo minimo della prima candela esplosiva
FAST_BODY_ATR_MIN = 0.50

# Breakout minimo oltre la struttura
FAST_FOLLOW_THROUGH_ATR15 = 0.15

# La candela deve chiudere vicino al proprio estremo
FAST_CLOSE_POSITION_MIN = 0.75

# Movimento precedente ancora giovane:
# evita di chiamare "prima candela" una candela che arriva
# quando il movimento e' gia' molto avanzato.
FAST_MAX_SWING_ATR15 = 1.75

# Manteniamo 1H forte + 4H concorde
FAST_REQUIRE_4H = True

# Come richiesto: BTC NON viene alleggerito.
# LONG -> BTC LONG_STRONG
# SHORT -> BTC SHORT_STRONG
FAST_REQUIRE_STRONG_BTC = True


# ==========================================================
# SWING GUARD
# ==========================================================

SWING_LOOKBACK_BARS = 12
NORMAL_MAX_SWING_ATR15 = 4.00


# ==========================================================
# STOP / TARGET
# ==========================================================

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


# ==========================================================
# LEVA
# ==========================================================

LEVERAGE_SAFETY = 0.35
LEVERAGE_STEPS = [
    1, 2, 3, 5, 10, 15,
    20, 25, 30, 40, 50, 75, 100
]


# ==========================================================
# REST / STATO
# ==========================================================

MIN_REST_GAP_SECONDS = 0.30
MAX_RETRIES = 4
RETRY_FALLBACK_SECONDS = [5, 10, 20, 30]

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
            data={
                "chat_id": CHAT_ID,
                "text": text
            },
            timeout=20,
        )
        r.raise_for_status()

    except Exception as e:
        print("Errore Telegram:", e)


# ==========================================================
# BINANCE REST
# ==========================================================

def wait_rest_slot():
    global last_rest_request

    with request_lock:
        now = time.monotonic()

        wait = MIN_REST_GAP_SECONDS - (
            now - last_rest_request
        )

        if wait > 0:
            time.sleep(wait)

        last_rest_request = time.monotonic()


def get_json(url, params=None, timeout=15):
    last_error = None

    for attempt in range(MAX_RETRIES):

        wait_rest_slot()

        try:
            r = session.get(
                url,
                params=params,
                timeout=timeout
            )

            if r.status_code in (418, 429):

                retry_after = r.headers.get(
                    "Retry-After"
                )

                try:
                    wait = float(retry_after)

                except (TypeError, ValueError):
                    wait = RETRY_FALLBACK_SECONDS[
                        min(
                            attempt,
                            len(RETRY_FALLBACK_SECONDS) - 1
                        )
                    ]

                wait = max(wait, 5.0)

                print(
                    f"Binance {r.status_code}: "
                    f"attendo {wait:.0f}s "
                    f"({attempt + 1}/{MAX_RETRIES})"
                )

                time.sleep(wait)

                last_error = requests.HTTPError(
                    f"HTTP {r.status_code}"
                )

                continue

            r.raise_for_status()

            return r.json()

        except requests.RequestException as e:

            last_error = e

            if attempt >= MAX_RETRIES - 1:
                break

            wait = min(
                RETRY_FALLBACK_SECONDS[
                    min(
                        attempt,
                        len(RETRY_FALLBACK_SECONDS) - 1
                    )
                ],
                30
            )

            print(
                "Errore rete Binance:",
                e,
                f"- retry tra {wait}s"
            )

            time.sleep(wait)

    raise last_error or RuntimeError(
        "Errore Binance sconosciuto"
    )


def get_klines(symbol, interval, limit=120):
    return get_json(
        KLINES_URL,
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
    )


def parse_candles(raw):
    return [
        {
            "o": float(c[1]),
            "h": float(c[2]),
            "l": float(c[3]),
            "c": float(c[4]),
            "v": float(c[5]),
            "t": int(c[0])
        }
        for c in raw
    ]


def get_cached_klines(
    symbol,
    interval,
    cache_seconds
):
    now = time.time()
    key = f"{symbol}:{interval}"

    with market_cache_lock:

        cached = market_cache.get(key)

        if (
            cached
            and now - cached["time"] < cache_seconds
        ):
            return cached["data"]

    data = parse_candles(
        get_klines(symbol, interval)
    )

    with market_cache_lock:
        market_cache[key] = {
            "time": now,
            "data": data
        }

    return data


def market_data(symbol):
    return {
        "15m": parse_candles(
            get_klines(symbol, "15m")
        ),
        "1h": get_cached_klines(
            symbol,
            "1h",
            CACHE_1H_SECONDS
        ),
        "4h": get_cached_klines(
            symbol,
            "4h",
            CACHE_4H_SECONDS
        ),
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
        value = (
            (price - value) * k
            + value
        )

    return value


def atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    ranges = []

    for i in range(1, len(candles)):

        h = candles[i]["h"]
        l = candles[i]["l"]
        pc = candles[i - 1]["c"]

        ranges.append(
            max(
                h - l,
                abs(h - pc),
                abs(l - pc)
            )
        )

    return sum(
        ranges[-period:]
    ) / period


def volume_ratio_closed(candles, period=20):

    if len(candles) < period + 2:
        return 0.0

    current_volume = candles[-2]["v"]

    previous = [
        c["v"]
        for c in candles[-(period + 2):-2]
    ]

    avg = sum(previous) / len(previous)

    return (
        current_volume / avg
        if avg > 0
        else 0.0
    )


def live_volume_ratio(candles, period=20):

    if len(candles) < period + 2:
        return 0.0, 0.0

    live = candles[-1]

    previous = [
        c["v"]
        for c in candles[-(period + 1):-1]
    ]

    if not previous:
        return 0.0, 0.0

    avg_closed_volume = (
        sum(previous) / len(previous)
    )

    if avg_closed_volume <= 0:
        return 0.0, 0.0

    now_ms = int(time.time() * 1000)

    elapsed_seconds = max(
        0.0,
        min(
            (now_ms - live["t"]) / 1000.0,
            15 * 60
        )
    )

    if elapsed_seconds < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return 0.0, elapsed_seconds

    fraction = max(
        elapsed_seconds / (15 * 60),
        0.01
    )

    expected_volume_now = (
        avg_closed_volume * fraction
    )

    if expected_volume_now <= 0:
        return 0.0, elapsed_seconds

    return (
        min(
            live["v"] / expected_volume_now,
            LIVE_VOLUME_PACE_CAP
        ),
        elapsed_seconds
    )


def candle_strength(candle, direction):

    rng = candle["h"] - candle["l"]

    if rng <= 0:
        return False

    body = (
        abs(candle["c"] - candle["o"])
        / rng
    )

    close_pos = (
        (candle["c"] - candle["l"])
        / rng
    )

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


def fast_candle_strength(
    candle,
    direction,
    atr15
):
    """
    Controllo piu' severo per FAST POWER.
    """

    if atr15 is None or atr15 <= 0:
        return False

    rng = candle["h"] - candle["l"]

    if rng <= 0:
        return False

    body_atr = (
        abs(candle["c"] - candle["o"])
        / atr15
    )

    close_position = (
        (candle["c"] - candle["l"])
        / rng
    )

    if body_atr < FAST_BODY_ATR_MIN:
        return False

    if direction == "LONG":
        return (
            candle["c"] > candle["o"]
            and close_position
            >= FAST_CLOSE_POSITION_MIN
        )

    return (
        candle["c"] < candle["o"]
        and close_position
        <= 1.0 - FAST_CLOSE_POSITION_MIN
    )


# ==========================================================
# DYNAMIC IMPULSE
# ==========================================================

def calculate_dynamic_impulse(
    candles,
    direction,
    atr15,
    max_lookback=IMPULSE_MAX_LOOKBACK_BARS
):
    """
    Rileva l'impulso recente usando da 2
    a massimo 6 candele chiuse.

    Non introduce alcuna attesa aggiuntiva.
    """

    if (
        atr15 is None
        or atr15 <= 0
        or len(candles) < 4
    ):
        return 0.0, 0.0, 0

    closed = candles[:-1]

    recent = closed[
        -min(max_lookback, len(closed)):
    ]

    if len(recent) < 2:
        return 0.0, 0.0, 0

    tolerance = (
        atr15
        * IMPULSE_PULLBACK_TOLERANCE_ATR15
    )

    start_index = len(recent) - 1

    if direction == "LONG":

        reference_low = recent[-1]["l"]

        for i in range(
            len(recent) - 2,
            -1,
            -1
        ):

            current = recent[i]
            nxt = recent[i + 1]

            progressing = (
                current["l"]
                <= reference_low + tolerance
            )

            close_not_too_high = (
                current["c"]
                <= nxt["c"] + tolerance
            )

            if (
                progressing
                and close_not_too_high
            ):
                start_index = i

                reference_low = min(
                    reference_low,
                    current["l"]
                )

            else:
                break

        bars_used = (
            len(recent) - start_index
        )

        if bars_used < IMPULSE_MIN_BARS:
            return 0.0, 0.0, bars_used

        impulse_start = min(
            c["l"]
            for c in recent[start_index:]
        )

        impulse_move = max(
            0.0,
            recent[-1]["c"]
            - impulse_start
        )

        total_move = max(
            0.0,
            candles[-1]["c"]
            - impulse_start
        )

    else:

        reference_high = recent[-1]["h"]

        for i in range(
            len(recent) - 2,
            -1,
            -1
        ):

            current = recent[i]
            nxt = recent[i + 1]

            progressing = (
                current["h"]
                >= reference_high - tolerance
            )

            close_not_too_low = (
                current["c"]
                >= nxt["c"] - tolerance
            )

            if (
                progressing
                and close_not_too_low
            ):

                start_index = i

                reference_high = max(
                    reference_high,
                    current["h"]
                )

            else:
                break

        bars_used = (
            len(recent) - start_index
        )

        if bars_used < IMPULSE_MIN_BARS:
            return 0.0, 0.0, bars_used

        impulse_start = max(
            c["h"]
            for c in recent[start_index:]
        )

        impulse_move = max(
            0.0,
            impulse_start
            - recent[-1]["c"]
        )

        total_move = max(
            0.0,
            impulse_start
            - candles[-1]["c"]
        )

    return (
        impulse_move / atr15,
        total_move / atr15,
        bars_used
    )


# ==========================================================
# SWING GUARD
# ==========================================================

def calculate_swing_excursion(
    candles,
    direction,
    atr15,
    lookback=SWING_LOOKBACK_BARS
):

    if (
        atr15 is None
        or atr15 <= 0
        or len(candles) < 4
    ):
        return 0.0

    closed = candles[:-1]

    recent = closed[
        -min(lookback, len(closed)):
    ]

    if not recent:
        return 0.0

    live_price = candles[-1]["c"]

    if direction == "LONG":

        swing_start = min(
            c["l"]
            for c in recent
        )

        move = max(
            0.0,
            live_price - swing_start
        )

    else:

        swing_start = max(
            c["h"]
            for c in recent
        )

        move = max(
            0.0,
            swing_start - live_price
        )

    return move / atr15


def calculate_closed_swing_excursion(
    candles,
    direction,
    atr15,
    lookback=SWING_LOOKBACK_BARS
):
    """
    Swing misurato sulla candela chiusa di breakout.
    Usato dal FAST POWER per evitare che la candela
    live successiva alteri la valutazione dell'ingresso.
    """

    if (
        atr15 is None
        or atr15 <= 0
        or len(candles) < 4
    ):
        return 0.0

    closed = candles[:-1]

    breakout_candle = closed[-1]

    previous = closed[
        -min(
            lookback + 1,
            len(closed)
        ):-1
    ]

    if not previous:
        return 0.0

    if direction == "LONG":

        swing_start = min(
            c["l"]
            for c in previous
        )

        move = max(
            0.0,
            breakout_candle["c"]
            - swing_start
        )

    else:

        swing_start = max(
            c["h"]
            for c in previous
        )

        move = max(
            0.0,
            swing_start
            - breakout_candle["c"]
        )

    return move / atr15


# ==========================================================
# LIVE REVERSAL / LOG
# ==========================================================

def live_reversal(candle, direction):

    rng = candle["h"] - candle["l"]

    if rng <= 0:
        return False

    if direction == "LONG":

        return (
            candle["c"] < candle["o"]
            and (
                candle["h"] - candle["c"]
            ) / rng >= MAX_LIVE_RETRACE
        )

    return (
        candle["c"] > candle["o"]
        and (
            candle["c"] - candle["l"]
        ) / rng >= MAX_LIVE_RETRACE
    )


def log_no_confirm(
    symbol,
    direction,
    reasons
):

    if DIAGNOSTIC_LOGS and reasons:

        print(
            f"{symbol} NO CONFIRM "
            f"{direction}: "
            + ", ".join(reasons)
        )


# ==========================================================
# CONFERMA LIVE NORMALE / EARLY
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
        return False, [
            "ATR15 non valido"
        ]

    if REQUIRE_LIVE_DIRECTION_AFTER_HOLD:

        if (
            direction == "LONG"
            and live15["c"] <= live15["o"]
        ):
            reasons.append(
                "candela live non verde"
            )

        if (
            direction == "SHORT"
            and live15["c"] >= live15["o"]
        ):
            reasons.append(
                "candela live non rossa"
            )

    live_body_atr = (
        abs(
            live15["c"]
            - live15["o"]
        )
        / atr15
    )

    if live_body_atr < LIVE_BODY_ATR_MIN:

        reasons.append(
            f"body live "
            f"{live_body_atr:.2f} ATR < "
            f"{LIVE_BODY_ATR_MIN:.2f}"
        )

    required_margin = (
        atr15
        * FINAL_BREAKOUT_MARGIN_ATR15
    )

    if (
        direction == "LONG"
        and live15["c"]
        < level + required_margin
    ):
        reasons.append(
            "continuazione LONG insufficiente"
        )

    if (
        direction == "SHORT"
        and live15["c"]
        > level - required_margin
    ):
        reasons.append(
            "continuazione SHORT insufficiente"
        )

    live_range = (
        live15["h"] - live15["l"]
    )

    if live_range <= 0:

        reasons.append(
            "range candela live non valido"
        )

    else:

        close_position = (
            (
                live15["c"]
                - live15["l"]
            )
            / live_range
        )

        if (
            direction == "LONG"
            and close_position
            < LIVE_CLOSE_POSITION_MIN
        ):
            reasons.append(
                f"chiusura LONG debole "
                f"({close_position:.2f})"
            )

        if (
            direction == "SHORT"
            and close_position
            > 1.0 - LIVE_CLOSE_POSITION_MIN
        ):
            reasons.append(
                f"chiusura SHORT debole "
                f"({close_position:.2f})"
            )

    if live_reversal(
        live15,
        direction
    ):
        reasons.append(
            "inversione live post-HOLD"
        )

    if (
        live_elapsed_seconds
        < LIVE_VOLUME_MIN_ELAPSED_SECONDS
    ):

        reasons.append(
            "volume live ancora troppo precoce"
        )

    elif (
        live_volume_pace
        < LIVE_VOLUME_PACE_MIN
    ):

        reasons.append(
            f"volume live pace "
            f"{live_volume_pace:.2f}x < "
            f"{LIVE_VOLUME_PACE_MIN:.2f}x"
        )

    if reasons:

        log_no_confirm(
            symbol,
            direction,
            reasons
        )

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
            {
                "symbol": symbol,
                "period": "15m",
                "limit": 3
            }
        )

        if len(data) >= 2:

            prev = float(
                data[-2][
                    "sumOpenInterestValue"
                ]
            )

            curr = float(
                data[-1][
                    "sumOpenInterestValue"
                ]
            )

            if prev > 0:

                oi_change = (
                    (curr - prev)
                    / prev
                    * 100
                )

    except Exception as e:
        print(
            symbol,
            "OI ERRORE:",
            e
        )

    try:

        data = get_json(
            PREMIUM_URL,
            {
                "symbol": symbol
            }
        )

        funding = float(
            data["lastFundingRate"]
        )

    except Exception as e:

        print(
            symbol,
            "FUNDING ERRORE:",
            e
        )

    return oi_change, funding


# ==========================================================
# LIQUIDAZIONI
# ==========================================================

def store_liquidation(payload):

    if (
        isinstance(payload, dict)
        and "data" in payload
    ):
        payload = payload["data"]

    items = (
        payload
        if isinstance(payload, list)
        else [payload]
    )

    for item in items:

        if not isinstance(item, dict):
            continue

        order = item.get(
            "o",
            item
        )

        symbol = order.get("s")

        if symbol not in SYMBOLS:
            continue

        qty = float(
            order.get("z")
            or order.get("q")
            or 0
        )

        price = float(
            order.get("ap")
            or order.get("p")
            or 0
        )

        notional = qty * price

        if notional <= 0:
            continue

        kind = (
            "LONG_LIQ"
            if order.get("S") == "SELL"
            else "SHORT_LIQ"
        )

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
        store_liquidation(
            json.loads(message)
        )

    except Exception as e:
        print(
            "LIQ parse error:",
            e
        )


def ws_error(ws, error):
    print(
        "LIQ WS error:",
        error
    )


def ws_open(ws):
    print(
        "LIQ WS connesso"
    )


def ws_loop():

    index = 0

    while True:

        url = LIQ_WS_URLS[
            index % len(LIQ_WS_URLS)
        ]

        print(
            "LIQ WS connessione:",
            url
        )

        try:

            ws = websocket.WebSocketApp(
                url,
                on_open=ws_open,
                on_message=ws_message,
                on_error=ws_error
            )

            ws.run_forever(
                ping_interval=120,
                ping_timeout=30
            )

        except Exception as e:

            print(
                "LIQ WS restart:",
                e
            )

        index += 1

        time.sleep(5)


def liquidation_metrics(symbol):

    cutoff = (
        time.time()
        - LIQ_WINDOW_SECONDS
    )

    long_liq = 0.0
    short_liq = 0.0

    with liq_lock:

        while (
            liquidation_events
            and liquidation_events[0]["time"]
            < cutoff
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
        return (
            f"${value / 1_000_000:.2f}M"
        )

    if value >= 1_000:
        return (
            f"${value / 1_000:.1f}K"
        )

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


def calculate_leverage(
    entry,
    stop,
    quality,
    aggressive_ok
):

    if entry <= 0:
        return 1

    stop_pct = (
        abs(entry - stop)
        / entry
    )

    if stop_pct <= 0:
        return 1

    raw = int(
        LEVERAGE_SAFETY
        / stop_pct
    )

    cap = leverage_cap(quality)

    if not aggressive_ok:
        cap = min(cap, 15)

    allowed = min(
        raw,
        cap,
        100
    )

    selected = 1

    for step in LEVERAGE_STEPS:

        if step <= allowed:
            selected = step

        else:
            break

    return selected


# ==========================================================
# BTC BIAS
# ==========================================================

def get_btc_bias(data):

    c15 = data["15m"]
    c1h = data["1h"]
    c4h = data["4h"]

    closes1h = [
        c["c"]
        for c in c1h[:-1]
    ]

    closes4h = [
        c["c"]
        for c in c4h[:-1]
    ]

    if (
        len(closes1h) < 51
        or len(closes4h) < 51
    ):
        return "NEUTRAL_STABLE"

    e20_1h = ema(
        closes1h,
        20
    )

    e50_1h = ema(
        closes1h,
        50
    )

    e20_4h = ema(
        closes4h,
        20
    )

    e50_4h = ema(
        closes4h,
        50
    )

    e20_prev = ema(
        closes1h[:-1],
        20
    )

    atr15 = atr(
        c15[:-1],
        14
    )

    atr1h = atr(
        c1h[:-1],
        14
    )

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

    ema20_rising = (
        e20_1h > e20_prev
    )

    ema20_falling = (
        e20_1h < e20_prev
    )

    long_1h = (
        price1h > e20_1h
    )

    short_1h = (
        price1h < e20_1h
    )

    strong_long_1h = (
        price1h
        > e20_1h
        > e50_1h
        and ema20_rising
    )

    strong_short_1h = (
        price1h
        < e20_1h
        < e50_1h
        and ema20_falling
    )

    long_4h = (
        last4h["c"] > e20_4h
    )

    short_4h = (
        last4h["c"] < e20_4h
    )

    volatile_live = (
        atr15 > 0
        and (
            live15["h"]
            - live15["l"]
        )
        >= atr15 * 1.40
    )

    if (
        strong_long_1h
        and long_4h
    ):
        return "LONG_STRONG"

    if (
        strong_short_1h
        and short_4h
    ):
        return "SHORT_STRONG"

    if (
        long_1h
        and ema20_rising
    ):
        return "LONG_LIGHT"

    if (
        short_1h
        and ema20_falling
    ):
        return "SHORT_LIGHT"

    return (
        "NEUTRAL_VOLATILE"
        if volatile_live
        else "NEUTRAL_STABLE"
    )


def btc_direction(btc_bias):

    if btc_bias in (
        "LONG_LIGHT",
        "LONG_STRONG"
    ):
        return "LONG"

    if btc_bias in (
        "SHORT_LIGHT",
        "SHORT_STRONG"
    ):
        return "SHORT"

    return "NEUTRAL"


def btc_is_strong(
    btc_bias,
    direction
):

    return btc_bias == (
        "LONG_STRONG"
        if direction == "LONG"
        else "SHORT_STRONG"
    )


def btc_is_aligned(
    btc_bias,
    direction
):

    return (
        btc_direction(btc_bias)
        == direction
    )


def btc_allows_confirmed(
    symbol,
    direction,
    btc_bias
):

    if (
        symbol == "BTCUSDT"
        or not REQUIRE_BTC_NOT_OPPOSITE
    ):
        return True

    direction_btc = (
        btc_direction(btc_bias)
    )

    return (
        direction_btc == "NEUTRAL"
        or direction_btc == direction
    )


def btc_allows_aggressive(
    symbol,
    direction,
    btc_bias
):

    if symbol == "BTCUSDT":
        return True

    if not REQUIRE_STRONG_BTC_FOR_AGGRESSIVE:

        return btc_allows_confirmed(
            symbol,
            direction,
            btc_bias
        )

    return btc_is_strong(
        btc_bias,
        direction
    )


# ==========================================================
# SCORE
# ==========================================================

def confirmation_grade(quality):

    if quality >= 12:
        return "MOLTO ALTO"

    if quality >= 10:
        return "ALTO"

    if quality >= 8:
        return "MEDIO-ALTO"

    return "MEDIO"


def volume_score(
    vol15,
    vol1h
):

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
# HOLD
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

    key = (
        f"{symbol}:{direction}"
    )

    tolerance = (
        atr15
        * BREAKOUT_RETEST_TOLERANCE_ATR15
    )

    if direction == "LONG":

        beyond_level = (
            price >= level
        )

        deeply_invalidated = (
            price
            < level - tolerance
        )

    else:

        beyond_level = (
            price <= level
        )

        deeply_invalidated = (
            price
            > level + tolerance
        )

    if deeply_invalidated:

        breakout_hold_state.pop(
            key,
            None
        )

        if DIAGNOSTIC_LOGS:

            print(
                f"{symbol} HOLD "
                f"{direction} ANNULLATO: "
                "breakout riassorbito"
            )

        return False

    state = breakout_hold_state.get(
        key
    )

    same_breakout = (
        state is not None
        and state["candle_time"]
        == breakout_candle_time
        and abs(
            state["level"] - level
        )
        <= max(
            atr15 * 0.02,
            1e-12
        )
    )

    if not same_breakout:

        if not beyond_level:

            if DIAGNOSTIC_LOGS:

                print(
                    f"{symbol} HOLD "
                    f"{direction} NON AVVIATO: "
                    "prezzo rientrato nel livello"
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
                f"{symbol} HOLD "
                f"{direction} AVVIATO: "
                f"attesa "
                f"{BREAKOUT_HOLD_SECONDS // 60} "
                "minuti"
            )

        return False

    if state.get(
        "passed",
        False
    ):

        if (
            REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD
            and not beyond_level
        ):
            return False

        return True

    elapsed = (
        now - state["start"]
    )

    if elapsed < BREAKOUT_HOLD_SECONDS:

        if DIAGNOSTIC_LOGS:

            print(
                f"{symbol} HOLD "
                f"{direction}: "
                f"{max(0, BREAKOUT_HOLD_SECONDS - elapsed):.0f}s "
                "rimanenti"
            )

        return False

    if (
        REQUIRE_PRICE_BEYOND_LEVEL_AFTER_HOLD
        and not beyond_level
    ):
        return False

    state["passed"] = True

    if DIAGNOSTIC_LOGS:

        print(
            f"{symbol} HOLD "
            f"{direction} SUPERATO"
        )

    return True


# ==========================================================
# STOP / TARGET
# ==========================================================

def intelligent_stop(
    direction,
    entry,
    level,
    breakout_candle,
    atr15
):

    if atr15 <= 0:
        return None

    min_distance = (
        atr15
        * SL_MIN_DISTANCE_ATR15
    )

    max_distance = (
        atr15
        * SL_MAX_DISTANCE_ATR15
    )

    buffer_value = (
        atr15
        * SL_STRUCTURE_BUFFER_ATR15
    )

    if direction == "LONG":

        technical_stop = min(
            breakout_candle["l"],
            level - buffer_value
        )

        distance = min(
            max(
                entry - technical_stop,
                min_distance
            ),
            max_distance
        )

        stop = entry - distance

        return (
            stop
            if stop < entry
            else None
        )

    technical_stop = max(
        breakout_candle["h"],
        level + buffer_value
    )

    distance = min(
        max(
            technical_stop - entry,
            min_distance
        ),
        max_distance
    )

    stop = entry + distance

    return (
        stop
        if stop > entry
        else None
    )


def intelligent_targets(
    direction,
    entry,
    stop,
    quality,
    aggressive_ok
):

    risk = abs(
        entry - stop
    )

    if risk <= 0:
        return None

    if (
        aggressive_ok
        and quality
        >= AGGRESSIVE_SCORE_MIN
    ):

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
            r1,
            r2,
            r3
        )

    return (
        entry - risk * r1,
        entry - risk * r2,
        entry - risk * r3,
        r1,
        r2,
        r3
    )


# ==========================================================
# ANALISI PRINCIPALE
# ==========================================================

def analyze_symbol(
    symbol,
    data,
    btc_bias
):

    c15 = data["15m"]
    c1h = data["1h"]
    c4h = data["4h"]

    last15 = c15[-2]
    live15 = c15[-1]
    last1h = c1h[-2]
    last4h = c4h[-2]

    closes1h = [
        c["c"]
        for c in c1h[:-1]
    ]

    closes4h = [
        c["c"]
        for c in c4h[:-1]
    ]

    e20_1h = ema(
        closes1h,
        20
    )

    e50_1h = ema(
        closes1h,
        50
    )

    e20_4h = ema(
        closes4h,
        20
    )

    e50_4h = ema(
        closes4h,
        50
    )

    atr15 = atr(
        c15[:-1],
        14
    )

    atr1h = atr(
        c1h[:-1],
        14
    )

    if None in (
        e20_1h,
        e50_1h,
        e20_4h,
        e50_4h,
        atr15,
        atr1h
    ):
        return None

    # ------------------------------------------------------
    # STRUTTURA NORMALE
    # ------------------------------------------------------

    structure = c15[
        -(PRE_STRUCTURE_BARS + 2):-2
    ]

    resistance = max(
        c["h"]
        for c in structure
    )

    support = min(
        c["l"]
        for c in structure
    )

    price = live15["c"]
    closed_price15 = last15["c"]
    price1h = last1h["c"]

    trend1h_long = (
        price1h > e20_1h
    )

    trend1h_short = (
        price1h < e20_1h
    )

    strong_trend1h_long = (
        price1h
        > e20_1h
        > e50_1h
    )

    strong_trend1h_short = (
        price1h
        < e20_1h
        < e50_1h
    )

    trend4h_long = (
        last4h["c"] > e20_4h
    )

    trend4h_short = (
        last4h["c"] < e20_4h
    )

    vol15 = volume_ratio_closed(
        c15
    )

    vol1h = volume_ratio_closed(
        c1h
    )

    live_volume_pace, live_volume_elapsed = (
        live_volume_ratio(c15)
    )

    atr_pct = (
        atr1h / price1h * 100
    )

    volatility_ok = (
        ATR_MIN_PCT
        <= atr_pct
        <= ATR_MAX_PCT
    )

    near_long = (
        0
        <= resistance - price
        <= atr15 * PRE_NEAR_ATR15
    )

    near_short = (
        0
        <= price - support
        <= atr15 * PRE_NEAR_ATR15
    )

    early_break_long = (
        resistance
        < price
        <= resistance
        + atr15 * MAX_EXTENSION_ATR15
    )

    early_break_short = (
        support
        > price
        >= support
        - atr15 * MAX_EXTENSION_ATR15
    )

    bullish15 = (
        last15["c"] > last15["o"]
    )

    bearish15 = (
        last15["c"] < last15["o"]
    )

    pre_long = (
        trend1h_long
        and bullish15
        and (
            near_long
            or early_break_long
        )
        and vol15
        >= PRE_VOL_15M_MIN
    )

    pre_short = (
        trend1h_short
        and bearish15
        and (
            near_short
            or early_break_short
        )
        and vol15
        >= PRE_VOL_15M_MIN
    )

    breakout_long = (
        closed_price15 > resistance
    )

    breakout_short = (
        closed_price15 < support
    )

    body_atr = (
        abs(
            last15["c"]
            - last15["o"]
        )
        / atr15
    )

    strong15_long = candle_strength(
        last15,
        "LONG"
    )

    strong15_short = candle_strength(
        last15,
        "SHORT"
    )

    reversing_long = live_reversal(
        live15,
        "LONG"
    )

    reversing_short = live_reversal(
        live15,
        "SHORT"
    )

    not_extended_long = (
        price - resistance
        <= atr15 * MAX_EXTENSION_ATR15
    )

    not_extended_short = (
        support - price
        <= atr15 * MAX_EXTENSION_ATR15
    )

    breakout_depth_long = (
        closed_price15 - resistance
    )

    breakout_depth_short = (
        support - closed_price15
    )

    follow_through_long = (
        bullish15
        and breakout_depth_long
        >= atr15
        * FOLLOW_THROUGH_MIN_ATR15
    )

    follow_through_short = (
        bearish15
        and breakout_depth_short
        >= atr15
        * FOLLOW_THROUGH_MIN_ATR15
    )

    candidate_long = (
        breakout_long
        and follow_through_long
        and trend1h_long
        and vol15
        >= CONFIRM_VOL_15M_FLOOR
        and vol1h
        >= CONFIRM_VOL_1H_FLOOR
        and body_atr
        >= CONFIRM_BODY_ATR_FLOOR
        and volatility_ok
        and not reversing_long
        and not_extended_long
    )

    candidate_short = (
        breakout_short
        and follow_through_short
        and trend1h_short
        and vol15
        >= CONFIRM_VOL_15M_FLOOR
        and vol1h
        >= CONFIRM_VOL_1H_FLOOR
        and body_atr
        >= CONFIRM_BODY_ATR_FLOOR
        and volatility_ok
        and not reversing_short
        and not_extended_short
    )

    # ------------------------------------------------------
    # EARLY POWER V6.3.6
    # ------------------------------------------------------

    early_structure = c15[
        -(EARLY_STRUCTURE_BARS + 2):-2
    ]

    early_resistance = max(
        c["h"]
        for c in early_structure
    )

    early_support = min(
        c["l"]
        for c in early_structure
    )

    early_depth_long = (
        closed_price15
        - early_resistance
    )

    early_depth_short = (
        early_support
        - closed_price15
    )

    swing_long_atr = (
        calculate_swing_excursion(
            c15,
            "LONG",
            atr15
        )
    )

    swing_short_atr = (
        calculate_swing_excursion(
            c15,
            "SHORT",
            atr15
        )
    )

    early_power_long = (
        closed_price15
        > early_resistance
        and bullish15
        and early_depth_long
        >= atr15
        * EARLY_FOLLOW_THROUGH_ATR15
        and strong_trend1h_long
        and (
            trend4h_long
            or not EARLY_REQUIRE_4H
        )
        and (
            btc_is_strong(
                btc_bias,
                "LONG"
            )
            or symbol == "BTCUSDT"
            or not EARLY_REQUIRE_STRONG_BTC
        )
        and vol15
        >= EARLY_VOL_15M_MIN
        and vol1h
        >= EARLY_VOL_1H_MIN
        and body_atr
        >= EARLY_BODY_ATR_MIN
        and strong15_long
        and volatility_ok
        and not reversing_long
        and swing_long_atr
        <= EARLY_MAX_SWING_ATR15
    )

    early_power_short = (
        closed_price15
        < early_support
        and bearish15
        and early_depth_short
        >= atr15
        * EARLY_FOLLOW_THROUGH_ATR15
        and strong_trend1h_short
        and (
            trend4h_short
            or not EARLY_REQUIRE_4H
        )
        and (
            btc_is_strong(
                btc_bias,
                "SHORT"
            )
            or symbol == "BTCUSDT"
            or not EARLY_REQUIRE_STRONG_BTC
        )
        and vol15
        >= EARLY_VOL_15M_MIN
        and vol1h
        >= EARLY_VOL_1H_MIN
        and body_atr
        >= EARLY_BODY_ATR_MIN
        and strong15_short
        and volatility_ok
        and not reversing_short
        and swing_short_atr
        <= EARLY_MAX_SWING_ATR15
    )

    # ------------------------------------------------------
    # FAST POWER V6.3.7
    #
    # Usa la PRIMA CANDELA CHIUSA.
    # Nessun HOLD.
    # Nessuna attesa della candela live successiva.
    #
    # BTC deve essere STRONG nella stessa direzione.
    # ------------------------------------------------------

    fast_structure = c15[
        -(FAST_STRUCTURE_BARS + 2):-2
    ]

    fast_resistance = max(
        c["h"]
        for c in fast_structure
    )

    fast_support = min(
        c["l"]
        for c in fast_structure
    )

    fast_depth_long = (
        closed_price15
        - fast_resistance
    )

    fast_depth_short = (
        fast_support
        - closed_price15
    )

    fast_swing_long_atr = (
        calculate_closed_swing_excursion(
            c15,
            "LONG",
            atr15
        )
    )

    fast_swing_short_atr = (
        calculate_closed_swing_excursion(
            c15,
            "SHORT",
            atr15
        )
    )

    fast_power_long = (
        FAST_POWER_ENABLED
        and closed_price15
        > fast_resistance
        and bullish15
        and fast_depth_long
        >= atr15
        * FAST_FOLLOW_THROUGH_ATR15
        and strong_trend1h_long
        and (
            trend4h_long
            or not FAST_REQUIRE_4H
        )
        and (
            btc_is_strong(
                btc_bias,
                "LONG"
            )
            or not FAST_REQUIRE_STRONG_BTC
        )
        and vol15
        >= FAST_VOL_15M_MIN
        and vol1h
        >= FAST_VOL_1H_MIN
        and fast_candle_strength(
            last15,
            "LONG",
            atr15
        )
        and volatility_ok
        and fast_swing_long_atr
        <= FAST_MAX_SWING_ATR15
    )

    fast_power_short = (
        FAST_POWER_ENABLED
        and closed_price15
        < fast_support
        and bearish15
        and fast_depth_short
        >= atr15
        * FAST_FOLLOW_THROUGH_ATR15
        and strong_trend1h_short
        and (
            trend4h_short
            or not FAST_REQUIRE_4H
        )
        and (
            btc_is_strong(
                btc_bias,
                "SHORT"
            )
            or not FAST_REQUIRE_STRONG_BTC
        )
        and vol15
        >= FAST_VOL_15M_MIN
        and vol1h
        >= FAST_VOL_1H_MIN
        and fast_candle_strength(
            last15,
            "SHORT",
            atr15
        )
        and volatility_ok
        and fast_swing_short_atr
        <= FAST_MAX_SWING_ATR15
    )

    # ------------------------------------------------------
    # NESSUN CANDIDATO -> PRE
    # ------------------------------------------------------

    if (
        not candidate_long
        and not candidate_short
        and not early_power_long
        and not early_power_short
        and not fast_power_long
        and not fast_power_short
    ):

        if pre_long:

            invalidation = (
                support
                if support < price
                else resistance
                - atr15 * 0.80
            )

            quality = (
                5
                + int(
                    strong_trend1h_long
                )
                + int(
                    trend4h_long
                )
                + int(
                    vol15 >= 1.10
                )
                + int(
                    early_break_long
                )
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
                "leverage": calculate_leverage(
                    price,
                    invalidation,
                    quality,
                    False
                ),
                "early_break": early_break_long
            }

        if pre_short:

            invalidation = (
                resistance
                if resistance > price
                else support
                + atr15 * 0.80
            )

            quality = (
                5
                + int(
                    strong_trend1h_short
                )
                + int(
                    trend4h_short
                )
                + int(
                    vol15 >= 1.10
                )
                + int(
                    early_break_short
                )
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
                "leverage": calculate_leverage(
                    price,
                    invalidation,
                    quality,
                    False
                ),
                "early_break": early_break_short
            }

        return None

    # ------------------------------------------------------
    # PRIORITA':
    # FAST POWER > EARLY POWER > NORMALE
    # ------------------------------------------------------

    fast_power = (
        fast_power_long
        or fast_power_short
    )

    early_power = False

    if fast_power_long:

        direction = "LONG"
        level = fast_resistance
        entry_mode = "FAST_POWER"

    elif fast_power_short:

        direction = "SHORT"
        level = fast_support
        entry_mode = "FAST_POWER"

    elif early_power_long:

        direction = "LONG"
        level = early_resistance
        early_power = True
        entry_mode = "EARLY_POWER"

    elif early_power_short:

        direction = "SHORT"
        level = early_support
        early_power = True
        entry_mode = "EARLY_POWER"

    else:

        direction = (
            "LONG"
            if candidate_long
            else "SHORT"
        )

        level = (
            resistance
            if direction == "LONG"
            else support
        )

        entry_mode = "NORMAL"

    breakout_depth_long = (
        closed_price15 - level
        if direction == "LONG"
        else 0.0
    )

    breakout_depth_short = (
        level - closed_price15
        if direction == "SHORT"
        else 0.0
    )

    # ------------------------------------------------------
    # FAST POWER
    # ------------------------------------------------------

    if fast_power:

        fast_swing_now = (
            fast_swing_long_atr
            if direction == "LONG"
            else fast_swing_short_atr
        )

        if DIAGNOSTIC_LOGS:

            print(
                f"{symbol} FAST POWER "
                f"{direction}: "
                f"PRIMA CANDELA FORTE, "
                f"volume15 {vol15:.2f}x, "
                f"volume1H {vol1h:.2f}x, "
                f"body {body_atr:.2f} ATR15, "
                f"swing {fast_swing_now:.2f} ATR15, "
                f"BTC {btc_bias}"
            )

        # Nel FAST POWER l'entry viene ancorata
        # alla chiusura della candela forte.
        # Non aspettiamo i 2 minuti di HOLD.
        price_for_entry = closed_price15

    else:

        price_for_entry = price

        # --------------------------------------------------
        # EARLY POWER
        # --------------------------------------------------

        if early_power:

            if DIAGNOSTIC_LOGS:

                swing_now = (
                    swing_long_atr
                    if direction == "LONG"
                    else swing_short_atr
                )

                print(
                    f"{symbol} EARLY POWER "
                    f"{direction}: "
                    "breakout 2 barre, "
                    "HOLD anticipato, "
                    f"swing "
                    f"{swing_now:.2f} ATR15"
                )

        # --------------------------------------------------
        # NORMALE
        # --------------------------------------------------

        else:

            if not breakout_hold_check(
                symbol,
                direction,
                level,
                price,
                atr15,
                last15["t"]
            ):
                return None

        # EARLY e NORMAL mantengono la
        # conferma della candela live successiva.
        ok, _ = final_live_confirmation(
            symbol,
            direction,
            live15,
            level,
            atr15,
            live_volume_pace,
            live_volume_elapsed
        )

        if not ok:
            return None

    # ------------------------------------------------------
    # ESTENSIONE / SWING GUARD
    # ------------------------------------------------------

    if fast_power:

        post_hold_extension_atr = (
            (
                price_for_entry - level
            )
            if direction == "LONG"
            else (
                level - price_for_entry
            )
        ) / atr15

        live_range_atr = (
            (
                last15["h"]
                - last15["l"]
            )
            / atr15
        )

        swing_excursion_atr = (
            fast_swing_long_atr
            if direction == "LONG"
            else fast_swing_short_atr
        )

        swing_limit = (
            FAST_MAX_SWING_ATR15
        )

    else:

        post_hold_extension_atr = (
            (
                price - level
            )
            if direction == "LONG"
            else (
                level - price
            )
        ) / atr15

        live_range_atr = (
            (
                live15["h"]
                - live15["l"]
            )
            / atr15
        )

        swing_excursion_atr = (
            calculate_swing_excursion(
                c15,
                direction,
                atr15
            )
        )

        swing_limit = (
            EARLY_MAX_SWING_ATR15
            if early_power
            else NORMAL_MAX_SWING_ATR15
        )

    if (
        swing_excursion_atr
        > swing_limit
    ):

        log_no_confirm(
            symbol,
            direction,
            [
                "SWING GUARD: ingresso tardivo",
                f"movimento dallo swing "
                f"{swing_excursion_atr:.2f} "
                f"ATR15 > "
                f"{swing_limit:.2f}"
            ]
        )

        return None

    if (
        post_hold_extension_atr
        > MAX_EXTENSION_ATR15
    ):

        log_no_confirm(
            symbol,
            direction,
            [
                f"prezzo troppo esteso "
                f"({post_hold_extension_atr:.2f} "
                "ATR15)"
            ]
        )

        return None

    if (
        not fast_power
        and post_hold_extension_atr
        > NORMAL_MAX_POST_HOLD_EXTENSION_ATR15
        and live_range_atr
        > NORMAL_MAX_LIVE_RANGE_ATR15
    ):

        log_no_confirm(
            symbol,
            direction,
            [
                "anti-esaurimento post-HOLD"
            ]
        )

        return None

    # ------------------------------------------------------
    # DYNAMIC IMPULSE
    # ------------------------------------------------------

    dynamic_impulse_atr, total_move_atr, impulse_bars = (
        calculate_dynamic_impulse(
            c15,
            direction,
            atr15
        )
    )

    if (
        impulse_bars
        >= IMPULSE_MIN_BARS
        and dynamic_impulse_atr
        > NORMAL_MAX_DYNAMIC_IMPULSE_ATR15
    ):

        log_no_confirm(
            symbol,
            direction,
            [
                "DYNAMIC IMPULSE EXHAUSTION",
                f"impulso "
                f"{dynamic_impulse_atr:.2f} "
                f"ATR15 > "
                f"{NORMAL_MAX_DYNAMIC_IMPULSE_ATR15:.2f}",
                f"{impulse_bars} "
                "candele 15m"
            ]
        )

        return None

    if (
        impulse_bars
        >= IMPULSE_MIN_BARS
        and total_move_atr
        > NORMAL_MAX_TOTAL_MOVE_ATR15
    ):

        log_no_confirm(
            symbol,
            direction,
            [
                "DYNAMIC IMPULSE EXHAUSTION",
                f"movimento totale "
                f"{total_move_atr:.2f} "
                f"ATR15 > "
                f"{NORMAL_MAX_TOTAL_MOVE_ATR15:.2f}",
                f"{impulse_bars} "
                "candele 15m"
            ]
        )

        return None

    # ------------------------------------------------------
    # BTC
    # ------------------------------------------------------

    if fast_power:

        # FAST POWER:
        # nessuna eccezione.
        # Anche BTCUSDT deve risultare STRONG
        # nella direzione rilevata dal regime BTC.
        if not btc_is_strong(
            btc_bias,
            direction
        ):

            log_no_confirm(
                symbol,
                direction,
                [
                    f"FAST POWER: "
                    f"BTC non STRONG "
                    f"({btc_bias})"
                ]
            )

            return None

    else:

        if not btc_allows_confirmed(
            symbol,
            direction,
            btc_bias
        ):

            log_no_confirm(
                symbol,
                direction,
                [
                    f"BTC contrario "
                    f"({btc_bias})"
                ]
            )

            return None

    # ------------------------------------------------------
    # OI / FUNDING
    # ------------------------------------------------------

    oi_change, funding = (
        get_derivatives(symbol)
    )

    if (
        oi_change is None
        or funding is None
    ):
        return None

    if oi_change < OI_HARD_FLOOR:

        log_no_confirm(
            symbol,
            direction,
            [
                f"OI troppo debole "
                f"{oi_change:+.2f}%"
            ]
        )

        return None

    funding_ok = (
        funding <= FUNDING_BLOCK
        if direction == "LONG"
        else funding >= -FUNDING_BLOCK
    )

    if not funding_ok:
        return None

    # ------------------------------------------------------
    # LIQUIDAZIONI
    # ------------------------------------------------------

    long_liq, short_liq = (
        liquidation_metrics(symbol)
    )

    liq_available = (
        long_liq + short_liq > 0
    )

    if direction == "LONG":

        liq_support = (
            liq_available
            and short_liq
            >= max(
                long_liq * 1.25,
                10000
            )
        )

        severe_liq_against = (
            liq_available
            and long_liq
            >= max(
                short_liq * 3.0,
                50000
            )
        )

    else:

        liq_support = (
            liq_available
            and long_liq
            >= max(
                short_liq * 1.25,
                10000
            )
        )

        severe_liq_against = (
            liq_available
            and short_liq
            >= max(
                long_liq * 3.0,
                50000
            )
        )

    if severe_liq_against:
        return None

    # ------------------------------------------------------
    # SCORE
    # ------------------------------------------------------

    score = 0
    score_breakdown = []

    vs = volume_score(
        vol15,
        vol1h
    )

    score += vs

    if vs:
        score_breakdown.append(
            f"VOL +{vs}"
        )

    strong_trend = (
        strong_trend1h_long
        if direction == "LONG"
        else strong_trend1h_short
    )

    score += (
        2
        if strong_trend
        else 1
    )

    score_breakdown.append(
        "TREND1H +2"
        if strong_trend
        else "TREND1H +1"
    )

    trend4h_aligned = (
        trend4h_long
        if direction == "LONG"
        else trend4h_short
    )

    if trend4h_aligned:

        score += 1

        score_breakdown.append(
            "4H +1"
        )

    if oi_change >= OI_SCORE_STRONG:

        score += 2

        score_breakdown.append(
            "OI +2"
        )

    elif oi_change >= OI_SCORE_POSITIVE:

        score += 1

        score_breakdown.append(
            "OI +1"
        )

    funding_good = (
        funding <= FUNDING_GOOD
        if direction == "LONG"
        else funding >= -FUNDING_GOOD
    )

    if funding_good:

        score += 1

        score_breakdown.append(
            "FUNDING +1"
        )

    if btc_is_strong(
        btc_bias,
        direction
    ):

        score += 2

        score_breakdown.append(
            "BTC +2"
        )

    elif btc_is_aligned(
        btc_bias,
        direction
    ):

        score += 1

        score_breakdown.append(
            "BTC +1"
        )

    strong15 = (
        strong15_long
        if direction == "LONG"
        else strong15_short
    )

    if strong15:

        score += 1

        score_breakdown.append(
            "CANDLE +1"
        )

    follow_atr = (
        breakout_depth_long / atr15
        if direction == "LONG"
        else breakout_depth_short / atr15
    )

    if follow_atr >= 0.15:

        score += 1

        score_breakdown.append(
            "FOLLOW +1"
        )

    if liq_support:

        score += 1

        score_breakdown.append(
            "LIQ +1"
        )

    if score < CONFIRM_SCORE_MIN:
        return None

    # ------------------------------------------------------
    # AGGRESSIVE
    # ------------------------------------------------------

    funding_aggressive = (
        funding <= FUNDING_AGGRESSIVE
        if direction == "LONG"
        else funding >= -FUNDING_AGGRESSIVE
    )

    aggressive_ok = (
        score
        >= AGGRESSIVE_SCORE_MIN
        and vol15
        >= AGGRESSIVE_VOL_15M_MIN
        and vol1h
        >= AGGRESSIVE_VOL_1H_MIN
        and oi_change
        >= OI_AGGRESSIVE_MIN
        and funding_aggressive
        and atr_pct
        <= ATR_AGGRESSIVE_MAX_PCT
        and strong15
        and btc_allows_aggressive(
            symbol,
            direction,
            btc_bias
        )
        and not severe_liq_against
    )

    # ------------------------------------------------------
    # ENTRY / SL / TP
    # ------------------------------------------------------

    entry = price_for_entry

    stop = intelligent_stop(
        direction,
        entry,
        level,
        last15,
        atr15
    )

    if stop is None:
        return None

    leverage = calculate_leverage(
        entry,
        stop,
        score,
        aggressive_ok
    )

    target_data = intelligent_targets(
        direction,
        entry,
        stop,
        score,
        aggressive_ok
    )

    if target_data is None:
        return None

    (
        tp1,
        tp2,
        tp3,
        tp1_r,
        tp2_r,
        tp3_r
    ) = target_data

    return {
        "type": "CONFIRMED",
        "direction": direction,
        "price": entry,
        "entry_low": entry - atr15 * 0.08,
        "entry_high": entry + atr15 * 0.08,
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
        "post_hold_extension_atr": post_hold_extension_atr,
        "live_range_atr": live_range_atr,
        "dynamic_impulse_atr": dynamic_impulse_atr,
        "total_move_atr": total_move_atr,
        "impulse_bars": impulse_bars,
        "aggressive": (
            leverage >= 20
            and aggressive_ok
        ),
        "entry_mode": entry_mode,
        "fast_power": fast_power,
        "early_power": early_power,
        "swing_excursion_atr": swing_excursion_atr
    }


# ==========================================================
# MESSAGGI
# ==========================================================

def build_message(
    symbol,
    signal
):

    pair = symbol.replace(
        "USDT",
        "/USDT"
    )

    grade = confirmation_grade(
        signal["quality"]
    )

    if signal["type"] == "PRE":

        setup = (
            "prima rottura struttura 15m"
            if signal["early_break"]
            else "avvicinamento struttura 15m"
        )

        return (
            f"🟠 PRE-SEGNALE\n"
            f"{pair} — "
            f"{signal['direction']}\n\n"

            f"Livello chiave: "
            f"{fmt_price(signal['level'])}\n"

            f"Prezzo attuale: "
            f"{fmt_price(signal['price'])}\n"

            f"Setup: {setup}\n"

            "Possibile ENTRY: "
            "solo dopo conferma\n"

            f"Invalidazione: "
            f"{fmt_price(signal['invalidation'])}\n"

            f"Volume 15m: "
            f"{signal['volume15']:.2f}x media\n"

            f"Volume 1H: "
            f"{signal['volume1h']:.2f}x media\n"

            f"ATR 1H: "
            f"{signal['atr_pct']:.2f}%\n"

            f"Leva potenziale: "
            f"{signal['leverage']}x\n"

            "Timeframe: 15m / 1H\n"

            f"Grado conferma: "
            f"{grade}"
        )

    if signal.get("fast_power"):

        title = (
            "⚡ FAST POWER CONFERMATO"
        )

        confirmation_text = (
            "Modalità ingresso: FAST POWER\n"
            "Prima candela 15m: FORTE\n"
            "Volume prima candela: ECCEZIONALE\n"
            "BTC STRONG concorde: CONFERMATO\n"
            "Trend 1H/4H: CONFERMATO\n"
            "HOLD 2 minuti: NON RICHIESTO\n"
            "Candela live successiva: NON ATTESA\n"
        )

    elif signal["aggressive"]:

        title = (
            "🔥 SEGNALE CONFERMATO AGGRESSIVO"
        )

        confirmation_text = (
            "Modalità ingresso: "
            f"{signal.get('entry_mode', 'NORMAL')}\n"
            "HOLD breakout: SUPERATO / ANTICIPATO\n"
            "Candela live successiva: CONFERMATA\n"
            "Continuazione: CONFERMATA\n"
            "Momentum: CONFERMATO\n"
            "Volume live: CONFERMATO\n"
        )

    else:

        title = (
            "🟢 SEGNALE CONFERMATO"
        )

        confirmation_text = (
            "Modalità ingresso: "
            f"{signal.get('entry_mode', 'NORMAL')}\n"
            "HOLD breakout: SUPERATO / ANTICIPATO\n"
            "Candela live successiva: CONFERMATA\n"
            "Continuazione: CONFERMATA\n"
            "Momentum: CONFERMATO\n"
            "Volume live: CONFERMATO\n"
        )

    funding_pct = (
        signal["funding"] * 100
    )

    if signal["direction"] == "LONG":

        favorable_liq = (
            signal["short_liq"]
        )

        adverse_liq = (
            signal["long_liq"]
        )

    else:

        favorable_liq = (
            signal["long_liq"]
        )

        adverse_liq = (
            signal["short_liq"]
        )

    liq_text = (
        f"Favorevoli "
        f"{fmt_money(favorable_liq)} "
        f"| Contrarie "
        f"{fmt_money(adverse_liq)}"
        if signal["liq_available"]
        else
        "In raccolta / nessun evento recente"
    )

    score_text = " | ".join(
        signal["score_breakdown"]
    )

    volume_live_text = (
        "NON ATTESO (FAST POWER)"
        if signal.get("fast_power")
        else
        f"{signal['live_volume_pace']:.2f}x "
        "ritmo atteso"
    )

    extension_label = (
        "Estensione al FAST ENTRY"
        if signal.get("fast_power")
        else "Estensione post-HOLD"
    )

    return (
        f"{title}\n"
        f"{pair} — "
        f"{signal['direction']}\n\n"

        f"ENTRY: "
        f"{fmt_price(signal['entry_low'])} - "
        f"{fmt_price(signal['entry_high'])}\n"

        f"SL intelligente: "
        f"{fmt_price(signal['sl'])}\n"

        f"TP1: "
        f"{fmt_price(signal['tp1'])} "
        f"({signal['tp1_r']:.2f}R)\n"

        f"TP2: "
        f"{fmt_price(signal['tp2'])} "
        f"({signal['tp2_r']:.2f}R)\n"

        f"TP3: "
        f"{fmt_price(signal['tp3'])} "
        f"({signal['tp3_r']:.2f}R)\n"

        f"Leva indicativa: "
        f"{signal['leverage']}x\n\n"

        f"Volume breakout 15m: "
        f"{signal['volume15']:.2f}x media\n"

        f"Volume 1H: "
        f"{signal['volume1h']:.2f}x media\n"

        f"Volume live: "
        f"{volume_live_text}\n"

        f"Open Interest 15m: "
        f"{signal['oi']:+.2f}%\n"

        f"Funding: "
        f"{funding_pct:+.4f}%\n"

        f"ATR 1H: "
        f"{signal['atr_pct']:.2f}%\n"

        f"Follow-through: "
        f"{signal['follow_through_atr']:.2f} "
        "ATR15\n"

        f"{extension_label}: "
        f"{signal['post_hold_extension_atr']:.2f} "
        "ATR15\n"

        f"Swing excursion: "
        f"{signal['swing_excursion_atr']:.2f} "
        "ATR15\n"

        f"BTC: "
        f"{signal['btc']}\n"

        f"Liquidazioni 15m: "
        f"{liq_text}\n"

        f"Score V6.3.7: "
        f"{signal['quality']}\n"

        f"Componenti: "
        f"{score_text}\n"

        f"Grado conferma: "
        f"{grade}\n"

        f"{confirmation_text}"

        "Anti-esaurimento: SUPERATO\n"
        "Swing Guard: SUPERATO\n"
        "Timeframe: 15m / 1H / 4H\n\n"

        "Nota: SL, TP e leva sono calcolati "
        "dal modello tecnico; "
        "non garantiscono l'esito "
        "dell'operazione."
    )


# ==========================================================
# ANTI-SPAM
# ==========================================================

def should_send(
    symbol,
    signal
):

    now = time.time()

    key = (
        f"{symbol}:"
        f"{signal['direction']}:"
        f"{signal['type']}"
    )

    signature = (
        signal["type"],
        signal["direction"],
        round(
            signal["level"],
            8
        )
    )

    previous = signal_state.get(
        key
    )

    if (
        previous
        and previous["signature"]
        == signature
        and now
        - previous["time"]
        < 6 * 3600
    ):
        return False

    signal_state[key] = {
        "signature": signature,
        "time": now
    }

    for old_key in list(
        signal_state
    ):

        if (
            now
            - signal_state[old_key]["time"]
            > 12 * 3600
        ):
            del signal_state[old_key]

    return True


def cleanup_hold_state():

    now = time.time()

    for key in list(
        breakout_hold_state
    ):

        if (
            now
            - breakout_hold_state[key]["start"]
            > 2 * 3600
        ):
            del breakout_hold_state[key]


# ==========================================================
# SCANNER
# ==========================================================

def scan_market():

    successful = 0

    cleanup_hold_state()

    btc_data = market_data(
        "BTCUSDT"
    )

    btc_bias = get_btc_bias(
        btc_data
    )

    print(
        f"BTC regime corrente: "
        f"{btc_bias}"
    )

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
                    "mode=",
                    signal.get(
                        "entry_mode",
                        "PRE"
                    )
                )

                if (
                    signal["type"]
                    == "CONFIRMED"
                    and should_send(
                        symbol,
                        signal
                    )
                ):

                    send_telegram(
                        build_message(
                            symbol,
                            signal
                        )
                    )

        except Exception as e:

            print(
                symbol,
                "ERRORE:",
                e
            )

        time.sleep(0.20)

    print(
        f"Scansione completata: "
        f"{successful}/"
        f"{len(SYMBOLS)} coppie"
    )


# ==========================================================
# AVVIO
# ==========================================================

threading.Thread(
    target=ws_loop,
    daemon=True
).start()

print(
    "CryptoSignalAI12 avviato - "
    "V6.3.7 FAST POWER ENTRY"
)

send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "V6.3.7 FAST POWER ENTRY attiva.\n"
    "15m aggiornato ad ogni scansione.\n"
    "1H e 4H ottimizzati tramite cache REST.\n"
    "Protezione Binance 429 potenziata.\n"

    "\n--- NORMALE ---\n"
    "HOLD normale anti falso-breakout: 2 minuti.\n"
    "Body live minimo: 0.05 ATR15.\n"
    "Continuazione minima: 0.06 ATR15.\n"
    "Chiusura live direzionale: minimo 60% del range.\n"
    "Volume live pace minimo: 1.00x.\n"
    "Volume breakout 15m floor: 1.15x.\n"
    "Volume 1H floor: 0.30x.\n"
    "Score minimo confermato: 7.\n"
    "OI hard floor: -0.10%.\n"

    "\n--- ANTI-ESaurimento ---\n"
    "Anti-esaurimento / anti-chasing attivo.\n"
    "Estensione normale post-HOLD: 0.90 ATR15.\n"
    "Dynamic Impulse Exhaustion attivo.\n"
    "Analisi dinamica: da 2 a massimo 6 candele 15m.\n"
    "Impulso dinamico massimo: 3.00 ATR15.\n"
    "Movimento totale massimo: 3.50 ATR15.\n"
    "Swing Guard normale: massimo 4.00 ATR15.\n"

    "\n--- EARLY POWER ---\n"
    "Struttura: 2 candele 15m.\n"
    "Volume 15m >= 1.50x, 1H >= 0.80x.\n"
    "Trend 1H forte + 4H + BTC forte.\n"
    "Swing massimo: 2.25 ATR15.\n"

    "\n--- FAST POWER ---\n"
    "Prima candela forte: ingresso anticipato.\n"
    "Volume 15m >= 2.00x.\n"
    "Volume 1H >= 0.80x.\n"
    "Body >= 0.50 ATR15.\n"
    "Breakout >= 0.15 ATR15.\n"
    "Chiusura >= 75% verso l'estremo.\n"
    "Trend 1H forte + 4H concorde.\n"
    "BTC STRONG obbligatoriamente concorde.\n"
    "Swing massimo: 1.75 ATR15.\n"
    "FAST POWER: nessun HOLD 2 minuti.\n"
    "FAST POWER: nessuna attesa candela successiva.\n"

    "\nAggressivo 20x+: parametri invariati.\n"
    "Scanner: 12 coppie / ciclo."
)


# ==========================================================
# LOOP PRINCIPALE
# ==========================================================

while True:

    cycle_start = (
        time.monotonic()
    )

    try:

        scan_market()

    except Exception as e:

        print(
            "Errore scansione generale:",
            e
        )

    elapsed = (
        time.monotonic()
        - cycle_start
    )

    time.sleep(
        max(
            1.0,
            SCAN_SECONDS - elapsed
        )
    )
