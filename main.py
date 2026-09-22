           import os
import time
import json
import threading
from collections import deque

import requests
import websocket

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

# V3: PRE più sensibili
PRE_VOL_1H_MIN = 1.05
PRE_VOL_15M_MIN = 1.10
PRE_NEAR_ATR = 0.35
PRE_BREAK_ATR = 0.40

# Confermati normali più elastici
CONFIRM_VOL_MIN = 1.20
OI_CONFIRM_MIN = -0.50

# Aggressivi 20x+ severi
AGGRESSIVE_VOL_MIN = 1.35
FUNDING_BLOCK = 0.0005
FUNDING_AGGRESSIVE = 0.0003
OI_AGGRESSIVE_MIN = 0.15

ATR_MIN_PCT = 0.10
ATR_MAX_PCT = 5.00
ATR_AGGRESSIVE_MAX_PCT = 2.50

LEVERAGE_SAFETY = 0.35
LEVERAGE_STEPS = [
    1, 2, 3, 5, 10, 15,
    20, 25, 30, 40, 50, 75, 100
]

# Protezione 429
MIN_REST_GAP_SECONDS = 0.18
MAX_RETRIES = 4
RETRY_FALLBACK_SECONDS = [3, 6, 12, 20]

session = requests.Session()
request_lock = threading.Lock()
last_rest_request = 0.0

signal_state = {}
liquidation_events = deque()
liq_lock = threading.Lock()


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


def wait_rest_slot():
    global last_rest_request

    with request_lock:
        now = time.monotonic()

        wait = (
            MIN_REST_GAP_SECONDS
            - (now - last_rest_request)
        )

        if wait > 0:
            time.sleep(wait)

        last_rest_request = time.monotonic()


def get_json(
    url,
    params=None,
    timeout=15
):
    last_error = None

    for attempt in range(
        MAX_RETRIES
    ):
        wait_rest_slot()

        try:
            r = session.get(
                url,
                params=params,
                timeout=timeout
            )

            if r.status_code in (
                418,
                429
            ):
                retry_after = (
                    r.headers.get(
                        "Retry-After"
                    )
                )

                try:
                    wait = float(
                        retry_after
                    )

                except (
                    TypeError,
                    ValueError
                ):
                    wait = (
                        RETRY_FALLBACK_SECONDS[
                            min(
                                attempt,
                                len(
                                    RETRY_FALLBACK_SECONDS
                                ) - 1
                            )
                        ]
                    )

                wait = max(
                    wait,
                    2.0
                )

                print(
                    f"Binance {r.status_code}: "
                    f"attendo {wait:.0f}s "
                    f"e riprovo "
                    f"({attempt + 1}/{MAX_RETRIES})"
                )

                time.sleep(wait)

                last_error = (
                    requests.HTTPError(
                        f"HTTP {r.status_code}"
                    )
                )

                continue

            r.raise_for_status()

            return r.json()

        except requests.RequestException as e:
            last_error = e

            if attempt >= MAX_RETRIES - 1:
                break

            wait = min(
                2 ** attempt,
                8
            )

            print(
                "Errore rete Binance:",
                e,
                f"- retry tra {wait}s"
            )

            time.sleep(wait)

    raise (
        last_error
        or RuntimeError(
            "Errore Binance sconosciuto"
        )
    )


def get_klines(
    symbol,
    interval,
    limit=120
):
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
            "t": int(c[0]),
        }
        for c in raw
    ]


def market_data(symbol):
    return {
        "15m": parse_candles(
            get_klines(
                symbol,
                "15m"
            )
        ),

        "1h": parse_candles(
            get_klines(
                symbol,
                "1h"
            )
        ),

        "4h": parse_candles(
            get_klines(
                symbol,
                "4h"
            )
        ),
    }


def ema(
    values,
    period
):
    if len(values) < period:
        return None

    k = 2 / (period + 1)

    value = (
        sum(values[:period])
        / period
    )

    for price in values[period:]:
        value = (
            (price - value)
            * k
            + value
        )

    return value


def atr(
    candles,
    period=14
):
    if len(candles) < period + 1:
        return None

    ranges = []

    for i in range(
        1,
        len(candles)
    ):
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

    return (
        sum(ranges[-period:])
        / period
    )


def volume_ratio_closed(
    candles,
    period=20
):
    if len(candles) < period + 2:
        return 0.0

    current_volume = (
        candles[-2]["v"]
    )

    previous = [
        c["v"]
        for c in
        candles[
            -(period + 2):-2
        ]
    ]

    avg = (
        sum(previous)
        / len(previous)
    )

    return (
        current_volume / avg
        if avg > 0
        else 0.0
    )


def candle_strength(
    candle,
    direction
):
    rng = (
        candle["h"]
        - candle["l"]
    )

    if rng <= 0:
        return False

    body = (
        abs(
            candle["c"]
            - candle["o"]
        )
        / rng
    )

    close_pos = (
        candle["c"]
        - candle["l"]
    ) / rng

    if direction == "LONG":
        return (
            candle["c"]
            > candle["o"]

            and body >= 0.55

            and close_pos >= 0.70
        )

    return (
        candle["c"]
        < candle["o"]

        and body >= 0.55

        and close_pos <= 0.30
    )


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
            data[
                "lastFundingRate"
            ]
        )

    except Exception as e:
        print(
            symbol,
            "FUNDING ERRORE:",
            e
        )

    return (
        oi_change,
        funding
    )


def store_liquidation(payload):
    if (
        isinstance(
            payload,
            dict
        )
        and "data" in payload
    ):
        payload = (
            payload["data"]
        )

    items = (
        payload
        if isinstance(
            payload,
            list
        )
        else [payload]
    )

    for item in items:
        if not isinstance(
            item,
            dict
        ):
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

        notional = (
            qty * price
        )

        if notional <= 0:
            continue

        kind = (
            "LONG_LIQ"
            if order.get("S")
            == "SELL"
            else "SHORT_LIQ"
        )

        ts = int(
            order.get("T")
            or item.get("E")
            or time.time() * 1000
        ) / 1000

        with liq_lock:
            liquidation_events.append({
                "symbol": symbol,
                "kind": kind,
                "notional": notional,
                "time": ts
            })


def ws_message(
    ws,
    message
):
    try:
        store_liquidation(
            json.loads(
                message
            )
        )

    except Exception as e:
        print(
            "LIQ parse error:",
            e
        )


def ws_error(
    ws,
    error
):
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
        url = (
            LIQ_WS_URLS[
                index
                % len(
                    LIQ_WS_URLS
                )
            ]
        )

        print(
            "LIQ WS connessione:",
            url
        )

        try:
            ws = (
                websocket.WebSocketApp(
                    url,
                    on_open=ws_open,
                    on_message=ws_message,
                    on_error=ws_error,
                )
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
            and liquidation_events[0][
                "time"
            ] < cutoff
        ):
            liquidation_events.popleft()

        for event in liquidation_events:
            if (
                event["symbol"]
                != symbol
            ):
                continue

            if (
                event["kind"]
                == "LONG_LIQ"
            ):
                long_liq += (
                    event[
                        "notional"
                    ]
                )

            else:
                short_liq += (
                    event[
                        "notional"
                    ]
                )

    return (
        long_liq,
        short_liq
    )


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
        abs(
            entry
            - stop
        )
        / entry
    )

    if stop_pct <= 0:
        return 1

    raw = int(
        LEVERAGE_SAFETY
        / stop_pct
    )

    cap = leverage_cap(
        quality
    )

    if not aggressive_ok:
        cap = min(
            cap,
            15
        )

    allowed = min(
        raw,
        cap,
        100
    )

    selected = 1

    for step in (
        LEVERAGE_STEPS
    ):
        if step <= allowed:
            selected = step
        else:
            break

    return selected


def get_btc_bias(data):
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

    if None in (
        e20_1h,
        e50_1h,
        e20_4h,
        e50_4h
    ):
        return "NEUTRAL"

    p1 = c1h[-2]["c"]
    p4 = c4h[-2]["c"]

    if (
        p1 > e20_1h > e50_1h
        and
        p4 > e20_4h > e50_4h
    ):
        return "LONG"

    if (
        p1 < e20_1h < e50_1h
        and
        p4 < e20_4h < e50_4h
    ):
        return "SHORT"

    return "NEUTRAL"


def confirmation_grade(
    quality
):
    if quality >= 10:
        return "ALTO"

    if quality >= 8:
        return "MEDIO-ALTO"

    return "MEDIO"


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

    current_atr = atr(
        c1h[:-1],
        14
    )

    vol1h = (
        volume_ratio_closed(
            c1h
        )
    )

    vol15 = (
        volume_ratio_closed(
            c15
        )
    )

    if None in (
        e20_1h,
        e50_1h,
        e20_4h,
        e50_4h,
        current_atr
    ):
        return None

    previous_20 = (
        c1h[-22:-2]
    )

    resistance = max(
        c["h"]
        for c in previous_20
    )

    support = min(
        c["l"]
        for c in previous_20
    )

    price1h = (
        last1h["c"]
    )

    price15 = (
        last15["c"]
    )

    live_price = (
        live15["c"]
    )

    trend1h_long = (
        price1h
        > e20_1h
        > e50_1h
    )

    trend1h_short = (
        price1h
        < e20_1h
        < e50_1h
    )

    trend4h_long = (
        last4h["c"]
        > e20_4h
        > e50_4h
    )

    trend4h_short = (
        last4h["c"]
        < e20_4h
        < e50_4h
    )

    breakout1h_long = (
        price1h
        > resistance
    )

    breakout1h_short = (
        price1h
        < support
    )

    atr_pct = (
        current_atr
        / price1h
        * 100
    )

    volatility_ok = (
        ATR_MIN_PCT
        <= atr_pct
        <= ATR_MAX_PCT
    )

    retest_window = (
        c15[-4:-1]
    )

    tolerance = (
        current_atr
        * 0.12
    )

    retest_long = any(
        c["l"]
        <= resistance
        + tolerance

        and

        c["c"]
        > resistance

        for c in retest_window
    )

    retest_short = any(
        c["h"]
        >= support
        - tolerance

        and

        c["c"]
        < support

        for c in retest_window
    )

    not_extended_long = (
        live_price
        <= resistance
        + current_atr * 0.80
    )

    not_extended_short = (
        live_price
        >= support
        - current_atr * 0.80
    )

    strong_candle_long = (
        candle_strength(
            last1h,
            "LONG"
        )
    )

    strong_candle_short = (
        candle_strength(
            last1h,
            "SHORT"
        )
    )

    # PRE più sensibili:
    # vicino al livello O appena oltre il livello su 15m

    near_long = (
        abs(
            live_price
            - resistance
        )
        <= current_atr
        * PRE_NEAR_ATR
    )

    near_short = (
        abs(
            live_price
            - support
        )
        <= current_atr
        * PRE_NEAR_ATR
    )

    early_break_long = (
        resistance
        < live_price
        <= resistance
        + current_atr
        * PRE_BREAK_ATR
    )

    early_break_short = (
        support
        > live_price
        >= support
        - current_atr
        * PRE_BREAK_ATR
    )

    pre_long = (
        trend1h_long

        and not trend4h_short

        and (
            near_long
            or early_break_long
        )

        and (
            vol1h
            >= PRE_VOL_1H_MIN

            or vol15
            >= PRE_VOL_15M_MIN
        )
    )

    pre_short = (
        trend1h_short

        and not trend4h_long

        and (
            near_short
            or early_break_short
        )

        and (
            vol1h
            >= PRE_VOL_1H_MIN

            or vol15
            >= PRE_VOL_15M_MIN
        )
    )

    # Confermati normali:
    # breakout 1H + trend + volume + volatilità.
    # Retest NON obbligatorio.
    # Candela forte/OI usati dopo come qualità.

    candidate_long = (
        breakout1h_long

        and trend1h_long

        and vol1h
        >= CONFIRM_VOL_MIN

        and not_extended_long

        and volatility_ok

        and not trend4h_short
    )

    candidate_short = (
        breakout1h_short

        and trend1h_short

        and vol1h
        >= CONFIRM_VOL_MIN

        and not_extended_short

        and volatility_ok

        and not trend4h_long
    )

    if (
        not candidate_long
        and not candidate_short
    ):

        if pre_long:
            invalidation = (
                resistance
                - current_atr
                * 0.55
            )

            quality = (
                5
                + int(
                    trend4h_long
                )
                + int(
                    vol15
                    >= PRE_VOL_15M_MIN
                )
            )

            leverage = (
                calculate_leverage(
                    live_price,
                    invalidation,
                    quality,
                    False
                )
            )

            return {
                "type": "PRE",
                "direction": "LONG",
                "price": live_price,
                "level": resistance,
                "invalidation":
                    invalidation,
                "volume1h":
                    vol1h,
                "volume15":
                    vol15,
                "atr_pct":
                    atr_pct,
                "quality":
                    quality,
                "leverage":
                    leverage,
                "early_break":
                    early_break_long,
            }

        if pre_short:
            invalidation = (
                support
                + current_atr
                * 0.55
            )

            quality = (
                5
                + int(
                    trend4h_short
                )
                + int(
                    vol15
                    >= PRE_VOL_15M_MIN
                )
            )

            leverage = (
                calculate_leverage(
                    live_price,
                    invalidation,
                    quality,
                    False
                )
            )

            return {
                "type": "PRE",
                "direction": "SHORT",
                "price": live_price,
                "level": support,
                "invalidation":
                    invalidation,
                "volume1h":
                    vol1h,
                "volume15":
                    vol15,
                "atr_pct":
                    atr_pct,
                "quality":
                    quality,
                "leverage":
                    leverage,
                "early_break":
                    early_break_short,
            }

        return None

    oi_change, funding = (
        get_derivatives(
            symbol
        )
    )

    if (
        oi_change is None
        or funding is None
    ):
        return None

    if (
        oi_change
        < OI_CONFIRM_MIN
    ):
        return None

    long_liq, short_liq = (
        liquidation_metrics(
            symbol
        )
    )

    liq_available = (
        long_liq
        + short_liq
        > 0
    )

    if candidate_long:
        direction = "LONG"
        level = resistance
        used_retest = (
            retest_long
        )

        funding_ok = (
            funding
            <= FUNDING_BLOCK
        )

        funding_aggressive = (
            funding
            <= FUNDING_AGGRESSIVE
        )

        oi_positive = (
            oi_change
            >= 0.0
        )

        oi_aggressive = (
            oi_change
            >= OI_AGGRESSIVE_MIN
        )

        btc_aligned = (
            symbol
            == "BTCUSDT"

            or btc_bias
            == "LONG"
        )

        btc_opposite = (
            symbol
            != "BTCUSDT"

            and btc_bias
            == "SHORT"
        )

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

        if (
            not funding_ok
            or severe_liq_against
        ):
            return None

        if (
            btc_opposite

            and not (
                oi_positive

                and (
                    strong_candle_long
                    or used_retest
                )
            )
        ):
            return None

        if not (
            strong_candle_long
            or used_retest
            or oi_positive
        ):
            return None

        entry = live_price

        structure_stop = (
            min(
                c["l"]
                for c in retest_window
            )

            if used_retest

            else (
                resistance
                - current_atr * 0.40
            )
        )

        stop = min(
            structure_stop,

            resistance
            - current_atr * 0.25
        ) - current_atr * 0.10

        if stop >= entry:
            return None

        risk = (
            entry
            - stop
        )

        quality = 7

        quality += int(
            trend4h_long
        )

        quality += int(
            oi_positive
        )

        quality += int(
            funding_aggressive
        )

        quality += int(
            btc_aligned
        )

        quality += int(
            liq_support
        )

        quality += int(
            atr_pct
            <= ATR_AGGRESSIVE_MAX_PCT
        )

        quality += int(
            used_retest
        )

        quality += int(
            strong_candle_long
        )

        aggressive_ok = (
            used_retest

            and trend4h_long

            and strong_candle_long

            and vol1h
            >= AGGRESSIVE_VOL_MIN

            and funding_aggressive

            and oi_aggressive

            and atr_pct
            <= ATR_AGGRESSIVE_MAX_PCT

            and btc_aligned

            and (
                liq_support

                or not liq_available
            )
        )

    else:
        direction = "SHORT"
        level = support
        used_retest = (
            retest_short
        )

        funding_ok = (
            funding
            >= -FUNDING_BLOCK
        )

        funding_aggressive = (
            funding
            >= -FUNDING_AGGRESSIVE
        )

        oi_positive = (
            oi_change
            >= 0.0
        )

        oi_aggressive = (
            oi_change
            >= OI_AGGRESSIVE_MIN
        )

        btc_aligned = (
            symbol
            == "BTCUSDT"

            or btc_bias
            == "SHORT"
        )

        btc_opposite = (
            symbol
            != "BTCUSDT"

            and btc_bias
            == "LONG"
        )

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

        if (
            not funding_ok
            or severe_liq_against
        ):
            return None

        if (
            btc_opposite

            and not (
                oi_positive

                and (
                    strong_candle_short
                    or used_retest
                )
            )
        ):
            return None

        if not (
            strong_candle_short
            or used_retest
            or oi_positive
        ):
            return None

        entry = live_price

        structure_stop = (
            max(
                c["h"]
                for c in retest_window
            )

            if used_retest

            else (
                support
                + current_atr * 0.40
            )
        )

        stop = max(
            structure_stop,

            support
            + current_atr * 0.25
        ) + current_atr * 0.10

        if stop <= entry:
            return None

        risk = (
            stop
            - entry
        )

        quality = 7

        quality += int(
            trend4h_short
        )

        quality += int(
            oi_positive
        )

        quality += int(
            funding_aggressive
        )

        quality += int(
            btc_aligned
        )

        quality += int(
            liq_support
        )

        quality += int(
            atr_pct
            <= ATR_AGGRESSIVE_MAX_PCT
        )

        quality += int(
            used_retest
        )

        quality += int(
            strong_candle_short
        )

        aggressive_ok = (
            used_retest

            and trend4h_short

            and strong_candle_short

            and vol1h
            >= AGGRESSIVE_VOL_MIN

            and funding_aggressive

            and oi_aggressive

            and atr_pct
            <= ATR_AGGRESSIVE_MAX_PCT

            and btc_aligned

            and (
                liq_support

                or not liq_available
            )
        )

    entry_low = (
        entry
        - current_atr * 0.08
    )

    entry_high = (
        entry
        + current_atr * 0.08
    )

    leverage = (
        calculate_leverage(
            entry,
            stop,
            quality,
            aggressive_ok
        )
    )

    if direction == "LONG":
        tp1 = (
            entry
            + risk
        )

        tp2 = (
            entry
            + risk * 2
        )

        tp3 = (
            entry
            + risk * 3
        )

    else:
        tp1 = (
            entry
            - risk
        )

        tp2 = (
            entry
            - risk * 2
        )

        tp3 = (
            entry
            - risk * 3
        )

    return {
        "type":
            "CONFIRMED",

        "direction":
            direction,

        "price":
            entry,

        "entry_low":
            entry_low,

        "entry_high":
            entry_high,

        "sl":
            stop,

        "tp1":
            tp1,

        "tp2":
            tp2,

        "tp3":
            tp3,

        "level":
            level,

        "volume1h":
            vol1h,

        "volume15":
            vol15,

        "atr_pct":
            atr_pct,

        "quality":
            quality,

        "leverage":
            leverage,

        "oi":
            oi_change,

        "funding":
            funding,

        "btc":
            btc_bias,

        "long_liq":
            long_liq,

        "short_liq":
            short_liq,

        "liq_available":
            liq_available,

        "retest":
            used_retest,

        "aggressive":
            leverage >= 20
            and aggressive_ok,
    }


def build_message(
    symbol,
    signal
):
    pair = (
        symbol.replace(
            "USDT",
            "/USDT"
        )
    )

    grade = (
        confirmation_grade(
            signal["quality"]
        )
    )

    if (
        signal["type"]
        == "PRE"
    ):
        setup = (
            "rottura 15m iniziale"

            if signal[
                "early_break"
            ]

            else (
                "avvicinamento "
                "al livello"
            )
        )

        return (
            "🟠 PRE-SEGNALE\n"

            f"{pair} — "
            f"{signal['direction']}\n\n"

            f"Livello chiave: "
            f"{fmt_price(signal['level'])}\n"

            f"Prezzo attuale: "
            f"{fmt_price(signal['price'])}\n"

            f"Setup: "
            f"{setup}\n"

            "Possibile ENTRY: "
            "solo dopo conferma\n"

            f"Invalidazione: "
            f"{fmt_price(signal['invalidation'])}\n"

            f"Volume 1H: "
            f"{signal['volume1h']:.2f}"
            "x media\n"

            f"Volume 15m: "
            f"{signal['volume15']:.2f}"
            "x media\n"

            f"ATR 1H: "
            f"{signal['atr_pct']:.2f}%\n"

            f"Leva potenziale: "
            f"{signal['leverage']}x\n"

            "Timeframe: "
            "15m / 1H\n"

            f"Grado conferma: "
            f"{grade}"
        )

    title = (
        "🔥 SEGNALE CONFERMATO "
        "AGGRESSIVO"

        if signal[
            "aggressive"
        ]

        else (
            "🟢 SEGNALE "
            "CONFERMATO"
        )
    )

    funding_pct = (
        signal["funding"]
        * 100
    )

    if (
        signal["direction"]
        == "LONG"
    ):
        favorable_liq = (
            signal[
                "short_liq"
            ]
        )

        adverse_liq = (
            signal[
                "long_liq"
            ]
        )

    else:
        favorable_liq = (
            signal[
                "long_liq"
            ]
        )

        adverse_liq = (
            signal[
                "short_liq"
            ]
        )

    if signal[
        "liq_available"
    ]:
        liq_text = (
            f"Favorevoli "
            f"{fmt_money(favorable_liq)}"
            " | "
            f"Contrarie "
            f"{fmt_money(adverse_liq)}"
        )

    else:
        liq_text = (
            "In raccolta / "
            "nessun evento recente"
        )

    retest_text = (
        "OK"
        if signal["retest"]
        else "Non obbligatorio"
    )

    return (
        f"{title}\n"

        f"{pair} — "
        f"{signal['direction']}\n\n"

        f"ENTRY: "
        f"{fmt_price(signal['entry_low'])}"
        " - "
        f"{fmt_price(signal['entry_high'])}\n"

        f"SL: "
        f"{fmt_price(signal['sl'])}\n"

        f"TP1: "
        f"{fmt_price(signal['tp1'])}\n"

        f"TP2: "
        f"{fmt_price(signal['tp2'])}\n"

        f"TP3: "
        f"{fmt_price(signal['tp3'])}\n"

        f"Leva indicativa: "
        f"{signal['leverage']}x\n\n"

        f"Volume 1H: "
        f"{signal['volume1h']:.2f}"
        "x media\n"

        f"Volume 15m: "
        f"{signal['volume15']:.2f}"
        "x media\n"

        f"Open Interest 15m: "
        f"{signal['oi']:+.2f}%\n"

        f"Funding: "
        f"{funding_pct:+.4f}%\n"

        f"ATR 1H: "
        f"{signal['atr_pct']:.2f}%\n"

        f"Retest: "
        f"{retest_text}\n"

        f"Filtro BTC: "
        f"{signal['btc']}\n"

        f"Liquidazioni 15m: "
        f"{liq_text}\n"

        f"Grado conferma: "
        f"{grade}\n"

        "Timeframe: "
        "15m / 1H / 4H\n\n"

        "Nota: leva indicativa; "
        "con leve elevate il margine "
        "di errore e molto ridotto."
    )


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

    previous = (
        signal_state.get(
            key
        )
    )

    if (
        previous

        and previous[
            "signature"
        ] == signature

        and now
        - previous["time"]
        < 6 * 3600
    ):
        return False

    signal_state[key] = {
        "signature":
            signature,

        "time":
            now
    }

    for old_key in list(
        signal_state.keys()
    ):
        if (
            now
            - signal_state[
                old_key
            ]["time"]
            > 12 * 3600
        ):
            del signal_state[
                old_key
            ]

    return True


def scan_market():
    successful = 0

    btc_data = (
        market_data(
            "BTCUSDT"
        )
    )

    btc = (
        get_btc_bias(
            btc_data
        )
    )

    for symbol in SYMBOLS:
        try:
            data = (
                btc_data

                if symbol
                == "BTCUSDT"

                else market_data(
                    symbol
                )
            )

            successful += 1

            signal = (
                analyze_symbol(
                    symbol,
                    data,
                    btc
                )
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

                if should_send(
                    symbol,
                    signal
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


threading.Thread(
    target=ws_loop,
    daemon=True
).start()


print(
    "CryptoSignalAI12 avviato - "
    "modalita bilanciata v3 attiva"
)


send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "Modalita bilanciata v3 attiva.\n"
    "PRE piu sensibili: 15m + volume.\n"
    "Confermati normali <20x piu elastici.\n"
    "Aggressivi 20x+: filtri severi invariati.\n"
    "Protezione Binance 429 attiva."
)


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

    sleep_time = max(
        1.0,
        SCAN_SECONDS
        - elapsed
    )

    time.sleep(
        sleep_time
    )
