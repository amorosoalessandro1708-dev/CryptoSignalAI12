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

KLINES_URL = (
    f"{REST}/fapi/v1/klines"
)

OI_URL = (
    f"{REST}/futures/data/openInterestHist"
)

PREMIUM_URL = (
    f"{REST}/fapi/v1/premiumIndex"
)

LIQ_WS_URLS = [
    "wss://fstream.binance.com/market/ws/!forceOrder@arr",
    "wss://fstream.binance.com/public/ws/!forceOrder@arr",
]

SCAN_SECONDS = 60
LIQ_WINDOW_SECONDS = 15 * 60

# PRE invariati
PRE_VOL_MIN = 1.20

# CONFERMATI NORMALI leggermente più permissivi
CONFIRM_VOL_MIN = 1.35

# Se non c'è retest serve comunque
# un breakout forte
STRONG_BREAKOUT_VOL = 1.60

# Filtri derivati invariati
FUNDING_BLOCK = 0.0005
FUNDING_AGGRESSIVE = 0.0003

OI_CONFIRM_MIN = -0.25
OI_AGGRESSIVE_MIN = 0.15

ATR_MIN_PCT = 0.10
ATR_MAX_PCT = 5.00
ATR_AGGRESSIVE_MAX_PCT = 2.50

LEVERAGE_SAFETY = 0.35

LEVERAGE_STEPS = [
    1, 2, 3, 5, 10, 15,
    20, 25, 30, 40, 50,
    75, 100
]

signal_state = {}

liquidation_events = deque()
liq_lock = threading.Lock()


# ==========================
# TELEGRAM
# ==========================

def send_telegram(text):

    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram non configurato")
        return

    try:

        response = requests.post(
            f"https://api.telegram.org/"
            f"bot{BOT_TOKEN}/sendMessage",
            data={
                "chat_id": CHAT_ID,
                "text": text
            },
            timeout=20
        )

        response.raise_for_status()

    except Exception as e:

        print(
            "Errore Telegram:",
            e
        )


# ==========================
# BINANCE REST
# ==========================

def get_json(
    url,
    params=None,
    timeout=15
):

    response = requests.get(
        url,
        params=params,
        timeout=timeout
    )

    response.raise_for_status()

    return response.json()


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
            "t": int(c[0])
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
        )
    }


# ==========================
# INDICATORI
# ==========================

def ema(
    values,
    period
):

    if len(values) < period:
        return None

    multiplier = (
        2 / (period + 1)
    )

    value = (
        sum(values[:period])
        / period
    )

    for price in values[period:]:

        value = (
            (price - value)
            * multiplier
        ) + value

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

        high = candles[i]["h"]
        low = candles[i]["l"]

        previous_close = (
            candles[i - 1]["c"]
        )

        ranges.append(
            max(
                high - low,
                abs(
                    high
                    - previous_close
                ),
                abs(
                    low
                    - previous_close
                )
            )
        )

    return (
        sum(ranges[-period:])
        / period
    )


def volume_ratio(
    candles,
    period=20
):

    if len(candles) < period + 2:
        return 0.0

    current_volume = (
        candles[-2]["v"]
    )

    previous = [
        candle["v"]
        for candle in
        candles[
            -(period + 2):-2
        ]
    ]

    average = (
        sum(previous)
        / len(previous)
    )

    if average <= 0:
        return 0.0

    return (
        current_volume
        / average
    )


def candle_strength(
    candle,
    direction
):

    candle_range = (
        candle["h"]
        - candle["l"]
    )

    if candle_range <= 0:
        return False

    body_ratio = (
        abs(
            candle["c"]
            - candle["o"]
        )
        / candle_range
    )

    close_position = (
        candle["c"]
        - candle["l"]
    ) / candle_range

    if direction == "LONG":

        return (
            candle["c"]
            > candle["o"]

            and body_ratio >= 0.55

            and close_position >= 0.70
        )

    return (
        candle["c"]
        < candle["o"]

        and body_ratio >= 0.55

        and close_position <= 0.30
    )


# ==========================
# OPEN INTEREST + FUNDING
# ==========================

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

            previous = float(
                data[-2][
                    "sumOpenInterestValue"
                ]
            )

            current = float(
                data[-1][
                    "sumOpenInterestValue"
                ]
            )

            if previous > 0:

                oi_change = (
                    (
                        current
                        - previous
                    )
                    / previous
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


# ==========================
# LIQUIDAZIONI
# ==========================

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

        if not isinstance(
            item,
            dict
        ):
            continue

        order = item.get(
            "o",
            item
        )

        symbol = order.get(
            "s"
        )

        if symbol not in SYMBOLS:
            continue

        quantity = float(
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
            quantity
            * price
        )

        if notional <= 0:
            continue

        side = order.get(
            "S"
        )

        kind = (
            "LONG_LIQ"
            if side == "SELL"
            else "SHORT_LIQ"
        )

        timestamp = int(
            order.get("T")
            or item.get("E")
            or time.time() * 1000
        ) / 1000

        with liq_lock:

            liquidation_events.append({
                "symbol": symbol,
                "kind": kind,
                "notional": notional,
                "time": timestamp
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

        url = LIQ_WS_URLS[
            index
            % len(LIQ_WS_URLS)
        ]

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
                    on_error=ws_error
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


def liquidation_metrics(
    symbol
):

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

        for event in (
            liquidation_events
        ):

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


# ==========================
# FORMATO
# ==========================

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

    return (
        f"${value:.0f}"
    )


# ==========================
# LEVA
# ==========================

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

    raw_leverage = int(
        LEVERAGE_SAFETY
        / stop_pct
    )

    quality_cap = (
        leverage_cap(
            quality
        )
    )

    # Confermati normali:
    # massimo 15x.
    if not aggressive_ok:

        quality_cap = min(
            quality_cap,
            15
        )

    allowed = min(
        raw_leverage,
        quality_cap,
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


# ==========================
# FILTRO BTC
# ==========================

def get_btc_bias(data):

    c1h = data["1h"]
    c4h = data["4h"]

    closes1h = [
        candle["c"]
        for candle in c1h[:-1]
    ]

    closes4h = [
        candle["c"]
        for candle in c4h[:-1]
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

    price1h = (
        c1h[-2]["c"]
    )

    price4h = (
        c4h[-2]["c"]
    )

    if (
        price1h
        > e20_1h
        > e50_1h

        and

        price4h
        > e20_4h
        > e50_4h
    ):
        return "LONG"

    if (
        price1h
        < e20_1h
        < e50_1h

        and

        price4h
        < e20_4h
        < e50_4h
    ):
        return "SHORT"

    return "NEUTRAL"


# ==========================
# GRADO
# ==========================

def confirmation_grade(
    quality
):

    if quality >= 10:
        return "ALTO"

    if quality >= 8:
        return "MEDIO-ALTO"

    return "MEDIO"


# ==========================
# ANALISI
# ==========================

def analyze_symbol(
    symbol,
    data,
    btc_bias
):

    c15 = data["15m"]
    c1h = data["1h"]
    c4h = data["4h"]

    last15 = c15[-2]
    last1h = c1h[-2]
    last4h = c4h[-2]

    closes1h = [
        candle["c"]
        for candle in c1h[:-1]
    ]

    closes4h = [
        candle["c"]
        for candle in c4h[:-1]
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

    vol_ratio = (
        volume_ratio(c1h)
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
        candle["h"]
        for candle
        in previous_20
    )

    support = min(
        candle["l"]
        for candle
        in previous_20
    )

    price1h = (
        last1h["c"]
    )

    price15 = (
        last15["c"]
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

    breakout_long = (
        price1h
        > resistance
    )

    breakout_short = (
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

        candle["l"]
        <= resistance
        + tolerance

        and

        candle["c"]
        > resistance

        for candle
        in retest_window
    )

    retest_short = any(

        candle["h"]
        >= support
        - tolerance

        and

        candle["c"]
        < support

        for candle
        in retest_window
    )

    not_extended_long = (
        price15
        <= resistance
        + current_atr * 0.80
    )

    not_extended_short = (
        price15
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


    # ==========================
    # BREAKOUT FORTE
    # ==========================

    strong_breakout_long = (
        breakout_long

        and strong_candle_long

        and vol_ratio
        >= STRONG_BREAKOUT_VOL

        and price1h
        <= resistance
        + current_atr * 0.75
    )

    strong_breakout_short = (
        breakout_short

        and strong_candle_short

        and vol_ratio
        >= STRONG_BREAKOUT_VOL

        and price1h
        >= support
        - current_atr * 0.75
    )


    # ==========================
    # CONFERMATI NORMALI
    #
    # Retest oppure breakout
    # forte.
    # ==========================

    normal_trigger_long = (
        breakout_long

        and trend1h_long

        and vol_ratio
        >= CONFIRM_VOL_MIN

        and strong_candle_long

        and not_extended_long

        and volatility_ok

        and not trend4h_short

        and (
            retest_long
            or strong_breakout_long
        )
    )


    normal_trigger_short = (
        breakout_short

        and trend1h_short

        and vol_ratio
        >= CONFIRM_VOL_MIN

        and strong_candle_short

        and not_extended_short

        and volatility_ok

        and not trend4h_long

        and (
            retest_short
            or strong_breakout_short
        )
    )


    # ==========================
    # PRE
    # ==========================

    near_long = (
        price15
        <= resistance

        and abs(
            resistance
            - price15
        )
        <= current_atr * 0.25

        and trend1h_long
    )


    near_short = (
        price15
        >= support

        and abs(
            price15
            - support
        )
        <= current_atr * 0.25

        and trend1h_short
    )


    if (
        not normal_trigger_long
        and not normal_trigger_short
    ):

        if (
            near_long

            and vol_ratio
            >= PRE_VOL_MIN
        ):

            invalidation = (
                resistance
                - current_atr * 0.5
            )

            quality = (
                5
                + int(
                    trend4h_long
                )
            )

            leverage = (
                calculate_leverage(
                    price15,
                    invalidation,
                    quality,
                    False
                )
            )

            return {
                "type": "PRE",
                "direction": "LONG",
                "price": price15,
                "level": resistance,
                "invalidation":
                    invalidation,
                "volume": vol_ratio,
                "atr_pct": atr_pct,
                "quality": quality,
                "leverage": leverage
            }


        if (
            near_short

            and vol_ratio
            >= PRE_VOL_MIN
        ):

            invalidation = (
                support
                + current_atr * 0.5
            )

            quality = (
                5
                + int(
                    trend4h_short
                )
            )

            leverage = (
                calculate_leverage(
                    price15,
                    invalidation,
                    quality,
                    False
                )
            )

            return {
                "type": "PRE",
                "direction": "SHORT",
                "price": price15,
                "level": support,
                "invalidation":
                    invalidation,
                "volume": vol_ratio,
                "atr_pct": atr_pct,
                "quality": quality,
                "leverage": leverage
            }

        return None


    # ==========================
    # DERIVATI
    # ==========================

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


    # ==========================
    # LONG
    # ==========================

    if normal_trigger_long:

        direction = "LONG"
        level = resistance
        used_retest = retest_long

        funding_ok = (
            funding
            <= FUNDING_BLOCK
        )

        funding_aggressive = (
            funding
            <= FUNDING_AGGRESSIVE
        )

        oi_ok = (
            oi_change
            >= OI_CONFIRM_MIN
        )

        oi_aggressive = (
            oi_change
            >= OI_AGGRESSIVE_MIN
        )

        btc_conflict = (
            symbol != "BTCUSDT"

            and

            btc_bias == "SHORT"
        )

        btc_aligned = (
            symbol == "BTCUSDT"

            or

            btc_bias == "LONG"
        )


        liq_support = (
            liq_available

            and

            short_liq
            >= max(
                long_liq * 1.25,
                10000
            )
        )


        liq_against = (
            liq_available

            and

            long_liq
            >= max(
                short_liq * 1.75,
                25000
            )
        )


        if (
            not funding_ok

            or not oi_ok

            or btc_conflict

            or liq_against
        ):
            return None


        entry = price15


        if used_retest:

            structure_stop = min(
                candle["l"]
                for candle
                in retest_window
            )

        else:

            structure_stop = (
                resistance
                - current_atr * 0.35
            )


        stop = min(
            structure_stop,

            resistance
            - current_atr * 0.25
        )


        stop -= (
            current_atr
            * 0.10
        )


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
            oi_aggressive
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


        # 20x+:
        # filtri severi invariati.
        aggressive_ok = (
            used_retest

            and trend4h_long

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


    # ==========================
    # SHORT
    # ==========================

    else:

        direction = "SHORT"
        level = support
        used_retest = retest_short

        funding_ok = (
            funding
            >= -FUNDING_BLOCK
        )

        funding_aggressive = (
            funding
            >= -FUNDING_AGGRESSIVE
        )

        oi_ok = (
            oi_change
            >= OI_CONFIRM_MIN
        )

        oi_aggressive = (
            oi_change
            >= OI_AGGRESSIVE_MIN
        )

        btc_conflict = (
            symbol != "BTCUSDT"

            and

            btc_bias == "LONG"
        )

        btc_aligned = (
            symbol == "BTCUSDT"

            or

            btc_bias == "SHORT"
        )


        liq_support = (
            liq_available

            and

            long_liq
            >= max(
                short_liq * 1.25,
                10000
            )
        )


        liq_against = (
            liq_available

            and

            short_liq
            >= max(
                long_liq * 1.75,
                25000
            )
        )


        if (
            not funding_ok

            or not oi_ok

            or btc_conflict

            or liq_against
        ):
            return None


        entry = price15


        if used_retest:

            structure_stop = max(
                candle["h"]
                for candle
                in retest_window
            )

        else:

            structure_stop = (
                support
                + current_atr * 0.35
            )


        stop = max(
            structure_stop,

            support
            + current_atr * 0.25
        )


        stop += (
            current_atr
            * 0.10
        )


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
            oi_aggressive
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


        aggressive_ok = (
            used_retest

            and trend4h_short

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


    # ==========================
    # ENTRY / TP / LEVA
    # ==========================

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
        "type": "CONFIRMED",
        "direction": direction,
        "price": entry,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "sl": stop,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "level": level,
        "volume": vol_ratio,
        "atr_pct": atr_pct,
        "quality": quality,
        "leverage": leverage,
        "oi": oi_change,
        "funding": funding,
        "btc": btc_bias,
        "long_liq": long_liq,
        "short_liq": short_liq,
        "liq_available":
            liq_available,
        "retest": used_retest,
        "aggressive": (
            leverage >= 20
            and aggressive_ok
        )
    }


# ==========================
# MESSAGGI
# ==========================

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


    if signal["type"] == "PRE":

        return (
            "🟠 PRE-SEGNALE\n"

            f"{pair} — "
            f"{signal['direction']}\n\n"

            f"Livello chiave: "
            f"{fmt_price(signal['level'])}\n"

            f"Prezzo attuale: "
            f"{fmt_price(signal['price'])}\n"

            "Possibile ENTRY: "
            "solo dopo conferma\n"

            f"Invalidazione: "
            f"{fmt_price(signal['invalidation'])}\n"

            f"Volume 1H: "
            f"{signal['volume']:.2f}"
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


    if signal["aggressive"]:

        title = (
            "🔥 SEGNALE CONFERMATO "
            "AGGRESSIVO"
        )

    else:

        title = (
            "🟢 SEGNALE CONFERMATO"
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


    if signal["liq_available"]:

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


    if signal["retest"]:

        retest_text = "OK"

    else:

        retest_text = (
            "Non obbligatorio: "
            "breakout forte"
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
        f"{signal['volume']:.2f}"
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


# ==========================
# ANTI-SPAM
# ==========================

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


# ==========================
# SCANSIONE
# ==========================

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

            if symbol == "BTCUSDT":

                data = btc_data

            else:

                data = (
                    market_data(
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
                    signal["leverage"]
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


    print(
        "Scansione completata: "
        f"{successful}/"
        f"{len(SYMBOLS)} coppie"
    )


# ==========================
# AVVIO
# ==========================

threading.Thread(
    target=ws_loop,
    daemon=True
).start()


print(
    "CryptoSignalAI12 avviato - "
    "modalita bilanciata v2 attiva"
)


send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "Modalita bilanciata v2 attiva.\n"
    "Confermati normali: volume 1.35x, "
    "retest oppure breakout forte 1.60x.\n"
    "Aggressivi 20x+: filtri severi "
    "invariati e retest obbligatorio.\n"
    "Scansione ogni 60 secondi."
)


while True:

    try:

        scan_market()

    except Exception as e:

        print(
            "Errore scansione generale:",
            e
        )

    time.sleep(
        SCAN_SECONDS
    )
