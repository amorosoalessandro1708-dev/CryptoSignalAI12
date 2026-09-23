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
# LOGICA "EARLY"
# ==========================================================

PRE_STRUCTURE_BARS = 6
PRE_VOL_15M_MIN = 0.90
PRE_NEAR_ATR15 = 0.30

CONFIRM_VOL_15M_MIN = 1.05
CONFIRM_BODY_ATR_MIN = 0.20
OI_CONFIRM_MIN = 0.20
FUNDING_BLOCK = 0.0008


# ==========================================================
# FILTRO BTC CONFERMATI
# ==========================================================

# Per tutte le altcoin:
# LONG  -> BTC deve essere LONG oppure NEUTRAL
# SHORT -> BTC deve essere SHORT oppure NEUTRAL
#
# BTC chiaramente contrario = niente CONFERMATO.
REQUIRE_BTC_NOT_OPPOSITE = True


# ==========================================================
# DIAGNOSTICA
# ==========================================================

DIAGNOSTIC_LOGS = True


# ==========================================================
# AGGRESSIVI 20X+
# ==========================================================

AGGRESSIVE_VOL_15M_MIN = 1.20
OI_AGGRESSIVE_MIN = 0.00
FUNDING_AGGRESSIVE = 0.0005
ATR_AGGRESSIVE_MAX_PCT = 3.00


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
# LEVA
# ==========================================================

LEVERAGE_SAFETY = 0.35

LEVERAGE_STEPS = [
    1, 2, 3, 5, 10, 15,
    20, 25, 30, 40, 50, 75, 100
]


# ==========================================================
# PROTEZIONE BINANCE 429
# ==========================================================

MIN_REST_GAP_SECONDS = 0.18
MAX_RETRIES = 4
RETRY_FALLBACK_SECONDS = [3, 6, 12, 20]


session = requests.Session()

request_lock = threading.Lock()
last_rest_request = 0.0

signal_state = {}

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
# REST
# ==========================================================

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

                wait = max(wait, 2.0)

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

            wait = min(2 ** attempt, 8)

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


# ==========================================================
# MARKET DATA
# ==========================================================

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


# ==========================================================
# INDICATORI
# ==========================================================

def ema(values, period):

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

    return (
        sum(ranges[-period:])
        / period
    )


def volume_ratio_closed(candles, period=20):

    if len(candles) < period + 2:
        return 0.0

    current_volume = candles[-2]["v"]

    previous = [
        c["v"]
        for c in candles[-(period + 2):-2]
    ]

    avg = (
        sum(previous)
        / len(previous)
    )

    if avg <= 0:
        return 0.0

    return (
        current_volume
        / avg
    )


def candle_strength(candle, direction):

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

    rng = (
        candle["h"]
        - candle["l"]
    )

    if rng <= 0:
        return False

    if direction == "LONG":

        retrace = (
            candle["h"]
            - candle["c"]
        ) / rng

        bearish = (
            candle["c"]
            < candle["o"]
        )

        return (
            bearish
            and retrace >= MAX_LIVE_RETRACE
        )

    retrace = (
        candle["c"]
        - candle["l"]
    ) / rng

    bullish = (
        candle["c"]
        > candle["o"]
    )

    return (
        bullish
        and retrace >= MAX_LIVE_RETRACE
    )


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

    return (
        oi_change,
        funding
    )


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

        notional = (
            qty * price
        )

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

        url = (
            LIQ_WS_URLS[
                index
                % len(LIQ_WS_URLS)
            ]
        )

        print(
            "LIQ WS connessione:",
            url
        )

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
            and liquidation_events[0]["time"] < cutoff
        ):
            liquidation_events.popleft()

        for event in liquidation_events:

            if event["symbol"] != symbol:
                continue

            if event["kind"] == "LONG_LIQ":

                long_liq += (
                    event["notional"]
                )

            else:

                short_liq += (
                    event["notional"]
                )

    return (
        long_liq,
        short_liq
    )


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

    c1h = data["1h"]

    closes = [
        c["c"]
        for c in c1h[:-1]
    ]

    e20 = ema(
        closes,
        20
    )

    e50 = ema(
        closes,
        50
    )

    if None in (
        e20,
        e50
    ):
        return "NEUTRAL"

    price = c1h[-2]["c"]

    if (
        price > e20 > e50
    ):
        return "LONG"

    if (
        price < e20 < e50
    ):
        return "SHORT"

    return "NEUTRAL"


def btc_allows_confirmed(
    symbol,
    direction,
    btc_bias
):

    if symbol == "BTCUSDT":
        return True

    if not REQUIRE_BTC_NOT_OPPOSITE:
        return True

    if btc_bias == "NEUTRAL":
        return True

    if (
        direction == "LONG"
        and btc_bias == "LONG"
    ):
        return True

    if (
        direction == "SHORT"
        and btc_bias == "SHORT"
    ):
        return True

    return False


# ==========================================================
# GRADO CONFERMA
# ==========================================================

def confirmation_grade(quality):

    if quality >= 10:
        return "ALTO"

    if quality >= 8:
        return "MEDIO-ALTO"

    return "MEDIO"


# ==========================================================
# DIAGNOSTICA CONFERMATI
# ==========================================================

def log_no_confirm(
    symbol,
    direction,
    reasons
):

    if not DIAGNOSTIC_LOGS:
        return

    if not reasons:
        return

    print(
        f"{symbol} NO CONFIRM {direction}: "
        + ", ".join(reasons)
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
    # STRUTTURA RECENTE 15M
    # 6 candele 15m = circa 90 minuti.
    # ------------------------------------------------------

    structure = (
        c15[
            -(PRE_STRUCTURE_BARS + 2):-2
        ]
    )

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

    # ------------------------------------------------------
    # TREND
    # ------------------------------------------------------

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
        last4h["c"]
        > e20_4h
    )

    trend4h_short = (
        last4h["c"]
        < e20_4h
    )

    # ------------------------------------------------------
    # VOLUME
    # ------------------------------------------------------

    vol15 = (
        volume_ratio_closed(
            c15
        )
    )

    vol1h = (
        volume_ratio_closed(
            c1h
        )
    )

    # ------------------------------------------------------
    # ATR
    # ------------------------------------------------------

    atr_pct = (
        atr1h
        / price1h
        * 100
    )

    volatility_ok = (
        ATR_MIN_PCT
        <= atr_pct
        <= ATR_MAX_PCT
    )

    # ------------------------------------------------------
    # PRE: AVVICINAMENTO / PRIMA ROTTURA
    # ------------------------------------------------------

    distance_long = (
        resistance
        - price
    )

    distance_short = (
        price
        - support
    )

    near_long = (
        distance_long >= 0
        and
        distance_long
        <= atr15 * PRE_NEAR_ATR15
    )

    near_short = (
        distance_short >= 0
        and
        distance_short
        <= atr15 * PRE_NEAR_ATR15
    )

    early_break_long = (
        price > resistance
        and
        price <= (
            resistance
            + atr15 * MAX_EXTENSION_ATR15
        )
    )

    early_break_short = (
        price < support
        and
        price >= (
            support
            - atr15 * MAX_EXTENSION_ATR15
        )
    )

    # ------------------------------------------------------
    # DIREZIONE CANDELA 15M
    # ------------------------------------------------------

    bullish15 = (
        last15["c"]
        > last15["o"]
    )

    bearish15 = (
        last15["c"]
        < last15["o"]
    )

    # ------------------------------------------------------
    # PRE
    # ------------------------------------------------------

    pre_long = (
        trend1h_long
        and bullish15
        and (
            near_long
            or early_break_long
        )
        and (
            vol15
            >= PRE_VOL_15M_MIN
        )
    )

    pre_short = (
        trend1h_short
        and bearish15
        and (
            near_short
            or early_break_short
        )
        and (
            vol15
            >= PRE_VOL_15M_MIN
        )
    )

    # ------------------------------------------------------
    # BREAKOUT CONFERMATO SU 15M CHIUSO
    # ------------------------------------------------------

    breakout_long = (
        closed_price15
        > resistance
    )

    breakout_short = (
        closed_price15
        < support
    )

    # ------------------------------------------------------
    # FORZA DELLA CANDELA
    # ------------------------------------------------------

    body15 = abs(
        last15["c"]
        - last15["o"]
    )

    body_atr = (
        body15 / atr15
        if atr15 > 0
        else 0
    )

    strong15_long = (
        candle_strength(
            last15,
            "LONG"
        )
    )

    strong15_short = (
        candle_strength(
            last15,
            "SHORT"
        )
    )

    # ------------------------------------------------------
    # ANTI-INVERSIONE LIVE
    # ------------------------------------------------------

    reversing_long = (
        live_reversal(
            live15,
            "LONG"
        )
    )

    reversing_short = (
        live_reversal(
            live15,
            "SHORT"
        )
    )

    # ------------------------------------------------------
    # ANTI-INSEGUIMENTO
    # ------------------------------------------------------

    extension_long = (
        price
        - resistance
    )

    extension_short = (
        support
        - price
    )

    not_extended_long = (
        extension_long
        <= atr15
        * MAX_EXTENSION_ATR15
    )

    not_extended_short = (
        extension_short
        <= atr15
        * MAX_EXTENSION_ATR15
    )

    # ------------------------------------------------------
    # CANDIDATI CONFERMATI
    # ------------------------------------------------------

    candidate_long = (
        breakout_long
        and trend1h_long
        and vol15 >= CONFIRM_VOL_15M_MIN
        and body_atr >= CONFIRM_BODY_ATR_MIN
        and volatility_ok
        and not reversing_long
        and not_extended_long
    )

    candidate_short = (
        breakout_short
        and trend1h_short
        and vol15 >= CONFIRM_VOL_15M_MIN
        and body_atr >= CONFIRM_BODY_ATR_MIN
        and volatility_ok
        and not reversing_short
        and not_extended_short
    )

    # ------------------------------------------------------
    # LOG DIAGNOSTICO PRIMA DEI DERIVATI
    # ------------------------------------------------------

    attempt_long = (
        breakout_long
        or early_break_long
    )

    attempt_short = (
        breakout_short
        or early_break_short
    )

    if (
        attempt_long
        and not candidate_long
    ):

        reasons = []

        if not breakout_long:
            reasons.append(
                "breakout15m non ancora chiuso"
            )

        if not trend1h_long:
            reasons.append(
                "trend1H non LONG"
            )

        if (
            vol15
            < CONFIRM_VOL_15M_MIN
        ):
            reasons.append(
                f"volume15 {vol15:.2f}x"
            )

        if (
            body_atr
            < CONFIRM_BODY_ATR_MIN
        ):
            reasons.append(
                f"body/ATR {body_atr:.2f}"
            )

        if not volatility_ok:
            reasons.append(
                f"ATR1H {atr_pct:.2f}%"
            )

        if reversing_long:
            reasons.append(
                "inversione live"
            )

        if not not_extended_long:
            reasons.append(
                "prezzo troppo esteso"
            )

        log_no_confirm(
            symbol,
            "LONG",
            reasons
        )

    if (
        attempt_short
        and not candidate_short
    ):

        reasons = []

        if not breakout_short:
            reasons.append(
                "breakout15m non ancora chiuso"
            )

        if not trend1h_short:
            reasons.append(
                "trend1H non SHORT"
            )

        if (
            vol15
            < CONFIRM_VOL_15M_MIN
        ):
            reasons.append(
                f"volume15 {vol15:.2f}x"
            )

        if (
            body_atr
            < CONFIRM_BODY_ATR_MIN
        ):
            reasons.append(
                f"body/ATR {body_atr:.2f}"
            )

        if not volatility_ok:
            reasons.append(
                f"ATR1H {atr_pct:.2f}%"
            )

        if reversing_short:
            reasons.append(
                "inversione live"
            )

        if not not_extended_short:
            reasons.append(
                "prezzo troppo esteso"
            )

        log_no_confirm(
            symbol,
            "SHORT",
            reasons
        )

    # ------------------------------------------------------
    # SE NON CONFERMATO -> PRE
    # I PRE RESTANO CALCOLATI INTERNAMENTE.
    # NON VERRANNO INVIATI SU TELEGRAM.
    # ------------------------------------------------------

    if (
        not candidate_long
        and not candidate_short
    ):

        if pre_long:

            invalidation = (
                support
                if support < price
                else (
                    resistance
                    - atr15 * 0.80
                )
            )

            quality = 5

            quality += int(
                strong_trend1h_long
            )

            quality += int(
                trend4h_long
            )

            quality += int(
                vol15 >= 1.10
            )

            quality += int(
                early_break_long
            )

            leverage = (
                calculate_leverage(
                    price,
                    invalidation,
                    quality,
                    False
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
                "leverage": leverage,
                "early_break": early_break_long,
            }

        if pre_short:

            invalidation = (
                resistance
                if resistance > price
                else (
                    support
                    + atr15 * 0.80
                )
            )

            quality = 5

            quality += int(
                strong_trend1h_short
            )

            quality += int(
                trend4h_short
            )

            quality += int(
                vol15 >= 1.10
            )

            quality += int(
                early_break_short
            )

            leverage = (
                calculate_leverage(
                    price,
                    invalidation,
                    quality,
                    False
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
                "leverage": leverage,
                "early_break": early_break_short,
            }

        return None

    # ======================================================
    # FILTRO BTC PER TUTTI I CONFERMATI
    # ======================================================

    if candidate_long:

        if not btc_allows_confirmed(
            symbol,
            "LONG",
            btc_bias
        ):

            log_no_confirm(
                symbol,
                "LONG",
                [
                    f"BTC contrario ({btc_bias})"
                ]
            )

            return None

    if candidate_short:

        if not btc_allows_confirmed(
            symbol,
            "SHORT",
            btc_bias
        ):

            log_no_confirm(
                symbol,
                "SHORT",
                [
                    f"BTC contrario ({btc_bias})"
                ]
            )

            return None

    # ======================================================
    # DERIVATI SOLO PER CONFERMATI
    # ======================================================

    oi_change, funding = (
        get_derivatives(
            symbol
        )
    )

    if (
        oi_change is None
        or funding is None
    ):

        reasons = []

        if oi_change is None:
            reasons.append(
                "OI non disponibile"
            )

        if funding is None:
            reasons.append(
                "funding non disponibile"
            )

        direction = (
            "LONG"
            if candidate_long
            else "SHORT"
        )

        log_no_confirm(
            symbol,
            direction,
            reasons
        )

        return None

    if (
        oi_change
        < OI_CONFIRM_MIN
    ):

        direction = (
            "LONG"
            if candidate_long
            else "SHORT"
        )

        log_no_confirm(
            symbol,
            direction,
            [
                f"OI {oi_change:+.2f}% "
                f"< {OI_CONFIRM_MIN:+.2f}%"
            ]
        )

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

    # ======================================================
    # LONG
    # ======================================================

    if candidate_long:

        direction = "LONG"
        level = resistance

        funding_ok = (
            funding
            <= FUNDING_BLOCK
        )

        funding_aggressive = (
            funding
            <= FUNDING_AGGRESSIVE
        )

        oi_positive = (
            oi_change >= 0.0
        )

        oi_aggressive = (
            oi_change
            >= OI_AGGRESSIVE_MIN
        )

        btc_aligned = (
            symbol == "BTCUSDT"
            or btc_bias == "LONG"
        )

        liq_support = (
            liq_available
            and
            short_liq >= max(
                long_liq * 1.25,
                10000
            )
        )

        severe_liq_against = (
            liq_available
            and
            long_liq >= max(
                short_liq * 3.0,
                50000
            )
        )

        if not funding_ok:

            log_no_confirm(
                symbol,
                "LONG",
                [
                    f"funding troppo alto "
                    f"{funding * 100:+.4f}%"
                ]
            )

            return None

        if severe_liq_against:

            log_no_confirm(
                symbol,
                "LONG",
                [
                    "liquidazioni fortemente contrarie"
                ]
            )

            return None

        entry = price

        stop = min(
            last15["l"],
            resistance
            - atr15 * 0.35
        )

        stop -= (
            atr15 * 0.10
        )

        if stop >= entry:

            log_no_confirm(
                symbol,
                "LONG",
                [
                    "stop non valido"
                ]
            )

            return None

        risk = (
            entry - stop
        )

        quality = 7

        quality += int(
            strong_trend1h_long
        )

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
            strong15_long
        )

        aggressive_ok = (
            vol15
            >= AGGRESSIVE_VOL_15M_MIN
            and oi_aggressive
            and funding_aggressive
            and atr_pct
            <= ATR_AGGRESSIVE_MAX_PCT
            and strong15_long
            and not reversing_long
            and not severe_liq_against
            and (
                btc_aligned
                or btc_bias == "NEUTRAL"
            )
        )

    # ======================================================
    # SHORT
    # ======================================================

    else:

        direction = "SHORT"
        level = support

        funding_ok = (
            funding
            >= -FUNDING_BLOCK
        )

        funding_aggressive = (
            funding
            >= -FUNDING_AGGRESSIVE
        )

        oi_positive = (
            oi_change >= 0.0
        )

        oi_aggressive = (
            oi_change
            >= OI_AGGRESSIVE_MIN
        )

        btc_aligned = (
            symbol == "BTCUSDT"
            or btc_bias == "SHORT"
        )

        liq_support = (
            liq_available
            and
            long_liq >= max(
                short_liq * 1.25,
                10000
            )
        )

        severe_liq_against = (
            liq_available
            and
            short_liq >= max(
                long_liq * 3.0,
                50000
            )
        )

        if not funding_ok:

            log_no_confirm(
                symbol,
                "SHORT",
                [
                    f"funding troppo basso "
                    f"{funding * 100:+.4f}%"
                ]
            )

            return None

        if severe_liq_against:

            log_no_confirm(
                symbol,
                "SHORT",
                [
                    "liquidazioni fortemente contrarie"
                ]
            )

            return None

        entry = price

        stop = max(
            last15["h"],
            support
            + atr15 * 0.35
        )

        stop += (
            atr15 * 0.10
        )

        if stop <= entry:

            log_no_confirm(
                symbol,
                "SHORT",
                [
                    "stop non valido"
                ]
            )

            return None

        risk = (
            stop - entry
        )

        quality = 7

        quality += int(
            strong_trend1h_short
        )

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
            strong15_short
        )

        aggressive_ok = (
            vol15
            >= AGGRESSIVE_VOL_15M_MIN
            and oi_aggressive
            and funding_aggressive
            and atr_pct
            <= ATR_AGGRESSIVE_MAX_PCT
            and strong15_short
            and not reversing_short
            and not severe_liq_against
            and (
                btc_aligned
                or btc_bias == "NEUTRAL"
            )
        )

    # ======================================================
    # ENTRY / LEVA / TARGET
    # ======================================================

    entry_low = (
        entry
        - atr15 * 0.10
    )

    entry_high = (
        entry
        + atr15 * 0.10
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
            entry + risk
        )

        tp2 = (
            entry + risk * 2
        )

        tp3 = (
            entry + risk * 3
        )

    else:

        tp1 = (
            entry - risk
        )

        tp2 = (
            entry - risk * 2
        )

        tp3 = (
            entry - risk * 3
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

        "volume1h": vol1h,
        "volume15": vol15,

        "atr_pct": atr_pct,

        "quality": quality,

        "leverage": leverage,

        "oi": oi_change,
        "funding": funding,

        "btc": btc_bias,

        "long_liq": long_liq,
        "short_liq": short_liq,

        "liq_available": liq_available,

        "aggressive": (
            leverage >= 20
            and aggressive_ok
        ),
    }


# ==========================================================
# MESSAGGIO
# ==========================================================

def build_message(symbol, signal):

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

    # Questo blocco resta disponibile internamente,
    # ma i PRE non vengono inviati a Telegram.
    if signal["type"] == "PRE":

        setup = (
            "prima rottura struttura 15m"
            if signal["early_break"]
            else "avvicinamento struttura 15m"
        )

        return (
            "🟠 PRE-SEGNALE\n"

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

            f"Grado conferma: {grade}"
        )

    title = (
        "🔥 SEGNALE CONFERMATO AGGRESSIVO"
        if signal["aggressive"]
        else "🟢 SEGNALE CONFERMATO"
    )

    funding_pct = (
        signal["funding"]
        * 100
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

        f"Volume 15m: "
        f"{signal['volume15']:.2f}x media\n"

        f"Volume 1H: "
        f"{signal['volume1h']:.2f}x media\n"

        f"Open Interest 15m: "
        f"{signal['oi']:+.2f}%\n"

        f"Funding: "
        f"{funding_pct:+.4f}%\n"

        f"ATR 1H: "
        f"{signal['atr_pct']:.2f}%\n"

        f"Filtro BTC: "
        f"{signal['btc']}\n"

        f"Liquidazioni 15m: "
        f"{liq_text}\n"

        f"Grado conferma: "
        f"{grade}\n"

        "Timeframe: 15m / 1H / 4H\n\n"

        "Nota: leva indicativa; "
        "con leve elevate il margine "
        "di errore e molto ridotto."
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
        round(
            signal["level"],
            8
        )
    )

    previous = (
        signal_state.get(key)
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


# ==========================================================
# SCANNER
# ==========================================================

def scan_market():

    successful = 0

    btc_data = (
        market_data(
            "BTCUSDT"
        )
    )

    btc_bias = (
        get_btc_bias(
            btc_data
        )
    )

    print(
        f"BTC bias corrente: {btc_bias}"
    )

    for symbol in SYMBOLS:

        try:

            data = (
                btc_data
                if symbol == "BTCUSDT"
                else market_data(
                    symbol
                )
            )

            successful += 1

            signal = (
                analyze_symbol(
                    symbol,
                    data,
                    btc_bias
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

                # ==========================================
                # MODIFICA:
                # Telegram riceve SOLO i CONFERMATI.
                # I PRE continuano a essere calcolati
                # e restano visibili nei Deploy Logs.
                # ==========================================

                if (
                    signal["type"] == "CONFIRMED"
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
# AVVIO WEBSOCKET
# ==========================================================

threading.Thread(
    target=ws_loop,
    daemon=True
).start()


print(
    "CryptoSignalAI12 avviato - "
    "modalita EARLY v5.1 CONFIRMED ONLY attiva"
)


send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "Modalita EARLY v5.1 CONFIRMED ONLY attiva.\n"
    "PRE calcolati internamente, notifiche disattivate.\n"
    "Telegram invia solo SEGNALI CONFERMATI.\n"
    "Confermati: BTC allineato o neutrale.\n"
    "BTC contrario blocca il confermato.\n"
    "Diagnostica NO CONFIRM attiva.\n"
    "Filtro anti-inversione live attivo.\n"
    "20x+ con conferme aggiuntive.\n"
    "Protezione Binance 429 attiva."
)


# ==========================================================
# LOOP
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

    sleep_time = max(
        1.0,
        SCAN_SECONDS
        - elapsed
    )

    time.sleep(
        sleep_time
    )
