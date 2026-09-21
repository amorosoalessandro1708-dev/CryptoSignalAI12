import os, time, json, threading
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
KLINES = f"{REST}/fapi/v1/klines"
OI_URL = f"{REST}/futures/data/openInterestHist"
PREMIUM = f"{REST}/fapi/v1/premiumIndex"

# Binance USD-M 2026: nuova architettura WebSocket.
# Prima /market, poi fallback /public.
LIQ_WS_URLS = [
    "wss://fstream.binance.com/market/ws/!forceOrder@arr",
    "wss://fstream.binance.com/public/ws/!forceOrder@arr",
]

SCAN_SECONDS = 60
LIQ_WINDOW = 15 * 60
FUNDING_BLOCK = 0.0005
FUNDING_AGGR = 0.0003
ATR_MIN_PCT = 0.10
ATR_MAX_PCT = 5.00
ATR_AGGR_MAX_PCT = 2.50
OI_CONFIRM_MIN = -0.25
OI_AGGR_MIN = 0.15
LEVERAGE_SAFETY = 0.35

LEV_STEPS = [
    1, 2, 3, 5, 10, 15, 20,
    25, 30, 40, 50, 75, 100
]

state = {}
liq_events = deque()
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


def get_json(url, params=None, timeout=15):
    r = requests.get(
        url,
        params=params,
        timeout=timeout
    )

    r.raise_for_status()
    return r.json()


def get_klines(symbol, interval, limit=120):
    return get_json(
        KLINES,
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
    )


def candles(raw):
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
        "15m": candles(
            get_klines(symbol, "15m")
        ),
        "1h": candles(
            get_klines(symbol, "1h")
        ),
        "4h": candles(
            get_klines(symbol, "4h")
        ),
    }


def ema(values, period):
    if len(values) < period:
        return None

    k = 2 / (period + 1)
    x = sum(values[:period]) / period

    for p in values[period:]:
        x = (p - x) * k + x

    return x


def atr(cs, period=14):
    if len(cs) < period + 1:
        return None

    tr = []

    for i in range(1, len(cs)):
        h = cs[i]["h"]
        l = cs[i]["l"]
        pc = cs[i - 1]["c"]

        tr.append(
            max(
                h - l,
                abs(h - pc),
                abs(l - pc)
            )
        )

    return sum(tr[-period:]) / period


def volume_ratio(cs, period=20):
    if len(cs) < period + 2:
        return 0.0

    cur = cs[-2]["v"]

    prev = [
        x["v"]
        for x in cs[-(period + 2):-2]
    ]

    avg = sum(prev) / len(prev)

    return cur / avg if avg > 0 else 0.0


def candle_strength(c, direction):
    rng = c["h"] - c["l"]

    if rng <= 0:
        return False

    body = abs(
        c["c"] - c["o"]
    ) / rng

    pos = (
        c["c"] - c["l"]
    ) / rng

    if direction == "LONG":
        return (
            c["c"] > c["o"]
            and body >= 0.55
            and pos >= 0.70
        )

    return (
        c["c"] < c["o"]
        and body >= 0.55
        and pos <= 0.30
    )


def derivatives(symbol):
    oi = None
    funding = None

    try:
        d = get_json(
            OI_URL,
            {
                "symbol": symbol,
                "period": "15m",
                "limit": 3
            }
        )

        if len(d) >= 2:
            a = float(
                d[-2]["sumOpenInterestValue"]
            )

            b = float(
                d[-1]["sumOpenInterestValue"]
            )

            if a > 0:
                oi = (
                    (b - a)
                    / a
                    * 100
                )

    except Exception as e:
        print(
            symbol,
            "OI ERRORE:",
            e
        )

    try:
        d = get_json(
            PREMIUM,
            {"symbol": symbol}
        )

        funding = float(
            d["lastFundingRate"]
        )

    except Exception as e:
        print(
            symbol,
            "FUNDING ERRORE:",
            e
        )

    return oi, funding


def store_liq(payload):
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

        o = item.get("o", item)
        symbol = o.get("s")

        if symbol not in SYMBOLS:
            continue

        qty = float(
            o.get("z")
            or o.get("q")
            or 0
        )

        price = float(
            o.get("ap")
            or o.get("p")
            or 0
        )

        notional = qty * price

        if notional <= 0:
            continue

        side = o.get("S")

        kind = (
            "LONG_LIQ"
            if side == "SELL"
            else "SHORT_LIQ"
        )

        ts = int(
            o.get("T")
            or item.get("E")
            or time.time() * 1000
        ) / 1000

        with liq_lock:
            liq_events.append({
                "s": symbol,
                "k": kind,
                "n": notional,
                "t": ts
            })


def ws_message(ws, message):
    try:
        store_liq(
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
    i = 0

    while True:
        url = LIQ_WS_URLS[
            i % len(LIQ_WS_URLS)
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

        i += 1
        time.sleep(5)


def liq_metrics(symbol):
    cutoff = (
        time.time()
        - LIQ_WINDOW
    )

    long_liq = 0.0
    short_liq = 0.0

    with liq_lock:
        while (
            liq_events
            and liq_events[0]["t"]
            < cutoff
        ):
            liq_events.popleft()

        for x in liq_events:
            if x["s"] != symbol:
                continue

            if x["k"] == "LONG_LIQ":
                long_liq += x["n"]
            else:
                short_liq += x["n"]

    return long_liq, short_liq


def fmt_price(v):
    if v >= 1000:
        return f"{v:.2f}"

    if v >= 1:
        return f"{v:.4f}"

    return f"{v:.6f}"


def fmt_money(v):
    if v >= 1_000_000:
        return (
            f"${v / 1_000_000:.2f}M"
        )

    if v >= 1_000:
        return (
            f"${v / 1_000:.1f}K"
        )

    return f"${v:.0f}"


def leverage_cap(q):
    if q >= 12:
        return 100

    if q >= 11:
        return 75

    if q >= 10:
        return 50

    if q >= 9:
        return 30

    if q >= 8:
        return 25

    if q >= 7:
        return 20

    return 15


def calc_leverage(
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

    out = 1

    for x in LEV_STEPS:
        if x <= allowed:
            out = x
        else:
            break

    return out


def btc_bias(data):
    c1 = data["1h"]
    c4 = data["4h"]

    a1 = [
        x["c"]
        for x in c1[:-1]
    ]

    a4 = [
        x["c"]
        for x in c4[:-1]
    ]

    e20_1 = ema(a1, 20)
    e50_1 = ema(a1, 50)

    e20_4 = ema(a4, 20)
    e50_4 = ema(a4, 50)

    if None in (
        e20_1,
        e50_1,
        e20_4,
        e50_4
    ):
        return "NEUTRAL"

    p1 = c1[-2]["c"]
    p4 = c4[-2]["c"]

    if (
        p1 > e20_1 > e50_1
        and p4 > e20_4 > e50_4
    ):
        return "LONG"

    if (
        p1 < e20_1 < e50_1
        and p4 < e20_4 < e50_4
    ):
        return "SHORT"

    return "NEUTRAL"


def grade(q):
    if q >= 10:
        return "ALTO"

    if q >= 8:
        return "MEDIO-ALTO"

    return "MEDIO"


def analyze(
    symbol,
    data,
    btc
):
    c15 = data["15m"]
    c1 = data["1h"]
    c4 = data["4h"]

    l15 = c15[-2]
    l1 = c1[-2]
    l4 = c4[-2]

    a1 = [
        x["c"]
        for x in c1[:-1]
    ]

    a4 = [
        x["c"]
        for x in c4[:-1]
    ]

    e20_1 = ema(a1, 20)
    e50_1 = ema(a1, 50)

    e20_4 = ema(a4, 20)
    e50_4 = ema(a4, 50)

    at = atr(
        c1[:-1],
        14
    )

    vol = volume_ratio(c1)

    if None in (
        e20_1,
        e50_1,
        e20_4,
        e50_4,
        at
    ):
        return None

    prev20 = c1[-22:-2]

    res = max(
        x["h"]
        for x in prev20
    )

    sup = min(
        x["l"]
        for x in prev20
    )

    p1 = l1["c"]
    p15 = l15["c"]

    t1_long = (
        p1 > e20_1 > e50_1
    )

    t1_short = (
        p1 < e20_1 < e50_1
    )

    t4_long = (
        l4["c"]
        > e20_4
        > e50_4
    )

    t4_short = (
        l4["c"]
        < e20_4
        < e50_4
    )

    br_long = (
        p1 > res
    )

    br_short = (
        p1 < sup
    )

    atr_pct = (
        at
        / p1
        * 100
    )

    vol_ok = (
        ATR_MIN_PCT
        <= atr_pct
        <= ATR_MAX_PCT
    )

    window = c15[-4:-1]

    tol = (
        at * 0.12
    )

    ret_long = any(
        x["l"] <= res + tol
        and x["c"] > res
        for x in window
    )

    ret_short = any(
        x["h"] >= sup - tol
        and x["c"] < sup
        for x in window
    )

    not_ext_long = (
        p15
        <= res + at * 0.80
    )

    not_ext_short = (
        p15
        >= sup - at * 0.80
    )

    strong_long = (
        candle_strength(
            l1,
            "LONG"
        )
    )

    strong_short = (
        candle_strength(
            l1,
            "SHORT"
        )
    )

    near_long = (
        p15 <= res
        and abs(res - p15)
        <= at * 0.25
        and t1_long
    )

    near_short = (
        p15 >= sup
        and abs(p15 - sup)
        <= at * 0.25
        and t1_short
    )

    prelim_long = (
        br_long
        and t1_long
        and vol >= 1.5
        and ret_long
        and strong_long
        and not_ext_long
        and vol_ok
        and not t4_short
    )

    prelim_short = (
        br_short
        and t1_short
        and vol >= 1.5
        and ret_short
        and strong_short
        and not_ext_short
        and vol_ok
        and not t4_long
    )

    # PRE-SEGNALI
    if (
        not prelim_long
        and not prelim_short
    ):

        if (
            near_long
            and vol >= 1.2
        ):
            inv = (
                res
                - at * 0.5
            )

            q = (
                5
                + (
                    1
                    if t4_long
                    else 0
                )
            )

            lev = calc_leverage(
                p15,
                inv,
                q,
                False
            )

            return {
                "type": "PRE",
                "direction": "LONG",
                "price": p15,
                "level": res,
                "invalidation": inv,
                "volume": vol,
                "atr_pct": atr_pct,
                "quality": q,
                "leverage": lev
            }

        if (
            near_short
            and vol >= 1.2
        ):
            inv = (
                sup
                + at * 0.5
            )

            q = (
                5
                + (
                    1
                    if t4_short
                    else 0
                )
            )

            lev = calc_leverage(
                p15,
                inv,
                q,
                False
            )

            return {
                "type": "PRE",
                "direction": "SHORT",
                "price": p15,
                "level": sup,
                "invalidation": inv,
                "volume": vol,
                "atr_pct": atr_pct,
                "quality": q,
                "leverage": lev
            }

        return None

    oi, funding = derivatives(
        symbol
    )

    if (
        oi is None
        or funding is None
    ):
        return None

    long_liq, short_liq = (
        liq_metrics(symbol)
    )

    liq_available = (
        long_liq
        + short_liq
        > 0
    )

    # LONG
    if prelim_long:
        direction = "LONG"
        level = res

        funding_ok = (
            funding
            <= FUNDING_BLOCK
        )

        funding_aggr = (
            funding
            <= FUNDING_AGGR
        )

        oi_ok = (
            oi
            >= OI_CONFIRM_MIN
        )

        oi_aggr = (
            oi
            >= OI_AGGR_MIN
        )

        btc_conflict = (
            symbol != "BTCUSDT"
            and btc == "SHORT"
        )

        btc_aligned = (
            symbol == "BTCUSDT"
            or btc == "LONG"
        )

        liq_support = (
            liq_available
            and short_liq
            >= max(
                long_liq * 1.25,
                10000
            )
        )

        liq_against = (
            liq_available
            and long_liq
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

        entry = p15

        stop = min(
            min(
                x["l"]
                for x in window
            ),
            res - at * 0.25
        )

        stop -= (
            at * 0.10
        )

        if stop >= entry:
            return None

        risk = (
            entry - stop
        )

        q = 7

        q += int(t4_long)
        q += int(oi_aggr)
        q += int(funding_aggr)
        q += int(btc_aligned)
        q += int(liq_support)

        q += int(
            atr_pct
            <= ATR_AGGR_MAX_PCT
        )

        aggr_ok = (
            t4_long
            and funding_aggr
            and oi_aggr
            and atr_pct
            <= ATR_AGGR_MAX_PCT
            and btc_aligned
            and (
                liq_support
                or not liq_available
            )
        )

    # SHORT
    else:
        direction = "SHORT"
        level = sup

        funding_ok = (
            funding
            >= -FUNDING_BLOCK
        )

        funding_aggr = (
            funding
            >= -FUNDING_AGGR
        )

        oi_ok = (
            oi
            >= OI_CONFIRM_MIN
        )

        oi_aggr = (
            oi
            >= OI_AGGR_MIN
        )

        btc_conflict = (
            symbol != "BTCUSDT"
            and btc == "LONG"
        )

        btc_aligned = (
            symbol == "BTCUSDT"
            or btc == "SHORT"
        )

        liq_support = (
            liq_available
            and long_liq
            >= max(
                short_liq * 1.25,
                10000
            )
        )

        liq_against = (
            liq_available
            and short_liq
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

        entry = p15

        stop = max(
            max(
                x["h"]
                for x in window
            ),
            sup + at * 0.25
        )

        stop += (
            at * 0.10
        )

        if stop <= entry:
            return None

        risk = (
            stop - entry
        )

        q = 7

        q += int(t4_short)
        q += int(oi_aggr)
        q += int(funding_aggr)
        q += int(btc_aligned)
        q += int(liq_support)

        q += int(
            atr_pct
            <= ATR_AGGR_MAX_PCT
        )

        aggr_ok = (
            t4_short
            and funding_aggr
            and oi_aggr
            and atr_pct
            <= ATR_AGGR_MAX_PCT
            and btc_aligned
            and (
                liq_support
                or not liq_available
            )
        )

    entry_low = (
        entry
        - at * 0.08
    )

    entry_high = (
        entry
        + at * 0.08
    )

    lev = calc_leverage(
        entry,
        stop,
        q,
        aggr_ok
    )

    if direction == "LONG":
        tp1 = entry + risk
        tp2 = entry + 2 * risk
        tp3 = entry + 3 * risk

    else:
        tp1 = entry - risk
        tp2 = entry - 2 * risk
        tp3 = entry - 3 * risk

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
        "volume": vol,
        "atr_pct": atr_pct,
        "quality": q,
        "leverage": lev,
        "oi": oi,
        "funding": funding,
        "btc": btc,
        "long_liq": long_liq,
        "short_liq": short_liq,
        "liq_available": liq_available,
        "aggressive": (
            lev >= 20
            and aggr_ok
        )
    }


def build_message(
    symbol,
    s
):
    pair = symbol.replace(
        "USDT",
        "/USDT"
    )

    g = grade(
        s["quality"]
    )

    # PRE
    if s["type"] == "PRE":
        return (
            "🟠 PRE-SEGNALE\n"

            f"{pair} — "
            f"{s['direction']}\n\n"

            f"Livello chiave: "
            f"{fmt_price(s['level'])}\n"

            f"Prezzo attuale: "
            f"{fmt_price(s['price'])}\n"

            "Possibile ENTRY: "
            "solo dopo conferma\n"

            f"Invalidazione: "
            f"{fmt_price(s['invalidation'])}\n"

            f"Volume 1H: "
            f"{s['volume']:.2f}x media\n"

            f"ATR 1H: "
            f"{s['atr_pct']:.2f}%\n"

            f"Leva potenziale: "
            f"{s['leverage']}x\n"

            "Timeframe: "
            "15m / 1H\n"

            f"Grado conferma: "
            f"{g}"
        )

    if s["aggressive"]:
        title = (
            "🔥 SEGNALE CONFERMATO "
            "AGGRESSIVO"
        )

    else:
        title = (
            "🟢 SEGNALE CONFERMATO"
        )

    funding_pct = (
        s["funding"]
        * 100
    )

    if s["direction"] == "LONG":
        fav = s["short_liq"]
        against = s["long_liq"]

    else:
        fav = s["long_liq"]
        against = s["short_liq"]

    if s["liq_available"]:
        liq_txt = (
            f"Favorevoli "
            f"{fmt_money(fav)} | "
            f"Contrarie "
            f"{fmt_money(against)}"
        )

    else:
        liq_txt = (
            "In raccolta / "
            "nessun evento recente"
        )

    return (
        f"{title}\n"

        f"{pair} — "
        f"{s['direction']}\n\n"

        f"ENTRY: "
        f"{fmt_price(s['entry_low'])}"
        " - "
        f"{fmt_price(s['entry_high'])}\n"

        f"SL: "
        f"{fmt_price(s['sl'])}\n"

        f"TP1: "
        f"{fmt_price(s['tp1'])}\n"

        f"TP2: "
        f"{fmt_price(s['tp2'])}\n"

        f"TP3: "
        f"{fmt_price(s['tp3'])}\n"

        f"Leva indicativa: "
        f"{s['leverage']}x\n\n"

        f"Volume 1H: "
        f"{s['volume']:.2f}x media\n"

        f"Open Interest 15m: "
        f"{s['oi']:+.2f}%\n"

        f"Funding: "
        f"{funding_pct:+.4f}%\n"

        f"ATR 1H: "
        f"{s['atr_pct']:.2f}%\n"

        "Retest: OK\n"

        f"Filtro BTC: "
        f"{s['btc']}\n"

        f"Liquidazioni 15m: "
        f"{liq_txt}\n"

        f"Grado conferma: "
        f"{g}\n"

        "Timeframe: "
        "15m / 1H / 4H\n\n"

        "Nota: leva indicativa; "
        "con leve elevate il margine "
        "di errore e molto ridotto."
    )


def should_send(
    symbol,
    s
):
    now = time.time()

    key = (
        f"{symbol}:"
        f"{s['direction']}:"
        f"{s['type']}"
    )

    sig = (
        s["type"],
        s["direction"],
        round(
            s["level"],
            8
        )
    )

    old = state.get(key)

    if (
        old
        and old["sig"] == sig
        and now - old["t"]
        < 6 * 3600
    ):
        return False

    state[key] = {
        "sig": sig,
        "t": now
    }

    for k in list(state):
        if (
            now - state[k]["t"]
            > 12 * 3600
        ):
            del state[k]

    return True


def scan_market():
    ok = 0

    btc_data = market_data(
        "BTCUSDT"
    )

    btc = btc_bias(
        btc_data
    )

    for symbol in SYMBOLS:

        try:
            if symbol == "BTCUSDT":
                data = btc_data

            else:
                data = market_data(
                    symbol
                )

            ok += 1

            s = analyze(
                symbol,
                data,
                btc
            )

            if s:
                print(
                    symbol,
                    s["type"],
                    s["direction"],
                    "quality=",
                    s["quality"],
                    "leverage=",
                    s["leverage"]
                )

                if should_send(
                    symbol,
                    s
                ):
                    send_telegram(
                        build_message(
                            symbol,
                            s
                        )
                    )

        except Exception as e:
            print(
                symbol,
                "ERRORE:",
                e
            )

    print(
        f"Scansione completata: "
        f"{ok}/"
        f"{len(SYMBOLS)} coppie"
    )


threading.Thread(
    target=ws_loop,
    daemon=True
).start()


print(
    "CryptoSignalAI12 avviato - "
    "filtri avanzati attivi"
)


send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "Filtri avanzati attivi: "
    "OI + funding + retest + "
    "liquidazioni + BTC + volatilita.\n"
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
