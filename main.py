import os
import time
import requests

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SYMBOLS = [
    "SOLUSDT", "BTCUSDT", "ETHUSDT", "XRPUSDT",
    "BNBUSDT", "DOGEUSDT", "AVAXUSDT", "SUIUSDT",
    "LINKUSDT", "ADAUSDT", "NEARUSDT", "UNIUSDT"
]

BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"

# Memoria anti-spam
signal_state = {}


def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram non configurato")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            data={
                "chat_id": CHAT_ID,
                "text": message
            },
            timeout=20
        )
        response.raise_for_status()

    except Exception as e:
        print("Errore Telegram:", e)


def get_klines(symbol, interval, limit=120):
    response = requests.get(
        BINANCE_URL,
        params={
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        },
        timeout=15
    )

    response.raise_for_status()
    return response.json()


def parse_candles(raw):
    candles = []

    for c in raw:
        candles.append({
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5])
        })

    return candles


def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    value = sum(values[:period]) / period

    for price in values[period:]:
        value = (
            (price - value) * multiplier
        ) + value

    return value


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    ranges = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        previous_close = candles[i - 1]["close"]

        true_range = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close)
        )

        ranges.append(true_range)

    return sum(ranges[-period:]) / period


def volume_ratio(candles, period=20):
    if len(candles) < period + 2:
        return 0

    # Ultima candela 1H chiusa
    current_volume = candles[-2]["volume"]

    previous = [
        c["volume"]
        for c in candles[-(period + 2):-2]
    ]

    average = sum(previous) / len(previous)

    if average == 0:
        return 0

    return current_volume / average


def market_data(symbol):
    return {
        "15m": parse_candles(
            get_klines(symbol, "15m")
        ),
        "1h": parse_candles(
            get_klines(symbol, "1h")
        ),
        "4h": parse_candles(
            get_klines(symbol, "4h")
        )
    }


def analyze_symbol(symbol, data):
    c15 = data["15m"]
    c1h = data["1h"]
    c4h = data["4h"]

    # -2 = ultima candela chiusa
    last15 = c15[-2]
    last1h = c1h[-2]

    closes1h = [
        c["close"] for c in c1h[:-1]
    ]

    closes4h = [
        c["close"] for c in c4h[:-1]
    ]

    ema20_1h = ema(closes1h, 20)
    ema50_1h = ema(closes1h, 50)

    ema20_4h = ema(closes4h, 20)
    ema50_4h = ema(closes4h, 50)

    current_atr = atr(c1h[:-1], 14)
    vol_ratio = volume_ratio(c1h)

    if None in (
        ema20_1h,
        ema50_1h,
        ema20_4h,
        ema50_4h,
        current_atr
    ):
        return None

    # Massimo/minimo delle 20 candele
    # precedenti alla candela valutata
    previous_20 = c1h[-22:-2]

    resistance = max(
        c["high"] for c in previous_20
    )

    support = min(
        c["low"] for c in previous_20
    )

    price = last1h["close"]

    trend1h_long = (
        price > ema20_1h
        and ema20_1h > ema50_1h
    )

    trend1h_short = (
        price < ema20_1h
        and ema20_1h < ema50_1h
    )

    trend4h_long = (
        ema20_4h > ema50_4h
    )

    trend4h_short = (
        ema20_4h < ema50_4h
    )

    breakout_long = (
        price > resistance
    )

    breakout_short = (
        price < support
    )

    # PRE: 15m vicino al livello chiave 1H
    distance_res = abs(
        resistance - last15["close"]
    )

    distance_sup = abs(
        last15["close"] - support
    )

    near_long = (
        last15["close"] <= resistance
        and distance_res <= current_atr * 0.25
    )

    near_short = (
        last15["close"] >= support
        and distance_sup <= current_atr * 0.25
    )

    long_score = 0
    short_score = 0

    if trend1h_long:
        long_score += 2

    if trend1h_short:
        short_score += 2

    if trend4h_long:
        long_score += 1

    if trend4h_short:
        short_score += 1

    if vol_ratio >= 1.5:
        long_score += 1
        short_score += 1

    if breakout_long:
        long_score += 3

    if breakout_short:
        short_score += 3

    if near_long:
        long_score += 1

    if near_short:
        short_score += 1

    # CONFERMATO LONG
    if (
        breakout_long
        and trend1h_long
        and vol_ratio >= 1.5
        and long_score >= 6
    ):
        entry_low = (
            price - current_atr * 0.15
        )

        entry_high = (
            price + current_atr * 0.10
        )

        stop = (
            price - current_atr * 1.2
        )

        risk = price - stop

        return {
            "type": "CONFIRMED",
            "direction": "LONG",
            "price": price,
            "entry_low": entry_low,
            "entry_high": entry_high,
            "sl": stop,
            "tp1": price + risk,
            "tp2": price + risk * 2,
            "tp3": price + risk * 3,
            "level": resistance,
            "score": long_score,
            "volume": vol_ratio,
            "atr": current_atr
        }

    # CONFERMATO SHORT
    if (
        breakout_short
        and trend1h_short
        and vol_ratio >= 1.5
        and short_score >= 6
    ):
        entry_low = (
            price - current_atr * 0.10
        )

        entry_high = (
            price + current_atr * 0.15
        )

        stop = (
            price + current_atr * 1.2
        )

        risk = stop - price

        return {
            "type": "CONFIRMED",
            "direction": "SHORT",
            "price": price,
            "entry_low": entry_low,
            "entry_high": entry_high,
            "sl": stop,
            "tp1": price - risk,
            "tp2": price - risk * 2,
            "tp3": price - risk * 3,
            "level": support,
            "score": short_score,
            "volume": vol_ratio,
            "atr": current_atr
        }

    # PRE LONG
    if (
        near_long
        and trend1h_long
        and vol_ratio >= 1.2
        and long_score >= 4
    ):
        return {
            "type": "PRE",
            "direction": "LONG",
            "price": last15["close"],
            "level": resistance,
            "score": long_score,
            "volume": vol_ratio,
            "atr": current_atr
        }

    # PRE SHORT
    if (
        near_short
        and trend1h_short
        and vol_ratio >= 1.2
        and short_score >= 4
    ):
        return {
            "type": "PRE",
            "direction": "SHORT",
            "price": last15["close"],
            "level": support,
            "score": short_score,
            "volume": vol_ratio,
            "atr": current_atr
        }

    return None


def confirmation_grade(score):
    if score >= 7:
        return "ALTO"

    if score >= 5:
        return "MEDIO-ALTO"

    return "MEDIO"


def fmt(value):
    if value >= 1000:
        return f"{value:.2f}"

    if value >= 1:
        return f"{value:.4f}"

    return f"{value:.6f}"


def build_message(symbol, signal):
    pair = symbol.replace(
        "USDT",
        "/USDT"
    )

    grade = confirmation_grade(
        signal["score"]
    )

    if signal["type"] == "PRE":

        direction = signal["direction"]

        if direction == "LONG":
            missing = (
                "Chiusura 1H sopra il livello "
                "chiave con volume forte"
            )

            invalidation = (
                signal["level"]
                - signal["atr"] * 0.5
            )

        else:
            missing = (
                "Chiusura 1H sotto il livello "
                "chiave con volume forte"
            )

            invalidation = (
                signal["level"]
                + signal["atr"] * 0.5
            )

        return (
            "🟠 PRE-SEGNALE\n"
            f"{pair} — {direction}\n\n"
            f"Livello chiave: "
            f"{fmt(signal['level'])}\n"
            f"Prezzo attuale: "
            f"{fmt(signal['price'])}\n"
            "Possibile ENTRY: solo dopo "
            "conferma del livello\n"
            f"Volume 1H: "
            f"{signal['volume']:.2f}x media\n"
            f"Manca: {missing}\n"
            f"Invalidazione: "
            f"{fmt(invalidation)}\n"
            "Timeframe: 15m / 1H\n"
            f"Grado conferma: {grade}"
        )

    direction = signal["direction"]

    return (
        "🟢 SEGNALE CONFERMATO\n"
        f"{pair} — {direction}\n\n"
        f"ENTRY: "
        f"{fmt(signal['entry_low'])} - "
        f"{fmt(signal['entry_high'])}\n"
        f"SL: {fmt(signal['sl'])}\n"
        f"TP1: {fmt(signal['tp1'])}\n"
        f"TP2: {fmt(signal['tp2'])}\n"
        f"TP3: {fmt(signal['tp3'])}\n"
        f"INVALIDAZIONE: "
        f"{fmt(signal['sl'])}\n"
        f"Volume 1H: "
        f"{signal['volume']:.2f}x media\n"
        "Timeframe: 15m / 1H\n"
        f"Grado conferma: {grade}\n"
        "Leva: da definire in base a "
        "stop, volatilità e rischio"
    )


def should_send(symbol, signal):
    key = (
        f"{symbol}:"
        f"{signal['direction']}"
    )

    previous = signal_state.get(key)
    current = signal["type"]

    if previous == current:
        return False

    signal_state[key] = current
    return True


def reset_inactive(active_keys):
    for key in list(signal_state.keys()):
        if key not in active_keys:
            del signal_state[key]


def scan_market():
    active_keys = set()
    successful = 0

    for symbol in SYMBOLS:

        try:
            data = market_data(symbol)
            successful += 1

            signal = analyze_symbol(
                symbol,
                data
            )

            if signal:
                key = (
                    f"{symbol}:"
                    f"{signal['direction']}"
                )

                active_keys.add(key)

                print(
                    symbol,
                    signal["type"],
                    signal["direction"],
                    "score=",
                    signal["score"]
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

    reset_inactive(active_keys)

    print(
        "Scansione completata: "
        f"{successful}/"
        f"{len(SYMBOLS)} coppie"
    )


print(
    "CryptoSignalAI12 avviato - "
    "motore segnali attivo"
)

send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "Monitor 12 perpetual attivo.\n"
    "Scansione 15m / 1H / 4H "
    "ogni 60 secondi."
)

while True:
    try:
        scan_market()

    except Exception as e:
        print(
            "Errore scansione generale:",
            e
        )

    time.sleep(60)
