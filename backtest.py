import requests
import time

SYMBOLS = [
    "SOLUSDT", "BTCUSDT", "ETHUSDT", "XRPUSDT",
    "BNBUSDT", "DOGEUSDT", "AVAXUSDT", "SUIUSDT",
    "LINKUSDT", "ADAUSDT", "NEARUSDT", "UNIUSDT"
]

BASE_URL = "https://fapi.binance.com/fapi/v1/klines"

# Numero di candele 1H da analizzare per ogni crypto.
# 1500 = circa 62 giorni.
LIMIT = 1500

# Quante candele successive controlliamo per vedere
# se il segnale raggiunge TP o SL.
LOOK_FORWARD = 48


def get_klines(symbol, interval, limit=1500):
    all_data = []
    end_time = None

    while len(all_data) < limit:
        batch = min(1000, limit - len(all_data))

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": batch
        }

        if end_time is not None:
            params["endTime"] = end_time

        response = requests.get(
            BASE_URL,
            params=params,
            timeout=20
        )
        response.raise_for_status()

        data = response.json()

        if not data:
            break

        all_data = data + all_data
        end_time = data[0][0] - 1

        time.sleep(0.1)

    return all_data[-limit:]


def parse(raw):
    result = []

    for c in raw:
        result.append({
            "time": int(c[0]),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5])
        })

    return result


def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    value = sum(values[:period]) / period

    for price in values[period:]:
        value = (
            price * multiplier
            + value * (1 - multiplier)
        )

    return value


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


def volume_ratio(candles, period=20):
    if len(candles) < period + 1:
        return None

    current = candles[-1]["volume"]

    previous = [
        c["volume"]
        for c in candles[-period - 1:-1]
    ]

    average = sum(previous) / len(previous)

    if average == 0:
        return None

    return current / average


def get_4h_context(candles_4h, timestamp):
    available = [
        c for c in candles_4h
        if c["time"] <= timestamp
    ]

    if len(available) < 55:
        return None, None

    closes = [c["close"] for c in available]

    ema20 = ema(closes[-100:], 20)
    ema50 = ema(closes[-100:], 50)

    return ema20, ema50


def evaluate_trade(
    future,
    direction,
    entry,
    sl,
    tp1,
    tp2,
    tp3
):
    hit1 = False
    hit2 = False
    hit3 = False

    for candle in future:
        high = candle["high"]
        low = candle["low"]

        if direction == "LONG":

            # Regola conservativa:
            # se nella stessa candela tocca SL e TP,
            # consideriamo prima lo SL.
            if low <= sl:
                return hit1, hit2, hit3, True

            if high >= tp1:
                hit1 = True

            if high >= tp2:
                hit2 = True

            if high >= tp3:
                hit3 = True

        else:

            if high >= sl:
                return hit1, hit2, hit3, True

            if low <= tp1:
                hit1 = True

            if low <= tp2:
                hit2 = True

            if low <= tp3:
                hit3 = True

        if hit3:
            return hit1, hit2, hit3, False

    return hit1, hit2, hit3, False


def backtest_symbol(symbol):
    print("\nAnalizzo", symbol)

    raw_1h = get_klines(symbol, "1h", LIMIT)
    raw_4h = get_klines(symbol, "4h", 500)

    c1h = parse(raw_1h)
    c4h = parse(raw_4h)

    stats = {
        "signals": 0,
        "tp1": 0,
        "tp2": 0,
        "tp3": 0,
        "sl": 0
    }

    # Servono almeno 60 candele per EMA50.
    for i in range(60, len(c1h) - LOOK_FORWARD):

        history = c1h[:i + 1]
        current = history[-1]

        closes = [
            c["close"]
            for c in history
        ]

        ema20_1h = ema(closes[-100:], 20)
        ema50_1h = ema(closes[-100:], 50)

        current_atr = atr(history[-30:], 14)
        vol_ratio = volume_ratio(history, 20)

        if (
            ema20_1h is None
            or ema50_1h is None
            or current_atr is None
            or vol_ratio is None
        ):
            continue

        previous_20 = history[-21:-1]

        resistance = max(
            c["high"] for c in previous_20
        )

        support = min(
            c["low"] for c in previous_20
        )

        price = current["close"]

        trend1h_long = (
            price > ema20_1h
            and ema20_1h > ema50_1h
        )

        trend1h_short = (
            price < ema20_1h
            and ema20_1h < ema50_1h
        )

        breakout_long = price > resistance
        breakout_short = price < support

        ema20_4h, ema50_4h = get_4h_context(
            c4h,
            current["time"]
        )

        if ema20_4h is None:
            continue

        trend4h_long = ema20_4h > ema50_4h
        trend4h_short = ema20_4h < ema50_4h

        direction = None
        score = 0

        # LONG
        if breakout_long and trend1h_long:

            score = 2  # trend 1H

            if trend4h_long:
                score += 1

            if vol_ratio >= 1.5:
                score += 1

            score += 3  # breakout

            if score >= 6 and vol_ratio >= 1.5:
                direction = "LONG"

        # SHORT
        elif breakout_short and trend1h_short:

            score = 2  # trend 1H

            if trend4h_short:
                score += 1

            if vol_ratio >= 1.5:
                score += 1

            score += 3  # breakout

            if score >= 6 and vol_ratio >= 1.5:
                direction = "SHORT"

        if direction is None:
            continue

        # Livelli coerenti con la logica ATR del bot.
        if direction == "LONG":
            entry = price
            sl = price - current_atr * 1.2

            risk = entry - sl

            tp1 = entry + risk
            tp2 = entry + risk * 2
            tp3 = entry + risk * 3

        else:
            entry = price
            sl = price + current_atr * 1.2

            risk = sl - entry

            tp1 = entry - risk
            tp2 = entry - risk * 2
            tp3 = entry - risk * 3

        future = c1h[
            i + 1:
            i + 1 + LOOK_FORWARD
        ]

        hit1, hit2, hit3, hit_sl = evaluate_trade(
            future,
            direction,
            entry,
            sl,
            tp1,
            tp2,
            tp3
        )

        stats["signals"] += 1

        if hit1:
            stats["tp1"] += 1

        if hit2:
            stats["tp2"] += 1

        if hit3:
            stats["tp3"] += 1

        if hit_sl:
            stats["sl"] += 1

    return stats


def percentage(value, total):
    if total == 0:
        return 0.0

    return value / total * 100


total = {
    "signals": 0,
    "tp1": 0,
    "tp2": 0,
    "tp3": 0,
    "sl": 0
}


print("================================")
print(" CryptoSignalAI12 BACKTEST")
print(" Segnali CONFERMATI")
print("================================")


for symbol in SYMBOLS:
    try:
        stats = backtest_symbol(symbol)

        print(
            symbol,
            "| segnali:", stats["signals"],
            "| TP1:", f"{percentage(stats['tp1'], stats['signals']):.1f}%",
            "| TP2:", f"{percentage(stats['tp2'], stats['signals']):.1f}%",
            "| TP3:", f"{percentage(stats['tp3'], stats['signals']):.1f}%"
        )

        for key in total:
            total[key] += stats[key]

    except Exception as e:
        print(symbol, "ERRORE:", e)


print("\n================================")
print(" RISULTATO COMPLESSIVO")
print("================================")

print("Segnali analizzati:", total["signals"])

print(
    "TP1 raggiunto:",
    f"{percentage(total['tp1'], total['signals']):.2f}%"
)

print(
    "TP2 raggiunto:",
    f"{percentage(total['tp2'], total['signals']):.2f}%"
)

print(
    "TP3 raggiunto:",
    f"{percentage(total['tp3'], total['signals']):.2f}%"
)

print(
    "Stop Loss:",
    f"{percentage(total['sl'], total['signals']):.2f}%"
)

print("================================")
