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

TIMEFRAMES = ["15m", "1h", "4h"]

BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"


def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram non configurato")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": message},
            timeout=20
        )
        response.raise_for_status()
    except Exception as e:
        print("Errore Telegram:", e)


def get_klines(symbol, interval, limit=100):
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


def scan_market():
    results = []

    for symbol in SYMBOLS:
        symbol_ok = True
        prices = {}

        for timeframe in TIMEFRAMES:
            try:
                candles = get_klines(symbol, timeframe)

                if not candles:
                    symbol_ok = False
                    break

                prices[timeframe] = float(candles[-1][4])

            except Exception as e:
                print(symbol, timeframe, "ERRORE:", e)
                symbol_ok = False
                break

        if symbol_ok:
            results.append(
                f"{symbol}: "
                f"15m={prices['15m']} | "
                f"1H={prices['1h']} | "
                f"4H={prices['4h']}"
            )

    return results


print("CryptoSignalAI12 avviato")

send_telegram(
    "CryptoSignalAI12 ONLINE\n"
    "Test dati Binance Futures avviato sulle 12 coppie."
)

while True:
    try:
        results = scan_market()

        print(
            f"Scansione completata: "
            f"{len(results)}/{len(SYMBOLS)} coppie"
        )

        if len(results) == len(SYMBOLS):
            print("Tutte le 12 coppie ricevute correttamente.")

        for result in results:
            print(result)

    except Exception as e:
        print("Errore scansione:", e)

    time.sleep(60)
