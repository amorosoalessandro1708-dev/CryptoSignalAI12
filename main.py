import time
import pytz
from datetime import datetime

# CONFIGURAZIONE ATTUALE 58% + FILTRO ORE
CONFIG = {
    "TIMEFRAME": "4H",
    "LOOKBACK_BARS": 10,
    "ATR_FILTER": 0.03,
    "RETEST_TOLLERANCE": 0.15,
    "VOLUME_MULTIPLIER": 1.35,
    "BODY_PERCENT": 0.75,
    "WICK_PERCENT": 0.15,
    "COOLDOWN_HOURS": 8,
    "TP1": 0.30,
    "TP2": 0.80,
    "TP3": 1.50,
    "COINS": 25,
    "TRADING_HOURS": "08:00-23:00 Europe/Rome"
}

def is_trading_hours():
    italy_tz = pytz.timezone('Europe/Rome')
    now = datetime.now(italy_tz)
    return 8 <= now.hour < 23

def check_signal(coin):
    if not is_trading_hours():
        return None  # Bloccato - ora morta
    # ... resto del tuo codice breakout 4H ...
    # ... ATR, volume, corpo, retest ...
    pass

# Loop
while True:
    if not is_trading_hours():
        print("Fuori fascia 08-23, in pausa...")
        time.sleep(600)
        continue
    
    # tuo scanner 25 coin qui
    time.sleep(60)
