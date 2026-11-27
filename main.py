from __future__ import annotations
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Optional
from zoneinfo import ZoneInfo
import requests
import websocket
ORIGINAL_50 = [
  "SOLUSDT", "BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT",
  "DOGEUSDT", "AVAXUSDT", "SUIUSDT", "LINKUSDT", "ADAUSDT",
  "NEARUSDT", "UNIUSDT", "LTCUSDT", "DOTUSDT", "APTUSDT",
  "ARBUSDT", "OPUSDT", "INJUSDT", "RENDERUSDT", "1000PEPEUSDT",
  "FILUSDT", "ETCUSDT", "AAVEUSDT", "TIAUSDT", "SEIUSDT",
  "HBARUSDT", "XLMUSDT", "TRXUSDT", "ONDOUSDT", "ENAUSDT",
  "WIFUSDT", "TAOUSDT", "1000SHIBUSDT", "1000BONKUSDT", "WLDUSDT",
  "FETUSDT", "ATOMUSDT", "BCHUSDT", "JUPUSDT", "PENDLEUSDT",
  "HYPEUSDT", "ZECUSDT", "XTZUSDT", "CRVUSDT", "STXUSDT",
  "ICPUSDT", "SANDUSDT", "POLUSDT", "ALGOUSDT", "LDOUSDT",
]
ADDED_10 = [
  "GALAUSDT", "RUNEUSDT", "JASMYUSDT", "ARUSDT", "APEUSDT",
  "CHZUSDT", "AXSUSDT", "EIGENUSDT", "PYTHUSDT", "WUSDT",
]
SYMBOLS = ORIGINAL_50 + ADDED_10
assert len(SYMBOLS) == 60 and len(set(SYMBOLS)) == 60
M15_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
       "LINKUSDT", "AVAXUSDT", "LTCUSDT", "ADAUSDT", "DOGEUSDT"}
assert M15_SYMBOLS <= set(SYMBOLS)
BOT_NAME = "MOMENTUM 4H 60"
BOT_VERSION = "1.11"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FOUR_HOUR_LOOKBACK = 6
FOUR_HOUR_BODY_MIN = 0.55
FOUR_HOUR_EACH_WICK_MAX = 0.225
ONE_HOUR_LOOKBACK = 48
ONE_HOUR_BODY_MIN = FOUR_HOUR_BODY_MIN
ONE_HOUR_EACH_WICK_MAX = FOUR_HOUR_EACH_WICK_MAX
FIFTEEN_MIN_BODY_MIN = 0.85
FIFTEEN_MIN_EACH_WICK_MAX = 0.075
FIFTEEN_MIN_WICKS_SUM_MAX = 0.15
VOLUME_LOOKBACK = 20
DIRECT_VOLUME_MIN = 1.50
RETEST_TOUCH_VOLUME_MAX = 1.00
RETEST_BOUNCE_VOLUME_MIN = 1.00
SL_BUFFER_ATR4H = 1.00
SL_BUFFER_ATR1H = 1.00
ATR_PERIOD = 14
TAKE_PROFITS_PERCENT = (0.02, 0.04, 0.06)
TAKE_PROFITS_1H_PERCENT = (0.005, 0.01, 0.015)
BTC_STRUCTURE_HOURS = 6
BTC_GROUP_SIZE = 3
DIRECT_LIFETIME_HOURS = 1
RETEST_LIFETIME_HOURS = 8
RETEST_MAX_EXCURSION_ATR4H = 1.00
M15_BREAKOUT_BARS = 24
M15_VOLUME_LOOKBACK = 20
M15_RETEST_TOUCH_VOLUME_MAX = 1.00
M15_RETEST_BOUNCE_VOLUME_MIN = 1.00
M15_BOUNCE_BODY_MIN = 0.60
M15_BOUNCE_EACH_WICK_MAX = 0.20
M15_RETEST_LIFETIME_BARS = 16
M15_RETEST_MAX_EXCURSION_ATR = 1.00
M15_SETUP_STRATEGY_VERSION = "BREAKOUT24_RETEST16_WICK020"
M15_ATR_PERIOD = 14
M15_SL_BUFFER_ATR_MULTIPLIER = 1.00
M15_TAKE_PROFITS_PERCENT = (0.004, 0.007, 0.01)
M15_COOLDOWN_BARS = 8
REJECTION_LOOKBACK_4H = 6
REJECTION_LOOKBACK_1H = 24
REJECTION_WICK_BODY_4H = 2.0
REJECTION_WICK_BODY_1H = 3.0
REJECTION_OPPOSITE_WICK_MAX = 0.05
REJECTION_VOLUME_MIN = 1.20
REJECTION_CONFIRM_BARS_4H = 2
REJECTION_CONFIRM_BARS_1H = 1
REJECTION_MAX_CONFIRM_BARS = 4
REJECTION_ATR_PERIOD = 14
REJECTION_SL_ATR = 1.0
REJECTION_TAKE_PROFITS_PERCENT = (0.005, 0.01, 0.015)
REJECTION_MODE = "PRICE REJECTION"
HISTORY_LIMIT = 420
FIFTEEN_MIN_MS = 15 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
FOUR_HOURS_MS = 4 * ONE_HOUR_MS
DIRECT_LIFETIME_MS = DIRECT_LIFETIME_HOURS * ONE_HOUR_MS
RETEST_LIFETIME_MS = RETEST_LIFETIME_HOURS * ONE_HOUR_MS
EVENT_LOOP_POLL_SECONDS = 0.10
EVENT_DEBOUNCE_SECONDS = 2.0
EVENT_MAX_RETRY_SECONDS = 40.0
REST_MIN_GAP_SECONDS = 0.14
REST_FALLBACK_AFTER_CLOSE_SECONDS = 12.0
REST_FALLBACK_RETRY_SECONDS = 180.0
REST_HISTORY_LIMIT = 420
REST_TIMEOUT = 18
BASE_HTTP = "https://fapi.binance.com"
BASE_WS = "wss://fstream.binance.com/market/stream?streams="
STATE_PATH = Path(os.getenv("STATE_FILE", "/tmp/momentum_4h_60_state.json"))
ROME = ZoneInfo("Europe/Rome")
logging.basicConfig(
  level=os.getenv("LOG_LEVEL", "INFO").upper(),
  format="%(asctime)s %(levelname)s %(message)s",
  stream=sys.stdout,
)
log = logging.getLogger(BOT_NAME)
def now_ms() -> int:
  return int(time.time() * 1000)
def price_string(value: float) -> str:
  if value >= 1000:
    digits = 2
  elif value >= 1:
    digits = 4
  elif value >= 0.01:
    digits = 6
  else:
    digits = 8
  return f"{value:.{digits}f}"
def is_bull(c: dict) -> bool:
  return c["c"] > c["o"]
def is_bear(c: dict) -> bool:
  return c["c"] < c["o"]
def ratios(c: dict) -> Optional[tuple[float, float, float]]:
  span = c["h"] - c["l"]
  if span <= 0:
    return None
  body = abs(c["c"] - c["o"]) / span
  upper = max(0.0, c["h"] - max(c["o"], c["c"])) / span
  lower = max(0.0, min(c["o"], c["c"]) - c["l"]) / span
  return body, upper, lower
def candle_shape_ok(c: dict, direction: str, *, macro: bool) -> bool:
  if not ((direction == "LONG" and is_bull(c)) or (direction == "SHORT" and is_bear(c))):
    return False
  r = ratios(c)
  if r is None:
    return False
  body, upper, lower = r
  eps = 1e-8
  if macro:
    return (body + eps >= FOUR_HOUR_BODY_MIN
        and upper <= FOUR_HOUR_EACH_WICK_MAX + eps
        and lower <= FOUR_HOUR_EACH_WICK_MAX + eps)
  return (body + eps >= FIFTEEN_MIN_BODY_MIN
      and upper <= FIFTEEN_MIN_EACH_WICK_MAX + eps
      and lower <= FIFTEEN_MIN_EACH_WICK_MAX + eps
      and upper + lower <= FIFTEEN_MIN_WICKS_SUM_MAX + eps)
def average_true_range(candles: list[dict], count: int = ATR_PERIOD) -> Optional[float]:
  if len(candles) < count + 1:
    return None
  chosen = candles[-(count + 1):]
  tr = []
  for prev, current in zip(chosen[:-1], chosen[1:]):
    tr.append(max(current["h"] - current["l"],
           abs(current["h"] - prev["c"]),
           abs(current["l"] - prev["c"])))
  value = sum(tr) / len(tr)
  return value if value > 0 else None
def volume_ratio(history: list[dict], target_index: int) -> Optional[float]:
  start = target_index - VOLUME_LOOKBACK
  if start < 0:
    return None
  base = median(float(c["v"]) for c in history[start:target_index])
  if base <= 0:
    return None
  return float(history[target_index]["v"]) / base
def aggregate_closed(candles_15m: list[dict], period_ms: int) -> list[dict]:
  count = period_ms // FIFTEEN_MIN_MS
  groups: dict[int, list[dict]] = defaultdict(list)
  for c in candles_15m:
    groups[(c["t"] // period_ms) * period_ms].append(c)
  result = []
  for start, bars in sorted(groups.items()):
    bars.sort(key=lambda b: b["t"])
    if len(bars) != count or any(b["t"] != start + i * FIFTEEN_MIN_MS
                   for i, b in enumerate(bars)):
      continue
    result.append({
      "t": start, "o": bars[0]["o"],
      "h": max(b["h"] for b in bars),
      "l": min(b["l"] for b in bars),
      "c": bars[-1]["c"], "v": sum(b["v"] for b in bars),
    })
  return result
def btc_regime(btc_15m: list[dict], close_time: int) -> str:
  hourly = [b for b in aggregate_closed(btc_15m, ONE_HOUR_MS)
       if b["t"] + ONE_HOUR_MS <= close_time]
  if len(hourly) < BTC_STRUCTURE_HOURS:
    return "UNKNOWN"
  hourly = hourly[-BTC_STRUCTURE_HOURS:]
  if any(hourly[i + 1]["t"] != hourly[i]["t"] + ONE_HOUR_MS
     for i in range(len(hourly) - 1)):
    return "UNKNOWN"
  older, recent = hourly[:BTC_GROUP_SIZE], hourly[BTC_GROUP_SIZE:]
  old_hi, old_lo = max(b["h"] for b in older), min(b["l"] for b in older)
  new_hi, new_lo = max(b["h"] for b in recent), min(b["l"] for b in recent)
  all_hi, all_lo = max(b["h"] for b in hourly), min(b["l"] for b in hourly)
  if all_hi <= all_lo:
    return "UNKNOWN"
  last_close = recent[-1]["c"]
  mid = (all_hi + all_lo) / 2.0
  if new_hi > old_hi and new_lo > old_lo and last_close >= mid:
    return "DIREZIONALE_RIALZISTA"
  if new_hi < old_hi and new_lo < old_lo and last_close <= mid:
    return "DIREZIONALE_RIBASSISTA"
  if new_lo >= old_lo and last_close >= mid:
    return "LATERALE_RIALZISTA"
  if new_hi <= old_hi and last_close <= mid:
    return "LATERALE_RIBASSISTA"
  return "INCERTO"
def btc_compatible(bias: str, direction: str, symbol: str) -> bool:
  if symbol == "BTCUSDT":
    return True
  favorable = ({"DIREZIONALE_RIALZISTA", "LATERALE_RIALZISTA"} if direction == "LONG"
        else {"DIREZIONALE_RIBASSISTA", "LATERALE_RIBASSISTA"})
  return bias in favorable
def btc_regime_15m(btc_bars: list[dict], close_time: int) -> str:
  bars = [b for b in btc_bars if b["t"] + FIFTEEN_MIN_MS <= close_time]
  if len(bars) < BTC_STRUCTURE_HOURS:
    return "UNKNOWN"
  bars = bars[-BTC_STRUCTURE_HOURS:]
  if (bars[-1]["t"] + FIFTEEN_MIN_MS != close_time
      or any(bars[i + 1]["t"] != bars[i]["t"] + FIFTEEN_MIN_MS
         for i in range(len(bars) - 1))):
    return "UNKNOWN"
  older, recent = bars[:BTC_GROUP_SIZE], bars[BTC_GROUP_SIZE:]
  old_hi, old_lo = max(b["h"] for b in older), min(b["l"] for b in older)
  new_hi, new_lo = max(b["h"] for b in recent), min(b["l"] for b in recent)
  all_hi, all_lo = max(b["h"] for b in bars), min(b["l"] for b in bars)
  if all_hi <= all_lo:
    return "UNKNOWN"
  last_close = recent[-1]["c"]
  mid = (all_hi + all_lo) / 2.0
  if new_hi > old_hi and new_lo > old_lo and last_close >= mid:
    return "DIREZIONALE_RIALZISTA"
  if new_hi < old_hi and new_lo < old_lo and last_close <= mid:
    return "DIREZIONALE_RIBASSISTA"
  if new_lo >= old_lo and last_close >= mid:
    return "LATERALE_RIALZISTA"
  if new_hi <= old_hi and last_close <= mid:
    return "LATERALE_RIBASSISTA"
  return "INCERTO"
def atr_wilder_last(candles: list[dict], period: int) -> Optional[float]:
  if len(candles) < period:
    return None
  tr = [float(candles[0]["h"] - candles[0]["l"])]
  for before, current in zip(candles[:-1], candles[1:]):
    tr.append(max(current["h"] - current["l"],
           abs(current["h"] - before["c"]),
           abs(current["l"] - before["c"])))
  average = sum(tr[:period]) / period
  for item in tr[period:]:
    average = (average * (period - 1) + item) / period
  return average if average > 0 else None
@dataclass
class M15RetestSetup:
  symbol: str
  direction: str
  level: float
  breakout_close_ms: int
  breakout_atr15: float
  touched_at: int = 0
def detect_m15_breakout(symbol: str, candles: list[dict]) -> Optional[M15RetestSetup]:
  required = max(M15_BREAKOUT_BARS + 1, M15_VOLUME_LOOKBACK + 1,
         M15_ATR_PERIOD + 1)
  if symbol not in M15_SYMBOLS or len(candles) < required:
    return None
  bar = candles[-1]
  previous_bars = candles[-(M15_BREAKOUT_BARS + 1):-1]
  if (previous_bars[-1]["t"] + FIFTEEN_MIN_MS != bar["t"]
      or any(previous_bars[i + 1]["t"] != previous_bars[i]["t"] + FIFTEEN_MIN_MS
         for i in range(len(previous_bars) - 1))):
    return None
  top = max(c["h"] for c in previous_bars)
  bottom = min(c["l"] for c in previous_bars)
  if bar["c"] > top:
    direction, level = "LONG", top
  elif bar["c"] < bottom:
    direction, level = "SHORT", bottom
  else:
    return None
  atr = atr_wilder_last(candles, M15_ATR_PERIOD)
  if atr is None:
    return None
  max_distance = M15_RETEST_MAX_EXCURSION_ATR * atr
  if direction == "LONG" and bar["h"] > level + max_distance:
    return None
  if direction == "SHORT" and bar["l"] < level - max_distance:
    return None
  return M15RetestSetup(symbol, direction, level, bar["t"] + FIFTEEN_MIN_MS, atr)
def m15_volume_ratio(history: list[dict], target_index: int) -> Optional[float]:
  start = target_index - M15_VOLUME_LOOKBACK
  if start < 0:
    return None
  base = median(float(c["v"]) for c in history[start:target_index])
  if base <= 0:
    return None
  return float(history[target_index]["v"]) / base
def m15_bounce_shape_ok(bar: dict, direction: str) -> bool:
  if not ((direction == "LONG" and is_bull(bar)) or
      (direction == "SHORT" and is_bear(bar))):
    return False
  parts = ratios(bar)
  if parts is None:
    return False
  body, upper, lower = parts
  eps = 1e-8
  return (body + eps >= M15_BOUNCE_BODY_MIN
      and upper <= M15_BOUNCE_EACH_WICK_MAX + eps
      and lower <= M15_BOUNCE_EACH_WICK_MAX + eps)
def m15_excursion_exceeded(setup: M15RetestSetup, candles: list[dict]) -> bool:
  limit = setup.breakout_atr15 * M15_RETEST_MAX_EXCURSION_ATR
  for bar in candles:
    if bar["t"] < setup.breakout_close_ms:
      continue
    if setup.direction == "LONG" and bar["h"] > setup.level + limit:
      return True
    if setup.direction == "SHORT" and bar["l"] < setup.level - limit:
      return True
  return False
def evaluate_m15_retest(setup: M15RetestSetup, candles: list[dict],
            btc_bias: str) -> tuple[Optional[dict], bool]:
  if not candles:
    return None, False
  bar = candles[-1]
  close_time = bar["t"] + FIFTEEN_MIN_MS
  if close_time <= setup.breakout_close_ms:
    return None, False
  if close_time - setup.breakout_close_ms > M15_RETEST_LIFETIME_BARS * FIFTEEN_MIN_MS:
    return None, True
  if m15_excursion_exceeded(setup, candles):
    return None, True
  ratio = m15_volume_ratio(candles, len(candles) - 1)
  if ratio is None:
    return None, False
  direction, level = setup.direction, setup.level
  if not setup.touched_at:
    if touched_level(bar, level) and ratio < M15_RETEST_TOUCH_VOLUME_MAX:
      setup.touched_at = bar["t"]
    return None, False
  if bar["t"] <= setup.touched_at:
    return None, False
  if not beyond_level(bar["c"], direction, level):
    return None, False
  if not m15_bounce_shape_ok(bar, direction):
    return None, False
  if ratio <= M15_RETEST_BOUNCE_VOLUME_MIN:
    return None, False
  favorable_btc = ({"DIREZIONALE_RIALZISTA", "LATERALE_RIALZISTA"}
          if direction == "LONG" else
          {"DIREZIONALE_RIBASSISTA", "LATERALE_RIBASSISTA"})
  if btc_bias not in favorable_btc:
    return None, False
  atr = atr_wilder_last(candles, M15_ATR_PERIOD)
  if atr is None:
    return None, False
  entry = float(bar["c"])
  sign = 1 if direction == "LONG" else -1
  sl = level - sign * M15_SL_BUFFER_ATR_MULTIPLIER * atr
  targets = [entry * (1 + sign * p) for p in M15_TAKE_PROFITS_PERCENT]
  return {
    "symbol": setup.symbol, "direction": direction,
    "mode": "BREAKOUT24 + RETEST", "timeframe": "M15",
    "entry": entry, "sl": sl,
    "tp1": targets[0], "tp2": targets[1], "tp3": targets[2],
    "volume_ratio": ratio, "level": level,
    "confirm_time": close_time, "breakout_time": setup.breakout_close_ms,
    "atr15": atr, "btc": btc_bias,
  }, True
@dataclass
class RejectionSetup:
  symbol: str
  direction: str
  timeframe: str
  macro_open_ms: int
  macro_close_ms: int
  level: float
  rejection_price: float
  atr: float
  sl: float
  @property
  def key(self) -> str:
    return f"REJECTION:{self.timeframe}:{self.symbol}:{self.macro_close_ms}"
def detect_price_rejection(symbol: str, candles: list[dict], close_time: int,
             timeframe: str) -> Optional[RejectionSetup]:
  if timeframe == "4H":
    period, lookback, wick_multiple = (FOUR_HOURS_MS, REJECTION_LOOKBACK_4H,
                     REJECTION_WICK_BODY_4H)
  elif timeframe == "1H":
    period, lookback, wick_multiple = (ONE_HOUR_MS, REJECTION_LOOKBACK_1H,
                     REJECTION_WICK_BODY_1H)
  else:
    return None
  if symbol not in SYMBOLS or close_time % period:
    return None
  macro = [b for b in aggregate_closed(candles, period)
      if b["t"] + period <= close_time]
  required = max(lookback, REJECTION_ATR_PERIOD + 1, 20) + 1
  if len(macro) < required:
    return None
  macro = macro[-required:]
  if (macro[-1]["t"] + period != close_time
      or any(b["t"] != a["t"] + period for a, b in zip(macro, macro[1:]))):
    return None
  bar, prior = macro[-1], macro[:-1]
  base = median(float(b["v"]) for b in prior[-20:])
  if base <= 0 or float(bar["v"]) / base < REJECTION_VOLUME_MIN:
    return None
  parts = ratios(bar)
  if parts is None:
    return None
  body, upper, lower = parts
  if body <= 0:
    return None
  levels = prior[-lookback:]
  top = max(b["h"] for b in levels)
  bottom = min(b["l"] for b in levels)
  eps = 1e-8
  if (is_bull(bar) and upper + eps >= wick_multiple * body
      and lower <= REJECTION_OPPOSITE_WICK_MAX + eps and bar["h"] >= top):
    direction, level, extreme = "SHORT", top, float(bar["h"])
  elif (is_bear(bar) and lower + eps >= wick_multiple * body
     and upper <= REJECTION_OPPOSITE_WICK_MAX + eps and bar["l"] <= bottom):
    direction, level, extreme = "LONG", bottom, float(bar["l"])
  else:
    return None
  atr = average_true_range(prior, REJECTION_ATR_PERIOD)
  if atr is None:
    return None
  sl = extreme + REJECTION_SL_ATR * atr if direction == "SHORT" else extreme - REJECTION_SL_ATR * atr
  if sl <= 0:
    return None
  return RejectionSetup(symbol, direction, timeframe, bar["t"], close_time,
             level, extreme, atr, sl)
def rejection_same_episode(a: RejectionSetup, b: RejectionSetup) -> bool:
  window = REJECTION_MAX_CONFIRM_BARS * FIFTEEN_MIN_MS
  return (a.symbol == b.symbol and a.direction == b.direction
      and a.timeframe != b.timeframe
      and a.macro_open_ms < b.macro_close_ms + window
      and b.macro_open_ms < a.macro_close_ms + window)
def evaluate_price_rejection(setup: RejectionSetup, candles: list[dict],
              close_time: int) -> Optional[dict]:
  elapsed = close_time - setup.macro_close_ms
  if not (0 < elapsed <= REJECTION_MAX_CONFIRM_BARS * FIFTEEN_MIN_MS):
    return None
  count = REJECTION_CONFIRM_BARS_4H if setup.timeframe == "4H" else REJECTION_CONFIRM_BARS_1H
  recent = candles[-count:]
  if (len(recent) != count or recent[-1]["t"] + FIFTEEN_MIN_MS != close_time
      or recent[0]["t"] < setup.macro_close_ms
      or any(b["t"] != a["t"] + FIFTEEN_MIN_MS for a, b in zip(recent, recent[1:]))):
    return None
  directional = is_bull if setup.direction == "LONG" else is_bear
  if not all(directional(b) for b in recent):
    return None
  entry = float(recent[-1]["c"])
  sign = 1 if setup.direction == "LONG" else -1
  if entry <= 0 or sign * (entry - setup.sl) <= 0:
    return None
  targets = [entry * (1 + sign * p) for p in REJECTION_TAKE_PROFITS_PERCENT]
  return {
    "symbol": setup.symbol, "direction": setup.direction,
    "timeframe": setup.timeframe, "mode": REJECTION_MODE,
    "entry": entry, "sl": setup.sl,
    "tp1": targets[0], "tp2": targets[1], "tp3": targets[2],
    "level": setup.level, "rejection_price": setup.rejection_price,
    "atr": setup.atr, "confirm_bars": count,
    "confirm_time": close_time, "rejection_time": setup.macro_close_ms,
  }
@dataclass
class Breakout:
  symbol: str
  direction: str
  macro_open_ms: int
  macro_close_ms: int
  level: float
  atr4h: float
  sl: float
  touched_at: int = 0
  sent: bool = False
  direct_valid: bool = True
def detect_breakout(symbol: str, candles_15m: list[dict], close_time: int) -> Optional[Breakout]:
  macro = aggregate_closed(candles_15m, FOUR_HOURS_MS)
  if not macro or macro[-1]["t"] + FOUR_HOURS_MS != close_time:
    return None
  if len(macro) < max(FOUR_HOUR_LOOKBACK + 1, ATR_PERIOD + 2):
    return None
  last = macro[-1]
  prior = macro[:-1]
  atr = average_true_range(prior)
  if atr is None:
    return None
  level_bars = prior[-FOUR_HOUR_LOOKBACK:]
  top, bottom = max(x["h"] for x in level_bars), min(x["l"] for x in level_bars)
  if last["c"] > top:
    direction, level = "LONG", top
  elif last["c"] < bottom:
    direction, level = "SHORT", bottom
  else:
    return None
  if not candle_shape_ok(last, direction, macro=True):
    return None
  sl = level - SL_BUFFER_ATR4H * atr if direction == "LONG" else level + SL_BUFFER_ATR4H * atr
  return Breakout(symbol, direction, last["t"], close_time, level, atr, sl)
def detect_breakout_1h(symbol: str, candles_15m: list[dict], close_time: int) -> Optional[Breakout]:
  hourly = aggregate_closed(candles_15m, ONE_HOUR_MS)
  if not hourly or hourly[-1]["t"] + ONE_HOUR_MS != close_time:
    return None
  if len(hourly) < max(ONE_HOUR_LOOKBACK + 1, ATR_PERIOD + 2):
    return None
  last, prior = hourly[-1], hourly[:-1]
  atr = average_true_range(prior)
  if atr is None:
    return None
  levels = prior[-ONE_HOUR_LOOKBACK:]
  high, low = max(c["h"] for c in levels), min(c["l"] for c in levels)
  if last["c"] > high:
    direction, level = "LONG", high
  elif last["c"] < low:
    direction, level = "SHORT", low
  else:
    return None
  if not candle_shape_ok(last, direction, macro=True):
    return None
  sl = (level - SL_BUFFER_ATR1H * atr if direction == "LONG"
     else level + SL_BUFFER_ATR1H * atr)
  return Breakout(symbol, direction, last["t"], close_time, level, atr, sl)
def evaluate_entry_1h(setup: Breakout, candles: list[dict], btc_bias: str,
           close_time: int) -> tuple[Optional[dict], bool]:
  out, expired = evaluate_entry(setup, candles, btc_bias, close_time)
  if out:
    sign = 1 if out["direction"] == "LONG" else -1
    entry = out["entry"]
    out.update(timeframe="1H",
         tp1=entry * (1 + sign * TAKE_PROFITS_1H_PERCENT[0]),
         tp2=entry * (1 + sign * TAKE_PROFITS_1H_PERCENT[1]),
         tp3=entry * (1 + sign * TAKE_PROFITS_1H_PERCENT[2]))
  return out, expired
def beyond_level(price: float, direction: str, level: float) -> bool:
  return price > level if direction == "LONG" else price < level
def touched_level(bar: dict, level: float) -> bool:
  return bar["l"] <= level <= bar["h"]
def invalidated(setup: Breakout, bar: dict) -> bool:
  return (bar["c"] <= setup.sl if setup.direction == "LONG"
      else bar["c"] >= setup.sl)
def signal_levels(entry: float, sl: float, direction: str) -> tuple[float, float, float]:
  sign = 1 if direction == "LONG" else -1
  if (direction == "LONG" and not (sl < entry)) or (direction == "SHORT" and not (sl > entry)):
    raise ValueError("Stop Loss non dalla parte corretta dell'Entry")
  return tuple(entry * (1 + sign * value) for value in TAKE_PROFITS_PERCENT)
def make_signal(setup: Breakout, bar: dict, mode: str, ratio: float, btc: str) -> dict:
  entry = bar["c"]
  targets = signal_levels(entry, setup.sl, setup.direction)
  return {
    "symbol": setup.symbol, "direction": setup.direction,
    "mode": mode, "entry": entry, "sl": setup.sl,
    "tp1": targets[0], "tp2": targets[1], "tp3": targets[2],
    "volume_ratio": ratio, "btc": btc,
    "level": setup.level, "confirm_time": bar["t"] + FIFTEEN_MIN_MS,
    "breakout_time": setup.macro_close_ms,
  }
def retest_excursion_exceeded(setup: Breakout, candles: list[dict], close_time: int) -> bool:
  limit = RETEST_MAX_EXCURSION_ATR4H * setup.atr4h
  return any(
    c["h"] > setup.level + limit if setup.direction == "LONG"
    else c["l"] < setup.level - limit
    for c in candles
    if setup.macro_close_ms <= c["t"] < close_time
  )
def evaluate_entry(setup: Breakout, candles: list[dict], btc_bias: str,
         close_time: int) -> tuple[Optional[dict], bool]:
  if setup.sent or not candles or close_time <= setup.macro_close_ms:
    return None, False
  elapsed = close_time - setup.macro_close_ms
  if elapsed > RETEST_LIFETIME_MS:
    return None, True
  bar = candles[-1]
  if bar["t"] + FIFTEEN_MIN_MS != close_time:
    return None, False
  if invalidated(setup, bar):
    return None, True
  direct_window = elapsed <= DIRECT_LIFETIME_MS
  retest_valid = not retest_excursion_exceeded(setup, candles, close_time)
  if (not retest_valid and (setup.touched_at or not direct_window)):
    return None, True
  ratio = volume_ratio(candles, len(candles) - 1)
  if ratio is None:
    return None, False
  direction, level = setup.direction, setup.level
  closed_beyond = beyond_level(bar["c"], direction, level)
  if not closed_beyond:
    setup.direct_valid = False
  if (direction == "LONG" and setup.sl >= bar["c"]) or (direction == "SHORT" and setup.sl <= bar["c"]):
    return None, False
  strong = closed_beyond and candle_shape_ok(bar, direction, macro=False)
  if setup.touched_at:
    if (close_time > setup.touched_at + FIFTEEN_MIN_MS and strong
        and ratio > RETEST_BOUNCE_VOLUME_MIN
        and btc_compatible(btc_bias, direction, setup.symbol)):
      return make_signal(setup, bar, "RETEST", ratio, btc_bias), False
    return None, False
  if retest_valid and touched_level(bar, level) and ratio < RETEST_TOUCH_VOLUME_MAX:
    setup.touched_at = bar["t"]
    return None, False
  if (direct_window and setup.direct_valid and strong and ratio >= DIRECT_VOLUME_MIN
      and btc_compatible(btc_bias, direction, setup.symbol)):
    return make_signal(setup, bar, "BREAKOUT DIRETTO", ratio, btc_bias), False
  return None, False
def load_state() -> dict:
  try:
    data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}
  except (OSError, ValueError):
    return {}
def save_state(data: dict) -> None:
  try:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, STATE_PATH)
  except OSError as exc:
    log.error("Impossibile salvare stato: %s", exc)
def telegram_send(message: str, *, tries: int = 3) -> bool:
  if not TOKEN or not CHAT_ID:
    log.warning("Telegram non configurato: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID mancanti")
    return False
  url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
  for i in range(tries):
    try:
      response = requests.post(url, data={"chat_id": CHAT_ID, "text": message}, timeout=15)
      if response.status_code == 429:
        try:
          pause = min(30, int(response.json().get("parameters", {}).get("retry_after", 2)))
        except Exception:
          pause = 3
        time.sleep(pause)
        continue
      response.raise_for_status()
      if response.json().get("ok"):
        return True
      log.error("Telegram rifiuta il messaggio: %s", response.text[:150])
    except (requests.RequestException, ValueError) as exc:
      log.warning("Invio Telegram fallito (%s/%s): %s", i + 1, tries, type(exc).__name__)
    if i + 1 < tries:
      time.sleep(2 * (i + 1))
  return False
def format_signal(s: dict) -> str:
  emoji = "🟢" if s["direction"] == "LONG" else "🔴"
  sym = s["symbol"].removesuffix("USDT") + "/USDT"
  when = datetime.fromtimestamp(s["confirm_time"] / 1000, ROME).strftime("%d/%m/%Y %H:%M")
  tf = s.get("timeframe", "4H")
  if s.get("mode") == REJECTION_MODE:
    return (
      f"{emoji} {s['direction']} | {sym} | {tf} | {REJECTION_MODE}\n\n"
      f"Entry: {price_string(s['entry'])}\n"
      f"Stop Loss: {price_string(s['sl'])} (estremo rifiuto +/- 1 ATR{tf})\n"
      f"TP1: {price_string(s['tp1'])} (+/-0.50%)\n"
      f"TP2: {price_string(s['tp2'])} (+/-1.00%)\n"
      f"TP3: {price_string(s['tp3'])} (+/-1.50%)\n\n"
      f"Livello 24h: {price_string(s['level'])}\n"
      f"Prezzo rifiuto: {price_string(s['rejection_price'])}\n"
      f"Conferma: {s['confirm_bars']} M15 direzionali\n"
      f"Orario: {when} (Roma)\n"
      "SOLO SEGNALE - nessun ordine automatico"
    )
  if tf == "M15":
    return (
      f"{emoji} {s['direction']} | {sym} | M15 | {s['mode']}\n\n"
      f"Entry: {price_string(s['entry'])}\n"
      f"Stop Loss: {price_string(s['sl'])} (livello rotto +/- 1 ATR15)\n"
      f"TP1: {price_string(s['tp1'])} (+/-0.40%)\n"
      f"TP2: {price_string(s['tp2'])} (+/-0.70%)\n"
      f"TP3: {price_string(s['tp3'])} (+/-1.00%)\n\n"
      f"Volume rimbalzo / mediana20: {s['volume_ratio']:.2f}x\n"
      f"Livello M15: {price_string(s['level'])}\n"
      f"BTC 15m: {s['btc'].replace('_', ' ').lower()}\n"
      f"Orario: {when} (Roma)\n"
      "SOLO SEGNALE - nessun ordine automatico"
    )
  labels = ("0.50", "1.00", "1.50") if tf == "1H" else ("2", "4", "6")
  return (
    f"{emoji} {s['direction']} | {sym} | {tf} | {s['mode']}\n\n"
    f"Entry: {price_string(s['entry'])}\n"
    f"Stop Loss: {price_string(s['sl'])}\n"
    f"TP1: {price_string(s['tp1'])} (+/-{labels[0]}%)\n"
    f"TP2: {price_string(s['tp2'])} (+/-{labels[1]}%)\n"
    f"TP3: {price_string(s['tp3'])} (+/-{labels[2]}%)\n\n"
    f"Volume 15m: {s['volume_ratio']:.2f}x\n"
    f"Livello {tf}: {price_string(s['level'])}\n"
    f"BTC 1H: {s['btc'].replace('_', ' ').lower()}\n"
    f"Orario: {when} (Roma)\n"
    "SOLO SEGNALE - nessun ordine automatico"
  )
class BinanceREST:
  def __init__(self) -> None:
    self.lock = threading.Lock()
    self.last = 0.0
  def get(self, path: str, params: Optional[dict] = None) -> dict | list:
    for attempt in range(4):
      with self.lock:
        pause = REST_MIN_GAP_SECONDS - (time.monotonic() - self.last)
        if pause > 0:
          time.sleep(pause)
        self.last = time.monotonic()
      try:
        resp = requests.get(BASE_HTTP + path, params=params, timeout=REST_TIMEOUT)
        if resp.status_code in (418, 429, 500, 502, 503, 504):
          log.warning("Binance REST status %s (%s)", resp.status_code, path)
          if resp.status_code == 418:
            raise RuntimeError("IP Binance bloccato temporaneamente (HTTP 418)")
          time.sleep(min(12, 2 ** (attempt + 1)))
          continue
        resp.raise_for_status()
        return resp.json()
      except requests.RequestException as exc:
        log.warning("REST %s fallito: %s", path, exc)
        if attempt < 3:
          time.sleep(min(12, 2 ** (attempt + 1)))
    raise RuntimeError(f"REST Binance non disponibile: {path}")
  def active_perpetuals(self) -> set[str]:
    result = self.get("/fapi/v1/exchangeInfo")
    return {
      s["symbol"] for s in result["symbols"]
      if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
      and s.get("contractType") == "PERPETUAL"
    }
  def recent_bars(self, symbol: str, limit: int) -> list[dict]:
    rows = self.get("/fapi/v1/klines", {"symbol": symbol, "interval": "15m", "limit": limit})
    current_ms = now_ms()
    return [{
      "t": int(c[0]), "o": float(c[1]), "h": float(c[2]),
      "l": float(c[3]), "c": float(c[4]), "v": float(c[5]),
    } for c in rows if int(c[0]) + FIFTEEN_MIN_MS <= current_ms]
class SignalEngine:
  def __init__(self) -> None:
    self.rest = BinanceREST()
    self.running = threading.Event()
    self.running.set()
    self.lock = threading.RLock()
    self.active_symbols: list[str] = []
    self.bars: dict[str, dict[int, dict]] = {}
    self.last_processed: dict[str, int] = {}
    self.last_macro_checked: dict[str, int] = {}
    self.last_hour_checked: dict[str, int] = {}
    self.checked_1h: dict[str, int] = {}
    self.checked_15m: dict[str, int] = {}
    self.checked_4h: dict[str, int] = {}
    self.scan_errors = 0
    self.setups: dict[str, Breakout] = {}
    self.setups_1h: dict[str, Breakout] = {}
    self.pending: dict[str, tuple[int, float, float]] = {}
    self.events: queue.Queue[tuple[str, dict]] = queue.Queue(maxsize=4096)
    self.state = load_state()
    self.rejection_setups: dict[str, RejectionSetup] = {}
    self.rejection_sent: dict[str, RejectionSetup] = {}
    for field, destination in (("rejection_setups", self.rejection_setups),
                 ("rejection_sent", self.rejection_sent)):
      raw = self.state.get(field, {})
      if not isinstance(raw, dict):
        continue
      for key, value in raw.items():
        try:
          setup = RejectionSetup(**value)
          if (setup.key == key and setup.symbol in SYMBOLS
              and setup.timeframe in {"4H", "1H"}
              and setup.direction in {"LONG", "SHORT"}):
            destination[key] = setup
        except (TypeError, ValueError):
          log.warning("Stato PRICE REJECTION non valido: %s", key)
    self.m15_setups: dict[str, M15RetestSetup] = {}
    saved_m15_setups = self.state.get("m15_setups", {})
    if self.state.get("m15_setup_strategy_version") != M15_SETUP_STRATEGY_VERSION:
      if saved_m15_setups:
        log.info("Nuova strategia M15: annullati %d setup della configurazione precedente",
            len(saved_m15_setups))
      saved_m15_setups = {}
    for sym, raw_setup in saved_m15_setups.items():
      try:
        if sym in M15_SYMBOLS and isinstance(raw_setup, dict):
          setup = M15RetestSetup(**raw_setup)
          if setup.symbol == sym:
            self.m15_setups[sym] = setup
      except (TypeError, ValueError):
        log.warning("Setup M15 salvato non valido per %s: ignorato", sym)
    self.sent = set(self.state.get("sent", []))
    self.sent_slots: set[str] = set(self.state.get("sent_slots", []))
    self.m15_last_signal_bar: dict[str, int] = {
      str(k): int(v) for k, v in self.state.get("m15_last_signal_bar", {}).items()
    }
    self.outbox: dict[str, dict] = dict(self.state.get("outbox", {}))
    rank = {"4H": 0, "1H": 1, "M15": 2}
    selected: dict[str, tuple[str, dict]] = {}
    for old_key, payload in self.outbox.items():
      if not isinstance(payload, dict):
        continue
      tf = payload.get("timeframe", "4H")
      if tf not in rank or not {"symbol", "confirm_time"}.issubset(payload):
        continue
      if tf == "M15" and payload.get("mode") != "BREAKOUT24 + RETEST":
        continue
      slot = f"{payload['confirm_time']}:{payload['symbol']}"
      prior = selected.get(slot)
      if prior is None or rank[tf] < rank[prior[1].get("timeframe", "4H")]:
        selected[slot] = (old_key, payload)
    cleaned = {key: data for key, data in selected.values()}
    if len(cleaned) < len(self.outbox):
      log.info("Coda ripulita: %d segnali duplicati/1H-4C/M15 non ammessi rimossi",
          len(self.outbox) - len(cleaned))
    self.outbox = cleaned
    self.sent_slots.update(selected)
    self.ws_last_message = 0.0
    self.ws = None
    self.last_recovery = defaultdict(float)
  def persist(self) -> None:
    recent = sorted(self.sent)[-5000:]
    self.state["sent"] = recent
    self.state["sent_slots"] = sorted(self.sent_slots)[-5000:]
    self.state["m15_last_signal_bar"] = self.m15_last_signal_bar
    self.state["m15_setup_strategy_version"] = M15_SETUP_STRATEGY_VERSION
    self.state["m15_setups"] = {
      sym: {
        "symbol": setup.symbol, "direction": setup.direction,
        "level": setup.level, "breakout_close_ms": setup.breakout_close_ms,
        "breakout_atr15": setup.breakout_atr15, "touched_at": setup.touched_at,
      } for sym, setup in self.m15_setups.items()
    }
    self.state["rejection_setups"] = {k: asdict(v) for k, v in self.rejection_setups.items()}
    self.state["rejection_sent"] = {k: asdict(v) for k, v in self.rejection_sent.items()}
    self.state["outbox"] = self.outbox
    save_state(self.state)
  def enqueue_unique(self, key: str, payload: dict) -> bool:
    slot = f"{payload['confirm_time']}:{payload['symbol']}"
    if slot in self.sent_slots:
      log.info("DUPLICATO SOPPRESSO %s %s: esiste gia' segnale alle %s",
          payload["symbol"], payload.get("timeframe", "4H"), payload["confirm_time"])
      return False
    if key in self.sent:
      return False
    self.sent.add(key)
    self.sent_slots.add(slot)
    self.outbox[key] = payload
    self.persist()
    return True
  def initialize(self) -> None:
    tradable = self.rest.active_perpetuals()
    self.active_symbols = [s for s in SYMBOLS if s in tradable]
    missing = [s for s in SYMBOLS if s not in tradable]
    if missing:
      log.warning("NON disponibili su Binance futures USDT: %s", ", ".join(missing))
    if "BTCUSDT" not in self.active_symbols:
      raise RuntimeError("BTCUSDT non disponibile: impossibile applicare filtro BTC1H")
    log.info("Avvio raccolta dati: %s/%s coppie disponibili", len(self.active_symbols), len(SYMBOLS))
    for index, sym in enumerate(self.active_symbols, 1):
      try:
        loaded = self.rest.recent_bars(sym, REST_HISTORY_LIMIT)
        if len(loaded) < 265:
          log.warning("Dati insufficienti per %s: %s barre", sym, len(loaded))
        with self.lock:
          self.bars[sym] = {bar["t"]: bar for bar in loaded[-HISTORY_LIMIT:]}
          if loaded:
            self.last_processed[sym] = loaded[-1]["t"]
            four = aggregate_closed(loaded, FOUR_HOURS_MS)
            if four:
              self.last_macro_checked[sym] = four[-1]["t"] + FOUR_HOURS_MS
            hourly = aggregate_closed(loaded, ONE_HOUR_MS)
            if hourly:
              self.last_hour_checked[sym] = hourly[-1]["t"] + ONE_HOUR_MS
      except Exception as exc:
        log.error("Inizializzazione %s fallita: %s", sym, exc)
        with self.lock:
          self.bars[sym] = {}
      if index % 10 == 0:
        log.info("Storico pronto per %d/%d coppie", index, len(self.active_symbols))
    status = (
      f"🟢 {BOT_NAME} V{BOT_VERSION} AVVIATO [SIMULAZIONE]\n"
      f"{len(self.active_symbols)}/{len(SYMBOLS)} crypto USDT perpetual | 4H: rottura su 6 candele (24h)\n"
      "15m DIRETTO volume >=1.50x | RETEST tocco <1.00x + ripartenza >1.00x\n"
      "DIRETTO max 4 candele 15m (1h) | RETEST max 32 candele 15m (8h)\n"
      "RETEST annullato se il prezzo avanza oltre 1.00 ATR4H dal livello rotto\n"
      "BTC1H favorevole/laterale | SL livello 4H +/-1.00 ATR4H\n"
      "4H TP fissi 2% / 4% / 6% dall'Entry | nessun ordine automatico\n"
      "1H: breakout 48 candele 1H (48h), conferma 15m | SL livello +/-1.00 ATR1H\n"
      "1H TP dall'Entry: 0.50% / 1.00% / 1.50% | DIRETTO 1h, RETEST 8h\n"
      "M15 10 crypto (BTC/ETH/SOL/BNB/XRP/LINK/AVAX/LTC/ADA/DOGE): breakout delle 24 M15; SOLO RETEST entro 4h\n"
      "M15 RETEST: tocco <1.00x, rimbalzo >1.00x mediana20; corpo >=60%, wick <=20% per lato\n"
      "M15 BTC15m concorde, escursione max 1 ATR15, SL livello rotto +/- 1 ATR15\n"
      "M15 TP 0.40% / 0.70% / 1.00%, cooldown 8 barre per direzione\n"
      "PRICE REJECTION 4H: wick rifiuto >=2x corpo; opposta <=5% range; livello 24h (6 barre)\n"
      "PRICE REJECTION 1H: wick rifiuto >=3x corpo; opposta <=5% range; livello 24h (24 barre)\n"
      "REJECTION SHORT macro verde / LONG macro rossa; conferma M15 4H=2 consecutive, 1H=1; max 4 M15\n"
      "REJECTION: solo colore direzionale M15; no filtri volume/corpo/wick/BTC o breakout M15\n"
      "REJECTION SL estremo wick +/-1 ATR14 macro; TP 0.50% / 1.00% / 1.50% dall'Entry\n"
      "REJECTION ANTIDUPLICATI: 4H prioritario su 1H dello stesso episodio; no ripetizione se 1H gia' accodato\n"
      "ANTIDUPLICATI: 4H > 1H > M15, una notifica per coppia e chiusura 15m"
    )
    log.info("%s", status)
  def recent(self, symbol: str) -> list[dict]:
    return [v for _, v in sorted(self.bars.get(symbol, {}).items())]
  def merge(self, symbol: str, additions: list[dict]) -> None:
    bucket = self.bars.setdefault(symbol, {})
    for c in additions:
      bucket[c["t"]] = c
    if len(bucket) > HISTORY_LIMIT:
      for old in sorted(bucket)[:-HISTORY_LIMIT]:
        bucket.pop(old, None)
  def websocket_loop(self) -> None:
    retry = 1
    while self.running.is_set():
      streams = "/".join(s.lower() + "@kline_15m" for s in self.active_symbols)
      url = BASE_WS + streams
      def on_open(_ws):
        nonlocal retry
        retry = 1
        self.ws_last_message = time.monotonic()
        log.info("WebSocket Binance connesso: %s stream 15m", len(self.active_symbols))
      def on_message(_ws, raw):
        self.ws_last_message = time.monotonic()
        try:
          event = json.loads(raw)
          data = event.get("data", event)
          if data.get("e") != "kline":
            return
          k = data["k"]
          if k.get("i") != "15m" or not k.get("x"):
            return
          sym = data.get("s", k.get("s", ""))
          if sym not in self.active_symbols:
            return
          bar = {
            "t": int(k["t"]), "o": float(k["o"]),
            "h": float(k["h"]), "l": float(k["l"]),
            "c": float(k["c"]), "v": float(k["v"]),
          }
          try:
            self.events.put_nowait((sym, bar))
          except queue.Full:
            log.error("Coda eventi piena: %s, necessaria ottimizzazione", sym)
        except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
          log.warning("Evento WS non valido: %s", exc)
      def on_error(_ws, error):
        log.warning("WebSocket errore: %s", error)
      def on_close(_ws, code, message):
        log.warning("WebSocket chiuso: %s %s", code, message)
      try:
        self.ws = websocket.WebSocketApp(
          url, on_open=on_open, on_message=on_message,
          on_error=on_error, on_close=on_close,
        )
        self.ws.run_forever(ping_interval=180, ping_timeout=30)
      except Exception as exc:
        log.warning("WebSocket riavvio: %s", exc)
      if self.running.is_set():
        time.sleep(retry)
        retry = min(retry * 2, 30)
  def fallback_loop(self) -> None:
    while self.running.is_set():
      t = now_ms()
      current_bar_t = (t // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
      previous_start = current_bar_t - FIFTEEN_MIN_MS
      if t - current_bar_t >= REST_FALLBACK_AFTER_CLOSE_SECONDS * 1000:
        for sym in self.active_symbols:
          if not self.running.is_set():
            break
          with self.lock:
            has_closed = previous_start in self.bars.get(sym, {})
          if has_closed:
            continue
          if time.monotonic() - self.last_recovery[sym] < REST_FALLBACK_RETRY_SECONDS:
            continue
          self.last_recovery[sym] = time.monotonic()
          try:
            recent = self.rest.recent_bars(sym, 8)
            if not recent:
              continue
            with self.lock:
              prior = set(self.bars.get(sym, {}))
              self.merge(sym, recent)
            for bar in recent:
              if bar["t"] not in prior and bar["t"] >= previous_start - FIFTEEN_MIN_MS:
                self.schedule(sym, bar["t"])
          except Exception as exc:
            log.warning("Fallback REST %s: %s", sym, exc)
      time.sleep(4)
  def schedule(self, symbol: str, bar_t: int) -> None:
    with self.lock:
      if bar_t <= self.last_processed.get(symbol, -1):
        return
      current = self.pending.get(symbol)
      if current is None or bar_t > current[0]:
        self.pending[symbol] = (
          bar_t, time.monotonic() + EVENT_DEBOUNCE_SECONDS,
          time.monotonic() + EVENT_MAX_RETRY_SECONDS,
        )
  def apply_new_close(self, sym: str, bar: dict) -> None:
    with self.lock:
      self.merge(sym, [bar])
    self.schedule(sym, bar["t"])
  def process(self, symbol: str, bar_t: int) -> bool:
    close_t = bar_t + FIFTEEN_MIN_MS
    with self.lock:
      history = self.recent(symbol)
      btc = self.recent("BTCUSDT")
      if not history or history[-1]["t"] != bar_t:
        return True
      target_btc_hour_end = (close_t // ONE_HOUR_MS) * ONE_HOUR_MS
      btc_hourly = aggregate_closed(btc, ONE_HOUR_MS)
      if (symbol != "BTCUSDT" and
          (not btc_hourly or btc_hourly[-1]["t"] + ONE_HOUR_MS < target_btc_hour_end)):
        return False
      bias = btc_regime(btc, close_t)
      self.checked_15m[symbol] = bar_t
      setup = self.setups.get(symbol)
      if setup and close_t - setup.macro_close_ms > RETEST_LIFETIME_MS:
        self.setups.pop(symbol, None)
        setup = None
      if close_t % FOUR_HOURS_MS == 0 and self.last_macro_checked.get(symbol) != close_t:
        self.last_macro_checked[symbol] = close_t
        candidate = detect_breakout(symbol, history, close_t)
        self.checked_4h[symbol] = close_t
        if candidate and not setup:
          self.setups[symbol] = candidate
          setup = candidate
          log.info("BREAKOUT 4H %s %s, livello %s", symbol,
              candidate.direction, price_string(candidate.level))
      if not setup:
        return True
      dedup_key = f"{symbol}:{setup.macro_close_ms}"
      if dedup_key in self.sent:
        self.setups.pop(symbol, None)
        return True
      out, expired = evaluate_entry(setup, history, bias, close_t)
      if expired:
        self.setups.pop(symbol, None)
        return True
      if out:
        accepted = self.enqueue_unique(dedup_key, out)
        self.setups.pop(symbol, None)
        if accepted:
          log.info("SEGNALE IN CODA %s %s %s: volume %.2fx BTC %s",
              symbol, out["direction"], out["mode"], out["volume_ratio"], out["btc"])
      return True
  def process_1h(self, symbol: str, bar_t: int) -> None:
    close_t = bar_t + FIFTEEN_MIN_MS
    with self.lock:
      history = self.recent(symbol)
      if not history or history[-1]["t"] != bar_t:
        return
      setup = self.setups_1h.get(symbol)
      if setup and close_t - setup.macro_close_ms > RETEST_LIFETIME_MS:
        self.setups_1h.pop(symbol, None)
        setup = None
      if close_t % ONE_HOUR_MS == 0 and self.last_hour_checked.get(symbol) != close_t:
        self.last_hour_checked[symbol] = close_t
        candidate = detect_breakout_1h(symbol, history, close_t)
        self.checked_1h[symbol] = close_t
        if candidate and not setup:
          self.setups_1h[symbol] = candidate
          setup = candidate
          log.info("BREAKOUT 1H %s %s livello %s (48h)", symbol,
              candidate.direction, price_string(candidate.level))
      if not setup:
        return
      key = f"1H:{symbol}:{setup.macro_close_ms}"
      if key in self.sent:
        self.setups_1h.pop(symbol, None)
        return
      btc = self.recent("BTCUSDT")
      bias = btc_regime(btc, close_t)
      out, expired = evaluate_entry_1h(setup, history, bias, close_t)
      if expired:
        self.setups_1h.pop(symbol, None)
        return
      if out:
        accepted = self.enqueue_unique(key, out)
        self.setups_1h.pop(symbol, None)
        if accepted:
          log.info("SEGNALE 1H IN CODA %s %s %s volume %.2fx BTC %s",
              symbol, out["direction"], out["mode"], out["volume_ratio"], out["btc"])
  def process_rejection(self, symbol: str, bar_t: int, timeframe: str) -> None:
    close_t = bar_t + FIFTEEN_MIN_MS
    window = REJECTION_MAX_CONFIRM_BARS * FIFTEEN_MIN_MS
    period = FOUR_HOURS_MS if timeframe == "4H" else ONE_HOUR_MS
    with self.lock:
      history = self.recent(symbol)
      if not history or history[-1]["t"] != bar_t:
        return
      changed = False
      if timeframe == "4H" and close_t % ONE_HOUR_MS == 0:
        for key, old in list(self.rejection_sent.items()):
          if close_t - old.macro_close_ms > 48 * ONE_HOUR_MS:
            self.rejection_sent.pop(key, None)
            changed = True
      for key, old in list(self.rejection_setups.items()):
        if old.symbol == symbol and old.timeframe == timeframe and close_t > old.macro_close_ms + window:
          self.rejection_setups.pop(key, None)
          changed = True
          log.info("PRICE REJECTION %s SCADUTO %s", timeframe, symbol)
      if close_t % period == 0:
        candidate = detect_price_rejection(symbol, history, close_t, timeframe)
        if (candidate and candidate.key not in self.rejection_setups
            and candidate.key not in self.sent
            and not any(rejection_same_episode(candidate, sent)
                  for sent in self.rejection_sent.values())):
          self.rejection_setups[candidate.key] = candidate
          changed = True
          log.info("PRICE REJECTION SETUP %s %s %s livello24h %s estremo %s",
              timeframe, symbol, candidate.direction, price_string(candidate.level),
              price_string(candidate.rejection_price))
      for key, setup in list(self.rejection_setups.items()):
        if setup.symbol != symbol or setup.timeframe != timeframe:
          continue
        if key in self.sent or any(rejection_same_episode(setup, sent)
                     for sent in self.rejection_sent.values()):
          self.rejection_setups.pop(key, None)
          changed = True
          log.info("PRICE REJECTION DUPLICATO SOPPRESSO %s %s", timeframe, symbol)
          continue
        if close_t <= setup.macro_close_ms:
          continue
        blocked = timeframe == "1H" and any(
          other.timeframe == "4H" and rejection_same_episode(setup, other)
          and other.macro_close_ms <= close_t <= other.macro_close_ms + window
          for other in self.rejection_setups.values())
        out = None if blocked else evaluate_price_rejection(setup, history, close_t)
        if out is not None:
          self.rejection_setups.pop(key, None)
          changed = True
          self.rejection_sent[key] = setup
          if self.enqueue_unique(key, out):
            log.info("SEGNALE PRICE REJECTION %s IN CODA %s %s conferma %d M15",
                timeframe, symbol, setup.direction, out["confirm_bars"])
          else:
            self.rejection_sent.pop(key, None)
        elif close_t == setup.macro_close_ms + window:
          self.rejection_setups.pop(key, None)
          changed = True
          log.info("PRICE REJECTION %s SCADUTO %s%s", timeframe, symbol,
              " (priorita' 4H)" if blocked else "")
      if changed:
        self.persist()
  def process_m15(self, symbol: str, bar_t: int) -> bool:
    if symbol not in M15_SYMBOLS:
      return True
    close_t = bar_t + FIFTEEN_MIN_MS
    with self.lock:
      history = self.recent(symbol)
      if not history or history[-1]["t"] != bar_t:
        return True
      setup = self.m15_setups.get(symbol)
      if setup and close_t - setup.breakout_close_ms > M15_RETEST_LIFETIME_BARS * FIFTEEN_MIN_MS:
        self.m15_setups.pop(symbol, None)
        self.persist()
        log.info("M15 RETEST SCADUTO %s", symbol)
        setup = None
      if setup is None:
        candidate = detect_m15_breakout(symbol, history)
        if candidate:
          self.m15_setups[symbol] = candidate
          setup = candidate
          self.persist()
          log.info("M15 BREAKOUT %s %s livello %s | SOLO RETEST",
              symbol, candidate.direction, price_string(candidate.level))
      btc_bars = self.recent("BTCUSDT")
      if not btc_bars or btc_bars[-1]["t"] != bar_t:
        return False
      bias = btc_regime_15m(btc_bars, close_t)
      if bias == "UNKNOWN":
        return False
      if setup is not None and close_t > setup.breakout_close_ms:
        previous_touch = setup.touched_at
        out, expired = evaluate_m15_retest(setup, history, bias)
        if expired:
          self.m15_setups.pop(symbol, None)
          self.persist()
          setup = None
          if out is None:
            log.info("M15 RETEST ANNULLATO %s (scadenza/escursione)", symbol)
        elif setup.touched_at != previous_touch:
          self.persist()
          log.info("M15 RETEST TOCCO %s %s livello %s",
              symbol, setup.direction, price_string(setup.level))
        if out is not None:
          self.m15_setups.pop(symbol, None)
          self.persist()
          cooldown_key = f"{symbol}:{out['direction']}"
          prior_bar = self.m15_last_signal_bar.get(cooldown_key)
          if prior_bar is not None and (bar_t - prior_bar) < M15_COOLDOWN_BARS * FIFTEEN_MIN_MS:
            return True
          key = f"M15:{symbol}:{close_t}"
          if self.enqueue_unique(key, out):
            self.m15_last_signal_bar[cooldown_key] = bar_t
            self.persist()
            log.info("SEGNALE M15 IN CODA %s %s RETEST volume %.2fx BTC15 %s",
                symbol, out["direction"], out["volume_ratio"], bias)
          return True
      return True
  def telegram_loop(self) -> None:
    while self.running.is_set():
      with self.lock:
        item = next(iter(self.outbox.items()), None)
      if item is None:
        time.sleep(0.4)
        continue
      key, payload = item
      if telegram_send(format_signal(payload), tries=2):
        with self.lock:
          self.outbox.pop(key, None)
          self.persist()
        log.info("TELEGRAM CONSEGNATO: %s", key)
      else:
        log.error("Telegram in attesa di nuovo invio: %s", key)
        time.sleep(12)
  def event_loop(self) -> None:
    last_health = time.monotonic()
    while self.running.is_set():
      try:
        symbol, bar = self.events.get(timeout=EVENT_LOOP_POLL_SECONDS)
        self.apply_new_close(symbol, bar)
      except queue.Empty:
        pass
      with self.lock:
        ready = [(sym, item) for sym, item in self.pending.items()
            if time.monotonic() >= item[1]]
      for sym, item in ready:
        bar_t, due, deadline = item
        if bar_t <= self.last_processed.get(sym, -1):
          with self.lock:
            self.pending.pop(sym, None)
          continue
        try:
          finished = self.process(sym, bar_t)
          if finished:
            self.process_rejection(sym, bar_t, "4H")
            self.process_1h(sym, bar_t)
            self.process_rejection(sym, bar_t, "1H")
            if not self.process_m15(sym, bar_t):
              finished = False
          elif time.monotonic() >= deadline:
            self.process_rejection(sym, bar_t, "4H")
            self.process_rejection(sym, bar_t, "1H")
            self.process_m15(sym, bar_t)
        except Exception:
          self.scan_errors += 1
          self.checked_15m.pop(sym, None)
          log.exception("Errore scansione %s", sym)
          finished = False
        if not finished and time.monotonic() < deadline:
          with self.lock:
            self.pending[sym] = (bar_t, time.monotonic() + 2.0, deadline)
          continue
        with self.lock:
          self.last_processed[sym] = bar_t
          self.pending.pop(sym, None)
      if time.monotonic() - last_health >= 20:
        last_health = time.monotonic()
        bar_t = (now_ms() // FIFTEEN_MIN_MS - 1) * FIFTEEN_MIN_MS
        hour4_end = (now_ms() // FOUR_HOURS_MS) * FOUR_HOURS_MS
        with self.lock:
          total = len(self.active_symbols)
          feed = sum(bar_t in self.bars.get(s, {}) for s in self.active_symbols)
          checked = sum(self.checked_15m.get(s) == bar_t for s in self.active_symbols)
          checked_4h = sum(self.checked_4h.get(s) == hour4_end for s in self.active_symbols)
          log.info("SCAN 15m | feed %d/%d | analizzate %d/%d | 4H ultima chiusura %d/%d | errori %d | pending %d | setup 4H %d | WS %.1fs fa",
              feed, total, checked, total, checked_4h, total,
              self.scan_errors, len(self.pending), len(self.setups),
              time.monotonic() - self.ws_last_message if self.ws_last_message else -1)
          hour_end = (now_ms() // ONE_HOUR_MS) * ONE_HOUR_MS
          checked_1h = sum(self.checked_1h.get(s) == hour_end for s in self.active_symbols)
          log.info("SCAN 1H | controllate %d/%d | setup 1H %d | M15 10 crypto, setup retest %d",
              checked_1h, total, len(self.setups_1h), len(self.m15_setups))
          log.info("SCAN PRICE REJECTION | setup 4H %d | setup 1H %d",
              sum(s.timeframe == "4H" for s in self.rejection_setups.values()),
              sum(s.timeframe == "1H" for s in self.rejection_setups.values()))
  def stop(self, *_args) -> None:
    log.info("Interruzione richiesta")
    self.running.clear()
    if self.ws:
      try:
        self.ws.close()
      except Exception:
        pass
  def run(self) -> None:
    self.initialize()
    signal.signal(signal.SIGTERM, self.stop)
    signal.signal(signal.SIGINT, self.stop)
    thread = threading.Thread(target=self.websocket_loop, daemon=True, name="binance-ws")
    backup = threading.Thread(target=self.fallback_loop, daemon=True, name="rest-backup")
    sender = threading.Thread(target=self.telegram_loop, daemon=True, name="telegram-outbox")
    thread.start()
    backup.start()
    sender.start()
    self.event_loop()
def main() -> None:
  log.info("AVVIO %s V%s: 60 crypto; 4H + 1H-48h + M15(10) + PRICE REJECTION 4H/1H; scan EVENT-DRIVEN; BTC1H macro breakout",
      BOT_NAME, BOT_VERSION)
  if not TOKEN or not CHAT_ID:
    raise RuntimeError("Imposta TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nelle Variables di Railway")
  engine = SignalEngine()
  engine.run()
if __name__ == "__main__":
  main()
