from __future__ import annotations
import json
import logging
import os
import queue
import signal
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
BOT_NAME = "MOMENTUM 4H 60"
BOT_VERSION = "1.1"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FOUR_HOUR_LOOKBACK = 6
FOUR_HOUR_BODY_MIN = 0.55
FOUR_HOUR_EACH_WICK_MAX = 0.225
FIFTEEN_MIN_BODY_MIN = 0.85
FIFTEEN_MIN_EACH_WICK_MAX = 0.075
FIFTEEN_MIN_WICKS_SUM_MAX = 0.15
VOLUME_LOOKBACK = 20
DIRECT_VOLUME_MIN = 1.50
RETEST_TOUCH_VOLUME_MAX = 1.00
RETEST_BOUNCE_VOLUME_MIN = 1.00
SL_BUFFER_ATR4H = 0.50
ATR_PERIOD = 14
TAKE_PROFITS_PERCENT = (0.02, 0.04, 0.06)
BTC_STRUCTURE_HOURS = 6
BTC_GROUP_SIZE = 3
SETUP_LIFETIME_HOURS = 8
HISTORY_LIMIT = 420
FIFTEEN_MIN_MS = 15 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
FOUR_HOURS_MS = 4 * ONE_HOUR_MS
SETUP_LIFETIME_MS = SETUP_LIFETIME_HOURS * ONE_HOUR_MS
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
def evaluate_entry(setup: Breakout, candles: list[dict], btc_bias: str,
                   close_time: int) -> tuple[Optional[dict], bool]:
    if setup.sent or not candles or close_time <= setup.macro_close_ms:
        return None, False
    if close_time - setup.macro_close_ms > SETUP_LIFETIME_MS:
        return None, True
    bar = candles[-1]
    if bar["t"] + FIFTEEN_MIN_MS != close_time:
        return None, False
    if invalidated(setup, bar):
        return None, True
    ratio = volume_ratio(candles, len(candles) - 1)
    if ratio is None:
        return None, False
    direction, level = setup.direction, setup.level
    closed_beyond = beyond_level(bar["c"], direction, level)
    strong = closed_beyond and candle_shape_ok(bar, direction, macro=False)
    if setup.touched_at:
        if (close_time > setup.touched_at + FIFTEEN_MIN_MS and strong
                and ratio > RETEST_BOUNCE_VOLUME_MIN
                and btc_compatible(btc_bias, direction, setup.symbol)):
            return make_signal(setup, bar, "RETEST", ratio, btc_bias), False
        return None, False
    if touched_level(bar, level) and ratio < RETEST_TOUCH_VOLUME_MAX:
        setup.touched_at = bar["t"]
        return None, False
    if strong and ratio >= DIRECT_VOLUME_MIN and btc_compatible(btc_bias, direction, setup.symbol):
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
    return (
        f"{emoji} {s['direction']} | {sym} | 4H | {s['mode']}\n\n"
        f"Entry: {price_string(s['entry'])}\n"
        f"Stop Loss: {price_string(s['sl'])}\n"
        f"TP1: {price_string(s['tp1'])} (+/-2%)\n"
        f"TP2: {price_string(s['tp2'])} (+/-4%)\n"
        f"TP3: {price_string(s['tp3'])} (+/-6%)\n\n"
        f"Volume 15m: {s['volume_ratio']:.2f}x\n"
        f"Livello 4H: {price_string(s['level'])}\n"
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
        self.checked_15m: dict[str, int] = {}
        self.checked_4h: dict[str, int] = {}
        self.scan_errors = 0
        self.setups: dict[str, Breakout] = {}
        self.pending: dict[str, tuple[int, float, float]] = {}
        self.events: queue.Queue[tuple[str, dict]] = queue.Queue(maxsize=4096)
        self.state = load_state()
        self.sent = set(self.state.get("sent", []))
        self.outbox: dict[str, dict] = dict(self.state.get("outbox", {}))
        self.ws_last_message = 0.0
        self.ws = None
        self.last_recovery = defaultdict(float)
        self.startup_warning = ""
    def persist(self) -> None:
        recent = sorted(self.sent)[-5000:]
        self.state["sent"] = recent
        self.state["outbox"] = self.outbox
        save_state(self.state)
    def initialize(self) -> None:
        tradable = self.rest.active_perpetuals()
        self.active_symbols = [s for s in SYMBOLS if s in tradable]
        missing = [s for s in SYMBOLS if s not in tradable]
        if missing:
            self.startup_warning = "NON disponibili su Binance futures USDT: " + ", ".join(missing)
            log.warning(self.startup_warning)
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
            "BTC1H favorevole/laterale | SL livello 4H +/-0.15 ATR4H\n"
            "TP fissi 2% / 4% / 6% dall'Entry | nessun ordine automatico"
        )
        telegram_send(status)
        if missing:
            telegram_send("⚠️ " + self.startup_warning + "\nSostituire queste coppie per arrivare a 60 attive.")
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
            if setup and close_t - setup.macro_close_ms > SETUP_LIFETIME_MS:
                self.setups.pop(symbol, None)
                setup = None
            if close_t % FOUR_HOURS_MS == 0 and self.last_macro_checked.get(symbol) != close_t:
                self.last_macro_checked[symbol] = close_t
                candidate = detect_breakout(symbol, history, close_t)
                self.checked_4h[symbol] = close_t
                if candidate:
                    self.setups[symbol] = candidate
                    setup = candidate
                    log.info("BREAKOUT 4H %s %s, livello %s", symbol,
                             candidate.direction, price_string(candidate.level))
                else:
                    self.setups.pop(symbol, None)
                    setup = None
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
                self.sent.add(dedup_key)
                self.outbox[dedup_key] = out
                self.persist()
                self.setups.pop(symbol, None)
            else:
                return True
        log.info("SEGNALE IN CODA %s %s %s: volume %.2fx BTC %s",
                 symbol, out["direction"], out["mode"], out["volume_ratio"], out["btc"])
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
    log.info("AVVIO %s V%s: 60 crypto; 4H 6 candele; scan EVENT-DRIVEN; BTC1H", BOT_NAME, BOT_VERSION)
    if not TOKEN or not CHAT_ID:
        raise RuntimeError("Imposta TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nelle Variables di Railway")
    engine = SignalEngine()
    engine.run()
if __name__ == "__main__":
    main()
