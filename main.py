"""CryptoSignalAI RESET: un unico segnale FAST CANDLE LONG/SHORT.

Strategia sperimentale NON validata, da usare prima in simulazione.
Requisiti per entrambi i versi, sulla candela 15m LIVE:
- corpo >=90% del range e >=0.80 ATR(14) su candele precedenti CHIUSE;
- wick in direzione del movimento <=4% del range; altro wick <=6%;
- prezzo nel 5% finale del range, verso la direzione del movimento;
- volume progressivo >=2.50 volte media 10 candele 15m CHIUSE;
- BTC 15m LIVE nella medesima direzione (prezzo vs apertura).
Analisi ogni ~1s su dati WebSocket Binance Futures; 25 coppie.
SL largo sotto/sopra gli estremi delle ultime 5 candele 1H CHIUSE + 0.20 ATR1H.
TP1=0.30R, TP2=0.45R, TP3=0.60R.
Nessun altro indicatore, trend, punteggio o filtro di ingresso.
"""

import json
import os
import queue
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import websocket

# Variabili ambiente gia' usate nel progetto Telegram/Railway
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

SYMBOLS = [
    'SOLUSDT', 'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT',
    'DOGEUSDT', 'AVAXUSDT', 'SUIUSDT', 'LINKUSDT', 'ADAUSDT',
    'NEARUSDT', 'UNIUSDT', 'LTCUSDT', 'DOTUSDT', 'APTUSDT',
    'ARBUSDT', 'OPUSDT', 'INJUSDT', 'RENDERUSDT', '1000PEPEUSDT',
    'FILUSDT', 'ETCUSDT', 'AAVEUSDT', 'TIAUSDT', 'SEIUSDT',
]

# Gli UNICI criteri per il segnale
SCAN_SECONDS = 1.0
TIMEFRAME_SECONDS = 15 * 60
BODY_FRACTION_MIN = 0.90
LEADING_WICK_MAX = 0.04    # LONG sopra; SHORT sotto
TRAILING_WICK_MAX = 0.06   # LONG sotto; SHORT sopra
CLOSE_POSITION_MIN = 0.95  # LONG parte superiore; SHORT parte inferiore
BODY_ATR_MIN = 0.80
VOLUME_RATIO_MIN = 2.50
ATR_PERIOD = 14
VOLUME_AVG_BARS = 10

# SOLO livelli di uscita: NON filtri tecnici aggiuntivi per l'ingresso
SL_HOURLY_LOOKBACK = 5       # 5 candele orarie complete, NON quella live
SL_BUFFER_ATR1H = 0.20       # oltre i minimi/massimi di struttura 1H
TP1_R = 0.30
TP2_R = 0.45
TP3_R = 0.60
HOUR_MS = 60 * 60 * 1000
FIFTEEN_MIN_MS = TIMEFRAME_SECONDS * 1000

# Solo affidabilita'/trasporto, NON filtri di strategia
KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines'
WS_BASE_URL = 'wss://fstream.binance.com/stream?streams='
WS_STALE_SECONDS = 5.0
WS_MAX_RECONNECT_SECONDS = 30
REST_GAP_SECONDS = 0.30
REST_RETRY_WAIT = (5, 10, 20)
REST_REFRESH_SECONDS = 300
GAP_REFRESH_SECONDS = 15
LOG_INTERVAL_SECONDS = 15
TELEGRAM_RETRY_SECONDS = 30
HISTORY_MAXLEN = 120
ITALY = ZoneInfo('Europe/Rome')

cache_lock = threading.RLock()
rest_lock = threading.Lock()
alert_lock = threading.Lock()
market_cache = {}              # {symbol: [candele 15m, ...]}
last_ws_receive = {}          # {symbol: time.monotonic()}
needs_refresh = set()         # buchi nella serie, recuperati REST
websocket_connected = False
last_rest_call = 0.0
sent_alerts = set()           # (symbol, apertura della candela)
pending_alerts = set()
failed_alert_times = {}
telegram_queue = queue.Queue(maxsize=100)


def log(*parts):
    print(*parts, flush=True)


def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        log('TELEGRAM NON CONFIGURATO: impostare TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID')
        return False
    try:
        response = requests.post(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            data={'chat_id': CHAT_ID, 'text': text}, timeout=20,
        )
        response.raise_for_status()
        if not response.json().get('ok'):
            log('Telegram ha rifiutato il messaggio:', response.text[:250])
            return False
        return True
    except (requests.RequestException, ValueError) as exc:
        log('Errore Telegram:', exc)
        return False


def request_klines(symbol):
    global last_rest_call
    error = None
    for attempt in range(len(REST_RETRY_WAIT) + 1):
        with rest_lock:
            pause = REST_GAP_SECONDS - (time.monotonic() - last_rest_call)
            if pause > 0:
                time.sleep(pause)
            last_rest_call = time.monotonic()
        try:
            response = requests.get(
                KLINES_URL,
                params={'symbol': symbol, 'interval': '15m', 'limit': HISTORY_MAXLEN},
                timeout=15,
            )
            if response.status_code in (418, 429):
                header = response.headers.get('Retry-After')
                try:
                    wait = max(5.0, float(header))
                except (ValueError, TypeError):
                    wait = REST_RETRY_WAIT[min(attempt, len(REST_RETRY_WAIT)-1)]
                log('Limite API Binance:', response.status_code, symbol, 'pausa', wait)
                time.sleep(wait)
                error = RuntimeError(f'HTTP {response.status_code}')
                continue
            response.raise_for_status()
            raw = response.json()
            candles = [{
                't': int(row[0]), 'o': float(row[1]), 'h': float(row[2]),
                'l': float(row[3]), 'c': float(row[4]), 'v': float(row[5]),
            } for row in raw]
            if len(candles) < ATR_PERIOD + 2:
                raise ValueError(f'{symbol}: storico insufficiente')
            return candles
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            error = exc
        if attempt < len(REST_RETRY_WAIT):
            time.sleep(REST_RETRY_WAIT[attempt])
    raise RuntimeError(f'{symbol}: impossibile recuperare dati 15m ({error})')


def refresh_symbol(symbol):
    candles = request_klines(symbol)
    with cache_lock:
        old = market_cache.get(symbol)
        if old:
            # Non rimpiazzare dati WebSocket piu' recenti con REST in ritardo.
            latest_old, latest_new = old[-1], candles[-1]
            if latest_old['t'] > latest_new['t']:
                return
            if latest_old['t'] == latest_new['t'] and latest_old['v'] > latest_new['v']:
                candles[-1] = latest_old
        market_cache[symbol] = candles[-HISTORY_MAXLEN:]
        needs_refresh.discard(symbol)


def initialize_history():
    log('Caricamento storico 15m per', len(SYMBOLS), 'coppie...')
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(refresh_symbol, s): s for s in SYMBOLS}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                log('Errore storico', futures[future], exc)
    with cache_lock:
        available = len(market_cache)
    if available == 0:
        raise RuntimeError('Nessun dato Binance disponibile')
    log('Storico inizializzato:', available, '/', len(SYMBOLS))


def receive_kline(payload):
    event = payload.get('data', payload)
    if event.get('e') != 'kline':
        return
    k = event.get('k') or {}
    symbol = str(event.get('s', '')).upper()
    if symbol not in SYMBOLS or k.get('i') != '15m':
        return
    try:
        c = {
            't': int(k['t']), 'o': float(k['o']), 'h': float(k['h']),
            'l': float(k['l']), 'c': float(k['c']), 'v': float(k['v']),
        }
        if c['t'] <= 0 or c['v'] < 0 or not (c['l'] <= min(c['o'], c['c']) <= max(c['o'], c['c']) <= c['h']):
            return
    except (KeyError, ValueError, TypeError):
        return

    with cache_lock:
        previous = market_cache.get(symbol)
        if not previous:
            needs_refresh.add(symbol)
            return
        last = previous[-1]
        if c['t'] < last['t']:
            return
        if c['t'] == last['t']:
            if c['v'] < last['v']:  # scarta pacchetti cumulativi vecchi
                return
            market_cache[symbol] = previous[:-1] + [c]
        elif c['t'] == last['t'] + TIMEFRAME_SECONDS * 1000:
            market_cache[symbol] = (previous + [c])[-HISTORY_MAXLEN:]
        else:
            needs_refresh.add(symbol)  # serie incompleta: niente segnali finche' recuperata
            return
        last_ws_receive[symbol] = time.monotonic()


def websocket_worker():
    global websocket_connected
    streams = '/'.join(f'{s.lower()}@kline_15m' for s in SYMBOLS)
    url = WS_BASE_URL + streams
    delay = 1
    while True:
        try:
            def on_open(ws):
                nonlocal delay
                global websocket_connected
                delay = 1
                with cache_lock:
                    websocket_connected = True
                log('WS connesso,', len(SYMBOLS), 'stream 15m')

            def on_message(ws, message):
                try:
                    receive_kline(json.loads(message))
                except (ValueError, TypeError, AttributeError) as exc:
                    log('Messaggio WS non valido:', exc)

            def on_error(ws, error):
                log('WebSocket errore:', error)

            def on_close(ws, code, reason):
                global websocket_connected
                with cache_lock:
                    websocket_connected = False
                log('WS disconnesso:', code, reason)

            ws = websocket.WebSocketApp(
                url, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=60, ping_timeout=20)
        except Exception as exc:
            log('Errore connessione WS:', exc)
        finally:
            with cache_lock:
                websocket_connected = False
        time.sleep(delay)
        delay = min(delay * 2, WS_MAX_RECONNECT_SECONDS)


def history_worker():
    last_full = time.monotonic()
    last_gap = 0.0
    while True:
        now = time.monotonic()
        with cache_lock:
            gaps = list(needs_refresh)
            missing = [s for s in SYMBOLS if s not in market_cache]
        if now - last_gap >= GAP_REFRESH_SECONDS:
            for symbol in set(gaps + missing):
                try:
                    refresh_symbol(symbol)
                except Exception as exc:
                    log('Recupero REST fallito:', symbol, exc)
            last_gap = now
        if now - last_full >= REST_REFRESH_SECONDS:
            for symbol in SYMBOLS:
                try:
                    refresh_symbol(symbol)
                except Exception as exc:
                    log('Refresh REST fallito:', symbol, exc)
            last_full = time.monotonic()
        time.sleep(1)


def true_range_atr(closed_candles):
    """ATR14 su sole candele CHIUSE: non usa mai quella live."""
    if len(closed_candles) < ATR_PERIOD + 1:
        return None
    bars = closed_candles[-(ATR_PERIOD + 1):]
    tr = []
    for i in range(1, len(bars)):
        c, prev = bars[i], bars[i - 1]
        tr.append(max(c['h']-c['l'], abs(c['h']-prev['c']), abs(c['l']-prev['c'])))
    return sum(tr) / ATR_PERIOD


def live_volume_pace(candles, now_ms):
    """Volume proiettato a 15m / volume medio delle 10 precedenti chiuse."""
    if len(candles) < VOLUME_AVG_BARS + 1:
        return 0.0
    live = candles[-1]
    previous = candles[-(VOLUME_AVG_BARS + 1):-1]
    avg = sum(c['v'] for c in previous) / len(previous)
    if avg <= 0:
        return 0.0
    elapsed = min(float(TIMEFRAME_SECONDS), max(1.0, (now_ms - live['t']) / 1000.0))
    return max(0.0, live['v']) / (avg * elapsed / TIMEFRAME_SECONDS)



def hourly_closed_candles(candles_15m):
    """Ricostruisce vere OHLC 1H usando esclusivamente quartetti 15m CHIUSI.

    Il gruppo 1H parziale (inizio/fine dello storico e ora in corso) non
    viene incluso. L'allineamento e' alle ore UTC, come su Binance.
    """
    groups = {}
    for c in candles_15m[:-1]:  # esclusa sempre la 15m live
        hour_t = (c['t'] // HOUR_MS) * HOUR_MS
        groups.setdefault(hour_t, []).append(c)
    result = []
    for hour_t, group in sorted(groups.items()):
        if len(group) != 4:
            continue
        group.sort(key=lambda c: c['t'])
        if [c['t'] for c in group] != [hour_t + i * FIFTEEN_MIN_MS for i in range(4)]:
            continue
        result.append({
            't': hour_t,
            'o': group[0]['o'],
            'h': max(c['h'] for c in group),
            'l': min(c['l'] for c in group),
            'c': group[-1]['c'],
            'v': sum(c['v'] for c in group),
        })
    return result


def hourly_structural_stop(candles_15m, direction):
    """Stop largo basato su 5 ore chiuse + 0.20 ATR1H; mai su 1H parziale."""
    bars_1h = hourly_closed_candles(candles_15m)
    if len(bars_1h) < max(SL_HOURLY_LOOKBACK, ATR_PERIOD + 1):
        return None
    atr1h = true_range_atr(bars_1h)
    if not atr1h or atr1h <= 0:
        return None
    structural = bars_1h[-SL_HOURLY_LOOKBACK:]
    live = candles_15m[-1]
    if direction == 'LONG':
        # Non collocare mai lo stop sopra il minimo della candela di entrata.
        extreme = min(min(c['l'] for c in structural), live['l'])
        sl = extreme - SL_BUFFER_ATR1H * atr1h
    else:
        extreme = max(max(c['h'] for c in structural), live['h'])
        sl = extreme + SL_BUFFER_ATR1H * atr1h
    return {'sl': sl, 'atr1h': atr1h, 'structure_extreme': extreme}


def btc15_direction(btc_live):
    """Unica conferma esterna: soltanto il colore della candela BTC 15m LIVE."""
    if btc_live['c'] > btc_live['o']:
        return 'LONG'
    if btc_live['c'] < btc_live['o']:
        return 'SHORT'
    return 'NEUTRAL'


def analyze_candle(symbol, candles, btc_direction, now_ms):
    """Restituisce una segnalazione soltanto quando TUTTI i criteri concordano."""
    if len(candles) < max(ATR_PERIOD + 2, VOLUME_AVG_BARS + 1):
        return None
    current = candles[-1]
    if not (current['t'] <= now_ms < current['t'] + TIMEFRAME_SECONDS * 1000):
        return None
    opening, closing = current['o'], current['c']
    if closing == opening:
        return None
    direction = 'LONG' if closing > opening else 'SHORT'
    if btc_direction != direction:
        return None
    span = current['h'] - current['l']
    if span <= 0:
        return None
    body = abs(closing - opening)
    body_fraction = body / span
    if body_fraction < BODY_FRACTION_MIN:
        return None
    upper_wick = max(0.0, current['h'] - max(opening, closing)) / span
    lower_wick = max(0.0, min(opening, closing) - current['l']) / span
    leading_wick = upper_wick if direction == 'LONG' else lower_wick
    trailing_wick = lower_wick if direction == 'LONG' else upper_wick
    if leading_wick > LEADING_WICK_MAX or trailing_wick > TRAILING_WICK_MAX:
        return None
    close_position = ((closing-current['l']) / span) if direction == 'LONG' else ((current['h']-closing) / span)
    if close_position < CLOSE_POSITION_MIN:
        return None
    atr15 = true_range_atr(candles[:-1])
    if not atr15 or body / atr15 < BODY_ATR_MIN:
        return None
    volume_pace = live_volume_pace(candles, now_ms)
    if volume_pace < VOLUME_RATIO_MIN:
        return None
    hourly_stop = hourly_structural_stop(candles, direction)
    if not hourly_stop:  # impossibile inviare un SL 1H affidabile senza storico
        return None

    return {
        'symbol': symbol, 'direction': direction, 'entry': closing,
        'candle_start': current['t'], 'candle_high': current['h'],
        'candle_low': current['l'], 'body_fraction': body_fraction,
        'body_atr': body / atr15, 'leading_wick': leading_wick,
        'trailing_wick': trailing_wick, 'close_position': close_position,
        'volume_pace': volume_pace, 'btc_direction': btc_direction,
        'elapsed': min(TIMEFRAME_SECONDS, max(0.0, (now_ms - current['t']) / 1000)),
        'atr15': atr15, 'sl': hourly_stop['sl'],
        'atr1h': hourly_stop['atr1h'],
        'structure_extreme': hourly_stop['structure_extreme'],
    }


def price_format(price):
    if price >= 1000:
        return f'{price:.2f}'
    if price >= 1:
        return f'{price:.4f}'
    if price >= 0.01:
        return f'{price:.6f}'
    return f'{price:.9f}'


def message_for(signal):
    """SL su 5 candele 1H chiuse, TP1/2/3 in R; nessuna leva consigliata."""
    s = signal
    d = 1 if s['direction'] == 'LONG' else -1
    sl = s['sl']
    risk = abs(s['entry'] - sl)
    tp1 = s['entry'] + d * TP1_R * risk
    tp2 = s['entry'] + d * TP2_R * risk
    tp3 = s['entry'] + d * TP3_R * risk
    now_italy = datetime.now(ITALY).strftime('%d/%m/%Y %H:%M:%S')
    candle_italy = datetime.fromtimestamp(s['candle_start'] / 1000, ITALY).strftime('%H:%M')
    return (
        f"FAST CANDLE 15M - {s['direction']} [TEST]\n"
        f"{s['symbol'].replace('USDT', '/USDT')}\n"
        f"Orario Italia: {now_italy}\n"
        f"Candela LIVE iniziata alle: {candle_italy}\n"
        f"ENTRY indicativa: {price_format(s['entry'])}\n"
        f"SL largo 1H (5 candele chiuse + 0.20 ATR1H): {price_format(sl)}\n"
        f"TP1 (0.30R): {price_format(tp1)}\n"
        f"TP2 (0.45R): {price_format(tp2)}\n"
        f"TP3 (0.60R): {price_format(tp3)}\n\n"
        f"Corpo pieno: {s['body_fraction']*100:.1f}%\n"
        f"Corpo / ATR15: {s['body_atr']:.2f}x\n"
        f"Wick direzionale: {s['leading_wick']*100:.1f}% (max 4%)\n"
        f"Wick opposto: {s['trailing_wick']*100:.1f}% (max 6%)\n"
        f"Volume progressivo: {s['volume_pace']:.2f}x (min 2.50x)\n"
        f"BTC 15m LIVE: {s['btc_direction']}\n"
        f"Tempo candela: {s['elapsed']:.0f}s\n"
        f"Strategia sperimentale. Volume proiettato e candela non ancora chiusi.\n"
        f"SL/TP non garantiscono esecuzione o protezione da liquidazione."
    )


def enqueue_alert(signal):
    key = (signal['symbol'], signal['candle_start'])
    now = time.monotonic()
    with alert_lock:
        if key in sent_alerts or key in pending_alerts:
            return False
        if now - failed_alert_times.get(key, -1e9) < TELEGRAM_RETRY_SECONDS:
            return False
        pending_alerts.add(key)
    try:
        telegram_queue.put_nowait((key, message_for(signal)))
        return True
    except queue.Full:
        with alert_lock:
            pending_alerts.discard(key)
        log('Coda Telegram piena:', key)
        return False


def telegram_worker():
    while True:
        key, message = telegram_queue.get()
        try:
            ok = send_telegram(message)
            with alert_lock:
                if ok:
                    sent_alerts.add(key)
                else:
                    failed_alert_times[key] = time.monotonic()
        finally:
            with alert_lock:
                pending_alerts.discard(key)
            telegram_queue.task_done()


def current_snapshot():
    """Solo serie con WebSocket corrente e senza buchi; niente REST a ogni scan."""
    now = time.monotonic()
    with cache_lock:
        if not websocket_connected:
            return {}
        return {
            symbol: list(market_cache[symbol])
            for symbol in SYMBOLS
            if symbol in market_cache
            and symbol not in needs_refresh
            and now - last_ws_receive.get(symbol, -1e9) <= WS_STALE_SECONDS
        }


def scan_market():
    snapshot = current_snapshot()
    if 'BTCUSDT' not in snapshot:
        return 0
    now_ms = int(time.time() * 1000)
    btc_live = snapshot['BTCUSDT'][-1]
    if not (btc_live['t'] <= now_ms < btc_live['t'] + TIMEFRAME_SECONDS * 1000):
        return 0
    btc_direction = btc15_direction(btc_live)
    if btc_direction == 'NEUTRAL':
        return 0
    queued = 0
    for symbol, candles in snapshot.items():
        if candles[-1]['t'] != btc_live['t']:
            continue
        signal = analyze_candle(symbol, candles, btc_direction, now_ms)
        if signal and enqueue_alert(signal):
            queued += 1
            log('SEGNALE', signal['direction'], symbol,
                '| volume', f"{signal['volume_pace']:.2f}x",
                '| corpo', f"{signal['body_fraction']*100:.1f}%")
    # Liberiamo deduplica di sessioni vecchie, senza riavvisi nella stessa candela.
    oldest_allowed = btc_live['t'] - 4 * 60 * 60 * 1000
    with alert_lock:
        sent_alerts.intersection_update({k for k in sent_alerts if k[1] >= oldest_allowed})
        for key in list(failed_alert_times):
            if key[1] < oldest_allowed:
                del failed_alert_times[key]
    return queued


def main():
    log('CryptoSignalAI RESET - 1 solo segnale LONG/SHORT | 25 crypto WS 1s')
    log('PARAMETRI: corpo>=90%, wick direzionale<=3%, opposto<=5%,',
        'close>=95%, corpo>=0.8ATR15, volume pace>=2.50x, BTC15 concorde,',
        'SL=5 candele 1H chiuse +0.20 ATR1H; TP1=0.30R, TP2=0.45R, TP3=0.60R')
    if not BOT_TOKEN or not CHAT_ID:
        log('ATTENZIONE: mancano variabili ambiente Telegram; niente notifiche.')
    while True:
        try:
            initialize_history()
            break
        except Exception as exc:
            log('Storico Binance non disponibile:', exc, 'riprovo fra 20 secondi')
            time.sleep(20)

    threading.Thread(target=telegram_worker, daemon=True).start()
    threading.Thread(target=history_worker, daemon=True).start()
    threading.Thread(target=websocket_worker, daemon=True).start()
    send_telegram('CryptoSignalAI RESET AVVIATO. Un solo FAST CANDLE LONG/SHORT. '
                  '25 crypto; scansione 1s; corpo 90%, wick 4%/6%, '
                  'corpo >=0.80 ATR, volume progressivo >=2.50x, solo BTC15 concorde. '
                  'SL 5 candele 1H chiuse + 0.20 ATR1H. TP: 0.30R / 0.45R / 0.60R. '
                  'Strategia sperimentale: testare in simulazione.')
    next_scan = time.monotonic()
    last_log = next_scan
    while True:
        next_scan += SCAN_SECONDS
        try:
            scan_market()
        except Exception as exc:
            log('ERRORE SCANSIONE:', repr(exc))
        now = time.monotonic()
        if now - last_log >= LOG_INTERVAL_SECONDS:
            snap = current_snapshot()
            b = btc15_direction(snap['BTCUSDT'][-1]) if 'BTCUSDT' in snap else 'DATI NON FRESCHI'
            log('Scansione 1s | feed attivi:', len(snap), '/25 | BTC15:', b)
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()


if __name__ == '__main__':
    main()
