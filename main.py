"""MOMENTUM ONE V5 - BREAKOUT 1H/4H SENZA FILTRO VOLUME.

40 Binance USDT-M perpetual: 18 simboli con breakout 1H e 22 con breakout 4H.
Parametri identici alla V4 ROBUST condivisa dall'utente: lookback 15/6,
nessun volume minimo sul breakout 1H/4H; conferma 15m 0.60x
sulla mediana delle ultime 20 barre, filtro BTC e deroga +/-5pp,
body/wick/ATR, entrata DIRECT prima 15m o RETEST, SL e TP invariati.

Fix infrastruttura:
- Candele macro complete, allineate e consecutive per i filtri necessari.
- Feed operativo solo con 15m corrente E fonte WS/REST realmente recente.
- Conferma entro 30s dalla chiusura; BTC non pronto => ritenta senza consumarla.
- Setup protetti anche all'apertura di nuove macro e ri-valutati a ogni scan.
- Outbox Telegram su disco, retry fino a 5 minuti, delivery log, dead-letter.

Nessun ordine automatico. Solo segnali Telegram [SIMULAZIONE].
"""


import json
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from statistics import median
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
    # 15 nuove altcoin: SOLO strategia 4H; nessun filtro modificato.
    'HBARUSDT', 'XLMUSDT', 'TRXUSDT', 'ONDOUSDT', 'ENAUSDT',
    'WIFUSDT', 'TAOUSDT', '1000SHIBUSDT', '1000BONKUSDT', 'WLDUSDT',
    'FETUSDT', 'ATOMUSDT', 'BCHUSDT', 'JUPUSDT', 'PENDLEUSDT',
]

# Le 40 coppie restano monitorate in un solo processo.
ONE_HOUR_SYMBOLS = frozenset((
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT',
    '1000PEPEUSDT', '1000BONKUSDT', '1000SHIBUSDT', 'WIFUSDT',
    'TAOUSDT', 'FETUSDT', 'RENDERUSDT',
    'SUIUSDT', 'SEIUSDT', 'JUPUSDT', 'INJUSDT',
    'ARBUSDT', 'OPUSDT', 'PENDLEUSDT', 'ONDOUSDT',
))
FOUR_HOUR_SYMBOLS = frozenset(s for s in SYMBOLS if s not in ONE_HOUR_SYMBOLS)
SCAN_SECONDS = 1.0
TIMEFRAME_SECONDS = 15 * 60
FIFTEEN_MIN_MS = 15 * 60 * 1000
ATR_PERIOD = 14
BTC_NEUTRAL_BODY_ATR_MAX = 0.35
BTC_4H_NEUTRAL_BODY_ATR_MAX = 0.35  # SOLO segnali 1H: BTC 4H neutro se corpo < 0.35 ATR4H
ENTRY_MAX_DISTANCE_ATR1H = 0.50  # Tutti i segnali: estensione massima dell'entry dal breakout
VOLUME_AVG_BARS_15M = 20
LIVE_VOLUME_PACE_MIN_SECONDS = 60
LIVE_VOLUME_PROGRESS_LOG_SECONDS = 180
# Retest: deve toccare il livello di breakout; nessuna soglia ATR sul retest.
MICRO_CLOSE_BUFFER_ATR15 = 0.02
MACRO_CLOSE_GRACE_SECONDS = 1800
CLOSE_EVENT_MAX_DELAY_SECONDS = 30
TP1_R, TP2_R, TP3_R = 0.30, 0.80, 1.50
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
RELATIVE_BTC_OVERRIDE_PCT = 5.0  # filtro BTC15 opposto
# Booster volume macro +/-7pp disattivato: senza soglia macro non ha funzione.
# Resta invariata la deroga BTC +/-5pp per altcoin con forza relativa 4H.
DIRECT_CLOSE_POSITION_MIN = 0.80  # estremo direzionale per prima 15m diretta

# Parametri indipendenti per i due gruppi: nessuna sovrapposizione di segnali.
STRATEGIES = {
    '1H': {
        'name': '1H', 'macro_ms': ONE_HOUR_MS,
        'lookback': 15, 'breakout_buffer_atr': 0.04,
        'macro_close_position_min': 0.65,
        'confirm_volume_min': 0.60, 'body_fraction': 0.70,
        'body_atr_min': 0.60, 'wick_directional': 0.12,
        'wick_opposite': 0.12, 'sl_lookback': 5,
        'sl_buffer_atr': 0.25, 'lifetime_seconds': 6 * 60 * 60,
    },
    '4H': {
        'name': '4H', 'macro_ms': FOUR_HOURS_MS,
        'lookback': 6, 'breakout_buffer_atr': 0.03,
        'macro_close_position_min': 0.70,
        'confirm_volume_min': 0.60, 'body_fraction': 0.70,
        'body_atr_min': 0.50, 'wick_directional': 0.15,
        'wick_opposite': 0.15, 'sl_lookback': 3,
        'sl_buffer_atr': 0.25, 'lifetime_seconds': 12 * 60 * 60,
    },
}

def strategy_for(symbol):
    return STRATEGIES['1H'] if symbol in ONE_HOUR_SYMBOLS else STRATEGIES['4H']


# Configurazione affidabilita'/trasporto e storico sufficiente ai filtri 4H
KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines'
WS_BASE_URL = 'wss://fstream.binance.com/market/stream?streams='
WS_STALE_SECONDS = 90.0  # 5s scartava feed anche durante brevi pause negli scambi
REST_FRESH_SECONDS = 90.0  # REST recente e' un backup, non una finta notifica WS
REST_FALLBACK_TRIGGER_SECONDS = 15.0  # WS silenzioso: valuta recupero dati
REST_FALLBACK_POLL_SECONDS = 30.0  # limita le richieste REST per simbolo
REST_BOUNDARY_POLL_SECONDS = 5.0  # recupero rapido se la 15m live non e' ancora arrivata
REST_FALLBACK_RECENT_BARS = 5  # recupera solo ultime barre, non 480 ogni volta
WS_MAX_RECONNECT_SECONDS = 30
REST_GAP_SECONDS = 0.30
REST_RETRY_WAIT = (5, 10, 20)
REST_REFRESH_SECONDS = 300
GAP_REFRESH_SECONDS = 15
LOG_INTERVAL_SECONDS = 15
TELEGRAM_RETRY_SECONDS = 30
HISTORY_MAXLEN = 480  # almeno 20 4H precedenti + breakout per le altcoin
# Parametri trasporto Telegram (NON influiscono sui requisiti di trading)
TELEGRAM_MAX_ATTEMPTS = 8
TELEGRAM_DELIVERY_WINDOW_SECONDS = 300
TELEGRAM_RETRY_INITIAL_SECONDS = 3
TELEGRAM_RETRY_MAX_SECONDS = 40
# Imposta TELEGRAM_OUTBOX_FILE su un volume persistente Railway per durabilita' ai redeploy.
# Senza volume, il contenuto della directory /tmp NON sopravvive ai nuovi container.
TELEGRAM_OUTBOX_FILE = os.getenv('TELEGRAM_OUTBOX_FILE', '/tmp/momentum_one_telegram_outbox.json')
TELEGRAM_RETRY_ROUND_SECONDS = 45
ITALY = ZoneInfo('Europe/Rome')

cache_lock = threading.RLock()
rest_lock = threading.Lock()
alert_lock = threading.Lock()
market_cache = {}              # {symbol: [candele 15m, ...]}
last_ws_receive = {}          # {symbol: time.monotonic()}
last_rest_receive = {}        # {symbol: REST corrente verificato, monotonic()}
last_rest_recovery_try = {}   # {symbol: ultimo tentativo REST mirato, monotonic()}
needs_refresh = set()         # buchi nella serie, recuperati REST
websocket_connected = False
last_rest_call = 0.0
sent_alerts = set()           # (symbol, apertura della candela)
pending_alerts = set()
failed_alert_times = {}
telegram_queue = queue.Queue()  # non perde alert a causa del limite di capacita'
persisted_pending = {}
dead_letters = {}
# Tracking BTC1H: TP1 raggiunto blocca i nuovi segnali ALT fino a nuovo breakout 1H.
# Persistito nello stesso file outbox per conservare lo stato tra i riavvii.
btc1h_timing = {'active_signal': None, 'blocked': False, 'blocked_at_ms': None,
                'last_unblock_macro_t': None}

active_setups = {}             # {symbol: setup 4H confermato in attesa di retest 15m}
seen_macro_bars = {}            # {symbol: t della 4H gia' analizzata}


def log(*parts):
    print(*parts, flush=True)


def _outbox_save_locked():
    """Invocare con alert_lock: serializza jobs prima dell'invio.

    Per sopravvivere ai deploy Railway configurare un Volume e la variabile
    TELEGRAM_OUTBOX_FILE su quel mount. I/O su file separato dalla strategia.
    """
    try:
        folder = os.path.dirname(os.path.abspath(TELEGRAM_OUTBOX_FILE))
        os.makedirs(folder, exist_ok=True)
        payload = {
            'pending': list(persisted_pending.values()),
            'failed': list(dead_letters.values()),
            'sent': [[k[0], k[1]] for k in sent_alerts],
            'btc1h_timing': btc1h_timing,
        }
        temp_file = TELEGRAM_OUTBOX_FILE + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as file:
            json.dump(payload, file, ensure_ascii=False)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_file, TELEGRAM_OUTBOX_FILE)
    except Exception as exc:
        log('TELEGRAM OUTBOX: scrittura non riuscita', repr(exc))


def load_telegram_outbox():
    """Riprende gli alert recenti non confermati dopo un riavvio."""
    try:
        with open(TELEGRAM_OUTBOX_FILE, 'r', encoding='utf-8') as file:
            payload = json.load(file)
    except FileNotFoundError:
        return
    except Exception as exc:
        log('TELEGRAM OUTBOX: lettura non riuscita', repr(exc))
        return
    now = time.time()
    with alert_lock:
        timing = payload.get('btc1h_timing')
        if isinstance(timing, dict):
            btc1h_timing['blocked'] = timing.get('blocked') is True
            btc1h_timing['blocked_at_ms'] = timing.get('blocked_at_ms')
            btc1h_timing['last_unblock_macro_t'] = timing.get('last_unblock_macro_t')
            active = timing.get('active_signal')
            if (isinstance(active, dict) and
                    active.get('direction') in ('LONG', 'SHORT') and
                    isinstance(active.get('tp1'), (int, float)) and
                    isinstance(active.get('sl'), (int, float)) and
                    isinstance(active.get('initial_high'), (int, float)) and
                    isinstance(active.get('initial_low'), (int, float)) and
                    isinstance(active.get('entry_bar_t'), int)):
                btc1h_timing['active_signal'] = active
            if btc1h_timing['blocked']:
                log('BTC1H TIMING: BLOCCO ALT RIPRISTINATO DALLA OUTBOX')
        for record in payload.get('sent', []):
            if isinstance(record, list) and len(record) == 2:
                key = (str(record[0]), int(record[1]))
                sent_alerts.add(key)
        for record in payload.get('failed', []):
            try:
                key = (str(record['symbol']), int(record['macro_t']))
                dead_letters[key] = record
            except (KeyError, ValueError, TypeError):
                continue
        for record in payload.get('pending', []):
            try:
                key = (str(record['symbol']), int(record['macro_t']))
                created = float(record['created_at'])
                msg = str(record['message'])
                if key in sent_alerts:
                    continue
                if now - created > TELEGRAM_DELIVERY_WINDOW_SECONDS:
                    old = dict(record)
                    old['reason'] = 'alert scaduto durante il riavvio; NON inviato in ritardo'
                    dead_letters[key] = old
                    log('TELEGRAM SCADUTO AL RIAVVIO', key[0], key[1])
                    continue
                persisted_pending[key] = record
                pending_alerts.add(key)
                telegram_queue.put_nowait((key, msg, created))
                log('TELEGRAM OUTBOX RIPRISTINATO', key[0], key[1])
            except (KeyError, ValueError, TypeError):
                continue
        _outbox_save_locked()


def telegram_send_once(text):
    """Restituisce (consegnato, ritentabile, attesa_suggerita).

    Un timeout puo' essere ambiguo: Telegram potrebbe aver ricevuto il
    messaggio, anche se il client non ha ricevuto la conferma. L'ID stabile
    nel testo rende riconoscibili eventuali copie, senza poterle impedire.
    """
    if not BOT_TOKEN or not CHAT_ID:
        log('TELEGRAM NON CONFIGURATO: mancano TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID')
        return False, False, 0
    try:
        response = requests.post(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            data={'chat_id': CHAT_ID, 'text': text}, timeout=(8, 20),
        )
        if response.status_code == 429:
            retry_after = 5
            try:
                payload = response.json()
                retry_after = max(1, int(payload.get('parameters', {}).get('retry_after', 5)))
            except (ValueError, TypeError, AttributeError):
                pass
            log('Telegram rate limit HTTP 429; riprovo dopo', retry_after, 'secondi')
            return False, True, retry_after
        if response.status_code >= 500:
            log('Telegram errore temporaneo HTTP', response.status_code)
            return False, True, 0
        if response.status_code >= 400:
            log('Telegram rifiuto permanente HTTP', response.status_code, response.text[:200])
            return False, False, 0
        response.raise_for_status()
        data = response.json()
        if not data.get('ok'):
            log('Telegram risposta senza ok:', str(data)[:250])
            return False, True, 0
        return True, False, 0
    except (requests.RequestException, ValueError) as exc:
        log('Telegram errore di rete/risposta:', repr(exc))
        return False, True, 0


def send_telegram(text):
    """Compatibilita' con messaggio di avvio (non segnale operativo)."""
    ok, _, _ = telegram_send_once(text)
    return ok


def request_klines(symbol, limit=HISTORY_MAXLEN):
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
                params={'symbol': symbol, 'interval': '15m', 'limit': limit},
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
            min_bars = 3 if limit < ATR_PERIOD + 2 else ATR_PERIOD + 2
            if len(candles) < min_bars:
                raise ValueError(f'{symbol}: storico insufficiente')
            return candles
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            error = exc
        if attempt < len(REST_RETRY_WAIT):
            time.sleep(REST_RETRY_WAIT[attempt])
    raise RuntimeError(f'{symbol}: impossibile recuperare dati 15m ({error})')


def refresh_symbol(symbol, recent=False):
    """Aggiorna le candele da REST, con merge sicuro per fallback rapido.

    - All'avvio e nei refresh completi scarica 480 barre.
    - Per feed WS silenziosi scarica 5 barre e le fonde con lo storico.
    - Se mancano candele tra le 2 fonti, non inventa dati: richiede
      un recupero integrale e blocca il simbolo fino al ripristino.
    - Un backup REST e' marcato fresco SOLO se include la 15m corrente.
    """
    with cache_lock:
        has_cache = bool(market_cache.get(symbol))
    use_recent = recent and has_cache
    candles = request_klines(symbol, limit=(REST_FALLBACK_RECENT_BARS
                                            if use_recent else HISTORY_MAXLEN))
    expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
    with cache_lock:
        old = market_cache.get(symbol)
        if old and old[-1]['t'] > candles[-1]['t']:
            # REST in ritardo: non retrocedere la serie aggiornata dal WS.
            return False
        if old and use_recent:
            # Serve una sovrapposizione reale per unire serie senza buchi.
            old_by_t = {c['t']: c for c in old}
            if not any(c['t'] in old_by_t for c in candles):
                needs_refresh.add(symbol)
                return False
            by_t = dict(old_by_t)
            for candle in candles:
                previous = by_t.get(candle['t'])
                if previous is None or candle['v'] >= previous['v']:
                    by_t[candle['t']] = candle
            merged = [by_t[t] for t in sorted(by_t)][-HISTORY_MAXLEN:]
            if any(merged[i+1]['t'] - merged[i]['t'] != FIFTEEN_MIN_MS
                   for i in range(len(merged)-1)):
                needs_refresh.add(symbol)
                return False
            market_cache[symbol] = merged
        else:
            if old and old[-1]['t'] == candles[-1]['t'] and old[-1]['v'] > candles[-1]['v']:
                candles[-1] = old[-1]
            market_cache[symbol] = candles[-HISTORY_MAXLEN:]
        needs_refresh.discard(symbol)
        # I dati REST NON garantiscono continuita' di aggiornamenti WS.
        # Il backup vale solo se la sua risposta ha la candela oraria corrente.
        if candles[-1]['t'] == expected_t:
            last_rest_receive[symbol] = time.monotonic()
    return True


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
    """Recupera buchi e feed poco attivi senza modificare i segnali.

    Il fallback REST e' mirato: 5 candele, massimo ogni 30s per simbolo;
    dopo un cambio di candela 15m non ancora ricevuto, riprova ogni 5s.
    Queste chiamate sono nel worker e non bloccano la scansione ~1s.
    """
    last_full = time.monotonic()
    last_gap = 0.0
    while True:
        now = time.monotonic()
        expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
        with cache_lock:
            gaps = list(needs_refresh)
            missing = [s for s in SYMBOLS if s not in market_cache]
            fallback = []
            for symbol in SYMBOLS:
                if symbol not in market_cache or symbol in needs_refresh:
                    continue
                wrong_bar = market_cache[symbol][-1]['t'] != expected_t
                ws_quiet = (now - last_ws_receive.get(symbol, -1e9)
                            >= REST_FALLBACK_TRIGGER_SECONDS)
                rest_age = now - last_rest_receive.get(symbol, -1e9)
                if not wrong_bar and (not ws_quiet or rest_age < REST_FRESH_SECONDS):
                    continue
                min_wait = (REST_BOUNDARY_POLL_SECONDS if wrong_bar
                            else REST_FALLBACK_POLL_SECONDS)
                if now - last_rest_recovery_try.get(symbol, -1e9) < min_wait:
                    continue
                last_rest_recovery_try[symbol] = now
                fallback.append(symbol)

        if now - last_gap >= GAP_REFRESH_SECONDS:
            for symbol in set(gaps + missing):
                try:
                    refresh_symbol(symbol)
                except Exception as exc:
                    log('Recupero REST fallito:', symbol, exc)
            last_gap = now

        for symbol in fallback:
            try:
                if not refresh_symbol(symbol, recent=True):
                    # Il simbolo resta escluso se richiede uno storico completo.
                    pass
            except Exception as exc:
                log('Fallback feed REST fallito:', symbol, exc)

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


def median_volume(volumes):
    """Mediana dei volumi non negativi (nessuna candela corrente inclusa)."""
    values = [max(0.0, float(v)) for v in volumes]
    return float(median(values)) if values else 0.0


def live_volume_progress(candles, now_ms, threshold):
    """Misura il volume cumulativo della candela LIVE senza anticipare il segnale.

    actual_ratio = volume gia' scambiato / mediana delle 20 precedenti CHIUSE.
    pace_ratio = proiezione lineare a fine 15m, solo INDICATIVA; non e' il
    volume definitivo, ne' un filtro di ingresso. La mediana e' fissata per
    tutta la durata della candela live e non include mai la live stessa.
    """
    if len(candles) < VOLUME_AVG_BARS_15M + 1:
        return None
    live = candles[-1]
    elapsed_ms = now_ms - live['t']
    if not 0 <= elapsed_ms < FIFTEEN_MIN_MS:
        return None
    previous = candles[-(VOLUME_AVG_BARS_15M + 1):-1]
    avg = median_volume(c['v'] for c in previous)
    if avg <= 0:
        return None
    elapsed = elapsed_ms / 1000.0
    actual_ratio = max(0.0, live['v']) / avg
    pace_ratio = (actual_ratio * TIMEFRAME_SECONDS / elapsed
                  if elapsed >= LIVE_VOLUME_PACE_MIN_SECONDS else None)
    return {
        'candle_t': live['t'],
        'elapsed_seconds': elapsed,
        'actual_ratio': actual_ratio,
        'pace_ratio': pace_ratio,
        'progress_percent': 100.0 * actual_ratio / threshold,
    }


def monitor_live_confirmation_volume(setup, candles, now_ms):
    """Solo log del volume live: durante la prima 15m diretta o dopo retest.

    Il segnale usa ESCLUSIVAMENTE il volume definitivo alla chiusura 15m.
    """
    if not candles:
        return
    first_direct = (setup.get('allow_direct') and not setup['touched']
                    and candles[-1]['t'] == setup['macro_end_t'])
    after_retest = (setup['touched'] and
                    candles[-1]['t'] > setup['touch_t'])
    if not (first_direct or after_retest):
        return
    threshold = strategy_for(setup['symbol'])['confirm_volume_min']
    progress = live_volume_progress(candles, now_ms, threshold)
    if not progress:
        return
    t = progress['candle_t']
    state = setup.get('live_volume_state')
    if state is None or state['candle_t'] != t:
        state = {
            'candle_t': t,
            'reached_final_volume_live': False,
            'next_log_elapsed': LIVE_VOLUME_PROGRESS_LOG_SECONDS,
        }
        setup['live_volume_state'] = state
    # Il dato viene aggiornato ogni secondo; i log sono meno frequenti.
    state['actual_ratio'] = progress['actual_ratio']
    state['pace_ratio'] = progress['pace_ratio']
    state['progress_percent'] = progress['progress_percent']
    if (progress['actual_ratio'] >= threshold and
            not state['reached_final_volume_live']):
        state['reached_final_volume_live'] = True
        log('VOLUME LIVE', setup['symbol'], setup['direction'],
            f"{progress['actual_ratio']:.2f}x su {threshold:.2f}x",
            '| SOGLIA RAGGIUNTA: si attende CHIUSURA 15m + altri filtri')
    if (progress['pace_ratio'] is not None and
            progress['elapsed_seconds'] >= state['next_log_elapsed']):
        state['next_log_elapsed'] = ((int(progress['elapsed_seconds']) //
                                      LIVE_VOLUME_PROGRESS_LOG_SECONDS) + 1) * LIVE_VOLUME_PROGRESS_LOG_SECONDS
        log('PROGRESSO VOLUME LIVE', setup['symbol'], setup['direction'],
            f"reale {progress['actual_ratio']:.2f}x",
            f"({progress['progress_percent']:.0f}% della soglia)",
            f"ritmo stimato {progress['pace_ratio']:.2f}x",
            '| conferma SOLO dopo chiusura 15m')


def macro_closed_candles(candles_15m, macro_ms):
    """Ricostruisce candele 1H/4H solo da blocchi completi di 15m CHIUSE."""
    required_count = macro_ms // FIFTEEN_MIN_MS
    groups = {}
    for candle in candles_15m[:-1]:  # l'ultima 15m e' LIVE
        block_t = (candle['t'] // macro_ms) * macro_ms
        groups.setdefault(block_t, []).append(candle)
    result = []
    for block_t, group in sorted(groups.items()):
        if len(group) != required_count:
            continue
        group.sort(key=lambda c: c['t'])
        if any(c['t'] != block_t + i * FIFTEEN_MIN_MS for i, c in enumerate(group)):
            continue
        result.append({
            't': block_t, 'o': group[0]['o'],
            'h': max(c['h'] for c in group), 'l': min(c['l'] for c in group),
            'c': group[-1]['c'], 'v': sum(c['v'] for c in group),
        })
    return result


def macro_tail_complete_and_consecutive(bars, count, macro_ms):
    """Controlla l'ultimo segmento necessario, senza bloccare per buchi remoti."""
    if len(bars) < count:
        return False
    tail = bars[-count:]
    return all(tail[i]['t'] + macro_ms == tail[i + 1]['t']
               for i in range(count - 1))


def entry_extension_1h(signal, candles):
    """Limite prezzo d'ingresso, indipendente dal timeframe del breakout.

    ATR14 calcolato sulle ultime 1H complete; se manca lo storico,
    il segnale e' scartato (mai ignorare il limite di estensione).
    """
    bars = macro_closed_candles(candles, ONE_HOUR_MS)
    if not macro_tail_complete_and_consecutive(bars, ATR_PERIOD + 1, ONE_HOUR_MS):
        return None
    atr1h = true_range_atr(bars)
    if atr1h is None or atr1h <= 0:
        return None
    sign = 1 if signal['direction'] == 'LONG' else -1
    extension = sign * (signal['entry'] - signal['breakout_level'])
    limit = ENTRY_MAX_DISTANCE_ATR1H * atr1h
    return {'allowed': extension <= limit + 1e-12,
            'extension': extension, 'limit': limit, 'atr1h': atr1h}


def record_btc1h_signal(signal, btc_candles):
    """Registra TP1 del BTC 1H realmente accodato, senza modificare i target."""
    if signal['symbol'] != 'BTCUSDT' or signal['strategy'] != '1H':
        return
    direction = signal['direction']
    risk = abs(signal['entry'] - signal['sl'])
    tp1 = signal['entry'] + (1 if direction == 'LONG' else -1) * TP1_R * risk
    current = btc_candles[-1]
    with alert_lock:
        if btc1h_timing['blocked']:
            # Il blocco esistente si libera SOLO su nuovo breakout con volume.
            return
        btc1h_timing['active_signal'] = {
            'direction': direction, 'tp1': tp1, 'sl': signal['sl'],
            'entry_bar_t': current['t'],
            # Non contare come TP1 o SL le wick precedenti all'entry.
            'initial_high': current['h'], 'initial_low': current['l'],
            'macro_t': signal['macro_t'],
        }
        _outbox_save_locked()
    log('BTC1H TIMING ARMATO', direction, '| TP1', price_format(tp1))


def btc1h_first_target(signal, btc_candles, now_ms):
    """Identifica se TP1 precede lo SL, senza attribuire wick pre-entry.

    Se TP1 e SL compaiono entrambi nella stessa candela senza ordine certo,
    il risultato e' ambiguo: non assumiamo che TP1 sia stato preso prima.
    """
    entry_bar_t = signal['entry_bar_t']
    direction = signal['direction']
    tp1, sl = signal['tp1'], signal['sl']
    for bar in btc_candles:
        if bar['t'] < entry_bar_t:
            continue
        is_entry_bar = bar['t'] == entry_bar_t
        is_live = bar is btc_candles[-1]
        high, low, price = bar['h'], bar['l'], bar['c']
        if direction == 'LONG':
            tp_touch = (high >= tp1 and
                        (not is_entry_bar or signal['initial_high'] < tp1))
            sl_touch = (low <= sl and
                        (not is_entry_bar or signal['initial_low'] > sl))
            tp_touch |= is_live and price >= tp1
            sl_touch |= is_live and price <= sl
        else:
            tp_touch = (low <= tp1 and
                        (not is_entry_bar or signal['initial_low'] > tp1))
            sl_touch = (high >= sl and
                        (not is_entry_bar or signal['initial_high'] < sl))
            tp_touch |= is_live and price <= tp1
            sl_touch |= is_live and price >= sl
        if tp_touch or sl_touch:
            outcome = ('AMBIGUO' if tp_touch and sl_touch else
                       'TP1' if tp_touch else 'SL')
            return outcome, now_ms if is_live else bar['t'] + FIFTEEN_MIN_MS
    return None


def btc1h_new_breakout_with_volume(btc_candles, blocked_at_ms):
    """Sblocco: nuova 1H chiusa oltre le ultime 15 precedenti, con volume medio.

    LONG: chiusura sopra il massimo delle 15 precedenti.
    SHORT: chiusura sotto il minimo delle 15 precedenti (simmetrico).
    Volume della candela di breakout > media aritmetica dei volumi delle
    15 candele 1H precedenti, tutte CHIUSE. Media ricalcolata a ogni nuova 1H.
    """
    bars = macro_closed_candles(btc_candles, ONE_HOUR_MS)
    n = STRATEGIES['1H']['lookback']
    if not macro_tail_complete_and_consecutive(bars, n + 1, ONE_HOUR_MS):
        return None
    curr = bars[-1]
    close_t = curr['t'] + ONE_HOUR_MS
    if close_t <= blocked_at_ms:
        return None
    prev = bars[-(n + 1):-1]
    avg_volume = sum(bar['v'] for bar in prev) / n
    if curr['v'] <= avg_volume:
        return None
    if curr['c'] > max(c['h'] for c in prev):
        return curr['t'], 'LONG'
    if curr['c'] < min(c['l'] for c in prev):
        return curr['t'], 'SHORT'
    return None


def update_btc1h_timing(btc_candles, now_ms):
    """Avanza il gate globale ALT: TP1 BTC1H -> blocco -> breakout nuovo."""
    with alert_lock:
        active = btc1h_timing['active_signal']
        if active and not btc1h_timing['blocked']:
            outcome = btc1h_first_target(active, btc_candles, now_ms)
            if outcome is not None:
                result, hit_time = outcome
                if result == 'TP1':
                    btc1h_timing['blocked'] = True
                    btc1h_timing['blocked_at_ms'] = hit_time
                    log('BTC1H TP1 RAGGIUNTO | ALT BLOCCATE', active['direction'],
                        '| TP1', price_format(active['tp1']))
                else:
                    btc1h_timing['active_signal'] = None
                    log('BTC1H TIMING: TP1 NON CONFERMATO', result,
                        '| nessun nuovo blocco ALT')
                _outbox_save_locked()
        if btc1h_timing['blocked']:
            blocked_at = btc1h_timing['blocked_at_ms']
            if not isinstance(blocked_at, (int, float)):
                # Stato incompleto: mai sbloccare senza un breakout verificato.
                return
            new_breakout = btc1h_new_breakout_with_volume(btc_candles, blocked_at)
            if new_breakout is not None:
                macro_t, direction = new_breakout
                if macro_t != btc1h_timing['last_unblock_macro_t']:
                    btc1h_timing['blocked'] = False
                    btc1h_timing['blocked_at_ms'] = None
                    btc1h_timing['last_unblock_macro_t'] = macro_t
                    btc1h_timing['active_signal'] = None
                    _outbox_save_locked()
                    log('BTC1H NUOVO BREAKOUT + VOLUME | ALT SBLOCCATE', direction,
                        '| candela 1H', macro_t)


def macro_structural_stop(candles_15m, direction, cfg):
    """SL strutturale del gruppo 1H/4H e ATR solo su macro candele chiuse."""
    bars = macro_closed_candles(candles_15m, cfg['macro_ms'])
    if len(bars) < max(cfg['sl_lookback'], ATR_PERIOD + 1):
        return None
    if not macro_tail_complete_and_consecutive(bars, ATR_PERIOD + 1, cfg['macro_ms']):
        return None
    atr = true_range_atr(bars)
    if atr is None or atr <= 0:
        return None
    structural = bars[-cfg['sl_lookback']:]
    live = candles_15m[-1]
    if direction == 'LONG':
        extreme = min(min(c['l'] for c in structural), live['l'])
        sl = extreme - cfg['sl_buffer_atr'] * atr
    else:
        extreme = max(max(c['h'] for c in structural), live['h'])
        sl = extreme + cfg['sl_buffer_atr'] * atr
    return {'sl': sl, 'atr_macro': atr, 'structure_extreme': extreme}


def btc15_direction(btc_candles):
    """BTC concorde oppure neutro; BTC opposto non neutro blocca il segnale.

    BTC e' neutro se il corpo live e' <0.35 ATR15 sulle sole chiuse.
    Se l'ATR non e' disponibile, restituisce None: niente segnali.
    Non richiede volume BTC, medie o conferme 1H.
    """
    if len(btc_candles) < ATR_PERIOD + 2:
        return None
    atr15 = true_range_atr(btc_candles[:-1])
    if atr15 is None or atr15 <= 0:
        return None
    live = btc_candles[-1]
    displacement = live['c'] - live['o']
    if abs(displacement) / atr15 < BTC_NEUTRAL_BODY_ATR_MAX:
        return 'NEUTRAL'
    return 'LONG' if displacement > 0 else 'SHORT'


def beyond_breakout(price, direction, level, buffer):
    return price >= level + buffer if direction == 'LONG' else price <= level - buffer


def macro_breakout(symbol, candles, now_ms, cfg, btc_candles=None):
    """Breakout 1H su 18 crypto e 4H sulle restanti 22, solo chiuse."""
    macro_bars = macro_closed_candles(candles, cfg['macro_ms'])
    required = max(ATR_PERIOD + 2, cfg['lookback'] + 1)
    if len(macro_bars) < required:
        return None
    macro = macro_bars[-1]
    close_ms = macro['t'] + cfg['macro_ms']
    if not (0 <= now_ms - close_ms <= MACRO_CLOSE_GRACE_SECONDS * 1000):
        return None
    # Solo le 'required' macro recenti devono essere complete/consecutive.
    # Un buco di dati remoto non deve bloccare tutti i nuovi breakout.
    if not macro_tail_complete_and_consecutive(macro_bars, required, cfg['macro_ms']):
        return None
    prev = macro_bars[:-1]
    direction = ('LONG' if macro['c'] > macro['o'] else
                 'SHORT' if macro['c'] < macro['o'] else None)
    if direction is None:
        return None
    atr_macro = true_range_atr(prev)
    if not atr_macro or atr_macro <= 0:
        return None
    level_bars = prev[-cfg['lookback']:]
    level = max(c['h'] for c in level_bars) if direction == 'LONG' else min(c['l'] for c in level_bars)
    buffer = cfg['breakout_buffer_atr'] * atr_macro
    if not beyond_breakout(macro['c'], direction, level, buffer):
        return None
    span = macro['h'] - macro['l']
    if span <= 0:
        return None
    close_pos = ((macro['c']-macro['l']) / span if direction == 'LONG'
                 else (macro['h']-macro['c']) / span)
    if close_pos < cfg['macro_close_position_min']:
        return None
    # Volume di breakout 1H/4H NON richiesto: nessuna soglia ne' booster.
    # Il controllo volume 15m di conferma resta invariato (>=0.60x mediana 20).
    if not macro_structural_stop(candles, direction, cfg):
        return None
    return {
        'symbol': symbol, 'direction': direction,
        'strategy': cfg['name'], 'macro_t': macro['t'],
        'macro_end_t': close_ms, 'level': level, 'macro_buffer': buffer,
        'atr_macro_at_breakout': atr_macro, 'macro_close_pos': close_pos,
        'allow_direct': close_pos >= DIRECT_CLOSE_POSITION_MIN,
        'last_checked_15m_t': None,
        'touched': False, 'touch_t': None, 'touched_price': None,
    }


def btc4h_closed_direction(btc_candles, confirmation_t):
    """Direzione dell'ultima 4H BTC completamente CHIUSA, solo per segnali 1H.

    NEUTRAL se il corpo 4H e' inferiore a 0.35 ATR4H calcolato sulle
    precedenti candele 4H chiuse. Dati mancanti/non allineati => None.
    Nessuna modifica al filtro BTC dei segnali 4H.
    """
    macro_bars = macro_closed_candles(btc_candles, FOUR_HOURS_MS)
    required = ATR_PERIOD + 2
    if not macro_tail_complete_and_consecutive(macro_bars, required, FOUR_HOURS_MS):
        return None
    last = macro_bars[-1]
    confirmation_end = confirmation_t + FIFTEEN_MIN_MS
    if last['t'] + FOUR_HOURS_MS > confirmation_end:
        return None
    atr4h = true_range_atr(macro_bars[:-1])
    if atr4h is None or atr4h <= 0:
        return None
    body = last['c'] - last['o']
    if abs(body) / atr4h < BTC_4H_NEUTRAL_BODY_ATR_MAX:
        return 'NEUTRAL'
    return 'LONG' if body > 0 else 'SHORT'


def btc15_closed_direction(btc_candles, confirmation_t):
    """BTC15 sulla candela appena CHIUSA, non sulla nuova LIVE quasi neutra."""
    if len(btc_candles) < ATR_PERIOD + 3 or btc_candles[-2]['t'] != confirmation_t:
        return None
    atr = true_range_atr(btc_candles[:-2])
    if not atr or atr <= 0:
        return None
    candle = btc_candles[-2]
    movement = candle['c'] - candle['o']
    if abs(movement) / atr < BTC_NEUTRAL_BODY_ATR_MAX:
        return 'NEUTRAL'
    return 'LONG' if movement > 0 else 'SHORT'


def relative_performance_4h(alt_candles, btc_candles, confirmation_t):
    """Rendimento relativo con 17 chiusure 15m sincronizzate su 4H esatte.

    confirmation_t e' il timestamp di APERTURA della 15m CHIUSA terminale.
    Si possono interrogare anche chiusure macro passate (e.g. breakout 4H),
    senza usare candele future ne' la live. Se mancano dati, None.
    """
    bars_4h = FOUR_HOURS_MS // FIFTEEN_MIN_MS

    def window_ending_at(candles):
        closed = candles[:-1]
        idx = next((i for i in range(len(closed)-1, -1, -1)
                    if closed[i]['t'] == confirmation_t), None)
        if idx is None or idx < bars_4h:
            return None
        win = closed[idx-bars_4h:idx+1]
        if any(win[i+1]['t']-win[i]['t'] != FIFTEEN_MIN_MS
               for i in range(bars_4h)):
            return None
        return win

    alt_window = window_ending_at(alt_candles)
    btc_window = window_ending_at(btc_candles)
    if alt_window is None or btc_window is None:
        return None
    if any(a['t'] != b['t'] for a, b in zip(alt_window, btc_window)):
        return None
    a0, a1 = alt_window[0]['c'], alt_window[-1]['c']
    b0, b1 = btc_window[0]['c'], btc_window[-1]['c']
    if min(a0, a1, b0, b1) <= 0:
        return None
    alt_perf = (a1 / a0 - 1.0) * 100.0
    btc_perf = (b1 / b0 - 1.0) * 100.0
    return {'alt_pct': alt_perf, 'btc_pct': btc_perf,
            'relative_pct': alt_perf - btc_perf}


def btc_direction_with_relative_exception(symbol, direction, btc_direction,
                                          alt_candles, btc_candles, confirmation_t):
    """Deroga BTC15 opposto solo con sovra/sottoperformance relativa >=5 pp.

    BTCUSDT mantiene la propria eccezione neutra, gestita dal chiamante.
    In assenza di dati affidabili resta attivo il filtro BTC15 originale.
    """
    if not ((direction == 'LONG' and btc_direction == 'SHORT') or
            (direction == 'SHORT' and btc_direction == 'LONG')):
        return btc_direction, None, False
    perf = relative_performance_4h(alt_candles, btc_candles, confirmation_t)
    if perf is None:
        return btc_direction, None, False
    relative = perf['relative_pct']
    if ((direction == 'LONG' and relative >= RELATIVE_BTC_OVERRIDE_PCT) or
            (direction == 'SHORT' and relative <= -RELATIVE_BTC_OVERRIDE_PCT)):
        log('ECCEZIONE BTC15', symbol, direction,
            '| BTC15 originale:', btc_direction,
            '| alt 4H:', f"{perf['alt_pct']:+.2f}%",
            '| BTC 4H:', f"{perf['btc_pct']:+.2f}%",
            '| forza relativa:', f'{relative:+.2f} pp',
            '| filtro BTC impostato NEUTRAL')
        return 'NEUTRAL', perf, True
    return btc_direction, perf, False


def confirmation_15m_volume_ratio(candles):
    """Volume della 15m appena CHIUSA vs MEDIANA delle 20 CHIUSE precedenti.

    candles[-1] e' la 15m LIVE appena iniziata, candles[-2] la conferma chiusa.
    Nessuna proiezione: si richiede volume reale sulla conferma.
    """
    if len(candles) < VOLUME_AVG_BARS_15M + 2:
        return 0.0
    confirmation = candles[-2]
    previous = candles[-(VOLUME_AVG_BARS_15M + 2):-2]
    avg = median_volume(c['v'] for c in previous)
    if avg <= 0:
        return 0.0
    return max(0.0, confirmation['v']) / avg


def confirmation_15m_valid(candle, direction, level, atr15, cfg):
    """Conferma 15m CHIUSA secondo i parametri specifici 1H oppure 4H."""
    if atr15 is None or atr15 <= 0:
        return False
    span = candle['h'] - candle['l']
    if span <= 0:
        return False
    opening, closing = candle['o'], candle['c']
    if direction == 'LONG' and closing <= opening:
        return False
    if direction == 'SHORT' and closing >= opening:
        return False
    if direction not in ('LONG', 'SHORT'):
        return False
    if not beyond_breakout(closing, direction, level, MICRO_CLOSE_BUFFER_ATR15 * atr15):
        return False
    body = abs(closing - opening)
    if body / span < cfg['body_fraction']:
        return False
    if body / atr15 < cfg['body_atr_min']:
        return False
    upper_wick = max(0.0, candle['h'] - max(opening, closing)) / span
    lower_wick = max(0.0, min(opening, closing) - candle['l']) / span
    leading = upper_wick if direction == 'LONG' else lower_wick
    trailing = lower_wick if direction == 'LONG' else upper_wick
    return leading <= cfg['wick_directional'] and trailing <= cfg['wick_opposite']


def build_confirmed_signal(setup, candles, btc_direction, candle_15m, now_ms):
    """Entry sul primo tick dopo la chiusura 15m e stop strutturale per gruppo."""
    if btc_direction not in (setup['direction'], 'NEUTRAL'):
        return None
    live = candles[-1]
    price = live['c']
    if price <= 0 or not beyond_breakout(
            price, setup['direction'], setup['level'],
            MICRO_CLOSE_BUFFER_ATR15 * setup['atr15_retest']):
        return None
    cfg = strategy_for(setup['symbol'])
    info = macro_structural_stop(candles, setup['direction'], cfg)
    if not info:
        return None
    risk = price - info['sl'] if setup['direction'] == 'LONG' else info['sl'] - price
    if risk <= 0:
        return None
    return {
        'symbol': setup['symbol'], 'direction': setup['direction'],
        'strategy': setup['strategy'], 'entry': price, 'sl': info['sl'],
        'atr_macro': info['atr_macro'], 'stop_distance_pct': risk / price,
        'macro_t': setup['macro_t'], 'macro_end_t': setup['macro_end_t'],
        'confirm_15m_t': candle_15m['t'], 'retest_t': setup['touch_t'],
        'entry_mode': setup.get('entry_mode', 'RETEST'),
        'breakout_level': setup['level'], 'breakout_buffer': setup['macro_buffer'],
        'confirm_volume_ratio': setup['confirm_volume_ratio'],
        'macro_close_pos': setup['macro_close_pos'],
        'btc_direction': btc_direction,
        'btc_override': setup.get('btc_override', False),
        'btc_relative_4h': setup.get('btc_relative_4h'),
    }


def confirm_after_closed_15m(setup, candles, btc_candles, closed, now_ms,
                             atr15, entry_mode):
    """Filtri originali identici per entrata DIRETTA o DOPO RETEST.

    Non anticipa la chiusura, richiede la candela intera 15m appena chiusa.
    """
    sym = setup['symbol']
    d = setup['direction']
    cfg = strategy_for(sym)
    if not confirmation_15m_valid(closed, d, setup['level'], atr15, cfg):
        span = closed['h'] - closed['l']
        body = abs(closed['c'] - closed['o'])
        log('CONFERMA SCARTATA', sym, entry_mode, '| prezzo/corpo/wick',
            '| corpo%', f'{100*body/span:.1f}' if span > 0 else 'n/d',
            '| corpoATR', f'{body/atr15:.2f}')
        return None
    ratio15 = confirmation_15m_volume_ratio(candles)
    if ratio15 < cfg['confirm_volume_min']:
        log('CONFERMA SCARTATA', sym, entry_mode, '| volume', f'{ratio15:.2f}x',
            '< soglia', f"{cfg['confirm_volume_min']:.2f}x", '| mediana 20')
        return None
    # BTC 1H ha raggiunto TP1: blocco globale delle altcoin 1H e 4H.
    if sym != 'BTCUSDT' and btc1h_timing['blocked']:
        log('CONFERMA SCARTATA', sym, entry_mode,
            '| BTC1H ha raggiunto TP1: attendo nuovo breakout 1H + volume')
        return None
    # NUOVO FILTRO ESCLUSIVO 1H: BTC 4H deve essere concorde o neutro.
    # BTC 4H opposto blocca SEMPRE il segnale 1H, senza deroghe +/-5pp.
    # I segnali 4H non passano da questo filtro e restano invariati.
    if cfg['name'] == '1H':
        btc4h_direction = btc4h_closed_direction(btc_candles, closed['t'])
        if btc4h_direction is None:
            return 'BTC_PENDING'
        if btc4h_direction not in (d, 'NEUTRAL'):
            log('CONFERMA SCARTATA', sym, entry_mode,
                '| BTC4H', btc4h_direction, 'opposto a', d, '| filtro SOLO 1H')
            return None
    btc_direction = ('NEUTRAL' if sym == 'BTCUSDT' and cfg['name'] == '1H'
                     else btc15_closed_direction(btc_candles, closed['t']))
    if sym != 'BTCUSDT' and btc_direction is None:
        # Mancano i dati allineati BTC15: riprova entro 30s, SENZA consumare la candela.
        return 'BTC_PENDING'
    original_btc_direction = btc_direction
    btc_perf = None
    btc_override = False
    if sym != 'BTCUSDT':
        btc_direction, btc_perf, btc_override = btc_direction_with_relative_exception(
            sym, d, btc_direction, candles, btc_candles, closed['t'])
    if btc_direction not in (d, 'NEUTRAL'):
        relative_note = (f"| forza relativa 4H {btc_perf['relative_pct']:+.2f} pp"
                         if btc_perf is not None else '| forza relativa 4H non disponibile')
        log('CONFERMA SCARTATA', sym, entry_mode, '| BTC15', original_btc_direction,
            'contro', d, relative_note)
        return None
    setup['btc_override'] = btc_override
    setup['btc_relative_4h'] = btc_perf
    setup['atr15_retest'] = atr15  # nome storico usato nel calcolo dell'entry
    setup['confirm_volume_ratio'] = ratio15
    setup['entry_mode'] = entry_mode
    signal = build_confirmed_signal(setup, candles, btc_direction, closed, now_ms)
    if signal:
        distance = entry_extension_1h(signal, candles)
        if distance is None:
            log('CONFERMA SCARTATA', sym, entry_mode,
                '| ATR1H per distanza entry non disponibile')
            return None
        if not distance['allowed']:
            log('CONFERMA SCARTATA', sym, entry_mode, '| ENTRY TROPPO LONTANA',
                f"{distance['extension']/distance['atr1h']:.2f} ATR1H",
                '| massimo', f'{ENTRY_MAX_DISTANCE_ATR1H:.2f} ATR1H')
            return None
        return signal
    log('CONFERMA SCARTATA', sym, entry_mode,
        '| primo prezzo post-chiusura o SL non valido')
    return None


def evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms):
    """Identica conferma V4: diretta prima 15m o dopo retest.

    La 15m puo' essere valutata piu' volte SOLO se BTC non e' ancora pronto.
    Nessuna conferma puo' essere inviata oltre 30s dalla sua chiusura.
    """
    if len(candles) < ATR_PERIOD + 3:
        return 'WAIT', None
    live, closed = candles[-1], candles[-2]
    closed_end = closed['t'] + FIFTEEN_MIN_MS
    sym = setup['symbol']
    cfg = strategy_for(sym)
    if live['t'] != closed_end:
        # In attesa di REST/WS: non eliminare setup sano a causa di un lag.
        return 'WAIT', None
    if closed['t'] < setup['macro_end_t']:
        return 'WAIT', None
    if now_ms - setup['macro_end_t'] > cfg['lifetime_seconds'] * 1000:
        log('SETUP SCADUTO', cfg['name'], sym)
        return 'EXPIRED', None
    if setup['last_checked_15m_t'] == closed['t']:
        return 'WAIT', None
    if now_ms < closed_end:
        return 'WAIT', None

    # Un retest e' valido per le candele successive anche se il feed arrivo' tardi.
    # Una CONFERMA di ingresso non puo' invece essere inviata fuori dalla finestra.
    level, d = setup['level'], setup['direction']
    touched = closed['l'] <= level <= closed['h']
    if touched and not setup['touched']:
        setup['touched'] = True
        setup['touch_t'] = closed['t']
        setup['touched_price'] = closed['l'] if d == 'LONG' else closed['h']
        log('RETEST 15M', sym, d, '| tocco livello senza limite ATR', price_format(level))

    if now_ms - closed_end > CLOSE_EVENT_MAX_DELAY_SECONDS * 1000:
        setup['last_checked_15m_t'] = closed['t']
        log('15M CHIUSA TARDIVA', sym, '| nessun alert vecchio',
            '| ritardo', round((now_ms - closed_end) / 1000), 'sec')
        return 'WAIT', None

    atr15 = true_range_atr(candles[:-2])
    if not atr15 or atr15 <= 0:
        # Se la cache e' in recupero, ritenta finche' siamo nella finestra.
        return 'WAIT', None

    if setup.get('allow_direct') and closed['t'] == setup['macro_end_t']:
        signal = confirm_after_closed_15m(
            setup, candles, btc_candles, closed, now_ms, atr15, 'DIRECT')
        if signal == 'BTC_PENDING':
            return 'WAIT', None
        if signal:
            setup['last_checked_15m_t'] = closed['t']
            return 'SIGNAL', signal

    if not setup['touched'] or closed['t'] <= setup['touch_t']:
        setup['last_checked_15m_t'] = closed['t']
        return 'WAIT', None

    signal = confirm_after_closed_15m(
        setup, candles, btc_candles, closed, now_ms, atr15, 'RETEST')
    if signal == 'BTC_PENDING':
        return 'WAIT', None
    setup['last_checked_15m_t'] = closed['t']
    return ('SIGNAL', signal) if signal else ('WAIT', None)


def price_format(price):
    if price >= 1000:
        return f'{price:.2f}'
    if price >= 1:
        return f'{price:.4f}'
    if price >= 0.01:
        return f'{price:.6f}'
    return f'{price:.9f}'


def message_for(s):
    """Messaggio Telegram compatto: solo direzione, coppia, timeframe e livelli."""
    cfg = strategy_for(s['symbol'])
    direction = s['direction']
    icon = '🟢' if direction == 'LONG' else '🔴'
    d = 1 if direction == 'LONG' else -1
    risk = abs(s['entry'] - s['sl'])
    tp1 = s['entry'] + d * TP1_R * risk
    tp2 = s['entry'] + d * TP2_R * risk
    tp3 = s['entry'] + d * TP3_R * risk
    pair = s['symbol'].replace('USDT', '/USDT')
    return (
        f"{icon} {direction} | {pair} | {cfg['name']}\n\n"
        f"Entry: {price_format(s['entry'])}\n"
        f"Stop Loss: {price_format(s['sl'])}\n"
        f"TP1: {price_format(tp1)}\n"
        f"TP2: {price_format(tp2)}\n"
        f"TP3: {price_format(tp3)}"
    )


def enqueue_alert(signal):
    """Accoda e salva l'alert prima dell'invio, non cambia i requisiti strategici."""
    key = (signal['symbol'], signal['macro_t'])
    message = message_for(signal)
    with alert_lock:
        if key in sent_alerts or key in pending_alerts or key in dead_letters:
            log('TELEGRAM DUPLICATO/ARCHIVIATO', key[0], key[1])
            return False
        created_at = time.time()
        record = {'symbol': key[0], 'macro_t': key[1],
                  'message': message, 'created_at': created_at}
        pending_alerts.add(key)
        persisted_pending[key] = record
        _outbox_save_locked()
        telegram_queue.put_nowait((key, message, created_at))
    log('TELEGRAM IN CODA', key[0], '| breakout', key[1])
    return True


def telegram_worker():
    """Tenta la consegna, riprende dopo 8 tentativi e conserva dead-letter.

    Non invia dopo 5 minuti: un ENTRY ormai vecchio sarebbe potenzialmente
    pericoloso. Il job fallito resta visibile nel file outbox.
    """
    while True:
        key, message, created_at = telegram_queue.get()
        delivered = False
        last_error = 'nessuna risposta positiva Telegram'
        try:
            attempt = 0
            while time.time() - created_at <= TELEGRAM_DELIVERY_WINDOW_SECONDS:
                attempt += 1
                try:
                    ok, retryable, retry_after = telegram_send_once(message)
                except Exception as exc:
                    log('TELEGRAM WORKER ECCEZIONE', key[0], repr(exc))
                    ok, retryable, retry_after = False, True, 0
                if ok:
                    delivered = True
                    with alert_lock:
                        sent_alerts.add(key)
                        persisted_pending.pop(key, None)
                        pending_alerts.discard(key)
                        dead_letters.pop(key, None)
                        _outbox_save_locked()
                    log('TELEGRAM OK', key[0], '| breakout', key[1],
                        '| tentativo', attempt)
                    break
                if not retryable:
                    last_error = 'Telegram ha rifiutato definitivamente la richiesta (HTTP 4xx)'
                    break
                if attempt >= TELEGRAM_MAX_ATTEMPTS:
                    # Un nuovo ciclo (non perdita definitiva dopo 8 tentativi).
                    delay = TELEGRAM_RETRY_ROUND_SECONDS
                    attempt = 0
                else:
                    delay = min(TELEGRAM_RETRY_MAX_SECONDS,
                                TELEGRAM_RETRY_INITIAL_SECONDS * (2 ** (attempt - 1)))
                delay = max(float(retry_after), delay)
                remaining = TELEGRAM_DELIVERY_WINDOW_SECONDS - (time.time() - created_at)
                if remaining <= 0 or delay >= remaining:
                    last_error = 'finestra consegna 5 minuti esaurita'
                    break
                log('TELEGRAM RETRY', key[0], '| prossimo tentativo fra',
                    round(delay), 'sec')
                time.sleep(delay)
            if not delivered:
                record = {'symbol': key[0], 'macro_t': key[1],
                          'message': message, 'created_at': created_at,
                          'reason': last_error, 'failed_at': time.time()}
                with alert_lock:
                    persisted_pending.pop(key, None)
                    pending_alerts.discard(key)
                    dead_letters[key] = record
                    failed_alert_times[key] = time.monotonic()
                    _outbox_save_locked()
                log('TELEGRAM NON CONSEGNATO - SALVATO IN OUTBOX FALLITI',
                    key[0], key[1], '|', last_error)
        finally:
            telegram_queue.task_done()


def snapshot_with_sources():
    """Ammette solo serie corrente e WS/REST effettivamente recente.

    Obsoleti = simboli non idonei alla scansione, mai conteggiati fra attivi.
    """
    now = time.monotonic()
    expected_t = (int(time.time() * 1000) // FIFTEEN_MIN_MS) * FIFTEEN_MIN_MS
    fresh_ws, fresh_rest, obsolete = 0, 0, 0
    snapshot = {}
    with cache_lock:
        for symbol in SYMBOLS:
            candles = market_cache.get(symbol)
            if (not candles or symbol in needs_refresh or
                    candles[-1]['t'] != expected_t):
                obsolete += 1
                continue
            ws_ok = (websocket_connected and
                     now - last_ws_receive.get(symbol, -1e9) <= WS_STALE_SECONDS)
            rest_ok = now - last_rest_receive.get(symbol, -1e9) <= REST_FRESH_SECONDS
            if ws_ok:
                fresh_ws += 1
            elif rest_ok:
                fresh_rest += 1
            else:
                obsolete += 1
                continue
            snapshot[symbol] = list(candles)
    return snapshot, fresh_ws, fresh_rest, obsolete


def current_snapshot():
    """Compatibilita' con la strategia di scansione preesistente."""
    return snapshot_with_sources()[0]


def scan_market():
    """18 simboli 1H e 22 simboli 4H. Nessun filtro di trading modificato."""
    snapshot, _, _, _ = snapshot_with_sources()
    if 'BTCUSDT' not in snapshot:
        return 0
    btc_candles = snapshot['BTCUSDT']
    now_ms = int(time.time() * 1000)
    if not (btc_candles[-1]['t'] <= now_ms < btc_candles[-1]['t'] + FIFTEEN_MIN_MS):
        return 0
    # Prima di processare qualsiasi ALT, aggiorna TP1 BTC e l'eventuale sblocco.
    update_btc1h_timing(btc_candles, now_ms)
    queued = 0
    for symbol, candles in snapshot.items():
        if candles[-1]['t'] != btc_candles[-1]['t']:
            continue
        cfg = strategy_for(symbol)
        bars = macro_closed_candles(candles, cfg['macro_ms'])
        if not bars:
            continue
        macro_t = bars[-1]['t']
        setup = active_setups.get(symbol)
        if setup and now_ms - setup['macro_end_t'] > cfg['lifetime_seconds'] * 1000:
            log('SETUP SCADUTO', symbol, cfg['name'])
            del active_setups[symbol]
            setup = None
        if seen_macro_bars.get(symbol) != macro_t:
            required = max(ATR_PERIOD + 2, cfg['lookback'] + 1)
            if macro_tail_complete_and_consecutive(bars, required, cfg['macro_ms']):
                # Segna il breakout gia' analizzato soltanto quando i dati sono pronti.
                if not setup:
                    candidate = macro_breakout(symbol, candles, now_ms, cfg, btc_candles)
                    if candidate:
                        active_setups[symbol] = candidate
                        setup = candidate
                        log('BREAKOUT', cfg['name'], symbol, candidate['direction'],
                            '| volume macro NON richiesto',
                            '| diretto', candidate['allow_direct'])
                seen_macro_bars[symbol] = macro_t
            # Il vecchio setup resta attivo; NON usare 'continue' qui.
        setup = active_setups.get(symbol)
        if not setup:
            continue
        status, signal = evaluate_closed_retest_candle(setup, candles, btc_candles, now_ms)
        if status == 'EXPIRED':
            del active_setups[symbol]
        elif status == 'SIGNAL' and signal:
            if enqueue_alert(signal):
                queued += 1
                if symbol == 'BTCUSDT' and cfg['name'] == '1H':
                    record_btc1h_signal(signal, btc_candles)
                del active_setups[symbol]
                log('SEGNALE IN CODA', cfg['name'], signal['entry_mode'],
                    symbol, signal['direction'])
            else:
                log('SEGNALE NON ACCODATO', symbol, '| controllare outbox')
        if symbol in active_setups:
            monitor_live_confirmation_volume(setup, candles, now_ms)
    oldest_allowed = now_ms - 4 * FOUR_HOURS_MS
    with alert_lock:
        recent = {k for k in sent_alerts if k[1] >= oldest_allowed}
        if recent != sent_alerts:
            sent_alerts.intersection_update(recent)
            _outbox_save_locked()
    return queued


def main():
    log('MOMENTUM ONE V5 SENZA VOLUME BREAKOUT | 40 crypto | 18x1H + 22x4H | scan ~1s')
    log('1H: lookback 15, volume breakout NON richiesto, conferma 15m 0.60x, setup 6h')
    log('4H: lookback 6, volume breakout NON richiesto, conferma 15m 0.60x, setup 12h')
    log('INGRESSO DIRETTO >=80% e prima 15m chiusa, oppure RETEST senza ATR')
    log('Volume macro e booster volume NON applicati | BTC deroga +/-5pp invariata')
    log('FIX TECNICI: macro complete | feed attuali | BTC retry 30s | outbox Telegram')
    if not BOT_TOKEN or not CHAT_ID:
        log('ATTENZIONE: mancano variabili Telegram')
    load_telegram_outbox()
    while True:
        try:
            initialize_history()
            break
        except Exception as exc:
            log('Storico Binance non disponibile:', exc, 'riprovo 20s')
            time.sleep(20)
    threading.Thread(target=telegram_worker, daemon=True).start()
    threading.Thread(target=history_worker, daemon=True).start()
    threading.Thread(target=websocket_worker, daemon=True).start()
    if send_telegram('MOMENTUM ONE V5 SENZA VOLUME BREAKOUT AVVIATO [SIMULAZIONE] - '
                     '40 crypto (18x1H / 22x4H), lookback 15/6, nessun volume 1H/4H, '
                     'conferma 15m 0.60x, direct/retest, nessun ordine automatico'):
        log('TELEGRAM AVVIO OK')
    else:
        log('TELEGRAM AVVIO NON CONSEGNATO - controllare token/chat/rete')
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
            snap, ws_count, rest_count, obsolete = snapshot_with_sources()
            b = btc15_direction(snap['BTCUSDT']) if 'BTCUSDT' in snap else 'DATI NON FRESCHI'
            with alert_lock:
                in_delivery = len(pending_alerts)
                failures = len(dead_letters)
            s1 = sum(1 for s in active_setups if s in ONE_HOUR_SYMBOLS)
            s4 = sum(1 for s in active_setups if s in FOUR_HOUR_SYMBOLS)
            log('Scansione ~1s | feed attivi:', len(snap), '/40',
                f'| WS: {ws_count} | REST: {rest_count} | obsoleti: {obsolete}',
                '| BTC15:', b, '| setup 1H:', s1, '| setup 4H:', s4,
                '| Telegram in coda:', in_delivery, '| Telegram falliti:', failures)
            last_log = now
        time.sleep(max(0.0, next_scan - time.monotonic()))
        if next_scan < time.monotonic() - SCAN_SECONDS:
            next_scan = time.monotonic()


if __name__ == '__main__':
    main()
