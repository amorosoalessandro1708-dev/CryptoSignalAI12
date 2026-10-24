import os
import time
import threading
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
import requests

# V9.9.3: BTC15 TREND >=0.20 ATR + volume LIVE progressivo obbligatorio
#          (solo TREND); scansioni REST senza pausa aggiuntiva oltre 0.2 s.
#          SCAN_SECONDS=1 e' un obiettivo, NON garantisce un ciclo in 1 s.
# V9.9.2: SOLO TREND: corpo >=80% del range e wick per lato <=15%.
# V9.9.1: diagnostica ampliata SOLO INFORMATIVA; strategia identica alla V9.9.0.
# V9.9.0: FAST identico alla V9.8.9 (volume A progressivo o B sostenuto >=2.50x).
# TREND: tre candele direzionali quasi Marubozu, corpo >=0.35 ATR15 ciascuna;
# corpo >=80% del range e wick SUPERIORE/INFERIORE <=15% del range ciascuna.
# Terza LIVE >=80% della media dei primi due corpi e volume progressivo A.
# Nessun requisito di volume sulle prime due candele TREND.
# Segnali informativi: strategia non validata, testare in simulazione.
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
SYMBOLS = [
    'SOLUSDT','BTCUSDT','ETHUSDT','XRPUSDT','BNBUSDT','DOGEUSDT',
    'AVAXUSDT','SUIUSDT','LINKUSDT','ADAUSDT','NEARUSDT','UNIUSDT',
    'LTCUSDT','DOTUSDT','APTUSDT','ARBUSDT','OPUSDT','INJUSDT',
    'RENDERUSDT','1000PEPEUSDT','FILUSDT','ETCUSDT','AAVEUSDT',
    'TIAUSDT','SEIUSDT'
]
KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines'
SCAN_SECONDS = 1  # target; ogni scansione REST delle 25 coppie dura diversi secondi
SCAN_WORKERS = 6
CACHE_30M_SECONDS = 30
CACHE_1H_SECONDS = 60
CACHE_4H_SECONDS = 300
MIN_REST_GAP_SECONDS = .30
RETRY_FALLBACK_SECONDS = (5,10,20,30)
MAX_RETRIES = 4
DIAGNOSTIC_EVERY_SECONDS = 60
# Breve conferma, senza attesa di 5 minuti
CHECK_MIN_SECONDS = 20
CHECK_MAX_SECONDS = 55  # margine per scansioni da 7-14s; conferma appena >=20s
CHECK_MAX_GAP_SECONDS = 45
CHECK_LEVEL_TOLERANCE_ATR = .08  # oscillazione moderata attorno al livello
CHECK_MIN_PROGRESS_ATR = .015
BREAKOUT_MIN_ATR = .02
BREAKOUT_START_MAX_EXTENSION_ATR = .45
CHECK_MAX_EXTENSION_ATR = .75
BREAKOUT_VOLUME_PACE_MIN = 1.20
BREAKOUT_BODY_ATR_MIN = .15  # conferma finale: candela non eccessivamente debole
BREAKOUT_CLOSE_POSITION_MIN = .60
BREAKOUT_WICK_MAX = .85
MIN_CANDLE_ELAPSED_SECONDS = 15  # solo alla conferma; breakout rilevato anche prima
LIVE_VOLUME_MIN_ELAPSED_SECONDS = 15
LIVE_VOLUME_PACE_CAP = 20.
CLOSED_VOLUME_PREFERRED = 1.10
MIN_CLOSED_VOLUME_RATIO = .80  # soglia sulla candela chiusa precedente, SOLO per FAST
STRUCTURE_BARS = 6
PULLBACK_TOUCH_TOLERANCE_ATR = .35
PULLBACK_TRIGGER_BARS = 3
PULLBACK_LOOKBACK_BARS = 5
RESISTANCE_LOOKBACK_BARS = 48
MIN_ROOM_R = 1.20
MIN_QUALITY = 9
STOCH_K_PERIOD = 14
STOCH_K_SMOOTH = 3
STOCH_D_PERIOD = 3
BTC15_DIRECTIONAL_BODY_ATR_MIN = .20  # TREND BTC15: corpo LIVE >=0.20 ATR15
BTC30_DIRECTIONAL_BODY_ATR_MIN = .15  # 30m solo informativo: invariato
BTC15_STRONG_OPPOSITE_BODY_ATR_MIN = .45  # blocca solo corpo BTC15 opposto >= 0.45 ATR15
# V9.7: stop strutturale ampliato e filtro continuazione su due candele.
SL_STRUCTURE_LOOKBACK_BARS = 12      # struttura 15m piu ampia, solo candele chiuse
SL_1H_LOOKBACK_BARS = 5            # controllo livelli 1h recenti, solo candele chiuse
SL_BUFFER_STRONG_TREND_ATR15 = .50
SL_BUFFER_NORMAL_TREND_ATR15 = .45
SL_BUFFER_NEUTRAL_4H_ATR15 = .30
SL_MIN_DISTANCE_ATR15 = 1.20
SL_MAX_DISTANCE_ATR15 = 4.50       # limite massimo, non stop obbligatorio
# V9.8.9: volume LIVE progressivo A; soglia 2.50x SOLO per il FAST B.
# Il minimo dinamico deriva da media e mediana delle ultime 20 candele chiuse.
# Per accelerare, la velocita' degli scambi deve aumentare fra due intervalli
# temporali consecutivi osservati (NON il volume cumulativo, che sale sempre).
# Il volume resta un filtro: una crescita da valori troppo bassi non e' valida.
VOLUME_SAMPLE_MIN_GAP_SECONDS = 4.0
VOLUME_SAMPLE_MAX_GAP_SECONDS = 60.0
VOLUME_SAMPLE_LOOKBACK_SECONDS = 120.0
VOLUME_ACCEL_MIN_GROWTH = .12       # accelerazione di almeno +12%, NON soglia di volume
VOLUME_RECENT_RATE_FLOOR_PART = .75 # recente >=75% della velocita' storica dinamica
# SOLO FAST, opzione B: volume molto alto e stabile (nessun segnale separato).
FAST_SUSTAINED_VOLUME_MULTIPLIER = 2.50 # intensita >=2.50x vs media+mediana storico
FAST_SUSTAINED_MAX_RATE_DROP = .15   # calo velocita' max 15% fra due intervalli
FAST_BODY_ATR_MIN = .70
FAST_BODY_FRACTION_MIN = .80  # corpo almeno 80% del range totale
FAST_WICK_MAX = .35
FAST_MAX_EXTENSION_ATR = 1.40
FAST_BTC15_BODY_ATR_MIN = .30
# TREND: 3 candele quasi Marubozu, sia per LONG che per SHORT.
# Corpo >=0.35 ATR15, pienezza >=80%, due wick <=15% del range ciascuna.
# Restano controllate sia la wick superiore che quella inferiore.
TREND_PREV_BODY_ATR_MIN = .35
TREND_PREV_BODY_FRACTION_MIN = .80
TREND_BODY_ATR_MIN = .35
TREND_BODY_FRACTION_MIN = .80
TREND_UPPER_WICK_MAX_RANGE = .15
TREND_LOWER_WICK_MAX_RANGE = .15
TREND_THIRD_MIN_VS_PREV_AVG = .80  # terza live >=80% della media dei due corpi
TREND_MAX_EXTENSION_ATR = 2.20
TREND_BTC15_BODY_ATR_MIN = .20
# BTC15 puo' essere NEUTRAL solo se non supera la soglia di forza;
# per entrambi gli ingressi richiediamo invece direzione esplicita.
# Minimo spazio tecnico resta 1.20R; leva early <= 15x.
TP_MIN_NET_PCT = .0015
LEVERAGE_SAFETY = .35
LEVERAGE_STEPS = (1,2,3,5,10,15,20,25,30,40,50,75,100)

request_lock = threading.Lock()
cache_lock = threading.Lock()
last_rest_request = 0.
market_cache = {}
pending = {}
last_observed = {}
signal_state = {}
last_diag = 0.
diagnostic_totals = Counter()
diagnostic_since_last = Counter()  # finestra circa 60s, solo visibilita' dei motivi
# Campionamento in RAM per singola coppia; un riavvio richiede alcune scansioni
# prima di avere due intervalli confrontabili sulla candela live.
live_volume_samples = {}
volume_samples_lock = threading.Lock()

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print('Telegram non configurato')
        return False
    try:
        r = requests.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
                          data={'chat_id':CHAT_ID,'text':message}, timeout=20)
        r.raise_for_status()
        return bool(r.json().get('ok'))
    except Exception as exc:
        print('Errore Telegram:', exc)
        return False

def get_json(url, params):
    global last_rest_request
    error = None
    for attempt in range(MAX_RETRIES):
        with request_lock:
            wait = MIN_REST_GAP_SECONDS - (time.monotonic() - last_rest_request)
            if wait > 0:
                time.sleep(wait)
            last_rest_request = time.monotonic()
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code in (418,429):
                try:
                    delay = max(5.,float(r.headers.get('Retry-After')))
                except (TypeError,ValueError):
                    delay = RETRY_FALLBACK_SECONDS[min(attempt,3)]
                print('Rate limit Binance',r.status_code,'attesa',delay)
                time.sleep(delay)
                error = RuntimeError(f'HTTP {r.status_code}')
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            error = exc
            if attempt < MAX_RETRIES-1:
                time.sleep(RETRY_FALLBACK_SECONDS[min(attempt,3)])
    raise error or RuntimeError('Binance non disponibile')

def parse_candles(raw):
    return [{'o':float(x[1]),'h':float(x[2]),'l':float(x[3]),
             'c':float(x[4]),'v':float(x[5]),'t':int(x[0])} for x in raw]

def cached_candles(symbol, interval, lifetime):
    key = (symbol,interval)
    with cache_lock:
        cached = market_cache.get(key)
        if cached and time.time()-cached['time'] < lifetime:
            return cached['data']
    data = parse_candles(get_json(KLINES_URL,{'symbol':symbol,'interval':interval,'limit':120}))
    with cache_lock:
        market_cache[key] = {'time':time.time(),'data':data}
    return data

def market_data(symbol):
    return {'15m':parse_candles(get_json(KLINES_URL,{'symbol':symbol,'interval':'15m','limit':120})),
            '1h':cached_candles(symbol,'1h',CACHE_1H_SECONDS),
            '4h':cached_candles(symbol,'4h',CACHE_4H_SECONDS)}

def ema(values, period):
    if len(values)<period:
        return None
    result = sum(values[:period])/period
    alpha = 2/(period+1)
    for value in values[period:]:
        result = alpha*value+(1-alpha)*result
    return result

def atr(candles, period=14):
    if len(candles)<period+1:
        return None
    ranges = [max(candles[i]['h']-candles[i]['l'],
                  abs(candles[i]['h']-candles[i-1]['c']),
                  abs(candles[i]['l']-candles[i-1]['c'])) for i in range(1,len(candles))]
    return sum(ranges[-period:])/period

def live_volume_ratio(candles):
    if len(candles)<22:
        return 0.,0.
    avg = sum(x['v'] for x in candles[-21:-1])/20
    live = candles[-1]
    elapsed = max(0.,min((time.time()*1000-live['t'])/1000,900.))
    if avg<=0 or elapsed<LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return 0.,elapsed
    return min(live['v']/(avg*max(elapsed/900.,.01)),LIVE_VOLUME_PACE_CAP),elapsed

def progressive_volume_metrics(symbol, candles, now):
    """Controlla volume LIVE senza soglie x fisse, usando due campioni di velocita'.

    - Livello: proiezione della candela 15m vs media+mediana volumi storici.
    - Accelerazione: velocita' degli scambi nell'ultimo intervallo vs precedente.
    - FAST B: volume >=2.50x e ritmo alto in entrambe le finestre consecutive
      senza rallentare oltre il 15%; solo FAST, non TREND.
    - TREND: verifica esclusivamente il ritmo progressivo della terza LIVE;
      nessun vincolo su volume delle prime due candele chiuse.
    Tutti i campioni si riferiscono ALLA STESSA candela Binance, senza
    interpolare tra chiusura e apertura di due candele differenti.
    """
    result = {'ready': False, 'accel_ok': False, 'intensity_ok': False,
              'sustained_ok': False,
              'pace': 0., 'relative_pace': 0.,
              'dynamic_floor': 0., 'growth': 0., 'recent_rate': 0.,
              'prior_rate': 0., 'projected': 0.}
    if len(candles) < 22:
        return result

    live = candles[-1]
    elapsed = max(0., min((time.time()*1000-live['t'])/1000., 900.))
    prior = [max(0., x['v']) for x in candles[-21:-1]]
    avg = sum(prior)/len(prior)
    sorted_prior = sorted(prior)
    median = (sorted_prior[9]+sorted_prior[10])/2.
    reference = (avg+median)/2.
    # Memorizza anche i campioni iniziali (<15s), senza usarli per segnalare.
    # Usare tempo monotonic per i delta, tempo UNIX solo per il minuto candela.
    with volume_samples_lock:
        state = live_volume_samples.get(symbol)
        if (state is None or state['candle_time'] != live['t'] or
                (state['samples'] and live['v'] < state['samples'][-1][1])):
            state = {'candle_time': live['t'], 'samples': deque(maxlen=16)}
            live_volume_samples[symbol] = state
        samples = state['samples']
        while samples and now-samples[0][0] > VOLUME_SAMPLE_LOOKBACK_SECONDS:
            samples.popleft()
        if not samples or now-samples[-1][0] >= VOLUME_SAMPLE_MIN_GAP_SECONDS:
            samples.append((now, max(0., live['v'])))
        points = tuple(samples)

    if avg <= 0 or reference <= 0 or elapsed < LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        return result

    # Proiezione su 900s con il volume accumulato nei secondi effettivi.
    projected = max(0., live['v'])*900./elapsed
    pace = min(projected/avg, LIVE_VOLUME_PACE_CAP)
    dyn_floor = reference/avg
    relative = projected/reference
    result.update(pace=pace, dynamic_floor=dyn_floor,
                  relative_pace=relative, projected=projected,
                  intensity_ok=(projected >= reference))

    # Minimo 3 osservazioni e 2 finestre temporali comparabili.
    if len(points) < 3:
        return result
    t0,v0 = points[-3]
    t1,v1 = points[-2]
    t2,v2 = points[-1]
    dt_prev,dt_new = t1-t0,t2-t1
    if (not (VOLUME_SAMPLE_MIN_GAP_SECONDS <= dt_prev <= VOLUME_SAMPLE_MAX_GAP_SECONDS)
            or not (VOLUME_SAMPLE_MIN_GAP_SECONDS <= dt_new <= VOLUME_SAMPLE_MAX_GAP_SECONDS)):
        return result
    prev_rate = max(0., v1-v0)/dt_prev
    new_rate = max(0., v2-v1)/dt_new
    # La crescita percentuale e' significativa solo se il nuovo ritmo e'
    # abbastanza elevato in assoluto rispetto alla storia della coppia.
    historical_rate = reference/900.
    # Se il ritmo precedente era zero, usiamo una base piccola ma non nulla
    # derivata dallo storico: evita valori infiniti nel messaggio Telegram.
    growth = min(3., new_rate/max(prev_rate, historical_rate*.10)-1.) if new_rate>0 else 0.
    accel_ok = (new_rate > 0 and new_rate >= prev_rate*(1.+VOLUME_ACCEL_MIN_GROWTH)
                and new_rate >= historical_rate*VOLUME_RECENT_RATE_FLOOR_PART)
    # Via B: almeno 2.50x dello storico sia sul totale live proiettato,
    # sia SU ENTRAMBI gli intervalli consecutivi misurati. Il ritmo puo'
    # essere stabile o calare fino al 15%, ma non deve crollare.
    # Un picco isolato di una sola scansione NON conferma il volume sostenuto.
    sustained_min_rate = historical_rate*FAST_SUSTAINED_VOLUME_MULTIPLIER
    sustained_ok = (relative >= FAST_SUSTAINED_VOLUME_MULTIPLIER and
                    prev_rate >= sustained_min_rate and
                    new_rate >= sustained_min_rate and
                    new_rate >= prev_rate*(1.-FAST_SUSTAINED_MAX_RATE_DROP))
    result.update(ready=True, accel_ok=accel_ok, sustained_ok=sustained_ok,
                  growth=growth,
                  recent_rate=new_rate, prior_rate=prev_rate)
    return result

def closed_volume_ratio(candles):
    if len(candles)<22:
        return 0.
    avg = sum(x['v'] for x in candles[-22:-2])/20
    return candles[-2]['v']/avg if avg>0 else 0.

def candle_metrics(c, direction, a15):
    body=abs(c['c']-c['o'])
    span=c['h']-c['l']
    if body<=0 or span<=0 or a15<=0:
        return None
    if direction=='LONG':
        if c['c']<=c['o']:
            return None
        pos=(c['c']-c['l'])/span
        wick=max(0.,c['h']-c['c'])/body
    else:
        if c['c']>=c['o']:
            return None
        pos=(c['h']-c['c'])/span
        wick=max(0.,c['c']-c['l'])/body
    upper_wick=max(0., c['h']-max(c['o'], c['c']))/span
    lower_wick=max(0., min(c['o'], c['c'])-c['l'])/span
    return {'body':body/a15,'close':pos,'wick':wick,'body_fraction':body/span,
            'upper_wick_range':upper_wick,'lower_wick_range':lower_wick}

def trend_shape_ok(m):
    """Entrambe le wick <=15% del range, LONG e SHORT uguali."""
    return (m is not None and
            m['body_fraction']>=TREND_BODY_FRACTION_MIN and
            m['upper_wick_range']<=TREND_UPPER_WICK_MAX_RANGE and
            m['lower_wick_range']<=TREND_LOWER_WICK_MAX_RANGE)

def trend_1h(candles):
    closes=[x['c'] for x in candles[:-1]]
    e20,e50,prev=ema(closes,20),ema(closes,50),ema(closes[:-1],20)
    if None in (e20,e50,prev):
        return 'NEUTRAL'
    price=candles[-2]['c']
    if price>e20>e50 and e20>=prev:
        return 'LONG'
    if price<e20<e50 and e20<=prev:
        return 'SHORT'
    return 'NEUTRAL'

def context_4h(candles):
    closes=[x['c'] for x in candles[:-1]]
    e20,e50=ema(closes,20),ema(closes,50)
    if e20 is None or e50 is None:
        return 'NEUTRAL'
    if candles[-2]['c']>e20>e50:
        return 'LONG'
    if candles[-2]['c']<e20<e50:
        return 'SHORT'
    return 'NEUTRAL'

def btc_bias(data):
    one,four=data['1h'],data['4h']
    close1=[x['c'] for x in one[:-1]]
    close4=[x['c'] for x in four[:-1]]
    a,b=ema(close1,20),ema(close1,50)
    c,d=ema(close4,20),ema(close4,50)
    prev=ema(close1[:-1],20)
    if None in (a,b,c,d,prev):
        return 'NEUTRAL_STABLE'
    if one[-2]['c']>a>b and a>prev and four[-2]['c']>c:
        return 'LONG_STRONG'
    if one[-2]['c']<a<b and a<prev and four[-2]['c']<c:
        return 'SHORT_STRONG'
    if one[-2]['c']>a and a>prev:
        return 'LONG_LIGHT'
    if one[-2]['c']<a and a<prev:
        return 'SHORT_LIGHT'
    return 'NEUTRAL_STABLE'

def btc_30m_direction(candles):
    if len(candles)<22:
        return 'NEUTRAL'
    a=atr(candles[:-1])
    if not a:
        return 'NEUTRAL'
    live=candles[-1]
    direction='LONG' if live['c']>live['o'] else 'SHORT'
    metrics=candle_metrics(live,direction,a)
    return direction if metrics and metrics['body']>=BTC30_DIRECTIONAL_BODY_ATR_MIN else 'NEUTRAL'

def btc15_direction(data):
    c=data['15m']
    a=atr(c[:-1])
    pace,_=live_volume_ratio(c)
    result={'directional':'NEUTRAL','body_atr':0.,'volume_pace':pace}
    if not a:
        return result
    live=c[-1]
    direction='LONG' if live['c']>live['o'] else 'SHORT'
    m=candle_metrics(live,direction,a)
    if m:
        result['body_atr']=m['body']
        if m['body']>=BTC15_DIRECTIONAL_BODY_ATR_MIN:
            result['directional']=direction
    return result

def stochastic(candles):
    if len(candles)<STOCH_K_PERIOD+STOCH_K_SMOOTH+STOCH_D_PERIOD-2:
        return None
    fast=[]
    for end in range(STOCH_K_PERIOD-1,len(candles)):
        window=candles[end-STOCH_K_PERIOD+1:end+1]
        high=max(x['h'] for x in window)
        low=min(x['l'] for x in window)
        fast.append(50. if high<=low else 100*(candles[end]['c']-low)/(high-low))
    slow=[sum(fast[i-STOCH_K_SMOOTH+1:i+1])/STOCH_K_SMOOTH
          for i in range(STOCH_K_SMOOTH-1,len(fast))]
    if len(slow)<STOCH_D_PERIOD:
        return None
    return slow[-1],sum(slow[-STOCH_D_PERIOD:])/STOCH_D_PERIOD

def signed(value,direction):
    return value if direction=='LONG' else -value

def aligned_ema(c15,direction):
    closes=[x['c'] for x in c15]
    e20=ema(closes,20)
    prev=ema(closes[:-1],20)
    if e20 is None or prev is None:
        return False
    return (closes[-1]>e20 and e20>prev) if direction=='LONG' else (closes[-1]<e20 and e20<prev)

def opportunity_level(c15,direction,a15):
    closed=c15[:-1]
    long=direction=='LONG'
    structure=closed[-STRUCTURE_BARS:]
    breakout=max(x['h'] for x in structure) if long else min(x['l'] for x in structure)
    levels=[('BREAKOUT',breakout)]
    e20=ema([x['c'] for x in closed],20)
    if e20 is None:
        return levels
    recent=closed[-PULLBACK_LOOKBACK_BARS:]
    touch=(min(x['l'] for x in recent)<=e20+PULLBACK_TOUCH_TOLERANCE_ATR*a15
           if long else max(x['h'] for x in recent)>=e20-PULLBACK_TOUCH_TOLERANCE_ATR*a15)
    recovery=closed[-1]['c']>e20 if long else closed[-1]['c']<e20
    if touch and recovery:
        trigger=closed[-PULLBACK_TRIGGER_BARS:]
        level=max(x['h'] for x in trigger) if long else min(x['l'] for x in trigger)
        if abs(level-breakout)>.02*a15:
            levels.append(('RIPRESA TREND',level))
    return levels

# Stop strutturale invariato dalla V9.8.7.
def intelligent_stop(direction,entry,level,live,a15,recent,ctx,bias,one_hour):
    if ctx==direction and bias.startswith(direction):
        buffer=SL_BUFFER_STRONG_TREND_ATR15
    elif ctx=='NEUTRAL':
        buffer=SL_BUFFER_NEUTRAL_4H_ATR15
    else:
        buffer=SL_BUFFER_NORMAL_TREND_ATR15

    closed15=recent[-SL_STRUCTURE_LOOKBACK_BARS:]
    closed1h=one_hour[:-1][-SL_1H_LOOKBACK_BARS:]
    if not closed15:
        return None
    margin=a15*buffer
    maximum=a15*SL_MAX_DISTANCE_ATR15

    if direction=='LONG':
        support15=min([live['l'],level]+[x['l'] for x in closed15])
        structural=support15-margin
        # Il supporto 1h si usa solo se e' sotto l'entry e rientra nel rischio massimo.
        if closed1h:
            support1h=min(x['l'] for x in closed1h)
            candidate=support1h-margin
            if 0 < entry-candidate <= maximum:
                structural=min(structural,candidate)
        raw=entry-structural
    else:
        resistance15=max([live['h'],level]+[x['h'] for x in closed15])
        structural=resistance15+margin
        # La resistenza 1h si usa solo se e' sopra l'entry e rientra nel rischio massimo.
        if closed1h:
            resistance1h=max(x['h'] for x in closed1h)
            candidate=resistance1h+margin
            if 0 < candidate-entry <= maximum:
                structural=max(structural,candidate)
        raw=structural-entry

    if raw<=0 or raw>maximum:
        return None
    distance=max(raw,a15*SL_MIN_DISTANCE_ATR15)
    if distance>maximum:
        return None
    return entry-distance if direction=='LONG' else entry+distance

def available_room(c15,direction,entry,risk):
    history=c15[:-1]
    older=history[-(RESISTANCE_LOOKBACK_BARS+STRUCTURE_BARS):-STRUCTURE_BARS]
    if direction=='LONG':
        levels=[x['h'] for x in older if x['h']>entry]
        return min(levels)-entry if levels else float('inf')
    levels=[x['l'] for x in older if x['l']<entry]
    return entry-max(levels) if levels else float('inf')

def clamp(v,lo,hi):
    return max(lo,min(hi,v))

def adaptive_targets(direction,entry,stop,a15,quality,pace,body,follow,ctx,bias):
    risk=abs(entry-stop)
    if risk<=0:
        return None
    strength=clamp((pace-1.2)/2.5,0,1)*.30
    strength+=clamp((body-.16)/.70,0,1)*.25
    strength+=clamp((follow-.06)/.60,0,1)*.20
    strength+=clamp((quality-7)/6,0,1)*.15
    strength+=.05*int(ctx==direction)+.05*int(bias.startswith(direction))
    strength=clamp(strength,0,1)
    adjusted=clamp(strength*(1-.35*clamp((body-1.2)/.35,0,1)),0,1)
    d1=max(risk*.30,entry*TP_MIN_NET_PCT)
    # Mantieni TP2 e TP3 IDENTICI alla V9.8.6 anche quando scatta il
    # pavimento TP_MIN_NET_PCT: usa la vecchia distanza TP1 solo per lo spacing.
    previous_tp1_distance=max(risk*.35,entry*TP_MIN_NET_PCT)
    d2=max(risk*.45,previous_tp1_distance*1.15)
    d3=max(risk*.60,d2*1.15)
    sign=1 if direction=='LONG' else -1
    return entry+sign*d1,entry+sign*d2,entry+sign*d3,strength

def leverage_cap(q):
    if q>=13:return 100
    if q>=12:return 75
    if q>=11:return 50
    if q>=10:return 30
    if q>=9:return 20
    return 15

def calculate_leverage(entry,stop,quality,aggressive_ok):
    stop_pct=abs(entry-stop)/entry
    if stop_pct<=0:
        return 1
    cap=leverage_cap(quality)
    if not aggressive_ok:
        cap=min(cap,15)
    allowed=min(int(LEVERAGE_SAFETY/stop_pct),cap,100)
    return max((x for x in LEVERAGE_STEPS if x<=allowed),default=1)

def invalidate(symbol,reason,counters):
    if symbol in pending:
        old=pending.pop(symbol)
        print(f'{symbol}: CHECK ANNULLATO ({reason}) {old["direction"]}')
    counters[reason]+=1

def trend_15m(candles, direction):
    """Prezzo, EMA20/EMA50 e pendenza EMA20 coerenti sul 15m live."""
    closes = [x['c'] for x in candles]
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    prev20 = ema(closes[:-1], 20)
    if None in (e20, e50, prev20):
        return False
    if direction == 'LONG':
        return closes[-1] > e20 > e50 and e20 > prev20
    return closes[-1] < e20 < e50 and e20 < prev20


def candle_color(c):
    if c['c'] > c['o']:
        return 'LONG'
    if c['c'] < c['o']:
        return 'SHORT'
    return 'NEUTRAL'



def diagnose_candle_setups(direction, live, prev1, prev2, metrics, prev1_metrics, prev2_metrics, counters):
    """Diagnostica OSSERVATIVA: non abilita, modifica o blocca nessun segnale.

    Dopo trend 1H valido, candela LIVE concorde e almeno 15 s di formazione,
    analizza indipendentemente FAST e TREND. Per ciascuna modalita' registra
    il PRIMO motivo di esclusione della struttura (un motivo per modalita').
    Non interpreta questi controlli come trade persi: sono valutazioni ripetute.
    I contatori 'test' sono aggiuntivi, non esclusivi, e servono a confrontare
    separatamente pienezza e wick (una candela puo' fallire entrambi).
    """
    sign = 1 if direction == 'LONG' else -1

    # FAST: la candela precedente non deve avere lo stesso colore; i valori
    # vengono confrontati con le medesime soglie usate dalla strategia LIVE.
    if candle_color(prev1) == direction:
        fast_reason = 'precedente gia nella direzione'
    elif metrics['body'] < FAST_BODY_ATR_MIN:
        fast_reason = 'corpo LIVE sotto minimo'
    elif metrics['body_fraction'] < FAST_BODY_FRACTION_MIN:
        fast_reason = 'corpo LIVE poco pieno'
    elif metrics['wick'] > FAST_WICK_MAX:
        fast_reason = 'wick FAST eccessiva'
    else:
        fast_reason = 'struttura FAST VALIDA'
    counters['DET FAST: ' + fast_reason] += 1

    # TREND: le verifiche rispecchiano l'ordine delle condizioni gia' usate
    # dalla strategia corrente. Nessuna variabile diagnostica entra nella decisione.
    if prev2_metrics is None or prev1_metrics is None:
        trend_reason = 'prime due: colori non concordi'
    elif (prev2_metrics['body'] < TREND_PREV_BODY_ATR_MIN or
          prev1_metrics['body'] < TREND_PREV_BODY_ATR_MIN):
        trend_reason = 'prime due: corpo ATR insufficiente'
    elif (prev2_metrics['body_fraction'] < TREND_PREV_BODY_FRACTION_MIN or
          prev1_metrics['body_fraction'] < TREND_PREV_BODY_FRACTION_MIN):
        trend_reason = 'prime due: pienezza insufficiente'
    elif (prev2_metrics['upper_wick_range'] > TREND_UPPER_WICK_MAX_RANGE or
          prev2_metrics['lower_wick_range'] > TREND_LOWER_WICK_MAX_RANGE or
          prev1_metrics['upper_wick_range'] > TREND_UPPER_WICK_MAX_RANGE or
          prev1_metrics['lower_wick_range'] > TREND_LOWER_WICK_MAX_RANGE):
        trend_reason = 'prime due: wick eccessive'
    elif (sign * (prev1['c']-prev2['c']) <= 0 or
          sign * (live['c']-prev1['c']) <= 0):
        trend_reason = 'chiusure non progressive'
    elif metrics['body'] < TREND_BODY_ATR_MIN:
        trend_reason = 'terza LIVE: corpo ATR insufficiente'
    elif metrics['body'] < (TREND_THIRD_MIN_VS_PREV_AVG *
                            (prev2_metrics['body']+prev1_metrics['body'])/2.):
        trend_reason = 'terza LIVE: sotto 80pct media corpi'
    elif metrics['body_fraction'] < TREND_BODY_FRACTION_MIN:
        trend_reason = 'terza LIVE: pienezza insufficiente'
    elif (metrics['upper_wick_range'] > TREND_UPPER_WICK_MAX_RANGE or
          metrics['lower_wick_range'] > TREND_LOWER_WICK_MAX_RANGE):
        trend_reason = 'terza LIVE: wick eccessive'
    else:
        trend_reason = 'struttura TREND VALIDA'
    counters['DET TREND: ' + trend_reason] += 1

    # Indicatori NON ESCLUSIVI della forma: utili anche quando un altro
    # requisito e' gia' fallito. Vengono calcolati solo se la coppia delle
    # prime due candele ha il colore corretto rispetto al trend 1H.
    if prev2_metrics is not None and prev1_metrics is not None:
        counters['DET TREND test: due chiuse stesso colore'] += 1
        if (prev2_metrics['body'] < TREND_PREV_BODY_ATR_MIN or
                prev1_metrics['body'] < TREND_PREV_BODY_ATR_MIN):
            counters['DET TREND test: corpo prime due debole'] += 1
        if (prev2_metrics['body_fraction'] < TREND_PREV_BODY_FRACTION_MIN or
                prev1_metrics['body_fraction'] < TREND_PREV_BODY_FRACTION_MIN):
            counters['DET TREND test: pienezza prime due scarsa'] += 1
        if (prev2_metrics['upper_wick_range'] > TREND_UPPER_WICK_MAX_RANGE or
                prev2_metrics['lower_wick_range'] > TREND_LOWER_WICK_MAX_RANGE or
                prev1_metrics['upper_wick_range'] > TREND_UPPER_WICK_MAX_RANGE or
                prev1_metrics['lower_wick_range'] > TREND_LOWER_WICK_MAX_RANGE):
            counters['DET TREND test: wick prime due lunghe'] += 1
        if (sign * (prev1['c']-prev2['c']) <= 0 or
                sign * (live['c']-prev1['c']) <= 0):
            counters['DET TREND test: progressione non valida'] += 1
        if (metrics['body'] < TREND_THIRD_MIN_VS_PREV_AVG *
                (prev2_metrics['body']+prev1_metrics['body'])/2.):
            counters['DET TREND test: terza piccola vs prime due'] += 1
    if metrics['body'] < TREND_BODY_ATR_MIN:
        counters['DET TREND test: terza corpo sotto minimo'] += 1
    if metrics['body_fraction'] < TREND_BODY_FRACTION_MIN:
        counters['DET TREND test: terza pienezza scarsa'] += 1
    if (metrics['upper_wick_range'] > TREND_UPPER_WICK_MAX_RANGE or
            metrics['lower_wick_range'] > TREND_LOWER_WICK_MAX_RANGE):
        counters['DET TREND test: terza wick lunga'] += 1


def analyze_symbol(symbol,data,bias,btc15,btc30,btc1h,now,counters,btc_volume=None,volume_override=None):
    """Due vie: FAST prima candela forte; TREND terza dello stesso colore.

    Nessuna attesa fissa; la candela live e' valutata a ogni scansione.
    Deduplica per simbolo/direzione/candela dopo invio Telegram riuscito.
    """
    c15,c1h,c4h=data['15m'],data['1h'],data['4h']
    # Registra il volume a OGNI scansione, anche quando il trend 1H e' neutro.
    # Altrimenti perderemmo l'inizio dell'accelerazione per mancanza di storico.
    # BTC gia' campionato all'inizio del ciclo. Non duplicare un campione
    # con lo stesso volume vecchio quando le altre 24 coppie finiscono il download.
    volume=(volume_override if volume_override is not None
            else progressive_volume_metrics(symbol,c15,now))
    if min(len(c15),len(c1h),len(c4h))<55:
        counters['dati insufficienti']+=1
        return None
    direction=trend_1h(c1h)
    if direction=='NEUTRAL':
        counters['trend 1H neutro']+=1
        return None
    sign=1 if direction=='LONG' else -1
    live=c15[-1]
    prev1,prev2=c15[-2],c15[-3]
    a15=atr(c15[:-1])
    if not a15 or a15<=0:
        counters['ATR mancante']+=1
        return None
    metrics=candle_metrics(live,direction,a15)
    if metrics is None:
        counters['candela live contraria']+=1
        return None
    pace,elapsed=live_volume_ratio(c15)
    if elapsed<LIVE_VOLUME_MIN_ELAPSED_SECONDS:
        counters['volume live in formazione']+=1
        return None

    # FAST: prima candela direzionale dopo una candela chiusa non concorde.
    # TREND: due candele chiuse e la terza live dello stesso colore.
    fast=(candle_color(prev1)!=direction and
          metrics['body']>=FAST_BODY_ATR_MIN and
          metrics['body_fraction']>=FAST_BODY_FRACTION_MIN and
          metrics['wick']<=FAST_WICK_MAX)
    # TREND: 3 corpi forti e wick <=15% su entrambi i lati.
    # Le prime 2 sono chiuse; la terza e' LIVE: non attende la chiusura.
    # Nessun controllo volume sulle prime due; conta solo l'accelerazione LIVE.
    prev2_metrics=candle_metrics(prev2,direction,a15)
    prev1_metrics=candle_metrics(prev1,direction,a15)
    trend=(prev2_metrics is not None and prev1_metrics is not None and
           prev2_metrics['body']>=TREND_PREV_BODY_ATR_MIN and
           prev1_metrics['body']>=TREND_PREV_BODY_ATR_MIN and
           prev2_metrics['body_fraction']>=TREND_PREV_BODY_FRACTION_MIN and
           prev1_metrics['body_fraction']>=TREND_PREV_BODY_FRACTION_MIN and
           trend_shape_ok(prev2_metrics) and trend_shape_ok(prev1_metrics) and
           sign*(prev1['c']-prev2['c'])>0 and
           sign*(live['c']-prev1['c'])>0 and
           metrics['body']>=TREND_BODY_ATR_MIN and
           metrics['body']>=TREND_THIRD_MIN_VS_PREV_AVG *
                   (prev2_metrics['body']+prev1_metrics['body'])/2. and
           trend_shape_ok(metrics))
    # La diagnostica e' osservativa; FAST, volume e conferme restano invariati.
    diagnose_candle_setups(direction, live, prev1, prev2, metrics,
                           prev1_metrics, prev2_metrics, counters)
    # Il minimo dinamico e i campioni sufficienti rimangono obbligatori.
    # FAST: via A (accelerazione) OPPURE via B (volume >=2.50x sostenuto).
    # TREND: resta SOLO la via A progressiva, sulla terza candela LIVE.
    volume_confirmation = 'PROGRESSIVO (A)'
    if fast or trend:
        if not volume['ready']:
            counters['volume progressivo: campioni insufficienti']+=1
            return None
        if not volume['intensity_ok']:
            counters['volume progressivo: intensita insufficiente']+=1
            return None
        fast_volume_ok = volume['accel_ok'] or volume['sustained_ok']
        if fast and not fast_volume_ok:
            counters['volume FAST: ne progressivo ne alto sostenuto']+=1
            return None
        # Se FAST e' vero ha precedenza sul TREND, come in V9.8.8.
        if not fast and trend and not volume['accel_ok']:
            counters['volume progressivo: accelerazione insufficiente']+=1
            return None
        if fast and not volume['accel_ok'] and volume['sustained_ok']:
            volume_confirmation = 'ALTO SOSTENUTO (B >=2.50x)'
    if fast:
        mode='FAST ENTRY - PRIMA CANDELA'
        reference=live['o']
        max_extension=FAST_MAX_EXTENSION_ATR
        btc_min=FAST_BTC15_BODY_ATR_MIN
    elif trend:
        mode='TREND ENTRY - TERZA CANDELA'
        reference=prev2['o']
        max_extension=TREND_MAX_EXTENSION_ATR
        btc_min=TREND_BTC15_BODY_ATR_MIN
    else:
        counters['setup FAST/TREND non valido']+=1
        return None

    total_move=sign*(live['c']-reference)/a15
    if total_move>max_extension:
        counters['impulso troppo esteso']+=1
        return None
    if btc15['directional']!=direction or btc15['body_atr']<btc_min:
        counters['BTC15 non forte/concorde']+=1
        return None
    # Nuovo filtro V9.9.3 SOLO per TREND: stessa logica volume progressivo
    # della terza candela (3 osservazioni, intensita e accelerazione +12%).
    # FAST rimane invariato: BTC15 volume solamente informativo.
    if mode == 'TREND ENTRY - TERZA CANDELA':
        if not btc_volume or not btc_volume['ready']:
            counters['BTC15 TREND: campioni volume insufficienti']+=1
            return None
        if not btc_volume['intensity_ok']:
            counters['BTC15 TREND: volume sotto intensita minima']+=1
            return None
        if not btc_volume['accel_ok']:
            counters['BTC15 TREND: volume non progressivo']+=1
            return None
    # BTC1H puo' essere neutro, ma mai contrario. Bias BTC deve concordare.
    if btc1h not in (direction,'NEUTRAL') or not bias.startswith(direction+'_'):
        counters['BTC 1H/bias contrario']+=1
        return None
    if not trend_15m(c15,direction):
        counters['EMA20/50 15m contrarie']+=1
        return None
    sto=stochastic(c15)
    if sto is None or signed(sto[0]-sto[1],direction)<=0:
        counters['stocastico contrario']+=1
        return None

    ctx=context_4h(c4h)
    entry=live['c']
    # Il livello e' la partenza dell'impulso, non richiede un nuovo breakout.
    level=reference
    stop=intelligent_stop(direction,entry,level,live,a15,c15[:-1],ctx,bias,c1h)
    if stop is None:
        counters['stop strutturale fuori range']+=1
        return None
    risk=abs(entry-stop)
    room=available_room(c15,direction,entry,risk)
    if room<MIN_ROOM_R*risk:
        counters['spazio tecnico insufficiente']+=1
        return None
    closed_vol=closed_volume_ratio(c15)
    # TREND: nessun vincolo sui volumi delle due candele chiuse.
    # FAST conserva integralmente la condizione volume precedente >=0.80x.
    if fast and closed_vol<MIN_CLOSED_VOLUME_RATIO:
        counters['volume candela precedente insufficiente (FAST)']+=1
        return None
    body=metrics['body']
    # FAST B forte e sostenuto prende il punto volume anche se non accelera.
    # Tutti gli altri punteggi restano identici alla V9.8.8.
    volume_quality_bonus = volume['growth']>=.25 or (fast and volume_confirmation.startswith('ALTO SOSTENUTO'))
    quality=5+int(volume['relative_pace']>=1.10)+int(volume_quality_bonus)+int(body>=.40)+int(body>=.65)
    # Il punteggio TREND non dipende dal volume delle prime due candele.
    # Si conserva il peso del punto di qualita' per mantenere la stessa scala.
    quality+=int(total_move>=.35)+(int(closed_vol>=CLOSED_VOLUME_PREFERRED) if fast else 1)
    quality+=int(ctx==direction)+int(signed(sto[0]-sto[1],direction)>=2.)
    quality=min(quality,13)
    if quality<MIN_QUALITY:
        counters['qualita insufficiente']+=1
        return None
    # Leva indicativa limitata a 15x; non e' un dimensionamento posizione.
    leverage=calculate_leverage(entry,stop,quality,False)
    targets=adaptive_targets(direction,entry,stop,a15,quality,pace,body,total_move,ctx,bias)
    if targets is None:
        counters['target non valido']+=1
        return None
    tp1,tp2,tp3,strength=targets
    counters['segnali idonei '+mode]+=1
    return {'direction':direction,'mode':mode,
            'price':entry,'entry_low':entry-a15*.06,'entry_high':entry+a15*.06,
            'sl':stop,'tp1':tp1,'tp2':tp2,'tp3':tp3,
            'leverage':leverage,'quality':quality,'level':level,
            'candle_time':live['t'],
            'live_volume_pace':pace,'closed_volume_ratio':closed_vol,
            'volume_dynamic_floor':volume['dynamic_floor'],
            'volume_acceleration':volume['growth'],
            'volume_confirmation':volume_confirmation,
            'volume_relative_strength':volume['relative_pace'],
            'live_body_atr':body,'follow_through_atr':total_move,
            'rejection_wick_ratio':metrics['wick'],'price_progress_atr':total_move,
            'stoch_k':sto[0],'stoch_d':sto[1],
            'context1h':direction,'context4h':ctx,'btc':bias,
            'btc15_direction':btc15['directional'],
            'btc15_body_atr':btc15['body_atr'],
            'btc15_volume_pace':btc15['volume_pace'],
            'btc15_volume_progressive':bool(btc_volume and btc_volume['ready'] and
                                            btc_volume['intensity_ok'] and btc_volume['accel_ok']),
            'btc15_volume_growth':btc_volume['growth'] if btc_volume else 0.,
            'btc30_direction':btc30,'btc1h_direction':btc1h,
            'target_strength':strength,'check_seconds':int(elapsed),
            'room_r':room/risk if room!=float('inf') else None}

def fmt_price(v):
    if v>=1000:return f'{v:.2f}'
    if v>=1:return f'{v:.4f}'
    if v>=.01:return f'{v:.6f}'
    return f'{v:.9f}'

def build_message(symbol,s):
    pair=symbol.replace('USDT','/USDT')
    italian_time=datetime.now(ZoneInfo('Europe/Rome')).strftime('%d/%m/%Y %H:%M:%S')
    room=f"{s['room_r']:.2f}R" if s['room_r'] is not None else 'nessun ostacolo rilevato'
    return (
        f"🚀 FAST/TREND V9.9.3 (SPERIMENTALE)\n{pair} — {s['direction']}\n"
        f"Orario Italia: {italian_time}\n"
        f"Setup: {s['mode']} | Qualita': {s['quality']}/13\n\n"
        f"ENTRY: {fmt_price(s['entry_low'])} - {fmt_price(s['entry_high'])}\n"
        f"SL strutturale 15m/1H: {fmt_price(s['sl'])}\n\n"
        f"TP1: {fmt_price(s['tp1'])}\nTP2: {fmt_price(s['tp2'])}\n"
        f"TP3: {fmt_price(s['tp3'])}\n\n"
        f"Leva indicativa: {s['leverage']}x\n"
        f"Candela live in corso da: {s['check_seconds']}s\n"
        f"Livello: {fmt_price(s['level'])}\n"
        f"Volume LIVE: {s['live_volume_pace']:.2f}x\n"
        f"Volume soglia dinamica: {s['volume_dynamic_floor']:.2f}x\n"
        f"Conferma volume: {s['volume_confirmation']}\n"
        f"Variazione velocita' volume: {s['volume_acceleration']*100:+.0f}%\n"
        f"Volume chiuso: {s['closed_volume_ratio']:.2f}x\n"
        f"Progressione: {s['price_progress_atr']:+.3f} ATR15\n"
        f"Estensione: {s['follow_through_atr']:.2f} ATR15\n"
        f"Stoch K/D: {s['stoch_k']:.1f}/{s['stoch_d']:.1f}\n"
        f"Spazio tecnico storico: {room}\n\n"
        f"Trend 1H: {s['context1h']} | 4H: {s['context4h']}\n"
        f"BTC bias: {s['btc']}\n"
        f"BTC15: {s['btc15_direction']} (corpo {s['btc15_body_atr']:.2f} ATR) | BTC1H: {s['btc1h_direction']}\n"
        f"BTC15 volume progressivo: {'SI' if s['btc15_volume_progressive'] else 'NO'} "
        f"(+{s['btc15_volume_growth']*100:.0f}% ritmo; obbligatorio solo TREND)\n"
        f"BTC30 informativo: {s['btc30_direction']}\n"
        'Segnale anticipato non validato. Stop e leva non garantiscono protezione da liquidazione.'
    )

def should_send(symbol,signal):
    now=time.time()
    for key,ts in list(signal_state.items()):
        if now-ts>12*3600:
            del signal_state[key]
    key=(symbol,signal['direction'],signal['candle_time'])
    return key not in signal_state

def scan_market():
    global last_diag
    started=time.monotonic()
    counters=Counter()
    btc_data=market_data('BTCUSDT')
    # Una misura del volume BTC all'inizio di OGNI ciclo, indipendentemente
    # dalla presenza di setup sulle altcoin.
    btc_volume=progressive_volume_metrics('BTCUSDT',btc_data['15m'],time.monotonic())
    bias=btc_bias(btc_data)
    btc15=btc15_direction(btc_data)
    btc30=btc_30m_direction(cached_candles('BTCUSDT','30m',CACHE_30M_SECONDS))
    btc1h=trend_1h(btc_data['1h'])
    btc_vol_status=('OK' if (btc_volume['ready'] and btc_volume['intensity_ok']
                              and btc_volume['accel_ok']) else 'NON CONFERMATO')
    print(f"BTC: {bias} | BTC15: {btc15['directional']} "
          f"({btc15['body_atr']:.2f} ATR) | BTC15VOL: {btc_vol_status} "
          f"| BTC30: {btc30} | BTC1H: {btc1h}")
    market={'BTCUSDT':btc_data}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures={pool.submit(market_data,s):s for s in SYMBOLS if s!='BTCUSDT'}
        for future in as_completed(futures):
            symbol=futures[future]
            try:
                market[symbol]=future.result()
            except Exception as exc:
                print(symbol,'ERRORE download:',exc)
                counters['download fallito']+=1
    now=time.monotonic()
    successful=0
    for symbol in SYMBOLS:
        if symbol not in market:
            if symbol in pending:
                invalidate(symbol,'download mancante',counters)
            continue
        try:
            successful+=1
            data=market[symbol]
            signal=analyze_symbol(symbol,data,bias,btc15,btc30,btc1h,now,counters,
                                  btc_volume=btc_volume,
                                  volume_override=btc_volume if symbol=='BTCUSDT' else None)
            if signal and should_send(symbol,signal):
                print(symbol,'FAST/TREND V9.9.3',signal['direction'],signal['mode'])
                if send_telegram(build_message(symbol,signal)):
                    signal_state[(symbol,signal['direction'],signal['candle_time'])]=time.time()
            trend=trend_1h(data['1h'])
            last_observed[symbol]={'price':data['15m'][-1]['c'],'direction':trend}
        except Exception as exc:
            print(symbol,'ERRORE analisi:',exc)
            if symbol in pending:
                invalidate(symbol,'errore analisi',counters)
    elapsed=time.monotonic()-started
    print(f'Scansione completata: {successful}/{len(SYMBOLS)} coppie | '
          f'durata {elapsed:.1f}s | target {SCAN_SECONDS}s | FAST/TREND attivi')
    diagnostic_totals.update(counters)
    diagnostic_since_last.update(counters)
    if time.monotonic()-last_diag>=DIAGNOSTIC_EVERY_SECONDS:
        # Scarti originali: un motivo operativo per coppia/scansione.
        # I dettagli DET sono conteggi INFORMATIVI e non vanno sommati agli scarti.
        primary_scan={k:v for k,v in counters.items() if not k.startswith('DET ')}
        primary_total={k:v for k,v in diagnostic_totals.items() if not k.startswith('DET ')}
        detail_window={k:v for k,v in diagnostic_since_last.items() if k.startswith('DET ')}
        print('DIAGNOSTICA V9.9.3 (questa scansione):',primary_scan)
        print('DIAGNOSTICA V9.9.3 (cumulativa dall avvio):',primary_total)
        print('DETTAGLIO CANDELE V9.9.3 (circa ultimi 60s):',
              dict(sorted(detail_window.items(),key=lambda item:(-item[1],item[0]))))
        print('NOTA: DET FAST/TREND = primo ostacolo per modalita; DET test = non esclusivo.')
        diagnostic_since_last.clear()
        last_diag=time.monotonic()

def main():
    print('CryptoSignalAI V9.9.3 - TREND 80/15, BTC15 TREND 0.20 ATR + VOLUME PROGRESSIVO, SCANSIONE CONTINUA')
    send_telegram(
        'CryptoSignalAI V9.9.3 ONLINE - TREND 80/15 (SPERIMENTALE)\n'
        'FAST invariato; TREND pienezza 80% e wick per lato 15%; diagnostica Railway attiva.\n'
        '25 coppie. FAST INVARIATO: prima live corpo >=0.70 ATR15 e >=80% candela.\n'
        'TREND: 3 corpi >=0.35 ATR15, pieni >=80%, chiusure progressive; terza LIVE >=80% media corpi prime due.\n'
        'TREND wick: sia superiore sia inferiore <=15% del range per tutte e tre; corpi >=80% del range.\n'
        'FAST volume: progressivo A OPPURE sostenuto B >=2.50x, calo ritmo <=15% su 2 intervalli live.\n'
        'TREND: volume progressivo A SOLO sulla terza LIVE; nessun filtro volume sulle prime due.\n'
        'Volume candela precedente >=0.80x SOLO per FAST.\n'
        'Nessuna attesa fissa 60/120s; campionamento volume minimo 15s.\n'
        'EMA20/50 15m e trend 1H, stocastico e BTC15 direzionale concordi.\n'
        'BTC15 FAST >=0.30 ATR; TREND >=0.20 ATR + volume progressivo obbligatorio; bias BTC concorde.\n'
        'BTC15 progressivo: 3 campioni live, intensita storica adeguata, velocita in aumento >=12%.\n'
        'Scansioni REST: target 1s, senza sovrapposizioni; durata effettiva dipende da 25 download.\n'
        'SL strutturale 15m/1H massimo 4.50 ATR15; spazio >=1.20R. TP1 0.30R, TP2 0.45R, TP3 0.60R.\n'
        'Leva indicativa <=15x. Strategia non validata: testare in simulazione.')
    while True:
        started=time.monotonic()
        try:
            scan_market()
        except Exception as exc:
            print('Errore scansione generale:',exc)
            pending.clear()
        time.sleep(max(0.2,SCAN_SECONDS-(time.monotonic()-started)))

if __name__=='__main__':
    main()
