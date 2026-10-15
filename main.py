import os
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
import requests

# V9.6: identica alla V9.5, modificato esclusivamente il calcolo dello stop loss.
# BTC15 blocca solo se fortemente contrario; BTC1H concorde o NEUTRAL, mai contrario.
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
SCAN_SECONDS = 5
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
BTC15_DIRECTIONAL_BODY_ATR_MIN = .15
BTC15_STRONG_OPPOSITE_BODY_ATR_MIN = .45  # blocca solo corpo BTC15 opposto >= 0.45 ATR15
# V9.6: solo parametri stop loss (gli altri filtri sono invariati).
SL_STRUCTURE_LOOKBACK_BARS = 8      # struttura 15m piu ampia, solo candele chiuse
SL_1H_LOOKBACK_BARS = 3            # controllo livelli 1h recenti, solo candele chiuse
SL_BUFFER_STRONG_TREND_ATR15 = .35
SL_BUFFER_NORMAL_TREND_ATR15 = .28
SL_BUFFER_NEUTRAL_4H_ATR15 = .20
SL_MIN_DISTANCE_ATR15 = .90
SL_MAX_DISTANCE_ATR15 = 2.40       # rischio massimo invariato: nessuno stop forzato oltre limite
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
    return {'body':body/a15,'close':pos,'wick':wick}

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
    return direction if metrics and metrics['body']>=BTC15_DIRECTIONAL_BODY_ATR_MIN else 'NEUTRAL'

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

# V9.6: unica funzione strategica modificata: stop dietro struttura 15m
# e, se compatibile con il rischio massimo, anche dietro struttura 1h.
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
    d1=max(risk*(1+.4*adjusted),entry*TP_MIN_NET_PCT)
    d2=max(risk*(1.6+.6*adjusted),d1*1.15)
    d3=max(risk*(2.4+.9*adjusted),d2*1.15)
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

def analyze_symbol(symbol,data,bias,btc15,btc30,btc1h,now,counters):
    c15,c1h,c4h=data['15m'],data['1h'],data['4h']
    if min(len(c15),len(c1h),len(c4h))<55:
        invalidate(symbol,'dati insufficienti',counters)
        return None
    trend=trend_1h(c1h)
    if trend=='NEUTRAL':
        invalidate(symbol,'trend 1H neutro',counters)
        return None
    direction=trend
    sign=1 if direction=='LONG' else -1
    live=c15[-1]
    a15=atr(c15[:-1])
    if not a15 or a15<=0:
        invalidate(symbol,'ATR mancante',counters)
        return None
    ctx=context_4h(c4h)
    item=pending.get(symbol)
    previous=last_observed.get(symbol)
    if item is None:
        # Prima cerchiamo l'attraversamento strutturale.
        # NON blocchiamo l'evento in base a corpo/ombra/volume iniziali.
        if previous is None or previous['direction'] != direction:
            counters['osservazione iniziale'] += 1
            return None
        levels = opportunity_level(c15, direction, a15)
        for mode, level in levels:
            extension = sign * (live['c'] - level) / a15
            crossed = sign * (previous['price'] - level) <= 0
            if crossed and BREAKOUT_MIN_ATR <= extension <= BREAKOUT_START_MAX_EXTENSION_ATR:
                pending[symbol] = {
                    'direction': direction, 'mode': mode, 'level': level,
                    'started': now, 'last_seen': now,
                    'start_price': live['c'], 'start_candle_time': live['t'],
                    'start_atr': a15,
                }
                print(f'{symbol}: CHECK RAPIDO AVVIATO {mode} {direction} livello={level:.9g}')
                counters['check avviati'] += 1
                return None
        counters['nessun attraversamento valido'] += 1
        return None
    if item['direction']!=direction:
        invalidate(symbol,'cambio trend 1H',counters)
        return None
    age=now-item['started']
    if now-item['last_seen']>CHECK_MAX_GAP_SECONDS or age>CHECK_MAX_SECONDS:
        invalidate(symbol,'check scaduto/gap dati',counters)
        return None
    item['last_seen']=now
    level=item['level']
    # Con la tolleranza, una breve oscillazione e' ammessa; il prezzo finale
    # deve comunque essere dalla parte corretta del livello.
    extension=sign*(live['c']-level)/a15
    if extension < -CHECK_LEVEL_TOLERANCE_ATR:
        invalidate(symbol,'rientro eccessivo nel livello',counters)
        return None
    if extension>CHECK_MAX_EXTENSION_ATR:
        invalidate(symbol,'prezzo troppo esteso',counters)
        return None
    if age<CHECK_MIN_SECONDS:
        counters['check in corso']+=1
        return None
    if extension<BREAKOUT_MIN_ATR:
        invalidate(symbol,'non sopra/sotto livello al check',counters)
        return None
    # BTC15: consentito se concorde, neutrale o debolmente contrario.
    # Un corpo opposto >= 0.45 ATR15 e' considerato fortemente contrario.
    if (btc15['directional'] not in (direction, 'NEUTRAL')
            and btc15['body_atr'] >= BTC15_STRONG_OPPOSITE_BODY_ATR_MIN):
        invalidate(symbol,'BTC15 fortemente contrario',counters)
        return None
    if btc1h not in (direction, 'NEUTRAL'):
        invalidate(symbol,'BTC1H contrario',counters)
        return None
    if not bias.startswith(direction+'_'):
        invalidate(symbol,'bias BTC non concorde',counters)
        return None
    if not aligned_ema(c15,direction):
        invalidate(symbol,'EMA20 15m contraria',counters)
        return None
    sto=stochastic(c15)
    if sto is None or signed(sto[0]-sto[1],direction)<=0:
        invalidate(symbol,'stocastico contrario',counters)
        return None
    pace,elapsed=live_volume_ratio(c15)
    if elapsed<MIN_CANDLE_ELAPSED_SECONDS or pace<BREAKOUT_VOLUME_PACE_MIN:
        invalidate(symbol,'volume finale insufficiente',counters)
        return None
    progress=sign*(live['c']-item['start_price'])/a15
    if progress<CHECK_MIN_PROGRESS_ATR:
        invalidate(symbol,'prezzo non progredito',counters)
        return None
    metrics=candle_metrics(live,direction,a15)
    if (metrics is None or metrics['body'] < BREAKOUT_BODY_ATR_MIN
            or metrics['close'] < BREAKOUT_CLOSE_POSITION_MIN
            or metrics['wick'] > BREAKOUT_WICK_MAX):
        invalidate(symbol,'candela finale debole',counters)
        return None
    body=metrics['body']
    wick=metrics['wick']
    entry=live['c']
    stop=intelligent_stop(direction,entry,level,live,a15,c15[:-1],ctx,bias,c1h)
    if stop is None:
        invalidate(symbol,'stop strutturale fuori range',counters)
        return None
    risk=abs(entry-stop)
    room=available_room(c15,direction,entry,risk)
    if room<MIN_ROOM_R*risk:
        invalidate(symbol,'spazio tecnico insufficiente',counters)
        return None
    closed_vol=closed_volume_ratio(c15)
    quality=5+int(pace>=1.8)+int(pace>=2.5)+int(body>=.40)+int(body>=.65)
    quality+=int(progress>=.06)+int(closed_vol>=CLOSED_VOLUME_PREFERRED)
    quality+=int(ctx==direction)+int(signed(sto[0]-sto[1],direction)>=2.)
    quality=min(quality,13)
    if quality<MIN_QUALITY:
        invalidate(symbol,'qualita insufficiente',counters)
        return None
    aggressive_ok=(quality>=11 and pace>=2.5 and body>=.65 and
                   progress>=.06 and ctx==direction)
    leverage=calculate_leverage(entry,stop,quality,aggressive_ok)
    targets=adaptive_targets(direction,entry,stop,a15,quality,pace,body,extension,ctx,bias)
    if targets is None:
        invalidate(symbol,'target non valido',counters)
        return None
    tp1,tp2,tp3,strength=targets
    signal={'direction':direction,'mode':item['mode']+' CHECK RAPIDO',
            'price':entry,'entry_low':entry-a15*.06,'entry_high':entry+a15*.06,
            'sl':stop,'tp1':tp1,'tp2':tp2,'tp3':tp3,
            'leverage':leverage,'quality':quality,'level':level,
            'candle_time':item['start_candle_time'],
            'live_volume_pace':pace,'closed_volume_ratio':closed_vol,
            'live_body_atr':body,'follow_through_atr':extension,
            'rejection_wick_ratio':wick,'price_progress_atr':progress,
            'stoch_k':sto[0],'stoch_d':sto[1],
            'context1h':trend,'context4h':ctx,'btc':bias,
            'btc15_direction':btc15['directional'],
            'btc15_body_atr':btc15['body_atr'],
            'btc15_volume_pace':btc15['volume_pace'],
            'btc30_direction':btc30,'btc1h_direction':btc1h,
            'target_strength':strength,'check_seconds':int(age),
            'room_r':room/risk if room!=float('inf') else None}
    pending.pop(symbol,None)
    counters['check confermati']+=1
    return signal

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
        f"🚀 MOMENTUM CONFERMATO V9.6 (SPERIMENTALE)\n{pair} — {s['direction']}\n"
        f"Orario Italia: {italian_time}\n"
        f"Setup: {s['mode']} | Qualita': {s['quality']}/13\n\n"
        f"ENTRY: {fmt_price(s['entry_low'])} - {fmt_price(s['entry_high'])}\n"
        f"SL strutturale 15m/1H: {fmt_price(s['sl'])}\n\n"
        f"TP1: {fmt_price(s['tp1'])}\nTP2: {fmt_price(s['tp2'])}\n"
        f"TP3: {fmt_price(s['tp3'])}\n\n"
        f"Leva indicativa: {s['leverage']}x\n"
        f"Controllo rapido: {s['check_seconds']}s (nessun HOLD 5m)\n"
        f"Livello: {fmt_price(s['level'])}\n"
        f"Volume LIVE: {s['live_volume_pace']:.2f}x\n"
        f"Volume chiuso: {s['closed_volume_ratio']:.2f}x\n"
        f"Progressione: {s['price_progress_atr']:+.3f} ATR15\n"
        f"Estensione: {s['follow_through_atr']:.2f} ATR15\n"
        f"Stoch K/D: {s['stoch_k']:.1f}/{s['stoch_d']:.1f}\n"
        f"Spazio tecnico storico: {room}\n\n"
        f"Trend 1H: {s['context1h']} | 4H: {s['context4h']}\n"
        f"BTC bias: {s['btc']}\n"
        f"BTC15: {s['btc15_direction']} | BTC1H: {s['btc1h_direction']}\n"
        f"BTC30 informativo: {s['btc30_direction']}\n"
        'Strategia non validata. Stop e leva non garantiscono protezione da liquidazione.'
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
    bias=btc_bias(btc_data)
    btc15=btc15_direction(btc_data)
    btc30=btc_30m_direction(cached_candles('BTCUSDT','30m',CACHE_30M_SECONDS))
    btc1h=trend_1h(btc_data['1h'])
    print(f"BTC: {bias} | BTC15: {btc15['directional']} | BTC30: {btc30} | BTC1H: {btc1h}")
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
            signal=analyze_symbol(symbol,data,bias,btc15,btc30,btc1h,now,counters)
            if signal and should_send(symbol,signal):
                print(symbol,'MOMENTUM CONFERMATO V9.6',signal['direction'],signal['mode'])
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
          f'durata {elapsed:.1f}s | target {SCAN_SECONDS}s | CHECK attivi {len(pending)}')
    diagnostic_totals.update(counters)
    if time.monotonic()-last_diag>=DIAGNOSTIC_EVERY_SECONDS:
        print('DIAGNOSTICA V9.6 (questa scansione):',dict(counters))
        print('DIAGNOSTICA V9.6 (cumulativa dall avvio):',dict(diagnostic_totals))
        last_diag=time.monotonic()

def main():
    print('CryptoSignalAI V9.6 - CHECK RAPIDO >=20s, BTC15 BLOCCA SOLO SE FORTE CONTRARIO, BTC1H NEUTRAL AMMESSO')
    send_telegram(
        'CryptoSignalAI V9.6 ONLINE\n'
        '25 coppie. Breakout prima dei filtri; conferma successiva. Nessun HOLD 5 minuti.\n'
        'Controllo rapido 20-55s: tenuta livello con tolleranza 0.08 ATR, prezzo in progresso.\n'
        'BTC15: blocco solo se fortemente contrario (corpo >=0.45 ATR15); BTC1H concorde o NEUTRAL. Bias BTC concorde obbligatorio.\n'
        'Trend 1H EMA20/50, EMA20 15m e Stocastico favorevoli alla conferma.\n'
        'Candela finale controllata, volume LIVE >=1.20x. SL strutturale 15m/1H, massimo 2.40 ATR15.\n'
        'TP e leva dinamici. Strategia non validata: testare in simulazione.')
    while True:
        started=time.monotonic()
        try:
            scan_market()
        except Exception as exc:
            print('Errore scansione generale:',exc)
            pending.clear()
        time.sleep(max(1.,SCAN_SECONDS-(time.monotonic()-started)))

if __name__=='__main__':
    main()
