"""
Unicorn Model — Python (matches Pine Script v6 logic)
======================================================
1. HTF levels: OHLC + Swing de 15m / 30m / 1H
2. Sweep: mecha que rompe el nivel (close al otro lado)
3. Breaker: 2+ velas consecutivas en dirección contraria
4. Confirmación: close rompe el breaker block
5. FVG overlap (Unicorn mode): FVG sin mitigar en el breaker
6. SL: extremo opuesto del breaker; TP: 2R default
"""
import logging
log = logging.getLogger("unicorn")


def _atr(candles, p=14):
    trs=[max(c["high"]-c["low"],abs(c["high"]-candles[i-1]["close"]),abs(c["low"]-candles[i-1]["close"]))
         for i,c in enumerate(candles) if i>0]
    if not trs: return 0.0
    a=trs[0]
    for t in trs[1:]: a=t/p+a*(1-1/p)
    return a


def _get_htf_levels(candles_htf, htf_label="1H"):
    """OHLC (vela anterior) + Swing highs/lows del HTF."""
    levels=[]
    if len(candles_htf)<3: return levels
    prev=candles_htf[-2]
    levels.append({"price":prev["high"],"is_high":True, "type":"OHLC","htf":htf_label})
    levels.append({"price":prev["low"], "is_high":False,"type":"OHLC","htf":htf_label})
    for i in range(2,min(len(candles_htf)-1,22)):
        c,cp=candles_htf[-i],candles_htf[-i-1]
        if c["high"]>cp["high"] and c["close"]<cp["close"]:
            levels.append({"price":c["high"],"is_high":True, "type":"Swing","htf":htf_label})
        if c["low"]<cp["low"] and c["close"]>cp["close"]:
            levels.append({"price":c["low"], "is_high":False,"type":"Swing","htf":htf_label})
    return levels


def _check_sweep(candles_5m, level, lookback=30):
    """Mecha que toca el nivel con close al otro lado."""
    is_high=level["is_high"]; price=level["price"]
    check=candles_5m[-lookback:]
    for i in range(len(check)-1,-1,-1):
        c=check[i]
        if is_high and c["high"]>=price and c["close"]<price:
            return len(candles_5m)-lookback+i
        if not is_high and c["low"]<=price and c["close"]>price:
            return len(candles_5m)-lookback+i
    return None


def _find_breaker(candles, sweep_idx, direction, max_s=40):
    """2+ velas consecutivas en dirección contraria al sweep."""
    s=sweep_idx+1; e=min(s+max_s,len(candles)-1)
    for i in range(s,e):
        c=candles[i]; match=(c["close"]>c["open"]) if direction=="BULL" else (c["close"]<c["open"])
        if not match: continue
        end=i
        for j in range(i+1,min(i+20,e)):
            nx=candles[j]
            if (nx["close"]>nx["open"]) if direction=="BULL" else (nx["close"]<nx["open"]): end=j
            else: break
        if end-i+1>=2:
            top=max(c["high"] for c in candles[i:end+1])
            bot=min(c["low"]  for c in candles[i:end+1])
            return i,end,top,bot
    return None


def _find_fvg(candles, b_top, b_bot, direction):
    """FVG sin mitigar solapado con el breaker."""
    s=candles[-100:] if len(candles)>100 else candles
    if direction=="BULL":
        for i in range(2,len(s)):
            ft=s[i]["low"]; fb=s[i-2]["high"]
            if ft<=fb: continue
            if min(ft,b_top)>max(fb,b_bot):
                if not any(c["low"]<fb for c in s[i:]): return ft,fb
    else:
        for i in range(2,len(s)):
            ft=s[i-2]["low"]; fb=s[i]["high"]
            if fb<=ft: continue
            if min(ft,b_top)>max(fb,b_bot):
                if not any(c["high"]>ft for c in s[i:]): return ft,fb
    return None,None


def get_signal(candles_5m, candles_1h, config,
               candles_15m=None, candles_30m=None):
    """
    Unicorn Model en Python.
    candles_5m  → velas de entrada/breaker/FVG
    candles_1h  → niveles HTF source C
    candles_15m → niveles HTF source A (opcional)
    candles_30m → niveles HTF source B (opcional)
    """
    R={
        "signal":None,"entry_price":0,"sl_price":0,"tp_price":0,
        "swept_level":0,"breaker_top":0,"breaker_bottom":0,
        "fvg_top":None,"fvg_bottom":None,"has_fvg":False,"atr":0,
        "level_type":"","htf":"",
    }
    sweep_lb   =getattr(config,"UNICORN_SWEEP_LB",30)
    req_fvg    =getattr(config,"UNICORN_REQUIRE_FVG",True)
    rr         =getattr(config,"UNICORN_RR",2.0)
    direction  =getattr(config,"DIRECTION","BOTH")

    if len(candles_5m)<80 or len(candles_1h)<3: return R

    atr=_atr(candles_5m,14); R["atr"]=atr

    # Recopilar niveles de todos los HTF
    levels=[]
    if candles_15m and len(candles_15m)>=3:
        levels+=_get_htf_levels(candles_15m,"15m")
    if candles_30m and len(candles_30m)>=3:
        levels+=_get_htf_levels(candles_30m,"30m")
    levels+=_get_htf_levels(candles_1h,"1H")
    if not levels: return R

    def _try_setup(level, bull):
        sw=_check_sweep(candles_5m,level,sweep_lb)
        if sw is None: return None
        br=_find_breaker(candles_5m,sw,"BULL" if bull else "BEAR")
        if br is None: return None
        _,_,b_top,b_bot=br
        # Validar que el breaker no sobrepasa el extremo del sweep
        if bull:
            sw_ext=min(c["low"] for c in candles_5m[sw:min(sw+5,len(candles_5m))])
            if b_bot<sw_ext: return None
            last=candles_5m[-2]["close"]
            if last<=b_top: return None          # no confirmado aún
        else:
            sw_ext=max(c["high"] for c in candles_5m[sw:min(sw+5,len(candles_5m))])
            if b_top>sw_ext: return None
            last=candles_5m[-2]["close"]
            if last>=b_bot: return None
        ft,fb=_find_fvg(candles_5m,b_top,b_bot,"BULL" if bull else "BEAR")
        has_fvg=ft is not None
        if req_fvg and not has_fvg: return None
        entry=last
        if bull:
            sl=b_bot-atr*0.2; risk=entry-sl
            if risk<=0: return None
            tp=entry+rr*risk
        else:
            sl=b_top+atr*0.2; risk=sl-entry
            if risk<=0: return None
            tp=entry-rr*risk
        return {"signal":"LONG" if bull else "SHORT","entry_price":entry,
                "sl_price":sl,"tp_price":tp,"swept_level":level["price"],
                "breaker_top":b_top,"breaker_bottom":b_bot,
                "fvg_top":ft,"fvg_bottom":fb,"has_fvg":has_fvg,
                "level_type":level.get("type",""),"htf":level.get("htf","1H"),"atr":atr}

    if direction in ("LONG","BOTH"):
        for lv in sorted([l for l in levels if not l["is_high"]],
                         key=lambda x:x["price"],reverse=True):
            res=_try_setup(lv,bull=True)
            if res: R.update(res); return R

    if direction in ("SHORT","BOTH"):
        for lv in sorted([l for l in levels if l["is_high"]],
                         key=lambda x:x["price"]):
            res=_try_setup(lv,bull=False)
            if res: R.update(res); return R

    return R


def check_tp_exit(candles, side, tp_price):
    if not tp_price or not candles: return False
    last=candles[-2]["close"]
    return (side=="LONG" and last>=tp_price*0.999) or \
           (side=="SHORT" and last<=tp_price*1.001)
