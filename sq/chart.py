"""
sq/chart.py  [v1.1]
Trade Desk — เว็บกราฟแบบ TradingView (Lightweight Charts) + จุดสำคัญตามกลยุทธ์ สำหรับใช้ระหว่างตลาดเปิด
ระบบแยก อ่านอย่างเดียว: ไม่แตะ Hub / Shortlist / Govern / Audit · อ่านผลจาก Early Trend · Mid-Long Lab · ทองคำ (ส่วนถัดไป)

  python3 -m sq.chart serve [--port 8810]   เปิด http://127.0.0.1:8810 (มี intraday + ราคาล่าสุดทุก 60 วิ)
  python3 -m sq.chart plan                  พิมพ์แผน + ระดับราคาของวันทำการถัดไป (ใช้ก่อนตลาดเปิด)
  python3 -m sq.chart export [--out site]   เว็บ static (แผน + กราฟรายวัน · ไม่มี intraday)
  python3 -m sq.chart selftest

หลักการ: ทุกเส้นมาจากกฎเดียวกับ backtest — หน้าเว็บไม่สร้างสัญญาณใหม่
  · ราคาผ่านระดับระหว่างวัน ≠ สัญญาณ (สัญญาณ = ราคาปิด + volume ทั้งวัน · ตัดสินหลังตลาดปิด)
  · Early Trend เข้าที่ราคาเปิด · stop/เพดานคำนวณใหม่จากราคาเปิดจริง (L4: เสี่ยง > 3 ATR หรือ > 8% หรือต้นทุน > 0.30R = ข้าม)
  · ไม้ที่ถือ: stop แตะระหว่างวัน = ออก · +2R ขาย 1/3 แล้ว stop = ทุน · Chandelier / failed BO / time stop ตัดสินที่ราคาปิด
  · Mid-Long: ซื้อขายเฉพาะวันทำการแรกของเดือนที่ราคาเปิด · ระหว่างเดือนแจ้งเตือนเท่านั้น
ข้อมูล Yahoo ล่าช้า ~15 นาที · ใช้ส่วนตัว — เว็บไซต์สาธารณะต้องใช้ข้อมูลที่มีสิทธิ์เผยแพร่
"""
import argparse
import logging
import math
import os
import shutil
import tempfile
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from sq import etrend as ET

__version__ = "1.1"
log = logging.getLogger("sq.chart")
WEB = Path(__file__).resolve().parent / "web"
HTML = WEB / "chart.html"
MID_RES = Path(os.getenv("SQ_MID_RESULT", "sq_mid_result.json"))
MID_DATA = Path(os.getenv("SQ_MID_DATA", "sq_mid_data.pkl"))
EXTRA = [Path(os.getenv("SQ_GOLD_RESULT", "sq_gold_result.json"))]
IV = {"5m": "5d", "15m": "1mo", "60m": "6mo"}
US = {"GLD", "IAU", "SPY"}
WATCH_MAX = 40
GROUPS = [("ET_ENTRY", "🌱 Early Trend · ซื้อที่ราคาเปิด"), ("ET_OPEN", "🌱 Early Trend · ถืออยู่ (paper)"),
          ("ET_WATCH", "🌱 Early Trend · เฝ้าระดับ (ตัดสินที่ราคาปิด)"), ("MID_TRADE", "📅 Mid-Long · rebalance"),
          ("MID_HOLD", "📅 Mid-Long · พอร์ตโมเดล"), ("GOLD", "🥇 ทองคำ")]
_L = threading.RLock()
_C: Dict[str, Any] = {}


def _cache(key, ttl, fn):
    with _L:
        hit = _C.get(key)
        if hit is not None and time.time() - hit[0] < ttl:
            return hit[1]
    val = fn()
    with _L:
        _C[key] = (time.time(), val)
    return val


def sym_of(s) -> str:
    s = str(s or "").strip().upper()
    return s[:-3] if s.endswith(".BK") else s


def ticker(sym) -> str:
    raw = str(sym or "").strip().upper()
    if raw.startswith("^") or "=" in raw:
        return raw
    s = sym_of(raw)
    if not s or "." in s or "-" in s or s in US:
        return s
    return s + ".BK"


# ═══════════════ DATA ═══════════════
def _pick(raw, tk, nb):
    if raw is None or raw.empty:
        return None
    if isinstance(raw.columns, pd.MultiIndex):
        if tk in raw.columns.get_level_values(0):
            return raw[tk]
        if tk in raw.columns.get_level_values(1):
            return raw.xs(tk, axis=1, level=1)
        return None
    return raw if nb == 1 else None


def _dl(tks: List[str]) -> Dict[str, pd.DataFrame]:
    import yfinance as yf
    out = {}
    for i in range(0, len(tks), 40):
        b = tks[i:i + 40]
        try:
            raw = yf.download(b, period="2y", interval="1d", auto_adjust=True, group_by="ticker", threads=True,
                              progress=False)
        except Exception as e:
            log.warning("yfinance daily: %s", e)
            continue
        for tk in b:
            df = ET._clean(_pick(raw, tk, len(b)))
            if df is not None:
                out[tk] = df
    return out


def _pkl(sym) -> Optional[pd.DataFrame]:
    for p in (ET.DATA_FILE, MID_DATA):
        try:
            if p.is_file():
                c = _cache(f"pkl|{p}|{p.stat().st_mtime}", 86400, lambda p=p: pd.read_pickle(p))
                d = (c.get("frames") or {}).get(sym)
                if d is not None and len(d):
                    return d
        except Exception as e:
            log.warning("pickle %s: %s", p, e)
    return None


def daily_many(syms) -> Dict[str, Optional[pd.DataFrame]]:
    syms = sorted({sym_of(s) for s in syms if s})
    now = time.time()
    with _L:
        need = [s for s in syms if not (f"d|{s}" in _C and now - _C[f"d|{s}"][0] < 900)]
    if need:
        got = _dl([ticker(s) for s in need])
        with _L:
            for s in need:
                _C[f"d|{s}"] = (time.time(), got.get(ticker(s)))
    with _L:
        out = {s: _C[f"d|{s}"][1] for s in syms}
    return {s: (d if d is not None else _pkl(s)) for s, d in out.items()}


def _closed(df):
    """ตัดแท่งวันนี้ถ้ายังไม่ปิด (วันทำการก่อน 17:00)"""
    now = datetime.now()
    if df is None or not len(df) or now.weekday() >= 5 or now.hour >= 17:
        return df
    return df[df.index < pd.Timestamp(now.date())]


def _upto(df, asof):
    return df[df.index <= pd.Timestamp(str(asof)[:10])]


def atr(df, n=14) -> pd.Series:
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


def last_bday_of_month(d: date) -> date:
    e = (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    while e.weekday() >= 5:
        e -= timedelta(days=1)
    return e


# ═══════════════ LEVELS ═══════════════
def L(price, label, kind):
    x = ET._n(price)
    return None if x is None else {"price": round(x, 4), "label": label, "kind": kind}


def item(sym, strat, group, title, levels, rules, calc=None, markers=None, info=None):
    return {"symbol": sym, "strategy": strat, "group": group, "title": title, "levels": [l for l in levels if l],
            "rules": [r for r in rules if r], "calc": calc or {}, "markers": [m for m in (markers or []) if m.get("time")],
            "info": info or {}}


def entry_calc(c: Dict[str, Any], op: float) -> Dict[str, Any]:
    """กฎ L4 ของ Early Trend จากราคาเปิดจริง (เหมือน etrend._plan) — หน้าเว็บใช้สูตรเดียวกัน"""
    a = float(c["atr"])
    st = float(c["st_hl"])
    if op - st < c["stop_min_atr"] * a:
        st = op - c["stop_min_atr"] * a
    risk = op - st
    why = None
    if st <= 0 or risk <= 0:
        why = "stop ไม่ถูกต้อง"
    elif risk > c["stop_max_atr"] * a + 1e-12:
        why = f"เสี่ยง > {c['stop_max_atr']:g} ATR"
    elif risk / op * 100 > c["stop_max_pct"] + 1e-12:
        why = f"เสี่ยง > {c['stop_max_pct']:g}%"
    elif c.get("rt_pct") is not None and c["rt_pct"] / 100 * op / risk > c["max_cost_r"]:
        why = f"ต้นทุน > {c['max_cost_r']:g}R"
    return {"entry": op, "stop": st, "risk_pct": risk / op * 100 if risk > 0 else None, "t2r": op + c["be_r"] * risk,
            "skip": why is not None, "why": why}


def pos_state(df, asof, en, st0, P) -> Dict[str, Any]:
    """สถานะไม้ที่ถือจากแท่งปิดแล้ว (ลำดับเดียวกับ etrend.sim)"""
    w = df[df.index > pd.Timestamp(str(asof)[:10])]
    A = atr(df, int(P.get("atr_len")))
    rd = en - st0
    be, pf, tra = float(P.get("be_r")), float(P.get("part_frac")), float(P.get("trail_atr"))
    tgt, part, stop, hh, ch, mfe = en + be * rd, False, st0, en, -math.inf, 0.0
    for k, row in w.iterrows():
        h = float(row["high"])
        if h != h:
            continue
        if not part and pf > 0 and h >= tgt:
            part, stop = True, max(stop, en)
        mfe, hh = max(mfe, (h - en) / rd), max(hh, h)
        a = A.loc[k]
        if a == a:
            ch = max(ch, hh - tra * float(a))
    return {"held": len(w), "part": part, "stop_now": stop, "t2r": tgt, "chand": None if ch == -math.inf else ch,
            "mfe": mfe, "entry_date": str(w.index[0].date()) if len(w) else None}


def et_entry(e, df, asof, P, trade):
    if df is None:
        return None
    w = _upto(df, asof)
    if len(w) < 30:
        return None
    a = ET._n(atr(w, int(P.get("atr_len"))).iloc[-1])
    ref, st, lvl, cr = ET._n(e.get("ref")), ET._n(e.get("stop")), ET._n(e.get("lvl")), ET._n(e.get("cost_R"))
    if not a or not ref:
        return None
    hl = float(w["low"].iloc[-int(P.get("hl_len")):].min()) - float(P.get("hl_buf_atr")) * a
    mxa, mxp, be = float(P.get("stop_max_atr")), float(P.get("stop_max_pct")), float(P.get("be_r"))
    en_max = min(hl + mxa * a, hl / (1 - mxp / 100)) if hl > 0 else None
    rt = cr * (ref - st) / ref * 100 if (cr is not None and st and ref > st) else ET.cost_rt_pct(ref, trade, 0, None, None, P)
    calc = {"type": "et_entry", "st_hl": hl, "atr": a, "stop_min_atr": float(P.get("stop_min_atr")), "stop_max_atr": mxa,
            "stop_max_pct": mxp, "be_r": be, "rt_pct": rt, "max_cost_r": float(P.get("max_cost_r")), "en_max": en_max}
    nm = "pivot" if e.get("setup") == "A" else "ระดับ breakout"
    lv = [L(en_max, "เพดานราคาเปิด (เกิน = ข้าม)", "ceiling"), L(lvl, f"{nm} (failed BO)", "level"),
          L(ref, "ปิดวันสัญญาณ", "ref"), L(st, "stop (ถ้าเปิดที่ราคาปิด)", "stop"),
          L(ref + be * (ref - st) if st and ref > st else None, f"+{be:g}R (ถ้าเปิดที่ราคาปิด)", "target")]
    rules = [f"ซื้อที่ราคาเปิด {trade} ตามกฎ backtest · ไม่ไล่ราคาระหว่างวัน (ยังไม่ได้ทดสอบ)",
             f"ราคาเปิด > {en_max:.2f} → ข้ามไม้ (เสี่ยง > {mxa:g} ATR หรือ > {mxp:g}%)" if en_max else None,
             f"stop = {hl:.2f} หรือ ราคาเปิด − {float(P.get('stop_min_atr')):g} ATR ถ้าใกล้กว่า · แตะระหว่างวัน = ออก (gap ใต้ stop = ขายที่ราคาเปิด)",
             f"แตะ +{be:g}R → ขาย 1/3 แล้ว stop = ทุน · ที่เหลือใช้ Chandelier {float(P.get('trail_atr')):g} ATR (ตัดสินที่ราคาปิด)",
             f"ปิดต่ำกว่า {lvl:.2f} ภายใน {int(P.get('fail_bars'))} แท่ง + volume ≥ ค่าเฉลี่ย 50 วัน → ขายที่ราคาเปิดวันถัดไป" if lvl else None,
             f"ครบ {int(P.get('time_bars'))} แท่งแล้วยังไม่เคยกำไร {float(P.get('time_min_r')):g}R → ขายที่ราคาเปิดวันถัดไป",
             f"ขนาด {e.get('shares')} หุ้น · เสี่ยง {e.get('risk_pct')}% · ต้นทุนไป-กลับ {cr}R · RS rank {e.get('rs_rank')}"]
    return item(e["symbol"], "ET", "ET_ENTRY", f"Setup {e.get('setup')} · ซื้อที่ราคาเปิด {trade}", lv, rules, calc,
                [{"time": str(asof)[:10], "text": "สัญญาณ", "kind": "level", "shape": "arrowUp", "pos": "belowBar"}],
                {"shares": e.get("shares")})


def et_open(r, sig, df, P):
    if df is None:
        return None
    en, st0 = ET._n(r.get("entry")), ET._n(r.get("stop"))
    if not en or st0 is None or st0 >= en:
        return None
    ps = pos_state(_closed(df), r["asof"], en, st0, P)
    lvl = ET._n(sig.get("lvl"))
    fb, tb, be = int(P.get("fail_bars")), int(P.get("time_bars")), float(P.get("be_r"))
    lv = [L(ps["stop_now"], "stop = ทุน (หลัง +2R)" if ps["part"] else "stop", "stop"),
          L(None if ps["part"] else ps["t2r"], f"+{be:g}R ขาย 1/3", "target"),
          L(ps["chand"], "Chandelier (ปิดต่ำกว่า = ขายวันถัดไป)", "trail"),
          L(lvl if ps["held"] <= fb else None, f"failed BO (เหลือ {max(0, fb - ps['held'])} แท่ง)", "level"),
          L(en, "ราคาเข้า", "entry")]
    pend = r.get("pending")
    rules = [f"⚠ เงื่อนไขออกเกิดที่ราคาปิดล่าสุด ({pend}) → ขายที่ราคาเปิดวันนี้" if pend else None,
             f"ถือมา {ps['held']} แท่ง · กำไรสูงสุด {ps['mfe']:+.2f}R" + (" · ขาย 1/3 แล้ว" if ps["part"] else ""),
             f"time stop: ครบ {tb} แท่ง (อีก {max(0, tb - ps['held'])}) ถ้ากำไรสูงสุดยังไม่ถึง {float(P.get('time_min_r')):g}R → ขาย"
             if ps["mfe"] < float(P.get("time_min_r")) else None,
             "stop แตะระหว่างวัน = ออกที่ stop · เส้นอื่นตัดสินที่ราคาปิดแล้วขายที่ราคาเปิดวันถัดไป",
             "ไม้ paper จาก forward log — ยังไม่ใช่พอร์ตจริง"]
    return item(r["symbol"], "ET", "ET_OPEN", f"Setup {r.get('setup')} · เข้า {ps['entry_date']}", lv, rules,
                {"type": "et_open", "stop_now": ps["stop_now"], "t2r": ps["t2r"], "part": ps["part"], "pending": pend},
                [{"time": str(r["asof"])[:10], "text": "สัญญาณ", "kind": "level", "shape": "arrowUp", "pos": "belowBar"},
                 {"time": ps["entry_date"], "text": "เข้า", "kind": "entry", "shape": "circle", "pos": "belowBar"}])


def et_watch(wi, df, asof, P):
    if df is None:
        return None
    w = _upto(df, asof)
    if len(w) < 60:
        return None
    a = ET._n(atr(w, int(P.get("atr_len"))).iloc[-1])
    lvl = ET._n(wi.get("level"))
    if not a or not lvl:
        return None
    v = w["volume"].iloc[-50:]
    vma = float(v.mean()) if v.notna().sum() >= 40 else None
    setup = wi.get("setup")
    mult = float(P.get("vol_mult_a" if setup == "A" else "vol_mult_b"))
    thr = None if vma is None else mult * vma
    lmax = lvl + float(P.get("ext_atr")) * a if setup == "B" else None
    hl = float(w["low"].iloc[-int(P.get("hl_len")):].min()) - float(P.get("hl_buf_atr")) * a
    lv = [L(lvl, "ต้องปิดเหนือระดับนี้", "level"), L(lmax, "ปิดเกินนี้ = ไล่ราคา (ไม่ใช่สัญญาณ B)", "ceiling"),
          L(hl, "stop โดยประมาณ ถ้าเกิดสัญญาณ", "stop")]
    rules = [f"ผ่านระดับระหว่างวัน ≠ สัญญาณ — ต้องปิด > {lvl:.2f} และ volume ทั้งวัน ≥ {mult:g}× ค่าเฉลี่ย 50 วัน"
             + (f" ({thr:,.0f} หุ้น)" if thr else ""),
             "ถ้าเกิดสัญญาณ → แผนซื้อที่ราคาเปิดวันทำการถัดไปจะขึ้นหลังรันหลังตลาดปิด",
             f"ห่างระดับ {wi.get('dist_atr')} ATR ณ ราคาปิด {wi.get('close')}"]
    return item(wi["symbol"], "ET", "ET_WATCH", f"Setup {setup} · ระดับ {lvl:.2f}", lv, rules,
                {"type": "et_watch", "level": lvl, "level_max": lmax, "atr": a, "vol_thr": thr})


def _et_items(rep, D, P) -> List[Dict[str, Any]]:
    sc, fw = rep.get("scan") or {}, rep.get("forward") or {}
    asof = sc.get("asof")
    if not asof:
        return []
    trade = str(ET.next_bday(date.fromisoformat(str(asof)[:10])))
    sig = {f"{e.get('asof')}|{e.get('symbol')}|{e.get('setup')}": e for e in ET.read_lines(ET.SIG_LOG)}
    out = [et_entry(e, D.get(e["symbol"]), asof, P, trade) for e in sc.get("entries") or [] if not e.get("skip")]
    out += [et_open(r, sig.get(f"{r.get('asof')}|{r.get('symbol')}|{r.get('setup')}") or {}, D.get(r["symbol"]), P)
            for r in fw.get("open_rows") or []]
    out += [et_watch(w, D.get(w["symbol"]), asof, P) for w in (sc.get("watch") or [])[:WATCH_MAX]]
    return [x for x in out if x]


def _mid_items(rep, D) -> List[Dict[str, Any]]:
    td = rep.get("today") or {}
    asof = td.get("asof")
    if not asof:
        return []
    nxt = last_bday_of_month(date.fromisoformat(str(asof)[:10]))
    out = []
    for c in td.get("current") or []:
        df = D.get(c["symbol"])
        w = _upto(df, asof) if df is not None else None
        s200 = ET._n(w["close"].rolling(200, min_periods=180).mean().iloc[-1]) if w is not None and len(w) >= 180 else None
        rp = ET._n(c.get("ret_pct"))
        out.append(item(c["symbol"], "MID", "MID_HOLD", f"พอร์ตโมเดล {td.get('arm')} · {c['w_now']:.1f}%",
                        [L(s200, "SMA200 (หลุด = แจ้งเตือน)", "alert"), L(c.get("price"), "ราคาปิดล่าสุด", "ref")],
                        [f"น้ำหนัก {c['w_now']:.1f}% · ตั้งแต่ rebalance {'—' if rp is None else f'{rp:+.1f}%'}",
                         f"ระหว่างเดือนไม่ซื้อขาย (ตามกฎ backtest) · rebalance ครั้งถัดไปหลังราคาปิด {nxt}",
                         c.get("flag")], {"type": "mid_hold", "sma200": s200}))
    if td.get("is_signal"):
        for ch in td.get("changes") or []:
            tp = ET._n(ch.get("tick_pct"))
            out.append(item(ch["symbol"], "MID", "MID_TRADE", f"{ch['action']} · rebalance {td.get('exec_date')}",
                            [L(ch.get("price"), "ราคาปิดสิ้นเดือน (อ้างอิง)", "ref")],
                            [f"{ch['action']} {ch['w_cur']:.1f}% → {ch['w_tgt']:.1f}% · เป้า {ch.get('shares_tgt')} หุ้น (จากทุนในค่าตั้ง)",
                             f"ซื้อขายที่ราคาเปิด {td.get('exec_date')} ตามกฎ backtest · ทดสอบส่งช้า 5 วันแล้วผลแย่ลง → อย่าเลื่อน",
                             None if tp is None else f"1 ช่องราคา ≈ {tp:.2f}%"],
                            {"type": "mid_trade"}, None, {"action": ch["action"]}))
    return out


# ═══════════════ PLAN ═══════════════
def _build(ep, mp, ex) -> Dict[str, Any]:
    er, mr = ET._jload(ep), ET._jload(mp)
    sc, td, fw = er.get("scan") or {}, mr.get("today") or {}, er.get("forward") or {}
    syms = {e["symbol"] for e in sc.get("entries") or [] if not e.get("skip")}
    syms |= {r["symbol"] for r in fw.get("open_rows") or []}
    syms |= {w["symbol"] for w in (sc.get("watch") or [])[:WATCH_MAX]}
    syms |= {c["symbol"] for c in td.get("current") or []}
    if td.get("is_signal"):
        syms |= {c["symbol"] for c in td.get("changes") or []}
    D = daily_many(sorted(syms)) if syms else {}
    P = ET.POLICY.snap()
    items, errs, src = [], [], {}
    for name, label, rep, sub, fn in (("etrend", "Early Trend", er, sc, lambda: _et_items(er, D, P)),
                                      ("midterm", "Mid-Long Lab", mr, td, lambda: _mid_items(mr, D))):
        src[name] = {"name": label, "ok": bool(rep), "state": rep.get("state"), "state_label": rep.get("state_label"),
                     "asof": sub.get("asof"), "stale": sub.get("stale"), "computed_at": rep.get("computed_at")}
        if rep:
            try:
                items += fn()
            except Exception as e:
                log.exception("plan %s", name)
                errs.append(f"{name}: {type(e).__name__}: {e}")
    for p in ex:
        rp = ET._jload(p)
        src[p.stem] = {"name": rp.get("name") or p.stem, "ok": bool(rp), "state": rp.get("state"),
                       "state_label": rp.get("state_label"), "asof": rp.get("asof"), "computed_at": rp.get("computed_at")}
        items += [i for i in rp.get("plan_items") or [] if isinstance(i, dict) and i.get("symbol")]
    days = [str(s["asof"])[:10] for s in src.values() if s.get("asof")]
    trade = str(ET.next_bday(date.fromisoformat(max(days)))) if days else None
    return {"version": __version__, "generated_at": ET._now(), "trade_date": trade,
            "old": bool(trade and trade < date.today().isoformat()),
            "paper_only": not any(s.get("state") == "READY" for s in src.values()), "sources": src, "items": items,
            "errors": errs}


def plan(force=False, et_path=None, mid_path=None, extra=None) -> Dict[str, Any]:
    ep, mp = Path(et_path or ET.RES_FILE), Path(mid_path or MID_RES)
    ex = EXTRA if extra is None else list(extra)
    key = "plan|" + "|".join(f"{p}:{p.stat().st_mtime if p.is_file() else 0}" for p in [ep, mp, *ex])
    if force:
        with _L:
            _C.pop(key, None)
    return _cache(key, 300, lambda: _build(ep, mp, ex))


def plan_text(pl) -> str:
    out = [f"แผนวันทำการ {pl.get('trade_date')} · สร้าง {pl.get('generated_at')}" + (" · ⚠ แผนเก่า" if pl.get("old") else "")]
    for k, s in (pl.get("sources") or {}).items():
        out.append(f"  {s.get('name', k)}: {(s.get('state_label') or s.get('state')) if s.get('ok') else 'ไม่มีผล'}"
                   f" · ข้อมูลถึง {s.get('asof')}" + (" · ⚠ ข้อมูลค้าง" if s.get("stale") else ""))
    if pl.get("paper_only"):
        out.append("  ⚠ PAPER เท่านั้น — ยังไม่มีกลยุทธ์ใดผ่านด่าน edge + forward paper")
    for g, lab in GROUPS:
        its = [i for i in pl.get("items") or [] if i.get("group") == g]
        if its:
            out.append(f"\n{lab} ({len(its)})")
            for i in its:
                out.append(f"  {i['symbol']:<8} {i['title']}")
                out.append("           " + " · ".join(f"{l['label']} {l['price']:.2f}" for l in i["levels"]))
    out += ["✕ " + e for e in pl.get("errors") or []]
    return "\n".join(out)


# ═══════════════ OHLC · QUOTES ═══════════════
def _intraday(tk, iv) -> Optional[pd.DataFrame]:
    import yfinance as yf
    try:
        raw = yf.Ticker(tk).history(period=IV[iv], interval=iv, auto_adjust=False, prepost=False, actions=False)
    except Exception as e:
        log.warning("intraday %s: %s", tk, e)
        return None
    if raw is None or raw.empty:
        return None
    cols = {str(c).lower(): c for c in raw.columns}
    if not all(k in cols for k in ("open", "high", "low", "close")):
        return None
    df = pd.DataFrame({k: pd.to_numeric(raw[cols[k]], errors="coerce") for k in ("open", "high", "low", "close")})
    df["volume"] = pd.to_numeric(raw[cols["volume"]], errors="coerce") if "volume" in cols else 0.0
    df = df.dropna(subset=["open", "high", "low", "close"])
    return df[~df.index.duplicated(keep="last")].sort_index()


def ohlc(symbol, interval="1d") -> Dict[str, Any]:
    sym = sym_of(symbol)
    tk = ticker(sym)
    prec = 1 if "=" in tk else 2
    base = {"symbol": sym, "ticker": tk, "precision": prec, "min_move": 10 ** -prec}
    if interval not in IV:
        df = daily_many([sym]).get(sym)
        if df is None or not len(df):
            return {**base, "interval": "1d", "bars": [], "error": "ไม่มีข้อมูลรายวัน"}
        df = df.dropna(subset=["open", "high", "low", "close"]).iloc[-600:]
        c = df["close"]
        ds = [str(i.date()) for i in df.index]
        vals = df[["open", "high", "low", "close", "volume"]].to_numpy(dtype=float).tolist()
        return {**base, "interval": "1d", "note": "รายวัน · ราคาปรับปันผล", "bars": [[d, *v] for d, v in zip(ds, vals)],
                "sma50": [[d, v] for d, v in zip(ds, c.rolling(50, min_periods=45).mean().tolist())],
                "sma200": [[d, v] for d, v in zip(ds, c.rolling(200, min_periods=180).mean().tolist())]}
    df = _cache(f"i|{tk}|{interval}", 60, lambda: _intraday(tk, interval))
    if df is None or not len(df):
        return {**base, "interval": interval, "bars": [], "error": "ไม่มีข้อมูล intraday"}
    vals = df[["open", "high", "low", "close", "volume"]].to_numpy(dtype=float).tolist()
    return {**base, "interval": interval, "note": f"{interval} · ล่าช้า ~15 นาที",
            "bars": [[int(t.timestamp()), *v] for t, v in zip(df.index, vals)]}


def quotes(symbols) -> Dict[str, Any]:
    syms = sorted({sym_of(s) for s in symbols if str(s).strip()})[:80]
    if not syms:
        return {"quotes": {}}

    def f():
        import yfinance as yf
        tks = [ticker(s) for s in syms]
        try:
            raw = yf.download(tks, period="1d", interval="5m", group_by="ticker", auto_adjust=False, prepost=False,
                              threads=True, progress=False)
        except Exception as e:
            return {"quotes": {}, "error": f"{type(e).__name__}: {e}"}
        out = {}
        for s, tk in zip(syms, tks):
            sub = _pick(raw, tk, len(tks))
            if sub is None:
                continue
            cols = {str(c).lower(): c for c in sub.columns}
            if not all(k in cols for k in ("open", "high", "low", "close")):
                continue
            sub = sub.dropna(subset=[cols["close"]])
            if not len(sub):
                continue
            t = sub.index[-1]
            dk = t.tz_convert("Asia/Bangkok").date() if getattr(t, "tzinfo", None) else t.date()
            out[s] = {"last": float(sub[cols["close"]].iloc[-1]), "open": float(sub[cols["open"]].iloc[0]),
                      "high": float(sub[cols["high"]].max()), "low": float(sub[cols["low"]].min()),
                      "vol": float(pd.to_numeric(sub[cols["volume"]], errors="coerce").fillna(0).sum()) if "volume" in cols else None,
                      "time": int(t.timestamp()), "date": str(dk)}
        return {"quotes": out, "at": ET._now()}
    return _cache("q|" + ",".join(syms), 60, f)


# ═══════════════ SERVER · EXPORT ═══════════════
def make_app():
    from fastapi import FastAPI, Query
    from fastapi.responses import FileResponse, JSONResponse
    app = FastAPI(title="SQ Trade Desk", version=__version__)

    def J(fn):
        try:
            return JSONResponse(ET._js(fn()))
        except Exception as e:
            log.exception("chart api")
            return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=502)

    @app.get("/")
    def index():
        return FileResponse(str(HTML), media_type="text/html")

    @app.get("/api/chart/plan")
    def a_plan(force: bool = Query(False)):
        return J(lambda: plan(force))

    @app.get("/api/chart/ohlc")
    def a_ohlc(symbol: str = Query(...), interval: str = Query("1d")):
        return J(lambda: ohlc(symbol, interval))

    @app.get("/api/chart/quotes")
    def a_quotes(symbols: str = Query("")):
        return J(lambda: quotes(symbols.split(",")))

    @app.get("/api/chart/selftest")
    def a_self():
        return J(selftest)
    return app


def export(out="site") -> Dict[str, Any]:
    import json
    o = Path(out)
    (o / "data" / "daily").mkdir(parents=True, exist_ok=True)
    shutil.copy2(HTML, o / "index.html")
    pl = plan(True)
    (o / "data" / "plan.json").write_text(json.dumps(ET._js(pl), ensure_ascii=False), "utf-8")
    syms = sorted({i["symbol"] for i in pl["items"]})
    for s in syms:
        (o / "data" / "daily" / f"{s}.json").write_text(json.dumps(ET._js(ohlc(s, "1d")), ensure_ascii=False), "utf-8")
    return {"out": str(o.resolve()), "symbols": len(syms), "items": len(pl["items"])}


# ═══════════════ SELFTEST ═══════════════
def selftest() -> Dict[str, Any]:
    res = []

    def T(name, fn):
        try:
            ok, det = fn()
        except Exception as e:
            ok, det = False, f"{type(e).__name__}: {e}"
        res.append({"name": name, "ok": bool(ok), "detail": str(det)})

    tk = [ticker("PTT"), ticker("ptt.bk"), ticker("GC=F"), ticker("^SET.BK"), ticker("GLD"), sym_of("KBANK.BK")]
    T("ชื่อ ticker: PTT → PTT.BK · GC=F / ^SET.BK / GLD ไม่เติม .BK",
      lambda: (tk == ["PTT.BK", "PTT.BK", "GC=F", "^SET.BK", "GLD", "KBANK"], str(tk)))
    rg = np.random.default_rng(1)
    ix = pd.bdate_range("2026-01-01", periods=40)
    c = 100 * np.exp(np.cumsum(rg.normal(0, 0.02, 40)))
    df = pd.DataFrame({"open": c, "high": c * 1.02, "low": c * 0.98, "close": c, "volume": 1e6}, index=ix)
    pc = np.r_[np.nan, c[:-1]]
    tr = np.nanmax(np.vstack([df["high"] - df["low"], np.abs(df["high"] - pc), np.abs(df["low"] - pc)]), axis=0)
    y = tr[0]
    for x in tr[1:]:
        y = y + (x - y) / 14
    T("ATR แบบ Wilder ตรงกับสูตรเดียวกับ etrend", lambda: (abs(atr(df).iloc[-1] - y) < 1e-9, f"{atr(df).iloc[-1]:.6f}"))
    cc = {"st_hl": 9.0, "atr": 0.5, "stop_min_atr": 1.5, "stop_max_atr": 3.0, "stop_max_pct": 8.0, "be_r": 2.0,
          "rt_pct": None, "max_cost_r": 0.3}
    r1, r2, r3 = entry_calc(cc, 9.5), entry_calc(cc, 10.4), entry_calc(cc, 9.78)
    r4 = entry_calc({**cc, "rt_pct": 3.0}, 9.5)
    T("ราคาเปิด 9.50 → stop 8.75 (ขั้นต่ำ 1.5 ATR) · +2R 11.00 · เปิด 10.40 → ข้าม (> 8%) · 9.78 ผ่าน · ต้นทุน 0.38R → ข้าม",
      lambda: (abs(r1["stop"] - 8.75) < 1e-9 and abs(r1["t2r"] - 11.0) < 1e-9 and not r1["skip"] and r2["skip"]
               and not r3["skip"] and r4["skip"], f"{r1['stop']} {r1['t2r']} · {r2['why']} · {r3['skip']} · {r4['why']}"))
    ix2 = pd.bdate_range("2026-01-01", periods=60)
    fl = pd.DataFrame({"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1e6}, index=ix2)
    fl.iloc[52, fl.columns.get_loc("high")] = 106.5
    ps = pos_state(fl, str(ix2[49].date()), 100.0, 97.0, ET._P())
    T("ไม้ที่ถือ: high 106.5 ≥ +2R (106) → ขาย 1/3 · stop = ทุน 100 · ถือ 10 แท่ง · MFE 2.17R",
      lambda: (ps["part"] and ps["stop_now"] == 100.0 and ps["held"] == 10 and abs(ps["mfe"] - 6.5 / 3) < 1e-9
               and ps["chand"] is not None, str({k: ps[k] for k in ("part", "stop_now", "held", "chand")})))
    wv = et_watch({"symbol": "X", "setup": "B", "level": 102.0, "dist_atr": 1.0, "close": 100.0}, fl.assign(high=101.0), str(ix2[59].date()), ET._P())
    T("เฝ้าระดับ B: เกณฑ์ volume = 1.3 × ค่าเฉลี่ย 50 วัน · เพดานไล่ราคา = ระดับ + 1 ATR (ATR = 2)",
      lambda: (abs(wv["calc"]["vol_thr"] - 1.3e6) < 1e-6 and abs(wv["calc"]["level_max"] - 104.0) < 1e-9,
               str(wv["calc"])))
    with tempfile.TemporaryDirectory() as td:
        pl = plan(True, Path(td) / "a.json", Path(td) / "b.json", [])
    T("ไม่มีไฟล์ผล → แผนว่าง ไม่ error · ระบุว่าไม่มีผล",
      lambda: (pl["items"] == [] and not pl["sources"]["etrend"]["ok"] and pl["paper_only"], str(pl["sources"])))
    lb = [last_bday_of_month(date(2026, 11, 12)), last_bday_of_month(date(2026, 5, 15))]
    T("วันทำการสุดท้ายของเดือน: พ.ย. 2569 → 30 · พ.ค. 2569 (31 = อาทิตย์) → 29",
      lambda: (lb == [date(2026, 11, 30), date(2026, 5, 29)], str(lb)))
    n_ok = sum(1 for r in res if r["ok"])
    return {"ok": n_ok == len(res), "passed": n_ok, "total": len(res), "tests": res, "version": __version__}


def _cli():
    ap = argparse.ArgumentParser(prog="python3 -m sq.chart")
    ap.add_argument("cmd", choices=("serve", "plan", "export", "selftest"))
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8810)
    ap.add_argument("--out", default="site")
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if a.cmd == "selftest":
        r = selftest()
        for t in r["tests"]:
            print(("✓ " if t["ok"] else "✕ ") + t["name"] + " — " + t["detail"])
        print(f"\nผ่าน {r['passed']} / {r['total']}")
    elif a.cmd == "plan":
        print(plan_text(plan(True)))
    elif a.cmd == "export":
        print(export(a.out))
    else:
        import uvicorn
        print(f"Trade Desk: http://{a.host}:{a.port}  (Ctrl+C เพื่อหยุด)")
        uvicorn.run(make_app(), host=a.host, port=a.port, log_level="warning")


if __name__ == "__main__":
    _cli()
# ═══ END sq/chart.py v1.1 ═══