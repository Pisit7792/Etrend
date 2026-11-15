"""
sq/midterm.py  [v1.2]
Mid-Long Lab — พอร์ตระยะกลาง-ยาว (ถือหลายสัปดาห์–หลายเดือน) · รันวันละครั้งหลังตลาดปิด · rebalance เดือนละครั้ง
ระบบแยก: ไม่ครอบ Hub · ไม่แตะ Shortlist / Edge / Govern / Costs / Audit · ใช้ helper ของ sq.etrend (ข้อมูล · ต้นทุน · สถิติ)

สมมติฐาน (ประกาศก่อนเห็นผล — จากงานวิจัย ไม่ได้จูนกับข้อมูลชุดนี้)
  EW  ทุกตัวที่ผ่านเกณฑ์ น้ำหนักเท่ากัน (ตัวแทนตลาด → ส่วนเกิน = เทียบ EW)
  T1  ราคา > SMA200 (time-series trend)
  M1  top 20% ผลตอบแทน 12 เดือนข้ามเดือนล่าสุด (12-1) · 10–20 ตัว
  M2  M1 เฉพาะ > SMA200 และ momentum > 0 (ไม่พอ = เงินสด)
  M3  M2 × ตัวคูณ regime (R1 1.0 · R2 0.6 · R3 0.5 · R4 0.2)
  M4  M3 + น้ำหนักกลับด้าน volatility + buffer (ตัวเดิมถือต่อถ้ายังอยู่ใน 40% บน) — แขนใช้จริงค่าเริ่มต้น
กฎ        สัญญาณ = ราคาปิดวันทำการสุดท้ายของเดือน → ซื้อขายที่ราคาเปิดวันถัดไป · ระหว่างเดือนไม่เทรด · เพดาน 10%/ตัว · ≤ 2% ADV
ต้นทุน    ค่าคอม + ค่าตลาด × VAT + ช่องราคาตามวันที่ (รวมตารางใหม่ 16 พ.ย. 2569) + impact k·σ·√(Q/ADV)
วัด edge  ส่วนเกินรายเดือน (t) · headroom ≥ 2 · ต้นทุน ×2 · ช้า 1 สัปดาห์ · random portfolio วันเดียวกัน · 3 ช่วง · 2 กลุ่ม regime
          · Sharpe vs ดัชนี/EW · DD vs ดัชนี · Deflated Sharpe (trial × 2 แขนหลัก) · sensitivity 6 ชุด
สถานะ    VOID · INSUFFICIENT · NOT_PROVEN · CONTRADICTED · BACKTEST_PASS → READY (forward paper ≥ 6 รอบ)
CLI      python3 -m sq.midterm selftest | backtest [--force] [--reps N] [--symbols A,B] | today
"""
import argparse
import csv
import hashlib
import json
import logging
import math
import os
import tempfile
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from sq import etrend as ET

__version__ = "1.2"
log = logging.getLogger("sq.midterm")
CFG_FILE = Path(os.getenv("SQ_MID_CFG", "sq_mid.json"))
DATA_FILE = Path(os.getenv("SQ_MID_DATA", "sq_mid_data.pkl"))
RES_FILE = Path(os.getenv("SQ_MID_RESULT", "sq_mid_result.json"))
FWD_LOG = Path(os.getenv("SQ_MID_FORWARD", "sq_mid_forward.jsonl"))
TRIALS = Path(os.getenv("SQ_MID_TRIALS", "sq_mid_trials.jsonl"))
REB_CSV = Path(os.getenv("SQ_MID_REBAL", "sq_mid_rebalances.csv"))

DEFAULTS: Dict[str, Any] = {
    "arm_live": "M4", "period_years": 15, "min_hist": 273, "min_price": 1.0, "min_adv_mb": 20.0, "adv_cap_pct": 2.0,
    "mom_long": 252, "mom_skip": 21, "sma": 200, "vol_len": 63, "top_pct": 20.0, "min_names": 10, "max_names": 20,
    "buffer_pct": 40.0, "max_w": 10.0, "min_pool": 30,
    "idx_sma": 200, "slope_len": 20, "breadth_sma": 50, "m_r1": 1.0, "m_r2": 0.6, "m_r3": 0.5, "m_r4": 0.2,
    "equity": 10000000.0, "comm_pct": 0.15, "venue_pct": 0.007, "vat_pct": 7.0, "ftt_pct": 0.0, "exec_ticks": 0.75,
    "impact_k": 1.0, "lag_days": 5,
    "null_reps": 200, "need_months": 60, "t_need": 2.0, "dsr_need": 0.95, "null_pct": 0.95, "head_min": 2.0,
    "fwd_need": 6, "stale_days": 3,
}
RANGES = {
    "period_years": (5, 25), "min_hist": (260, 600), "min_price": (0.0, 50.0), "min_adv_mb": (0.0, 1000.0),
    "adv_cap_pct": (0.1, 20.0), "mom_long": (63, 504), "mom_skip": (0, 63), "sma": (50, 300), "vol_len": (20, 252),
    "top_pct": (5.0, 50.0), "min_names": (1, 50), "max_names": (1, 100), "buffer_pct": (5.0, 80.0),
    "max_w": (2.0, 100.0), "min_pool": (2, 300), "idx_sma": (50, 300), "slope_len": (5, 60), "breadth_sma": (20, 200),
    "m_r1": (0.0, 1.0), "m_r2": (0.0, 1.0), "m_r3": (0.0, 1.0), "m_r4": (0.0, 1.0), "equity": (1e5, 1e10),
    "comm_pct": (0.0, 1.0), "venue_pct": (0.0, 0.1), "vat_pct": (0.0, 20.0), "ftt_pct": (0.0, 1.0),
    "exec_ticks": (0.0, 5.0), "impact_k": (0.0, 5.0), "lag_days": (1, 21), "null_reps": (10, 2000),
    "need_months": (24, 240), "t_need": (1.0, 5.0), "dsr_need": (0.5, 0.999), "null_pct": (0.5, 0.999),
    "head_min": (1.0, 10.0), "fwd_need": (3, 60), "stale_days": (1, 20)}
OPS = {"null_reps", "stale_days", "arm_live"}
ARMS = [("EW", "EW · ทุกตัวที่ผ่านเกณฑ์ (ตัวแทนตลาด)"), ("T1", "T1 · ราคา > SMA200"),
        ("M1", "M1 · Momentum 12-1 top 20%"), ("M2", "M2 · M1 + SMA200 + momentum > 0"),
        ("M3", "M3 · M2 × regime"), ("M4", "M4 · M3 + inverse-vol + buffer")]
MAIN = ("M3", "M4")
ABL = [("T1", "EW", "Trend SMA200"), ("M1", "EW", "Momentum 12-1"), ("M2", "M1", "Trend + momentum > 0"),
       ("M3", "M2", "Regime"), ("M4", "M3", "Inverse-vol + buffer")]
SENS = [({"mom_long": 189}, "momentum 9-1"), ({"mom_long": 126}, "momentum 6-1"), ({"top_pct": 15.0}, "top 15%"),
        ({"top_pct": 25.0}, "top 25%"), ({"sma": 150}, "SMA150"), ({"sma": 250}, "SMA250")]
STATE_TH = {"VOID": "วัดไม่ได้ (VOID)", "INSUFFICIENT": "ข้อมูลไม่พอ", "NOT_PROVEN": "ยังพิสูจน์ไม่ได้",
            "CONTRADICTED": "แย่กว่าตลาดอย่างมีนัย", "BACKTEST_PASS": "ผ่าน backtest — รอ forward paper",
            "READY": "ผ่าน backtest + forward paper", "FORWARD_CONTRADICTED": "forward paper แย่กว่าตลาดอย่างมีนัย",
            "—": "—"}
NOTES = [
    "universe = รายชื่อหุ้นปัจจุบัน (survivorship bias) → ผลย้อนหลังดีเกินจริง โดยเฉพาะปีเก่า · ต้องผ่าน forward paper",
    "ราคาปรับปันผล/split (auto_adjust) · เงินสดไม่มีดอกเบี้ย · ขนาดคำสั่งจริงคิดจาก equity ในค่าตั้ง",
    "รันก่อน 17:00 วันทำการ = ตัดแท่งวันนี้ (ยังไม่ปิด) · ระหว่างเดือนไม่เทรด (ราคาหลุด SMA200 = แจ้งเตือนเท่านั้น)",
    "ทุกค่าตั้งมาจากงานวิจัย (ไม่จูน) · เปลี่ยนค่าใด = trial ใหม่ → Deflated Sharpe เข้มขึ้น · เลือกแขนใช้จริงหลังเห็นผล = นับ 2 trial",
]


# ═══════════════ POLICY ═══════════════
class _P:
    def __init__(self, **kw):
        self.d = {**DEFAULTS, **kw}

    def get(self, k):
        return self.d[k]


class Policy:
    def __init__(self, path: Path = CFG_FILE):
        self.path, self._l, self.data = Path(path), threading.RLock(), dict(DEFAULTS)
        for e in self._apply(ET._jload(self.path))["errors"]:
            log.warning("sq_mid.json: %s", e)

    def _apply(self, patch):
        errs, ch = [], []
        for k, v in (patch or {}).items():
            if k not in DEFAULTS:
                continue
            d = DEFAULTS[k]
            try:
                if isinstance(d, str):
                    v = str(v).strip().upper()
                    if v not in MAIN:
                        raise ValueError
                else:
                    x = ET._n(v)
                    if x is None:
                        raise ValueError
                    lo, hi = RANGES.get(k, (0.0, 1e12))
                    x = max(lo, min(hi, x))
                    v = int(round(x)) if isinstance(d, int) else float(x)
            except Exception:
                errs.append(f"{k}: ค่าไม่ถูกต้อง")
                continue
            if self.data.get(k) != v:
                self.data[k] = v
                ch.append(k)
        return {"changed": ch, "errors": errs}

    def get(self, k):
        v = self.data.get(k)
        return DEFAULTS.get(k) if v is None else v

    def snap(self) -> _P:
        return _P(**self.data)

    def view(self):
        return {**self.data, "file": str(self.path), "defaults": DEFAULTS, "ranges": RANGES, "ops": sorted(OPS),
                "prereg_hash": prereg_hash(self)}

    def update(self, patch):
        with self._l:
            res = self._apply(patch or {})
            ok = ET._jsave(self.path, self.data) if res["changed"] else True
        return {"ok": ok and not res["errors"], **res, "policy": self.view()}


def prereg_hash(P) -> str:
    spec = {k: P.get(k) for k in sorted(DEFAULTS) if k not in OPS}
    return hashlib.sha1(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:12]


def _pdict(P) -> Dict[str, Any]:
    return dict(P.d) if hasattr(P, "d") else dict(P.data)


def trial_mark(P, path=TRIALS) -> int:
    h = prereg_hash(P)
    if h not in {e.get("key") for e in ET.read_lines(path)}:
        ET.append_line(path, {"key": h, "ts_release": ET._now(), "version": __version__,
                              "params": {k: P.get(k) for k in sorted(DEFAULTS) if k not in OPS}})
    return max(1, len({e.get("key") for e in ET.read_lines(path)}))


# ═══════════════ DATA ═══════════════
def _fetch(tickers: List[str], years: int) -> Dict[str, pd.DataFrame]:
    import yfinance as yf
    start = (date.today() - timedelta(days=int(365.25 * years) + 30)).isoformat()
    out = {}
    for i in range(0, len(tickers), 40):
        b = tickers[i:i + 40]
        try:
            raw = yf.download(b, start=start, interval="1d", auto_adjust=True, group_by="ticker", threads=True,
                              progress=False)
        except Exception as e:
            log.warning("yfinance: %s", e)
            continue
        if raw is None or raw.empty:
            continue
        for tk in b:
            sub = None
            if isinstance(raw.columns, pd.MultiIndex):
                if tk in raw.columns.get_level_values(0):
                    sub = raw[tk]
                elif tk in raw.columns.get_level_values(1):
                    sub = raw.xs(tk, axis=1, level=1)
            elif len(b) == 1:
                sub = raw
            df = ET._clean(sub) if sub is not None else None
            if df is not None:
                out[tk] = df
    return out


def load(symbols, P, force=False, say=None):
    syms = sorted({ET.norm(s) for s in symbols if s and not str(s).startswith("^")})
    today, yrs = date.today().isoformat(), int(P.get("period_years"))
    if not force and DATA_FILE.is_file():
        try:
            c = pd.read_pickle(DATA_FILE)
            if c.get("v") == 2 and c.get("day") == today and c.get("years") == yrs and set(syms) <= set(c.get("requested") or []):
                fr = {s: c["frames"][s] for s in syms if s in c["frames"]}
                return fr, c.get("index"), {**c["meta"], "requested": len(syms), "cached": True}
        except Exception as e:
            log.warning("cache: %s", e)
    if say:
        say(f"ดึงราคา {len(syms)} หุ้น + ดัชนี SET ย้อนหลัง {yrs} ปี (yfinance)…")
    got = _fetch([f"{s}.BK" for s in syms], yrs)
    idx, isrc = ET.fetch_index(yrs, say)
    fr = {ET.norm(k): v for k, v in got.items()}
    meta = {"fetched_at": ET._now(), "requested": len(syms), "got": len(fr), "missing": sorted(set(syms) - set(fr))[:200],
            "index_ok": idx is not None, "index_src": isrc, "cached": False}
    try:
        pd.to_pickle({"v": 2, "day": today, "years": yrs, "requested": syms, "frames": fr, "index": idx, "meta": meta}, DATA_FILE)
    except Exception as e:
        log.warning("บันทึก cache: %s", e)
    return fr, idx, meta


def prep(frames, idx_df, P, min_syms=5):
    g = P.get
    syms = sorted(s for s, d in frames.items() if d is not None and len(d))
    if idx_df is not None and len(idx_df) > 300:
        dates, grade = ET._axis(frames, syms, idx_df), ET._igrade(idx_df)
    else:
        dates = pd.DatetimeIndex(sorted(set().union(*[set(frames[s].index) for s in syms]))) if syms else pd.DatetimeIndex([])
        grade = "PROXY"

    def M(k):
        return pd.DataFrame({s: frames[s][k] for s in syms}, index=dates)
    O, H, L, C, V = (M(k) for k in ("open", "high", "low", "close", "volume"))
    with np.errstate(invalid="ignore"):
        bad = ((H < L * (1 - 1e-9)) | (C > H * 1.002) | (C < L * 0.998) | (C <= 0) | (O <= 0)
               | (O > H * 1.002) | (O < L * 0.998))
    nb, cnt = bad.sum(), C.notna().sum()
    O, C = O.mask(bad), C.mask(bad)
    V = V.mask(bad | (V <= 0))
    drop = [s for s in syms if cnt[s] < 60 or nb[s] > 0.05 * max(1, cnt[s])]
    keep = [s for s in syms if s not in drop]
    if len(keep) < min_syms:
        raise ValueError(f"หุ้นที่ใช้ได้ {len(keep)} ตัว (< {min_syms}) · ถูกกักกัน {len(drop)} ตัว")
    O, C, V = O[keep], C[keep], V[keep]
    if grade != "PROXY":
        I = pd.to_numeric(idx_df["close"], errors="coerce").reindex(dates).ffill(limit=5)
    else:
        I = (1 + C.pct_change(fill_method=None).mean(axis=1).fillna(0.0)).cumprod() * 1000  # proxy = EW
    with np.errstate(all="ignore"):
        lr = np.log(C).diff()
        sl, vl = int(g("sma")), int(g("vol_len"))
        sma = C.rolling(sl, min_periods=int(sl * 0.9)).mean()
        vol = lr.rolling(vl, min_periods=int(vl * 0.8)).std() * math.sqrt(252)
        mom = C.shift(int(g("mom_skip"))) / C.shift(int(g("mom_long"))) - 1
        hist = C.notna().cumsum()
        adv = (C * V).rolling(20, min_periods=15).median()
        advsh = V.rolling(20, min_periods=15).median()
        sg = lr.rolling(20, min_periods=15).std()
        ma = C.rolling(int(g("breadth_sma")), min_periods=40).mean()
        nma = ma.notna().sum(axis=1)
        br = ((C > ma) & ma.notna()).sum(axis=1) / nma.where(nma >= min(10, C.shape[1])) * 100
        ln = int(g("idx_sma"))
        isma = I.rolling(ln, min_periods=int(ln * 0.9)).mean()
        slope = (isma - isma.shift(int(g("slope_len")))) > 0
        above = I > isma
        reg = np.select([(isma.isna() | br.isna() | I.isna()).to_numpy(), (above & slope & (br >= 50)).to_numpy(),
                         above.to_numpy(), (br >= 40).to_numpy()], [4, 1, 2, 3], 4)
        Cf = C.ffill()
        Of = O.where(O.notna(), Cf)
        el = ((hist >= g("min_hist")) & mom.notna() & (C >= g("min_price")) & (adv >= g("min_adv_mb") * 1e6)
              & vol.notna() & sma.notna())
    mm = {1: g("m_r1"), 2: g("m_r2"), 3: g("m_r3"), 4: g("m_r4")}
    n = len(dates)
    sig = [t for t in range(n - 1) if dates[t].month != dates[t + 1].month]
    elv = el.to_numpy(dtype=bool)
    start = next((t for t in sig if elv[t].sum() >= int(g("min_pool"))), None)
    if start is None:
        raise ValueError(f"ไม่มีเดือนที่มีหุ้นผ่านเกณฑ์ ≥ {int(g('min_pool'))} ตัว — ข้อมูลไม่พอ")

    def A(X):
        return X.to_numpy(dtype=float)
    return SimpleNamespace(
        dates=dates, syms=keep, n=n, k=len(keep), o=A(O), c=A(C), cf=A(Cf), of=A(Of), sma=A(sma), vol=A(vol),
        mom=A(mom), adv=A(adv), advsh=A(advsh), sg=A(sg), el=elv, I=I.to_numpy(dtype=float),
        isma=isma.to_numpy(dtype=float), br=br.to_numpy(dtype=float), reg=reg.astype(int),
        m=np.array([mm[int(x)] for x in reg], dtype=float), sig=sig, start=start, grade=grade, quarantined=drop,
        bad_bars=int(nb.sum()))


# ═══════════════ SELECTION ═══════════════
def capw(w, cap):
    """เพดานน้ำหนักต่อตัว · ส่วนเกินกระจายให้ตัวที่ยังไม่เต็ม (ถ้าเต็มทุกตัว = เงินสด)"""
    w = np.asarray(w, dtype=float).copy()
    for _ in range(30):
        over = w > cap + 1e-12
        if not over.any():
            break
        ex = float((w[over] - cap).sum())
        w[over] = cap
        fr = ~over & (w < cap - 1e-12)
        if not fr.any() or ex <= 0:
            break
        w[fr] += ex * w[fr] / w[fr].sum()
    return np.minimum(w, cap)


def n_target(cnt, P) -> int:
    return int(min(P.get("max_names"), max(P.get("min_names"), round(cnt * P.get("top_pct") / 100))))


def picker(F, arm, P):
    g = P.get
    cap = g("max_w") / 100

    def f(t, prev):
        idx = np.flatnonzero(F.el[t])
        ex = float(F.m[t]) if arm in ("M3", "M4") else 1.0
        if not len(idx):
            return {}, ex, 0
        if arm == "EW":
            sel = [int(j) for j in idx]
        elif arm == "T1":
            sel = [int(j) for j in idx if F.c[t, j] > F.sma[t, j]]
        else:
            nt = n_target(len(idx), P)
            order = idx[np.argsort(-F.mom[t, idx], kind="stable")]
            if arm == "M1":
                sel = [int(j) for j in order[:nt]]
            else:
                ok = [int(j) for j in order if F.c[t, j] > F.sma[t, j] and F.mom[t, j] > 0]
                if arm == "M4" and prev:
                    rk = {int(j): r for r, j in enumerate(order)}
                    lim = len(order) * g("buffer_pct") / 100
                    keep = [j for j in ok if j in prev and rk[j] < lim][:nt]
                    ks = set(keep)
                    sel = keep + [j for j in ok if j not in ks][:max(0, nt - len(keep))]
                else:
                    sel = ok[:nt]
        if not sel:
            return {}, ex, 0
        sel = np.array(sel, dtype=int)
        if arm == "M4":
            iv = 1.0 / np.maximum(F.vol[t, sel], 0.05)
            w = iv / iv.sum()
        else:
            w = np.full(len(sel), 1.0 / len(sel))
        if arm != "EW":
            w = capw(w, cap)
        return {int(j): float(x * ex) for j, x in zip(sel, w)}, ex, len(sel)
    return f


def rnd_picker(F, P, plan, rng):
    """random portfolio: จำนวนตัว · exposure · จำนวนตัวที่ถือต่อจากเดือนก่อน เท่าแขนจริง (turnover ใกล้เคียงกัน)"""
    cap = P.get("max_w") / 100

    def f(t, prev):
        c, ex, kp = plan.get(t, (0, 1.0, 0))
        idx = np.flatnonzero(F.el[t])
        if not c or not len(idx):
            return {}, ex, 0
        c = min(int(c), len(idx))
        stay = np.array([j for j in sorted(prev) if F.el[t, j]], dtype=int)
        nk = min(int(kp), len(stay), c)
        keep = rng.choice(stay, size=nk, replace=False) if nk > 0 else np.array([], dtype=int)
        rest = np.setdiff1d(idx, keep)
        na = min(c - nk, len(rest))
        add = rng.choice(rest, size=na, replace=False) if na > 0 else np.array([], dtype=int)
        sel = np.concatenate([keep, add]).astype(int)
        if not len(sel):
            return {}, ex, 0
        w = capw(np.full(len(sel), 1.0 / len(sel)), cap)
        return {int(j): float(x * ex) for j, x in zip(sel, w)}, ex, len(sel)
    return f


# ═══════════════ SIMULATION ═══════════════
def _ratio(a, b):
    with np.errstate(all="ignore"):
        r = a / b
    return np.where(np.isfinite(r) & (r > 0), r, 1.0)


def sim(F, P, pick, start, lag=0, cost_mult=1.0, rec=False) -> Optional[Dict[str, Any]]:
    """สัญญาณ t (ราคาปิดสิ้นเดือน) → ซื้อขายที่ราคาเปิด t+1+lag · mark-to-market ราคาปิดรายวัน · ต้นทุนต่อข้าง = rt/2"""
    n, k = F.n, F.k
    sched = {t + 1 + lag: t for t in F.sig if t >= start and t + 1 + lag < n}
    if not sched:
        return None
    d0 = min(sched)
    E, acap = float(P.get("equity")), P.get("adv_cap_pct") / 100
    hold, cash, vprev = np.zeros(k), E, E
    ret, prev, logs, cf, tv = np.zeros(n - d0), set(), [], 0.0, 0.0
    for d in range(d0, n):
        if d in sched:
            t = sched[d]
            hold = hold * _ratio(F.of[d], F.cf[d - 1])
            vo = cash + hold.sum()
            w, ex, cnt = pick(t, prev)
            tgt = np.zeros(k)
            for j, x in w.items():
                tgt[j] = x * vo
            lim = F.adv[t] * acap
            tgt = np.where(np.isfinite(lim), np.minimum(tgt, lim), 0.0)
            tgt = np.where(np.isfinite(F.o[d]), tgt, hold)
            tr = np.abs(tgt - hold)
            c, on = 0.0, F.dates[d].date()
            for j in np.flatnonzero(tr > 1.0):
                px = F.o[d, j]
                s_, a_ = F.sg[t, j], F.advsh[t, j]
                c += tr[j] * ET.cost_rt_pct(px, on, tr[j] / px, float(s_) if s_ == s_ else None,
                                            float(a_) if a_ == a_ else None, P) / 200
            c *= cost_mult
            cash = vo - tgt.sum() - c
            hold = tgt * _ratio(F.cf[d], F.of[d])
            prev = set(np.flatnonzero(tgt > 0).tolist())
            cf += c / vo
            tv += tr.sum() / vo
            if rec:
                logs.append({"t": t, "e": d, "date": str(F.dates[t].date()), "n": cnt, "ex": ex, "turn": tr.sum() / vo,
                             "cost": c / vo, "w": {int(j): float(tgt[j] / vo) for j in np.flatnonzero(tgt > 0)}})
        else:
            hold = hold * _ratio(F.cf[d], F.cf[d - 1])
        v = cash + hold.sum()
        ret[d - d0] = v / vprev - 1
        vprev = v
    return {"ret": ret, "d0": d0, "cost": cf, "turn": tv, "logs": logs, "hold": hold, "V": vprev}


def mser(F, r, d0) -> pd.Series:
    s = pd.Series(r, index=F.dates[d0:d0 + len(r)])
    return (1 + s).groupby(s.index.to_period("M")).prod() - 1


def exyr(F, S, ew_m) -> Optional[float]:
    if S is None:
        return None
    e = (mser(F, S["ret"], S["d0"]) - ew_m).dropna()
    return float(e.mean() * 1200) if len(e) else None


# ═══════════════ STATS ═══════════════
def arm_stats(F, S, ew_m, reg_m, ew_cost_yr) -> Dict[str, Any]:
    r = S["ret"]
    yrs = len(r) / 252
    pf = ET._perf(r)
    ex = (mser(F, r, S["d0"]) - ew_m).dropna()
    x = ex.to_numpy()
    lgs = S["logs"]
    if lgs:
        exs = pd.Series([L["ex"] for L in lgs], index=pd.DatetimeIndex([F.dates[L["e"]] for L in lgs]).to_period("M"))
        exs = exs.groupby(level=0).last().reindex(ex.index).ffill().fillna(1.0)
    else:
        exs = pd.Series(1.0, index=ex.index)
    adj = (mser(F, r, S["d0"]).reindex(ex.index) - exs * ew_m.reindex(ex.index)).dropna().to_numpy()
    rg = reg_m.reindex(ex.index)
    good, bad = ex[rg.isin([1, 2]).to_numpy()], ex[rg.isin([3, 4]).to_numpy()]
    roll = ex.rolling(36).mean().dropna()
    cost_yr = S["cost"] / yrs * 100 if yrs > 0 else None
    ex_yr = float(x.mean() * 1200) if len(x) else None
    gross = None if ex_yr is None or cost_yr is None else ex_yr + cost_yr - ew_cost_yr
    lg = S["logs"]
    return {**pf, "months": len(x), "first": str(ex.index[0]) if len(x) else None,
            "last": str(ex.index[-1]) if len(x) else None, "ex_yr": ex_yr, "t": ET._t(x),
            "pos_m": float((x > 0).mean()) if len(x) else None,
            "folds": [float(f.mean() * 1200) for f in np.array_split(x, 3) if len(f)] if len(x) >= 3 else [],
            "reg_good": {"n": len(good), "ex_yr": float(good.mean() * 1200) if len(good) else None},
            "reg_bad": {"n": len(bad), "ex_yr": float(bad.mean() * 1200) if len(bad) else None},
            "roll36_pos": float((roll > 0).mean()) if len(roll) else None, "cost_yr": cost_yr,
            "turn_yr": S["turn"] / yrs * 100 if yrs > 0 else None,
            "head": (gross / cost_yr) if gross is not None and cost_yr and cost_yr > 0 else None,
            "avg_n": float(np.mean([L["n"] for L in lg])) if lg else None,
            "avg_ex": float(np.mean([L["ex"] for L in lg])) if lg else None, "rebalances": len(lg), "ex_adj_yr": float(adj.mean() * 1200) if len(adj) else None, "t_adj": ET._t(adj)}


def placebo(F, P, S, ew_m, start, reps, seed=17) -> Dict[str, Any]:
    """random portfolio วันเดียวกัน · จำนวนตัว/exposure เท่าแขนจริงในแต่ละเดือน · pool = หุ้นที่ผ่านเกณฑ์วันนั้น"""
    lg = S["logs"]
    plan = {L["t"]: (L["n"], L["ex"], len(set(L["w"]) & set(lg[i - 1]["w"])) if i else 0) for i, L in enumerate(lg)}
    e0 = (mser(F, S["ret"], S["d0"]) - ew_m).dropna()
    if len(e0) < 12:
        return {"available": False, "reason": "น้อยกว่า 12 เดือน"}
    obs, rng, dist = float(e0.mean()), np.random.default_rng(seed), []
    for _ in range(int(reps)):
        R = sim(F, P, rnd_picker(F, P, plan, rng), start)
        e = (mser(F, R["ret"], R["d0"]) - ew_m).dropna()
        if len(e):
            dist.append(float(e.mean()))
    if not dist:
        return {"available": False, "reason": "สุ่มไม่ได้"}
    d = np.array(dist)
    return {"available": True, "obs": obs * 1200, "null_mean": float(d.mean() * 1200), "p5": float(np.percentile(d, 5) * 1200),
            "p95": float(np.percentile(d, 95) * 1200), "pct": float((d < obs).mean()), "reps": len(d)}


def sensitivity(frames, idx, P, arm) -> List[Dict[str, Any]]:
    out = []
    for ch, lab in SENS:
        P2 = _P(**{**_pdict(P), **ch})
        try:
            F2 = prep(frames, idx, P2)
            ew = sim(F2, P2, picker(F2, "EW", P2), F2.start)
            S2 = sim(F2, P2, picker(F2, arm, P2), F2.start)
            ex = (mser(F2, S2["ret"], S2["d0"]) - mser(F2, ew["ret"], ew["d0"])).dropna().to_numpy()
            out.append({"param": lab, "ex_yr": float(ex.mean() * 1200), "t": ET._t(ex), "months": len(ex)})
        except Exception as e:
            out.append({"param": lab, "ex_yr": None, "error": f"{type(e).__name__}: {e}"})
    return out


def gates(a, idx_pf, ew_pf, void, P, trials, sens=None):
    g, s, G = P.get, a["stats"], []

    def add(k, lab, ok, det):
        G.append({"key": k, "label": lab, "ok": bool(ok), "detail": det})

    def f(x, d=2):
        return "—" if x is None else f"{x:+.{d}f}"
    add("N", f"≥ {int(g('need_months'))} เดือน", s["months"] >= g("need_months"),
        f"{s['months']} เดือน ({s.get('first')} → {s.get('last')})")
    add("T", f"ส่วนเกินเทียบ EW > 0 และ t รายเดือน ≥ {g('t_need'):g}",
        (s.get("ex_yr") or -1) > 0 and (s.get("t") or -9) >= g("t_need"), f"{f(s.get('ex_yr'))}%/ปี · t {f(s.get('t'))}")
    add("HEAD", f"ส่วนเกินก่อนต้นทุน ≥ {g('head_min'):g} เท่าของต้นทุน", (s.get("head") or 0) >= g("head_min"),
        f"headroom {f(s.get('head'))} · ต้นทุน {f(s.get('cost_yr'))}%/ปี · turnover {f(s.get('turn_yr'), 0)}%/ปี")
    add("COST2", "ต้นทุน ×2 ส่วนเกินยังบวก", (a.get("c2") or -1) > 0, f"{f(a.get('c2'))}%/ปี")
    add("LAG", f"ส่งคำสั่งช้า {int(g('lag_days'))} วันทำการ ส่วนเกินยังบวก", (a.get("lag") or -1) > 0, f"{f(a.get('lag'))}%/ปี")
    nl = a.get("null") or {}
    add("NULL", f"ชนะ random portfolio วันเดียวกัน ≥ P{g('null_pct') * 100:.0f}",
        nl.get("available") and nl["pct"] >= g("null_pct"),
        f"percentile {nl['pct'] * 100:.1f} · random {f(nl['null_mean'])}%/ปี (P95 {f(nl['p95'])})"
        if nl.get("available") else nl.get("reason", "—"))
    fo = s.get("folds") or []
    add("FOLD", "ส่วนเกินบวกทั้ง 3 ช่วงเวลา", len(fo) == 3 and all(x > 0 for x in fo), " · ".join(f(x) for x in fo) or "—")
    rg, rb = s.get("reg_good") or {}, s.get("reg_bad") or {}
    add("REGIME", "ส่วนเกินบวกทั้งตลาดดี (R1–R2) และตลาดไม่ดี (R3–R4) · ≥ 6 เดือนต่อกลุ่ม",
        rg.get("n", 0) >= 6 and rb.get("n", 0) >= 6 and (rg.get("ex_yr") or -1) > 0 and (rb.get("ex_yr") or -1) > 0,
        f"ดี {rg.get('n')} เดือน {f(rg.get('ex_yr'))} · ไม่ดี {rb.get('n')} เดือน {f(rb.get('ex_yr'))}")
    si, se = idx_pf.get("sharpe"), ew_pf.get("sharpe")
    add("SHARPE", "Sharpe > ถือดัชนี และ > EW",
        s.get("sharpe") is not None and si is not None and se is not None and s["sharpe"] > max(si, se),
        f"{f(s.get('sharpe'))} vs ดัชนี {f(si)} · EW {f(se)}")
    di = idx_pf.get("maxdd_pct")
    add("DD", "Max drawdown ไม่ลึกกว่าถือดัชนี", s.get("maxdd_pct") is not None and di is not None and s["maxdd_pct"] >= di,
        f"{f(s.get('maxdd_pct'), 1)}% vs {f(di, 1)}%")
    add("DSR", f"Deflated Sharpe ของส่วนเกินเทียบ EW ≥ {g('dsr_need'):g} (trial {trials})", (a.get("dsr") or 0) >= g("dsr_need"),
        f(a.get("dsr"), 3))
    if sens is not None:
        add("SENS", "พารามิเตอร์ใกล้เคียง 6 ชุด ส่วนเกินบวกทุกชุด", bool(sens) and all((x.get("ex_yr") or -1) > 0 for x in sens),
            " · ".join(f"{x['param']} {f(x.get('ex_yr'))}" for x in sens))
    if void:
        st = "VOID"
    elif s["months"] < 24:
        st = "INSUFFICIENT"
    elif (s.get("ex_yr") or 0) < 0 and (s.get("t") or 0) <= -2:
        st = "CONTRADICTED"
    elif all(x["ok"] for x in G):
        st = "BACKTEST_PASS"
    else:
        st = "NOT_PROVEN"
    return G, st


def ablation(arms) -> List[Dict[str, Any]]:
    out = []
    for k, b, lab in ABL:
        s1, s0 = arms[k]["stats"], arms[b]["stats"]
        if s1.get("ex_yr") is None or s0.get("ex_yr") is None or s1.get("sharpe") is None or s0.get("sharpe") is None:
            out.append({"layer": lab, "arm": k, "vs": b, "d_ex": None, "d_sh": None, "verdict": "ข้อมูลไม่พอ"})
            continue
        dx, ds = s1["ex_yr"] - s0["ex_yr"], s1["sharpe"] - s0["sharpe"]
        v = ("ช่วย" if dx > 0 and ds > 0 else "ปน (ส่วนเกินกับ Sharpe ไปคนละทาง)" if dx > 0 or ds > 0
             else "ไม่ช่วย — ตัดได้ (นับเป็น trial ใหม่)")
        out.append({"layer": lab, "arm": k, "vs": b, "d_ex": dx, "d_sh": ds, "verdict": v})
    return out


# ═══════════════ TODAY · FORWARD ═══════════════
def today(F, P, S) -> Dict[str, Any]:
    g, arm, t = P.get, P.get("arm_live"), F.n - 1
    asof = F.dates[t].date()
    lagd = int(np.busday_count(str(asof), str(date.today())))
    is_sig = ET.next_bday(asof).month != asof.month
    idx = np.flatnonzero(F.el[t])
    order = idx[np.argsort(-F.mom[t, idx], kind="stable")]
    rk = {int(j): round((1 - r / max(1, len(order))) * 100, 1) for r, j in enumerate(order)}
    hold, V = S["hold"], S["V"]
    last = S["logs"][-1] if S["logs"] else None
    held = [int(j) for j in np.flatnonzero(hold > 0)]
    cur = []
    for j in held:
        e = last["e"] if last else None
        p0 = F.of[e, j] if e is not None else np.nan
        tr_ok = bool(F.c[t, j] > F.sma[t, j]) if np.isfinite(F.c[t, j]) and np.isfinite(F.sma[t, j]) else None
        cur.append({"symbol": F.syms[j], "w_now": hold[j] / V * 100, "w_set": (last["w"].get(j, 0.0) * 100) if last else None,
                    "ret_pct": (F.cf[t, j] / p0 - 1) * 100 if p0 == p0 and p0 > 0 else None, "price": F.cf[t, j],
                    "trend_ok": tr_ok, "mom_rank": rk.get(j),
                    "flag": "ราคาหลุด SMA200 — แจ้งเตือน (ไม่ขายระหว่างเดือนตามกฎ)" if tr_ok is False else None})
    cur.sort(key=lambda x: -x["w_now"])
    w, ex, cnt = picker(F, arm, P)(t, set(held))
    chg = []
    for j in sorted(set(w) | set(held)):
        wc, wt = (hold[j] / V if V > 0 else 0.0), w.get(j, 0.0)
        if abs(wt - wc) < 0.01 and not (wt == 0 and wc > 0):
            continue
        act = "ซื้อ" if wc == 0 else "ขาย" if wt == 0 else ("เพิ่ม" if wt > wc else "ลด")
        px = F.cf[t, j]
        chg.append({"symbol": F.syms[j], "action": act, "w_cur": wc * 100, "w_tgt": wt * 100, "price": px,
                    "shares_tgt": int(wt * g("equity") / px // 100 * 100) if px == px and px > 0 else None,
                    "mom_rank": rk.get(j), "tick_pct": (ET.tick_at(px, ET.next_bday(asof)) or 0) / px * 100 if px == px and px > 0 else None})
    chg.sort(key=lambda x: ("ขาย ลด เพิ่ม ซื้อ".split().index(x["action"]), -x["w_tgt"]))
    return {"asof": str(asof), "lag_days": lagd, "stale": lagd > int(g("stale_days")), "is_signal": is_sig,
            "exec_date": str(ET.next_bday(asof)) if is_sig else None, "arm": arm, "regime": int(F.reg[t]),
            "m": float(F.m[t]), "breadth": float(F.br[t]) if F.br[t] == F.br[t] else None, "pool": int(len(idx)),
            "current": cur, "target_n": cnt, "target_exposure": ex, "changes": chg,
            "target": [{"symbol": F.syms[j], "w": x * 100, "mom_rank": rk.get(j)} for j, x in sorted(w.items(), key=lambda kv: -kv[1])],
            "_w": {F.syms[j]: round(x, 6) for j, x in w.items()}}


def fwd_log(td, P, path=FWD_LOG) -> int:
    if not td["is_signal"] or td["stale"]:
        return 0
    h = prereg_hash(P)
    return ET.append_line(path, {"key": f"{td['asof']}|{td['arm']}|{h}", "ts_release": ET._now(), "asof": td["asof"],
                                 "arm": td["arm"], "prereg_hash": h, "regime": td["regime"], "m": td["m"],
                                 "weights": td["_w"], "version": __version__})


def fwd_eval(F, P, path=FWD_LOG) -> Dict[str, Any]:
    """ผลของพอร์ตที่บันทึกไว้ (ก่อนหักต้นทุน) จากราคาเปิดวันซื้อขาย → ราคาเปิดรอบถัดไป · เทียบ EW ของ pool วันนั้น"""
    h, arm = prereg_hash(P), P.get("arm_live")
    L = sorted([e for e in ET.read_lines(path) if e.get("prereg_hash") == h and e.get("arm") == arm],
               key=lambda e: str(e.get("asof")))
    pos, dix = {s: j for j, s in enumerate(F.syms)}, {str(d.date()): i for i, d in enumerate(F.dates)}
    rows = []
    for i, e in enumerate(L):
        t = dix.get(str(e.get("asof")))
        if t is None or t + 1 >= F.n:
            rows.append({"asof": e.get("asof"), "status": "PENDING"})
            continue
        t2 = dix.get(str(L[i + 1].get("asof"))) if i + 1 < len(L) else None
        end = t2 + 1 if t2 is not None and t2 + 1 < F.n else None
        p0, p1 = F.of[t + 1], (F.of[end] if end is not None else F.cf[F.n - 1])
        with np.errstate(all="ignore"):
            rr = p1 / p0 - 1
        r = sum(float(w) * (float(rr[pos[s]]) if s in pos and np.isfinite(rr[pos[s]]) else 0.0)
                for s, w in (e.get("weights") or {}).items())
        pool = np.flatnonzero(F.el[t])
        ew = float(np.nanmean(rr[pool])) if len(pool) else None
        rows.append({"asof": e.get("asof"), "status": "CLOSED" if end is not None else "OPEN", "ret_pct": r * 100,
                     "ew_pct": None if ew is None else ew * 100, "excess_pct": None if ew is None else (r - ew) * 100})
    cl = [x for x in rows if x["status"] == "CLOSED" and x.get("excess_pct") is not None]
    out = {"logged": len(L), "closed": len(cl), "need": int(P.get("fwd_need")), "rows": rows[-24:]}
    if not cl:
        return {**out, "state": "INSUFFICIENT"}
    x = np.array([c["excess_pct"] for c in cl])
    t_ = ET._t(x)
    if len(cl) < P.get("fwd_need"):
        st = "INSUFFICIENT"
    elif x.mean() > 0 and (x > 0).mean() >= 0.5:
        st = "CONSISTENT"
    elif x.mean() < 0 and (t_ or 0) <= -2:
        st = "CONTRADICTED"
    else:
        st = "NOT_FOUND"
    return {**out, "state": st, "mean_excess_pct": float(x.mean()), "t": t_, "pos": float((x > 0).mean())}


# ═══════════════ RUN ═══════════════
def compute(frames, idx, meta, P, say, trials):
    say("เตรียมข้อมูล…")
    F = prep(frames, idx, P)
    st, live = F.start, P.get("arm_live")
    sims, arms = {}, {}
    for key, lab in ARMS:
        say(f"backtest {lab}…")
        sims[key] = sim(F, P, picker(F, key, P), st, rec=True)
    ew = sims["EW"]
    ew_m = mser(F, ew["ret"], ew["d0"])
    ew_cost = ew["cost"] / (len(ew["ret"]) / 252) * 100
    reg_m = pd.Series(F.reg, index=F.dates).groupby(F.dates.to_period("M")).last().shift(1)
    ixr = pd.Series(F.I).ffill().pct_change().fillna(0.0).to_numpy()[ew["d0"]:]
    idx_pf, ew_pf = ET._perf(ixr), ET._perf(ew["ret"])
    for key, lab in ARMS:
        arms[key] = {"key": key, "label": lab, "stats": arm_stats(F, sims[key], ew_m, reg_m, ew_cost), "state": "—"}
    reps = int(P.get("null_reps"))
    for key in MAIN:
        a = arms[key]
        say(f"ตรวจ edge {key}: ต้นทุน ×2 · ช้า {int(P.get('lag_days'))} วัน · random portfolio · Deflated Sharpe…")
        a["c2"] = exyr(F, sim(F, P, picker(F, key, P), st, cost_mult=2.0), ew_m)
        a["lag"] = exyr(F, sim(F, P, picker(F, key, P), st, lag=int(P.get("lag_days"))), ew_m)
        a["null"] = placebo(F, P, sims[key], ew_m, st, reps if key == live else max(20, reps // 2))
        a["dsr"] = ET.dsr(sims[key]["ret"] - ew["ret"], trials)
    say("sensitivity 6 ชุด…")
    sens = sensitivity(frames, idx, P, live)
    void = []
    if F.grade == "PROXY":
        void.append("ไม่มีดัชนี SET จริง (^SET.BK) — regime / Sharpe ดัชนี / DD เทียบไม่ได้")
    cov = F.k / max(1, meta.get("requested") or F.k)
    if cov < 0.7:
        void.append(f"ได้ข้อมูลหุ้น {F.k}/{meta.get('requested')} ({cov * 100:.0f}%) < 70%")
    for key in MAIN:
        arms[key]["gates"], arms[key]["state"] = gates(arms[key], idx_pf, ew_pf, void, P, trials,
                                                       sens if key == live else None)
    bench = {"idx": idx_pf, "ew": ew_pf, "from": str(F.dates[ew["d0"]].date()), "grade": F.grade, "ew_cost_yr": ew_cost}
    return F, arms, bench, sens, void, sims


def write_rebal(F, S, path=REB_CSV):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["signal_date", "exec_date", "names", "exposure", "turnover", "cost_pct", "holdings"])
        for L in S["logs"]:
            w.writerow([L["date"], str(F.dates[L["e"]].date()), L["n"], round(L["ex"], 3), round(L["turn"], 4),
                        round(L["cost"] * 100, 4), "|".join(f"{F.syms[j]}:{x * 100:.1f}"
                                                             for j, x in sorted(L["w"].items(), key=lambda kv: -kv[1]))])


_SVC: Dict[str, Any] = {"svc": None}


def run(force=False, reps=None, say=None) -> Dict[str, Any]:
    say = say or (lambda s: None)
    t0 = time.time()
    P = POLICY.snap()
    if reps:
        P.d["null_reps"] = int(reps)
    syms, src = ET.universe(_SVC.get("svc"))
    if len(syms) < 10:
        raise RuntimeError("ไม่มี universe — สร้าง sq_etrend_universe.txt (1 หุ้นต่อบรรทัด) หรือใช้ --symbols")
    frames, idx, meta = load(syms, P, force, say)
    frames, idx, cut = ET.confirmed(frames, idx)
    trials = trial_mark(P) * len(MAIN)
    F, arms, bench, sens, void, sims = compute(frames, idx, meta, P, say, trials)
    live = P.get("arm_live")
    td = today(F, P, sims[live])
    nlog = fwd_log(td, P)
    fw = fwd_eval(F, P)
    write_rebal(F, sims[live])
    st = arms[live]["state"]
    if st == "BACKTEST_PASS" and fw.get("state") == "CONSISTENT":
        st = "READY"
    elif fw.get("state") == "CONTRADICTED" and st != "VOID":
        st = "FORWARD_CONTRADICTED"
    td.pop("_w", None)
    rep = ET._js({
        "version": __version__, "computed_at": ET._now(), "computed_day": date.today().isoformat(),
        "compute_sec": round(time.time() - t0, 1), "prereg_hash": prereg_hash(P), "trials": trials, "universe_source": src,
        "arm_live": live, "state": st, "state_label": STATE_TH.get(st, st),
        "data": {"grade": F.grade, "symbols": F.k, "requested": meta.get("requested"), "missing": (meta.get("missing") or [])[:60],
                 "quarantined": F.quarantined[:60], "bad_bars": F.bad_bars, "first": str(F.dates[0].date()),
                 "last": str(F.dates[-1].date()), "bars": F.n, "fetched_at": meta.get("fetched_at"),
                 "cached": meta.get("cached"), "cut_today": cut, "index_src": meta.get("index_src")},
        "arms": list(arms.values()), "ablation": ablation(arms), "bench": bench, "sens": sens, "void": void,
        "today": td, "forward": fw, "forward_logged": nlog, "notes": NOTES, "state_names": STATE_TH})
    ET._jsave(RES_FILE, rep)
    return rep


_RUN: Dict[str, Any] = {"th": None, "state": "idle", "msg": "", "started": None, "finished": None, "err": None}
_RL = threading.RLock()


def status():
    return {k: _RUN[k] for k in ("state", "msg", "started", "finished", "err")}


def start(force=False) -> bool:
    with _RL:
        th = _RUN["th"]
        if th is not None and th.is_alive():
            return False

        def job():
            try:
                run(force, say=lambda s: _RUN.update(msg=s))
                _RUN.update(state="done", err=None, msg="เสร็จ")
            except Exception as e:
                log.exception("midterm run")
                _RUN.update(state="error", err=f"{type(e).__name__}: {e}")
            finally:
                _RUN["finished"] = ET._now()
        _RUN.update(state="running", msg="เริ่ม", started=ET._now(), err=None)
        th = threading.Thread(target=job, daemon=True, name="sq-midterm")
        _RUN["th"] = th
        th.start()
        return True


# ═══════════════ TEXT REPORT ═══════════════
def text_report(rep, only_today=False) -> str:
    def f(x, d=2):
        return "—" if x is None else f"{x:+.{d}f}"
    L, d = [], rep.get("data") or {}
    L.append(f"Mid-Long Lab v{rep.get('version')} · ข้อมูล {d.get('first')} → {d.get('last')} · หุ้น {d.get('symbols')}/"
             f"{d.get('requested')} · ดัชนี {d.get('grade')} · กักกัน {len(d.get('quarantined') or [])} · trial {rep.get('trials')}"
             f" · prereg {rep.get('prereg_hash')} · {rep.get('compute_sec')} วิ")
    if not only_today:
        L.append("ดัชนีอ้างอิง: " + str(d.get("index_src")))
        L.append(f"\nสถานะ (แขนใช้จริง {rep.get('arm_live')}): {rep.get('state_label')}")
        for v in rep.get("void") or []:
            L.append("  VOID: " + v)
        b = rep.get("bench") or {}
        ix, ew = b.get("idx") or {}, b.get("ew") or {}
        L.append(f"\nตั้งแต่ {b.get('from')} · ถือดัชนี: CAGR {f(ix.get('cagr_pct'), 1)}% · Sharpe {f(ix.get('sharpe'))} · "
                 f"DD {f(ix.get('maxdd_pct'), 1)}%  |  EW: CAGR {f(ew.get('cagr_pct'), 1)}% · Sharpe {f(ew.get('sharpe'))} · "
                 f"DD {f(ew.get('maxdd_pct'), 1)}%")
        L.append("\n═══ แขนทดสอบ (ส่วนเกิน = เทียบ EW · %/ปี · t รายเดือน · หลังหักต้นทุน) ═══")
        for a in rep.get("arms") or []:
            s = a["stats"]
            L.append(f"  {a['key']:<3} CAGR {f(s.get('cagr_pct'), 1):>6}%  Sharpe {f(s.get('sharpe')):>6}  DD {f(s.get('maxdd_pct'), 1):>6}%"
                     f"  ส่วนเกิน {f(s.get('ex_yr')):>6}  t {f(s.get('t')):>6}  ปรับ exposure {f(s.get('ex_adj_yr')):>6} (t {f(s.get('t_adj'))})  ต้นทุน {f(s.get('cost_yr')):>5}%/ปี"
                     f"  turnover {f(s.get('turn_yr'), 0):>6}%  ถือเฉลี่ย {f(s.get('avg_n'), 1)} ตัว  exposure {f(s.get('avg_ex'))}")
        L.append("")
        for x in rep.get("ablation") or []:
            L.append(f"  ชั้น {x['layer']:<22} {x['arm']} vs {x['vs']}: ส่วนเกิน {f(x.get('d_ex'))} · Sharpe {f(x.get('d_sh'))}"
                     f" → {x['verdict']}")
        for a in rep.get("arms") or []:
            if a["key"] not in MAIN:
                continue
            L.append(f"\n═══ {a['label']} → {STATE_TH.get(a['state'], a['state'])} ═══")
            for g in a.get("gates") or []:
                L.append(f"  {'✓' if g['ok'] else '✕'} {g['label']} — {g['detail']}")
    td, fw = rep.get("today") or {}, rep.get("forward") or {}
    L.append(f"\n═══ วันนี้ ณ {td.get('asof')} · regime R{td.get('regime')} (×{td.get('m')}) · breadth {f(td.get('breadth'), 0)}% · "
             f"pool {td.get('pool')} ตัว ═══")
    if td.get("stale"):
        L.append(f"  ⚠ ข้อมูลค้าง {td.get('lag_days')} วันทำการ — ไม่บันทึก forward")
    L.append(f"  {'🔔 วันนี้คือวัน rebalance → ซื้อขายที่ราคาเปิด ' + str(td.get('exec_date')) if td.get('is_signal') else 'ไม่ใช่วัน rebalance (rebalance หลังราคาปิดวันทำการสุดท้ายของเดือน)'}")
    L.append(f"  พอร์ตโมเดล {td.get('arm')} ตอนนี้ {len(td.get('current') or [])} ตัว:")
    for c in (td.get("current") or [])[:25]:
        L.append(f"    {c['symbol']:<8} น้ำหนัก {c['w_now']:5.1f}%  ตั้งแต่ rebalance {f(c.get('ret_pct'), 1)}%  อันดับ momentum "
                 f"{c.get('mom_rank')}" + (f"  ⚠ {c['flag']}" if c.get("flag") else ""))
    L.append(f"  ถ้า rebalance วันนี้: {td.get('target_n')} ตัว · exposure {f(td.get('target_exposure'))}")
    for c in (td.get("changes") or [])[:30]:
        L.append(f"    {c['action']:<4} {c['symbol']:<8} {f(c['w_cur'], 1)}% → {f(c['w_tgt'], 1)}%  ราคา {c['price']:.2f}  "
                 f"เป้า {c['shares_tgt']} หุ้น")
    L.append(f"\nForward paper: บันทึก {fw.get('logged')} รอบ · ปิดแล้ว {fw.get('closed')}/{fw.get('need')} · สถานะ {fw.get('state')}"
             + (f" · ส่วนเกินเฉลี่ย {f(fw.get('mean_excess_pct'))}%/รอบ" if fw.get("mean_excess_pct") is not None else ""))
    if not only_today:
        L.append("\n" + "\n".join("หมายเหตุ: " + x for x in rep.get("notes") or []))
    return "\n".join(L)


# ═══════════════ SELFTEST ═══════════════
def _synth(k=40, n=1300, seed=3, drift=True):
    rng = np.random.default_rng(seed)
    ix = pd.bdate_range("2018-01-01", periods=n)
    mu = rng.normal(0.0, 0.0008, k) if drift else np.zeros(k)
    fr, lr = {}, []
    for i in range(k):
        r = mu[i] + rng.normal(0.0, 0.015, n)
        c = 30 * np.exp(np.cumsum(r))
        o = np.r_[c[0], c[:-1]]
        fr[f"S{i:02d}"] = pd.DataFrame({"open": o, "high": np.maximum(o, c) * 1.005, "low": np.minimum(o, c) * 0.995,
                                        "close": c, "volume": rng.lognormal(math.log(1e7), 0.2, n)}, index=ix)
        lr.append(r)
    return fr, pd.DataFrame({"close": 1000 * np.exp(np.cumsum(np.mean(lr, axis=0)))}, index=ix)


def _flat2(jump=True, n=400):
    ix = pd.bdate_range("2023-01-02", periods=n)
    a = pd.DataFrame({"open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0, "volume": 1e6}, index=ix)
    b = a.copy()
    if jump:
        b.iloc[350:, :4] = [110.0, 110.5, 109.5, 110.0]
        b.iloc[350] = [100.0, 110.0, 100.0, 110.0, 1e6]
    return {"AAA": a, "BBB": b}, pd.DataFrame({"close": 1000.0}, index=ix)


def selftest() -> Dict[str, Any]:
    res = []

    def T(name, fn):
        try:
            ok, det = fn()
        except Exception as e:
            ok, det = False, f"{type(e).__name__}: {e}"
        res.append({"name": name, "ok": bool(ok), "detail": str(det)})

    c1, c2 = capw([0.5, 0.3, 0.2], 0.4), capw([0.2] * 5, 0.1)
    T("เพดานน้ำหนัก: [0.5 0.3 0.2] เพดาน 0.4 → [0.4 0.36 0.24] · 5 ตัวเพดาน 10% → เงินสด 50%",
      lambda: (np.allclose(c1, [0.4, 0.36, 0.24]) and abs(c2.sum() - 0.5) < 1e-12, f"{np.round(c1, 3)} · {c2.sum():.2f}"))
    PZ = _P(min_pool=2, min_names=1, equity=1e6, comm_pct=0.0, venue_pct=0.0, exec_ticks=0.0, impact_k=0.0, ftt_pct=0.0)
    fj, ij = _flat2(True)
    Fj = prepare_ok = prep(fj, ij, PZ, 1)
    Sj = sim(Fj, PZ, picker(Fj, "EW", PZ), Fj.start)
    tj = float(np.prod(1 + Sj["ret"]) - 1)
    T("บัญชีพอร์ต: 2 หุ้น 50/50 · หุ้นหนึ่งขึ้น 10% · ไม่มีต้นทุน → พอร์ต +5.000%",
      lambda: (abs(tj - 0.05) < 1e-9, f"{tj * 100:.4f}% · เริ่มซื้อแท่ง {Sj['d0']}"))
    PC = _P(min_pool=2, min_names=1, equity=1e6, comm_pct=0.15, venue_pct=0.0, vat_pct=0.0, exec_ticks=0.0, impact_k=0.0, ftt_pct=0.0)
    ff, i0 = _flat2(False)
    Ff = prep(ff, i0, PC, 1)
    Sc = sim(Ff, PC, picker(Ff, "EW", PC), Ff.start)
    tc = float(np.prod(1 + Sc["ret"]) - 1)
    T("ต้นทุน: ราคาไม่เปลี่ยน · ค่าคอม 0.15% ต่อข้าง · ซื้อครั้งแรก 100% → พอร์ต ≈ −0.15%",
      lambda: (abs(tc + 0.0015) < 3e-5, f"{tc * 100:.5f}%"))
    fs, isy = _synth(40, 1300, 9, True)
    PP = _P(min_pool=10)
    Fa = prep(fs, isy, PP)
    Fb = prep({s: d.iloc[:800] for s, d in fs.items()}, isy.iloc[:800], PP)
    t = 799
    wa, wb = picker(Fa, "M4", PP)(t, set())[0], picker(Fb, "M4", PP)(t, set())[0]
    same = (np.array_equal(Fa.el[t], Fb.el[t]) and np.allclose(Fa.mom[t], Fb.mom[t], equal_nan=True)
            and Fa.m[t] == Fb.m[t] and wa.keys() == wb.keys() and all(abs(wa[j] - wb[j]) < 1e-12 for j in wa))
    T("ไม่มี look-ahead: ตัดข้อมูลที่แท่ง 800 → ผ่านเกณฑ์ / momentum / regime / พอร์ต M4 ของแท่ง 799 เหมือนข้อมูลเต็ม",
      lambda: (same, f"{len(wa)} ตัว · m {Fa.m[t]}"))
    fn_, in_ = _synth(40, 1300, 4, False)
    Fn = prep(fn_, in_, PP)
    cases = [t for t in Fn.sig if t >= Fn.start]
    okx = all(abs(sum(picker(Fn, "M3", PP)(t, set())[0].values())
                  - Fn.m[t] * sum(picker(Fn, "M2", PP)(t, set())[0].values())) < 1e-12 for t in cases)
    nlow = sum(1 for t in cases if Fn.m[t] < 1)
    T("Regime: น้ำหนักรวม M3 = ตัวคูณ regime × M2 ทุกเดือน (มีเดือนที่ตัวคูณ < 1)",
      lambda: (okx and nlow > 0, f"{len(cases)} เดือน · ตัวคูณ < 1 {nlow} เดือน"))
    out = {}
    for lab, (F_, ) in (("planted", (Fa,)), ("null", (Fn,))):
        E_ = sim(F_, PP, picker(F_, "EW", PP), F_.start)
        M_ = sim(F_, PP, picker(F_, "M1", PP), F_.start, rec=True)
        em = mser(F_, E_["ret"], E_["d0"])
        x = (mser(F_, M_["ret"], M_["d0"]) - em).dropna().to_numpy()
        out[lab] = (float(x.mean() * 1200), ET._t(x), placebo(F_, PP, M_, em, F_.start, 30) if lab == "planted" else None)
    (pe, pt, pn), (ne, nt_, _) = out["planted"], out["null"]
    T("Planted momentum: drift คงที่ → M1 ส่วนเกิน t ≥ 3 และชนะ random ≥ P95 · ไม่มี drift → ส่วนเกินต่ำกว่าครึ่งหนึ่ง",
      lambda: ((pt or 0) >= 3 and pn.get("pct", 0) >= 0.95 and ne < pe / 2,
               f"planted {pe:+.1f}%/ปี t {pt:+.1f} pct {pn.get('pct')} · null {ne:+.1f}%/ปี"))
    S3 = sim(Fn, PP, picker(Fn, "M3", PP), Fn.start, rec=True)
    S4 = sim(Fn, PP, picker(Fn, "M4", PP), Fn.start, rec=True)

    def churn(S):
        L = S["logs"]
        return float(np.mean([len(set(b["w"]) - set(a["w"])) for a, b in zip(L, L[1:])])) if len(L) > 1 else 0.0
    c3, c4 = churn(S3), churn(S4)
    T("Buffer: M4 เปลี่ยนตัวต่อเดือนน้อยกว่า M3 (ข้อมูลสุ่ม)", lambda: (c4 < c3, f"M3 {c3:.2f} · M4 {c4:.2f} ตัว/เดือน"))
    with tempfile.TemporaryDirectory() as tdir:
        pth = Path(tdir) / "f.jsonl"
        h = prereg_hash(PP)
        sg = [t for t in Fn.sig if t >= Fn.start]
        t1, t2 = sg[-3], sg[-2]
        wts = {"S00": 0.6, "S01": 0.4}
        k1 = ET.append_line(pth, {"key": "a", "asof": str(Fn.dates[t1].date()), "arm": "M4", "prereg_hash": h, "weights": wts})
        k2 = ET.append_line(pth, {"key": "a", "asof": str(Fn.dates[t1].date()), "arm": "M4", "prereg_hash": h, "weights": wts})
        ET.append_line(pth, {"key": "b", "asof": str(Fn.dates[t2].date()), "arm": "M4", "prereg_hash": h, "weights": wts})
        fv = fwd_eval(Fn, PP, pth)
        e0, e1 = t1 + 1, t2 + 1
        exp = 0.6 * (Fn.of[e1, 0] / Fn.of[e0, 0] - 1) + 0.4 * (Fn.of[e1, 1] / Fn.of[e0, 1] - 1)
        T("Forward paper: append-only (key ซ้ำไม่บันทึก) · รอบที่ปิด = ราคาเปิดวันซื้อขาย → ราคาเปิดรอบถัดไป",
          lambda: (k1 == 1 and k2 == 0 and fv["closed"] == 1 and abs(fv["rows"][0]["ret_pct"] - exp * 100) < 1e-9
                   and fv["rows"][1]["status"] == "OPEN", f"{fv['rows'][0]['ret_pct']:.4f}% vs {exp * 100:.4f}%"))
    pol = Policy(Path(os.devnull))
    h0 = prereg_hash(pol)
    pol._apply({"top_pct": 99})
    h1 = prereg_hash(pol)
    pol._apply({"null_reps": 500, "arm_live": "M3"})
    T("Policy: ค่าเกินช่วงถูกปรับ · เปลี่ยนกฎ = prereg ใหม่ · จำนวนรอบ random / แขนใช้จริง ไม่เปลี่ยน prereg",
      lambda: (pol.data["top_pct"] == 50.0 and h0 != h1 and prereg_hash(pol) == h1, f"{h0} → {h1}"))
    n_ok = sum(1 for r in res if r["ok"])
    return {"ok": n_ok == len(res), "passed": n_ok, "total": len(res), "tests": res, "version": __version__}


# ═══════════════ ROUTES · INSTALL ═══════════════
def routes(svc, safe, ok_code):
    from fastapi import Body

    @safe
    def m_main():
        rep = ET._jload(RES_FILE)
        if (not rep or rep.get("computed_day") != date.today().isoformat()) and _RUN["state"] != "running":
            start(False)
        return {"available": True, "version": __version__, "run": status(), "policy": POLICY.view(), "report": rep or None}

    @safe
    def m_run(payload: Optional[Dict[str, Any]] = Body(None)):
        return ok_code({"ok": True, "started": start(bool((payload or {}).get("force"))), "run": status()})

    @safe
    def m_fwd():
        return {"rows": ET.read_lines(FWD_LOG)[-120:]}

    @safe
    def m_cfg_get():
        return POLICY.view()

    @safe
    def m_cfg_set(payload: Optional[Dict[str, Any]] = Body(None)):
        return ok_code(POLICY.update(payload or {}))

    @safe
    def m_self():
        return selftest()

    return [("GET", "/api/midterm/selftest", m_self), ("GET", "/api/midterm/config", m_cfg_get),
            ("POST", "/api/midterm/config", m_cfg_set), ("POST", "/api/midterm/run", m_run),
            ("GET", "/api/midterm/forward", m_fwd), ("GET", "/api/midterm", m_main)]


def install(svc) -> Dict[str, Any]:
    """ลงทะเบียนเท่านั้น — ไม่ครอบ hub.build_core · ไม่แตะโมดูลเดิม"""
    _SVC["svc"] = svc
    st = selftest()
    errs = [] if st["ok"] else [f"selftest ผ่าน {st['passed']}/{st['total']} — ดู /api/midterm/selftest"]
    rep = {"ok": not errs, "version": __version__, "isolated": True, "selftest": f"{st['passed']}/{st['total']}",
           "prereg_hash": prereg_hash(POLICY), "errors": errs}
    log.info("midterm.install: %s", rep)
    return rep


POLICY = Policy()


def _cli():
    ap = argparse.ArgumentParser(prog="python3 -m sq.midterm")
    ap.add_argument("cmd", choices=("selftest", "backtest", "today"))
    ap.add_argument("--force", action="store_true", help="ดึงข้อมูลใหม่ (ไม่ใช้ cache วันนี้)")
    ap.add_argument("--reps", type=int, help="จำนวนรอบ random portfolio (ไม่นับเป็น trial)")
    ap.add_argument("--symbols", help="รายชื่อหุ้นคั่นด้วย , (แทน universe)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if a.symbols:
        ET._CLI_SYMS.extend(sorted({ET.norm(s) for s in a.symbols.split(",") if s.strip()}))
    if a.cmd == "selftest":
        r = selftest()
        for t in r["tests"]:
            print(("✓ " if t["ok"] else "✕ ") + t["name"] + " — " + t["detail"])
        print(f"\nผ่าน {r['passed']} / {r['total']}")
        return
    rep = run(a.force, a.reps, say=lambda s: print("…", s, flush=True))
    print("\n" + text_report(rep, a.cmd == "today"))
    print(f"\nผล: {RES_FILE} · rebalance: {REB_CSV} · forward: {FWD_LOG} · trial: {TRIALS}")


if __name__ == "__main__":
    _cli()
# ═══ END sq/midterm.py v1.2 ═══