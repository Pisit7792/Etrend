"""
sq/etrend.py  [v1.2]
Early Trend Lab — ระบบแยกใหม่ทั้งหมด · ไม่ครอบ Hub · ไม่แตะ Shortlist / Edge / Govern / Costs / registry เดิม
merge จาก: Early trend.txt · ระบบเทรดจับสัญญาณ early trend SET100 · แผนการค้นคว้า Market Regime SET100
           · TH_CORE v3.2.0 (ต้นทุน · VOID/BAR · log) · TH_MONITOR v2.2.0 · ต้นทุน.txt · ตลท. tick size 16 พ.ย. 2569

ชั้น   L0 สภาพคล่อง → L1 Regime (m 1.0/0.6/0.5/0.2) → L2 Setup A (ฐาน/VCP + RS) · Setup B (strength breakout)
       → L3 E1 breakout (E2 retest = แขน ablation) → L4 stop ใต้ higher low · breakeven B (+2R ขาย 1/3)
       → L6 Chandelier 3 ATR · failed breakout · time stop        (L5 pyramid ยังไม่ทำ)
วัด edge  R สุทธิหลังต้นทุนจริง · t ระดับวันเข้า · Wilson vs จุดคุ้มทุน · ต้นทุน ×0–3 · เข้าช้า 1 วัน
          · random entry หุ้นอื่น "วันเดียวกัน" · 3 ช่วงเวลา · Sharpe พอร์ต vs ถือดัชนี · Deflated Sharpe (trial ledger)
          · sensitivity ±20% · ablation S0 → A1 → A2 → A3 → A4 · B4
สถานะ    VOID · INSUFFICIENT · NOT_PROVEN · CONTRADICTED · BACKTEST_PASS → READY (ต้องผ่าน forward paper ด้วย)
Forward  บันทึกสัญญาณทุกวัน (append-only · แท่งปิดแล้วเท่านั้น) → วัดผลด้วยกฎเดียวกัน
CLI      python3 -m sq.etrend selftest | backtest [--force] [--reps N] [--symbols A,B] | scan
"""
import argparse
import copy
import csv
import hashlib
import json
import logging
import math
import os
import tempfile
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import NormalDist
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

__version__ = "1.2"
log = logging.getLogger("sq.etrend")
ND = NormalDist()
CFG_FILE = Path(os.getenv("SQ_ETREND_CFG", "sq_etrend.json"))
DATA_FILE = Path(os.getenv("SQ_ETREND_DATA", "sq_etrend_data.pkl"))
RES_FILE = Path(os.getenv("SQ_ETREND_RESULT", "sq_etrend_result.json"))
TRADES_CSV = Path(os.getenv("SQ_ETREND_TRADES", "sq_etrend_trades.csv"))
SIG_LOG = Path(os.getenv("SQ_ETREND_SIGNALS", "sq_etrend_signals.jsonl"))
TRIALS = Path(os.getenv("SQ_ETREND_TRIALS", "sq_etrend_trials.jsonl"))
UNI_FILE = Path(os.getenv("SQ_ETREND_UNIVERSE", "sq_etrend_universe.txt"))
UNI_CACHE = Path("sq_etrend_universe_cache.json")
INDEX_TICKER = "^SET.BK"
NEW_TICK_DAY = date(2026, 11, 16)
TICK_OLD = [(2.0, 0.01), (5.0, 0.02), (10.0, 0.05), (25.0, 0.10), (100.0, 0.25), (200.0, 0.50), (400.0, 1.00)]
TICK_NEW = [(2.0, 0.01), (10.0, 0.02), (25.0, 0.05), (50.0, 0.10), (100.0, 0.25), (200.0, 0.50), (400.0, 1.00)]

DEFAULTS: Dict[str, Any] = {
    "setups": "AB", "period_years": 10, "min_bars": 300,
    "min_price": 1.0, "min_adv_mb": 20.0, "adv_cap_pct": 2.0,
    "idx_sma": 200, "slope_len": 20, "breadth_sma": 50, "m_r1": 1.0, "m_r2": 0.6, "m_r3": 0.5, "m_r4": 0.2,
    "base_len": 60, "seg_len": 20, "base_depth": 0.35, "dry_max": 0.7, "rs_rank_min": 70.0, "rs_hi_len": 60,
    "pivot_buf_atr": 0.1, "vol_mult_a": 1.5,
    "h52_min": 0.85, "comp_pct_min": 60.0, "brk_len": 55, "brk_buf_atr": 0.25, "vol_mult_b": 1.3, "ext_atr": 1.0,
    "rt_min": 5, "rt_max": 15, "rt_band_atr": 0.5,
    "atr_len": 14, "hl_len": 10, "hl_buf_atr": 0.25, "stop_min_atr": 1.5, "stop_max_atr": 3.0, "stop_max_pct": 8.0,
    "be_r": 2.0, "part_frac": 0.3333, "trail_atr": 3.0, "fail_bars": 5, "time_bars": 20, "time_min_r": 1.0,
    "max_hold": 250, "cooldown": 5,
    "equity": 10000000.0, "risk_pct": 1.0, "max_pos": 20, "max_val_pct": 10.0,
    "comm_pct": 0.15, "venue_pct": 0.007, "vat_pct": 7.0, "ftt_pct": 0.0, "exec_ticks": 0.75, "impact_k": 1.0,
    "max_cost_r": 0.30,
    "null_reps": 100, "min_n": 30, "need_n": 100, "need_days": 50, "t_need": 3.0, "dsr_need": 0.95, "null_pct": 0.95,
    "fold_max": 0.6, "sens_pct": 20.0, "fwd_need": 30, "stale_days": 3,
}
OPS = {"null_reps", "stale_days"}
RANGES = {
    "period_years": (3, 20), "min_bars": (260, 2000), "min_price": (0.0, 50.0), "min_adv_mb": (0.0, 1000.0),
    "adv_cap_pct": (0.1, 20.0), "idx_sma": (50, 300), "slope_len": (5, 60), "breadth_sma": (20, 200),
    "m_r1": (0.0, 1.0), "m_r2": (0.0, 1.0), "m_r3": (0.0, 1.0), "m_r4": (0.0, 1.0), "base_len": (40, 200),
    "seg_len": (5, 60), "base_depth": (0.05, 0.6), "dry_max": (0.2, 1.5), "rs_rank_min": (0.0, 100.0),
    "rs_hi_len": (20, 252), "pivot_buf_atr": (0.0, 2.0), "vol_mult_a": (0.5, 5.0), "h52_min": (0.5, 1.0),
    "comp_pct_min": (0.0, 100.0), "brk_len": (10, 252), "brk_buf_atr": (0.0, 2.0), "vol_mult_b": (0.5, 5.0),
    "ext_atr": (0.1, 5.0), "rt_min": (1, 30), "rt_max": (2, 60), "rt_band_atr": (0.0, 3.0), "atr_len": (5, 50),
    "hl_len": (3, 40), "hl_buf_atr": (0.0, 2.0), "stop_min_atr": (0.5, 5.0), "stop_max_atr": (1.0, 10.0),
    "stop_max_pct": (1.0, 30.0), "be_r": (0.5, 10.0), "part_frac": (0.0, 1.0), "trail_atr": (1.0, 10.0),
    "fail_bars": (0, 20), "time_bars": (5, 120), "time_min_r": (0.0, 5.0), "max_hold": (20, 1000), "cooldown": (0, 60),
    "equity": (1e5, 1e10), "risk_pct": (0.1, 5.0), "max_pos": (1, 100), "max_val_pct": (1.0, 100.0),
    "comm_pct": (0.0, 1.0), "venue_pct": (0.0, 0.1), "vat_pct": (0.0, 20.0), "ftt_pct": (0.0, 1.0),
    "exec_ticks": (0.0, 5.0), "impact_k": (0.0, 5.0), "max_cost_r": (0.05, 2.0), "null_reps": (10, 1000),
    "min_n": (10, 500), "need_n": (30, 2000), "need_days": (10, 1000), "t_need": (1.0, 6.0), "dsr_need": (0.5, 0.999),
    "null_pct": (0.5, 0.999), "fold_max": (0.34, 1.0), "sens_pct": (5.0, 50.0), "fwd_need": (10, 500),
    "stale_days": (1, 20)}
ARMS = [("S0", "S0 · breakout ธรรมดา (ไม่มีฐาน/RS/regime)", False), ("A1", "A1 · ฐาน/VCP + breakout E1", False),
        ("A2", "A2 · ฐาน + retest E2 (แขนข้าง)", False), ("A3", "A3 · A1 + RS", False),
        ("A4", "A4 · Setup A ฉบับเต็ม (+ ขนาดตาม regime)", True), ("B4", "B4 · Setup B ฉบับเต็ม (+ ขนาดตาม regime)", True),
        ("AB", "AB · ระบบรวม A + B", True)]
MAIN = ("A4", "B4", "AB")
ABL = [("A1", "S0", "ฐาน/VCP"), ("A2", "A1", "Retest E2"), ("A3", "A1", "RS"), ("A4", "A3", "Regime (ขนาดตาม m)")]
STATE_TH = {"VOID": "วัดไม่ได้ (VOID)", "INSUFFICIENT": "ข้อมูลไม่พอ", "NOT_PROVEN": "ยังพิสูจน์ไม่ได้",
            "CONTRADICTED": "ขาดทุนอย่างมีนัย", "BACKTEST_PASS": "ผ่าน backtest — รอ forward paper",
            "READY": "ผ่าน backtest + forward paper", "FORWARD_CONTRADICTED": "forward paper ขาดทุนอย่างมีนัย", "—": "—"}
CSV_COLS = ["arm", "setup", "symbol", "signal_date", "entry_date", "exit_date", "entry", "stop", "risk_pct", "shares", "m",
            "regime", "R_gross", "cost_R", "R_net", "exit_reason", "hold", "mae_R", "mfe_R", "partial", "open"]
NOTES = [
    "universe = รายชื่อหุ้นปัจจุบัน (survivorship bias) → ผ่าน backtest ยังไม่พอ ต้องผ่าน forward paper",
    "ราคาปรับปันผล/split (auto_adjust) · ต้นทุน tick คิดบนราคาปรับ = ระวังเกินในปีเก่า",
    "พอร์ตไม่คิดดอกเบี้ยเงินสด · ขนาดไม้คิดจากทุนตั้งต้น (ไม่ทบต้น)",
    "รันก่อน 17:00 วันทำการ = ตัดแท่งวันนี้ (ยังไม่ปิด) · สัญญาณ = ราคาปิด → ซื้อที่ราคาเปิดวันถัดไป",
    "ทุกค่าตั้งมาจากเอกสาร (ไม่จูน) · เปลี่ยนค่าใด = trial ใหม่ → Deflated Sharpe เข้มขึ้น",
    "ยังไม่ทำ: pyramid L5 · ปัจจัยพื้นฐาน (ไม่มีข้อมูล PIT) · foreign flow · HMM · PBO (ใช้ sensitivity + DSR แทน)",
]


# ═══════════════ HELPERS ═══════════════
def _n(v) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _f2(v, nd=3):
    x = _n(v)
    return None if x is None else round(x, nd)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def norm(s) -> str:
    s = str(s or "").strip().upper()
    return s[:-3] if s.endswith(".BK") else s


def _date(x) -> date:
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    return pd.Timestamp(str(x)[:10]).date()


def next_bday(d: date) -> date:
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _js(o):
    if isinstance(o, dict):
        return {str(k): _js(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_js(v) for v in o]
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, (float, np.floating)):
        x = float(o)
        return round(x, 5) if math.isfinite(x) else None
    if isinstance(o, (date, datetime, pd.Timestamp)):
        return str(o)[:19]
    return o


_JL = threading.RLock()


def _jload(p) -> Dict[str, Any]:
    with _JL:
        try:
            p = Path(p)
            if p.is_file() and p.stat().st_size > 0:
                d = json.loads(p.read_text("utf-8"))
                return d if isinstance(d, dict) else {}
        except Exception as e:
            log.warning("อ่าน %s ไม่สำเร็จ: %s", p, e)
        return {}


def _jsave(p, d) -> bool:
    with _JL:
        try:
            p = Path(p)
            tmp = p.with_name(p.name + ".tmp")
            tmp.write_text(json.dumps(_js(d), ensure_ascii=False, default=str), "utf-8")
            tmp.replace(p)
            return True
        except Exception as e:
            log.error("บันทึก %s ไม่สำเร็จ: %s", p, e)
            return False


def read_lines(p, limit=None) -> List[Dict[str, Any]]:
    p = Path(p)
    if not p.is_file():
        return []
    out = []
    for ln in p.read_text("utf-8").splitlines()[-limit:] if limit else p.read_text("utf-8").splitlines():
        try:
            out.append(json.loads(ln))
        except Exception:
            continue
    return out


def append_line(p, entry) -> int:
    """append-only · key ซ้ำ = ไม่บันทึก"""
    with _JL:
        if str(entry.get("key")) in {str(e.get("key")) for e in read_lines(p)}:
            return 0
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(_js(entry), ensure_ascii=False, default=str) + "\n")
        return 1


def _t(x) -> Optional[float]:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return None
    sd = float(x.std(ddof=1))
    return float(x.mean() / (sd / math.sqrt(len(x)))) if sd > 0 else None


def wilson(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    den = 1 + z * z / n
    c = (p + z * z / (2 * n)) / den
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return max(0.0, c - h), min(1.0, c + h)


def _sh(a, k: int):
    out = np.full(a.shape, np.nan)
    if k < len(a):
        out[k:] = a[:len(a) - k]
    return out


# ═══════════════ POLICY ═══════════════
class _P:
    def __init__(self, **kw):
        self.d = {**DEFAULTS, **kw}

    def get(self, k):
        return self.d[k]


class Policy:
    def __init__(self, path: Path = CFG_FILE):
        self.path, self._l, self.data, self.rev = Path(path), threading.RLock(), dict(DEFAULTS), 0
        for e in self._apply(_jload(self.path))["errors"]:
            log.warning("sq_etrend.json: %s", e)

    def _apply(self, patch):
        errs, ch = [], []
        for k, v in (patch or {}).items():
            if k not in DEFAULTS:
                continue
            d = DEFAULTS[k]
            try:
                if isinstance(d, str):
                    v = str(v).strip().upper()
                    if v not in ("A", "B", "AB"):
                        raise ValueError
                else:
                    x = _n(v)
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
            ok = _jsave(self.path, self.data) if res["changed"] else True
            if res["changed"]:
                self.rev += 1
        return {"ok": ok and not res["errors"], **res, "policy": self.view()}


def prereg_hash(P) -> str:
    spec = {k: P.get(k) for k in sorted(DEFAULTS) if k not in OPS}
    return hashlib.sha1(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:12]


def _pdict(P) -> Dict[str, Any]:
    return dict(P.d) if hasattr(P, "d") else dict(P.data)


# ═══════════════ TICK · COST ═══════════════
def tick_at(p, on) -> Optional[float]:
    p = _n(p)
    if p is None or p <= 0:
        return None
    tb = TICK_NEW if _date(on) >= NEW_TICK_DAY else TICK_OLD
    for hi, t in tb:
        if p < hi:
            return t
    return 2.0


def cost_rt_pct(price, on, shares, sigma, adv_sh, P) -> float:
    """ต้นทุนไป-กลับ (% ของมูลค่า) = 2×(ค่าคอม+ค่าตลาด)×(1+VAT) + FTT + 2×exec_ticks×tick/ราคา + 2×k·σ·√(Q/ADV)"""
    fee = (P.get("comm_pct") + P.get("venue_pct")) * (1 + P.get("vat_pct") / 100)
    tk = tick_at(price, on) or 0.0
    spread = 2 * P.get("exec_ticks") * tk / price * 100
    imp = 0.0
    if shares and sigma is not None and adv_sh and adv_sh > 0 and math.isfinite(sigma):
        imp = P.get("impact_k") * sigma * math.sqrt(shares / adv_sh) * 100
    return 2 * fee + P.get("ftt_pct") + spread + 2 * imp


# ═══════════════ DATA ═══════════════
def _clean(sub) -> Optional[pd.DataFrame]:
    if sub is None or len(sub) == 0:
        return None
    cols = {str(c).strip().lower(): c for c in sub.columns}
    if not all(k in cols for k in ("open", "high", "low", "close")):
        return None
    df = pd.DataFrame({k: pd.to_numeric(sub[cols[k]], errors="coerce") for k in ("open", "high", "low", "close")})
    df["volume"] = pd.to_numeric(sub[cols["volume"]], errors="coerce") if "volume" in cols else np.nan
    ix = pd.DatetimeIndex(pd.to_datetime(df.index))
    if ix.tz is not None:
        ix = ix.tz_localize(None)
    df.index = ix.normalize()
    df = df[~df.index.duplicated(keep="last")].dropna(subset=["close"]).sort_index()
    return df if len(df) else None


def _yf(tickers: List[str], years: int) -> Dict[str, pd.DataFrame]:
    import yfinance as yf
    out = {}
    for i in range(0, len(tickers), 40):
        b = tickers[i:i + 40]
        try:
            raw = yf.download(b, start=(date.today() - timedelta(days=int(365.25 * years) + 30)).isoformat(), interval="1d", auto_adjust=True, group_by="ticker",
                              threads=True, progress=False)
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
            df = _clean(sub) if sub is not None else None
            if df is not None:
                out[tk] = df
    return out


INDEX_CSV = Path(os.getenv("SQ_SET_INDEX_CSV", "sq_set_index.csv"))
INDEX_SOURCES = [("^SET.BK", "INDEX"), ("CSV", "INDEX"), ("^SET50.BK", "SET50"), ("TDEX.BK", "ETF50")]


def _igrade(df) -> str:
    """เกรดดัชนีอ้างอิง: INDEX · SET50 · ETF50 (ใช้ได้) · PROXY (VOID)"""
    if df is not None and "grade" in getattr(df, "columns", []) and len(df):
        return str(df["grade"].iloc[-1])
    return "INDEX"


def _ihist(tk, years):
    import yfinance as yf
    start = (date.today() - timedelta(days=int(365.25 * years) + 30)).isoformat()
    try:
        raw = yf.Ticker(tk).history(start=start, interval="1d", auto_adjust=True, actions=False)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:120]
    df = _clean(raw)
    return df, (None if df is not None else "ไม่มีข้อมูล")


def _icsv():
    if not INDEX_CSV.is_file():
        return None, "ไม่มีไฟล์"
    try:
        d = pd.read_csv(INDEX_CSV, thousands=",")
        cols = {str(c).strip().lower(): c for c in d.columns}
        dc = next((cols[k] for k in ("date", "วันที่", "time") if k in cols), d.columns[0])
        cc = next((cols[k] for k in ("close", "price", "last", "ปิด") if k in cols), None)
        if cc is None:
            return None, "ไม่พบคอลัมน์ close"
        s = pd.Series(pd.to_numeric(d[cc], errors="coerce").to_numpy(), index=pd.to_datetime(d[dc], errors="coerce"))
        s = s[s.index.notna() & s.notna() & (s > 0)]
        s.index = pd.DatetimeIndex(s.index).normalize()
        s = s[~s.index.duplicated(keep="last")].sort_index()
        if not len(s):
            return None, "ไม่มีแถวที่ใช้ได้"
        return pd.DataFrame({"open": s, "high": s, "low": s, "close": s, "volume": np.nan}), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:120]


def fetch_index(years, say=None):
    """→ (DataFrame[close, grade] | None, คำอธิบาย)"""
    notes, best = [], None
    for tk, grade in INDEX_SOURCES:
        df, err = _icsv() if tk == "CSV" else _ihist(tk, years)
        if err is None and (df is None or len(df) < 300):
            err = f"{0 if df is None else len(df)} แท่ง (< 300)"
        if err is None:
            lag = int(np.busday_count(str(df.index[-1].date()), str(date.today())))
            if lag > 7:
                err = f"ล่าสุด {df.index[-1].date()} (ค้าง {lag} วันทำการ)"
        if err:
            notes.append(f"{tk} ✕ {err}")
            continue
        span = (df.index[-1] - df.index[0]).days / 365.25
        notes.append(f"{tk} ✓ {df.index[0].date()} → {df.index[-1].date()}")
        out = df[["close"]].copy()
        out["grade"] = grade
        if best is None or span > best[2] or span >= 0.8 * years:
            best = (out, tk, span)
        if span >= 0.8 * years:
            break
    txt = " · ".join(notes)
    if say:
        say("ดัชนีอ้างอิง: " + txt)
    if best is None:
        return None, "ไม่พบ — " + txt
    return best[0], f"ใช้ {best[1]} ({best[0]['grade'].iloc[-1]}) · " + txt


def load_data(symbols, P, force=False, say=None):
    syms = sorted({norm(s) for s in symbols if s and not str(s).startswith("^")})
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
    got = _yf([f"{s}.BK" for s in syms], yrs)
    idx, isrc = fetch_index(yrs, say)
    fr = {norm(k): v for k, v in got.items()}
    meta = {"fetched_at": _now(), "requested": len(syms), "got": len(fr), "missing": sorted(set(syms) - set(fr))[:200],
            "index_ok": idx is not None, "index_src": isrc, "cached": False}
    try:
        pd.to_pickle({"v": 2, "day": today, "years": yrs, "requested": syms, "frames": fr, "index": idx, "meta": meta}, DATA_FILE)
    except Exception as e:
        log.warning("บันทึก cache: %s", e)
    return fr, idx, meta


def confirmed(frames, idx, now: Optional[datetime] = None):
    """ตัดแท่งวันนี้ถ้ายังไม่ปิด (วันทำการก่อน 17:00) — สัญญาณใช้เฉพาะแท่งที่ปิดแล้ว"""
    now = now or datetime.now()
    if now.weekday() >= 5 or now.hour >= 17:
        return frames, idx, False
    td = pd.Timestamp(now.date())

    def cut(df):
        return None if df is None else df[df.index < td]
    return {s: cut(d) for s, d in frames.items()}, cut(idx), True


def _roll_pct(a, w: int, minp: int):
    """percentile ของค่าวันนี้เทียบ w−1 วันก่อนหน้า (causal)"""
    from numpy.lib.stride_tricks import sliding_window_view
    n, k = a.shape
    out = np.full((n, k), np.nan)
    if n < w:
        return out
    for j in range(k):
        W = sliding_window_view(a[:, j], w)
        last, prior = W[:, -1], W[:, :-1]
        with np.errstate(invalid="ignore"):
            valid = np.sum(~np.isnan(prior), axis=1)
            less = np.sum(prior < last[:, None], axis=1)
            out[w - 1:, j] = np.where((valid >= minp) & ~np.isnan(last), less / np.maximum(valid, 1) * 100, np.nan)
    return out


def _axis(frames, syms, idx_df) -> pd.DatetimeIndex:
    """แกนวันที่ = วันของดัชนี ∪ วันที่หุ้น ≥ 50% มีข้อมูล — ดัชนีค้าง/ขาดบางวัน ข้อมูลหุ้นวันล่าสุดต้องไม่หาย"""
    i0 = pd.DatetimeIndex(idx_df.index)
    if not syms:
        return i0
    cnt = pd.Series(np.concatenate([frames[s].index.values for s in syms])).value_counts()
    ex = pd.DatetimeIndex(cnt[cnt >= 0.5 * len(syms)].index)
    return i0.union(ex[ex >= i0[0]])


def prepare(frames, idx_df, P, min_syms: int = 5):
    mb = int(P.get("min_bars"))
    syms = sorted(s for s, d in frames.items() if d is not None and len(d))
    if idx_df is not None and len(idx_df) > 250:
        dates, grade = _axis(frames, syms, idx_df), _igrade(idx_df)
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
    O, H, L, C = (X.mask(bad) for X in (O, H, L, C))
    V = V.mask(bad | (V <= 0))
    drop = [s for s in syms if cnt[s] < mb or nb[s] > 0.05 * max(1, cnt[s])]
    keep = [s for s in syms if s not in drop]
    if len(keep) < min_syms:
        raise ValueError(f"หุ้นที่ใช้ได้ {len(keep)} ตัว (< {min_syms}) — ข้อมูลไม่พอ / ถูกกักกัน {len(drop)} ตัว")
    O, H, L, C, V = (X[keep] for X in (O, H, L, C, V))
    if grade != "PROXY":
        I = pd.to_numeric(idx_df["close"], errors="coerce").reindex(dates).ffill(limit=5)
    else:
        I = (1 + C.pct_change(fill_method=None).mean(axis=1).fillna(0.0)).cumprod() * 1000  # proxy = EW
    F = features(O, H, L, C, V, I, P)
    F.grade, F.quarantined, F.bad_bars = grade, drop, int(nb.sum())
    return F


def features(O, H, L, C, V, I, P):
    g = P.get
    with np.errstate(all="ignore"):
        n = int(g("atr_len"))
        pc = C.shift(1)
        TR = pd.DataFrame(np.fmax(np.fmax((H - L).to_numpy(), (H - pc).abs().to_numpy()), (L - pc).abs().to_numpy()),
                          index=C.index, columns=C.columns)
        ATR = TR.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
        vma = V.rolling(50, min_periods=40).mean()
        vratio = V / vma.shift(1)
        dry = (V.rolling(10, min_periods=8).mean() / vma).shift(1)
        bl = int(g("brk_len"))
        hh = H.rolling(bl, min_periods=bl).max().shift(1)
        h52 = C / H.rolling(252, min_periods=200).max()
        lc, li = np.log(C), np.log(I)
        rs63 = (lc - lc.shift(63)).sub(li - li.shift(63), axis=0)
        rsrank = rs63.rank(axis=1, pct=True) * 100
        rsl = C.div(I, axis=0)
        rsnh = rsl.rolling(5, min_periods=1).max() >= rsl.rolling(int(g("rs_hi_len")), min_periods=40).max() - 1e-12
        a10, a50 = TR.rolling(10, min_periods=8).mean(), TR.rolling(50, min_periods=40).mean()
        comp = _roll_pct(-(a10 / a50).to_numpy(dtype=float), 252, 200)
        sg, bs = int(g("seg_len")), int(g("base_len"))
        mx, mn = H.rolling(sg, min_periods=sg).max(), L.rolling(sg, min_periods=sg).min()
        rr = (mx - mn) / mx
        hb, lb = H.rolling(bs, min_periods=bs).max(), L.rolling(bs, min_periods=bs).min()
        depth = ((hb - lb) / hb).shift(1)
        adv = (C * V).rolling(20, min_periods=15).median()
        advsh = V.rolling(20, min_periods=15).median()
        sigma = lc.diff().rolling(20, min_periods=15).std()
        ma = C.rolling(int(g("breadth_sma")), min_periods=40).mean()
        cnt = ma.notna().sum(axis=1)
        br = ((C > ma) & ma.notna()).sum(axis=1) / cnt.where(cnt >= min(10, C.shape[1])) * 100
        ln = int(g("idx_sma"))
        isma = I.rolling(ln, min_periods=int(ln * 0.9)).mean()
        slope = (isma - isma.shift(int(g("slope_len")))) > 0
        above = I > isma
        reg = np.select([(isma.isna() | br.isna() | I.isna()).to_numpy(), (above & slope & (br >= 50)).to_numpy(),
                         above.to_numpy(), (br >= 40).to_numpy()], [4, 1, 2, 3], 4)
    mm = {1: g("m_r1"), 2: g("m_r2"), 3: g("m_r3"), 4: g("m_r4")}

    def A(X):
        return X.to_numpy(dtype=float)
    return SimpleNamespace(
        dates=pd.DatetimeIndex(C.index), syms=list(C.columns), n=len(C), k=C.shape[1],
        o=A(O), h=A(H), l=A(L), c=A(C), v=A(V), atr=A(ATR), vma=A(vma), vratio=A(vratio), dry=A(dry), hh=A(hh),
        h52=A(h52), rs63=A(rs63), rsrank=A(rsrank), rsnh=rsnh.to_numpy(dtype=bool), comp=comp, rr=A(rr),
        depth=A(depth), mx=A(mx), adv=A(adv), advsh=A(advsh), sigma=A(sigma), br=br.to_numpy(dtype=float),
        I=I.to_numpy(dtype=float), isma=isma.to_numpy(dtype=float), reg=reg.astype(int),
        m=np.array([mm[int(x)] for x in reg], dtype=float), liq=None)


# ═══════════════ SIGNALS ═══════════════
def retest(F, a1, pivot, P):
    """E2: หลัง breakout 5–15 แท่ง ย่อลงใกล้ pivot (≤ 0.5 ATR) · ปิดเหนือ pivot · volume ต่ำกว่าค่าเฉลี่ย"""
    out, lv = np.zeros(a1.shape, dtype=bool), np.full(a1.shape, np.nan)
    lo, hi, band = int(P.get("rt_min")), int(P.get("rt_max")), float(P.get("rt_band_atr"))
    for j in range(F.k):
        for b in np.flatnonzero(a1[:, j]):
            pv = pivot[b, j]
            if not np.isfinite(pv):
                continue
            for k in range(b + 1, min(F.n, b + hi + 1)):
                ck, lk = F.c[k, j], F.l[k, j]
                if not (np.isfinite(ck) and np.isfinite(lk)):
                    continue
                if ck < pv:
                    break
                if k >= b + lo and lk <= pv + band * F.atr[k, j] and F.v[k, j] < F.vma[k, j]:
                    out[k, j], lv[k, j] = True, pv
                    break
    return out, lv


def masks(F, P):
    g = P.get
    with np.errstate(invalid="ignore"):
        c, atr = F.c, F.atr
        liq = (F.adv >= g("min_adv_mb") * 1e6) & (c >= g("min_price"))
        lvl_b = F.hh + g("brk_buf_atr") * atr
        s0 = (c > lvl_b) & (F.vratio > g("vol_mult_b"))
        sg = int(g("seg_len"))
        r3, r2, r1 = _sh(F.rr, 1), _sh(F.rr, 1 + sg), _sh(F.rr, 1 + 2 * sg)
        pivot = _sh(F.mx, 1)
        base = (F.depth <= g("base_depth")) & (r1 > r2) & (r2 > r3) & (F.dry <= g("dry_max"))
        a1 = base & (c > pivot + g("pivot_buf_atr") * atr) & (F.vratio >= g("vol_mult_a"))
        rs_a = (F.rsrank >= g("rs_rank_min")) | F.rsnh
        bpre = (F.h52 > g("h52_min")) & (F.rs63 > 0) & (_sh(F.comp, 1) > g("comp_pct_min"))
        b4 = bpre & s0 & ((c - lvl_b) <= g("ext_atr") * atr)
    a2, lvl_rt = retest(F, a1, pivot, P)
    F.liq = liq
    a3 = a1 & rs_a & liq
    s = str(g("setups"))
    z = np.zeros(a3.shape, dtype=bool)
    Mk = {"S0": s0 & liq, "A1": a1 & liq, "A2": a2 & liq, "A3": a3, "A4": a3, "B4": b4 & liq}
    Mk["AB"] = (Mk["A4"] if "A" in s else z) | (Mk["B4"] if "B" in s else z)
    lab = np.where(Mk["A4"], 1, 2) if "A" in s else np.full(a3.shape, 2)
    lvl = {"S0": lvl_b, "A1": pivot, "A2": lvl_rt, "A3": pivot, "A4": pivot, "B4": lvl_b,
           "AB": np.where(lab == 1, pivot, lvl_b)}
    return Mk, lvl, lab, {"base": base & liq, "pivot": pivot, "lvl_b": lvl_b, "bpre": bpre & liq}


# ═══════════════ TRADE SIM ═══════════════
def _plan(F, j, t, en, P, weighted, stop_atr=None, on=None):
    g = P.get
    a = F.atr[t, j]
    if not (en == en and en > 0 and a == a and a > 0):
        return None, "NODATA"
    if stop_atr is None:
        seg = F.l[max(0, t - int(g("hl_len")) + 1):t + 1, j]
        seg = seg[np.isfinite(seg)]
        st = (float(seg.min()) if len(seg) else np.nan) - g("hl_buf_atr") * a
        if not np.isfinite(st) or en - st < g("stop_min_atr") * a:
            st = en - g("stop_min_atr") * a
        if en - st > g("stop_max_atr") * a or (en - st) / en * 100 > g("stop_max_pct"):
            return None, "RISK_WIDE"
    else:
        st = en - stop_atr * a
    if st <= 0 or st >= en:
        return None, "RISK_BAD"
    rd = en - st
    m = float(F.m[t]) if weighted else 1.0
    on = on or F.dates[min(t + 1, F.n - 1)].date()
    sig, asz, adv = F.sigma[t, j], F.advsh[t, j], F.adv[t, j]
    cs0 = en * cost_rt_pct(en, on, 0, None, None, P) / 100
    eq = g("equity")
    sh = math.floor(eq * g("risk_pct") / 100 * m / (rd + cs0) / 100) * 100
    sh = min(sh, math.floor(eq * g("max_val_pct") / 100 / en / 100) * 100)
    if adv == adv and adv > 0:
        sh = min(sh, math.floor(g("adv_cap_pct") / 100 * adv / en / 100) * 100)
    if sh <= 0:
        return None, "SIZE_ZERO"
    rt = cost_rt_pct(en, on, sh, float(sig) if sig == sig else None, float(asz) if asz == asz else None, P)
    cr = rt / 100 * en / rd
    if cr > g("max_cost_r"):
        return None, "COST"
    return {"en": float(en), "st": float(st), "rd": float(rd), "sh": int(sh), "m": m, "cost_R": float(cr),
            "rt_pct": float(rt), "risk_pct": float(rd / en * 100)}, None


def sim(F, j, t, P, lvl, weighted, lag=0, stop_atr=None):
    """สัญญาณที่ราคาปิดแท่ง t → เข้าที่ราคาเปิด t+1+lag · stop ใน bar (gap = ราคาเปิด · แท่งเดียวกับเป้า = stop ก่อน)
    · +2R ขาย part_frac แล้ว stop = ทุน · Chandelier / failed BO / time stop ตัดสินด้วยราคาปิด → ออกที่ราคาเปิดถัดไป"""
    e = t + 1 + int(lag)
    while e < F.n and not F.o[e, j] == F.o[e, j]:
        e += 1
    if e >= F.n:
        return None, "END"
    O, H, L, C = F.o[:, j], F.h[:, j], F.l[:, j], F.c[:, j]
    V, VM, AT = F.v[:, j], F.vma[:, j], F.atr[:, j]
    pl, why0 = _plan(F, j, t, float(O[e]), P, weighted, stop_atr, F.dates[e].date())
    if pl is None:
        return None, why0
    g = P.get
    en, rd, stop = pl["en"], pl["rd"], pl["st"]
    tgt, pf = en + g("be_r") * rd, float(g("part_frac"))
    fb, tb, tmr, mh, tra = int(g("fail_bars")), int(g("time_bars")), float(g("time_min_r")), int(g("max_hold")), float(g("trail_atr"))
    lv = float(lvl) if lvl is not None and lvl == lvl else en
    part, kp, ppx, pend = False, None, None, None
    hh, chand, mae, mfe = en, -np.inf, 0.0, 0.0
    px, why, x = None, None, None
    for k in range(e, F.n):
        o, h, l, c = O[k], H[k], L[k], C[k]
        if not (o == o and h == h and l == l and c == c):
            continue
        if o <= stop:
            px, x, why = float(o), k, ("BE" if part else "STOP")
            mae = min(mae, (o - en) / rd)
            break
        if l <= stop:
            px, x, why = float(stop), k, ("BE" if part else "STOP")
            mae = min(mae, (stop - en) / rd)
            break
        if not part and pf > 0 and h >= tgt:
            part, kp, ppx = True, k, float(max(o, tgt))
            stop = max(stop, en)
        mae, mfe = min(mae, (l - en) / rd), max(mfe, (h - en) / rd)
        hh = max(hh, h)
        if AT[k] == AT[k]:
            chand = max(chand, hh - tra * AT[k])
        held = k - e + 1
        r = None
        if c < chand:
            r = "TRAIL"
        elif held <= fb and c < lv and V[k] == V[k] and VM[k] == VM[k] and V[k] >= VM[k]:
            r = "FAILED"
        elif held >= tb and mfe < tmr:
            r = "TIME"
        elif held >= mh:
            r = "MAXHOLD"
        if r:
            nk = k + 1
            while nk < F.n and not O[nk] == O[nk]:
                nk += 1
            if nk < F.n:
                px, x, why = float(O[nk]), nk, r
                mae = min(mae, (O[nk] - en) / rd)
            else:
                pend = r
            break
    opn = px is None
    if opn:
        last = next((float(C[i]) for i in range(F.n - 1, e - 1, -1) if C[i] == C[i]), en)
        fr = (last - en) / rd
    else:
        fr = (px - en) / rd
    Rg = pf * (ppx - en) / rd + (1 - pf) * fr if part else fr
    rs = F.rs63[t, j]
    return {"j": j, "sym": F.syms[j], "t": t, "e": e, "x": x, "sig": str(F.dates[t].date()), "ent": str(F.dates[e].date()),
            "exd": None if x is None else str(F.dates[x].date()), "en": en, "st": pl["st"], "rd": rd,
            "risk_pct": pl["risk_pct"], "sh": pl["sh"], "m": pl["m"], "reg": int(F.reg[t]), "R_gross": float(Rg),
            "cost_R": pl["cost_R"], "R_net": float(Rg - pl["cost_R"]), "why": why or "OPEN", "pending": pend,
            "hold": None if x is None else x - e, "held": (x if x is not None else F.n - 1) - e, "mae": float(mae),
            "mfe": float(mfe), "part": part, "kp": kp, "ppx": ppx, "px": px, "rs": float(rs) if rs == rs else None,
            "open": opn, "lvl": lv}, None


def run_arm(F, mask, lvl, P, weighted, lag=0, lab=None):
    """ไม้ต่อหุ้นไม่ซ้อนกัน · cooldown หลังออก"""
    trades, sk = [], Counter()
    cd = int(P.get("cooldown"))
    for j in range(F.k):
        busy = -1
        for t in np.flatnonzero(mask[:, j]):
            t = int(t)
            if t <= busy:
                continue
            r, why = sim(F, j, t, P, lvl[t, j], weighted, lag)
            if r is None:
                sk[why] += 1
                continue
            if lab is not None:
                r["setup"] = "A" if lab[t, j] == 1 else "B"
            trades.append(r)
            busy = (r["x"] if r["x"] is not None else F.n) + cd
    trades.sort(key=lambda r: (r["e"], r["sym"]))
    return trades, dict(sk)


# ═══════════════ STATS ═══════════════
def tstats(trades, P, mult=1.0, weighted=False) -> Dict[str, Any]:
    rs = [r for r in trades if not r["open"]]
    n = len(rs)
    if not n:
        return {"n": 0, "days": 0}
    R = np.array([r["R_gross"] - mult * r["cost_R"] for r in rs])
    w = np.array([r["m"] if weighted else 1.0 for r in rs])
    if w.sum() <= 0:
        w = np.ones(n)
    mean = float((w * R).sum() / w.sum())
    win = R > 0
    nw = int(win.sum())
    aw = float(R[win].mean()) if nw else 0.0
    al = float(R[~win].mean()) if nw < n else 0.0
    pbe = abs(al) / (aw + abs(al)) if (aw + abs(al)) > 0 else None
    wl, _ = wilson(nw, n)
    neg = float(R[~win].sum())
    byd = defaultdict(lambda: [0.0, 0.0])
    for r, x, ww in zip(rs, R, w):
        byd[r["e"]][0] += ww * x
        byd[r["e"]][1] += ww
    keys = sorted(byd)
    dm = np.array([byd[k][0] / byd[k][1] for k in keys if byd[k][1] > 0])
    parts = [f for f in np.array_split(dm, 3) if len(f)]
    folds = [float(f.mean()) for f in parts]
    pos = [max(0.0, float(f.sum())) for f in parts]
    return {"n": n, "days": len(keys), "mean": mean, "mean_day": float(dm.mean()) if len(dm) else None, "t_day": _t(dm),
            "win": nw / n, "avg_win": aw, "avg_loss": al, "p_be": pbe, "wilson_lo": wl,
            "pf": float(R[win].sum() / abs(neg)) if neg < 0 else None, "folds": folds,
            "fold_pos": sum(1 for f in folds if f > 0), "fold_share": max(pos) / sum(pos) if sum(pos) > 0 else None,
            "gross": float(np.mean([r["R_gross"] for r in rs])), "cost_R": float(np.mean([r["cost_R"] for r in rs])),
            "first": rs[0]["ent"], "last": rs[-1]["ent"]}






def tables(trades, weighted) -> Dict[str, Any]:
    rs = [r for r in trades if not r["open"]]
    yr, rg, ex = defaultdict(list), defaultdict(list), defaultdict(list)
    for r in rs:
        yr[r["ent"][:4]].append(r["R_net"])
        rg[r["reg"]].append(r["R_net"])
        ex[r["why"]].append(r["R_net"])

    def row(k, v):
        x = np.array(v)
        return {"k": k, "n": len(x), "mean": float(x.mean()), "t": _t(x), "win": float((x > 0).mean())}
    ys = sorted(yr)
    roll = [float(np.mean(sum((yr[y] for y in ys[i:i + 3]), []))) for i in range(len(ys) - 2)]
    win = [r for r in rs if r["R_net"] > 0]
    los = [r for r in rs if r["R_net"] <= 0]
    hold = [r["hold"] for r in rs if r["hold"] is not None]
    return {"years": [row(k, yr[k]) for k in ys], "regime": [row(f"R{k}", rg[k]) for k in sorted(rg)],
            "exits": [row(k, v) for k, v in sorted(ex.items(), key=lambda kv: -len(kv[1]))],
            "roll3_pos": (sum(1 for x in roll if x > 0) / len(roll)) if roll else None, "roll3_n": len(roll),
            "win_mae_med": float(np.median([r["mae"] for r in win])) if win else None,
            "loss_mfe_ge1": float(np.mean([r["mfe"] >= 1 for r in los])) if los else None,
            "hold_med": float(np.median(hold)) if hold else None,
            "partial_share": float(np.mean([r["part"] for r in rs])) if rs else None,
            "open": sum(1 for r in trades if r["open"])}


def _perf(ret) -> Dict[str, Any]:
    r = np.nan_to_num(np.asarray(ret, dtype=float))
    if len(r) < 20:
        return {}
    sd = float(r.std(ddof=1))
    eq = np.cumprod(1 + r)
    yrs = len(r) / 252
    return {"sharpe": float(r.mean() / sd * math.sqrt(252)) if sd > 0 else None,
            "maxdd_pct": float((eq / np.maximum.accumulate(eq) - 1).min() * 100),
            "cagr_pct": float((eq[-1] ** (1 / yrs) - 1) * 100) if eq[-1] > 0 and yrs > 0 else None, "days": len(r)}


def portfolio(F, trades, P, start) -> Dict[str, Any]:
    """เลือกไม้ตามลำดับวันเข้า (เสมอกัน = RS63 สูงก่อน) · สูงสุด max_pos ไม้ · mark-to-market รายวัน · หักต้นทุนเข้า/ออก"""
    pf, eq0, mp = float(P.get("part_frac")), float(P.get("equity")), int(P.get("max_pos"))
    N = F.n
    pnl, expo = np.zeros(N), np.zeros(N)
    act, acc = [], 0
    for r in sorted(trades, key=lambda r: (r["e"], -(r["rs"] if r["rs"] is not None else -9e9))):
        e = r["e"]
        act = [a for a in act if a > e]
        if len(act) >= mp:
            continue
        x = r["x"]
        end = x if x is not None else N - 1
        act.append(x if x is not None else N)
        acc += 1
        C = F.c[:, r["j"]]
        sh = float(r["sh"])
        rem, last = sh, r["en"]
        cost = r["cost_R"] * r["rd"] * sh
        pnl[e] -= cost / 2
        if x is not None:
            pnl[x] -= cost / 2
        for k in range(e, end + 1):
            expo[k] += 1
            if x is not None and k == x:
                pnl[k] += rem * (r["px"] - last)
                break
            if r["part"] and k == r["kp"]:
                q = sh * pf
                pnl[k] += q * (r["ppx"] - last)
                rem -= q
            if C[k] == C[k]:
                pnl[k] += rem * (C[k] - last)
                last = C[k]
    eq = eq0 + np.cumsum(pnl)
    prev = np.r_[eq0, eq[:-1]]
    ret = (pnl / prev)[start:]
    return {**_perf(ret), "accepted": acc, "exposure": float(expo[start:].mean()) if N > start else None, "ret": ret}


def bench(F, start) -> Dict[str, Any]:
    I = pd.Series(F.I).ffill().to_numpy()
    S = pd.Series(F.isma).ffill().to_numpy()
    with np.errstate(all="ignore"):
        r = np.nan_to_num(np.r_[0.0, I[1:] / I[:-1] - 1])
        inn = np.r_[False, I[:-1] > S[:-1]]
    return {"bh": _perf(r[start:]), "sma": _perf((r * inn)[start:]), "grade": F.grade, "from": str(F.dates[start].date())}


def dsr(ret, trials) -> Optional[float]:
    """Deflated Sharpe (Bailey & López de Prado 2014) · SR0 จากจำนวน trial ใน ledger"""
    r = np.asarray(ret, dtype=float)
    r = r[np.isfinite(r)]
    T = len(r)
    if T < 60 or r.std(ddof=1) <= 0:
        return None
    sd0 = r.std()
    sr = r.mean() / r.std(ddof=1)
    sk = float(((r - r.mean()) ** 3).mean() / sd0 ** 3)
    ku = float(((r - r.mean()) ** 4).mean() / sd0 ** 4)
    N, gm = max(1, int(trials)), 0.5772156649
    sr0 = 0.0 if N <= 1 else math.sqrt(1.0 / (T - 1)) * ((1 - gm) * ND.inv_cdf(1 - 1.0 / N) + gm * ND.inv_cdf(1 - 1.0 / (N * math.e)))
    den = 1 - sk * sr + (ku - 1) / 4 * sr * sr
    if den <= 0:
        return None
    return float(ND.cdf((sr - sr0) * math.sqrt(T - 1) / math.sqrt(den)))


def placebo(F, trades, P, weighted, reps, seed=11, cap=1500) -> Dict[str, Any]:
    """random entry ที่จับคู่เวลา: หุ้นอื่นที่ผ่าน L0 "วันเดียวกัน" · stop หน่วย ATR เท่าไม้จริง · กฎออก/ต้นทุนเดียวกัน
    → หักผลของช่วงตลาดออก (บทเรียนจาก diag_shortlist v1.0)"""
    rs = [r for r in trades if not r["open"]]
    if len(rs) < 10:
        return {"available": False, "reason": "ไม้ปิดแล้วน้อยกว่า 10"}
    rng = np.random.default_rng(seed)
    if len(rs) > cap:
        rs = [rs[i] for i in sorted(rng.choice(len(rs), cap, replace=False))]
    w = np.array([r["m"] if weighted else 1.0 for r in rs])
    obs = float((w * np.array([r["R_net"] for r in rs])).sum() / w.sum())
    cache: Dict[int, np.ndarray] = {}
    dist = []
    for _ in range(int(reps)):
        acc = ws = 0.0
        for r in rs:
            t = r["t"]
            if t not in cache:
                ok = F.liq[t] & np.isfinite(F.atr[t])
                if t + 1 < F.n:
                    ok &= np.isfinite(F.o[t + 1])
                cache[t] = np.flatnonzero(ok)
            cs = cache[t]
            cs = cs[cs != r["j"]]
            if not len(cs):
                continue
            j2 = int(cs[rng.integers(len(cs))])
            a = F.atr[t, r["j"]]
            res, _ = sim(F, j2, t, P, F.c[t, j2], weighted, 0, stop_atr=(r["en"] - r["st"]) / a)
            if res is None or res["open"]:
                continue
            ww = res["m"] if weighted else 1.0
            acc += ww * res["R_net"]
            ws += ww
        if ws:
            dist.append(acc / ws)
    if not dist:
        return {"available": False, "reason": "สุ่มไม่ได้ (ไม่มีหุ้นอื่นผ่าน L0 วันเดียวกัน)"}
    d = np.array(dist)
    return {"available": True, "obs": obs, "null_mean": float(d.mean()), "p5": float(np.percentile(d, 5)),
            "p95": float(np.percentile(d, 95)), "pct": float((d < obs).mean()), "reps": len(d), "n": len(rs)}


def sensitivity(F, P) -> List[Dict[str, Any]]:
    s = float(P.get("sens_pct")) / 100
    out = []
    for keys, lab in ((("vol_mult_a", "vol_mult_b"), "เกณฑ์ volume"), (("base_depth",), "ความลึกฐาน"),
                      (("trail_atr",), "Chandelier ATR")):
        for sg in (-1, 1):
            d = _pdict(P)
            for k in keys:
                d[k] = d[k] * (1 + sg * s)
            P2 = _P(**d)
            Mk, lvl, lab2, _ = masks(F, P2)
            tr, _ = run_arm(F, Mk["AB"], lvl["AB"], P2, True, 0, lab2)
            st = tstats(tr, P2, 1.0, True)
            out.append({"param": lab, "change": f"{sg * s * 100:+.0f}%", "n": st["n"], "mean": st.get("mean"),
                        "t": st.get("t_day")})
    return out


# ═══════════════ GATES ═══════════════
def gates(a, bh, sens, void, P, trials):
    g = P.get
    s = a["stats"]
    G = []

    def add(k, lab, ok, det):
        G.append({"key": k, "label": lab, "ok": bool(ok), "detail": det})

    def f(x, d=3):
        return "—" if x is None else f"{x:+.{d}f}"
    n = s.get("n", 0)
    add("N", f"ไม้ ≥ {int(g('need_n'))} และวันเข้า ≥ {int(g('need_days'))}",
        n >= g("need_n") and s.get("days", 0) >= g("need_days"), f"{n} ไม้ · {s.get('days', 0)} วัน")
    add("T", f"R สุทธิเฉลี่ย > 0 และ t รายวัน ≥ {g('t_need'):g}",
        (s.get("mean") or -1) > 0 and (s.get("t_day") or -9) >= g("t_need"), f"เฉลี่ย {f(s.get('mean'))}R · t {f(s.get('t_day'), 2)}")
    add("WILSON", "Wilson ล่างของ win rate > จุดคุ้มทุน", (s.get("wilson_lo") or 0) > (s.get("p_be") or 1),
        f"Wilson ล่าง {(s.get('wilson_lo') or 0) * 100:.1f}% vs คุ้มทุน {(s.get('p_be') or 0) * 100:.1f}%")
    c2 = a.get("c2") or {}
    add("COST2", "ต้นทุน ×2 ยังบวก", (c2.get("mean") or -1) > 0, f"{f(c2.get('mean'))}R")
    lg = a.get("lag") or {}
    add("LAG", "เข้าช้า 1 วันยังบวก", (lg.get("mean") or -1) > 0, f"{f(lg.get('mean'))}R ({lg.get('n', 0)} ไม้)")
    nl = a.get("null") or {}
    add("NULL", f"ชนะ random entry วันเดียวกัน ≥ P{g('null_pct') * 100:.0f}",
        nl.get("available") and nl["pct"] >= g("null_pct"),
        f"percentile {nl['pct'] * 100:.1f} · random {f(nl['null_mean'])}R" if nl.get("available") else nl.get("reason", "—"))
    add("FOLD", f"บวก ≥ 2/3 ช่วงเวลา และกำไรช่วงเดียว ≤ {g('fold_max') * 100:.0f}%",
        s.get("fold_pos", 0) >= 2 and (s.get("fold_share") or 1) <= g("fold_max"),
        "ช่วง " + " · ".join(f(x) for x in s.get("folds") or []) + f" · ส่วนแบ่งสูงสุด {f(s.get('fold_share'), 2)}")
    pt, b0 = a.get("port") or {}, (bh.get("bh") or {})
    add("SHARPE", "Sharpe พอร์ต > Sharpe ถือดัชนี", (pt.get("sharpe") or -9) > (b0.get("sharpe") if b0.get("sharpe") is not None else 9),
        f"{f(pt.get('sharpe'), 2)} vs {f(b0.get('sharpe'), 2)}")
    add("DSR", f"Deflated Sharpe ≥ {g('dsr_need'):g} (trial {trials})", (a.get("dsr") or 0) >= g("dsr_need"),
        f"{f(a.get('dsr'), 3)}")
    if sens is not None:
        add("SENS", f"พารามิเตอร์ ±{g('sens_pct'):g}% ยังบวกทุกชุด", bool(sens) and all((x["mean"] or -1) > 0 for x in sens),
            " · ".join(f"{x['param']} {x['change']} {f(x['mean'])}" for x in sens))
    if void:
        st = "VOID"
    elif n < g("min_n"):
        st = "INSUFFICIENT"
    elif (s.get("mean") or 0) < 0 and (s.get("t_day") or 0) <= -2:
        st = "CONTRADICTED"
    elif all(x["ok"] for x in G):
        st = "BACKTEST_PASS"
    else:
        st = "NOT_PROVEN"
    return G, st


def ablation(arms, P) -> List[Dict[str, Any]]:
    out = []
    for k, b, lab in ABL:
        s1, s0 = arms[k]["stats"], arms[b]["stats"]
        ok = s1.get("n", 0) >= P.get("min_n") and s0.get("n", 0) >= P.get("min_n")
        d = (s1["mean"] - s0["mean"]) if ok else None
        out.append({"layer": lab, "arm": k, "vs": b, "diff": d,
                    "verdict": "ข้อมูลไม่พอ" if not ok else "ช่วย" if d > 0 else "ไม่ช่วย — พิจารณาตัด (นับเป็น trial ใหม่)"})
    return out


# ═══════════════ SCAN · FORWARD ═══════════════
def scan(F, Mk, lvl, lab, X, P) -> Dict[str, Any]:
    g = P.get
    t = F.n - 1
    asof = F.dates[t].date()
    lagd = int(np.busday_count(str(asof), str(date.today())))
    on = next_bday(asof)
    ent, wat = [], []
    for j in range(F.k):
        c, a = F.c[t, j], F.atr[t, j]
        if not (c == c and a == a and a > 0):
            continue
        base = {"symbol": F.syms[j], "close": float(c), "atr_pct": float(a / c * 100), "rs_rank": _f2(F.rsrank[t, j], 1),
                "rs63": _f2(F.rs63[t, j]), "vratio": _f2(F.vratio[t, j], 2), "tick_pct": _f2((tick_at(c, on) or 0) / c * 100)}
        if Mk["AB"][t, j]:
            pl, why = _plan(F, j, t, float(c), P, True, None, on)
            ent.append({**base, "setup": "A" if lab[t, j] == 1 else "B", "ref": float(c), "lvl": _f2(lvl["AB"][t, j], 4),
                        "stop": None if pl is None else round(pl["st"], 4), "risk_pct": None if pl is None else round(pl["risk_pct"], 2),
                        "shares": None if pl is None else pl["sh"], "cost_R": None if pl is None else round(pl["cost_R"], 3),
                        "skip": why})
            continue
        if X["base"][t, j]:
            lv = X["pivot"][t, j] + g("pivot_buf_atr") * a
            if lv - a <= c < lv:
                wat.append({**base, "setup": "A", "level": round(float(lv), 4), "dist_atr": round(float((lv - c) / a), 2)})
                continue
        if X["bpre"][t, j]:
            lv = X["lvl_b"][t, j]
            if lv == lv and lv - a <= c <= lv:
                wat.append({**base, "setup": "B", "level": round(float(lv), 4), "dist_atr": round(float((lv - c) / a), 2)})
    ent.sort(key=lambda x: -(x["rs_rank"] or 0))
    wat.sort(key=lambda x: x["dist_atr"])
    return {"asof": str(asof), "stale": lagd > int(g("stale_days")), "lag_days": lagd, "regime": int(F.reg[t]),
            "m": float(F.m[t]), "breadth": _f2(F.br[t], 1), "entries": ent, "watch": wat[:60], "n_watch": len(wat)}


def fwd_log(sc, P, path=SIG_LOG) -> int:
    h, n = prereg_hash(P), 0
    for e in sc.get("entries") or []:
        if e.get("skip"):
            continue
        n += append_line(path, {"key": f"{sc['asof']}|{e['symbol']}|{e['setup']}", "ts_release": _now(), "asof": sc["asof"],
                                "symbol": e["symbol"], "setup": e["setup"], "ref": e["ref"], "stop": e["stop"],
                                "lvl": e["lvl"], "regime": sc["regime"], "m": sc["m"], "prereg_hash": h,
                                "version": __version__})
    return n


def fwd_eval(F, P, path=SIG_LOG) -> Dict[str, Any]:
    h = prereg_hash(P)
    L = read_lines(path)
    pos = {s: j for j, s in enumerate(F.syms)}
    dix = {str(d.date()): i for i, d in enumerate(F.dates)}
    rows, other = [], 0
    for e in L:
        if e.get("prereg_hash") != h:
            other += 1
            continue
        j, t = pos.get(e.get("symbol")), dix.get(str(e.get("asof")))
        base = {"asof": e.get("asof"), "symbol": e.get("symbol"), "setup": e.get("setup")}
        if j is None or t is None:
            rows.append({**base, "status": "NODATA"})
            continue
        r, why = sim(F, j, t, P, _n(e.get("lvl")), True, 0)
        if r is None:
            rows.append({**base, "status": "SKIP", "why": why})
            continue
        st = "OPEN" if r["open"] else "CLOSED"
        ph = None
        if st == "OPEN":
            lc = F.c[F.n - 1, j]
            ph = "CONFIRM" if 5 <= r["held"] <= 10 and lc == lc and lc > r["lvl"] else "HOLD"
        rows.append({**base, "status": st, "entry": r["en"], "stop": r["st"], "R_net": r["R_net"], "why": r["why"],
                     "pending": r["pending"], "held": r["held"], "phase": ph, "exit_date": r["exd"]})
    cl = [x for x in rows if x["status"] == "CLOSED"]
    out = {"logged": len(L), "this_hash": len(L) - other, "other_hash": other, "closed": len(cl),
           "open": sum(1 for x in rows if x["status"] == "OPEN"), "need": int(P.get("fwd_need")),
           "open_rows": [x for x in rows if x["status"] == "OPEN"][-60:], "recent": cl[-40:]}
    if not cl:
        return {**out, "state": "INSUFFICIENT"}
    R = np.array([x["R_net"] for x in cl])
    nw = int((R > 0).sum())
    aw = float(R[R > 0].mean()) if nw else 0.0
    al = float(R[R <= 0].mean()) if nw < len(R) else 0.0
    pbe = abs(al) / (aw + abs(al)) if aw + abs(al) > 0 else None
    wl, _ = wilson(nw, len(R))
    t = _t(R)
    if len(R) < P.get("fwd_need"):
        st = "INSUFFICIENT"
    elif R.mean() > 0 and pbe is not None and wl > pbe:
        st = "CONSISTENT"
    elif R.mean() < 0 and (t or 0) <= -2:
        st = "CONTRADICTED"
    else:
        st = "NOT_FOUND"
    return {**out, "state": st, "mean": float(R.mean()), "t": t, "win": nw / len(R), "wilson_lo": wl, "p_be": pbe}


# ═══════════════ RUN ═══════════════
def trial_mark(P, path=TRIALS) -> int:
    h = prereg_hash(P)
    if h not in {e.get("key") for e in read_lines(path)}:
        append_line(path, {"key": h, "ts_release": _now(), "version": __version__,
                           "params": {k: P.get(k) for k in sorted(DEFAULTS) if k not in OPS}})
    return max(1, len({e.get("key") for e in read_lines(path)}))


def compute(frames, idx_df, meta, P, say=None, trials=1, min_syms=5):
    say = say or (lambda s: None)
    say("เตรียมข้อมูลและ feature…")
    F = prepare(frames, idx_df, P, min_syms)
    Mk, lvl, lab, X = masks(F, P)
    start = min(F.n - 1, 260)
    arms = {}
    for key, label, wtd in ARMS:
        say(f"backtest {label}…")
        tr, sk = run_arm(F, Mk[key], lvl[key], P, wtd, 0, lab if key == "AB" else None)
        arms[key] = {"key": key, "label": label, "weighted": wtd, "signals": int(Mk[key].sum()), "trades": tr,
                     "skips": sk, "stats": tstats(tr, P, 1.0, wtd), "state": "—"}
    reps = int(P.get("null_reps"))
    for key in MAIN:
        a = arms[key]
        tr, w = a["trades"], a["weighted"]
        say(f"ตรวจ edge {key}: ต้นทุน ×0–3 · เข้าช้า 1 วัน · random entry วันเดียวกัน · พอร์ต…")
        a["cost_sweep"] = {f"x{m}": tstats(tr, P, m, w).get("mean") for m in (0, 1, 2, 3)}
        a["c2"] = tstats(tr, P, 2.0, w)
        lt, _ = run_arm(F, Mk[key], lvl[key], P, w, 1, None)
        a["lag"] = tstats(lt, P, 1.0, w)
        a["null"] = placebo(F, tr, P, w, reps if key == "AB" else max(20, reps // 2))
        pt = portfolio(F, tr, P, start)
        a["dsr"] = dsr(pt.pop("ret"), trials)
        a["port"] = pt
        a["tables"] = tables(tr, w)
    bh = bench(F, start)
    say("sensitivity ±… (6 ชุด)…")
    sens = sensitivity(F, P)
    void = []
    if F.grade == "PROXY":
        void.append("ไม่มีดัชนี SET จริง (^SET.BK) — ใช้ค่าแทน: regime / RS / Sharpe เทียบไม่ได้ (TH_CORE V3)")
    cov = F.k / max(1, meta.get("requested") or F.k)
    if cov < 0.7:
        void.append(f"ได้ข้อมูลหุ้น {F.k}/{meta.get('requested')} ({cov * 100:.0f}%) < 70%")
    for key in MAIN:
        arms[key]["gates"], arms[key]["state"] = gates(arms[key], bh, sens if key == "AB" else None, void, P, trials)
    return F, arms, bh, sens, void, Mk, lvl, lab, X


def write_trades(arms, path=TRADES_CSV):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_COLS)
        for a in arms.values():
            for r in a["trades"]:
                w.writerow([a["key"], r.get("setup", ""), r["sym"], r["sig"], r["ent"], r["exd"] or "", round(r["en"], 4),
                            round(r["st"], 4), round(r["risk_pct"], 3), r["sh"], r["m"], r["reg"], round(r["R_gross"], 4),
                            round(r["cost_R"], 4), round(r["R_net"], 4), r["why"], "" if r["hold"] is None else r["hold"],
                            round(r["mae"], 3), round(r["mfe"], 3), int(r["part"]), int(r["open"])])


def read_trades(arm="AB", limit=500) -> List[Dict[str, Any]]:
    if not TRADES_CSV.is_file():
        return []
    with open(TRADES_CSV, encoding="utf-8") as f:
        out = [r for r in csv.DictReader(f) if r.get("arm") == arm]
    return out[-int(limit):]


def assemble(F, arms, bh, sens, void, sc, fw, meta, P, trials, t0, src, nlog, cut) -> Dict[str, Any]:
    st = arms["AB"]["state"]
    if st == "BACKTEST_PASS" and fw.get("state") == "CONSISTENT":
        st = "READY"
    elif fw.get("state") == "CONTRADICTED" and st != "VOID":
        st = "FORWARD_CONTRADICTED"
    t = F.n - 1
    return _js({
        "version": __version__, "computed_at": _now(), "computed_day": date.today().isoformat(),
        "compute_sec": round(time.time() - t0, 1), "prereg_hash": prereg_hash(P), "trials": trials,
        "universe_source": src, "state": st, "state_label": STATE_TH.get(st, st),
        "data": {"grade": F.grade, "symbols": F.k, "requested": meta.get("requested"), "missing": (meta.get("missing") or [])[:60],
                 "quarantined": F.quarantined[:60], "bad_bars": F.bad_bars, "first": str(F.dates[0].date()),
                 "last": str(F.dates[-1].date()), "bars": F.n, "fetched_at": meta.get("fetched_at"),
                 "cached": meta.get("cached"), "cut_today": cut, "index_src": meta.get("index_src")},
        "regime_now": {"asof": str(F.dates[t].date()), "regime": int(F.reg[t]), "m": float(F.m[t]), "breadth": F.br[t],
                       "index": F.I[t], "index_sma": F.isma[t]},
        "arms": [{k: v for k, v in a.items() if k != "trades"} for a in arms.values()],
        "ablation": ablation(arms, P), "bench": bh, "sens": sens, "void": void, "scan": sc, "forward": fw,
        "forward_logged": nlog, "notes": NOTES, "state_names": STATE_TH})


_CLI_SYMS: List[str] = []
_SVC: Dict[str, Any] = {"svc": None}


def universe(svc=None) -> Tuple[List[str], str]:
    if _CLI_SYMS:
        return list(_CLI_SYMS), "--symbols"
    if UNI_FILE.is_file():
        s = [norm(x.split("#")[0]) for x in UNI_FILE.read_text("utf-8").splitlines()]
        s = sorted({x for x in s if x})
        if s:
            return s, UNI_FILE.name
    svc = svc or _SVC.get("svc")
    xc = getattr(svc, "xray_cache", None) if svc is not None else None
    if xc:
        s = sorted({norm(k) for k in list(xc) if not str(k).startswith("^")})
        if s:
            _jsave(UNI_CACHE, {"at": _now(), "symbols": s})
            return s, "แอป (xray_cache)"
    c = _jload(UNI_CACHE).get("symbols") or []
    return list(c), (UNI_CACHE.name if c else "ไม่มี")


def run(force=False, reps=None, say=None) -> Dict[str, Any]:
    say = say or (lambda s: None)
    t0 = time.time()
    P = POLICY.snap()
    if reps:
        P.d["null_reps"] = int(reps)
    syms, src = universe()
    if len(syms) < 5:
        raise RuntimeError("ไม่มี universe — สร้าง sq_etrend_universe.txt (1 หุ้นต่อบรรทัด) · ใช้ --symbols · "
                           "หรือเปิด serve.py หนึ่งครั้งหลังติดตั้งส่วนที่ 3")
    frames, idx, meta = load_data(syms, P, force, say)
    frames, idx, cut = confirmed(frames, idx)
    trials = trial_mark(P)
    F, arms, bh, sens, void, Mk, lvl, lab, X = compute(frames, idx, meta, P, say, trials)
    sc = scan(F, Mk, lvl, lab, X, P)
    nlog = 0 if sc["stale"] else fwd_log(sc, P)
    fw = fwd_eval(F, P)
    write_trades(arms)
    rep = assemble(F, arms, bh, sens, void, sc, fw, meta, P, trials, t0, src, nlog, cut)
    _jsave(RES_FILE, rep)
    return rep


_RUN: Dict[str, Any] = {"th": None, "state": "idle", "msg": "", "started": None, "finished": None, "err": None}
_RL = threading.RLock()


def status() -> Dict[str, Any]:
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
                log.exception("etrend run")
                _RUN.update(state="error", err=f"{type(e).__name__}: {e}")
            finally:
                _RUN["finished"] = _now()
        _RUN.update(state="running", msg="เริ่ม", started=_now(), err=None)
        th = threading.Thread(target=job, daemon=True, name="sq-etrend")
        _RUN["th"] = th
        th.start()
        return True


# ═══════════════ TEXT REPORT (CLI) ═══════════════
def text_report(rep, only_scan=False) -> str:
    def f(x, d=3):
        return "—" if x is None else f"{x:+.{d}f}"
    L, d = [], rep.get("data") or {}
    L.append(f"Early Trend Lab v{rep.get('version')} · ข้อมูล {d.get('first')} → {d.get('last')} · หุ้น {d.get('symbols')}/"
             f"{d.get('requested')} · ดัชนี {d.get('grade')} · กักกัน {len(d.get('quarantined') or [])} · trial {rep.get('trials')} "
             f"· prereg {rep.get('prereg_hash')} · {rep.get('compute_sec')} วิ")
    if not only_scan:
        L.append("ดัชนีอ้างอิง: " + str(d.get("index_src")))
        L.append(f"\nสถานะรวม: {rep.get('state_label')}")
        for v in rep.get("void") or []:
            L.append("  VOID: " + v)
        L.append("\n═══ Ablation (R สุทธิต่อไม้ · t ระดับวันเข้า) ═══")
        for a in rep.get("arms") or []:
            s = a["stats"]
            L.append(f"  {a['key']:<3} {s.get('n', 0):5d} ไม้ {s.get('days', 0):5d} วัน  เฉลี่ย {f(s.get('mean'))}R  "
                     f"t {f(s.get('t_day'), 2)}  ชนะ {(s.get('win') or 0) * 100:4.1f}%  gross {f(s.get('gross'))}  "
                     f"ต้นทุน {f(s.get('cost_R'))}  {STATE_TH.get(a.get('state'), '')}")
        for x in rep.get("ablation") or []:
            L.append(f"  ชั้น {x['layer']:<20} {x['arm']} vs {x['vs']}: {f(x['diff'])}R → {x['verdict']}")
        for a in rep.get("arms") or []:
            if a["key"] not in MAIN:
                continue
            L.append(f"\n═══ {a['label']} → {STATE_TH.get(a['state'])} ═══")
            for g in a.get("gates") or []:
                L.append(f"  {'✓' if g['ok'] else '✕'} {g['label']} — {g['detail']}")
            cs, pt = a.get("cost_sweep") or {}, a.get("port") or {}
            L.append("  ต้นทุน ×0/×1/×2/×3: " + " / ".join(f(cs.get(f"x{m}")) for m in (0, 1, 2, 3)))
            L.append(f"  พอร์ต: Sharpe {f(pt.get('sharpe'), 2)} · DD {f(pt.get('maxdd_pct'), 1)}% · CAGR {f(pt.get('cagr_pct'), 1)}% "
                     f"· ไม้ที่รับ {pt.get('accepted')} · ถือเฉลี่ย {f(pt.get('exposure'), 1)} ตัว · DSR {f(a.get('dsr'))}")
            tb = a.get("tables") or {}
            L.append("  รายปี: " + " · ".join(f"{y['k']} {y['n']}/{f(y['mean'], 2)}" for y in tb.get("years") or []))
            L.append("  regime: " + " · ".join(f"{y['k']} {y['n']}/{f(y['mean'], 2)}" for y in tb.get("regime") or [])
                     + f" · ช่วง 3 ปีที่บวก {f(tb.get('roll3_pos'), 2)}")
            L.append("  ออกเพราะ: " + " · ".join(f"{y['k']} {y['n']}/{f(y['mean'], 2)}" for y in tb.get("exits") or []))
        b = rep.get("bench") or {}
        L.append(f"\nถือดัชนี: Sharpe {f((b.get('bh') or {}).get('sharpe'), 2)} · DD {f((b.get('bh') or {}).get('maxdd_pct'), 1)}% "
                 f"· ดัชนี > SMA200: Sharpe {f((b.get('sma') or {}).get('sharpe'), 2)} (ตั้งแต่ {b.get('from')})")
    sc, fw = rep.get("scan") or {}, rep.get("forward") or {}
    L.append(f"\n═══ สแกน ณ {sc.get('asof')} · regime R{sc.get('regime')} (m {sc.get('m')}) · breadth {sc.get('breadth')}% "
             f"{'· ⚠ ข้อมูลค้าง ' + str(sc.get('lag_days')) + ' วันทำการ (ไม่บันทึก forward)' if sc.get('stale') else ''} ═══")
    for e in (sc.get("entries") or [])[:20]:
        L.append(f"  ซื้อ {e['symbol']:<8} {e['setup']}  ปิด {e['ref']:.2f}  stop {e['stop']}  เสี่ยง {e['risk_pct']}%  "
                 f"{e['shares']} หุ้น  ต้นทุน {e['cost_R']}R  RS {e['rs_rank']}" + (f"  ข้าม: {e['skip']}" if e["skip"] else ""))
    for w in (sc.get("watch") or [])[:15]:
        L.append(f"  เฝ้า {w['symbol']:<8} {w['setup']}  ปิด {w['close']:.2f}  ระดับ {w['level']}  ห่าง {w['dist_atr']} ATR")
    L.append(f"\nForward paper: บันทึก {fw.get('logged')} · ปิดแล้ว {fw.get('closed')}/{fw.get('need')} · เปิด {fw.get('open')} "
             f"· สถานะ {fw.get('state')}" + (f" · เฉลี่ย {f(fw.get('mean'))}R" if fw.get("mean") is not None else ""))
    if not only_scan:
        L.append("\n" + "\n".join("หมายเหตุ: " + x for x in rep.get("notes") or []))
    return "\n".join(L)


# ═══════════════ SELFTEST ═══════════════
def _flat(n=300):
    ix = pd.bdate_range("2023-01-02", periods=n)
    df = pd.DataFrame({"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1e6}, index=ix)
    return {"TST": df}, pd.DataFrame({"close": 1000.0}, index=ix)


def _base_case(dry=True):
    rows = []
    for i in range(300):
        c = 100 + (0.3 if i % 2 else -0.3)
        rows.append((c, c + 0.5, c - 0.5, c, 1e6))
    for amp, vol in ((3.0, 1e6), (1.5, 7e5), (0.75, 4e5)):
        for i in range(20):
            c = 100 + (amp if i % 2 == 0 else -amp)
            rows.append((c, c + amp, c - amp, c, vol if dry else 1e6))
    rows.append((101.0, 104.0, 101.0, 103.5, 3e6))
    for i in range(30):
        rows.append((103.5, 104.0, 103.0, 103.5, 1e6))
    ix = pd.bdate_range("2023-01-02", periods=len(rows))
    df = pd.DataFrame(rows, columns=["open", "high", "low", "close", "volume"], index=ix)
    return {"TST": df}, pd.DataFrame({"close": 100.0}, index=ix), 360


def _synth(k=30, n=500, seed=5, drift=0.0):
    rng = np.random.default_rng(seed)
    ix = pd.bdate_range("2019-01-01", periods=n)
    fr = {}
    for i in range(k):
        r = rng.normal(0.0, 0.015, n)
        v = rng.lognormal(math.log(1e6), 0.25, n)
        t, ev = 280 + int(rng.integers(0, 30)), []
        while t < n - 40:
            ev.append(t)
            t += 60 + int(rng.integers(0, 20))
        for t in ev:
            c = 50 * np.exp(np.cumsum(r))
            r[t] = math.log(c[t - 55:t].max() * 1.01 * 1.03 / c[t - 1])
            r[t + 1:t + 31] += drift
            v[t] *= 4
        c = 50 * np.exp(np.cumsum(r))
        h, l = c * 1.01, c * 0.99
        fr[f"S{i:02d}"] = pd.DataFrame({"open": np.clip(np.r_[c[0], c[:-1]], l, h), "high": h, "low": l, "close": c,
                                        "volume": v}, index=ix)
    idx = pd.DataFrame({"close": 1000 * np.exp(np.cumsum(rng.normal(0.0002, 0.008, n)))}, index=ix)
    return fr, idx


def selftest() -> Dict[str, Any]:
    res = []

    def T(name, fn):
        try:
            ok, det = fn()
        except Exception as e:
            ok, det = False, f"{type(e).__name__}: {e}"
        res.append({"name": name, "ok": bool(ok), "detail": str(det)})

    O_, N_ = date(2026, 11, 13), date(2026, 11, 16)
    tk = [tick_at(5, O_), tick_at(5, N_), tick_at(30, O_), tick_at(30, N_), tick_at(1.99, N_), tick_at(400, N_)]
    T("ตาราง tick ตามวัน: 5 บาท 0.05 → 0.02 · 30 บาท 0.25 → 0.10 · < 2 บาท 0.01 · ≥ 400 บาท 2",
      lambda: (tk == [0.05, 0.02, 0.25, 0.10, 0.01, 2.0], str(tk)))
    cr = cost_rt_pct(10.0, N_, 0, None, None, _P())
    T("ต้นทุนไป-กลับ 10 บาท (tick ใหม่): ค่าคอม+ตลาด×VAT 0.336% + 1.5 ช่อง 0.75% = 1.086%",
      lambda: (abs(cr - 1.08598) < 1e-4, f"{cr:.5f}%"))
    P1 = _P(min_bars=260)
    fr, ix, tb = _base_case(True)
    F1 = prepare(fr, ix, P1, 1)
    M1 = masks(F1, P1)[0]
    fr0, ix0, _ = _base_case(False)
    F0 = prepare(fr0, ix0, P1, 1)
    M0 = masks(F0, P1)[0]
    T("Setup A: ฐาน 60 แท่ง contraction 11% → 6% → 3% + volume แห้ง + breakout → สัญญาณที่แท่ง breakout เท่านั้น · volume ไม่แห้ง → ไม่มีสัญญาณ",
      lambda: (bool(M1["A1"][tb, 0]) and int(M1["A1"][:tb, 0].sum()) == 0 and not M0["A1"][tb, 0],
               f"แท่ง {tb}: {bool(M1['A1'][tb, 0])} · ก่อนหน้า {int(M1['A1'][:tb, 0].sum())} · ไม่แห้ง {bool(M0['A1'][tb, 0])}"))
    PZ = _P(min_bars=260, comm_pct=0, venue_pct=0, exec_ticks=0, impact_k=0, ftt_pct=0)
    frf, ixf = _flat()
    Ff = prepare(frf, ixf, PZ, 1)
    Ff.liq = np.ones((Ff.n, Ff.k), dtype=bool)

    def case(edit):
        G = copy.deepcopy(Ff)
        edit(G)
        return sim(G, 0, 250, PZ, 90.0, False)[0]

    def bar(G, k, o, h, l, c):
        G.o[k, 0], G.h[k, 0], G.l[k, 0], G.c[k, 0] = o, h, l, c
    ra = case(lambda G: bar(G, 253, 95, 96, 94, 95))
    rb = case(lambda G: bar(G, 252, 100, 107, 96, 100))

    def ec(G):
        bar(G, 252, 100, 106.5, 100, 105)
        bar(G, 253, 104, 104.5, 99.5, 101)
    rc = case(ec)
    rd = case(lambda G: None)

    def ee(G):
        for i, k in enumerate(range(252, 262)):
            c = 102 + 2 * i
            bar(G, k, c - 1, c + 1, c - 1, c)
        bar(G, 262, 118, 118.5, 110, 110)
        for k in range(263, G.n):
            bar(G, k, 110, 111, 109, 110)
    re_ = case(ee)

    #T("จำลองไม้: gap ใต้ stop = ราคาเปิด (−1.67R) · แท่งเดียวกับเป้า = stop ก่อน (−1R) · +2R ขาย 1/3 แล้ว BE (+0.67R) "
    #  "· time stop 20 แท่ง (0R) · Chandelier → ราคาเปิดถัดไป",
    #  lambda: (ra["why"] == "STOP" and abs(ra["R_gross"] + 5 / 3) < 1e-6 and rb["why"] == "STOP" and abs(rb["R_gross"] + 1) < 1e-6
    #           and rc["why"] == "BE" and abs(rc["R_gross"] - 0.6666) < 1e-3 and rd["why"] == "TIME" and rd["x"] == 271
    #           and abs(rd["R_gross"]) < 1e-9 and re_["why"] == "TRAIL" and abs(re_["R_gross"] - 2.8889) < 1e-3),
    #           f"{ra['why']} {ra['R_gross']:.3f} · {rb['why']} {rb['R_gross']:.2f} · {rc['why']} {rc['R_gross']:.3f} · "
    #           f"{rd['why']} x{rd['x']} · {re_['why']} {re_['R_gross']:.3f}")


    T("จำลองไม้: gap ใต้ stop = ราคาเปิด (−1.67R) · แท่งเดียวกับเป้า = stop ก่อน (−1R) · +2R ขาย 1/3 แล้ว BE (+0.67R) "
      "· time stop 20 แท่ง (0R) · Chandelier → ราคาเปิดถัดไป",
      lambda: (ra["why"] == "STOP" and abs(ra["R_gross"] + 5 / 3) < 1e-6 and rb["why"] == "STOP" and abs(rb["R_gross"] + 1) < 1e-6
               and rc["why"] == "BE" and abs(rc["R_gross"] - 0.6666) < 1e-3 and rd["why"] == "TIME" and rd["x"] == 271
               and abs(rd["R_gross"]) < 1e-9 and re_["why"] == "TRAIL" and abs(re_['R_gross'] - 2.8889) < 1e-3,
               f"{ra['why']} {ra['R_gross']:.3f} · {rb['why']} {rb['R_gross']:.2f} · {rc['why']} {rc['R_gross']:.3f} · "
               f"{rd['why']} x{rd['x']} · {re_['why']} {re_['R_gross']:.3f}"))






    rl = sim(Ff, 0, 250, PZ, 90.0, False, lag=1)[0]
    T("Lag +1: สัญญาณแท่ง 250 → เข้าแท่ง 252", lambda: (rl["e"] == 252, f"เข้าแท่ง {rl['e']}"))
    fs, ixs = _synth(30, 500, 9, 0.01)
    Fa = prepare(fs, ixs, P1)
    Ma = masks(Fa, P1)[0]
    Fb = prepare({s: d.iloc[:400] for s, d in fs.items()}, ixs.iloc[:400], P1)
    Mb = masks(Fb, P1)[0]
    same = all(np.array_equal(Ma[k][399], Mb[k][399]) for k in ("S0", "A1", "A3", "B4", "AB"))
    rk = np.allclose(Fa.rsrank[399], Fb.rsrank[399], equal_nan=True) and np.allclose(Fa.comp[399], Fb.comp[399], equal_nan=True)
    T("ไม่มี look-ahead: ตัดข้อมูลที่แท่ง 400 → สัญญาณ/RS rank/compression ของแท่ง 399 เหมือนข้อมูลเต็ม",
      lambda: (same and rk, f"สัญญาณ {same} · rank/comp {rk}"))
    PP = _P(min_bars=260, max_cost_r=2.0, stop_max_atr=10.0, stop_max_pct=30.0, min_adv_mb=5.0)
    outs = {}
    for lab_, dr in (("planted", 0.01), ("null", 0.0)):
        fz, iz = _synth(40, 700, 5, dr)
        Fz = prepare(fz, iz, PP)
        Mz, lz, _, _ = masks(Fz, PP)
        tz, _ = run_arm(Fz, Mz["S0"], lz["S0"], PP, False)
        sz = tstats(tz, PP)
        nz = placebo(Fz, tz, PP, False, 30)
        outs[lab_] = (sz, nz)
    sp, npl = outs["planted"]
    sn, _ = outs["null"]
    T("Planted signal: breakout + drift → R สุทธิ > 0.3 และชนะ random วันเดียวกัน ≥ P95 · ไม่มี drift → ต่ำกว่าอย่างชัด",
      lambda: (sp["mean"] > 0.3 and npl.get("pct", 0) >= 0.95 and sn["mean"] < sp["mean"] - 0.3,
               f"planted {sp['mean']:+.2f}R ({sp['n']} ไม้) · percentile {npl.get('pct')} · null {sn['mean']:+.2f}R"))
    rr = np.random.default_rng(2).normal(0.0008, 0.01, 1000)
    d1, d2 = dsr(rr, 1), dsr(rr, 200)
    T("Deflated Sharpe: trial มากขึ้น → ความน่าจะเป็นลดลง", lambda: (d1 > d2, f"{d1:.3f} → {d2:.3f}"))
    wl, wh = wilson(0, 20)
    T("Wilson: 0/20 → ขอบบน ≈ 16%", lambda: (wl == 0 and 0.15 < wh < 0.17, f"{wl:.3f}–{wh:.3f}"))
    with tempfile.TemporaryDirectory() as td:
        pth = Path(td) / "s.jsonl"
        sc = {"asof": str(Ff.dates[250].date()), "regime": 3, "m": 0.5,
              "entries": [{"symbol": "TST", "setup": "A", "ref": 100.0, "stop": 97.0, "lvl": 90.0, "skip": None}]}
        n1, n2 = fwd_log(sc, PZ, pth), fwd_log(sc, PZ, pth)
        fv = fwd_eval(Ff, PZ, pth)
        T("Forward paper: บันทึกครั้งเดียว (append-only) · วัดผลด้วยกฎเดียวกับ backtest",
          lambda: (n1 == 1 and n2 == 0 and fv["closed"] == 1 and abs(fv["recent"][0]["R_net"]) < 1e-9
                   and fv["recent"][0]["why"] == "TIME", f"{n1} · {n2} · {fv['recent'][0] if fv['recent'] else fv}"))
    bad = frf["TST"].copy()
    bad.iloc[::8, bad.columns.get_loc("high")] = 98.0
    Fq = prepare({"TST": frf["TST"], "BAD": bad}, ixf, PZ, 1)
    T("Validator: หุ้นที่ OHLC ผิดรูป > 5% ของแท่ง → กักกัน", lambda: (Fq.quarantined == ["BAD"] and Fq.syms == ["TST"],
                                                                str(Fq.quarantined)))
    pol = Policy(Path(os.devnull))
    h0 = prereg_hash(pol)
    pol._apply({"vol_mult_b": 99})
    h1 = prereg_hash(pol)
    pol._apply({"null_reps": 500})
    T("Policy: ค่าเกินช่วงถูกปรับ · เปลี่ยนกฎ = prereg ใหม่ (trial ใหม่) · จำนวนรอบ random ไม่นับเป็น trial",
      lambda: (pol.data["vol_mult_b"] == 5.0 and h0 != h1 and prereg_hash(pol) == h1, f"{h0} → {h1}"))
    n_ok = sum(1 for r in res if r["ok"])
    return {"ok": n_ok == len(res), "passed": n_ok, "total": len(res), "tests": res, "version": __version__}


# ═══════════════ ROUTES · INSTALL ═══════════════
def routes(svc, safe, ok_code):
    from fastapi import Body, Query

    @safe
    def e_main():
        rep = _jload(RES_FILE)
        if (not rep or rep.get("computed_day") != date.today().isoformat()) and _RUN["state"] != "running":
            start(False)
        return {"available": True, "version": __version__, "run": status(), "policy": POLICY.view(), "report": rep or None}

    @safe
    def e_run(payload: Optional[Dict[str, Any]] = Body(None)):
        ok = start(bool((payload or {}).get("force")))
        return ok_code({"ok": True, "started": ok, "run": status()})

    @safe
    def e_trades(arm: str = Query("AB"), limit: int = Query(500)):
        return {"arm": arm, "rows": read_trades(arm, limit)}

    @safe
    def e_signals():
        return {"rows": read_lines(SIG_LOG)[-300:]}

    @safe
    def e_cfg_get():
        return POLICY.view()

    @safe
    def e_cfg_set(payload: Optional[Dict[str, Any]] = Body(None)):
        return ok_code(POLICY.update(payload or {}))

    @safe
    def e_self():
        return selftest()

    return [("GET", "/api/etrend/selftest", e_self), ("GET", "/api/etrend/config", e_cfg_get),
            ("POST", "/api/etrend/config", e_cfg_set), ("POST", "/api/etrend/run", e_run),
            ("GET", "/api/etrend/trades", e_trades), ("GET", "/api/etrend/signals", e_signals),
            ("GET", "/api/etrend", e_main)]


def install(svc) -> Dict[str, Any]:
    """ลงทะเบียนเท่านั้น — ไม่ครอบ hub.build_core · ไม่แตะโมดูลเดิม"""
    _SVC["svc"] = svc
    errs = []
    try:
        syms, src = universe(svc)
    except Exception as e:
        syms, src = [], f"error {e}"
    st = selftest()
    if not st["ok"]:
        errs.append(f"selftest ผ่าน {st['passed']}/{st['total']} — ดู /api/etrend/selftest")
    rep = {"ok": not errs, "version": __version__, "isolated": True, "universe": f"{len(syms)} หุ้น ({src})",
           "selftest": f"{st['passed']}/{st['total']}", "prereg_hash": prereg_hash(POLICY), "errors": errs}
    log.info("etrend.install: %s", rep)
    return rep


POLICY = Policy()


def _cli():
    ap = argparse.ArgumentParser(prog="python3 -m sq.etrend")
    ap.add_argument("cmd", choices=("selftest", "backtest", "scan"))
    ap.add_argument("--force", action="store_true", help="ดึงข้อมูลใหม่ (ไม่ใช้ cache วันนี้)")
    ap.add_argument("--reps", type=int, help="จำนวนรอบ random entry (ไม่นับเป็น trial)")
    ap.add_argument("--symbols", help="รายชื่อหุ้นคั่นด้วย , (แทน universe)")
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if a.symbols:
        _CLI_SYMS.extend(sorted({norm(s) for s in a.symbols.split(",") if s.strip()}))
    if a.cmd == "selftest":
        r = selftest()
        for t in r["tests"]:
            print(("✓ " if t["ok"] else "✕ ") + t["name"] + " — " + t["detail"])
        print(f"\nผ่าน {r['passed']} / {r['total']}")
        return
    rep = run(a.force, a.reps, say=lambda s: print("…", s, flush=True))
    print("\n" + text_report(rep, a.cmd == "scan"))
    print(f"\nผล: {RES_FILE} · ไม้รายตัว: {TRADES_CSV} · forward: {SIG_LOG} · trial: {TRIALS}")


if __name__ == "__main__":
    _cli()
# ═══ END sq/etrend.py v1.2 ═══