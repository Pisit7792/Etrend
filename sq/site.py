"""
sq/site.py  [v1.0]
Trade Desk สำหรับมือถือ / iPad — เว็บ static + ฟังก์ชันราคาระหว่างวัน · ใช้ได้แม้ไม่ได้เปิดแอป และแม้ตลาดปิด
  python3 -m sq.site build [--out site]                     สร้าง site/ + functions/api/*.js (Cloudflare Pages Functions)
  python3 -m sq.site serve [--host 0.0.0.0] [--port 8810]   เปิดบน Mac (iPad/มือถือใน Wi-Fi เดียวกันเข้าได้)
  python3 -m sq.site selftest
ระบบแยก อ่านอย่างเดียว · แผน/ระดับราคามาจาก sq.chart (กฎเดียวกับ backtest) · HTML อยู่ในไฟล์นี้ (ไม่ต้องมี chart.html)
ราคาระหว่างวัน: Yahoo v8 chart (ฝั่งเซิร์ฟเวอร์ · ล่าช้า ~15 นาที) — ใช้ส่วนตัวเท่านั้น
"""
import argparse
import functools
import http.server
import json
import logging
import re
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from sq import chart as CH
from sq import etrend as ET

__version__ = "1.0"
log = logging.getLogger("sq.site")
ALWAYS = ["GC=F", "GLD", "TDEX"]
RANGE = {"5m": "5d", "15m": "1mo", "60m": "6mo", "1d": "2y"}
UA = {"User-Agent": "Mozilla/5.0 (Macintosh) SQDesk/1.0", "Accept": "application/json"}
_C: Dict[str, Any] = {}
_L = threading.RLock()


def fname(sym) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", str(sym).upper()) + ".json"


def parse(j) -> Dict[str, Any]:
    res = (((j or {}).get("chart") or {}).get("result") or [None])[0]
    if not res:
        raise ValueError("ไม่มีข้อมูล")
    q = (((res.get("indicators") or {}).get("quote")) or [{}])[0] or {}
    ts = res.get("timestamp") or []
    col = {k: q.get(k) or [] for k in ("open", "high", "low", "close", "volume")}
    bars = []
    for i, t in enumerate(ts):
        row = [col[k][i] if i < len(col[k]) else None for k in ("open", "high", "low", "close")]
        if any(v is None for v in row):
            continue
        v = col["volume"][i] if i < len(col["volume"]) else 0
        bars.append([int(t)] + [float(x) for x in row] + [float(v or 0)])
    return {"meta": res.get("meta") or {}, "bars": bars}


def yahoo(t, iv, rg) -> Dict[str, Any]:
    key = f"{t}|{iv}|{rg}"
    with _L:
        hit = _C.get(key)
        if hit and time.time() - hit[0] < 60:
            return hit[1]
    u = ("https://query1.finance.yahoo.com/v8/finance/chart/" + urllib.parse.quote(t)
         + f"?interval={iv}&range={rg}&includePrePost=false")
    with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=15) as r:
        d = parse(json.loads(r.read().decode("utf-8")))
    with _L:
        _C[key] = (time.time(), d)
    return d


def dstr(t, off) -> str:
    return datetime.fromtimestamp(int(t) + int(off), tz=timezone.utc).strftime("%Y-%m-%d")


def summary(d) -> Optional[Dict[str, Any]]:
    b = d["bars"]
    if not b:
        return None
    off = int(d["meta"].get("gmtoffset") or 25200)
    day = dstr(b[-1][0], off)
    bb = [x for x in b if dstr(x[0], off) == day]
    return {"last": bb[-1][4], "open": bb[0][1], "high": max(x[2] for x in bb), "low": min(x[3] for x in bb),
            "vol": sum(x[5] for x in bb), "time": b[-1][0], "date": day, "prev_close": d["meta"].get("chartPreviousClose")}


def api_bars(symbol, interval) -> Dict[str, Any]:
    if interval not in RANGE:
        raise ValueError("interval ไม่ถูกต้อง")
    t = CH.ticker(symbol)
    d = yahoo(t, interval, RANGE[interval])
    off = int(d["meta"].get("gmtoffset") or 25200)
    bars = [[dstr(b[0], off)] + b[1:] for b in d["bars"]] if interval == "1d" else d["bars"]
    return {"symbol": str(symbol).strip().upper(), "ticker": t, "interval": interval, "precision": 1 if "=" in t else 2,
            "note": f"{interval} · Yahoo ล่าช้า ~15 นาที", "bars": bars}


def api_quote(symbols) -> Dict[str, Any]:
    syms = list(dict.fromkeys(s.strip().upper() for s in symbols if s and s.strip()))[:40]
    out = {}

    def one(s):
        try:
            return s, summary(yahoo(CH.ticker(s), "5m", "1d"))
        except Exception:
            return s, None
    if syms:
        with ThreadPoolExecutor(8) as ex:
            for s, q in ex.map(one, syms):
                if q:
                    out[s] = q
    return {"quotes": out, "at": ET._now()}


# ═══════════════ CLOUDFLARE PAGES FUNCTIONS (สูตรเดียวกับข้างบน) ═══════════════
JS_LIB = r'''const UA = {"User-Agent": "Mozilla/5.0 (Macintosh) SQDesk/1.0", "Accept": "application/json"};
const RANGE = {"5m": "5d", "15m": "1mo", "60m": "6mo", "1d": "2y"};
function tk(s) {
  s = String(s || "").trim().toUpperCase();
  if (s.startsWith("^") || s.includes("=")) return s;
  if (s.endsWith(".BK")) s = s.slice(0, -3);
  if (!s || s.includes(".") || s.includes("-") || ["GLD", "IAU", "SPY"].includes(s)) return s;
  return s + ".BK";
}
async function yahoo(t, iv, rg) {
  const u = "https://query1.finance.yahoo.com/v8/finance/chart/" + encodeURIComponent(t) + "?interval=" + iv + "&range=" + rg + "&includePrePost=false";
  const r = await fetch(u, {headers: UA, cf: {cacheTtl: 60, cacheEverything: true}});
  if (!r.ok) throw new Error("Yahoo HTTP " + r.status);
  const j = await r.json();
  const res = j && j.chart && j.chart.result && j.chart.result[0];
  if (!res) throw new Error("ไม่มีข้อมูล " + t);
  const q = ((res.indicators || {}).quote || [{}])[0] || {}, ts = res.timestamp || [], bars = [];
  for (let i = 0; i < ts.length; i++) {
    const o = (q.open || [])[i], h = (q.high || [])[i], l = (q.low || [])[i], c = (q.close || [])[i];
    if (o == null || h == null || l == null || c == null) continue;
    bars.push([ts[i], o, h, l, c, (q.volume || [])[i] || 0]);
  }
  return {meta: res.meta || {}, bars: bars};
}
function dstr(t, off) { return new Date((t + off) * 1000).toISOString().slice(0, 10); }
function summary(d) {
  const b = d.bars;
  if (!b.length) return null;
  const off = d.meta.gmtoffset || 25200, day = dstr(b[b.length - 1][0], off);
  const bb = b.filter(x => dstr(x[0], off) === day);
  let hi = -Infinity, lo = Infinity, v = 0;
  bb.forEach(x => { hi = Math.max(hi, x[2]); lo = Math.min(lo, x[3]); v += x[5] || 0; });
  return {last: bb[bb.length - 1][4], open: bb[0][1], high: hi, low: lo, vol: v, time: b[b.length - 1][0], date: day,
          prev_close: d.meta.chartPreviousClose != null ? d.meta.chartPreviousClose : null};
}
function J(o, st) {
  return new Response(JSON.stringify(o), {status: st || 200, headers: {"content-type": "application/json; charset=utf-8", "cache-control": "no-store"}});
}
'''
JS_BARS = r'''export async function onRequestGet({request}) {
  const u = new URL(request.url), sym = u.searchParams.get("symbol") || "", iv = u.searchParams.get("interval") || "15m";
  if (!RANGE[iv]) return J({error: "interval ไม่ถูกต้อง"}, 502);
  try {
    const t = tk(sym), d = await yahoo(t, iv, RANGE[iv]), off = d.meta.gmtoffset || 25200;
    const bars = iv === "1d" ? d.bars.map(b => [dstr(b[0], off)].concat(b.slice(1))) : d.bars;
    return J({symbol: sym.toUpperCase(), ticker: t, interval: iv, precision: t.includes("=") ? 1 : 2,
              note: iv + " · Yahoo ล่าช้า ~15 นาที", bars: bars});
  } catch (e) { return J({error: String(e.message || e)}, 502); }
}
'''
JS_QUOTE = r'''export async function onRequestGet({request}) {
  const u = new URL(request.url);
  const syms = [...new Set((u.searchParams.get("symbols") || "").split(",").map(s => s.trim().toUpperCase()).filter(Boolean))].slice(0, 40);
  const out = {};
  await Promise.all(syms.map(async s => {
    try { const q = summary(await yahoo(tk(s), "5m", "1d")); if (q) out[s] = q; } catch (e) { /* ข้ามตัวที่ดึงไม่ได้ */ }
  }));
  return J({quotes: out, at: new Date().toISOString()});
}
'''
MANIFEST = json.dumps({"name": "SQ Trade Desk", "short_name": "SQ Desk", "start_url": ".", "display": "standalone",
                       "background_color": "#0b0f14", "theme_color": "#0b0f14"}, ensure_ascii=False)

HTML = r"""<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#0b0f14">
<link rel="manifest" href="manifest.webmanifest">
<title>SQ Trade Desk</title>
<script src="https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
:root{--bg:#0b0f14;--pn:#0f1620;--bd:#1d2733;--tx:#d7e1ec;--mu:#7f8da0;--pos:#22c98a;--neg:#ff5c6c;--warn:#ffd166;--acc:#5aa9ff}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--tx);font:13px/1.45 -apple-system,"Segoe UI",Sarabun,sans-serif;padding-bottom:env(safe-area-inset-bottom)}
header{display:flex;gap:6px;align-items:center;flex-wrap:wrap;padding:8px 10px;padding-top:max(8px,env(safe-area-inset-top));border-bottom:1px solid var(--bd);background:var(--pn)}
h1{font-size:15px;margin:0 4px 0 0}
.chip{display:inline-block;border:1px solid var(--bd);border-radius:5px;padding:1px 7px;font-size:11px;font-weight:700;white-space:nowrap}
.c-pos{background:#0d2a1e;border-color:#1d5c42;color:#8fe8c0}.c-neg{background:#2a1117;border-color:#5a2733;color:#ffb0bc}
.c-warn{background:#2c2008;border-color:#6b5010;color:#ffd699}.c-info{background:#0d1d31;border-color:#1d4468;color:#a8d3ff}
.pos{color:var(--pos)}.neg{color:var(--neg)}.mu{color:var(--mu)}
#banner{padding:6px 10px;font-size:12px;border-bottom:1px solid var(--bd);line-height:1.8}
main{display:grid;grid-template-columns:270px 1fr 340px;height:calc(100vh - 92px)}
aside{overflow:auto}#list{border-right:1px solid var(--bd)}#detail{border-left:1px solid var(--bd);padding:8px 10px}
.grp{font-size:10.5px;color:var(--warn);font-weight:700;padding:9px 10px 4px}
.it{padding:7px 10px;border-bottom:1px solid #141c26;cursor:pointer;display:flex;justify-content:space-between;gap:6px}
.it:hover{background:#121b26}.it.on{background:#13253a}
.it b{font-size:13px}.it small{display:block;color:var(--mu);font-size:10.5px}
#center{display:flex;flex-direction:column;min-width:0}
.bar{display:flex;gap:6px;align-items:center;padding:6px 10px;border-bottom:1px solid var(--bd);flex-wrap:wrap}
button,input,a.btn{background:#13202d;border:1px solid var(--bd);color:var(--tx);border-radius:6px;padding:5px 9px;font-size:12px;text-decoration:none}
button{cursor:pointer}button.on{border-color:var(--acc);color:var(--acc)}button:disabled{opacity:.35;cursor:default}
#chart{flex:1;min-height:300px}
table{width:100%;border-collapse:collapse;font-size:12px}td{padding:4px;border-bottom:1px solid #141c26;vertical-align:top}
td.n{text-align:right;font-family:monospace;white-space:nowrap}
.sw{display:inline-block;width:12px;height:3px;margin-right:6px;vertical-align:middle}
.card{border:1px solid var(--bd);border-radius:8px;padding:8px;margin-bottom:8px;background:var(--pn)}
.card h3{margin:0 0 6px;font-size:12.5px}
ul{margin:6px 0 0;padding-left:16px;color:#b9c6d4;font-size:11.5px}li{margin:2px 0}
@media(max-width:900px){main{display:block;height:auto}#list{max-height:34vh;border-right:0;border-bottom:1px solid var(--bd)}
#chart{height:52vh;flex:none}#detail{border-left:0}header input{width:110px}}
</style>
</head>
<body>
<header>
  <h1>📈 SQ Trade Desk</h1><span id="src"></span><span class="mu" id="clock"></span>
  <span style="margin-left:auto"></span>
  <input id="q" placeholder="PTT / GC=F" autocapitalize="characters"><button id="go">ดู</button><button id="rf">↻</button>
</header>
<div id="banner"></div>
<main>
  <aside id="list"></aside>
  <section id="center">
    <div class="bar"><b id="sym">—</b><span id="px" class="mu"></span><span style="margin-left:auto"></span>
      <span id="ivs"></span><a class="btn" id="tv" target="_blank" rel="noopener">TradingView ↗</a></div>
    <div id="chart"></div>
  </section>
  <aside id="detail"></aside>
</main>
<script>
(function () {
  "use strict";
  var LC = window.LightweightCharts, BKK = 7 * 3600;
  var IVS = [["5m", "5น."], ["15m", "15น."], ["60m", "1ชม."], ["1d", "วัน"]];
  var COL = {stop: "#ff5c6c", ceiling: "#ff9f43", target: "#22c98a", level: "#5aa9ff", ref: "#8a97a8", entry: "#d7e1ec",
             trail: "#b48cff", alert: "#ffd166"};
  var GROUPS = [["ET_ENTRY", "🌱 Early Trend · ซื้อที่ราคาเปิด"], ["ET_OPEN", "🌱 Early Trend · ถืออยู่ (paper)"],
                ["ET_WATCH", "🌱 Early Trend · เฝ้าระดับ (ตัดสินที่ราคาปิด)"], ["MID_TRADE", "📅 Mid-Long · rebalance"],
                ["MID_HOLD", "📅 Mid-Long · พอร์ตโมเดล"], ["GOLD", "🥇 ทองคำ"]];
  var S = {plan: null, api: false, sym: null, iv: "1d", data: null, quotes: {}, lines: [], extra: []};
  var chart, cs, vs;

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c];
    });
  }
  function f(v, d) {
    if (v == null || v === "" || isNaN(+v)) return "—";
    d = d == null ? 2 : d;
    return (+v).toLocaleString("en-US", {minimumFractionDigits: d, maximumFractionDigits: d});
  }
  function J(url) {
    return fetch(url, {cache: "no-store"}).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!r.ok) throw new Error(j.error || ("HTTP " + r.status));
        return j;
      });
    });
  }
  function fname(s) { return String(s).toUpperCase().replace(/[^A-Z0-9]/g, "_") + ".json"; }
  function bkk() { return new Date(Date.now() + BKK * 1000); }
  function marketOpen() {
    var d = bkk(), w = d.getUTCDay(), m = d.getUTCHours() * 60 + d.getUTCMinutes();
    return w >= 1 && w <= 5 && ((m >= 600 && m <= 750) || (m >= 870 && m <= 1000));
  }
  function isGold(s) { return /=F$|^GLD$|^IAU$/.test(s || ""); }
  function tvSym(s) {
    if (s === "GC=F") return "COMEX:GC1!";
    if (/^(GLD|IAU|SPY)$/.test(s)) return "AMEX:" + s;
    if (/^\^|=/.test(s)) return null;
    return "SET:" + s;
  }
  function itemsOf(sym) { return S.plan ? (S.plan.items || []).filter(function (i) { return i.symbol === sym; }) : []; }
  function qToday(sym) {
    var q = S.quotes[sym];
    return q && S.plan && q.date === S.plan.trade_date ? q : null;
  }

  /* กฎ L4 ของ Early Trend (เหมือน sq/chart.py entry_calc) */
  function entryCalc(c, op) {
    var a = +c.atr, st = +c.st_hl;
    if (op - st < c.stop_min_atr * a) st = op - c.stop_min_atr * a;
    var risk = op - st, why = null;
    if (st <= 0 || risk <= 0) why = "stop ไม่ถูกต้อง";
    else if (risk > c.stop_max_atr * a + 1e-12) why = "เสี่ยง > " + c.stop_max_atr + " ATR";
    else if (risk / op * 100 > c.stop_max_pct + 1e-12) why = "เสี่ยง > " + c.stop_max_pct + "%";
    else if (c.rt_pct != null && c.rt_pct / 100 * op / risk > c.max_cost_r) why = "ต้นทุน > " + c.max_cost_r + "R";
    return {entry: op, stop: st, risk_pct: risk > 0 ? risk / op * 100 : null, t2r: op + c.be_r * risk, skip: !!why, why: why};
  }
  function todayOpen(sym) {
    var q = qToday(sym);
    if (q) return q.open;
    var D = S.data, td = S.plan && S.plan.trade_date;
    if (!D || sym !== S.sym) return null;
    for (var k = 0; k < D.bars.length; k++) {
      var b = D.bars[k], ds = D.intr ? new Date(b.time * 1000).toISOString().slice(0, 10) : b.time;
      if (ds === td) return b.open;
    }
    return null;
  }
  function status(i) {
    var q = qToday(i.symbol), c = i.calc || {};
    if (i.group === "ET_ENTRY") {
      var op = todayOpen(i.symbol);
      if (!op) return ["รอราคาเปิด", "c-info"];
      var r = entryCalc(c, op);
      return r.skip ? ["ข้าม: " + r.why, "c-neg"] : ["เข้าได้ @" + f(op), "c-pos"];
    }
    if (i.group === "ET_OPEN") {
      if (c.pending) return ["ขายที่ราคาเปิด", "c-warn"];
      if (q && q.low <= c.stop_now) return ["แตะ stop", "c-neg"];
      if (q && !c.part && q.high >= c.t2r) return ["+2R ขาย 1/3", "c-pos"];
      return ["ถือ", "c-info"];
    }
    if (i.group === "ET_WATCH") {
      if (!q) return null;
      var pace = c.vol_thr ? q.vol / c.vol_thr * 100 : null;
      if (q.last >= c.level) return ["ผ่านระดับ · รอปิด · vol " + f(pace, 0) + "%", "c-warn"];
      return ["ห่าง " + f((c.level - q.last) / c.atr, 1) + " ATR", "c-info"];
    }
    if (i.group === "MID_TRADE") {
      var act = (i.info || {}).action || "";
      return [act, act === "ขาย" || act === "ลด" ? "c-neg" : "c-pos"];
    }
    if (i.group === "MID_HOLD") {
      var p = S.quotes[i.symbol];
      return p && c.sma200 && p.last < c.sma200 ? ["ต่ำกว่า SMA200", "c-warn"] : null;
    }
    return i.status ? [i.status, i.tone || "c-info"] : null;
  }
  function dyn(i) {
    var L = (i.levels || []).slice();
    if (i.group === "ET_ENTRY") {
      var op = todayOpen(i.symbol);
      if (op) {
        var r = entryCalc(i.calc || {}, op);
        L = L.filter(function (l) { return l.kind === "level" || l.kind === "ceiling"; });
        L.push({price: op, label: "ราคาเปิด (เข้า)", kind: "entry"});
        if (!r.skip) {
          L.push({price: r.stop, label: "stop", kind: "stop"});
          L.push({price: r.t2r, label: "+2R ขาย 1/3", kind: "target"});
        }
      }
    }
    return L;
  }
  function sma(bars, n) {
    var out = [], sum = 0;
    for (var i = 0; i < bars.length; i++) {
      sum += bars[i].close;
      if (i >= n) sum -= bars[i - n].close;
      if (i >= n - 1) out.push({time: bars[i].time, value: sum / n});
    }
    return out;
  }

  /* ═══ CHART ═══ */
  function initChart() {
    chart = LC.createChart($("chart"), {
      autoSize: true, layout: {background: {type: "solid", color: "#0b0f14"}, textColor: "#c9d4e0"},
      grid: {vertLines: {color: "#121a24"}, horzLines: {color: "#121a24"}}, rightPriceScale: {borderColor: "#1d2733"},
      timeScale: {borderColor: "#1d2733", timeVisible: true, secondsVisible: false, rightOffset: 6},
      crosshair: {mode: LC.CrosshairMode.Normal}});
    cs = chart.addCandlestickSeries({upColor: "#22c98a", downColor: "#ff5c6c", borderVisible: false,
                                     wickUpColor: "#22c98a", wickDownColor: "#ff5c6c"});
    cs.priceScale().applyOptions({scaleMargins: {top: 0.06, bottom: 0.24}});
    vs = chart.addHistogramSeries({priceFormat: {type: "volume"}, priceScaleId: "vol", lastValueVisible: false, priceLineVisible: false});
    chart.priceScale("vol").applyOptions({scaleMargins: {top: 0.8, bottom: 0}});
    S.lines = []; S.extra = [];
  }
  function clearOv() {
    S.lines.forEach(function (l) { try { cs.removePriceLine(l); } catch (e) { /* ignore */ } });
    S.extra.forEach(function (s) { try { chart.removeSeries(s); } catch (e) { /* ignore */ } });
    S.lines = []; S.extra = [];
    cs.setMarkers([]);
  }
  function line(p, label, kind) {
    if (p == null || isNaN(+p)) return;
    S.lines.push(cs.createPriceLine({price: +p, color: COL[kind] || "#8a97a8", lineWidth: kind === "stop" ? 2 : 1,
      lineStyle: kind === "stop" || kind === "entry" ? LC.LineStyle.Solid : LC.LineStyle.Dashed,
      axisLabelVisible: true, title: label}));
  }
  function overlays() {
    var D = S.data;
    if (!D || !cs) return;
    clearOv();
    if (!D.intr) {
      [[50, "#5aa9ff"], [200, "#ffd166"]].forEach(function (x) {
        var arr = sma(D.bars, x[0]);
        if (!arr.length) return;
        var s = chart.addLineSeries({color: x[1], lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false});
        s.setData(arr);
        S.extra.push(s);
      });
    }
    var times = {}, mk = [];
    D.bars.forEach(function (b) { times[b.time] = 1; });
    itemsOf(S.sym).forEach(function (i) {
      dyn(i).forEach(function (l) { line(l.price, l.label, l.kind); });
      if (!D.intr) (i.markers || []).forEach(function (m) {
        if (times[m.time]) mk.push({time: m.time, position: m.pos || "belowBar", color: COL[m.kind] || "#5aa9ff",
                                    shape: m.shape || "arrowUp", text: m.text});
      });
    });
    mk.sort(function (a, b) { return a.time < b.time ? -1 : a.time > b.time ? 1 : 0; });
    cs.setMarkers(mk);
  }
  function loadBars() {
    var sym = S.sym, iv = S.iv;
    if (!sym) return Promise.resolve();
    var api = "api/bars?symbol=" + encodeURIComponent(sym) + "&interval=" + iv;
    var p = iv === "1d"
      ? J("data/daily/" + fname(sym) + "?v=" + encodeURIComponent((S.plan || {}).built_at || "")).catch(function () {
          if (!S.api) throw new Error("ไม่มีกราฟรายวันของตัวนี้ในเว็บ static");
          return J(api);
        })
      : J(api);
    return p.then(function (d) {
      if (sym !== S.sym || iv !== S.iv) return;
      var intr = iv !== "1d", seen = {}, bars = [];
      (d.bars || []).forEach(function (b) {
        if (b[1] == null || b[2] == null || b[3] == null || b[4] == null) return;
        var t = intr ? b[0] + BKK : b[0];
        var o = {time: t, open: b[1], high: b[2], low: b[3], close: b[4], volume: b[5] || 0};
        if (seen[t] != null) bars[seen[t]] = o; else { seen[t] = bars.length; bars.push(o); }
      });
      if (!bars.length) throw new Error(d.error || "ไม่มีข้อมูล");
      if (S.data && S.data.intr !== intr) { chart.remove(); initChart(); } else clearOv();
      var pr = d.precision || 2;
      cs.applyOptions({priceFormat: {type: "price", precision: pr, minMove: Math.pow(10, -pr)}});
      cs.setData(bars.map(function (b) { return {time: b.time, open: b.open, high: b.high, low: b.low, close: b.close}; }));
      vs.setData(bars.map(function (b) {
        return {time: b.time, value: b.volume, color: b.close >= b.open ? "rgba(34,201,138,.35)" : "rgba(255,92,108,.35)"};
      }));
      S.data = {bars: bars, intr: intr};
      var last = bars[bars.length - 1];
      $("px").innerHTML = "<b>" + f(last.close, pr) + '</b> <span class="mu">' + esc(d.ticker || sym) + " · " + esc(d.note || "รายวัน (ราคาปรับปันผล)") + "</span>";
      overlays();
      detail();
    }).catch(function (e) {
      if (sym !== S.sym) return;
      $("px").innerHTML = '<span class="neg">โหลดกราฟไม่ได้: ' + esc(e.message) + "</span> — ลองปุ่ม TradingView";
    });
  }

  /* ═══ UI ═══ */
  function head() {
    var p = S.plan || {}, src = p.sources || {}, h = [];
    Object.keys(src).forEach(function (k) {
      var s = src[k], tone = !s.ok ? "c-neg" : s.state === "READY" ? "c-pos" : s.state === "BACKTEST_PASS" ? "c-warn" : "c-neg";
      h.push('<span class="chip ' + tone + '">' + esc(s.name || k) + ": " + esc(s.ok ? (s.state_label || s.state) : "ไม่มีผล")
             + (s.stale ? " · ค้าง" : "") + "</span>");
    });
    $("src").innerHTML = h.join(" ");
    var b = [];
    if (p.paper_only) b.push('<span class="chip c-neg">PAPER เท่านั้น</span> ยังไม่มีกลยุทธ์ใดผ่านด่าน edge + forward paper');
    if (p.trade_date && p.trade_date < bkk().toISOString().slice(0, 10)) b.push('<span class="chip c-warn">แผนเก่า</span> แผนของ ' + esc(p.trade_date));
    (p.errors || []).forEach(function (e) { b.push('<span class="chip c-neg">error</span> ' + esc(e)); });
    b.push('<span class="mu">แผนวันทำการ ' + esc(p.trade_date || "—") + " · สร้าง " + esc(String(p.built_at || "—").replace("T", " ").slice(0, 16))
           + " · " + (S.api ? "ราคาระหว่างวัน: ใช้ได้ (ล่าช้า ~15 นาที)" : "เว็บ static: กราฟรายวันเท่านั้น") + "</span>");
    $("banner").innerHTML = b.join(" · ");
  }
  function list() {
    var p = S.plan || {items: []}, h = [];
    GROUPS.forEach(function (g) {
      var its = (p.items || []).filter(function (i) { return i.group === g[0]; });
      if (!its.length) return;
      h.push('<div class="grp">' + esc(g[1]) + " (" + its.length + ")</div>");
      its.forEach(function (i) {
        var st = status(i), q = S.quotes[i.symbol];
        h.push('<div class="it' + (i.symbol === S.sym ? " on" : "") + '" data-s="' + esc(i.symbol) + '"><div><b>' + esc(i.symbol)
               + "</b><small>" + esc(i.title) + '</small></div><div style="text-align:right">'
               + (st ? '<span class="chip ' + st[1] + '">' + esc(st[0]) + "</span>" : "") + "<small>" + (q ? f(q.last) : "") + "</small></div></div>");
      });
    });
    if (!h.length) h.push('<div class="grp">ยังไม่มีแผน — รัน python3 daily_run.py --post</div>');
    $("list").innerHTML = h.join("");
  }
  function detail() {
    if (!S.sym) { $("detail").innerHTML = ""; return; }
    var its = itemsOf(S.sym), h = [];
    if (!its.length) h.push('<div class="card"><h3>' + esc(S.sym) + '</h3><div class="mu">ไม่อยู่ในแผน — แสดงกราฟเท่านั้น</div></div>');
    its.forEach(function (i) {
      var st = status(i), c = i.calc || {}, q = qToday(i.symbol);
      h.push('<div class="card"><h3>' + esc(i.title) + " " + (st ? '<span class="chip ' + st[1] + '">' + esc(st[0]) + "</span>" : "") + "</h3>");
      if (i.group === "ET_ENTRY") {
        var op = todayOpen(i.symbol);
        if (op) {
          var r = entryCalc(c, op);
          h.push("<div>" + (r.skip ? '<b class="neg">ข้ามไม้ — ' + esc(r.why) + "</b>"
                 : "ราคาเปิด " + f(op) + " → stop " + f(r.stop) + " · เสี่ยง " + f(r.risk_pct) + "% · +2R " + f(r.t2r)) + "</div>");
        }
      }
      if (i.group === "ET_WATCH" && q && c.vol_thr) {
        h.push("<div>volume วันนี้ " + f(q.vol, 0) + " = <b>" + f(q.vol / c.vol_thr * 100, 0) + "%</b> ของเกณฑ์</div>");
      }
      h.push("<table>" + dyn(i).map(function (l) {
        return '<tr><td><span class="sw" style="background:' + (COL[l.kind] || "#8a97a8") + '"></span>' + esc(l.label)
               + '</td><td class="n">' + f(l.price) + "</td></tr>";
      }).join("") + "</table>");
      if ((i.rules || []).length) h.push("<ul>" + i.rules.map(function (r) { return "<li>" + esc(r) + "</li>"; }).join("") + "</ul>");
      h.push("</div>");
    });
    $("detail").innerHTML = h.join("");
  }
  function ivBar() {
    $("ivs").innerHTML = IVS.map(function (x) {
      var dis = !S.api && x[0] !== "1d";
      return '<button data-iv="' + x[0] + '" class="' + (S.iv === x[0] ? "on" : "") + '"' + (dis ? " disabled" : "") + ">" + x[1] + "</button>";
    }).join(" ");
  }
  function select(sym) {
    S.sym = sym;
    $("sym").textContent = sym;
    var tv = tvSym(sym);
    $("tv").style.display = tv ? "" : "none";
    if (tv) $("tv").href = "https://www.tradingview.com/chart/?symbol=" + encodeURIComponent(tv);
    list(); detail();
    if (window.innerWidth < 900) $("center").scrollIntoView({behavior: "smooth"});
    return loadBars();
  }
  function loadPlan() {
    return J("data/plan.json?t=" + Date.now()).then(function (p) {
      S.plan = p;
      head(); ivBar(); list();
      if (!S.sym && (p.items || []).length) return select(p.items[0].symbol);
      detail(); overlays();
    }).catch(function (e) {
      $("banner").innerHTML = '<span class="chip c-neg">โหลดแผนไม่ได้</span> ' + esc(e.message) + " — รัน python3 -m sq.site build";
    });
  }
  function loadQuotes() {
    if (!S.api || !S.plan) return Promise.resolve();
    var syms = [];
    (S.plan.items || []).forEach(function (i) { if (syms.indexOf(i.symbol) < 0) syms.push(i.symbol); });
    if (S.sym && syms.indexOf(S.sym) < 0) syms.push(S.sym);
    if (!syms.length) return Promise.resolve();
    return J("api/quote?symbols=" + encodeURIComponent(syms.join(","))).then(function (q) {
      S.quotes = q.quotes || {};
      list(); detail(); overlays();
    }).catch(function () { /* ราคาล่าสุดไม่จำเป็นต่อการแสดงแผน */ });
  }

  document.addEventListener("click", function (ev) {
    var t = ev.target, b;
    if (!t || !t.closest) return;
    if ((b = t.closest("[data-s]"))) { select(b.getAttribute("data-s")); return; }
    if ((b = t.closest("[data-iv]")) && !b.disabled) { S.iv = b.getAttribute("data-iv"); ivBar(); loadBars(); }
  });
  $("go").onclick = function () { var v = ($("q").value || "").trim().toUpperCase(); if (v) select(v); };
  $("q").onkeydown = function (e) { if (e.key === "Enter") $("go").onclick(); };
  $("rf").onclick = function () { loadPlan().then(loadQuotes); loadBars(); };
  function tick() { $("clock").textContent = "BKK " + bkk().toISOString().slice(11, 16) + (marketOpen() ? " · ตลาดเปิด" : " · ตลาดปิด"); }
  setInterval(tick, 15000);
  setInterval(function () {
    if (document.hidden || !S.api) return;
    if (marketOpen() || isGold(S.sym)) { loadQuotes(); if (S.iv !== "1d") loadBars(); }
  }, 60000);
  setInterval(function () { if (!document.hidden) loadPlan(); }, 600000);

  if (!LC) { $("banner").innerHTML = '<span class="chip c-neg">โหลดไลบรารีกราฟไม่ได้</span> ต้องต่ออินเทอร์เน็ต'; return; }
  tick(); initChart(); ivBar();
  J("api/quote?symbols=").then(function () { S.api = true; }).catch(function () { S.api = false; })
    .then(function () { if (S.api && marketOpen()) S.iv = "15m"; ivBar(); return loadPlan(); })
    .then(loadQuotes);
})();
</script>
</body>
</html>
"""


# ═══════════════ BUILD · SERVE ═══════════════
def build(out="site", pl=None, ohlc=None, functions_dir=None) -> Dict[str, Any]:
    o = Path(out)
    dd = o / "data" / "daily"
    dd.mkdir(parents=True, exist_ok=True)
    pl = dict(pl if pl is not None else CH.plan(True))
    ohlc = ohlc or CH.ohlc
    syms = sorted({i["symbol"] for i in pl.get("items") or []} | set(ALWAYS))
    ok, bad = [], []
    for s in syms:
        try:
            d = ohlc(s, "1d")
        except Exception as e:
            d = {"error": f"{type(e).__name__}: {e}"}
        if d.get("bars"):
            (dd / fname(s)).write_text(json.dumps(ET._js(d), ensure_ascii=False), "utf-8")
            ok.append(s)
        else:
            bad.append(s)
    pl.update(daily_symbols=ok, built_at=ET._now(), site_version=__version__)
    (o / "data" / "plan.json").write_text(json.dumps(ET._js(pl), ensure_ascii=False), "utf-8")
    (o / "index.html").write_text(HTML, "utf-8")
    (o / "manifest.webmanifest").write_text(MANIFEST, "utf-8")
    (o / "_headers").write_text("/data/*\n  Cache-Control: no-cache\n/index.html\n  Cache-Control: no-cache\n", "utf-8")
    fd = Path(functions_dir) if functions_dir else o.resolve().parent / "functions" / "api"
    fd.mkdir(parents=True, exist_ok=True)
    (fd / "bars.js").write_text(JS_LIB + JS_BARS, "utf-8")
    (fd / "quote.js").write_text(JS_LIB + JS_QUOTE, "utf-8")
    return {"out": str(o.resolve()), "functions": str(fd), "symbols": len(ok), "missing": bad,
            "items": len(pl.get("items") or []), "trade_date": pl.get("trade_date")}


class _H(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _json(self, o, st=200):
        b = json.dumps(ET._js(o), ensure_ascii=False).encode("utf-8")
        self.send_response(st)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        u = urllib.parse.urlsplit(self.path)
        q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        try:
            if u.path == "/api/bars":
                return self._json(api_bars(q.get("symbol", ""), q.get("interval", "15m")))
            if u.path == "/api/quote":
                return self._json(api_quote((q.get("symbols") or "").split(",")))
        except Exception as e:
            return self._json({"error": f"{type(e).__name__}: {e}"}, 502)
        return super().do_GET()


def server(out, host, port):
    return http.server.ThreadingHTTPServer((host, port), functools.partial(_H, directory=str(Path(out).resolve())))


def lan_ip() -> Optional[str]:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


# ═══════════════ SELFTEST ═══════════════
def selftest() -> Dict[str, Any]:
    res = []

    def T(name, fn):
        try:
            ok, det = fn()
        except Exception as e:
            ok, det = False, f"{type(e).__name__}: {e}"
        res.append({"name": name, "ok": bool(ok), "detail": str(det)})

    j = {"chart": {"result": [{"meta": {"gmtoffset": 25200}, "timestamp": [1, 2, 3],
                               "indicators": {"quote": [{"open": [1, None, 3], "high": [2, 2, 4], "low": [0.5, 1, 2.5],
                                                         "close": [1.5, 1.8, 3.5], "volume": [10, 20, None]}]}}]}}
    p = parse(j)
    T("parse: ข้ามแท่งที่มีค่าว่าง · volume ว่าง = 0",
      lambda: (len(p["bars"]) == 2 and p["bars"][1] == [3, 3.0, 4.0, 2.5, 3.5, 0.0], str(p["bars"])))
    b0 = int(datetime(2026, 11, 12, 3, 0, tzinfo=timezone.utc).timestamp())
    sm = summary({"meta": {"gmtoffset": 25200}, "bars": [[b0, 10, 11, 9, 10.5, 100], [b0 + 86400, 11, 12, 10.5, 11.5, 200],
                                                         [b0 + 86700, 11.5, 13, 11, 12, 300]]})
    T("สรุปราคาวันล่าสุด (เวลาไทย): เปิด 11 · สูง 13 · ต่ำ 10.5 · ล่าสุด 12 · volume 500 · วันที่ 2026-11-13",
      lambda: ((sm["open"], sm["high"], sm["low"], sm["last"], sm["vol"], sm["date"]) == (11, 13, 10.5, 12, 500, "2026-11-13"), str(sm)))
    T("ฟังก์ชันคลาวด์ใช้ช่วงข้อมูลเดียวกับ Python", lambda: (all(f'"{k}": "{v}"' in JS_LIB for k, v in RANGE.items()), "ok"))
    with tempfile.TemporaryDirectory() as td:
        fake = {"trade_date": "2026-11-16", "items": [{"symbol": "PTT", "group": "MID_HOLD", "title": "x", "levels": []}]}
        r = build(Path(td) / "site", fake, lambda s, iv: {"bars": [["2026-11-13", 1, 1, 1, 1, 0]], "precision": 2},
                  Path(td) / "functions" / "api")
        st = Path(td) / "site"
        plj = json.loads((st / "data" / "plan.json").read_text("utf-8"))
        T("build: index.html · plan.json · กราฟรายวัน (GC=F → GC_F.json) · ฟังก์ชันคลาวด์ 2 ไฟล์",
          lambda: ("SQ Trade Desk" in (st / "index.html").read_text("utf-8") and (st / "data" / "daily" / "GC_F.json").is_file()
                   and len(plj["daily_symbols"]) == 4 and "onRequestGet" in (Path(td) / "functions" / "api" / "quote.js").read_text("utf-8"),
                   str(r)))
        srv = server(st, "127.0.0.1", 0)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{srv.server_address[1]}"
        try:
            a1 = json.loads(urllib.request.urlopen(base + "/data/plan.json", timeout=5).read())
            a2 = json.loads(urllib.request.urlopen(base + "/api/quote?symbols=", timeout=5).read())
            try:
                urllib.request.urlopen(base + "/api/bars?symbol=PTT&interval=9x", timeout=5)
                a3 = None
            except urllib.error.HTTPError as e:
                a3 = e.code
        finally:
            srv.shutdown()
        T("เซิร์ฟเวอร์: ไฟล์ static · /api/quote ว่าง → {} · interval ผิด → 502",
          lambda: (a1["trade_date"] == "2026-11-16" and a2["quotes"] == {} and a3 == 502, f"{a2} · {a3}"))
    n_ok = sum(1 for r in res if r["ok"])
    return {"ok": n_ok == len(res), "passed": n_ok, "total": len(res), "tests": res, "version": __version__}


def _cli():
    ap = argparse.ArgumentParser(prog="python3 -m sq.site")
    ap.add_argument("cmd", choices=("build", "serve", "selftest"))
    ap.add_argument("--out", default="site")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8810)
    ap.add_argument("--build", action="store_true", help="serve: สร้างเว็บใหม่ก่อนเปิด")
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if a.cmd == "selftest":
        r = selftest()
        for t in r["tests"]:
            print(("✓ " if t["ok"] else "✕ ") + t["name"] + " — " + t["detail"])
        print(f"\nผ่าน {r['passed']} / {r['total']}")
        return
    if a.cmd == "build" or a.build or not (Path(a.out) / "data" / "plan.json").is_file():
        print("สร้างเว็บ…", build(a.out))
    if a.cmd == "serve":
        srv = server(a.out, a.host, a.port)
        print(f"Trade Desk: http://127.0.0.1:{a.port}")
        if a.host == "0.0.0.0" and lan_ip():
            print(f"iPad / มือถือ (Wi-Fi เดียวกัน): http://{lan_ip()}:{a.port}")
        print("Ctrl+C เพื่อหยุด")
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    _cli()
# ═══ END sq/site.py v1.0 ═══