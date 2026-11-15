#!/usr/bin/env python3
"""
daily_run.py  [v1.0]
รันวันละ 2 ครั้ง (ก่อนตลาดเปิด · หลังตลาดปิด) — ใช้ได้ทั้งบน Mac และบน GitHub Actions (คลาวด์)
  python3 daily_run.py --post    หลังตลาดปิด: Early Trend · Mid-Long · ทองคำ (ถ้ามี) → ล็อกแผน → สร้างเว็บ
  python3 daily_run.py --pre     ก่อนตลาดเปิด: ถ้าแผนวันนี้ล็อกแล้ว = สร้างเว็บใหม่อย่างเดียว · ยังไม่มี = คำนวณ
  python3 daily_run.py --auto    เลือกเองตามเวลาไทย (ก่อน 12:00 = pre)
  python3 daily_run.py --make-cloud ~/Desktop/sq_cloud    สร้างโฟลเดอร์ repo สำหรับคลาวด์
ล็อกแผน: plans/plan_<วันทำการ>.json เขียนครั้งเดียว (append-only) · ไม่คำนวณซ้ำวันเดียวกันถ้าไม่ใส่ --force
"""
import argparse
import json
import shutil
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
TZ = ZoneInfo("Asia/Bangkok")
PLANS, LOG = ROOT / "plans", ROOT / "daily_run.log"
CLOUD_FILES = ["sq/etrend.py", "sq/midterm.py", "sq/chart.py", "sq/site.py", "sq/goldtrend.py", "daily_run.py",
               "sq_etrend_universe.txt", "sq_etrend.json", "sq_mid.json", "sq_set_index.csv", "sq_etrend_signals.jsonl",
               "sq_etrend_trials.jsonl", "sq_mid_forward.jsonl", "sq_mid_trials.jsonl", "sq_etrend_result.json",
               "sq_mid_result.json", "sq_gold_result.json", "sq_gold_forward.jsonl", "sq_gold_trials.jsonl"]
WORKFLOW = """name: daily
on:
  schedule:
    - cron: "30 1 * * 1-5"    # 08:30 เวลาไทย
    - cron: "45 10 * * 1-5"   # 17:45 เวลาไทย
  workflow_dispatch:
    inputs:
      mode:
        description: "pre / post / auto"
        default: "auto"
permissions:
  contents: write
concurrency:
  group: daily
  cancel-in-progress: false
jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    env:
      TZ: Asia/Bangkok
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - run: pip install -r requirements.txt
      - run: python daily_run.py --${{ github.event.inputs.mode || 'auto' }}
      - name: save evidence
        if: always()
        run: |
          git config user.name "sq-bot"
          git config user.email "sq-bot@users.noreply.github.com"
          git add -A
          git diff --cached --quiet || git commit -m "daily $(date +%F_%H%M)"
          git push || true
      - uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
          command: pages deploy site --project-name=sq-desk --branch=main
"""
GITIGNORE = "__pycache__/\n*.pkl\nsite/\nfunctions/\n*.bak_*\n_fixbak_*/\n.DS_Store\n"
README = """# SQ Cloud — Trade Desk ส่วนตัว
1. GitHub: สร้าง repo แบบ Private → push โฟลเดอร์นี้
2. Cloudflare: Workers & Pages → Create → Pages → Direct Upload → ชื่อโปรเจกต์ sq-desk (อัปโหลดไฟล์อะไรก็ได้ 1 ครั้งเพื่อสร้าง)
3. Cloudflare: My Profile → API Tokens → Create → สิทธิ์ Account · Cloudflare Pages · Edit → คัดลอก token และ Account ID
4. GitHub repo → Settings → Secrets and variables → Actions → เพิ่ม CLOUDFLARE_API_TOKEN และ CLOUDFLARE_ACCOUNT_ID
5. GitHub → Actions → daily → Run workflow (mode = post) → ตรวจว่าผ่าน
6. Cloudflare Zero Trust → Access → Applications → Self-hosted → โดเมน sq-desk.pages.dev
   → Policy: Allow · Emails = อีเมลของคุณ · Login method: One-time PIN  (ฟรี · คนอื่นเข้าไม่ได้)
7. iPhone/iPad: Safari เปิด https://sq-desk.pages.dev → แชร์ → เพิ่มไปยังหน้าจอโฮม
หลักฐาน forward paper อยู่ใน repo นี้ (bot commit ทุกครั้ง) — อย่าแก้หรือลบไฟล์ .jsonl
"""


def now() -> datetime:
    return datetime.now(TZ)


def say(msg):
    line = f"{now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def prev_bday(d: date) -> date:
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def next_bday(d: date) -> date:
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def expected_trade_date(t: datetime) -> date:
    d = t.date()
    asof = d if (d.weekday() < 5 and t.hour >= 17) else prev_bday(d)
    return next_bday(asof)


def run_strats() -> dict:
    st, jobs = {}, []
    import sq.etrend as ETM
    import sq.midterm as MTM
    jobs += [("etrend", lambda: ETM.run(True, 30, say=lambda s: None)),
             ("midterm", lambda: MTM.run(True, 100, say=lambda s: None))]
    try:
        import sq.goldtrend as GT
        jobs.append(("goldtrend", lambda: GT.run(True, None, say=lambda s: None)))
    except ImportError:
        st["goldtrend"] = "ยังไม่ติดตั้ง (ส่วนที่ 3)"
    for name, fn in jobs:
        t0 = time.time()
        try:
            rep = fn()
            sub = rep.get("scan") or rep.get("today") or {}
            st[name] = f"✓ {rep.get('state_label')} · ข้อมูลถึง {sub.get('asof') or rep.get('asof')} · {time.time() - t0:.0f} วิ"
        except Exception as e:
            st[name] = f"✕ {type(e).__name__}: {e}"[:300]
            with open(LOG, "a", encoding="utf-8") as fh:
                fh.write(traceback.format_exc() + "\n")
        say(f"  {name}: {st[name]}")
    return st


def build_site(status=None):
    import sq.chart as CH
    import sq.site as SITE
    r = SITE.build(str(ROOT / "site"))
    say(f"  เว็บ: {r['items']} รายการ · กราฟรายวัน {r['symbols']} ตัว · แผนวันทำการ {r['trade_date']}"
        + (f" · ไม่มีข้อมูล {r['missing']}" if r["missing"] else ""))
    if status is not None:
        pl = CH.plan(False)
        td = pl.get("trade_date")
        PLANS.mkdir(exist_ok=True)
        lock = PLANS / f"plan_{td}.json"
        if td and not lock.exists():
            lock.write_text(json.dumps({"locked_at": now().isoformat(timespec="seconds"), "status": status, "plan": pl},
                                       ensure_ascii=False, default=str), "utf-8")
            say(f"  ล็อกแผน → {lock.relative_to(ROOT)}")
    return r


def post(force=False):
    td = expected_trade_date(now())
    lock = PLANS / f"plan_{td}.json"
    if lock.exists() and not force:
        say(f"แผนวันทำการ {td} ล็อกแล้ว — ไม่คำนวณซ้ำ (ใช้ --force ถ้าจำเป็น)")
        build_site()
        return 0
    say(f"คำนวณแผนสำหรับวันทำการ {td}")
    st = run_strats()
    build_site(st)
    return 0 if any(v.startswith("✓") for v in st.values()) else 1


def pre(force=False):
    td = expected_trade_date(now())
    if not (PLANS / f"plan_{td}.json").exists() or force:
        say(f"ก่อนตลาดเปิด: ยังไม่มีแผนวันทำการ {td} → คำนวณ")
        return post(True)
    say(f"ก่อนตลาดเปิด: แผน {td} ล็อกแล้ว → อัปเดตกราฟรายวัน/ทอง")
    build_site()
    return 0


def make_cloud(dest):
    d = Path(dest).expanduser()
    (d / "sq").mkdir(parents=True, exist_ok=True)
    (d / ".github" / "workflows").mkdir(parents=True, exist_ok=True)
    got, miss = [], []
    for rel in CLOUD_FILES:
        p = ROOT / rel
        if p.exists():
            shutil.copy2(p, d / rel)
            got.append(rel)
        else:
            miss.append(rel)
    (d / "sq" / "__init__.py").write_text("", "utf-8")
    (d / "requirements.txt").write_text("numpy\npandas\nyfinance\n", "utf-8")
    (d / ".gitignore").write_text(GITIGNORE, "utf-8")
    (d / ".github" / "workflows" / "daily.yml").write_text(WORKFLOW, "utf-8")
    (d / "README_cloud.md").write_text(README, "utf-8")
    print(f"สร้าง {d} · คัดลอก {len(got)} ไฟล์")
    print("ยังไม่มี (ข้ามได้): " + ", ".join(miss) if miss else "ครบทุกไฟล์")
    print(f"\nขั้นต่อไป: cd {d} && git init && git add -A && git commit -m init  แล้วทำตาม README_cloud.md")


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pre", action="store_true")
    g.add_argument("--post", action="store_true")
    g.add_argument("--auto", action="store_true")
    g.add_argument("--make-cloud", metavar="DIR")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    if a.make_cloud:
        make_cloud(a.make_cloud)
        return
    mode = "pre" if a.pre or (a.auto and now().hour < 12) else "post"
    say(f"══ daily_run {mode} ══")
    sys.exit(pre(a.force) if mode == "pre" else post(a.force))


if __name__ == "__main__":
    main()
# ═══ END daily_run.py v1.0 ═══