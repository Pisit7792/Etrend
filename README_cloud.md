# SQ Cloud — Trade Desk ส่วนตัว
1. GitHub: สร้าง repo แบบ Private → push โฟลเดอร์นี้
2. Cloudflare: Workers & Pages → Create → Pages → Direct Upload → ชื่อโปรเจกต์ sq-desk (อัปโหลดไฟล์อะไรก็ได้ 1 ครั้งเพื่อสร้าง)
3. Cloudflare: My Profile → API Tokens → Create → สิทธิ์ Account · Cloudflare Pages · Edit → คัดลอก token และ Account ID
4. GitHub repo → Settings → Secrets and variables → Actions → เพิ่ม CLOUDFLARE_API_TOKEN และ CLOUDFLARE_ACCOUNT_ID
5. GitHub → Actions → daily → Run workflow (mode = post) → ตรวจว่าผ่าน
6. Cloudflare Zero Trust → Access → Applications → Self-hosted → โดเมน sq-desk.pages.dev
   → Policy: Allow · Emails = อีเมลของคุณ · Login method: One-time PIN  (ฟรี · คนอื่นเข้าไม่ได้)
7. iPhone/iPad: Safari เปิด https://sq-desk.pages.dev → แชร์ → เพิ่มไปยังหน้าจอโฮม
หลักฐาน forward paper อยู่ใน repo นี้ (bot commit ทุกครั้ง) — อย่าแก้หรือลบไฟล์ .jsonl
