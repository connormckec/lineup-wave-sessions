'use strict';
const express  = require('express');
const { chromium } = require('playwright');
const cron     = require('node-cron');
const path     = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT       = process.env.PORT || 3000;
const TOPIC      = process.env.NTFY_TOPIC || '';
const THRESH     = parseInt(process.env.LOW_SLOTS_THRESHOLD || '2');
const BOOKING    = 'https://booking.atlanticparksurf.com/activity-agenda';
const CHECK_MINS = parseInt(process.env.CHECK_EVERY_MINS || '5');

// ── In-memory state (persists while the server is running) ───────────────────
let sessions  = [];   // all sessions from last scrape
let watchList = [];   // [{id, key, ts, wave, level, time, date, dayLabel}]
let history   = {};   // {key: {available, slots}} — for change detection
let lastCheck = null; // ISO string of last successful check

// ── Push notification via Ntfy.sh ────────────────────────────────────────────
async function push(title, body, urgent = false) {
  if (!TOPIC) {
    console.log(`[push — no topic set] ${title}`);
    return;
  }
  try {
    const r = await fetch(`https://ntfy.sh/${TOPIC}`, {
      method: 'POST',
      headers: {
        Title:    title,
        Priority: urgent ? 'urgent' : 'high',
        Tags:     urgent ? 'wave,exclamation' : 'wave,tada',
        Click:    BOOKING,
      },
      body,
    });
    console.log(`📲  "${title}" → ntfy ${r.status}`);
  } catch (e) {
    console.error('ntfy error:', e.message);
  }
}

// ── Try to read how many slots are available in a session modal ──────────────
async function getSlotCount(page, ts, wave) {
  try {
    const tile = await page.$(`div[class*="booking-agenda-clickable_${ts}_${wave}"]`);
    if (!tile) return null;

    await tile.click();
    await page.waitForTimeout(1200);

    // Strategy 1: input[type=number] with a max attribute
    const fromAttr = await page.evaluate(() => {
      for (const inp of document.querySelectorAll('input[type="number"]')) {
        const m = inp.getAttribute('max');
        if (m !== null && m !== '' && parseInt(m) >= 0) return parseInt(m);
      }
      return null;
    });
    if (fromAttr !== null) { await closeModal(page); return fromAttr; }

    // Strategy 2: text like "3/12" or "3 slots remaining"
    const fromText = await page.evaluate(() => {
      const txt = document.body.innerText;
      const m1  = txt.match(/(\d+)\s*\/\s*12/);
      if (m1) return parseInt(m1[1]);
      const m2  = txt.match(/(\d+)\s*(slot|spot|available|remaining)/i);
      if (m2) return parseInt(m2[1]);
      return null;
    });
    if (fromText !== null) { await closeModal(page); return fromText; }

    // Strategy 3: click + until it stops, count clicks = available slots
    let n = 0;
    for (let i = 0; i < 13; i++) {
      const btn = await page.$(
        'button:has-text("+"), [class*="plus"], [aria-label*="ncrease"], [aria-label*="add"]'
      );
      if (!btn || await btn.isDisabled()) break;
      await btn.click();
      n++;
      await page.waitForTimeout(120);
    }
    await closeModal(page);
    return n > 0 ? n : null;

  } catch (e) {
    console.error(`getSlotCount(${ts}_${wave}):`, e.message);
    try { await closeModal(page); } catch {}
    return null;
  }
}

async function closeModal(page) {
  try {
    const x = await page.$('[class*="close"], [aria-label*="lose"], [data-dismiss="modal"], button.cancel');
    if (x) { await x.click(); await page.waitForTimeout(300); return; }
  } catch {}
  await page.keyboard.press('Escape');
  await page.waitForTimeout(400);
}

// ── Main check: open booking page, read sessions, notify on changes ──────────
async function runCheck() {
  console.log(`\n[${new Date().toLocaleTimeString()}] Checking sessions...`);

  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });

    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125 Safari/537.36',
    });
    const page = await context.newPage();
    await page.goto(BOOKING, { waitUntil: 'networkidle', timeout: 30_000 });
    await page.waitForSelector('.dynamic-cal-booking-ts', { timeout: 15_000 });

    const fresh = await page.evaluate(() => {
      const seen = new Set(), out = [];
      document.querySelectorAll('div.dynamic-cal-booking-ts[data-original-title]').forEach(el => {
        const cls = el.className;
        const t   = el.dataset.originalTitle || '';
        const lm  = t.match(/Session level\s*:<\/b>\s*([^<]+)/i);
        const wm  = cls.match(/booking-agenda-clickable_(\d+)_(\d+)/);
        if (!lm || !wm) return;
        const level = lm[1].trim();
        if (level === 'Cabanas') return;
        const ts = +wm[1], wave = +wm[2], key = `${ts}_${wave}`;
        if (seen.has(key)) return; seen.add(key);
        const fm = t.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
        const d  = new Date(ts * 1000);
        const today = new Date(), tom = new Date(today);
        tom.setDate(tom.getDate() + 1);
        let dayLabel;
        if (d.toDateString() === today.toDateString())     dayLabel = 'Today';
        else if (d.toDateString() === tom.toDateString())  dayLabel = 'Tomorrow';
        else dayLabel = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
        out.push({
          key, ts, wave, level,
          available : !cls.includes('expired_timeslot'),
          time      : fm ? fm[1].trim() : '?',
          date      : d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }),
          dayLabel,
        });
      });
      return out;
    });

    // Get slot counts only for watched sessions that are available
    for (const w of watchList) {
      const s = fresh.find(s => s.key === w.key && s.available);
      if (s) s.slots = await getSlotCount(page, s.ts, s.wave);
    }

    // Detect changes and fire notifications
    for (const s of fresh) {
      if (!watchList.find(w => w.key === s.key)) continue;
      const prev  = history[s.key] || {};
      const label = `${s.level} · ${s.date} ${s.time} (Wave ${s.wave})`;

      if (s.available && prev.available === false) {
        // Was full — just opened!
        await push(`🏄 Spot opened! ${s.level}`, `${label}\n\nBook now: ${BOOKING}`, true);

      } else if (s.available && s.slots != null && s.slots <= THRESH) {
        // Just dropped to low slots
        if (prev.slots == null || prev.slots > THRESH) {
          const n = s.slots === 1 ? 'slot' : 'slots';
          await push(
            `⚡ Only ${s.slots} ${n} left — ${s.level}`,
            `${label}\n\nBook soon: ${BOOKING}`,
            true
          );
        }
      }

      history[s.key] = { available: s.available, slots: s.slots ?? null };
    }

    sessions  = fresh;
    lastCheck = new Date().toISOString();
    const open = fresh.filter(s => s.available).length;
    console.log(`  ${fresh.length} sessions found, ${open} open`);

  } catch (e) {
    console.error('runCheck failed:', e.message);
  } finally {
    if (browser) await browser.close();
  }
}

// ── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/status', (_req, res) => {
  res.json({ sessions, watchList, history, lastCheck, ntfyOk: !!TOPIC });
});

app.post('/api/watch', (req, res) => {
  const { key, ts, wave, level, time, date, dayLabel } = req.body;
  if (!key) return res.status(400).json({ error: 'key required' });
  if (watchList.some(w => w.key === key)) return res.json({ ok: true, duplicate: true });
  const id = Date.now().toString();
  watchList.push({ id, key, ts: +ts, wave: +wave, level, time, date, dayLabel });
  console.log(`  👁  Watching: ${level} ${date} ${time} W${wave}`);
  res.json({ ok: true, id });
});

app.delete('/api/watch/:id', (req, res) => {
  const before = watchList.length;
  watchList = watchList.filter(w => w.id !== req.params.id);
  console.log(`  🗑  Removed watch (${before - watchList.length} removed)`);
  res.json({ ok: true });
});

// ── Boot ─────────────────────────────────────────────────────────────────────
const cronExpr = `*/${CHECK_MINS} * * * *`;
cron.schedule(cronExpr, () => runCheck().catch(console.error));

runCheck().catch(console.error); // check immediately on startup

app.listen(PORT, () => {
  console.log(`\nAP Session Watcher running on :${PORT}`);
  console.log(`Checking every ${CHECK_MINS} minutes`);
  console.log(TOPIC ? `Ntfy topic: ${TOPIC}` : 'WARNING: NTFY_TOPIC not set — no notifications will be sent');
});
