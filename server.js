'use strict';
const express  = require('express');
const { chromium } = require('playwright');
const cron     = require('node-cron');
const path     = require('path');
const fs       = require('fs');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT       = process.env.PORT || 3000;
const TOPIC      = process.env.NTFY_TOPIC || '';
const THRESH     = parseInt(process.env.LOW_SLOTS_THRESHOLD || '2');
const BOOKING    = 'https://booking.atlanticparksurf.com/activity-agenda';
const CHECK_MINS      = parseInt(process.env.CHECK_EVERY_MINS || '5', 10);
const MAX_SLOT_CHECKS = parseInt(process.env.MAX_SLOT_CHECKS || '50', 10);
const SLOT_CACHE_STALE_CYCLES = parseInt(process.env.SLOT_CACHE_STALE_CYCLES || '3', 10);
const SCRAPE_WEEKS_AHEAD = parseInt(process.env.SCRAPE_WEEKS_AHEAD || '4', 10);
const DATA_DIR           = process.env.DATA_DIR || path.join(__dirname, 'data');
const SNAPSHOT_PATH      = path.join(DATA_DIR, 'snapshot.json');
const PERSISTENT_CACHE   = !!process.env.DATA_DIR;
const EXCLUDED_LEVELS    = ['Cabanas', 'Beach Pass'];
const EXCLUDED_WAVES     = [5, 6];
const SCRAPE_OPTS = { excludedLevels: EXCLUDED_LEVELS, excludedWaves: EXCLUDED_WAVES };

const TIER_CONFIG = {
  1: { label: 'today/tomorrow', slotCounts: true,  weekStart: 0, weekEnd: 0, minDay: 0,  maxDay: 1 },
  2: { label: 'this week',      slotCounts: false, weekStart: 0, weekEnd: 0, minDay: 2,  maxDay: 6 },
  3: { label: 'weeks 2–3',      slotCounts: false, weekStart: 1, weekEnd: 2, minDay: 7,  maxDay: 20 },
  4: { label: 'weeks 4+',       slotCounts: false, weekStart: 3, weekEnd: null, minDay: 21, maxDay: null },
};

// ── In-memory state (persists while the server is running) ───────────────────
let sessions      = [];   // merged view served via API
const sessionsByKey = new Map();
let watchList     = [];   // [{id, key, ts, wave, level, time, date, dayLabel}]
let history       = {};   // {key: {available, slots}} — for change detection
let slotCache     = {};   // {key: {slots, available, lastCheckedCycle}}
let slotCheckDeferrals = new Set();
let checkCycle    = 0;
let lastCheck     = null;
let lastSuccessfulScrape = null;
let lastScrapeAttempt  = null;
let lastScrapeError    = null;
let scrapeInProgress   = false;
let hasFreshScrapeThisBoot = false;
let slotChecksThisCycle = 0;
let weeksAvailableOnSite = null; // detected from booking UI
let effectiveWeeksAhead  = SCRAPE_WEEKS_AHEAD;
const lastTierRun = { 1: null, 2: null, 3: null, 4: null };
let scrapeLock = Promise.resolve();

function logPersistentCacheWarning() {
  if (PERSISTENT_CACHE) {
    console.log(`Persistent cache: DATA_DIR=${DATA_DIR}`);
    return;
  }
  console.warn('WARNING: DATA_DIR not set — snapshot cache is stored locally and will be lost on redeploy/restart.');
  console.warn('         Mount a Railway Volume at /data and set DATA_DIR=/data for persistent stale-while-revalidate cache.');
}

function getStatusFields() {
  const dataAgeMinutes = lastSuccessfulScrape
    ? Math.max(0, Math.round((Date.now() - new Date(lastSuccessfulScrape).getTime()) / 60000))
    : null;
  return {
    sessionsCount: sessions.length,
    isUsingCachedData: sessions.length > 0 && !hasFreshScrapeThisBoot,
    lastSuccessfulScrape,
    lastScrapeAttempt,
    lastScrapeError,
    dataAgeMinutes,
    scrapeInProgress,
  };
}

async function loadSnapshot() {
  try {
    const raw = await fs.promises.readFile(SNAPSHOT_PATH, 'utf8');
    const snap = JSON.parse(raw);

    if (Array.isArray(snap.sessions) && snap.sessions.length) {
      sessionsByKey.clear();
      for (const s of snap.sessions) sessionsByKey.set(s.key, s);
      rebuildSessionsArray();
    }

    const meta = snap.scrapeMeta || {};
    if (meta.weeksAvailableOnSite != null) weeksAvailableOnSite = meta.weeksAvailableOnSite;
    if (meta.effectiveWeeksAhead != null) effectiveWeeksAhead = meta.effectiveWeeksAhead;
    if (meta.lastTierRun) Object.assign(lastTierRun, meta.lastTierRun);

    if (snap.slotCache && typeof snap.slotCache === 'object') slotCache = snap.slotCache;

    lastSuccessfulScrape = snap.lastSuccessfulScrape || snap.savedAt || null;
    lastCheck = lastSuccessfulScrape;

    const age = lastSuccessfulScrape
      ? Math.round((Date.now() - new Date(lastSuccessfulScrape).getTime()) / 60000)
      : '?';
    console.log(`  loaded snapshot: ${sessions.length} sessions (saved ${age}m ago) from ${SNAPSHOT_PATH}`);
  } catch (e) {
    if (e.code === 'ENOENT') {
      console.log('  no snapshot found — dashboard will populate after first successful scrape');
    } else {
      console.error('  snapshot load failed:', e.message);
    }
  }
}

async function saveSnapshot() {
  try {
    await fs.promises.mkdir(DATA_DIR, { recursive: true });
    const payload = {
      version: 1,
      savedAt: new Date().toISOString(),
      lastSuccessfulScrape,
      sessions,
      slotCache,
      scrapeMeta: {
        weeksAvailableOnSite,
        effectiveWeeksAhead,
        scrapeWeeksAhead: SCRAPE_WEEKS_AHEAD,
        lastTierRun: { ...lastTierRun },
      },
    };
    const tmp = `${SNAPSHOT_PATH}.tmp`;
    await fs.promises.writeFile(tmp, JSON.stringify(payload), 'utf8');
    await fs.promises.rename(tmp, SNAPSHOT_PATH);
    console.log(`  snapshot saved (${sessions.length} sessions) → ${SNAPSHOT_PATH}`);
  } catch (e) {
    console.error('  snapshot save failed:', e.message);
  }
}

function withScrapeLock(fn) {
  const run = scrapeLock.then(fn, fn);
  scrapeLock = run.catch(() => {});
  return run;
}

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

const MAX_SLOT_CLICKS = 30; // safety ceiling only — actual max comes from the booking UI
const MODAL_SELECTORS = ['.modal.in', '.modal.show', '.modal', '[class*="modal-dialog"]', '[class*="popup"]', '[class*="booking"]', '.popover'];
const PLUS_SELECTORS = [
  '.btn-plus',
  'button.btn-plus',
  '.btn-default.btn-plus',
  'button:has-text("+")',
  '[class*="increment"]',
  '[class*="plus"]',
  '[aria-label*="add"]',
  '[aria-label*="increase"]',
  '.quantity-plus',
  '.qty-plus',
];

function activeModal(page) {
  return page.locator('.modal.in, .modal.show').last();
}

async function isModalVisible(page) {
  const modal = activeModal(page);
  if (await modal.count() && await modal.isVisible().catch(() => false)) return true;
  for (const sel of MODAL_SELECTORS) {
    const el = page.locator(sel).last();
    if (await el.count() && await el.isVisible().catch(() => false)) return true;
  }
  return false;
}

async function waitForModal(page, label) {
  try {
    const modal = activeModal(page);
    await modal.waitFor({ state: 'visible', timeout: 4_000 });
    console.log(`  [getSlotCount ${label}] modal appeared: .modal.in`);
    return modal;
  } catch {
    console.log(`  [getSlotCount ${label}] .modal.in timed out, trying fallbacks...`);
  }

  for (const sel of MODAL_SELECTORS) {
    try {
      const modal = page.locator(sel).last();
      await modal.waitFor({ state: 'visible', timeout: 4_000 });
      console.log(`  [getSlotCount ${label}] modal appeared: ${sel}`);
      return modal;
    } catch {
      console.log(`  [getSlotCount ${label}] modal selector timed out: ${sel}`);
    }
  }
  console.log(`  [getSlotCount ${label}] no modal found after trying all selectors`);
  return null;
}

async function isPlusDisabled(btn) {
  if (!btn) return true;
  try {
    if (await btn.isDisabled()) return true;
  } catch {}
  const cls = (await btn.getAttribute('class')) || '';
  return /disabled|inactive/i.test(cls);
}

async function findPlusButton(modal, label, logAll = false) {
  for (const sel of PLUS_SELECTORS) {
    const all = modal.locator(sel);
    const count = await all.count();
    let btn = null;
    let vis = false;
    for (let i = count - 1; i >= 0; i--) {
      const candidate = all.nth(i);
      if (await candidate.isVisible().catch(() => false)) {
        btn = candidate;
        vis = true;
        break;
      }
    }
    if (logAll) {
      console.log(`  [getSlotCount ${label}] plus selector "${sel}": count=${count} visible=${vis}`);
    }
    if (vis) return btn;
  }
  return null;
}

async function closeModal(page, label = '') {
  const tag = label ? `getSlotCount ${label}` : 'closeModal';
  console.log(`  [${tag}] closing modal...`);

  const modal = activeModal(page);
  const closeSelectors = [
    'button.close',
    '[data-dismiss="modal"]',
    'button:has-text("Cancel")',
    'button.cancel',
    '.modal-header button',
    '[aria-label*="lose"]',
    '[class*="close"]',
  ];

  if (await modal.count()) {
    for (const sel of closeSelectors) {
      try {
        const btn = modal.locator(sel).first();
        if (await btn.isVisible().catch(() => false)) {
          await btn.click({ timeout: 5_000 });
          await page.waitForTimeout(300);
          console.log(`  [${tag}] clicked close selector in modal: ${sel}`);
          if (!(await isModalVisible(page))) {
            console.log(`  [${tag}] modal confirmed gone`);
            return true;
          }
        }
      } catch (e) {
        console.log(`  [${tag}] close selector "${sel}" failed: ${e.message}`);
      }
    }
  }

  try {
    await page.mouse.click(10, 10);
    await page.waitForTimeout(300);
    console.log(`  [${tag}] clicked outside modal (10,10)`);
    if (!(await isModalVisible(page))) {
      console.log(`  [${tag}] modal confirmed gone after outside click`);
      return true;
    }
  } catch (e) {
    console.log(`  [${tag}] outside click failed: ${e.message}`);
  }

  try {
    await page.keyboard.press('Escape');
    await page.waitForTimeout(400);
    console.log(`  [${tag}] pressed Escape`);
  } catch (e) {
    console.log(`  [${tag}] Escape failed: ${e.message}`);
  }

  const gone = !(await isModalVisible(page));
  console.log(`  [${tag}] modal gone = ${gone}`);
  return gone;
}

async function getSlotCount(page, ts, wave) {
  const label = `${ts}_${wave}`;
  console.log(`\n[getSlotCount ${label}] starting`);

  try {
    const tileSel = `div[class*="booking-agenda-clickable_${ts}_${wave}"]`;
    const tile = await page.$(tileSel);
    if (!tile) {
      console.log(`  [getSlotCount ${label}] tile not found (${tileSel})`);
      return null;
    }
    console.log(`  [getSlotCount ${label}] tile found, clicking...`);

    await tile.click({ timeout: 10_000 });
    console.log(`  [getSlotCount ${label}] tile click registered`);

    const modal = await waitForModal(page, label);
    if (!modal) {
      console.log(`  [getSlotCount ${label}] abort — modal never appeared`);
      return null;
    }

    const screenshotPath = path.join(__dirname, 'debug-modal.png');
    if (process.env.DEBUG_MODAL === '1') {
      await page.screenshot({ path: screenshotPath });
      console.log(`  [getSlotCount ${label}] debug screenshot saved → ${screenshotPath}`);
    }

    await findPlusButton(modal, label, true);

    let n = 0;
    for (let i = 0; i < MAX_SLOT_CLICKS; i++) {
      const btn = await findPlusButton(modal, label, false);
      if (!btn) {
        console.log(`  [getSlotCount ${label}] click ${i + 1}: no visible + button found, stopping`);
        break;
      }
      if (await isPlusDisabled(btn)) {
        console.log(`  [getSlotCount ${label}] click ${i + 1}: + button disabled, stopping at ${n}`);
        break;
      }
      await btn.click({ timeout: 5_000 });
      n++;
      const qty = await modal.locator('input.qty-info').last().inputValue().catch(() => '?');
      console.log(`  [getSlotCount ${label}] click ${i + 1}: + clicked, count=${n}, qty-input=${qty}`);
      await page.waitForTimeout(120);
    }

    if (n > 20) {
      console.warn(`  [getSlotCount ${label}] WARNING: ${n} clicks — exceeds expected max (Progressive≈18, Pro≈10); + button detection may have run away`);
    }

    console.log(`  [getSlotCount ${label}] result: ${n} available slot(s)`);
    await closeModal(page, label);
    return n > 0 ? n : null;

  } catch (e) {
    console.error(`  [getSlotCount ${label}] ERROR: ${e.message}`);
    try { await closeModal(page, label); } catch (ce) {
      console.error(`  [getSlotCount ${label}] close after error failed: ${ce.message}`);
    }
    return null;
  }
}

// Parse session tiles currently visible in the agenda DOM.
function scrapeVisibleSessions({ excludedLevels = [], excludedWaves = [] } = {}) {
  const seen = new Set(), out = [];
  const allEls = document.querySelectorAll('div.dynamic-cal-booking-ts[data-original-title]');
  const rawCount = allEls.length;
  allEls.forEach(el => {
    const cls = el.className;
    const t   = el.dataset.originalTitle || '';
    const lm  = t.match(/Session level\s*:<\/b>\s*([^<]+)/i);
    const wm  = cls.match(/booking-agenda-clickable_(\d+)_(\d+)/);
    if (!lm || !wm) return;
    const level = lm[1].trim();
    const wave  = +wm[2];
    if (excludedLevels.includes(level)) return;
    if (excludedWaves.includes(wave)) return;
    const ts = +wm[1], key = `${ts}_${wave}`;
    if (seen.has(key)) return; seen.add(key);
    const fm = t.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
    const d  = new Date(ts * 1000);
    const today = new Date(), tom = new Date(today);
    tom.setDate(tom.getDate() + 1);
    let dayLabel;
    if (d.toDateString() === today.toDateString())     dayLabel = 'Today';
    else if (d.toDateString() === tom.toDateString())  dayLabel = 'Tomorrow';
    else dayLabel = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
    const dateKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    out.push({
      key, ts, wave, level,
      available : !cls.includes('expired_timeslot'),
      time      : fm ? fm[1].trim() : '?',
      date      : d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }),
      dayLabel,
      dateKey,
    });
  });
  return { sessions: out, rawCount, duplicateSkips: rawCount - out.length };
}

// Calendar week nav: .glyphicon-chevron-right advances one week forward.
// Each click replaces the visible week (does not accumulate in the DOM).
async function advanceCalendarWeek(page) {
  const chevron = page.locator('.glyphicon-chevron-right').first();
  if (!await chevron.count()) return false;
  await chevron.click();
  await page.waitForTimeout(1500);
  try {
    await page.waitForSelector('.dynamic-cal-booking-ts', { timeout: 10_000 });
  } catch {}
  return true;
}

function todayDateKey() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function slotCheckDecision(s, prevSession, watchKeys) {
  const cached = slotCache[s.key];
  const prevAvail = prevSession?.available;

  if (!s.available) return { useCache: false, recheck: false };

  if (watchKeys.has(s.key)) return { useCache: false, recheck: true, reason: 'watched' };
  if (prevAvail === false) return { useCache: false, recheck: true, reason: 'opened' };
  if (slotCheckDeferrals.has(s.key)) return { useCache: false, recheck: true, reason: 'deferred' };

  if (cached?.slots != null && cached.available === s.available) {
    const cyclesSince = checkCycle - cached.lastCheckedCycle;
    if (cyclesSince < SLOT_CACHE_STALE_CYCLES) {
      return { useCache: true, slots: cached.slots, recheck: false };
    }
    return { useCache: false, recheck: true, reason: 'stale' };
  }

  return { useCache: false, recheck: true, reason: 'uncached' };
}

function prioritizeForSlotCheck(batch) {
  const todayKey = todayDateKey();
  const reasonScore = { watched: 0, opened: 1, deferred: 2, stale: 3, uncached: 4 };

  return batch.sort((a, b) => {
    const ra = a._recheckReason || 'uncached';
    const rb = b._recheckReason || 'uncached';
    const rDiff = (reasonScore[ra] ?? 5) - (reasonScore[rb] ?? 5);
    if (rDiff) return rDiff;
    const aToday = a.dateKey === todayKey ? 0 : 1;
    const bToday = b.dateKey === todayKey ? 0 : 1;
    if (aToday !== bToday) return aToday - bToday;
    return a.ts - b.ts;
  });
}

async function fillSlotCounts(page, batch, byKey, prevByKey, stats) {
  const watchKeys = new Set(watchList.map(w => w.key));
  const toRecheck = [];

  for (const s of batch) {
    const entry = byKey.get(s.key);
    if (!entry?.available) continue;

    const decision = slotCheckDecision(s, prevByKey.get(s.key), watchKeys);
    if (decision.useCache) {
      entry.slots = decision.slots;
      stats.cached++;
      continue;
    }
    if (decision.recheck) toRecheck.push({ session: s, reason: decision.reason });
  }

  const ordered = prioritizeForSlotCheck(
    toRecheck.map(r => ({ ...r.session, _recheckReason: r.reason }))
  );

  if (ordered.length && !stats.queueLogged) {
    console.log(`  slot check queue (${ordered.length} to recheck, max ${MAX_SLOT_CHECKS} this cycle):`);
    ordered.slice(0, 30).forEach((s, i) => {
      console.log(`    ${i + 1}. ${s.dayLabel} ${s.time} ${s.level} W${s.wave} [${s._recheckReason}]`);
    });
    if (ordered.length > 30) console.log(`    ... +${ordered.length - 30} more`);
    stats.queueLogged = true;
  }

  const deferredThisCycle = new Set();

  for (let i = 0; i < ordered.length; i++) {
    const s = ordered[i];
    const entry = byKey.get(s.key);
    const reason = s._recheckReason;

    if (slotChecksThisCycle >= MAX_SLOT_CHECKS) {
      const remaining = ordered.slice(i);
      console.log(`  [fillSlotCounts] MAX_SLOT_CHECKS=${MAX_SLOT_CHECKS} reached — ${remaining.length} recheck(s) deferred to next cycle`);
      for (const rest of remaining) {
        deferredThisCycle.add(rest.key);
        const cached = slotCache[rest.key];
        if (cached?.slots != null) byKey.get(rest.key).slots = cached.slots;
      }
      break;
    }

    entry.slots = await getSlotCount(page, s.ts, s.wave);
    slotChecksThisCycle++;
    stats.rechecked++;
    stats.byReason[reason] = (stats.byReason[reason] || 0) + 1;

    if (entry.slots != null) {
      slotCache[s.key] = { slots: entry.slots, available: true, lastCheckedCycle: checkCycle };
      slotCheckDeferrals.delete(s.key);
    } else {
      deferredThisCycle.add(s.key);
    }
  }

  slotCheckDeferrals.clear();
  for (const key of deferredThisCycle) slotCheckDeferrals.add(key);
}

function applySlotCacheFallback(byKey) {
  for (const s of byKey.values()) {
    if (!s.available || s.slots != null) continue;
    const cached = slotCache[s.key];
    if (cached?.slots != null && cached.available) s.slots = cached.slots;
  }
}

function syncSlotCacheAvailability(fresh) {
  for (const s of fresh) {
    if (!s.available && slotCache[s.key]) slotCache[s.key].available = false;
  }
}

function dedupeBatch(batch) {
  const byKey = new Map();
  for (const s of batch) {
    if (!byKey.has(s.key)) byKey.set(s.key, { ...s });
    else byKey.get(s.key).available = s.available;
  }
  return [...byKey.values()];
}

function daysFromToday(dateKey) {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const d = new Date(dateKey + 'T12:00:00');
  d.setHours(0, 0, 0, 0);
  return Math.round((d - today) / 86_400_000);
}

function tierMaxDay(tier) {
  const cfg = TIER_CONFIG[tier];
  if (cfg.maxDay != null) return cfg.maxDay;
  return effectiveWeeksAhead * 7 - 1;
}

function sessionInTier(s, tier) {
  const days = daysFromToday(s.dateKey);
  const cfg = TIER_CONFIG[tier];
  return days >= cfg.minDay && days <= tierMaxDay(tier);
}

function filterBatchForTier(batch, tier) {
  return batch.filter(s => sessionInTier(s, tier));
}

function rebuildSessionsArray() {
  const maxDay = effectiveWeeksAhead * 7;
  sessions = [...sessionsByKey.values()]
    .filter(s => {
      const days = daysFromToday(s.dateKey);
      return days >= 0 && days < maxDay;
    })
    .sort((a, b) => a.ts - b.ts || a.wave - b.wave);
}

function mergeBatchIntoStore(batch, tier, { preserveSlots = true } = {}) {
  const now = new Date().toISOString();
  const updatedKeys = [];

  for (const raw of batch) {
    if (!sessionInTier(raw, tier)) continue;
    const existing = sessionsByKey.get(raw.key);
    const merged = {
      ...(existing || {}),
      ...raw,
      tier,
      lastScraped: now,
    };
    if (preserveSlots && existing?.slots != null && merged.slots == null) {
      merged.slots = existing.slots;
    }
    sessionsByKey.set(raw.key, merged);
    updatedKeys.push(raw.key);
  }

  rebuildSessionsArray();
  return updatedKeys;
}

async function launchBrowser() {
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125 Safari/537.36',
  });
  const page = await context.newPage();
  return { browser, page };
}

async function openBookingPage(page) {
  await page.goto(BOOKING, { waitUntil: 'networkidle', timeout: 30_000 });
  await page.waitForSelector('.dynamic-cal-booking-ts', { timeout: 15_000 });
}

async function detectAvailableWeeks(page) {
  await openBookingPage(page);
  let weeks = 1;
  const seenKeys = new Set();

  async function absorbWeek() {
    const { sessions: batch } = await page.evaluate(scrapeVisibleSessions, SCRAPE_OPTS);
    let added = 0;
    for (const s of batch) {
      if (!seenKeys.has(s.key)) { seenKeys.add(s.key); added++; }
    }
    return added;
  }

  await absorbWeek();

  while (weeks < 12) {
    const chevron = page.locator('.glyphicon-chevron-right').first();
    if (!await chevron.count()) break;
    const disabled = await chevron.evaluate(el =>
      el.classList.contains('disabled') ||
      !!el.closest('[disabled]') ||
      window.getComputedStyle(el).opacity === '0.3'
    ).catch(() => false);
    if (disabled) break;

    if (!await advanceCalendarWeek(page)) break;
    weeks++;
    const added = await absorbWeek();
    if (added === 0) break;
  }

  return weeks;
}

function updateEffectiveWeeksCap(detectedWeeks) {
  weeksAvailableOnSite = detectedWeeks;
  effectiveWeeksAhead = Math.min(SCRAPE_WEEKS_AHEAD, detectedWeeks);
  console.log(`  booking calendar: ${detectedWeeks} week(s) available on site`);
  console.log(`  SCRAPE_WEEKS_AHEAD=${SCRAPE_WEEKS_AHEAD} → effective lookahead ${effectiveWeeksAhead} week(s)`);
  if (effectiveWeeksAhead < SCRAPE_WEEKS_AHEAD) {
    console.log(`  capped SCRAPE_WEEKS_AHEAD at ${effectiveWeeksAhead} — site does not expose further weeks`);
  }
}

async function navigateToWeek(page, weekIndex) {
  for (let i = 0; i < weekIndex; i++) {
    if (!await advanceCalendarWeek(page)) return false;
  }
  return true;
}

async function scrapeWeekRange(page, weekStart, weekEnd) {
  const collected = [];
  let rawTilesTotal = 0;
  let duplicateSkipsTotal = 0;

  if (!await navigateToWeek(page, weekStart)) {
    return { batch: collected, rawTilesTotal, duplicateSkipsTotal };
  }

  for (let week = weekStart; week <= weekEnd; week++) {
    const { sessions: batch, rawCount, duplicateSkips } = await page.evaluate(scrapeVisibleSessions, SCRAPE_OPTS);
    rawTilesTotal += rawCount;
    duplicateSkipsTotal += duplicateSkips;
    collected.push(...batch);
    console.log(`    week ${week + 1}: ${batch.length} sessions (${rawCount} tiles)`);
    if (week < weekEnd && !(await advanceCalendarWeek(page))) break;
  }

  return { batch: collected, rawTilesTotal, duplicateSkipsTotal };
}

async function processNotifications(updatedKeys, { slotsAlerts = false } = {}) {
  for (const key of updatedKeys) {
    const s = sessionsByKey.get(key);
    if (!s || !watchList.find(w => w.key === key)) continue;
    const prev = history[s.key] || {};
    const label = `${s.level} · ${s.date} ${s.time} (Wave ${s.wave})`;

    if (s.available && prev.available === false) {
      await push(`🏄 Spot opened! ${s.level}`, `${label}\n\nBook now: ${BOOKING}`, true);
    } else if (slotsAlerts && s.available && s.slots != null && s.slots <= THRESH) {
      if (prev.slots == null || prev.slots > THRESH) {
        const n = s.slots === 1 ? 'slot' : 'slots';
        await push(`⚡ Only ${s.slots} ${n} left — ${s.level}`, `${label}\n\nBook soon: ${BOOKING}`, true);
      }
    }

    history[s.key] = { available: s.available, slots: s.slots ?? null };
  }
}

async function runTierScrape(tier) {
  return withScrapeLock(async () => {
    const cfg = TIER_CONFIG[tier];
    const configuredEnd = cfg.weekEnd == null ? effectiveWeeksAhead - 1 : cfg.weekEnd;
    const weekEnd = Math.min(configuredEnd, effectiveWeeksAhead - 1);
    if (weekEnd < cfg.weekStart) {
      console.log(`[tier ${tier}] skipped — no weeks in range (weeks ${cfg.weekStart}–${weekEnd})`);
      return;
    }

    console.log(`\n[${new Date().toLocaleTimeString()}] Tier ${tier} scrape (${cfg.label}, weeks ${cfg.weekStart + 1}–${weekEnd + 1})`);

    scrapeInProgress = true;
    lastScrapeAttempt = new Date().toISOString();

    if (tier === 1) {
      checkCycle++;
      slotChecksThisCycle = 0;
    }

    const slotStats = { cached: 0, rechecked: 0, byReason: {}, queueLogged: false };
    const prevByKey = new Map(sessions.map(s => [s.key, s]));
    let launched;

    try {
      launched = await launchBrowser();
      const { page } = launched;
      await openBookingPage(page);

      const { batch: rawBatch, rawTilesTotal } =
        await scrapeWeekRange(page, cfg.weekStart, weekEnd);

      const batch = dedupeBatch(filterBatchForTier(rawBatch, tier));
      const byKey = new Map(batch.map(s => [s.key, s]));

      if (cfg.slotCounts) {
        await fillSlotCounts(page, batch, byKey, prevByKey, slotStats);
        applySlotCacheFallback(byKey);
      }

      const merged = [...byKey.values()];
      const updatedKeys = mergeBatchIntoStore(merged, tier, { preserveSlots: !cfg.slotCounts });

      if (cfg.slotCounts) {
        syncSlotCacheAvailability(merged);
        const reasonParts = Object.entries(slotStats.byReason).map(([k, n]) => `${n} ${k}`);
        console.log(`  slot counts: ${slotStats.cached} from cache, ${slotStats.rechecked} re-checked${reasonParts.length ? ` (${reasonParts.join(', ')})` : ''}`);
      }

      console.log(`  tier ${tier} summary: ${rawTilesTotal} tiles, ${batch.length} in range, ${updatedKeys.length} updated`);
      await processNotifications(updatedKeys, { slotsAlerts: cfg.slotCounts });

      lastTierRun[tier] = new Date().toISOString();
      lastSuccessfulScrape = new Date().toISOString();
      lastCheck = lastSuccessfulScrape;
      lastScrapeError = null;
      hasFreshScrapeThisBoot = true;
      await saveSnapshot();

    } catch (e) {
      lastScrapeError = e.message;
      console.error(`tier ${tier} scrape failed:`, e.message);
    } finally {
      scrapeInProgress = false;
      if (launched?.browser) await launched.browser.close();
    }
  });
}

async function detectWeeksOnStartup() {
  return withScrapeLock(async () => {
    let launched;
    try {
      launched = await launchBrowser();
      const detected = await detectAvailableWeeks(launched.page);
      updateEffectiveWeeksCap(detected);
    } catch (e) {
      console.error('week detection failed:', e.message);
      effectiveWeeksAhead = SCRAPE_WEEKS_AHEAD;
    } finally {
      if (launched?.browser) await launched.browser.close();
    }
  });
}

// ── REST API ─────────────────────────────────────────────────────────────────
function statusPayload() {
  return {
    sessions,
    watchList,
    history,
    lastCheck: lastSuccessfulScrape || lastCheck,
    ntfyOk: !!TOPIC,
    scrapeMeta: {
      weeksAvailableOnSite,
      effectiveWeeksAhead,
      scrapeWeeksAhead: SCRAPE_WEEKS_AHEAD,
      lastTierRun: { ...lastTierRun },
      persistentCache: PERSISTENT_CACHE,
      snapshotPath: SNAPSHOT_PATH,
    },
    ...getStatusFields(),
  };
}

app.get('/api/status', (_req, res) => {
  res.json(statusPayload());
});

app.get('/api/sessions', (_req, res) => {
  res.json(statusPayload());
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

// ── Boot: tiered cron schedules ───────────────────────────────────────────────
function bootstrapInBackground() {
  detectWeeksOnStartup()
    .then(() => runTierScrape(1))
    .then(() => {
      setTimeout(() => runTierScrape(2).catch(console.error), 30_000);
      setTimeout(() => runTierScrape(3).catch(console.error), 90_000);
      setTimeout(() => runTierScrape(4).catch(console.error), 180_000);
    })
    .catch(console.error);
}

cron.schedule(`*/${CHECK_MINS} * * * *`, () => runTierScrape(1).catch(console.error));
cron.schedule('*/30 * * * *', () => runTierScrape(2).catch(console.error));
cron.schedule('0 */6 * * *', () => runTierScrape(3).catch(console.error));
cron.schedule('0 0 * * *', () => {
  detectWeeksOnStartup()
    .then(() => runTierScrape(4))
    .catch(console.error);
});

async function startServer() {
  logPersistentCacheWarning();
  await loadSnapshot();

  app.listen(PORT, () => {
    console.log(`\nAP Session Watcher running on :${PORT}`);
    console.log(`Tier 1 (today/tomorrow + slots): every ${CHECK_MINS} min`);
    console.log('Tier 2 (this week):              every 30 min');
    console.log('Tier 3 (weeks 2–3):              every 6 hours');
    console.log('Tier 4 (weeks 4+):               daily at midnight');
    console.log(`Lookahead: ${SCRAPE_WEEKS_AHEAD} weeks (capped by site availability)`);
    console.log(TOPIC ? `Ntfy topic: ${TOPIC}` : 'WARNING: NTFY_TOPIC not set — no notifications will be sent');
    if (sessions.length) {
      console.log(`Serving ${sessions.length} cached sessions while background scrape runs…`);
    }
    bootstrapInBackground();
  });
}

startServer().catch(console.error);
