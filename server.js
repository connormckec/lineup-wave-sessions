'use strict';
const express  = require('express');
const { chromium } = require('playwright');
const cron     = require('node-cron');
const path     = require('path');
const { createClient } = require('@supabase/supabase-js');
const WebSocket = require('ws');

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
const SUPABASE_URL              = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const EXCLUDED_LEVELS    = ['Cabanas', 'Beach Pass'];
const EXCLUDED_WAVES     = [5, 6];
const BOOKING_TZ = 'America/New_York';

let lastWeeksScraped = 0;

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
let lastScrapeErrorStack = null;
let scrapeInProgress   = false;
let hasFreshScrapeThisBoot = false;
let dataSource = 'memory';
let supabaseConfigured = false;
let supabase = null;
let supabaseInitError = null;
let slotChecksThisCycle = 0;
let weeksAvailableOnSite = null; // detected from booking UI
let effectiveWeeksAhead  = SCRAPE_WEEKS_AHEAD;
const datesCheckedDuringScrape = new Set();
const lastTierRun = { 1: null, 2: null, 3: null, 4: null };
let scrapeLock = Promise.resolve();

function initSupabaseClient() {
  supabaseInitError = null;
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    supabaseConfigured = false;
    supabase = null;
    console.log('Supabase persistence disabled');
    return null;
  }
  try {
    supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
      realtime: {
        transport: WebSocket,
      },
    });
    supabaseConfigured = true;
    console.log('Supabase persistence enabled');
    return supabase;
  } catch (e) {
    supabaseConfigured = false;
    supabase = null;
    supabaseInitError = e.message;
    console.error('Supabase client init failed:', e.message);
    return null;
  }
}

function buildScrapeMetaPayload() {
  return {
    weeksAvailableOnSite,
    effectiveWeeksAhead,
    scrapeWeeksAhead: SCRAPE_WEEKS_AHEAD,
    lastTierRun: { ...lastTierRun },
    slotCache,
    weeksScraped: lastWeeksScraped,
    datesCheckedDuringScrape: [...datesCheckedDuringScrape],
  };
}

function applyLoadedSnapshot(snapSessions, meta, loadedAt) {
  if (Array.isArray(snapSessions) && snapSessions.length) {
    sessionsByKey.clear();
    for (const s of snapSessions) sessionsByKey.set(s.key, s);
    rebuildSessionsArray();
  }
  if (meta) {
    if (meta.weeksAvailableOnSite != null) weeksAvailableOnSite = meta.weeksAvailableOnSite;
    if (meta.effectiveWeeksAhead != null) effectiveWeeksAhead = meta.effectiveWeeksAhead;
    if (meta.lastTierRun) Object.assign(lastTierRun, meta.lastTierRun);
    if (meta.slotCache && typeof meta.slotCache === 'object') slotCache = meta.slotCache;
    if (meta.weeksScraped != null) lastWeeksScraped = meta.weeksScraped;
    if (Array.isArray(meta.datesCheckedDuringScrape)) {
      datesCheckedDuringScrape.clear();
      for (const d of meta.datesCheckedDuringScrape) datesCheckedDuringScrape.add(d);
    }
  }
  if (loadedAt) {
    lastSuccessfulScrape = loadedAt;
    lastCheck = loadedAt;
  }
}

function asSessionArray(value) {
  return Array.isArray(value) ? value : [];
}

function stackPreview(stack, maxLines = 6) {
  if (!stack) return null;
  return stack.split('\n').slice(0, maxLines).join('\n');
}

function recordScrapeError(e, context = 'scrape') {
  lastScrapeError = e?.message || String(e);
  lastScrapeErrorStack = e?.stack || null;
  console.error(`${context} failed:`, lastScrapeError);
  if (lastScrapeErrorStack) console.error(lastScrapeErrorStack);
}

function getStatusFields() {
  const dataAgeMinutes = lastSuccessfulScrape
    ? Math.max(0, Math.round((Date.now() - new Date(lastSuccessfulScrape).getTime()) / 60000))
    : null;
  return {
    sessionsCount: sessions.length,
    source: dataSource,
    supabaseConfigured,
    supabaseInitError,
    isUsingCachedData: sessions.length > 0 && !hasFreshScrapeThisBoot,
    lastSuccessfulScrape,
    lastScrapeAttempt,
    lastScrapeError,
    lastScrapeErrorStackPreview: stackPreview(lastScrapeErrorStack),
    dataAgeMinutes,
    scrapeInProgress,
  };
}

async function loadLatestSnapshotFromSupabase() {
  if (!supabase) return false;
  try {
    const { data, error } = await supabase
      .from('scrape_snapshots')
      .select('*')
      .eq('id', 'latest')
      .maybeSingle();
    if (error) throw error;
    if (!data?.sessions?.length) {
      console.log('  Supabase: no cached snapshot with sessions');
      return false;
    }

    applyLoadedSnapshot(data.sessions, data.scrape_meta, data.last_successful_scrape);
    lastScrapeAttempt = data.last_scrape_attempt || null;
    lastScrapeError = data.last_scrape_error || null;
    dataSource = 'supabase-cache';
    hasFreshScrapeThisBoot = false;

    const age = lastSuccessfulScrape
      ? Math.round((Date.now() - new Date(lastSuccessfulScrape).getTime()) / 60000)
      : '?';
    console.log(`  Supabase: loaded ${sessions.length} cached sessions (saved ${age}m ago)`);
    return true;
  } catch (e) {
    console.error('  Supabase load failed:', e.message);
    return false;
  }
}

async function saveLatestSnapshotToSupabase() {
  if (!supabase) return;
  try {
    const now = new Date().toISOString();
    const { error } = await supabase.from('scrape_snapshots').upsert({
      id: 'latest',
      sessions,
      scrape_meta: buildScrapeMetaPayload(),
      last_successful_scrape: lastSuccessfulScrape,
      last_scrape_attempt: lastScrapeAttempt || lastSuccessfulScrape || now,
      last_scrape_error: null,
      updated_at: now,
    }, { onConflict: 'id' });
    if (error) throw error;
    console.log(`  Supabase: saved snapshot (${sessions.length} sessions)`);
  } catch (e) {
    console.error('  Supabase save failed:', e.message);
  }
}

async function saveScrapeErrorToSupabase(errorMessage) {
  if (!supabase) return;
  const now = new Date().toISOString();
  lastScrapeAttempt = now;
  try {
    const { data: existing } = await supabase
      .from('scrape_snapshots')
      .select('id')
      .eq('id', 'latest')
      .maybeSingle();

    if (existing) {
      const { error } = await supabase.from('scrape_snapshots').update({
        last_scrape_attempt: now,
        last_scrape_error: errorMessage,
        updated_at: now,
      }).eq('id', 'latest');
      if (error) throw error;
      console.log('  Supabase: recorded scrape error');
    } else {
      console.log('  Supabase: scrape error logged locally (no snapshot row to update yet)');
    }
  } catch (e) {
    console.error('  Supabase error save failed:', e.message);
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
// Dates derive from the tile unix timestamp (Atlantic Park local time via browser TZ).
function scrapeVisibleSessions({ excludedLevels = [], excludedWaves = [], weekOffset = 0 } = {}) {
  const WAVE_SIDES = {
    1: 'Right Wave', 2: 'Left Wave', 3: 'Right Lesson', 4: 'Left Lesson',
  };
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
    if (seen.has(key)) return;
    seen.add(key);
    const fm = t.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
    const d  = new Date(ts * 1000);
    const today = new Date(), tom = new Date(today);
    tom.setDate(tom.getDate() + 1);
    const isoDate = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    const displayDate = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
    const weekday = d.toLocaleDateString('en-US', { weekday: 'long' });
    let dayLabel;
    if (d.toDateString() === today.toDateString())     dayLabel = 'Today';
    else if (d.toDateString() === tom.toDateString())  dayLabel = 'Tomorrow';
    else dayLabel = displayDate;
    out.push({
      key, ts, wave, level,
      available : !cls.includes('expired_timeslot'),
      time      : fm ? fm[1].trim() : '?',
      date      : displayDate,
      dayLabel,
      dateKey   : isoDate,
      isoDate,
      displayDate,
      weekday,
      waveSide  : WAVE_SIDES[wave] || `Wave ${wave}`,
      sessionType: level,
      weekOffset,
    });
  });
  return { sessions: out, rawCount, duplicateSkips: rawCount - out.length };
}

async function getCalendarFingerprint(page) {
  return page.evaluate(() => {
    const timestamps = [];
    document.querySelectorAll('div.dynamic-cal-booking-ts').forEach(el => {
      const m = el.className.match(/booking-agenda-clickable_(\d+)_/);
      if (m) timestamps.push(+m[1]);
    });
    timestamps.sort((a, b) => a - b);
    const labelEl = document.querySelector(
      '.dynamic-cal-booking-date, .booking-agenda-date, [class*="calendar-title"], [class*="agenda-title"], .panel-heading h4, .panel-title'
    );
    const label = labelEl?.textContent?.replace(/\s+/g, ' ').trim() || '';
    return {
      label,
      count: timestamps.length,
      firstTs: timestamps[0] || null,
      lastTs: timestamps[timestamps.length - 1] || null,
      sig: timestamps.slice(0, 8).join(','),
    };
  });
}

async function canAdvanceCalendar(page) {
  const chevron = page.locator('.glyphicon-chevron-right').first();
  if (!await chevron.count()) return false;
  return chevron.evaluate(el =>
    !el.classList.contains('disabled') &&
    !el.closest('.disabled') &&
    window.getComputedStyle(el).visibility !== 'hidden' &&
    window.getComputedStyle(el).display !== 'none'
  ).catch(() => false);
}

// Click the booking calendar next-week chevron and verify the DOM updated.
async function advanceCalendarWeek(page) {
  const before = await getCalendarFingerprint(page);
  const chevron = page.locator('.glyphicon-chevron-right').first();
  if (!await chevron.count()) {
    console.log('  next-week arrow not found');
    return false;
  }
  const clickable = await canAdvanceCalendar(page);
  if (!clickable) {
    console.log('  next-week arrow present but not clickable');
    return false;
  }

  console.log(`  clicking next-week arrow (currently: "${before.label || 'n/a'}", ${before.count} tiles)`);
  await chevron.click();
  await page.waitForTimeout(2000);

  try {
    await page.waitForFunction(
      (prevSig) => {
        const timestamps = [];
        document.querySelectorAll('div.dynamic-cal-booking-ts').forEach(el => {
          const m = el.className.match(/booking-agenda-clickable_(\d+)_/);
          if (m) timestamps.push(+m[1]);
        });
        timestamps.sort((a, b) => a - b);
        const sig = timestamps.slice(0, 8).join(',');
        return sig !== prevSig || timestamps.length === 0;
      },
      before.sig,
      { timeout: 12_000 }
    );
  } catch {
    console.log('  calendar tiles did not change within timeout after next-week click');
    return false;
  }

  const after = await getCalendarFingerprint(page);
  if (after.sig === before.sig && after.firstTs === before.firstTs && after.count > 0) {
    console.log('  calendar unchanged after next-week click — pagination stopped');
    return false;
  }

  console.log(`  calendar advanced → "${after.label || 'n/a'}", ${after.count} tiles`);
  return true;
}

async function scrapePaginatedWeeks(page, startWeek, endWeek, { requiredDates = null } = {}) {
  const allByKey = new Map();
  const datesSeen = new Set();
  let rawTilesTotal = 0;
  let duplicateSkipsTotal = 0;
  let weeksScraped = 0;

  console.log(`  paginating weeks ${startWeek}–${endWeek} (0 = initial visible week)`);

  for (let w = 0; w < startWeek; w++) {
    if (!await advanceCalendarWeek(page)) {
      console.log(`  failed to reach week offset ${startWeek} (stopped at ${w})`);
      return { sessions: [], weeksScraped: 0, rawTilesTotal, duplicateSkipsTotal, datesSeen };
    }
  }

  const seenSigs = new Set();
  for (let weekOffset = startWeek; weekOffset <= endWeek; weekOffset++) {
    const fp = await getCalendarFingerprint(page);
    if (seenSigs.has(fp.sig) && fp.count > 0) {
      console.log(`  [week offset ${weekOffset}] repeated calendar fingerprint — stopping pagination`);
      break;
    }
    seenSigs.add(fp.sig);

    const { pageSessions, visible, rawCount, added } =
      await absorbVisibleSessions(page, allByKey, datesSeen, weekOffset);
    rawTilesTotal += rawCount;
    duplicateSkipsTotal += Math.max(0, rawCount - pageSessions.length);

    const dateRange = visible.length
      ? `${visible[0]} → ${visible[visible.length - 1]}`
      : 'none';

    console.log(
      `  [week offset ${weekOffset}] label="${fp.label || 'n/a'}" ` +
      `tiles=${rawCount} parsed=${pageSessions.length} new=${added} ` +
      `cumulative=${allByKey.size} dates=${dateRange}`
    );
    weeksScraped++;

    if (requiredDates?.length && requiredDates.every(d => datesSeen.has(d))) {
      console.log('  all required tier dates seen on calendar');
      break;
    }

    if (weekOffset < endWeek) {
      const hasNext = await canAdvanceCalendar(page);
      if (!hasNext) {
        console.log(`  [week offset ${weekOffset}] no next-week arrow — end of calendar`);
        break;
      }
      const advanced = await advanceCalendarWeek(page);
      if (!advanced) break;
    }
  }

  if (requiredDates?.length) {
    const stillMissing = requiredDates.filter(d => !datesSeen.has(d));
    if (stillMissing.length) {
      await fillMissingDates(page, stillMissing, allByKey, datesSeen);
    }
  }

  lastWeeksScraped = Math.max(lastWeeksScraped, weeksScraped);
  return {
    sessions: [...allByKey.values()],
    weeksScraped,
    rawTilesTotal,
    duplicateSkipsTotal,
    datesSeen,
  };
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
  for (const s of asSessionArray(batch)) {
    if (!byKey.has(s.key)) byKey.set(s.key, { ...s });
    else byKey.get(s.key).available = s.available;
  }
  return [...byKey.values()];
}

function daysFromToday(dateKey) {
  const today = parseDateKey(todayDateKey());
  const d = parseDateKey(dateKey);
  return Math.round((d - today) / 86_400_000);
}

function dateKeyFromDate(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function todayDateKey() {
  return dateKeyInBookingTz(new Date());
}

function dateKeyInBookingTz(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: BOOKING_TZ }).format(date);
}

function parseDateKey(dateKey) {
  const [y, m, d] = dateKey.split('-').map(Number);
  return new Date(y, m - 1, d, 12, 0, 0, 0);
}

function enumerateDateKeys(fromKey, toKey) {
  const out = [];
  const cur = parseDateKey(fromKey);
  const end = parseDateKey(toKey);
  while (cur <= end) {
    out.push(dateKeyFromDate(cur));
    cur.setDate(cur.getDate() + 1);
  }
  return out;
}

function expectedDatesInScrapeWindow() {
  const dates = [];
  const start = parseDateKey(todayDateKey());
  const count = effectiveWeeksAhead * 7;
  for (let i = 0; i < count; i++) {
    const d = new Date(start);
    d.setDate(start.getDate() + i);
    dates.push(dateKeyFromDate(d));
  }
  return dates;
}

function expectedDatesForTier(tier) {
  return expectedDatesInScrapeWindow().filter((dateKey) => {
    const days = daysFromToday(dateKey);
    return days >= TIER_CONFIG[tier].minDay && days <= tierMaxDay(tier);
  });
}

function markCalendarSpanChecked(datesSeen, fromKey, toKey) {
  for (const d of enumerateDateKeys(fromKey, toKey)) {
    datesSeen.add(d);
    datesCheckedDuringScrape.add(d);
  }
}

async function getVisibleDateKeysFromPage(page) {
  return page.evaluate(() => {
    const keys = new Set();
    document.querySelectorAll('div.dynamic-cal-booking-ts').forEach(el => {
      const m = el.className.match(/booking-agenda-clickable_(\d+)_/);
      if (m) {
        const d = new Date(+m[1] * 1000);
        keys.add(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`);
      }
    });
    document.querySelectorAll('[data-date]').forEach(el => {
      const attr = el.getAttribute('data-date');
      if (attr && /^\d{4}-\d{2}-\d{2}$/.test(attr)) keys.add(attr);
    });
    return [...keys].sort();
  });
}

async function absorbVisibleSessions(page, allByKey, datesSeen, weekOffset = 0) {
  const result = await page.evaluate(scrapeVisibleSessions, { ...SCRAPE_OPTS, weekOffset });
  const pageSessions = asSessionArray(result?.sessions);
  const visible = await getVisibleDateKeysFromPage(page);
  if (visible.length) markCalendarSpanChecked(datesSeen, visible[0], visible[visible.length - 1]);
  let added = 0;
  for (const s of pageSessions) {
    if (!allByKey.has(s.key)) {
      allByKey.set(s.key, s);
      added++;
    }
  }
  return { pageSessions, visible, rawCount: result?.rawCount || 0, added };
}

async function retreatCalendarWeek(page) {
  const before = await getCalendarFingerprint(page);
  const chevron = page.locator('.glyphicon-chevron-left').first();
  if (!await chevron.count()) return false;
  const clickable = await chevron.evaluate(el =>
    !el.classList.contains('disabled') &&
    !el.closest('.disabled') &&
    window.getComputedStyle(el).visibility !== 'hidden' &&
    window.getComputedStyle(el).display !== 'none'
  ).catch(() => false);
  if (!clickable) return false;

  await chevron.click();
  await page.waitForTimeout(2000);
  try {
    await page.waitForFunction(
      (prevSig) => {
        const timestamps = [];
        document.querySelectorAll('div.dynamic-cal-booking-ts').forEach(el => {
          const m = el.className.match(/booking-agenda-clickable_(\d+)_/);
          if (m) timestamps.push(+m[1]);
        });
        timestamps.sort((a, b) => a - b);
        return timestamps.slice(0, 8).join(',') !== prevSig || timestamps.length === 0;
      },
      before.sig,
      { timeout: 12_000 }
    );
  } catch {
    return false;
  }
  const after = await getCalendarFingerprint(page);
  if (after.sig === before.sig && after.firstTs === before.firstTs && after.count > 0) return false;
  console.log(`  calendar retreated → "${after.label || 'n/a'}", ${after.count} tiles`);
  return true;
}

async function navigateToWeekOffset(page, weekOffset) {
  await openBookingPage(page);
  for (let w = 0; w < weekOffset; w++) {
    if (!await advanceCalendarWeek(page)) return false;
  }
  return true;
}

async function fillMissingDates(page, missingDates, allByKey, datesSeen) {
  if (!missingDates.length) return;
  console.log(`  filling ${missingDates.length} missing date(s): ${missingDates.join(', ')}`);

  for (const dateKey of missingDates) {
    if (datesSeen.has(dateKey)) continue;

    const baseWeek = Math.max(0, Math.floor(daysFromToday(dateKey) / 7));
    const offsetsToTry = [...new Set([
      baseWeek, baseWeek - 1, baseWeek + 1, baseWeek + 2, baseWeek - 2,
      0, 1, 2, 3, effectiveWeeksAhead - 1,
    ].filter(w => w >= 0 && w < effectiveWeeksAhead))];

    let found = false;
    for (const weekOffset of offsetsToTry) {
      if (datesSeen.has(dateKey)) break;
      if (!await navigateToWeekOffset(page, weekOffset)) continue;
      const { visible } = await absorbVisibleSessions(page, allByKey, datesSeen, weekOffset);
      if (visible.includes(dateKey)) {
        console.log(`  ✓ ${dateKey} found at week offset ${weekOffset}`);
        found = true;
        break;
      }
    }

    if (!found) {
      await openBookingPage(page);
      const seenSigs = new Set();
      for (let step = 0; step < effectiveWeeksAhead + 2; step++) {
        const fp = await getCalendarFingerprint(page);
        if (seenSigs.has(fp.sig) && fp.count > 0) break;
        seenSigs.add(fp.sig);
        const { visible } = await absorbVisibleSessions(page, allByKey, datesSeen, step);
        if (visible.includes(dateKey)) {
          console.log(`  ✓ ${dateKey} found during forward sweep step ${step}`);
          found = true;
          break;
        }
        if (!await advanceCalendarWeek(page)) break;
      }
    }

    if (!found) {
      await navigateToWeekOffset(page, Math.max(0, effectiveWeeksAhead - 1));
      for (let step = 0; step < effectiveWeeksAhead + 2; step++) {
        const { visible } = await absorbVisibleSessions(page, allByKey, datesSeen, step);
        if (visible.includes(dateKey)) {
          console.log(`  ✓ ${dateKey} found during backward sweep step ${step}`);
          found = true;
          break;
        }
        if (!await retreatCalendarWeek(page)) break;
      }
    }

    if (!found) {
      console.log(`  ⚠ ${dateKey} not found on calendar after fill attempts (0 sessions)`);
    }
    datesSeen.add(dateKey);
    datesCheckedDuringScrape.add(dateKey);
  }
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

function weeksForTier(tier) {
  const cfg = TIER_CONFIG[tier];
  const startWeek = Math.floor(cfg.minDay / 7);
  let endWeek = Math.min(Math.floor(tierMaxDay(tier) / 7), effectiveWeeksAhead - 1);
  endWeek = Math.min(endWeek + 1, effectiveWeeksAhead - 1);
  return { startWeek, endWeek: Math.max(startWeek, endWeek) };
}

function computeDateCoverage() {
  const expected = expectedDatesInScrapeWindow();
  const dateKeys = [...new Set(sessions.map(s => s.dateKey).filter(Boolean))].sort();
  const sessionsByDate = {};
  const sessionsByDateAndSide = {};
  for (const s of sessions) {
    if (!s.dateKey) continue;
    sessionsByDate[s.dateKey] = (sessionsByDate[s.dateKey] || 0) + 1;
    const side = s.waveSide || `Wave ${s.wave}`;
    if (!sessionsByDateAndSide[s.dateKey]) sessionsByDateAndSide[s.dateKey] = {};
    sessionsByDateAndSide[s.dateKey][side] = (sessionsByDateAndSide[s.dateKey][side] || 0) + 1;
  }
  const checkedDates = expected.filter(d => datesCheckedDuringScrape.has(d));
  const missingDatesInScrapeWindow = expected.filter(d => !datesCheckedDuringScrape.has(d));
  const datesWithSessions = expected.filter(d => (sessionsByDate[d] || 0) > 0);
  const expectedDatesCount = expected.length;
  const coveredDatesCount = checkedDates.length;
  const coveragePercent = expectedDatesCount
    ? Math.round((coveredDatesCount / expectedDatesCount) * 100)
    : 0;

  if (missingDatesInScrapeWindow.length) {
    console.log(`  ⚠ missingDatesInScrapeWindow (${missingDatesInScrapeWindow.length}): ${missingDatesInScrapeWindow.join(', ')}`);
  }

  return {
    earliestSessionDate: dateKeys[0] || null,
    latestSessionDate: dateKeys[dateKeys.length - 1] || null,
    uniqueDatesCount: dateKeys.length,
    sessionsByDate,
    sessionsByDateAndSide,
    expectedDatesCount,
    coveredDatesCount,
    coveragePercent,
    missingDatesInScrapeWindow,
    datesCheckedDuringScrape: [...datesCheckedDuringScrape].sort(),
    datesWithSessionsCount: datesWithSessions.length,
    weeksScraped: lastWeeksScraped,
  };
}

function filterBatchForTier(batch, tier) {
  return asSessionArray(batch).filter(s => sessionInTier(s, tier));
}

function rebuildSessionsArray() {
  const maxDay = effectiveWeeksAhead * 7;
  sessions = asSessionArray([...sessionsByKey.values()])
    .filter(s => {
      const days = daysFromToday(s.dateKey);
      return days >= 0 && days < maxDay;
    })
    .sort((a, b) => a.ts - b.ts || a.wave - b.wave);
}

function mergeBatchIntoStore(batch, tier, { preserveSlots = true } = {}) {
  const now = new Date().toISOString();
  const updatedKeys = [];

  for (const raw of asSessionArray(batch)) {
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
    timezoneId: BOOKING_TZ,
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

  async function absorbWeek(weekOffset) {
    const result = await page.evaluate(scrapeVisibleSessions, { ...SCRAPE_OPTS, weekOffset });
    const batch = asSessionArray(result?.sessions);
    let added = 0;
    for (const s of batch) {
      if (!seenKeys.has(s.key)) { seenKeys.add(s.key); added++; }
    }
    return added;
  }

  await absorbWeek(0);

  while (weeks < SCRAPE_WEEKS_AHEAD + 1) {
    if (!await canAdvanceCalendar(page)) break;
    if (!await advanceCalendarWeek(page)) break;
    weeks++;
    const added = await absorbWeek(weeks - 1);
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
    const { startWeek, endWeek } = weeksForTier(tier);
    if (endWeek < startWeek || startWeek >= effectiveWeeksAhead) {
      console.log(`[tier ${tier}] skipped — no weeks in range (offsets ${startWeek}–${endWeek}, effective=${effectiveWeeksAhead})`);
      return;
    }

    console.log(`\n[${new Date().toLocaleTimeString()}] Tier ${tier} scrape (${cfg.label}, week offsets ${startWeek}–${endWeek})`);

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

      const tierRequiredDates = expectedDatesForTier(tier);
      const { sessions: rawBatch, rawTilesTotal, weeksScraped, datesSeen } =
        await scrapePaginatedWeeks(page, startWeek, endWeek, { requiredDates: tierRequiredDates });

      if (datesSeen) {
        for (const d of datesSeen) datesCheckedDuringScrape.add(d);
      }

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

      console.log(`  tier ${tier} summary: ${rawTilesTotal} tiles, ${weeksScraped} week(s), ${batch.length} in date range, ${updatedKeys.length} updated`);
      const coverage = computeDateCoverage();
      console.log(`  date coverage: ${coverage.earliestSessionDate || '?'} → ${coverage.latestSessionDate || '?'} (${coverage.uniqueDatesCount} days, ${coverage.coveragePercent}% dates checked)`);
      await processNotifications(updatedKeys, { slotsAlerts: cfg.slotCounts });

      lastTierRun[tier] = new Date().toISOString();
      lastSuccessfulScrape = new Date().toISOString();
      lastCheck = lastSuccessfulScrape;
      lastScrapeError = null;
      lastScrapeErrorStack = null;
      hasFreshScrapeThisBoot = true;
      dataSource = 'memory';
      await saveLatestSnapshotToSupabase();

    } catch (e) {
      recordScrapeError(e, `tier ${tier} scrape`);
      await saveScrapeErrorToSupabase(lastScrapeError);
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
  const dateCoverage = computeDateCoverage();
  return {
    sessions: asSessionArray(sessions),
    watchList: asSessionArray(watchList),
    history: history || {},
    lastCheck: lastSuccessfulScrape || lastCheck,
    ntfyOk: !!TOPIC,
    ...dateCoverage,
    scrapeMeta: {
      weeksAvailableOnSite,
      effectiveWeeksAhead,
      scrapeWeeksAhead: SCRAPE_WEEKS_AHEAD,
      lastTierRun: { ...lastTierRun },
      supabaseConfigured,
      ...dateCoverage,
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

app.get('/api/debug/scrape', (_req, res) => {
  const coverage = computeDateCoverage();
  res.json({
    scrapeInProgress,
    sessionsCount: sessions.length,
    lastScrapeAttempt,
    lastScrapeError,
    lastScrapeErrorStackPreview: stackPreview(lastScrapeErrorStack),
    lastSuccessfulScrape,
    dataSource,
    supabaseConfigured,
    effectiveWeeksAhead,
    lastTierRun: { ...lastTierRun },
    lastWeeksScraped,
    ...coverage,
  });
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
      setTimeout(() => runTierScrape(3).catch(console.error), 45_000);
      setTimeout(() => runTierScrape(4).catch(console.error), 120_000);
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

async function startBackgroundServices() {
  initSupabaseClient();
  if (supabase) {
    try {
      await loadLatestSnapshotFromSupabase();
    } catch (e) {
      supabaseInitError = supabaseInitError || e.message;
      console.error('Supabase cache load failed:', e.message);
    }
  }
  if (sessions.length) {
    console.log(`Serving ${sessions.length} cached sessions (${dataSource}) while background scrape runs…`);
  }
  bootstrapInBackground();
}

function startServer() {
  app.listen(PORT, () => {
    console.log(`\nAP Session Watcher running on :${PORT}`);
    console.log(`Tier 1 (today/tomorrow + slots): every ${CHECK_MINS} min`);
    console.log('Tier 2 (this week):              every 30 min');
    console.log('Tier 3 (weeks 2–3):              every 6 hours');
    console.log('Tier 4 (weeks 4+):               daily at midnight');
    console.log(`Lookahead: ${SCRAPE_WEEKS_AHEAD} weeks (capped by site availability)`);
    console.log(TOPIC ? `Ntfy topic: ${TOPIC}` : 'WARNING: NTFY_TOPIC not set — no notifications will be sent');
    startBackgroundServices().catch((e) => {
      console.error('Background startup error:', e.message);
      bootstrapInBackground();
    });
  });
}

startServer();
