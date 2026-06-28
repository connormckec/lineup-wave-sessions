'use strict';
const express  = require('express');
const { chromium } = require('playwright');
const cron     = require('node-cron');
const path     = require('path');
const crypto   = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const WebSocket = require('ws');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT       = process.env.PORT || 3000;
const TOPIC      = process.env.NTFY_TOPIC || '';
const INTERNAL_BETA = process.env.INTERNAL_BETA_NOTIFICATIONS === 'true';
// ntfy is internal demo infrastructure only — public MVP should use native push, web push, SMS, or email after login.
const INTERNAL_DEFAULT_NTFY_TOPIC = 'ap-surf-connor-2026';
const THRESH     = parseInt(process.env.LOW_SLOTS_THRESHOLD || '2');
const BOOKING    = 'https://booking.atlanticparksurf.com/activity-agenda';
const APP_URL    = process.env.APP_URL || BOOKING;
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
let watchItems      = [];   // active watchlist rows (all users), synced from Supabase
const lastAlertState = new Map(); // `${userKey}:${sessionKey}:${eventType}` -> { slots, available, at }
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
let lastHistorySnapshotSavedAt = null;
let lastLatestSnapshotSavedAt = null;
let lastSnapshotRowsInsertedLastRun = 0;
const HISTORY_SNAPSHOTS_ENABLED = process.env.HISTORY_SNAPSHOTS !== 'false';
const PARK = 'atlantic_park';
const serverStartedAt = new Date().toISOString();
const scrapeScheduleEnabled = true;
const ambiguousSideMappings = [];
const recentSideParseLogs = [];
const MAX_SIDE_PARSE_LOGS = 40;

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

function normalizeDataSource(src) {
  if (src === 'supabase-current' || src === 'supabase-cache' || src === 'supabase') return 'supabase';
  if (src === 'memory' && supabaseConfigured) return 'supabase';
  return 'memory-fallback';
}

function currentRowToSession(row) {
  const raw = row.raw && typeof row.raw === 'object' ? row.raw : {};
  const capacity = row.capacity ?? raw.capacity ?? null;
  const slots = row.slots_available ?? raw.slots ?? null;
  let estimatedBooked = row.estimated_booked ?? raw.estimatedBooked ?? null;
  let fillRate = row.fill_rate ?? raw.fillRate ?? null;
  if (capacity != null && slots != null && estimatedBooked == null) {
    estimatedBooked = capacity - slots;
    fillRate = estimatedBooked / capacity;
  }
  return {
    ...raw,
    key: row.session_key,
    dateKey: row.iso_date || raw.dateKey,
    isoDate: row.iso_date || raw.isoDate,
    ts: row.start_ts ?? raw.ts,
    time: row.start_time || raw.time,
    date: row.display_date || raw.date,
    weekday: row.weekday || raw.weekday,
    waveSide: row.wave_side || raw.waveSide,
    level: row.session_type || raw.level,
    available: row.available,
    slots,
    capacity,
    estimatedBooked,
    fillRate,
    priceText: row.price_text ?? raw.priceText ?? null,
    priceMin: row.price_min ?? raw.priceMin ?? null,
    priceMax: row.price_max ?? raw.priceMax ?? null,
    currency: row.currency ?? raw.currency ?? 'USD',
    tier: row.source_tier ?? raw.tier,
    lastScraped: row.last_scraped_at || raw.lastScraped,
  };
}

function sessionToCurrentRow(s, sourceTier) {
  const now = new Date().toISOString();
  const capacity = s.capacity ?? sessionCapacityForLevel(s.level);
  const slots = s.slots ?? null;
  let estimatedBooked = s.estimatedBooked ?? null;
  let fillRate = s.fillRate ?? null;
  if (capacity != null && slots != null && estimatedBooked == null) {
    estimatedBooked = capacity - slots;
    fillRate = estimatedBooked / capacity;
  }
  return {
    park: PARK,
    session_key: s.key,
    iso_date: s.isoDate || s.dateKey || null,
    start_ts: s.ts ?? null,
    start_time: s.time || null,
    display_date: s.date || null,
    weekday: s.weekday || null,
    wave_side: s.waveSide || null,
    session_type: s.level || null,
    available: s.available,
    slots_available: slots,
    capacity,
    estimated_booked: estimatedBooked,
    fill_rate: fillRate,
    price_text: s.priceText ?? null,
    price_min: s.priceMin ?? null,
    price_max: s.priceMax ?? null,
    currency: s.currency || 'USD',
    status_label: availabilityStatusLabel(s),
    source_tier: sourceTier,
    raw: s,
    last_seen_at: now,
    last_scraped_at: now,
  };
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
  const coverage = computeDateCoverage();
  return {
    sessionsCount: sessions.length,
    currentSessionsCount: sessions.length,
    source: normalizeDataSource(dataSource),
    dataSource: normalizeDataSource(dataSource),
    supabaseConfigured,
    supabaseInitError,
    isUsingCachedData: sessions.length > 0 && !hasFreshScrapeThisBoot,
    lastSuccessfulScrape,
    lastScrapeAttempt,
    lastScrapeError,
    lastScrapeErrorStackPreview: stackPreview(lastScrapeErrorStack),
    dataAgeMinutes,
    scrapeInProgress,
    totalSessionsTracked: sessions.length,
    latestSnapshotSavedAt: lastLatestSnapshotSavedAt,
    historySnapshotsEnabled: supabaseConfigured && HISTORY_SNAPSHOTS_ENABLED,
    snapshotRowsInsertedLastRun: lastSnapshotRowsInsertedLastRun,
    minutesSinceLastScrape: dataAgeMinutes,
    missingDatesInScrapeWindow: coverage.missingDatesInScrapeWindow,
    coveragePercent: coverage.coveragePercent,
    scrapeScheduleEnabled,
    serverStartedAt,
  };
}

async function ensureSessionsForStatus() {
  if (sessions.length > 0) return;
  if (!supabase) return;
  const loaded = await loadCurrentSessionsFromSupabase();
  if (!loaded) await loadLatestSnapshotFromSupabase();
}

function waveSideSlug(side) {
  return (side || 'unknown').toLowerCase().replace(/\s+/g, '-');
}

function logWaveSideParse(session) {
  if (!session?.key) return;
  const entry = {
    iso_date: session.isoDate || session.dateKey,
    time: session.time,
    session_type: session.level,
    rawTileText: session.tileText || session.sideParseRaw || null,
    parsed_wave_side: session.waveSide,
    waveSideSource: session.waveSideSource || 'unknown',
    wave: session.wave,
    session_key: session.key,
    sideKey: `${session.ts}_${waveSideSlug(session.waveSide)}`,
  };
  if (session.waveSideAmbiguous) {
    ambiguousSideMappings.push(entry);
    if (ambiguousSideMappings.length > 50) ambiguousSideMappings.shift();
  }
  recentSideParseLogs.push(entry);
  if (recentSideParseLogs.length > MAX_SIDE_PARSE_LOGS) recentSideParseLogs.shift();
  if (process.env.DEBUG_WAVE_SIDE === '1' || session.waveSideAmbiguous) {
    console.log(`  [wave side] ${entry.iso_date} ${entry.time} ${entry.session_type} wave=${entry.wave} → ${entry.parsed_wave_side} (${entry.waveSideSource})${session.waveSideAmbiguous ? ' AMBIGUOUS' : ''}`);
  }
}

async function loadScrapeMetaFromSupabase() {
  if (!supabase) return;
  try {
    const { data, error } = await supabase
      .from('scrape_snapshots')
      .select('scrape_meta, last_successful_scrape, last_scrape_attempt, last_scrape_error')
      .eq('id', 'latest')
      .maybeSingle();
    if (error) throw error;
    if (!data) return;
    applyLoadedSnapshot(null, data.scrape_meta, data.last_successful_scrape);
    lastScrapeAttempt = data.last_scrape_attempt || lastScrapeAttempt;
    lastScrapeError = data.last_scrape_error || null;
  } catch (e) {
    console.error('  Supabase scrape meta load failed:', e.message);
  }
}

async function loadCurrentSessionsFromSupabase() {
  if (!supabase) return false;
  try {
    const { data, error } = await supabase
      .from('current_sessions')
      .select('*')
      .eq('park', PARK)
      .order('start_ts', { ascending: true });
    if (error) throw error;
    if (!data?.length) {
      console.log('  Supabase: no rows in current_sessions');
      return false;
    }

    sessionsByKey.clear();
    for (const row of data) {
      const s = currentRowToSession(row);
      if (s.key) sessionsByKey.set(s.key, s);
    }
    rebuildSessionsArray();
    await loadScrapeMetaFromSupabase();

    const latestScraped = data.reduce((max, row) => {
      const t = row.last_scraped_at ? new Date(row.last_scraped_at).getTime() : 0;
      return t > max ? t : max;
    }, 0);
    if (latestScraped) {
      const fromRows = new Date(latestScraped).toISOString();
      if (!lastSuccessfulScrape || new Date(fromRows) > new Date(lastSuccessfulScrape)) {
        lastSuccessfulScrape = fromRows;
        lastCheck = fromRows;
      }
    }

    dataSource = 'supabase';
    hasFreshScrapeThisBoot = false;

    const age = lastSuccessfulScrape
      ? Math.round((Date.now() - new Date(lastSuccessfulScrape).getTime()) / 60000)
      : '?';
    console.log(`  Supabase: loaded ${sessions.length} current session(s) (last scrape ${age}m ago)`);
    return true;
  } catch (e) {
    console.error('  Supabase current_sessions load failed:', e.message);
    return false;
  }
}

async function upsertCurrentSessionsToSupabase(scrapedSessions, sourceTier) {
  if (!supabase) return 0;
  const batch = asSessionArray(scrapedSessions);
  if (!batch.length) return 0;

  try {
    const rows = batch.map(s => sessionToCurrentRow(s, sourceTier));
    let upserted = 0;
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const { error } = await supabase
        .from('current_sessions')
        .upsert(chunk, { onConflict: 'park,session_key' });
      if (error) throw error;
      upserted += chunk.length;
    }
    console.log(`  Supabase: upserted ${upserted} current_sessions row(s)`);
    return upserted;
  } catch (e) {
    console.error('  Supabase current_sessions upsert failed:', e.message);
    return 0;
  }
}

async function beginScrapeRun(tier) {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase
      .from('scrape_runs')
      .insert({ park: PARK, tier: String(tier), started_at: new Date().toISOString() })
      .select('id')
      .single();
    if (error) throw error;
    return data?.id || null;
  } catch (e) {
    console.error('  Supabase scrape_runs insert failed:', e.message);
    return null;
  }
}

async function finishScrapeRun(runId, {
  success,
  sessionsFound = null,
  datesCovered = null,
  missingDates = null,
  coveragePercent = null,
  error = null,
  errorStack = null,
} = {}) {
  if (!supabase || !runId) return;
  try {
    const { error: updateError } = await supabase
      .from('scrape_runs')
      .update({
        finished_at: new Date().toISOString(),
        success,
        sessions_found: sessionsFound,
        dates_covered: datesCovered,
        missing_dates: missingDates,
        coverage_percent: coveragePercent,
        error,
        error_stack: errorStack,
      })
      .eq('id', runId);
    if (updateError) throw updateError;
  } catch (e) {
    console.error('  Supabase scrape_runs update failed:', e.message);
  }
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
    dataSource = 'supabase';
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
    lastLatestSnapshotSavedAt = now;
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

function sessionCapacityForLevel(level) {
  if (level === 'Progressive') return 18;
  if (level === 'Pro Turns') return 10;
  return 12;
}

function parsePriceFromText(text) {
  if (!text) return {};
  const currency = 'USD';
  const rangeMatch = text.match(/\$\s*([\d,]+(?:\.\d{2})?)\s*[–\-]\s*\$\s*([\d,]+(?:\.\d{2})?)/);
  if (rangeMatch) {
    const min = parseFloat(rangeMatch[1].replace(/,/g, ''));
    const max = parseFloat(rangeMatch[2].replace(/,/g, ''));
    return {
      price_text: `$${rangeMatch[1].replace(/,/g, '')}–$${rangeMatch[2].replace(/,/g, '')}`,
      price_min: min,
      price_max: max,
      currency,
    };
  }
  const singleMatch = text.match(/\$\s*([\d,]+(?:\.\d{2})?)/);
  if (singleMatch) {
    const v = parseFloat(singleMatch[1].replace(/,/g, ''));
    return {
      price_text: `$${singleMatch[1].replace(/,/g, '')}`,
      price_min: v,
      price_max: v,
      currency,
    };
  }
  return {};
}

function attachSessionMetrics(entry, details, level) {
  if (!entry) return;
  const slots = entry.slots ?? details?.slots ?? null;
  const capacity = details?.capacity ?? entry.capacity ?? sessionCapacityForLevel(level);
  if (capacity != null && slots != null) {
    entry.capacity = capacity;
    entry.estimatedBooked = capacity - slots;
    entry.fillRate = entry.estimatedBooked / capacity;
  }
  if (details?.price_text) {
    entry.priceText = details.price_text;
    entry.priceMin = details.price_min;
    entry.priceMax = details.price_max;
    entry.currency = details.currency || 'USD';
  }
}

function availabilityStatusLabel(s) {
  if (!s.available) return 'PACKED';
  if (s.slots == null) return 'OPEN';
  if (s.slots >= 10) return 'FIRING';
  if (s.slots >= 5) return 'OPEN';
  if (s.slots >= 3) return 'GETTING_CROWDED';
  return 'CLOSING_OUT';
}

async function saveAvailabilitySnapshotsToSupabase(scrapedSessions, sourceTier) {
  if (!supabase || !HISTORY_SNAPSHOTS_ENABLED) return 0;
  const batch = asSessionArray(scrapedSessions);
  if (!batch.length) return 0;

  try {
    const scrapedAt = new Date().toISOString();
    const rows = batch.map((s) => {
      const capacity = s.capacity ?? sessionCapacityForLevel(s.level);
      const slotsAvailable = s.slots != null ? s.slots : null;
      let estimatedBooked = s.estimatedBooked ?? null;
      let fillRate = s.fillRate ?? null;
      if (capacity != null && slotsAvailable != null && estimatedBooked == null) {
        estimatedBooked = capacity - slotsAvailable;
        fillRate = estimatedBooked / capacity;
      }
      return {
        scraped_at: scrapedAt,
        park: PARK,
        session_key: s.key,
        iso_date: s.isoDate || s.dateKey,
        start_ts: s.ts,
        start_time: s.time,
        weekday: s.weekday || null,
        wave_side: s.waveSide || null,
        session_type: s.level,
        available: s.available,
        slots_available: slotsAvailable,
        capacity,
        estimated_booked: estimatedBooked,
        fill_rate: fillRate,
        price_text: s.priceText ?? null,
        price_min: s.priceMin ?? null,
        price_max: s.priceMax ?? null,
        currency: s.currency || 'USD',
        status_label: availabilityStatusLabel(s),
        source_tier: sourceTier,
        raw: s,
      };
    });

    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const { error } = await supabase.from('availability_snapshots').insert(chunk);
      if (error) throw error;
    }

    lastHistorySnapshotSavedAt = scrapedAt;
    console.log(`  Supabase: saved ${rows.length} availability snapshot row(s)`);
    return rows.length;
  } catch (e) {
    console.error('  Supabase availability snapshots failed:', e.message);
    return 0;
  }
}

function tryAcquireScrapeLock(context = 'scrape') {
  if (scrapeInProgress) {
    console.log(`  ${context} skipped — scrape already running`);
    return false;
  }
  scrapeInProgress = true;
  return true;
}

function releaseScrapeLock() {
  scrapeInProgress = false;
}

// ── Push notification via Ntfy.sh ────────────────────────────────────────────
async function sendNtfy(topic, title, body, { urgent = false, clickUrl = APP_URL } = {}) {
  const cleanTopic = (topic || '').trim();
  if (!cleanTopic) return { ok: false, error: 'no topic' };
  try {
    const r = await fetch(`https://ntfy.sh/${encodeURIComponent(cleanTopic)}`, {
      method: 'POST',
      headers: {
        Title: title,
        Priority: urgent ? 'urgent' : 'high',
        Tags: urgent ? 'wave,exclamation' : 'wave',
        Click: clickUrl,
      },
      body,
    });
    return { ok: r.ok, status: r.status, error: r.ok ? null : `HTTP ${r.status}` };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

function activeWatchItems() {
  return watchItems.filter(w => w.active !== false);
}

function watchedSessionKeys() {
  return new Set(activeWatchItems().map(w => w.session_key));
}

function watchItemToClient(w) {
  const wave = w.wave ?? (w.session_key?.includes('_') ? +w.session_key.split('_').pop() : null);
  return {
    id: w.id,
    key: w.session_key,
    session_key: w.session_key,
    user_key: w.user_key,
    ntfy_topic: w.ntfy_topic,
    ts: w.start_ts,
    wave,
    level: w.session_type,
    session_type: w.session_type,
    wave_side: w.wave_side,
    waveSide: w.wave_side,
    iso_date: w.iso_date,
    time: w.time || w.start_time,
    date: w.date || w.display_date,
    dayLabel: w.day_label,
    alert_when_opens: w.alert_when_opens !== false,
    alert_when_low_slots: w.alert_when_low_slots !== false,
    low_slots_threshold: w.low_slots_threshold ?? THRESH,
  };
}

function canonicalSessionForWatch(w) {
  const key = w.session_key;
  let live = sessionsByKey.get(key);
  if (!live && w.start_ts != null && w.wave != null) {
    live = sessionsByKey.get(`${w.start_ts}_${w.wave}`);
  }
  if (!live && w.iso_date && w.time) {
    live = sessions.find(s =>
      (s.isoDate || s.dateKey) === w.iso_date
      && s.time === (w.time || w.start_time)
      && (!w.session_type || s.level === w.session_type)
    ) || null;
  }
  return live;
}

const pendingWatchSideSync = new Set();

async function syncWatchSideToSupabase(watch) {
  if (!supabase || !watch?.id) return;
  try {
    await supabase.from('watchlist_items').update({
      wave_side: watch.wave_side,
      wave: watch.wave,
    }).eq('id', watch.id);
  } catch (e) {
    console.error('  watchlist wave_side sync failed:', e.message);
  }
}

function queueWatchSideSync(watch) {
  if (!watch?.id || pendingWatchSideSync.has(watch.id)) return;
  pendingWatchSideSync.add(watch.id);
  syncWatchSideToSupabase(watch).finally(() => pendingWatchSideSync.delete(watch.id));
}

function enrichWatchItemForClient(w) {
  const base = watchItemToClient(w);
  const live = canonicalSessionForWatch(w);
  if (!live) return base;

  if (live.waveSide && live.waveSide !== w.wave_side) {
    w.wave_side = live.waveSide;
    w.wave = live.wave ?? w.wave;
    queueWatchSideSync(w);
  }

  return {
    ...base,
    wave: live.wave ?? base.wave,
    wave_side: live.waveSide ?? base.wave_side,
    waveSide: live.waveSide ?? base.wave_side,
    slots: live.slots,
    available: live.available,
    capacity: live.capacity,
    estimatedBooked: live.estimatedBooked,
    priceText: live.priceText,
  };
}

function buildWatchlistSideDebug(userKey) {
  const items = userKey
    ? activeWatchItems().filter(w => w.user_key === userKey)
    : activeWatchItems();
  return items.slice(0, 30).map(w => {
    const live = canonicalSessionForWatch(w);
    return {
      session_key: w.session_key,
      stored_wave_side: w.wave_side,
      current_wave_side: live?.waveSide ?? null,
      wave: live?.wave ?? w.wave,
      iso_date: w.iso_date || live?.isoDate || live?.dateKey,
      time: w.time || w.start_time || live?.time,
      session_type: w.session_type || live?.level,
    };
  });
}

function watchlistForUser(userKey) {
  if (!userKey) return [];
  return activeWatchItems()
    .filter(w => w.user_key === userKey)
    .map(enrichWatchItemForClient);
}

function watchAlertDefaults(row = {}) {
  return {
    alert_when_opens: row.alert_when_opens !== false,
    alert_when_low_slots: row.alert_when_low_slots !== false,
    low_slots_threshold: row.low_slots_threshold ?? THRESH,
    alert_when_selling_fast: row.alert_when_selling_fast !== false,
    fast_drop_threshold: row.fast_drop_threshold ?? 3,
    alert_last_call: row.alert_last_call !== false,
    last_call_minutes_before: row.last_call_minutes_before ?? 120,
  };
}

function enrichWatchBaseline(row, { reset = false } = {}) {
  const s = sessionsByKey.get(row.session_key);
  const now = new Date().toISOString();
  Object.assign(row, watchAlertDefaults(row));
  if (reset || row.watched_at == null) {
    row.watched_at = now;
    const bookable = !!(s?.available && s.slots !== 0);
    row.initial_available = bookable;
    row.initial_slots_available = s?.slots ?? null;
    row.last_seen_available = bookable;
    row.last_seen_slots_available = s?.slots ?? null;
    row.last_seen_at = now;
  }
  return row;
}

function alertDedupeKey(userKey, sessionKey, eventType) {
  return `${userKey}:${sessionKey}:${eventType}`;
}

function clearOpenedAlertState(watch) {
  lastAlertState.delete(alertDedupeKey(watch.user_key, watch.session_key, 'opened'));
}

function sessionAlertWhen(s) {
  const day = s.dayLabel || s.date || '';
  const time = s.time || '';
  const side = s.waveSide || `Wave ${s.wave}`;
  return { day, time, side, label: `${s.level} ${side}` };
}

function formatWhenClause(s) {
  const { day, time } = sessionAlertWhen(s);
  return `${day} at ${time}`.replace(/\s+/g, ' ').trim();
}

function formatSlotsSuffix(n) {
  if (n == null) return '';
  return ` · ${n} spot${n === 1 ? '' : 's'}`;
}

function buildAlertMessage(s, eventType, meta = {}) {
  const { side } = sessionAlertWhen(s);
  const when = formatWhenClause(s);
  if (eventType === 'opened') {
    return `${s.level} ${side} opened · ${when}${formatSlotsSuffix(s.slots)}`;
  }
  if (eventType === 'low_slots') {
    const dayWord = (s.dayLabel || '').toLowerCase() === 'today' ? 'today' : when;
    const n = s.slots;
    return `${s.level} ${side} is closing out · ${n} spot${n === 1 ? '' : 's'} left ${dayWord} at ${s.time || ''}`.replace(/\s+/g, ' ').trim();
  }
  if (eventType === 'selling_fast') {
    const from = meta.fromSlots ?? '?';
    const to = s.slots ?? '?';
    return `${s.level} ${side} is filling fast · ${from} → ${to} spots`;
  }
  if (eventType === 'last_call') {
    const mins = meta.minutesUntil ?? '?';
    return `Last call: ${s.level} ${side} · ${s.slots} spot${s.slots === 1 ? '' : 's'} · starts in ${mins} min`;
  }
  return `${s.level} ${side} update · ${when}`;
}

function shouldSkipAlert(dedupeKey, eventType, session, meta = {}) {
  const prev = lastAlertState.get(dedupeKey);
  if (eventType === 'opened') {
    return !!prev?.sent;
  }
  if (eventType === 'low_slots') {
    return prev?.slots === session.slots;
  }
  if (eventType === 'selling_fast') {
    return prev?.from === meta.fromSlots && prev?.to === session.slots;
  }
  if (eventType === 'last_call') {
    return !!prev?.sent;
  }
  return false;
}

async function recordNotificationEvent(watch, session, eventType, message, result, detail = {}) {
  if (!supabase) return;
  try {
    await supabase.from('notification_events').insert({
      user_key: watch.user_key,
      session_key: watch.session_key,
      event_type: eventType,
      ntfy_topic: watch.ntfy_topic || null,
      message,
      sent_ok: !!result.ok,
      error: result.error || null,
      previous_available: detail.previousAvailable ?? null,
      current_available: detail.currentAvailable ?? session.available ?? null,
      previous_slots: detail.previousSlots ?? null,
      current_slots: detail.currentSlots ?? session.slots ?? null,
      event_reason: detail.eventReason ?? eventType,
    });
  } catch (e) {
    console.error('  notification_events insert failed:', e.message);
  }
}

function markAlertSent(dedupeKey, eventType, session, meta = {}) {
  const payload = { at: Date.now(), slots: session.slots ?? null, available: session.available };
  if (eventType === 'opened' || eventType === 'last_call') payload.sent = true;
  if (eventType === 'selling_fast') {
    payload.from = meta.fromSlots;
    payload.to = session.slots;
  }
  lastAlertState.set(dedupeKey, payload);
}

async function maybeSendWatchAlert(watch, session, eventType, { urgent = false, meta = {}, detail = {} } = {}) {
  const topic = resolveNtfyTopicForWatch(watch);
  if (!topic) {
    console.log(`  [alert skip] no ntfy topic for ${watch.session_key} (${eventType})`);
    return false;
  }
  if (!watch.ntfy_topic?.trim() && INTERNAL_BETA) {
    console.log(`  [alert] internal beta fallback topic for ${watch.user_key.slice(0, 8)}…`);
  }

  const dedupeKey = alertDedupeKey(watch.user_key, watch.session_key, eventType);
  if (shouldSkipAlert(dedupeKey, eventType, session, meta)) return false;

  const message = buildAlertMessage(session, eventType, meta);
  const eventDetail = {
    previousAvailable: detail.previousAvailable,
    currentAvailable: session.available,
    previousSlots: detail.previousSlots,
    currentSlots: session.slots ?? null,
    eventReason: detail.eventReason || eventType,
  };
  const result = await sendNtfy(topic, 'AP Session Alert', message, { urgent, clickUrl: APP_URL });
  await recordNotificationEvent(watch, session, eventType, message, result, eventDetail);
  if (result.ok) {
    console.log(`  📲 AP Session Alert → ${topic} (${eventType})`);
    markAlertSent(dedupeKey, eventType, session, meta);
    return true;
  }
  console.error(`  ntfy failed (${eventType}):`, result.error);
  return false;
}

function isOpenedTransition(watch, prevAvailable, prevSlots, session) {
  if (!session.available) return false;
  const currSlots = session.slots;
  const hasSpots = currSlots == null || currSlots > 0;
  if (!hasSpots) return false;

  if (prevAvailable === false) return true;
  if (watch.last_seen_available === false) return true;
  if (prevSlots === 0 && currSlots > 0) return true;
  return false;
}

function wasAlreadyAvailableForLowSlots(watch, prevAvailable, prevSlots) {
  if (prevAvailable === false) return false;
  if (prevSlots === 0) return false;
  if (watch.last_seen_available === false) return false;
  return true;
}

async function persistWatchItemState(watch) {
  const idx = watchItems.findIndex(w => w.id === watch.id);
  if (idx >= 0) watchItems[idx] = { ...watchItems[idx], ...watch };
  if (!supabase || !watch.id) return;
  try {
    const { error } = await supabase.from('watchlist_items').update({
      last_seen_available: watch.last_seen_available,
      last_seen_slots_available: watch.last_seen_slots_available,
      last_seen_at: watch.last_seen_at,
      initial_available: watch.initial_available,
      initial_slots_available: watch.initial_slots_available,
      watched_at: watch.watched_at,
    }).eq('id', watch.id);
    if (error) throw error;
  } catch (e) {
    console.error('  watchlist state persist failed:', e.message);
  }
}

function minutesUntilSessionStart(session) {
  if (!session?.ts) return null;
  return Math.round((session.ts * 1000 - Date.now()) / 60_000);
}

// Per-session watch alerts (saved-search alerts can hook in here later).
async function evaluateSessionWatchAlerts(watch, session, ctx) {
  const {
    prevAvailable,
    prevSlots,
    prevSeenAt,
    slotsAlerts,
  } = ctx;

  const threshold = watch.low_slots_threshold ?? THRESH;
  const fastDrop = watch.fast_drop_threshold ?? 3;
  const lastCallMins = watch.last_call_minutes_before ?? 120;
  const currSlots = session.slots ?? null;
  const alertDetail = {
    previousAvailable: prevAvailable,
    previousSlots: prevSlots,
  };

  if (!session.available) {
    clearOpenedAlertState(watch);
    return;
  }

  let sentOpened = false;
  if (watch.alert_when_opens !== false && isOpenedTransition(watch, prevAvailable, prevSlots, session)) {
    sentOpened = await maybeSendWatchAlert(watch, session, 'opened', {
      urgent: true,
      detail: { ...alertDetail, eventReason: 'unavailable_to_available' },
    });
  }

  if (!slotsAlerts) {
    if (!sentOpened && watch.alert_last_call !== false) {
      const minsUntil = minutesUntilSessionStart(session);
      if (minsUntil != null && minsUntil > 0 && minsUntil <= lastCallMins && (currSlots == null || currSlots > 0)) {
        await maybeSendWatchAlert(watch, session, 'last_call', {
          urgent: true,
          meta: { minutesUntil: minsUntil },
          detail: { ...alertDetail, eventReason: 'last_call_window' },
        });
      }
    }
    return;
  }

  if (currSlots == null) return;

  if (!sentOpened && watch.alert_when_low_slots !== false && wasAlreadyAvailableForLowSlots(watch, prevAvailable, prevSlots)) {
    if (currSlots <= threshold) {
      const crossedIntoLow = prevSlots == null || prevSlots > threshold;
      const decreasedWhileLow = prevSlots != null && prevSlots > currSlots && currSlots <= threshold;
      if (crossedIntoLow || decreasedWhileLow) {
        await maybeSendWatchAlert(watch, session, 'low_slots', {
          urgent: true,
          detail: { ...alertDetail, eventReason: crossedIntoLow ? 'crossed_low_threshold' : 'decreased_while_low' },
        });
      }
    }
  }

  if (!sentOpened && watch.alert_when_selling_fast !== false && wasAlreadyAvailableForLowSlots(watch, prevAvailable, prevSlots) && prevSlots != null && currSlots < prevSlots) {
    const drop = prevSlots - currSlots;
    let percentDrop = false;
    if (prevSeenAt && prevSlots > 0) {
      const minsSince = (Date.now() - prevSeenAt) / 60_000;
      percentDrop = minsSince <= 30 && drop / prevSlots >= 0.4;
    }
    if (drop >= fastDrop || percentDrop) {
      await maybeSendWatchAlert(watch, session, 'selling_fast', {
        urgent: currSlots <= threshold,
        meta: { fromSlots: prevSlots },
        detail: { ...alertDetail, eventReason: percentDrop ? 'percent_drop_30m' : 'absolute_drop' },
      });
    }
  }

  if (watch.alert_last_call !== false) {
    const minsUntil = minutesUntilSessionStart(session);
    if (minsUntil != null && minsUntil > 0 && minsUntil <= lastCallMins && currSlots > 0) {
      await maybeSendWatchAlert(watch, session, 'last_call', {
        urgent: true,
        meta: { minutesUntil: minsUntil },
        detail: { ...alertDetail, eventReason: 'last_call_window' },
      });
    }
  }
}

async function maybeSendImmediateWatchAlert(watch, session) {
  if (!session?.available || session.slots == null) return;
  if (watch.initial_available === false || session.slots === 0) return;
  const threshold = watch.low_slots_threshold ?? THRESH;
  if (watch.alert_when_low_slots !== false && session.slots <= threshold) {
    await maybeSendWatchAlert(watch, session, 'low_slots', {
      urgent: true,
      detail: {
        previousAvailable: watch.initial_available,
        previousSlots: watch.initial_slots_available,
        eventReason: 'immediate_low_on_watch',
      },
    });
  }
}

async function processWatchAlertsAfterScrape(updatedKeys, { slotsAlerts = false } = {}) {
  const updatedSet = new Set(updatedKeys);
  const now = new Date().toISOString();

  for (const watch of activeWatchItems()) {
    const session = sessionsByKey.get(watch.session_key);
    if (!session) continue;

    const prevAvailable = watch.last_seen_available;
    const prevSlots = watch.last_seen_slots_available;
    const prevSeenAt = watch.last_seen_at ? new Date(watch.last_seen_at).getTime() : null;

    if (updatedSet.has(watch.session_key)) {
      await evaluateSessionWatchAlerts(watch, session, {
        prevAvailable,
        prevSlots,
        prevSeenAt,
        slotsAlerts,
      });
    }

    if (session.waveSide && session.waveSide !== watch.wave_side) {
      watch.wave_side = session.waveSide;
      watch.wave = session.wave ?? watch.wave;
    }

    watch.last_seen_available = !!(session.available && session.slots !== 0);
    watch.last_seen_slots_available = session.slots ?? null;
    watch.last_seen_at = now;
    await persistWatchItemState(watch);

    history[watch.session_key] = { available: session.available, slots: session.slots ?? null };
  }

  for (const key of updatedKeys) {
    if (!activeWatchItems().some(w => w.session_key === key)) {
      const s = sessionsByKey.get(key);
      if (s) history[key] = { available: s.available, slots: s.slots ?? null };
    }
  }
}

function resolveNtfyTopicForWatch(watch) {
  const userTopic = (watch?.ntfy_topic || '').trim();
  if (userTopic) return userTopic;
  if (!INTERNAL_BETA) return null;
  return (TOPIC || INTERNAL_DEFAULT_NTFY_TOPIC).trim() || null;
}

function resolveNtfyTopicForRequest(requestTopic) {
  const userTopic = (requestTopic || '').trim();
  if (userTopic) return userTopic;
  if (!INTERNAL_BETA) return TOPIC.trim() || null;
  return (TOPIC || INTERNAL_DEFAULT_NTFY_TOPIC).trim() || null;
}

async function loadWatchlistFromSupabase() {
  if (!supabase) return;
  try {
    const { data, error } = await supabase
      .from('watchlist_items')
      .select('*')
      .eq('active', true);
    if (error) throw error;
    watchItems = asSessionArray(data).map(w => ({ ...watchAlertDefaults(w), ...w }));
    console.log(`  Supabase: loaded ${watchItems.length} watchlist item(s)`);
  } catch (e) {
    console.error('  Supabase watchlist load failed:', e.message);
  }
}

function mergeWatchBaseline(prev, row) {
  return {
    ...row,
    watched_at: prev?.watched_at ?? row.watched_at,
    initial_available: prev?.initial_available ?? row.initial_available,
    initial_slots_available: prev?.initial_slots_available ?? row.initial_slots_available,
    last_seen_available: prev?.last_seen_available ?? row.last_seen_available,
    last_seen_slots_available: prev?.last_seen_slots_available ?? row.last_seen_slots_available,
    last_seen_at: prev?.last_seen_at ?? row.last_seen_at,
  };
}

async function upsertWatchItem(row, { isNew = false } = {}) {
  Object.assign(row, watchAlertDefaults(row));
  if (!row.id) row.id = crypto.randomUUID();

  const memExisting = watchItems.find(
    w => w.user_key === row.user_key && w.session_key === row.session_key
  );

  if (!supabase) {
    watchItems = watchItems.filter(
      w => !(w.user_key === row.user_key && w.session_key === row.session_key)
    );
    if (memExisting && !isNew) {
      const saved = mergeWatchBaseline(memExisting, { ...memExisting, ...row, active: true });
      watchItems.push(saved);
      return saved;
    }
    if (isNew) enrichWatchBaseline(row, { reset: true });
    watchItems.push(row);
    return row;
  }

  try {
    const { data: existing } = await supabase
      .from('watchlist_items')
      .select('*')
      .eq('user_key', row.user_key)
      .eq('session_key', row.session_key)
      .maybeSingle();

    if (existing) {
      const merged = mergeWatchBaseline(existing, { ...existing, ...row, active: true });
      const { data, error } = await supabase
        .from('watchlist_items')
        .update(merged)
        .eq('id', existing.id)
        .select('*')
        .single();
      if (error) throw error;
      watchItems = watchItems.filter(w => w.id !== data.id);
      watchItems.push(data);
      return data;
    }

    if (isNew) enrichWatchBaseline(row, { reset: true });
    const { data, error } = await supabase
      .from('watchlist_items')
      .insert(row)
      .select('*')
      .single();
    if (error) throw error;
    watchItems = watchItems.filter(w => w.id !== data.id);
    watchItems.push(data);
    return data;
  } catch (e) {
    console.error('  Supabase watchlist upsert failed:', e.message);
    watchItems = watchItems.filter(
      w => !(w.user_key === row.user_key && w.session_key === row.session_key)
    );
    watchItems.push(row);
    return row;
  }
}

async function deactivateWatchItem(id, userKey) {
  watchItems = watchItems.filter(w => w.id !== id);
  if (!supabase) return;
  try {
    const { error } = await supabase
      .from('watchlist_items')
      .update({ active: false })
      .eq('id', id)
      .eq('user_key', userKey);
    if (error) throw error;
  } catch (e) {
    console.error('  Supabase watchlist deactivate failed:', e.message);
  }
}

function buildWatchRow(body) {
  const {
    user_key, ntfy_topic, session_key, key,
    start_ts, ts, iso_date, dateKey,
    wave_side, waveSide, session_type, level,
    time, date, dayLabel, wave,
    alert_when_opens, alert_when_low_slots, low_slots_threshold,
    alert_when_selling_fast, fast_drop_threshold,
    alert_last_call, last_call_minutes_before,
  } = body;

  const sessionKey = session_key || key;
  if (!user_key || !sessionKey) return null;

  const live = sessionsByKey.get(sessionKey);

  return {
    user_key,
    ntfy_topic: (ntfy_topic || '').trim() || null,
    session_key: sessionKey,
    iso_date: iso_date || dateKey || live?.isoDate || live?.dateKey || null,
    start_ts: start_ts ?? ts ?? live?.ts ?? null,
    wave_side: live?.waveSide || wave_side || waveSide || null,
    session_type: session_type || level || live?.level || null,
    start_time: time || live?.time || null,
    time: time || live?.time || null,
    date: date || live?.date || null,
    day_label: dayLabel || live?.dayLabel || null,
    wave: live?.wave ?? (wave != null ? +wave : null),
    ...watchAlertDefaults({
      alert_when_opens,
      alert_when_low_slots,
      low_slots_threshold,
      alert_when_selling_fast,
      fast_drop_threshold,
      alert_last_call,
      last_call_minutes_before,
    }),
    active: true,
  };
}

const MAX_SLOT_CLICKS = 30;
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

async function getSessionModalDetails(page, ts, wave) {
  const label = `${ts}_${wave}`;
  console.log(`\n[getSessionModalDetails ${label}] starting`);

  try {
    const tileSel = `div[class*="booking-agenda-clickable_${ts}_${wave}"]`;
    const tile = await page.$(tileSel);
    if (!tile) {
      console.log(`  [getSessionModalDetails ${label}] tile not found (${tileSel})`);
      return null;
    }
    console.log(`  [getSessionModalDetails ${label}] tile found, clicking...`);

    await tile.click({ timeout: 10_000 });
    console.log(`  [getSessionModalDetails ${label}] tile click registered`);

    const modal = await waitForModal(page, label);
    if (!modal) {
      console.log(`  [getSessionModalDetails ${label}] abort — modal never appeared`);
      return null;
    }

    const screenshotPath = path.join(__dirname, 'debug-modal.png');
    if (process.env.DEBUG_MODAL === '1') {
      await page.screenshot({ path: screenshotPath });
      console.log(`  [getSessionModalDetails ${label}] debug screenshot saved → ${screenshotPath}`);
    }

    await findPlusButton(modal, label, true);

    let n = 0;
    for (let i = 0; i < MAX_SLOT_CLICKS; i++) {
      const btn = await findPlusButton(modal, label, false);
      if (!btn) {
        console.log(`  [getSessionModalDetails ${label}] click ${i + 1}: no visible + button found, stopping`);
        break;
      }
      if (await isPlusDisabled(btn)) {
        console.log(`  [getSessionModalDetails ${label}] click ${i + 1}: + button disabled, stopping at ${n}`);
        break;
      }
      await btn.click({ timeout: 5_000 });
      n++;
      const qty = await modal.locator('input.qty-info').last().inputValue().catch(() => '?');
      console.log(`  [getSessionModalDetails ${label}] click ${i + 1}: + clicked, count=${n}, qty-input=${qty}`);
      await page.waitForTimeout(120);
    }

    if (n > 20) {
      console.warn(`  [getSessionModalDetails ${label}] WARNING: ${n} clicks — exceeds expected max; + button detection may have run away`);
    }

    let priceInfo = {};
    let capacityFromModal = null;
    try {
      const meta = await modal.evaluate(() => {
        const el = document.querySelector('.modal.in, .modal.show, .modal, [role="dialog"]') || document.body;
        const text = el.innerText || '';
        const qty = document.querySelector('input.qty-info');
        const maxAttr = qty?.getAttribute('max');
        const maxQty = maxAttr ? parseInt(maxAttr, 10) : null;
        return { text, maxQty: Number.isFinite(maxQty) && maxQty > 0 ? maxQty : null };
      });
      priceInfo = parsePriceFromText(meta.text);
      if (meta.maxQty) capacityFromModal = meta.maxQty;
      if (priceInfo.price_text) {
        console.log(`  [getSessionModalDetails ${label}] price: ${priceInfo.price_text}`);
      }
    } catch (pe) {
      console.log(`  [getSessionModalDetails ${label}] price parse skipped: ${pe.message}`);
    }

    console.log(`  [getSessionModalDetails ${label}] result: ${n} available slot(s)`);
    await closeModal(page, label);
    return {
      slots: n > 0 ? n : null,
      capacity: capacityFromModal,
      ...priceInfo,
    };

  } catch (e) {
    console.error(`  [getSessionModalDetails ${label}] ERROR: ${e.message}`);
    try { await closeModal(page, label); } catch (ce) {
      console.error(`  [getSessionModalDetails ${label}] close after error failed: ${ce.message}`);
    }
    return null;
  }
}

async function getSlotCount(page, ts, wave) {
  const details = await getSessionModalDetails(page, ts, wave);
  return details?.slots ?? null;
}

// Parse session tiles currently visible in the agenda DOM.
// Dates derive from the tile unix timestamp (Atlantic Park local time via browser TZ).
function scrapeVisibleSessions({ excludedLevels = [], excludedWaves = [], weekOffset = 0 } = {}) {
  // Site column index fallback — only used when tile/column text does not resolve side.
  // Atlantic Park agenda columns: 1=Left Wave, 2=Right Wave, 3=Left Lesson, 4=Right Lesson.
  const WAVE_INDEX_FALLBACK = {
    1: 'Left Wave', 2: 'Right Wave', 3: 'Left Lesson', 4: 'Right Lesson',
  };

  function isLessonLevel(level) {
    return /progressive|lesson|cruiser/i.test(level || '');
  }

  function normalizeSideLabel(raw, level) {
    const text = (raw || '').replace(/\s+/g, ' ').trim();
    const low = text.toLowerCase();
    if (!text) return null;
    const lesson = /lesson/.test(low) || isLessonLevel(level);
    const left = /\bleft\b/.test(low);
    const right = /\bright\b/.test(low);
    if (left) return lesson ? 'Left Lesson' : 'Left Wave';
    if (right) return lesson ? 'Right Lesson' : 'Right Wave';
    return null;
  }

  function parseSideFromTitle(html, level) {
    if (!html) return null;
    const text = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    const fieldPatterns = [
      /(?:wave\s*side|session\s*side|side|wave)\s*:?\s*(?:<\/b>)?\s*([^|<]+)/i,
    ];
    for (const re of fieldPatterns) {
      const m = html.match(re) || text.match(re);
      if (m) {
        const side = normalizeSideLabel(m[1], level);
        if (side) return { side, source: 'title_field', raw: m[1].trim() };
      }
    }
    const labelMatch = text.match(/\b(Left|Right)\s+(Wave|Lesson)\b/i)
      || text.match(/\b(Wave|Lesson)\s+(Left|Right)\b/i);
    if (labelMatch) {
      const side = normalizeSideLabel(labelMatch[0], level);
      if (side) return { side, source: 'title_text', raw: labelMatch[0] };
    }
    return null;
  }

  function parseSideFromColumn(el, level) {
    const td = el.closest('td');
    if (td) {
      const table = td.closest('table');
      if (table) {
        const colIdx = td.cellIndex;
        const headerRow = table.querySelector('thead tr') || table.querySelector('tr');
        const headerCell = headerRow?.cells?.[colIdx];
        const headerText = headerCell?.textContent?.replace(/\s+/g, ' ').trim();
        if (headerText) {
          const side = normalizeSideLabel(headerText, level);
          if (side) return { side, source: 'column_header', raw: headerText };
        }
      }
    }

    let node = el.parentElement;
    for (let depth = 0; depth < 6 && node; depth++) {
      const headers = node.querySelectorAll('th, [class*="header"], [class*="agenda-col"]');
      if (headers.length >= 2) {
        const idx = [...node.children].indexOf(el.parentElement);
        if (idx >= 0 && headers[idx]) {
          const headerText = headers[idx].textContent.replace(/\s+/g, ' ').trim();
          const side = normalizeSideLabel(headerText, level);
          if (side) return { side, source: 'grid_header', raw: headerText };
        }
      }
      node = node.parentElement;
    }
    return null;
  }

  function resolveWaveSide(el, wave, level, titleHtml) {
    const fromTitle = parseSideFromTitle(titleHtml, level);
    const fromColumn = parseSideFromColumn(el, level);
    const tileText = titleHtml.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

    if (fromTitle && fromColumn && fromTitle.side !== fromColumn.side) {
      return {
        waveSide: fromTitle.side,
        waveSideSource: 'title_over_column_conflict',
        sideParseRaw: `${fromTitle.raw} | column:${fromColumn.raw}`,
        tileText,
        waveSideAmbiguous: true,
      };
    }
    if (fromTitle) {
      return {
        waveSide: fromTitle.side,
        waveSideSource: fromTitle.source,
        sideParseRaw: fromTitle.raw,
        tileText,
        waveSideAmbiguous: false,
      };
    }
    if (fromColumn) {
      return {
        waveSide: fromColumn.side,
        waveSideSource: fromColumn.source,
        sideParseRaw: fromColumn.raw,
        tileText,
        waveSideAmbiguous: false,
      };
    }
    const fallback = WAVE_INDEX_FALLBACK[wave] || `Wave ${wave}`;
    return {
      waveSide: fallback,
      waveSideSource: 'wave_index_fallback',
      sideParseRaw: `wave_index_${wave}`,
      tileText,
      waveSideAmbiguous: true,
    };
  }

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
    const sideInfo = resolveWaveSide(el, wave, level, t);
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
      waveSide  : sideInfo.waveSide,
      waveSideSource: sideInfo.waveSideSource,
      sideParseRaw: sideInfo.sideParseRaw,
      tileText  : sideInfo.tileText,
      waveSideAmbiguous: sideInfo.waveSideAmbiguous,
      sideKey   : `${ts}_${sideInfo.waveSide.toLowerCase().replace(/\s+/g, '-')}`,
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
  const watchKeys = watchedSessionKeys();
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

    const details = await getSessionModalDetails(page, s.ts, s.wave);
    entry.slots = details?.slots ?? null;
    if (details) attachSessionMetrics(entry, details, s.level);
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
    if (existing) {
      for (const field of ['capacity', 'estimatedBooked', 'fillRate', 'priceText', 'priceMin', 'priceMax', 'currency']) {
        if (existing[field] != null && merged[field] == null) merged[field] = existing[field];
      }
    }
    sessionsByKey.set(raw.key, merged);
    updatedKeys.push(raw.key);
    logWaveSideParse(merged);
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


async function runTierScrape(tier) {
  if (!tryAcquireScrapeLock(`tier ${tier}`)) return;

  const cfg = TIER_CONFIG[tier];
  const { startWeek, endWeek } = weeksForTier(tier);
  if (endWeek < startWeek || startWeek >= effectiveWeeksAhead) {
    console.log(`[tier ${tier}] skipped — no weeks in range (offsets ${startWeek}–${endWeek}, effective=${effectiveWeeksAhead})`);
    releaseScrapeLock();
    return;
  }

  console.log(`\n[${new Date().toLocaleTimeString()}] Tier ${tier} scrape (${cfg.label}, week offsets ${startWeek}–${endWeek})`);

  lastScrapeAttempt = new Date().toISOString();
  const scrapeRunId = await beginScrapeRun(tier);

  if (tier === 1) {
    checkCycle++;
    slotChecksThisCycle = 0;
  }

  const slotStats = { cached: 0, rechecked: 0, byReason: {}, queueLogged: false };
  const prevByKey = new Map(sessions.map(s => [s.key, s]));
  let launched;
  let coverage = null;

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
    coverage = computeDateCoverage();
    console.log(`  date coverage: ${coverage.earliestSessionDate || '?'} → ${coverage.latestSessionDate || '?'} (${coverage.uniqueDatesCount} days, ${coverage.coveragePercent}% dates checked)`);
    await processWatchAlertsAfterScrape(updatedKeys, { slotsAlerts: cfg.slotCounts });

    lastTierRun[tier] = new Date().toISOString();
    lastSuccessfulScrape = new Date().toISOString();
    lastCheck = lastSuccessfulScrape;
    lastScrapeError = null;
    lastScrapeErrorStack = null;
    hasFreshScrapeThisBoot = true;
    dataSource = supabaseConfigured ? 'supabase' : 'memory-fallback';

    await upsertCurrentSessionsToSupabase(merged, tier);
    lastSnapshotRowsInsertedLastRun = await saveAvailabilitySnapshotsToSupabase(merged, tier);
    await saveLatestSnapshotToSupabase();

    await finishScrapeRun(scrapeRunId, {
      success: true,
      sessionsFound: merged.length,
      datesCovered: coverage.coveredDatesCount,
      missingDates: coverage.missingDatesInScrapeWindow,
      coveragePercent: coverage.coveragePercent,
    });

  } catch (e) {
    recordScrapeError(e, `tier ${tier} scrape`);
    await saveScrapeErrorToSupabase(lastScrapeError);
    await finishScrapeRun(scrapeRunId, {
      success: false,
      sessionsFound: sessions.length,
      datesCovered: coverage?.coveredDatesCount ?? null,
      missingDates: coverage?.missingDatesInScrapeWindow ?? null,
      error: lastScrapeError,
      errorStack: lastScrapeErrorStack,
    });
  } finally {
    releaseScrapeLock();
    if (launched?.browser) await launched.browser.close();
  }
}

async function detectWeeksOnStartup() {
  if (!tryAcquireScrapeLock('week detection')) return;

  let launched;
  try {
    launched = await launchBrowser();
    const detected = await detectAvailableWeeks(launched.page);
    updateEffectiveWeeksCap(detected);
  } catch (e) {
    console.error('week detection failed:', e.message);
    effectiveWeeksAhead = SCRAPE_WEEKS_AHEAD;
  } finally {
    releaseScrapeLock();
    if (launched?.browser) await launched.browser.close();
  }
}

// ── REST API ─────────────────────────────────────────────────────────────────
function statusPayload(userKey = null) {
  const dateCoverage = computeDateCoverage();
  return {
    sessions: asSessionArray(sessions),
    watchList: watchlistForUser(userKey),
    history: history || {},
    lastCheck: lastSuccessfulScrape || lastCheck,
    ntfyOk: !!TOPIC,
    ntfyFallbackConfigured: !!TOPIC,
    internalBetaNotifications: INTERNAL_BETA,
    internalDefaultNtfyTopic: INTERNAL_BETA ? INTERNAL_DEFAULT_NTFY_TOPIC : null,
    watchlistCount: activeWatchItems().length,
    watchlistSideDebug: buildWatchlistSideDebug(userKey),
    waveSideDebug: {
      ambiguousCount: ambiguousSideMappings.length,
      ambiguousSamples: ambiguousSideMappings.slice(-10),
      recentParses: recentSideParseLogs.slice(-12),
    },
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

app.get('/api/status', async (req, res) => {
  try {
    await ensureSessionsForStatus();
    res.json(statusPayload(req.query.user_key || null));
  } catch (e) {
    console.error('/api/status error:', e.message);
    res.json({ ...statusPayload(req.query.user_key || null), statusError: e.message });
  }
});

app.get('/api/sessions', async (req, res) => {
  try {
    await ensureSessionsForStatus();
    res.json(statusPayload(req.query.user_key || null));
  } catch (e) {
    console.error('/api/sessions error:', e.message);
    res.json({ ...statusPayload(req.query.user_key || null), statusError: e.message });
  }
});

app.get('/api/debug/scrape', (_req, res) => {
  const coverage = computeDateCoverage();
  res.json({
    scrapeInProgress,
    sessionsCount: sessions.length,
    currentSessionsCount: sessions.length,
    snapshotRowsInsertedLastRun: lastSnapshotRowsInsertedLastRun,
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

function aggregateAvailabilitySummary(rows) {
  const byWeekday = {};
  const bySessionType = {};
  const byHour = {};
  const openDays = {};
  const sessionSeries = new Map();

  for (const row of rows) {
    const slots = row.slots_available;
    if (slots == null) continue;

    const weekday = row.weekday || 'Unknown';
    const sessionType = row.session_type || 'Unknown';
    const hour = (row.start_time || '').split(':')[0] || 'Unknown';
    const isoDate = row.iso_date;

    if (!byWeekday[weekday]) byWeekday[weekday] = { total: 0, count: 0 };
    byWeekday[weekday].total += slots;
    byWeekday[weekday].count += 1;

    if (!bySessionType[sessionType]) bySessionType[sessionType] = { total: 0, count: 0 };
    bySessionType[sessionType].total += slots;
    bySessionType[sessionType].count += 1;

    if (!byHour[hour]) byHour[hour] = { total: 0, count: 0 };
    byHour[hour].total += slots;
    byHour[hour].count += 1;

    if (row.available && isoDate) {
      openDays[isoDate] = (openDays[isoDate] || 0) + 1;
    }

    if (!sessionSeries.has(row.session_key)) sessionSeries.set(row.session_key, []);
    sessionSeries.get(row.session_key).push({
      scrapedAt: row.scraped_at,
      slots,
      sessionType,
      isoDate,
      startTime: row.start_time,
    });
  }

  const average = (bucket) => Object.fromEntries(
    Object.entries(bucket).map(([k, v]) => [k, v.count ? +(v.total / v.count).toFixed(2) : 0])
  );

  const mostOpenDays = Object.entries(openDays)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([date, openSessionCount]) => ({ date, openSessionCount }));

  const fastestFilling = [];
  for (const [sessionKey, points] of sessionSeries.entries()) {
    if (points.length < 2) continue;
    points.sort((a, b) => new Date(a.scrapedAt) - new Date(b.scrapedAt));
    const first = points[0];
    const last = points[points.length - 1];
    const drop = first.slots - last.slots;
    if (drop <= 0) continue;
    const hours = Math.max(
      (new Date(last.scrapedAt) - new Date(first.scrapedAt)) / 3_600_000,
      0.01
    );
    fastestFilling.push({
      sessionKey,
      sessionType: first.sessionType,
      isoDate: first.isoDate,
      startTime: first.startTime,
      slotsStart: first.slots,
      slotsEnd: last.slots,
      slotsDropped: drop,
      dropPerHour: +(drop / hours).toFixed(2),
      snapshotCount: points.length,
    });
  }
  fastestFilling.sort((a, b) => b.dropPerHour - a.dropPerHour);

  return {
    snapshotCount: rows.length,
    averageSlotsByWeekday: average(byWeekday),
    averageSlotsBySessionType: average(bySessionType),
    averageSlotsByHour: average(byHour),
    mostOpenDays,
    fastestFillingSessions: fastestFilling.slice(0, 10),
  };
}

app.get('/api/analytics/availability-summary', async (_req, res) => {
  if (!supabase) {
    return res.json({ configured: false, message: 'Supabase not configured' });
  }
  try {
    const since = new Date(Date.now() - 30 * 86_400_000).toISOString();
    const { data, error } = await supabase
      .from('availability_snapshots')
      .select('scraped_at, session_key, iso_date, start_time, weekday, session_type, available, slots_available')
      .gte('scraped_at', since)
      .not('slots_available', 'is', null)
      .order('scraped_at', { ascending: true })
      .limit(15000);
    if (error) throw error;
    res.json({
      configured: true,
      since,
      ...aggregateAvailabilitySummary(asSessionArray(data)),
    });
  } catch (e) {
    res.status(500).json({ configured: true, error: e.message });
  }
});

app.get('/api/watchlist', (req, res) => {
  const userKey = req.query.user_key;
  if (!userKey) return res.status(400).json({ error: 'user_key required' });
  res.json({ items: watchlistForUser(userKey) });
});

app.post('/api/watchlist', async (req, res) => {
  const row = buildWatchRow(req.body);
  if (!row) return res.status(400).json({ error: 'user_key and session_key required' });
  try {
    const isNew = !watchItems.some(
      w => w.user_key === row.user_key && w.session_key === row.session_key
    );
    const saved = await upsertWatchItem(row, { isNew });
    if (isNew) {
      await maybeSendImmediateWatchAlert(saved, sessionsByKey.get(saved.session_key));
    }
    console.log(`  👁  Watching: ${saved.session_type} ${saved.day_label || saved.iso_date || ''} (${saved.user_key.slice(0, 8)}…)`);
    res.json({ ok: true, item: watchItemToClient(saved) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/watchlist/sync', async (req, res) => {
  const { user_key, ntfy_topic, items } = req.body || {};
  if (!user_key) return res.status(400).json({ error: 'user_key required' });
  const incoming = asSessionArray(items);
  const synced = [];

  for (const item of incoming) {
    const row = buildWatchRow({ ...item, user_key, ntfy_topic: item.ntfy_topic || ntfy_topic });
    if (!row) continue;
    const isNew = !watchItems.some(
      w => w.user_key === row.user_key && w.session_key === row.session_key
    );
    const saved = await upsertWatchItem(row, { isNew });
    synced.push(watchItemToClient(saved));
  }

  const incomingKeys = new Set(incoming.map(i => i.session_key || i.key));
  watchItems = watchItems.filter(
    w => w.user_key !== user_key || incomingKeys.has(w.session_key)
  );

  res.json({ ok: true, items: watchlistForUser(user_key) });
});

app.delete('/api/watchlist/:id', async (req, res) => {
  const userKey = req.query.user_key || req.body?.user_key;
  if (!userKey) return res.status(400).json({ error: 'user_key required' });
  await deactivateWatchItem(req.params.id, userKey);
  for (const key of [...lastAlertState.keys()]) {
    if (key.startsWith(`${userKey}:`)) lastAlertState.delete(key);
  }
  console.log(`  🗑  Removed watch ${req.params.id}`);
  res.json({ ok: true });
});

app.post('/api/notify/test', async (req, res) => {
  const topic = resolveNtfyTopicForRequest(req.body?.ntfy_topic);
  if (!topic) return res.status(400).json({ error: 'ntfy_topic required' });
  const result = await sendNtfy(
    topic,
    'AP Session Alert',
    'Test notification — your AP Sessions alerts are working.',
    { clickUrl: APP_URL }
  );
  if (!result.ok) return res.status(502).json({ ok: false, error: result.error });
  res.json({ ok: true });
});

// Legacy routes (deprecated)
app.post('/api/watch', async (req, res) => {
  const userKey = req.body.user_key;
  if (!userKey) return res.status(400).json({ error: 'user_key required — use POST /api/watchlist' });
  const row = buildWatchRow(req.body);
  if (!row) return res.status(400).json({ error: 'session key required' });
  const saved = await upsertWatchItem(row);
  res.json({ ok: true, id: saved.id, item: watchItemToClient(saved) });
});

app.delete('/api/watch/:id', async (req, res) => {
  const userKey = req.query.user_key;
  if (!userKey) return res.status(400).json({ error: 'user_key required' });
  await deactivateWatchItem(req.params.id, userKey);
  res.json({ ok: true });
});

async function loadPersistedData() {
  initSupabaseClient();
  if (!supabase) return;
  try {
    const loadedCurrent = await loadCurrentSessionsFromSupabase();
    if (!loadedCurrent) {
      await loadLatestSnapshotFromSupabase();
    }
    await loadWatchlistFromSupabase();
  } catch (e) {
    supabaseInitError = supabaseInitError || e.message;
    console.error('Supabase cache load failed:', e.message);
  }
}

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

async function startServer() {
  await loadPersistedData();
  if (sessions.length) {
    console.log(`Serving ${sessions.length} saved session(s) (${dataSource}) — background scrapes will refresh in place`);
  }

  app.listen(PORT, () => {
    console.log(`\nAP Session Watcher running on :${PORT}`);
    console.log(`Tier 1 (today/tomorrow + slots): every ${CHECK_MINS} min`);
    console.log('Tier 2 (this week):              every 30 min');
    console.log('Tier 3 (weeks 2–3):              every 6 hours');
    console.log('Tier 4 (weeks 4+):               daily at midnight');
    console.log(`Lookahead: ${SCRAPE_WEEKS_AHEAD} weeks (capped by site availability)`);
    if (supabaseConfigured) {
      console.log('Supabase collector: current_sessions + availability_snapshots + scrape_runs');
    }
    if (INTERNAL_BETA) {
      console.log(`Internal beta notifications enabled (default topic: ${INTERNAL_DEFAULT_NTFY_TOPIC})`);
    } else {
      console.log(TOPIC ? 'Ntfy fallback topic configured (personal testing)' : 'No NTFY_TOPIC fallback — users set topics in Setup');
    }
    bootstrapInBackground();
  });
}

startServer().catch((e) => {
  console.error('Server startup failed:', e.message);
  process.exit(1);
});
