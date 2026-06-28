'use strict';
const express  = require('express');
const { chromium } = require('playwright');
const cron     = require('node-cron');
const path     = require('path');
const crypto   = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const WebSocket = require('ws');

const pkg = require('./package.json');
const APP_VERSION = process.env.APP_VERSION || pkg.version || '1.0.0';
const BUILD_TIME = process.env.BUILD_TIME || new Date().toISOString();

const app = express();
app.use(express.json());

app.use((req, res, next) => {
  if (req.path.startsWith('/api/')) {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');
    res.set('Surrogate-Control', 'no-store');
  } else if (req.path === '/' || req.path === '/index.html') {
    res.set('Cache-Control', 'no-cache');
  } else if (/\.(png|jpg|jpeg|webp|svg|ico|woff2?)$/i.test(req.path)) {
    res.set('Cache-Control', 'public, max-age=86400');
  } else if (/\.(js|css)$/i.test(req.path)) {
    res.set('Cache-Control', 'public, max-age=3600');
  }
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

const PORT       = process.env.PORT || 3000;
const TOPIC      = process.env.NTFY_TOPIC || '';
const INTERNAL_BETA = process.env.INTERNAL_BETA_NOTIFICATIONS === 'true';
// ntfy is internal demo infrastructure only — public MVP should use native push, web push, SMS, or email after login.
const INTERNAL_DEFAULT_NTFY_TOPIC = 'ap-surf-connor-2026';
const INTERNAL_DEFAULT_PROFILE_CODE = 'ap-surf-connor-2026';
const THRESH     = parseInt(process.env.LOW_SLOTS_THRESHOLD || '2');
const BOOKING    = 'https://booking.atlanticparksurf.com/activity-agenda';
const APP_URL    = process.env.APP_URL || BOOKING;
const CHECK_MINS      = parseInt(process.env.CHECK_EVERY_MINS || '5', 10);
const MAX_SLOT_CHECKS = parseInt(process.env.MAX_SLOT_CHECKS || '50', 10);
const SLOT_CACHE_STALE_CYCLES = parseInt(process.env.SLOT_CACHE_STALE_CYCLES || '3', 10);
const DETAIL_ENRICH_MAX_PER_RUN = parseInt(process.env.DETAIL_ENRICH_MAX_PER_RUN || '25', 10);
const ENRICHMENT_STALE_HOURS = parseInt(process.env.ENRICHMENT_STALE_HOURS || '6', 10);
const ENRICHMENT_TIER2_EVERY_MINS = parseInt(process.env.ENRICHMENT_TIER2_EVERY_MINS || '45', 10);
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
let watchlistLastError = null;
let watchlistRowsLoaded = 0;
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
let detailEnrichmentInProgress = false;
let lastDetailEnrichmentAt = null;
let lastDetailEnrichmentError = null;
let lastRequestedDateForEnrichment = null;
let enrichmentQueuePendingCount = 0;
let enrichmentQueueRunningCount = 0;
let fallbackAvailableCached = false;
const enrichmentQueueMemory = new Map();
let hasFreshScrapeThisBoot = false;
let dataSource = 'memory';
let supabaseConfigured = false;
let supabase = null;
let supabaseInitError = null;

const REQUIRED_SUPABASE_TABLES = [
  'current_sessions',
  'scrape_snapshots',
  'availability_snapshots',
  'scrape_runs',
  'watchlist_items',
  'notification_events',
];

let supabaseSchemaHealth = {
  checkedAt: null,
  tables: {},
  missingTables: [],
  currentSessionsAvailable: false,
};

function isMissingTableError(err) {
  const msg = String(err?.message || err || '').toLowerCase();
  const code = err?.code || '';
  return code === 'PGRST205'
    || code === '42P01'
    || msg.includes('could not find the table')
    || msg.includes('schema cache')
    || (msg.includes('relation') && msg.includes('does not exist'));
}

function formatSchemaError(tableName) {
  return `Missing Supabase table ${tableName}. Run supabase/schema.sql in the Supabase SQL editor.`;
}

async function probeSupabaseTable(tableName) {
  if (!supabase) return { exists: false, error: 'Supabase client not configured' };
  try {
    const { error } = await supabase
      .from(tableName)
      .select('*', { head: true, count: 'exact' })
      .limit(1);
    if (error) {
      if (isMissingTableError(error)) {
        return { exists: false, error: formatSchemaError(tableName), code: error.code || null };
      }
      return { exists: false, error: error.message, code: error.code || null };
    }
    return { exists: true };
  } catch (e) {
    return { exists: false, error: e.message };
  }
}

async function auditSupabaseSchema() {
  if (!supabase) {
    supabaseSchemaHealth = {
      checkedAt: new Date().toISOString(),
      tables: {},
      missingTables: REQUIRED_SUPABASE_TABLES.slice(),
      currentSessionsAvailable: false,
    };
    return supabaseSchemaHealth;
  }

  const tables = {};
  const missingTables = [];
  for (const table of REQUIRED_SUPABASE_TABLES) {
    const result = await probeSupabaseTable(table);
    tables[table] = result;
    if (!result.exists) missingTables.push(table);
  }

  supabaseSchemaHealth = {
    checkedAt: new Date().toISOString(),
    tables,
    missingTables,
    currentSessionsAvailable: !!tables.current_sessions?.exists,
  };

  if (missingTables.length) {
    console.error(`  Supabase schema audit: missing table(s): ${missingTables.join(', ')}`);
    console.error('  → Run supabase/schema.sql in the Supabase SQL editor for this project.');
  } else {
    console.log('  Supabase schema audit: all required tables present');
  }

  return supabaseSchemaHealth;
}

function schemaHealthPayload() {
  return {
    ...supabaseSchemaHealth,
    schemaActionRequired: supabaseSchemaHealth.missingTables.includes('current_sessions')
      ? formatSchemaError('current_sessions')
      : null,
  };
}
let slotChecksThisCycle = 0;
let weeksAvailableOnSite = null; // detected from booking UI
let effectiveWeeksAhead  = SCRAPE_WEEKS_AHEAD;
const datesCheckedDuringScrape = new Set();
let persistedDatesChecked = new Set();
let datesCheckedEmpty = new Set();
let lastFullCoverageScrape = null;
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
    lastFullCoverageScrape,
    slotCache,
    weeksScraped: lastWeeksScraped,
    datesCheckedDuringScrape: [...persistedDatesChecked],
    datesCheckedEmpty: [...datesCheckedEmpty],
  };
}

function sessionDateKey(s) {
  return s?.dateKey || s?.isoDate || null;
}

function scrapeWindowDays() {
  return Math.max(SCRAPE_WEEKS_AHEAD, effectiveWeeksAhead || 1) * 7;
}

function sessionWithinScrapeWindow(s) {
  const dk = sessionDateKey(s);
  if (!dk) return true;
  const days = daysFromToday(dk);
  return days >= 0 && days < scrapeWindowDays();
}

function allStoredSessions() {
  return asSessionArray([...sessionsByKey.values()]).filter(sessionWithinScrapeWindow);
}

function applyLoadedSnapshot(snapSessions, meta, loadedAt) {
  if (Array.isArray(snapSessions) && snapSessions.length) {
    for (const s of snapSessions) {
      if (!s?.key) continue;
      const existing = sessionsByKey.get(s.key);
      sessionsByKey.set(s.key, existing ? { ...existing, ...s } : s);
    }
    rebuildSessionsArray();
  }
  if (meta) {
    if (meta.weeksAvailableOnSite != null) weeksAvailableOnSite = meta.weeksAvailableOnSite;
    if (meta.effectiveWeeksAhead != null) effectiveWeeksAhead = meta.effectiveWeeksAhead;
    if (meta.lastTierRun) Object.assign(lastTierRun, meta.lastTierRun);
    if (meta.lastFullCoverageScrape) lastFullCoverageScrape = meta.lastFullCoverageScrape;
    if (meta.slotCache && typeof meta.slotCache === 'object') slotCache = meta.slotCache;
    if (meta.weeksScraped != null) lastWeeksScraped = meta.weeksScraped;
    if (Array.isArray(meta.datesCheckedDuringScrape)) {
      for (const d of meta.datesCheckedDuringScrape) {
        datesCheckedDuringScrape.add(d);
        persistedDatesChecked.add(d);
      }
    }
    if (Array.isArray(meta.datesCheckedEmpty)) {
      for (const d of meta.datesCheckedEmpty) datesCheckedEmpty.add(d);
    }
  }
  if (loadedAt) {
    lastSuccessfulScrape = loadedAt;
    lastCheck = loadedAt;
  }
}

function normalizeDataSource(src) {
  if (!src) return supabaseConfigured ? 'supabase/current_sessions' : 'memory-fallback';
  const s = String(src);
  if (s.includes('scrape_snapshots')) return 'supabase/scrape_snapshots_fallback';
  if (s.includes('schema-missing') || s.includes('schema fallback')) return s;
  if (src === 'supabase/current_sessions' || src === 'supabase-current' || src === 'supabase-cache' || src === 'supabase') {
    return 'supabase/current_sessions';
  }
  if (src === 'memory' && supabaseConfigured) return 'supabase/current_sessions';
  return 'memory-fallback';
}

function datesCheckedForUi() {
  return [...persistedDatesChecked].sort();
}

function sessionsForDate(dateKey) {
  if (!dateKey) return [];
  return allStoredSessions().filter(s => sessionDateKey(s) === dateKey);
}

function normalizeIsoDateParam(raw) {
  if (!raw || typeof raw !== 'string') return null;
  const trimmed = raw.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
  const m = trimmed.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function isoDateFromRow(row) {
  const raw = row?.iso_date ?? row?.isoDate;
  if (!raw) return null;
  if (typeof raw === 'string') return raw.slice(0, 10);
  return String(raw).slice(0, 10);
}

async function loadSessionsForDateFromSupabase(isoDate) {
  if (!isoDate) {
    return { sessions: [], dataSource: 'none', schemaError: null, isFallback: false };
  }

  if (!supabase) {
    await ensureSessionsForStatus();
    const chain = await loadSessionsForDateAllSources(isoDate);
    return { ...chain, schemaError: null };
  }

  if (!supabaseSchemaHealth.currentSessionsAvailable && supabaseSchemaHealth.checkedAt) {
    return loadSessionsForDateFallback(isoDate, formatSchemaError('current_sessions'));
  }

  try {
    const chain = await loadSessionsForDateAllSources(isoDate);
    if (chain.sessions.length || chain.isFallback) {
      supabaseSchemaHealth.currentSessionsAvailable = true;
      return { ...chain, schemaError: null };
    }

    if (persistedDatesChecked.has(isoDate) || datesCheckedEmpty.has(isoDate)) {
      return { sessions: [], dataSource: 'supabase/current_sessions', schemaError: null, isFallback: false };
    }

    return { sessions: [], dataSource: 'supabase/current_sessions', schemaError: null, isFallback: false };
  } catch (e) {
    if (isMissingTableError(e)) {
      return loadSessionsForDateFallback(isoDate, formatSchemaError('current_sessions'));
    }
    console.error(`  Supabase current_sessions load for ${isoDate} failed:`, e.message);
    throw e;
  }
}

async function loadSessionsForDateFallback(isoDate, schemaError) {
  const chain = await loadSessionsForDateAllSources(isoDate, { skipPrimary: true });
  if (chain.sessions.length) {
    return {
      sessions: chain.sessions,
      dataSource: chain.dataSource,
      schemaError: schemaError || null,
      isFallback: chain.isFallback,
    };
  }

  let dateSessions = sessionsForDate(isoDate);
  let src = normalizeDataSource(dataSource);

  if (!dateSessions.length) {
    const loaded = await loadLatestSnapshotFromSupabase();
    if (loaded) {
      dateSessions = sessionsForDate(isoDate);
      if (dateSessions.length) src = 'supabase/scrape_snapshots_fallback';
    }
  }

  if (dateSessions.length) {
    return {
      sessions: dateSessions,
      dataSource: schemaError ? `${src}; schema fallback` : src,
      schemaError: schemaError || null,
      isFallback: true,
    };
  }

  return {
    sessions: [],
    dataSource: schemaError ? 'schema-missing' : src,
    schemaError: schemaError || null,
    isFallback: false,
  };
}

async function fetchLatestSnapshotSessions() {
  if (!supabase) return [];
  try {
    const { data, error } = await supabase
      .from('scrape_snapshots')
      .select('sessions')
      .eq('id', 'latest')
      .maybeSingle();
    if (error) throw error;
    return asSessionArray(data?.sessions);
  } catch (e) {
    console.warn('  fetchLatestSnapshotSessions failed:', e.message);
    return [];
  }
}

function snapshotJsonToSession(s) {
  if (!s?.key) return null;
  return normalizeSessionFromSource(s);
}

function availabilityRowToSession(row) {
  if (!row?.session_key) return null;
  return currentRowToSession({
    ...row,
    slots_available: row.slots_available,
    raw: row.raw && typeof row.raw === 'object' ? row.raw : {},
  });
}

function normalizeSessionFromSource(s) {
  const key = s.key || s.session_key;
  if (!key) return null;
  return {
    ...s,
    key,
    dateKey: s.dateKey || s.isoDate || s.iso_date || null,
    isoDate: s.isoDate || s.iso_date || s.dateKey || null,
    level: s.level || s.session_type || null,
    waveSide: s.waveSide || s.wave_side || null,
    slots: s.slots ?? s.slots_available ?? null,
    priceText: s.priceText ?? s.price_text ?? null,
    priceMin: s.priceMin ?? s.price_min ?? null,
    priceMax: s.priceMax ?? s.price_max ?? null,
  };
}

async function loadSessionsForDateFromSnapshotBlob(isoDate) {
  const snapSessions = await fetchLatestSnapshotSessions();
  return snapSessions
    .map(snapshotJsonToSession)
    .filter(s => s && sessionDateKey(s) === isoDate);
}

async function loadSessionsForDateFromAvailabilitySnapshots(isoDate) {
  if (!supabase) return [];
  try {
    const { data, error } = await supabase
      .from('availability_snapshots')
      .select('*')
      .eq('park', PARK)
      .eq('iso_date', isoDate)
      .order('scraped_at', { ascending: false })
      .limit(500);
    if (error) throw error;
    const byKey = new Map();
    for (const row of data || []) {
      if (!byKey.has(row.session_key)) {
        const s = availabilityRowToSession(row);
        if (s) byKey.set(row.session_key, s);
      }
    }
    return [...byKey.values()];
  } catch (e) {
    console.warn(`  availability_snapshots load for ${isoDate} failed:`, e.message);
    return [];
  }
}

function mergeSessionsIntoStore(sessionList, { onlyMissing = false } = {}) {
  let added = 0;
  let updated = 0;
  for (const raw of asSessionArray(sessionList)) {
    const s = normalizeSessionFromSource(raw);
    if (!s?.key || !sessionWithinScrapeWindow(s)) continue;
    if (onlyMissing && sessionsByKey.has(s.key)) continue;
    const existing = sessionsByKey.get(s.key);
    sessionsByKey.set(s.key, existing ? mergeSessionFieldsForUpsert(s, existing, { scrapeKind: 'basic' }) : s);
    if (existing) updated++;
    else added++;
  }
  if (added || updated) rebuildSessionsArray();
  return { added, updated };
}

async function mergeBroadSnapshotIntoStore() {
  const snapSessions = await fetchLatestSnapshotSessions();
  return mergeSessionsIntoStore(snapSessions, { onlyMissing: true });
}

async function loadSessionsForDateAllSources(isoDate, { skipPrimary = false } = {}) {
  if (!isoDate) {
    return { sessions: [], dataSource: 'none', isFallback: false };
  }

  if (!skipPrimary && supabase && supabaseSchemaHealth.currentSessionsAvailable !== false) {
    try {
      const { data, error } = await supabase
        .from('current_sessions')
        .select('*')
        .eq('park', PARK)
        .eq('iso_date', isoDate)
        .order('start_ts', { ascending: true });
      if (!error && data?.length) {
        const dateSessions = data.map(currentRowToSession).filter(s => s?.key);
        mergeSessionsIntoStore(dateSessions);
        return {
          sessions: dateSessions,
          dataSource: 'supabase/current_sessions',
          isFallback: false,
        };
      }
      if (error && !isMissingTableError(error)) throw error;
    } catch (e) {
      if (!isMissingTableError(e)) console.warn(`  current_sessions date query ${isoDate}:`, e.message);
    }
  }

  const fromSnapshot = await loadSessionsForDateFromSnapshotBlob(isoDate);
  if (fromSnapshot.length) {
    mergeSessionsIntoStore(fromSnapshot);
    return {
      sessions: fromSnapshot,
      dataSource: 'supabase/scrape_snapshots_fallback',
      isFallback: true,
    };
  }

  const fromAvailability = await loadSessionsForDateFromAvailabilitySnapshots(isoDate);
  if (fromAvailability.length) {
    mergeSessionsIntoStore(fromAvailability);
    return {
      sessions: fromAvailability,
      dataSource: 'supabase/availability_snapshots_fallback',
      isFallback: true,
    };
  }

  const mem = sessionsForDate(isoDate);
  if (mem.length) {
    return {
      sessions: mem,
      dataSource: normalizeDataSource(dataSource),
      isFallback: String(dataSource).includes('fallback'),
    };
  }

  return { sessions: [], dataSource: 'none', isFallback: false };
}

function isCurrentSessionsSparse() {
  const map = currentSessionsByDateMap();
  const expected = expectedDatesInScrapeWindow();
  const datesWithSessions = expected.filter(d => (map[d] || 0) > 0);
  const futureWithSessions = datesWithSessions.filter(d => daysFromToday(d) > 1);
  if (datesWithSessions.length === 0) return true;
  if (expected.length > 3 && futureWithSessions.length === 0) return true;
  return datesWithSessions.length <= 2 && expected.length > 2;
}

async function checkFallbackAvailable() {
  if (!supabase) return sessionsByKey.size > 0;
  try {
    const snap = await fetchLatestSnapshotSessions();
    if (snap.length > sessionsByKey.size) return true;
    const { count, error } = await supabase
      .from('availability_snapshots')
      .select('*', { count: 'exact', head: true })
      .eq('park', PARK)
      .gte('iso_date', todayDateKey());
    if (!error && (count || 0) > 0) return true;
  } catch (e) {
    console.warn('  checkFallbackAvailable failed:', e.message);
  }
  return false;
}

function isBackfillRecommended() {
  return isCurrentSessionsSparse();
}

async function gatherBackfillCandidates() {
  const byKey = new Map();
  const sourcesUsed = new Set();

  for (const s of allStoredSessions()) {
    if (!s?.key) continue;
    byKey.set(s.key, { session: s, source: 'memory', scrapedAt: s.lastScraped || s.lastScrapedAt || null });
    sourcesUsed.add('memory');
  }

  const snapSessions = await fetchLatestSnapshotSessions();
  if (snapSessions.length) {
    sourcesUsed.add('scrape_snapshots');
    for (const raw of snapSessions) {
      const s = snapshotJsonToSession(raw);
      if (!s?.key) continue;
      if (!byKey.has(s.key)) {
        byKey.set(s.key, { session: s, source: 'scrape_snapshots', scrapedAt: null });
      }
    }
  }

  if (supabase) {
    try {
      const windowEnd = expectedDatesInScrapeWindow().slice(-1)[0];
      const { data, error } = await supabase
        .from('availability_snapshots')
        .select('*')
        .eq('park', PARK)
        .gte('iso_date', todayDateKey())
        .lte('iso_date', windowEnd || '9999-12-31')
        .order('scraped_at', { ascending: false })
        .limit(15000);
      if (!error && data?.length) {
        sourcesUsed.add('availability_snapshots');
        for (const row of data) {
          if (byKey.has(row.session_key)) continue;
          const s = availabilityRowToSession(row);
          if (s?.key) {
            byKey.set(s.key, { session: s, source: 'availability_snapshots', scrapedAt: row.scraped_at });
          }
        }
      }
    } catch (e) {
      console.warn('  gatherBackfillCandidates availability_snapshots:', e.message);
    }
  }

  return { byKey, sourcesUsed: [...sourcesUsed] };
}

async function backfillCurrentSessions({ allowOverwrite = false } = {}) {
  const errors = [];
  const sparse = isCurrentSessionsSparse();
  const existingRows = new Map();

  if (supabase) {
    try {
      const { data, error } = await supabase
        .from('current_sessions')
        .select('session_key, last_scraped_at')
        .eq('park', PARK);
      if (error) throw error;
      for (const row of data || []) existingRows.set(row.session_key, row.last_scraped_at);
    } catch (e) {
      errors.push(e.message);
    }
  }

  const { byKey, sourcesUsed } = await gatherBackfillCandidates();
  const toUpsert = [];

  for (const { session, scrapedAt } of byKey.values()) {
    if (!session?.key || !sessionWithinScrapeWindow(session)) continue;
    const existingAt = existingRows.get(session.key);
    if (existingAt && !allowOverwrite && !sparse) {
      const existingTime = new Date(existingAt).getTime();
      const candidateTime = scrapedAt ? new Date(scrapedAt).getTime() : 0;
      if (candidateTime && existingTime >= candidateTime) continue;
      if (!candidateTime && existingRows.has(session.key)) continue;
    }
    toUpsert.push(session);
  }

  const sessionsByDate = {};
  for (const s of toUpsert) {
    const dk = sessionDateKey(s);
    if (dk) sessionsByDate[dk] = (sessionsByDate[dk] || 0) + 1;
  }

  let rowsUpserted = 0;
  if (toUpsert.length) {
    rowsUpserted = await upsertCurrentSessionsToSupabase(toUpsert, 0, { scrapeKind: 'basic' });
    mergeSessionsIntoStore(toUpsert);
    await saveLatestSnapshotToSupabase();
  }

  return {
    sourceUsed: sourcesUsed.join(', ') || 'none',
    rowsFound: byKey.size,
    rowsUpserted,
    datesCovered: Object.keys(sessionsByDate).sort(),
    sessionsByDate,
    errors,
    sparseBefore: sparse,
  };
}

async function startupCoverageCheck() {
  const sparse = isCurrentSessionsSparse();
  const fallback = await checkFallbackAvailable();
  console.log(`  Coverage check: ${Object.keys(currentSessionsByDateMap()).length} date(s) with sessions, sparse=${sparse}, fallbackAvailable=${fallback}`);

  if (sparse && fallback) {
    console.log('  Sparse current_sessions — running backfill from saved snapshot/history…');
    const result = await backfillCurrentSessions();
    console.log(`  Backfill: ${result.rowsUpserted} row(s) upserted from ${result.sourceUsed}, dates: ${result.datesCovered.join(', ') || 'none'}`);
    await refreshCoverageFlags();
    if (result.rowsUpserted === 0 && !scrapeInProgress) {
      console.log('  Backfill found nothing to upsert — scheduling Tier 2 broad scrape…');
      setTimeout(() => runTierScrape(2).catch(console.error), 8_000);
    }
    return result;
  }

  if (sparse && !scrapeInProgress) {
    console.log('  Sparse coverage, no fallback — scheduling Tier 2 scrape…');
    setTimeout(() => runTierScrape(2).catch(console.error), 8_000);
  }

  return { sparse, fallbackAvailable: fallback };
}

async function queryCurrentSessionsByDateFromDb() {
  if (!supabase) return currentSessionsByDateMap();
  try {
    const { data, error } = await supabase
      .from('current_sessions')
      .select('iso_date, last_basic_check_at, last_detailed_check_at, slots_available')
      .eq('park', PARK);
    if (error) throw error;
    const byDate = {};
    const basicDates = new Set();
    const detailedDates = new Set();
    for (const row of data || []) {
      const dk = isoDateFromRow(row);
      if (!dk) continue;
      byDate[dk] = (byDate[dk] || 0) + 1;
      if (row.last_basic_check_at) basicDates.add(dk);
      if (row.last_detailed_check_at || row.slots_available != null) detailedDates.add(dk);
    }
    return { byDate, basicDates: [...basicDates].sort(), detailedDates: [...detailedDates].sort() };
  } catch (e) {
    console.warn('  queryCurrentSessionsByDateFromDb failed:', e.message);
    const map = currentSessionsByDateMap();
    return { byDate: map, basicDates: [], detailedDates: [] };
  }
}

async function queryAvailabilitySnapshotsByDate() {
  const byDate = {};
  if (!supabase) return byDate;
  try {
    const windowEnd = expectedDatesInScrapeWindow().slice(-1)[0];
    const { data, error } = await supabase
      .from('availability_snapshots')
      .select('iso_date')
      .eq('park', PARK)
      .gte('iso_date', todayDateKey())
      .lte('iso_date', windowEnd || '9999-12-31');
    if (error) throw error;
    for (const row of data || []) {
      const dk = String(row.iso_date).slice(0, 10);
      byDate[dk] = (byDate[dk] || 0) + 1;
    }
  } catch (e) {
    console.warn('  queryAvailabilitySnapshotsByDate failed:', e.message);
  }
  return byDate;
}

async function fetchScrapeSnapshotMeta() {
  if (!supabase) {
    const snap = await fetchLatestSnapshotSessions();
    return {
      scrapeSnapshotsAvailable: snap.length > 0,
      scrapeSnapshotsSessionCount: snap.length,
      latestScrapeSnapshotAt: lastLatestSnapshotSavedAt,
      scrapeSnapshotsByDate: {},
    };
  }
  try {
    const { data, error } = await supabase
      .from('scrape_snapshots')
      .select('updated_at, sessions, last_successful_scrape')
      .eq('id', 'latest')
      .maybeSingle();
    if (error) throw error;
    const sessions = asSessionArray(data?.sessions);
    const byDate = {};
    for (const s of sessions) {
      const dk = sessionDateKey(snapshotJsonToSession(s));
      if (dk) byDate[dk] = (byDate[dk] || 0) + 1;
    }
    return {
      scrapeSnapshotsAvailable: sessions.length > 0,
      scrapeSnapshotsSessionCount: sessions.length,
      latestScrapeSnapshotAt: data?.updated_at || data?.last_successful_scrape || null,
      scrapeSnapshotsByDate: byDate,
    };
  } catch (e) {
    return {
      scrapeSnapshotsAvailable: false,
      scrapeSnapshotsSessionCount: 0,
      latestScrapeSnapshotAt: null,
      scrapeSnapshotsByDate: {},
    };
  }
}

async function buildCoverageDebugPayload() {
  const expected = expectedDatesInScrapeWindow();
  const dbStats = await queryCurrentSessionsByDateFromDb();
  const currentMap = dbStats.byDate;
  const availByDate = await queryAvailabilitySnapshotsByDate();
  const snapMeta = await fetchScrapeSnapshotMeta();
  const snapDates = Object.keys(snapMeta.scrapeSnapshotsByDate || {});

  const datesInCurrentSessions = expected.filter(d => (currentMap[d] || 0) > 0).sort();
  const missingDatesFromCurrentSessions = expected.filter(d => !(currentMap[d] > 0));
  const coverage = computeDateCoverage();
  const detailStats = detailCoverageStats();
  const sparse = isCurrentSessionsSparse();
  const fallback = await checkFallbackAvailable();

  let recommendedAction = 'none';
  if (sparse && fallback) recommendedAction = 'POST /api/admin/backfill-current-sessions';
  else if (sparse) recommendedAction = 'wait for Tier 2/3 scrape or run tier scrape manually';
  else if (missingDatesFromCurrentSessions.length) recommendedAction = 'background Tier 2/3 will fill missing dates';

  return {
    scrapeWeeksAhead: SCRAPE_WEEKS_AHEAD,
    effectiveWeeksAhead,
    expectedDates: expected,
    expectedDatesCount: expected.length,
    currentSessionsCount: Object.values(currentMap).reduce((a, b) => a + b, 0),
    currentSessionsByDate: currentMap,
    availabilitySnapshotsByDate: availByDate,
    scrapeSnapshotsAvailable: snapMeta.scrapeSnapshotsAvailable,
    scrapeSnapshotsSessionCount: snapMeta.scrapeSnapshotsSessionCount,
    latestScrapeSnapshotAt: snapMeta.latestScrapeSnapshotAt,
    scrapeSnapshotsByDate: snapMeta.scrapeSnapshotsByDate,
    datesInCurrentSessions,
    datesInScrapeSnapshots: snapDates.filter(d => expected.includes(d)).sort(),
    datesInAvailabilitySnapshots: Object.keys(availByDate).filter(d => expected.includes(d)).sort(),
    missingDates: missingDatesFromCurrentSessions,
    missingDatesFromCurrentSessions,
    datesWithBasicRows: dbStats.basicDates.filter(d => expected.includes(d)),
    datesWithDetailedRows: dbStats.detailedDates.filter(d => expected.includes(d)),
    sessionsByDate: currentMap,
    coveragePercent: coverage.sessionsCoveragePercent,
    sessionsCoveragePercent: coverage.sessionsCoveragePercent,
    detailCoveragePercent: detailStats.detailCoveragePercent,
    sparse,
    fallbackAvailable: fallback,
    backfillRecommended: sparse && fallback,
    recommendedAction,
    lastTier1Scrape: lastTierRun[1],
    lastTier2Scrape: lastTierRun[2],
    lastTier3Scrape: lastTierRun[3],
    lastFullCoverageScrape,
    scrapeInProgress,
    backgroundCollectorEnabled: scrapeScheduleEnabled,
    inMemorySessionsCount: sessionsByKey.size,
    inMemoryDatesCount: Object.keys(currentSessionsByDateMap()).length,
  };
}

function dateCoverageForIsoDate(isoDate) {
  const wasChecked = persistedDatesChecked.has(isoDate) || datesCheckedEmpty.has(isoDate);
  const checkedEmpty = datesCheckedEmpty.has(isoDate);
  const inMemoryCount = currentSessionsByDateMap()[isoDate] || 0;
  return {
    isoDate,
    sessionsForDate: inMemoryCount,
    wasDateChecked: wasChecked,
    checkedEmpty,
    inPersistedDatesChecked: persistedDatesChecked.has(isoDate),
    inDatesCheckedEmpty: datesCheckedEmpty.has(isoDate),
  };
}

function statusReasonForDate(isoDate, dateSessions, { isFallback = false } = {}) {
  if (dateSessions.length > 0) {
    return isFallback ? 'fallback_sessions_found' : 'saved_sessions_found';
  }
  if (datesCheckedEmpty.has(isoDate)) return 'checked_no_sessions';
  if (persistedDatesChecked.has(isoDate)) return 'checked_no_sessions';
  return 'not_checked';
}

function dateCheckTimestamps(dateSessions) {
  const basic = asSessionArray(dateSessions)
    .map(s => s.lastBasicCheckAt || s.last_basic_check_at)
    .filter(Boolean)
    .sort()
    .pop() || null;
  const detailed = asSessionArray(dateSessions)
    .map(s => s.lastDetailedCheckAt || s.last_detailed_check_at)
    .filter(Boolean)
    .sort()
    .pop() || null;
  return { lastBasicCheckAt: basic, lastDetailedCheckAt: detailed };
}

function resolveDateUiDisplay(isoDate, { sessions, statusReason, isFallback = false } = {}) {
  const count = asSessionArray(sessions).length;
  if (count > 0) {
    return {
      statusReason: statusReason || (isFallback ? 'fallback_sessions_found' : 'saved_sessions_found'),
      uiDisplay: 'sessions',
      uiMessage: `Render ${count} session card(s)${isFallback ? ' (saved snapshot fallback)' : ''}`,
    };
  }
  if (statusReason === 'checked_no_sessions') {
    return { statusReason, uiDisplay: 'empty', uiMessage: 'No sessions found for this date' };
  }
  if (statusReason === 'schema_error') {
    return { statusReason, uiDisplay: 'error', uiMessage: 'Database schema missing — run supabase/schema.sql' };
  }
  if (statusReason === 'error') {
    return { statusReason, uiDisplay: 'error', uiMessage: 'Could not load sessions for this date' };
  }
  return { statusReason: statusReason || 'not_checked', uiDisplay: 'not_checked', uiMessage: 'Not checked yet' };
}

async function buildSessionsForDatePayload(isoDate) {
  if (!persistedDatesChecked.size && supabase) {
    await loadScrapeMetaFromSupabase();
  }

  const { sessions: dateSessions, dataSource: src, schemaError, isFallback = false } =
    await loadSessionsForDateFromSupabase(isoDate);

  if (schemaError && !dateSessions.length) {
    return {
      isoDate,
      sessions: [],
      sessionsCount: 0,
      dataSource: 'schema-missing',
      lastSuccessfulScrape,
      lastCheckedForDate: null,
      wasDateChecked: false,
      isScrapeInProgress: scrapeInProgress,
      hasSavedSessions: false,
      statusReason: 'schema_error',
      error: schemaError,
      schemaError,
      dateCoverage: dateCoverageForIsoDate(isoDate),
      schemaHealth: schemaHealthPayload(),
    };
  }

  const hasSaved = dateSessions.length > 0;
  const wasChecked = hasSaved || persistedDatesChecked.has(isoDate) || datesCheckedEmpty.has(isoDate);
  const statusReason = statusReasonForDate(isoDate, dateSessions, { isFallback });
  const checkTimes = dateCheckTimestamps(dateSessions);
  return {
    isoDate,
    sessions: dateSessions,
    sessionsCount: dateSessions.length,
    dataSource: src,
    isFallback,
    lastSuccessfulScrape,
    lastBasicCheckAt: checkTimes.lastBasicCheckAt,
    lastDetailedCheckAt: checkTimes.lastDetailedCheckAt,
    lastCheckedForDate: wasChecked ? (lastSuccessfulScrape || lastTierRun[1] || lastTierRun[2] || null) : null,
    wasDateChecked: wasChecked,
    isScrapeInProgress: scrapeInProgress,
    hasSavedSessions: hasSaved,
    statusReason,
    schemaError: schemaError || null,
    error: schemaError || null,
    dateCoverage: dateCoverageForIsoDate(isoDate),
    schemaHealth: schemaHealthPayload(),
  };
}

function currentSessionsByDateMap() {
  const map = {};
  for (const s of allStoredSessions()) {
    const dk = sessionDateKey(s);
    if (!dk) continue;
    map[dk] = (map[dk] || 0) + 1;
  }
  return map;
}

function recordTierDateCoverage(datesSeen) {
  if (!datesSeen) return;
  const seenDates = datesSeen instanceof Set ? [...datesSeen] : asSessionArray(datesSeen);
  for (const d of seenDates) {
    datesCheckedDuringScrape.add(d);
    persistedDatesChecked.add(d);
    const hasSessions = allStoredSessions().some(s => sessionDateKey(s) === d);
    if (!hasSessions) datesCheckedEmpty.add(d);
    else datesCheckedEmpty.delete(d);
  }
}

function buildSelectedDateDebug(selectedDate) {
  if (!selectedDate) return null;
  const dateSessions = sessionsForDate(selectedDate);
  const hasSaved = dateSessions.length > 0;
  const wasChecked = hasSaved || persistedDatesChecked.has(selectedDate);
  const checkedEmpty = datesCheckedEmpty.has(selectedDate);
  let uiReason = 'not_checked';
  if (hasSaved) uiReason = 'has_sessions';
  else if (checkedEmpty || (wasChecked && !hasSaved)) uiReason = 'checked_empty';
  else if (scrapeInProgress) uiReason = 'checking';
  return {
    selectedDate,
    selectedDateHasSavedSessions: hasSaved,
    selectedDateWasChecked: wasChecked,
    selectedDateCheckedEmpty: checkedEmpty,
    sessionsForSelectedDateCount: dateSessions.length,
    uiReason,
  };
}

function currentRowToSession(row) {
  const raw = row.raw && typeof row.raw === 'object' ? row.raw : {};
  const metrics = computeSessionMetrics(
    row.slots_available ?? raw.slots ?? null,
    row.capacity ?? raw.capacity ?? null,
    row.session_type || raw.level,
    { inferCapacityFromLevel: false },
  );
  return {
    ...raw,
    key: row.session_key,
    dateKey: isoDateFromRow(row) || raw.dateKey,
    isoDate: isoDateFromRow(row) || raw.isoDate,
    ts: row.start_ts ?? raw.ts,
    time: row.start_time || raw.time,
    date: row.display_date || raw.date,
    weekday: row.weekday || raw.weekday,
    waveSide: row.wave_side || raw.waveSide,
    level: row.session_type || raw.level,
    available: row.available,
    slots: metrics.slots,
    capacity: metrics.capacity,
    estimatedBooked: row.estimated_booked ?? raw.estimatedBooked ?? metrics.estimatedBooked,
    fillRate: row.fill_rate ?? raw.fillRate ?? metrics.fillRate,
    detailWarning: row.detail_error ?? raw.detailWarning ?? metrics.detailWarning ?? null,
    priceText: row.price_text ?? raw.priceText ?? null,
    priceMin: row.price_min ?? raw.priceMin ?? null,
    priceMax: row.price_max ?? raw.priceMax ?? null,
    currency: row.currency ?? raw.currency ?? 'USD',
    tier: row.source_tier ?? raw.tier,
    lastScraped: row.last_scraped_at || raw.lastScraped,
    lastBasicCheckAt: row.last_basic_check_at ?? raw.lastBasicCheckAt ?? null,
    lastDetailedCheckAt: row.last_detailed_check_at ?? raw.lastDetailedCheckAt ?? null,
    detailStatus: row.detail_status ?? raw.detailStatus ?? null,
    detailError: row.detail_error ?? raw.detailError ?? null,
  };
}

function sessionToCurrentRow(s, sourceTier, { scrapeKind = 'basic' } = {}) {
  const now = new Date().toISOString();
  const metrics = computeSessionMetrics(
    s.slots ?? null,
    s.capacity ?? null,
    s.level,
    { inferCapacityFromLevel: scrapeKind === 'detailed' && s.slots != null },
  );
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
    slots_available: metrics.slots,
    capacity: metrics.capacity,
    estimated_booked: s.estimatedBooked ?? metrics.estimatedBooked,
    fill_rate: s.fillRate ?? metrics.fillRate,
    price_text: s.priceText ?? null,
    price_min: s.priceMin ?? null,
    price_max: s.priceMax ?? null,
    currency: s.currency || 'USD',
    status_label: availabilityStatusLabel({ ...s, slots: metrics.slots }),
    source_tier: sourceTier,
    raw: { ...s, detailWarning: s.detailWarning ?? metrics.detailWarning ?? null },
    last_seen_at: now,
    last_scraped_at: now,
    last_basic_check_at: s.lastBasicCheckAt ?? (scrapeKind === 'basic' ? now : null),
    last_detailed_check_at: s.lastDetailedCheckAt ?? (scrapeKind === 'detailed' ? now : null),
    detail_status: s.detailStatus ?? null,
    detail_error: s.detailError ?? s.detailWarning ?? metrics.detailWarning ?? null,
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

function tierCoverageSummary() {
  const coverage = {};
  for (const tier of [1, 2, 3, 4]) {
    const cfg = TIER_CONFIG[tier];
    const tierSessions = allStoredSessions().filter(s => sessionInTier(s, tier));
    const dates = new Set(tierSessions.map(sessionDateKey).filter(Boolean));
    coverage[tier] = {
      label: cfg.label,
      sessionCount: tierSessions.length,
      dateCount: dates.size,
      lastRun: lastTierRun[tier] || null,
    };
  }
  return coverage;
}

async function refreshCoverageFlags() {
  fallbackAvailableCached = await checkFallbackAvailable();
}

function getStatusFields() {
  const dataAgeMinutes = lastSuccessfulScrape
    ? Math.max(0, Math.round((Date.now() - new Date(lastSuccessfulScrape).getTime()) / 60000))
    : null;
  const coverage = computeDateCoverage();
  return {
    sessionsCount: sessions.length,
    currentSessionsCount: sessionsByKey.size,
    currentSessionsByDate: currentSessionsByDateMap(),
    earliestSessionDate: coverage.earliestSessionDate,
    latestSessionDate: coverage.latestSessionDate,
    uniqueDatesCount: coverage.uniqueDatesCount,
    expectedDatesCount: coverage.expectedDatesCount,
    sessionsCoveragePercent: coverage.sessionsCoveragePercent,
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
    totalSessionsTracked: sessionsByKey.size,
    latestSnapshotSavedAt: lastLatestSnapshotSavedAt,
    historySnapshotsEnabled: supabaseConfigured && HISTORY_SNAPSHOTS_ENABLED,
    snapshotRowsInsertedLastRun: lastSnapshotRowsInsertedLastRun,
    minutesSinceLastScrape: dataAgeMinutes,
    missingDatesInScrapeWindow: coverage.missingDatesInScrapeWindow,
    coveragePercent: coverage.sessionsCoveragePercent,
    sessionsCoveragePercent: coverage.sessionsCoveragePercent,
    earliestSessionDate: coverage.earliestSessionDate,
    latestSessionDate: coverage.latestSessionDate,
    uniqueDatesCount: coverage.uniqueDatesCount,
    expectedDatesCount: coverage.expectedDatesCount,
    tierCoverage: tierCoverageSummary(),
    lastFullCoverageScrape,
    lastTier1Scrape: lastTierRun[1],
    lastTier2Scrape: lastTierRun[2],
    lastTier3Scrape: lastTierRun[3],
    lastTier4Scrape: lastTierRun[4],
    datesCheckedEmpty: [...datesCheckedEmpty].sort(),
    scrapeScheduleEnabled,
    backgroundCollectorEnabled: scrapeScheduleEnabled,
    backfillRecommended: isBackfillRecommended(),
    fallbackAvailable: fallbackAvailableCached,
    serverStartedAt,
    schemaHealth: schemaHealthPayload(),
    schemaMissingTables: supabaseSchemaHealth.missingTables,
    schemaActionRequired: supabaseSchemaHealth.missingTables.includes('current_sessions')
      ? formatSchemaError('current_sessions')
      : null,
    currentSessionsTableAvailable: supabaseSchemaHealth.currentSessionsAvailable,
    ...detailCoverageStats(),
    enrichmentQueuePending: enrichmentQueuePendingCount,
    enrichmentQueueRunning: enrichmentQueueRunningCount,
    lastDetailEnrichmentAt,
    lastDetailEnrichmentError,
    detailEnrichmentInProgress,
  };
}

async function ensureSessionsForStatus() {
  if (!supabase) return;
  await loadCurrentSessionsFromSupabase({ reloadMeta: !scrapeInProgress });
  if (!sessions.length) await loadLatestSnapshotFromSupabase();
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

async function loadCurrentSessionsFromSupabase({ reloadMeta = true } = {}) {
  if (!supabase) return false;

  if (!supabaseSchemaHealth.currentSessionsAvailable && supabaseSchemaHealth.checkedAt) {
    console.warn('  Supabase: current_sessions unavailable — using scrape_snapshots fallback');
    return loadLatestSnapshotFromSupabase();
  }

  try {
    const { data, error } = await supabase
      .from('current_sessions')
      .select('*')
      .eq('park', PARK)
      .order('start_ts', { ascending: true });
    if (error) {
      if (isMissingTableError(error)) {
        supabaseSchemaHealth.currentSessionsAvailable = false;
        if (!supabaseSchemaHealth.missingTables.includes('current_sessions')) {
          supabaseSchemaHealth.missingTables.push('current_sessions');
        }
        console.error(`  ${formatSchemaError('current_sessions')}`);
        return loadLatestSnapshotFromSupabase({ fallback: true });
      }
      throw error;
    }

    supabaseSchemaHealth.currentSessionsAvailable = true;
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
    if (reloadMeta) await loadScrapeMetaFromSupabase();

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

    dataSource = 'supabase/current_sessions';
    hasFreshScrapeThisBoot = false;

    const age = lastSuccessfulScrape
      ? Math.round((Date.now() - new Date(lastSuccessfulScrape).getTime()) / 60000)
      : '?';
    console.log(`  Supabase: loaded ${sessionsByKey.size} current session(s) (${sessions.length} in serve window, last scrape ${age}m ago)`);
    return true;
  } catch (e) {
    if (isMissingTableError(e)) {
      console.error(`  ${formatSchemaError('current_sessions')}`);
      return loadLatestSnapshotFromSupabase({ fallback: true });
    }
    console.error('  Supabase current_sessions load failed:', e.message);
    return false;
  }
}

async function upsertCurrentSessionsToSupabase(scrapedSessions, sourceTier, { scrapeKind = 'basic' } = {}) {
  if (!supabase) return 0;
  const batch = asSessionArray(scrapedSessions);
  if (!batch.length) return 0;

  try {
    const rows = batch.map((s) => {
      const existing = sessionsByKey.get(s.key);
      const kind = scrapeKind === 'detailed' || sessionHasDetailedData(s) ? 'detailed' : 'basic';
      const merged = mergeSessionFieldsForUpsert(s, existing, { scrapeKind: kind });
      sessionsByKey.set(s.key, merged);
      return sessionToCurrentRow(merged, sourceTier, { scrapeKind: kind });
    });
    let upserted = 0;
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const { error } = await supabase
        .from('current_sessions')
        .upsert(chunk, { onConflict: 'park,session_key' });
      if (error) throw error;
      upserted += chunk.length;
    }
    rebuildSessionsArray();
    console.log(`  Supabase: upserted ${upserted} current_sessions row(s) (${scrapeKind})`);
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

async function loadLatestSnapshotFromSupabase({ fallback = false } = {}) {
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
    dataSource = fallback ? 'supabase/scrape_snapshots_fallback' : 'supabase';
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
    const byKey = new Map(allStoredSessions().map(s => [s.key, s]));
    const existingSnap = await fetchLatestSnapshotSessions();
    for (const raw of existingSnap) {
      const s = snapshotJsonToSession(raw);
      if (!s?.key || !sessionWithinScrapeWindow(s)) continue;
      if (!byKey.has(s.key)) byKey.set(s.key, s);
    }
    const snapshotSessions = [...byKey.values()].sort((a, b) => (a.ts || 0) - (b.ts || 0) || (a.wave || 0) - (b.wave || 0));
    const { error } = await supabase.from('scrape_snapshots').upsert({
      id: 'latest',
      sessions: snapshotSessions,
      scrape_meta: buildScrapeMetaPayload(),
      last_successful_scrape: lastSuccessfulScrape,
      last_scrape_attempt: lastScrapeAttempt || lastSuccessfulScrape || now,
      last_scrape_error: null,
      updated_at: now,
    }, { onConflict: 'id' });
    if (error) throw error;
    lastLatestSnapshotSavedAt = now;
    console.log(`  Supabase: saved snapshot (${snapshotSessions.length} sessions, ${persistedDatesChecked.size} dates checked)`);
  } catch (e) {
    console.error('  Supabase save failed:', e.message);
  }
}

async function prunePastSessionsFromSupabase() {
  if (!supabase) return;
  const today = todayDateKey();
  try {
    const { error } = await supabase
      .from('current_sessions')
      .delete()
      .eq('park', PARK)
      .lt('iso_date', today);
    if (error) throw error;
  } catch (e) {
    console.error('  Supabase prune past sessions failed:', e.message);
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

function computeSessionMetrics(slots, capacity, level, { inferCapacityFromLevel = false } = {}) {
  const slotsNum = slots != null && Number.isFinite(Number(slots)) ? Number(slots) : null;
  let cap = capacity != null && Number.isFinite(Number(capacity)) ? Number(capacity) : null;
  if (cap == null && inferCapacityFromLevel && slotsNum != null) {
    cap = sessionCapacityForLevel(level);
  }
  let estimatedBooked = null;
  let fillRate = null;
  let detailWarning = null;

  if (slotsNum != null && cap != null) {
    if (slotsNum > cap) {
      detailWarning = 'slots_exceed_capacity';
    } else {
      estimatedBooked = cap - slotsNum;
      fillRate = cap > 0 ? estimatedBooked / cap : null;
    }
  }

  return { slots: slotsNum, capacity: cap, estimatedBooked, fillRate, detailWarning };
}

function sessionHasDetailedData(s) {
  if (!s) return false;
  return s.slots != null || s.capacity != null || s.priceText != null || s.priceMin != null;
}

function sessionNeedsDetailEnrichment(s, maxAgeHours = ENRICHMENT_STALE_HOURS) {
  if (!s?.available || !s.key) return false;
  if (!sessionHasDetailedData(s)) return true;
  if (!s.lastDetailedCheckAt) return true;
  const ageMs = Date.now() - new Date(s.lastDetailedCheckAt).getTime();
  return ageMs > maxAgeHours * 3_600_000;
}

function enrichmentPriorityForSession(s) {
  const watchKeys = watchedSessionKeys();
  if (watchKeys.has(s.key)) return 1;
  const isoDate = s.isoDate || s.dateKey;
  if (isoDate && isoDate === lastRequestedDateForEnrichment) return 1;
  const days = daysFromToday(isoDate || todayDateKey());
  if (days <= 1) return 1;
  if (days <= 2) return 1;
  if (days <= 7) return 2;
  return 3;
}

function mergeSessionFieldsForUpsert(incoming, existing, { scrapeKind = 'basic' } = {}) {
  const now = new Date().toISOString();
  const merged = { ...(existing || {}), ...incoming };
  const preserveFields = [
    'slots', 'capacity', 'estimatedBooked', 'fillRate',
    'priceText', 'priceMin', 'priceMax', 'currency',
    'lastDetailedCheckAt', 'detailError', 'detailWarning',
  ];

  if (scrapeKind === 'basic' && existing) {
    for (const field of preserveFields) {
      if (existing[field] != null && incoming[field] == null) merged[field] = existing[field];
    }
    if (existing.detailStatus === 'checked' && sessionHasDetailedData(merged)) {
      merged.detailStatus = 'checked';
    }
  }

  if (scrapeKind === 'basic') {
    merged.lastBasicCheckAt = now;
    if (!sessionHasDetailedData(merged) && merged.detailStatus !== 'checking') {
      merged.detailStatus = merged.detailStatus || 'pending';
    }
  }

  if (scrapeKind === 'detailed') {
    merged.lastDetailedCheckAt = now;
    merged.detailStatus = sessionHasDetailedData(merged) ? 'checked' : (merged.available ? 'failed' : 'checked');
    if (merged.detailWarning) merged.detailError = merged.detailWarning;
    else if (merged.detailStatus === 'checked') merged.detailError = null;
  }

  return merged;
}

function detailCoverageStats() {
  const all = allStoredSessions();
  const withSlots = all.filter(s => s.slots != null);
  const withCapacity = all.filter(s => s.capacity != null);
  const withPrice = all.filter(s => s.priceText || s.priceMin != null);
  const needing = all.filter(s => sessionNeedsDetailEnrichment(s));
  const total = all.length || 1;
  return {
    sessionsWithSlotsCount: withSlots.length,
    sessionsWithCapacityCount: withCapacity.length,
    sessionsWithPriceCount: withPrice.length,
    sessionsNeedingDetailCount: needing.length,
    detailCoveragePercent: Math.round((withSlots.length / total) * 100),
  };
}

function applyDetailPayloadToSession(entry, details, level) {
  if (!entry || !details) return false;
  const now = new Date().toISOString();
  if (details.slots != null) entry.slots = details.slots;
  attachSessionMetrics(entry, details, level, { scrapeKind: 'detailed' });
  if (details.price_text || details.price_min != null) {
    entry.priceText = details.price_text ?? entry.priceText;
    entry.priceMin = details.price_min ?? entry.priceMin;
    entry.priceMax = details.price_max ?? entry.priceMax;
    entry.currency = details.currency || entry.currency || 'USD';
  }
  entry.lastDetailedCheckAt = now;
  entry.detailStatus = sessionHasDetailedData(entry) ? 'checked' : 'failed';
  entry.detailError = entry.detailWarning || null;
  return sessionHasDetailedData(entry);
}

function attachSessionMetrics(entry, details, level, { scrapeKind = 'detailed' } = {}) {
  if (!entry) return;
  const slots = entry.slots ?? details?.slots ?? null;
  const capacityHint = details?.capacity ?? entry.capacity ?? null;
  const metrics = computeSessionMetrics(slots, capacityHint, level, {
    inferCapacityFromLevel: scrapeKind === 'detailed' && slots != null,
  });
  if (metrics.slots != null) entry.slots = metrics.slots;
  if (metrics.capacity != null) entry.capacity = metrics.capacity;
  if (metrics.estimatedBooked != null) entry.estimatedBooked = metrics.estimatedBooked;
  if (metrics.fillRate != null) entry.fillRate = metrics.fillRate;
  if (metrics.detailWarning) {
    entry.detailWarning = metrics.detailWarning;
    entry.estimatedBooked = null;
    entry.fillRate = null;
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

async function saveAvailabilitySnapshotsToSupabase(scrapedSessions, sourceTier, { snapshotType = 'basic' } = {}) {
  if (!supabase || !HISTORY_SNAPSHOTS_ENABLED) return 0;
  const batch = asSessionArray(scrapedSessions);
  if (!batch.length) return 0;

  try {
    const scrapedAt = new Date().toISOString();
    const rows = batch.map((s) => {
      const metrics = computeSessionMetrics(
        s.slots ?? null,
        s.capacity ?? null,
        s.level,
        { inferCapacityFromLevel: snapshotType === 'detailed' && s.slots != null },
      );
      const type = snapshotType === 'detailed' || sessionHasDetailedData(s) ? 'detailed' : 'basic';
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
        slots_available: metrics.slots,
        capacity: metrics.capacity,
        estimated_booked: s.estimatedBooked ?? metrics.estimatedBooked,
        fill_rate: s.fillRate ?? metrics.fillRate,
        price_text: s.priceText ?? null,
        price_min: s.priceMin ?? null,
        price_max: s.priceMax ?? null,
        currency: s.currency || 'USD',
        status_label: availabilityStatusLabel({ ...s, slots: metrics.slots }),
        source_tier: sourceTier,
        snapshot_type: type,
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

// ── Detail enrichment (future dates: slots/capacity/price) ─────────────────────

function installBookingNetworkCapture(page) {
  const captured = [];
  page.on('response', async (response) => {
    try {
      const url = response.url();
      if (!/atlanticparksurf|wave7|booking|activity|agenda/i.test(url)) return;
      const ct = (response.headers()['content-type'] || '').toLowerCase();
      if (!ct.includes('json')) return;
      const body = await response.text();
      if (!body || body.length > 800_000) return;
      if (!/slot|capacity|avail|price|qty|quantity|remaining/i.test(body)) return;
      let parsed;
      try { parsed = JSON.parse(body); } catch { return; }
      captured.push({ url, body: parsed, at: Date.now() });
      if (captured.length > 80) captured.shift();
    } catch {}
  });
  return captured;
}

function deepFindSessionDetails(node, ts, depth = 0) {
  if (!node || depth > 12) return null;
  if (Array.isArray(node)) {
    for (const item of node) {
      const found = deepFindSessionDetails(item, ts, depth + 1);
      if (found) return found;
    }
    return null;
  }
  if (typeof node !== 'object') return null;

  const tsMs = Number(ts);
  const tsSec = Math.floor(tsMs / 1000);
  const candidates = [
    node.startTime, node.start_ts, node.startTs, node.timestamp, node.ts, node.timeSlot,
  ].map(v => (v != null ? Number(v) : null)).filter(v => Number.isFinite(v));
  const matchesTs = candidates.some(v => v === tsMs || v === tsSec || v * 1000 === tsMs);

  const slots = node.slotsAvailable ?? node.slots_available ?? node.availableSlots
    ?? node.remainingSlots ?? node.remaining ?? node.qty ?? node.quantity ?? null;
  const capacity = node.capacity ?? node.maxCapacity ?? node.maxQty ?? node.max ?? null;

  if (matchesTs && (slots != null || capacity != null)) {
    const priceRaw = node.priceText || node.price_text || node.price || '';
    const priceInfo = typeof priceRaw === 'string' ? parsePriceFromText(priceRaw) : {};
    return {
      slots: slots != null ? Number(slots) : null,
      capacity: capacity != null ? Number(capacity) : null,
      ...priceInfo,
    };
  }

  for (const v of Object.values(node)) {
    const found = deepFindSessionDetails(v, ts, depth + 1);
    if (found) return found;
  }
  return null;
}

function extractDetailsFromNetworkCapture(captured, ts) {
  if (!captured?.length) return null;
  for (let i = captured.length - 1; i >= 0; i--) {
    const found = deepFindSessionDetails(captured[i].body, ts);
    if (found && (found.slots != null || found.capacity != null || found.price_text)) return found;
  }
  return null;
}

async function getSessionDetailsWithFallback(page, ts, wave, networkCapture) {
  const label = `${ts}_${wave}`;
  const fromNetwork = networkCapture ? extractDetailsFromNetworkCapture(networkCapture, ts) : null;
  if (fromNetwork && (fromNetwork.slots != null || fromNetwork.capacity != null || fromNetwork.price_text)) {
    console.log(`  [details ${label}] from network response`);
    return fromNetwork;
  }
  return getSessionModalDetails(page, ts, wave);
}

async function navigateToSessionDate(page, isoDate) {
  if (!isoDate) return false;
  const weekOffset = Math.max(0, Math.floor(daysFromToday(isoDate) / 7));
  return navigateToWeekOffset(page, weekOffset);
}

async function refreshEnrichmentQueueCounts() {
  if (!supabase) {
    enrichmentQueuePendingCount = [...enrichmentQueueMemory.values()].filter(r => r.status === 'pending').length;
    enrichmentQueueRunningCount = [...enrichmentQueueMemory.values()].filter(r => r.status === 'running').length;
    return;
  }
  try {
    const { count: pending, error: pErr } = await supabase
      .from('session_enrichment_queue')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'pending');
    if (pErr && !isMissingTableError(pErr)) throw pErr;
    const { count: running, error: rErr } = await supabase
      .from('session_enrichment_queue')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'running');
    if (rErr && !isMissingTableError(rErr)) throw rErr;
    enrichmentQueuePendingCount = pending ?? enrichmentQueuePendingCount;
    enrichmentQueueRunningCount = running ?? enrichmentQueueRunningCount;
  } catch (e) {
    console.warn('  enrichment queue count failed:', e.message);
  }
}

async function enqueueSessionsForEnrichment(sessionsToQueue, { priority = 2, reason = 'auto' } = {}) {
  const items = asSessionArray(sessionsToQueue)
    .filter(s => s?.key && s.available && sessionNeedsDetailEnrichment(s))
    .map(s => ({
      park: PARK,
      session_key: s.key,
      iso_date: s.isoDate || s.dateKey || null,
      priority: Math.min(priority, enrichmentPriorityForSession(s)),
      reason,
      status: 'pending',
      updated_at: new Date().toISOString(),
    }));

  if (!items.length) return 0;

  if (!supabase) {
    for (const item of items) {
      const existing = enrichmentQueueMemory.get(item.session_key);
      if (!existing || item.priority < existing.priority) {
        enrichmentQueueMemory.set(item.session_key, { ...item, attempts: existing?.attempts || 0 });
      }
    }
    enrichmentQueuePendingCount = [...enrichmentQueueMemory.values()].filter(r => r.status === 'pending').length;
    return items.length;
  }

  try {
    const { error } = await supabase
      .from('session_enrichment_queue')
      .upsert(items, { onConflict: 'park,session_key' });
    if (error) {
      if (isMissingTableError(error)) {
        for (const item of items) enrichmentQueueMemory.set(item.session_key, item);
        return items.length;
      }
      throw error;
    }
    await refreshEnrichmentQueueCounts();
    return items.length;
  } catch (e) {
    console.warn('  enqueue enrichment failed:', e.message);
    return 0;
  }
}

async function enqueueDateForEnrichment(isoDate, { priority = 1, reason = 'manual_date' } = {}) {
  await ensureSessionsForStatus();
  const dateSessions = sessionsForDate(isoDate).filter(s => s.available);
  return enqueueSessionsForEnrichment(dateSessions, { priority, reason: `${reason}:${isoDate}` });
}

async function pickSessionsForDetailEnrichment({ priority = null, isoDate = null, limit = DETAIL_ENRICH_MAX_PER_RUN } = {}) {
  await ensureSessionsForStatus();

  if (isoDate) {
    return sessionsForDate(isoDate)
      .filter(s => s.available)
      .sort((a, b) => enrichmentPriorityForSession(a) - enrichmentPriorityForSession(b))
      .slice(0, limit);
  }

  let candidates = allStoredSessions().filter(s => sessionNeedsDetailEnrichment(s));
  if (priority != null) {
    candidates = candidates.filter(s => enrichmentPriorityForSession(s) === priority);
  }
  candidates.sort((a, b) => {
    const pDiff = enrichmentPriorityForSession(a) - enrichmentPriorityForSession(b);
    if (pDiff) return pDiff;
    return (a.ts || 0) - (b.ts || 0);
  });
  return candidates.slice(0, limit);
}

async function markQueueItemStatus(sessionKey, status, { error = null, incrementAttempt = false } = {}) {
  const now = new Date().toISOString();
  if (!supabase) {
    const row = enrichmentQueueMemory.get(sessionKey);
    if (row) {
      row.status = status;
      row.updated_at = now;
      row.last_attempt_at = now;
      if (error) row.last_error = error;
      if (incrementAttempt) row.attempts = (row.attempts || 0) + 1;
      if (status === 'done') enrichmentQueueMemory.delete(sessionKey);
    }
    await refreshEnrichmentQueueCounts();
    return;
  }
  try {
    const patch = { status, updated_at: now, last_attempt_at: now };
    if (error) patch.last_error = error;
    if (incrementAttempt) {
      const { data } = await supabase
        .from('session_enrichment_queue')
        .select('attempts')
        .eq('park', PARK)
        .eq('session_key', sessionKey)
        .maybeSingle();
      patch.attempts = (data?.attempts || 0) + 1;
    }
    await supabase
      .from('session_enrichment_queue')
      .update(patch)
      .eq('park', PARK)
      .eq('session_key', sessionKey);
    if (status === 'done') {
      await supabase
        .from('session_enrichment_queue')
        .delete()
        .eq('park', PARK)
        .eq('session_key', sessionKey);
    }
    await refreshEnrichmentQueueCounts();
  } catch (e) {
    if (!isMissingTableError(e)) console.warn('  queue status update failed:', e.message);
  }
}

function tryAcquireDetailEnrichmentLock(context = 'detail enrichment') {
  if (scrapeInProgress || detailEnrichmentInProgress) {
    console.log(`  ${context} skipped — busy (scrape=${scrapeInProgress}, enrich=${detailEnrichmentInProgress})`);
    return false;
  }
  detailEnrichmentInProgress = true;
  return true;
}

function releaseDetailEnrichmentLock() {
  detailEnrichmentInProgress = false;
}

async function runDetailEnrichment({ priority = null, isoDate = null, sessions: explicitSessions = null, reason = 'scheduled' } = {}) {
  if (!tryAcquireDetailEnrichmentLock(`detail enrichment (${reason})`)) {
    return { skipped: true, reason: 'busy' };
  }

  const stats = {
    isoDate: isoDate || null,
    sessionsAttempted: 0,
    sessionsUpdatedWithSlots: 0,
    sessionsUpdatedWithCapacity: 0,
    sessionsUpdatedWithPrice: 0,
    errors: [],
  };

  let launched;
  try {
    const toEnrich = explicitSessions?.length
      ? asSessionArray(explicitSessions).slice(0, DETAIL_ENRICH_MAX_PER_RUN)
      : await pickSessionsForDetailEnrichment({ priority, isoDate });

    if (!toEnrich.length) {
      return stats;
    }

    console.log(`\n[${new Date().toLocaleTimeString()}] Detail enrichment (${reason}): ${toEnrich.length} session(s)`);

    launched = await launchBrowser();
    const { page } = launched;
    const networkCapture = installBookingNetworkCapture(page);
    await openBookingPage(page);

    const updatedSessions = [];

    for (const s of toEnrich) {
      stats.sessionsAttempted++;
      await markQueueItemStatus(s.key, 'running', { incrementAttempt: true });

      const entry = { ...sessionsByKey.get(s.key), ...s };
      entry.detailStatus = 'checking';
      sessionsByKey.set(s.key, entry);

      try {
        await navigateToSessionDate(page, s.isoDate || s.dateKey);
        await page.waitForTimeout(400);

        const details = await getSessionDetailsWithFallback(page, s.ts, s.wave, networkCapture);
        if (!details) {
          stats.errors.push({ session_key: s.key, error: 'no_details' });
          entry.detailStatus = sessionHasDetailedData(entry) ? 'checked' : 'failed';
          entry.detailError = entry.detailError || 'no_details';
          await markQueueItemStatus(s.key, 'pending', { error: 'no_details' });
          continue;
        }

        const hadSlots = entry.slots != null;
        const hadCapacity = entry.capacity != null;
        const hadPrice = entry.priceText != null || entry.priceMin != null;

        applyDetailPayloadToSession(entry, details, s.level);
        sessionsByKey.set(s.key, entry);
        updatedSessions.push(entry);

        if (!hadSlots && entry.slots != null) stats.sessionsUpdatedWithSlots++;
        if (!hadCapacity && entry.capacity != null) stats.sessionsUpdatedWithCapacity++;
        if (!hadPrice && (entry.priceText || entry.priceMin != null)) stats.sessionsUpdatedWithPrice++;

        await markQueueItemStatus(s.key, 'done');
        await page.waitForTimeout(250);
      } catch (e) {
        stats.errors.push({ session_key: s.key, error: e.message });
        entry.detailStatus = sessionHasDetailedData(entry) ? 'checked' : 'failed';
        entry.detailError = e.message;
        sessionsByKey.set(s.key, entry);
        await markQueueItemStatus(s.key, 'pending', { error: e.message });
      }
    }

    rebuildSessionsArray();

    if (updatedSessions.length) {
      await upsertCurrentSessionsToSupabase(updatedSessions, 0, { scrapeKind: 'detailed' });
      await saveAvailabilitySnapshotsToSupabase(updatedSessions, 0, { snapshotType: 'detailed' });
      if (supabase) await loadCurrentSessionsFromSupabase({ reloadMeta: false });
    }

    lastDetailEnrichmentAt = new Date().toISOString();
    lastDetailEnrichmentError = null;
    console.log(`  detail enrichment done: ${stats.sessionsUpdatedWithSlots} slots, ${stats.sessionsUpdatedWithCapacity} capacity, ${stats.sessionsUpdatedWithPrice} price`);
    return stats;
  } catch (e) {
    lastDetailEnrichmentError = e.message;
    stats.errors.push({ error: e.message });
    console.error('  detail enrichment failed:', e.message);
    return stats;
  } finally {
    releaseDetailEnrichmentLock();
    if (launched?.browser) await launched.browser.close().catch(() => {});
    await refreshEnrichmentQueueCounts();
  }
}

async function runDetailEnrichmentByPriority(priority) {
  return runDetailEnrichment({ priority, reason: `priority_${priority}` });
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
  if (prevSlots === 0 && (currSlots == null || currSlots > 0)) return true;
  return false;
}

function wasAlreadyAvailableForLowSlots(watch, prevAvailable, prevSlots) {
  if (prevAvailable === false) return false;
  if (prevSlots === 0) return false;
  return true;
}

function shouldSendLowSlotsAlert(watch, prevAvailable, prevSlots, currSlots, threshold) {
  if (!wasAlreadyAvailableForLowSlots(watch, prevAvailable, prevSlots)) return false;
  if (currSlots == null || currSlots > threshold) return false;
  if (prevSlots == null) return false;
  if (prevSlots <= currSlots) return false;
  return true;
}

function shouldSendSellingFastAlert(watch, prevAvailable, prevSlots, currSlots, fastDrop) {
  if (!wasAlreadyAvailableForLowSlots(watch, prevAvailable, prevSlots)) return false;
  if (prevSlots == null || currSlots == null || currSlots >= prevSlots) return false;
  const drop = prevSlots - currSlots;
  if (drop < fastDrop) return false;
  if (currSlots > 4 && drop < fastDrop + 1) return false;
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

// Per-session watch alerts — one highest-priority event per scrape.
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

  let chosen = null;

  if (watch.alert_when_opens !== false && isOpenedTransition(watch, prevAvailable, prevSlots, session)) {
    chosen = {
      eventType: 'opened',
      urgent: true,
      detail: { ...alertDetail, eventReason: 'unavailable_to_available' },
    };
  }

  if (!chosen && slotsAlerts && currSlots != null) {
    if (watch.alert_when_low_slots !== false
      && shouldSendLowSlotsAlert(watch, prevAvailable, prevSlots, currSlots, threshold)) {
      chosen = {
        eventType: 'low_slots',
        urgent: true,
        detail: {
          ...alertDetail,
          eventReason: prevSlots > threshold ? 'crossed_low_threshold' : 'decreased_while_low',
        },
      };
    } else if (watch.alert_when_selling_fast !== false
      && shouldSendSellingFastAlert(watch, prevAvailable, prevSlots, currSlots, fastDrop)) {
      chosen = {
        eventType: 'selling_fast',
        urgent: currSlots <= threshold,
        meta: { fromSlots: prevSlots },
        detail: {
          ...alertDetail,
          eventReason: 'absolute_drop',
        },
      };
    }
  }

  if (!chosen && watch.alert_last_call !== false) {
    const minsUntil = minutesUntilSessionStart(session);
    if (minsUntil != null && minsUntil > 0 && minsUntil <= lastCallMins && (currSlots == null || currSlots > 0)) {
      chosen = {
        eventType: 'last_call',
        urgent: true,
        meta: { minutesUntil: minsUntil },
        detail: { ...alertDetail, eventReason: 'last_call_window' },
      };
    }
  }

  if (chosen) {
    await maybeSendWatchAlert(watch, session, chosen.eventType, {
      urgent: chosen.urgent,
      meta: chosen.meta || {},
      detail: chosen.detail,
    });
  }
}

async function maybeSendImmediateWatchAlert(_watch, _session) {
  // No alert when adding a watch — wait for a real state transition on the next scrape.
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

async function reloadWatchlistFromSupabase(userKey = null) {
  if (!supabase) {
    watchlistRowsLoaded = userKey
      ? watchItems.filter(w => w.user_key === userKey && w.active !== false).length
      : watchItems.filter(w => w.active !== false).length;
    return watchlistRowsLoaded;
  }
  try {
    let query = supabase.from('watchlist_items').select('*').eq('active', true);
    if (userKey) query = query.eq('user_key', userKey);
    const { data, error } = await query;
    if (error) throw error;
    const rows = asSessionArray(data).map(w => ({ ...watchAlertDefaults(w), ...w }));
    if (userKey) {
      watchItems = watchItems.filter(w => w.user_key !== userKey).concat(rows);
      watchlistRowsLoaded = rows.length;
      console.log(`  Supabase: loaded ${rows.length} watchlist row(s) for ${userKey.slice(0, 12)}…`);
    } else {
      watchItems = rows;
      watchlistRowsLoaded = rows.length;
      console.log(`  Supabase: loaded ${rows.length} watchlist item(s)`);
    }
    watchlistLastError = null;
    return watchlistRowsLoaded;
  } catch (e) {
    watchlistLastError = e.message;
    console.error('  Supabase watchlist load failed:', e.message);
    return userKey
      ? watchItems.filter(w => w.user_key === userKey && w.active !== false).length
      : watchItems.filter(w => w.active !== false).length;
  }
}

async function loadWatchlistFromSupabase() {
  return reloadWatchlistFromSupabase();
}

async function ensureWatchlistForUser(userKey) {
  if (!userKey) return;
  await reloadWatchlistFromSupabase(userKey);
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
    watchlistLastError = null;
    return data;
  } catch (e) {
    watchlistLastError = e.message;
    console.error('  Supabase watchlist upsert failed:', e.message);
    watchItems = watchItems.filter(
      w => !(w.user_key === row.user_key && w.session_key === row.session_key)
    );
    watchItems.push(row);
    return row;
  }
}

async function deactivateWatchItem(id, userKey) {
  if (!supabase) {
    watchItems = watchItems.filter(w => w.id !== id);
    return;
  }
  try {
    const { error } = await supabase
      .from('watchlist_items')
      .update({ active: false })
      .eq('id', id)
      .eq('user_key', userKey);
    if (error) throw error;
    watchItems = watchItems.filter(w => w.id !== id);
    watchlistLastError = null;
  } catch (e) {
    watchlistLastError = e.message;
    console.error('  Supabase watchlist deactivate failed:', e.message);
    throw e;
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
    if (details) {
      applyDetailPayloadToSession(entry, details, s.level);
    } else {
      const prev = prevByKey.get(s.key);
      if (prev?.slots != null) entry.slots = prev.slots;
    }
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
  const count = scrapeWindowDays();
  for (let i = 0; i < count; i++) {
    const d = new Date(start);
    d.setDate(start.getDate() + i);
    dates.push(dateKeyFromDate(d));
  }
  return dates;
}

function allCheckedDatesSet() {
  const checked = new Set(persistedDatesChecked);
  for (const d of datesCheckedDuringScrape) checked.add(d);
  return checked;
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
  const days = daysFromToday(sessionDateKey(s));
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
  const stored = allStoredSessions();
  const dateKeys = [...new Set(stored.map(sessionDateKey).filter(Boolean))].sort();
  const sessionsByDate = {};
  const sessionsByDateAndSide = {};
  for (const s of stored) {
    const dk = sessionDateKey(s);
    if (!dk) continue;
    sessionsByDate[dk] = (sessionsByDate[dk] || 0) + 1;
    const side = s.waveSide || `Wave ${s.wave}`;
    if (!sessionsByDateAndSide[dk]) sessionsByDateAndSide[dk] = {};
    sessionsByDateAndSide[dk][side] = (sessionsByDateAndSide[dk][side] || 0) + 1;
  }
  const checked = allCheckedDatesSet();
  const checkedDates = expected.filter(d => checked.has(d));
  const missingDatesInScrapeWindow = expected.filter(d => !(sessionsByDate[d] > 0));
  const datesWithSessions = expected.filter(d => (sessionsByDate[d] || 0) > 0);
  const expectedDatesCount = expected.length;
  const coveredDatesCount = checkedDates.length;
  const coveragePercent = expectedDatesCount
    ? Math.round((checkedDates.length / expectedDatesCount) * 100)
    : 0;
  const sessionsCoveragePercent = expectedDatesCount
    ? Math.round((datesWithSessions.length / expectedDatesCount) * 100)
    : 0;

  if (missingDatesInScrapeWindow.length) {
    console.log(`  ⚠ missingDatesInScrapeWindow (${missingDatesInScrapeWindow.length}): ${missingDatesInScrapeWindow.slice(0, 14).join(', ')}${missingDatesInScrapeWindow.length > 14 ? '…' : ''}`);
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
    sessionsCoveragePercent,
    missingDatesInScrapeWindow,
    datesCheckedDuringScrape: [...checked].sort(),
    datesCheckedEmpty: [...datesCheckedEmpty].sort(),
    datesWithSessionsCount: datesWithSessions.length,
    weeksScraped: lastWeeksScraped,
  };
}

function filterBatchForTier(batch, tier) {
  return asSessionArray(batch).filter(s => sessionInTier(s, tier));
}

function rebuildSessionsArray() {
  sessions = allStoredSessions().sort((a, b) => a.ts - b.ts || a.wave - b.wave);
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
      for (const field of ['capacity', 'estimatedBooked', 'fillRate', 'priceText', 'priceMin', 'priceMax', 'currency', 'lastDetailedCheckAt', 'detailStatus', 'detailError']) {
        if (existing[field] != null && merged[field] == null) merged[field] = existing[field];
      }
    }
    merged.lastBasicCheckAt = now;
    if (!sessionHasDetailedData(merged) && merged.detailStatus !== 'checking') {
      merged.detailStatus = merged.detailStatus || (existing?.detailStatus === 'checked' ? 'checked' : 'pending');
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

    const batch = dedupeBatch(filterBatchForTier(rawBatch, tier));
    const byKey = new Map(batch.map(s => [s.key, s]));

    if (cfg.slotCounts) {
      await fillSlotCounts(page, batch, byKey, prevByKey, slotStats);
      applySlotCacheFallback(byKey);
    }

    const merged = [...byKey.values()];
    const updatedKeys = mergeBatchIntoStore(merged, tier, { preserveSlots: !cfg.slotCounts });
    if (datesSeen) recordTierDateCoverage(datesSeen);

    if (cfg.slotCounts) {
      syncSlotCacheAvailability(merged);
      const reasonParts = Object.entries(slotStats.byReason).map(([k, n]) => `${n} ${k}`);
      console.log(`  slot counts: ${slotStats.cached} from cache, ${slotStats.rechecked} re-checked${reasonParts.length ? ` (${reasonParts.join(', ')})` : ''}`);
    }

    console.log(`  tier ${tier} summary: ${rawTilesTotal} tiles, ${weeksScraped} week(s), ${batch.length} in date range, ${updatedKeys.length} updated`);
    coverage = computeDateCoverage();
    console.log(`  tier ${tier} date coverage: ${coverage.earliestSessionDate || '?'} → ${coverage.latestSessionDate || '?'} (${coverage.uniqueDatesCount} days with sessions, ${coverage.sessionsCoveragePercent}% session coverage, ${coverage.coveragePercent}% dates checked)`);
    await processWatchAlertsAfterScrape(updatedKeys, { slotsAlerts: cfg.slotCounts });

    lastTierRun[tier] = new Date().toISOString();
    lastSuccessfulScrape = new Date().toISOString();
    lastCheck = lastSuccessfulScrape;
    lastScrapeError = null;
    lastScrapeErrorStack = null;
    hasFreshScrapeThisBoot = true;
    dataSource = supabaseConfigured ? 'supabase/current_sessions' : 'memory-fallback';

    await upsertCurrentSessionsToSupabase(merged, tier, { scrapeKind: cfg.slotCounts ? 'detailed' : 'basic' });
    lastSnapshotRowsInsertedLastRun = await saveAvailabilitySnapshotsToSupabase(
      merged,
      tier,
      { snapshotType: cfg.slotCounts ? 'detailed' : 'basic' },
    );
    if (!cfg.slotCounts) {
      const needing = merged.filter(sessionNeedsDetailEnrichment);
      if (needing.length) {
        await enqueueSessionsForEnrichment(needing, {
          priority: tier === 2 ? 2 : 3,
          reason: `tier_${tier}_basic`,
        });
      }
    }
    await saveLatestSnapshotToSupabase();
    await mergeBroadSnapshotIntoStore();
    await loadScrapeMetaFromSupabase();
    await refreshCoverageFlags();
    if (tier >= 2) lastFullCoverageScrape = lastSuccessfulScrape;
    if (tier === 1) await prunePastSessionsFromSupabase();

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
function statusPayload(userKey = null, selectedDate = null, profileCode = null) {
  const dateCoverage = computeDateCoverage();
  const uiDatesChecked = datesCheckedForUi();
  const selectedDateDebug = buildSelectedDateDebug(selectedDate);
  const userWatchlist = watchlistForUser(userKey);
  return {
    sessions: asSessionArray(sessions),
    watchList: userWatchlist,
    history: history || {},
    lastCheck: lastSuccessfulScrape || lastCheck,
    ntfyOk: !!TOPIC,
    ntfyFallbackConfigured: !!TOPIC,
    internalBetaNotifications: INTERNAL_BETA,
    internalDefaultNtfyTopic: INTERNAL_BETA ? INTERNAL_DEFAULT_NTFY_TOPIC : null,
    internalDefaultProfileCode: INTERNAL_DEFAULT_PROFILE_CODE,
    user_key: userKey || null,
    profileCode: profileCode || null,
    watchlistCount: userWatchlist.length,
    watchlistRowsLoaded: userKey ? userWatchlist.length : watchlistRowsLoaded,
    watchlistLastError,
    supabaseAvailable: !!supabase,
    appVersion: APP_VERSION,
    buildTime: BUILD_TIME,
    watchlistSideDebug: buildWatchlistSideDebug(userKey),
    waveSideDebug: {
      ambiguousCount: ambiguousSideMappings.length,
      ambiguousSamples: ambiguousSideMappings.slice(-10),
      recentParses: recentSideParseLogs.slice(-12),
    },
    ...dateCoverage,
    datesCheckedDuringScrape: uiDatesChecked,
    selectedDate: selectedDate || null,
    selectedDateHasSavedSessions: selectedDateDebug?.selectedDateHasSavedSessions ?? null,
    selectedDateWasChecked: selectedDateDebug?.selectedDateWasChecked ?? null,
    sessionsForSelectedDateCount: selectedDateDebug?.sessionsForSelectedDateCount ?? null,
    selectedDateDebug,
    datesCheckedEmpty: [...datesCheckedEmpty].sort(),
    scrapeMeta: {
      weeksAvailableOnSite,
      effectiveWeeksAhead,
      scrapeWeeksAhead: SCRAPE_WEEKS_AHEAD,
      lastTierRun: { ...lastTierRun },
      lastFullCoverageScrape,
      supabaseConfigured,
      ...dateCoverage,
      datesCheckedDuringScrape: uiDatesChecked,
      datesCheckedEmpty: [...datesCheckedEmpty].sort(),
    },
    ...getStatusFields(),
  };
}

app.get('/api/status', async (req, res) => {
  try {
    const userKey = req.query.user_key || null;
    const profileCode = req.query.profile_code || req.query.profileCode || null;
    await ensureSessionsForStatus();
    if (userKey) await ensureWatchlistForUser(userKey);
    const selectedDate = req.query.selected_date || req.query.selectedDate || null;
    res.json(statusPayload(userKey, selectedDate, profileCode));
  } catch (e) {
    console.error('/api/status error:', e.message);
    const userKey = req.query.user_key || null;
    const profileCode = req.query.profile_code || req.query.profileCode || null;
    const selectedDate = req.query.selected_date || req.query.selectedDate || null;
    res.json({ ...statusPayload(userKey, selectedDate, profileCode), statusError: e.message });
  }
});

app.get('/api/schema/health', async (_req, res) => {
  if (supabase && !supabaseSchemaHealth.checkedAt) {
    await auditSupabaseSchema();
  }
  res.json({
    supabaseConfigured,
    supabaseInitError,
    ...schemaHealthPayload(),
  });
});

app.get('/api/sessions', async (req, res) => {
  const isoDate = normalizeIsoDateParam(req.query.date || req.query.iso_date || req.query.isoDate);

  if (isoDate) {
    try {
      const payload = await buildSessionsForDatePayload(isoDate);
      res.json(payload);
    } catch (e) {
      console.error(`/api/sessions?date=${isoDate} error:`, e.message);
      const schemaErr = isMissingTableError(e) ? formatSchemaError('current_sessions') : null;
      res.status(schemaErr ? 503 : 500).json({
        isoDate,
        sessions: [],
        sessionsCount: 0,
        dataSource: schemaErr ? 'schema-missing' : (supabaseConfigured ? 'supabase/current_sessions' : 'memory-fallback'),
        lastSuccessfulScrape,
        lastCheckedForDate: null,
        wasDateChecked: false,
        isScrapeInProgress: scrapeInProgress,
        hasSavedSessions: false,
        statusReason: schemaErr ? 'schema_error' : 'error',
        error: schemaErr || e.message,
        schemaError: schemaErr,
        dateCoverage: dateCoverageForIsoDate(isoDate),
        schemaHealth: schemaHealthPayload(),
      });
    }
    return;
  }

  try {
    const userKey = req.query.user_key || null;
    const profileCode = req.query.profile_code || req.query.profileCode || null;
    await ensureSessionsForStatus();
    if (userKey) await ensureWatchlistForUser(userKey);
    const selectedDate = req.query.selected_date || req.query.selectedDate || null;
    res.json(statusPayload(userKey, selectedDate, profileCode));
  } catch (e) {
    console.error('/api/sessions error:', e.message);
    const userKey = req.query.user_key || null;
    const profileCode = req.query.profile_code || req.query.profileCode || null;
    const selectedDate = req.query.selected_date || req.query.selectedDate || null;
    res.json({ ...statusPayload(userKey, selectedDate, profileCode), statusError: e.message });
  }
});

function uiReasonText(reason) {
  switch (reason) {
    case 'has_sessions': return 'Show saved sessions for this date';
    case 'checked_empty': return 'Show "No sessions found for this date"';
    case 'checking': return 'Show "Still checking this date…" while scrape runs';
    case 'not_checked': return 'Show "Not checked yet"';
    default: return reason || 'unknown';
  }
}

app.get('/api/debug/coverage', async (_req, res) => {
  try {
    await ensureSessionsForStatus();
    const payload = await buildCoverageDebugPayload();
    res.json(payload);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/admin/backfill-current-sessions', async (req, res) => {
  try {
    await ensureSessionsForStatus();
    const allowOverwrite = req.body?.allowOverwrite === true;
    const result = await backfillCurrentSessions({ allowOverwrite });
    await refreshCoverageFlags();
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message, errors: [e.message] });
  }
});

app.get('/api/debug/date/:isoDate', async (req, res) => {
  const isoDate = req.params.isoDate;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) {
    return res.status(400).json({ error: 'isoDate must be YYYY-MM-DD' });
  }
  try {
    const apiPayload = await buildSessionsForDatePayload(isoDate);
    let primaryForDate = [];
    if (supabase) {
      const { data, error } = await supabase
        .from('current_sessions')
        .select('*')
        .eq('park', PARK)
        .eq('iso_date', isoDate)
        .order('start_ts', { ascending: true });
      if (!error) primaryForDate = (data || []).map(currentRowToSession).filter(s => s?.key);
    }
    const fallbackSnapshot = await loadSessionsForDateFromSnapshotBlob(isoDate);
    const fallbackAvailability = await loadSessionsForDateFromAvailabilitySnapshots(isoDate);
    const checkTimes = dateCheckTimestamps(apiPayload.sessions);
    const uiResolved = resolveDateUiDisplay(isoDate, {
      sessions: apiPayload.sessions,
      statusReason: apiPayload.statusReason,
      isFallback: apiPayload.isFallback,
    });
    const payload = {
      isoDate,
      apiContract: {
        isoDate: apiPayload.isoDate,
        sessionsCount: apiPayload.sessionsCount,
        dataSource: apiPayload.dataSource,
        statusReason: apiPayload.statusReason,
        hasSavedSessions: apiPayload.hasSavedSessions,
        lastBasicCheckAt: apiPayload.lastBasicCheckAt,
        lastDetailedCheckAt: apiPayload.lastDetailedCheckAt,
        isScrapeInProgress: apiPayload.isScrapeInProgress,
        isFallback: apiPayload.isFallback,
        error: apiPayload.error,
      },
      sessionsCount: apiPayload.sessionsCount,
      currentSessionsCountForDate: primaryForDate.length,
      currentSessionsForDate: primaryForDate.slice(0, 8),
      availabilitySnapshotsCountForDate: 0,
      scrapeSnapshotsForDate: fallbackSnapshot.slice(0, 8),
      fallbackSessionsFromSnapshot: fallbackSnapshot.slice(0, 8),
      fallbackSessionsFromAvailability: fallbackAvailability.slice(0, 8),
      fallbackSnapshotCount: fallbackSnapshot.length,
      fallbackAvailabilityCount: fallbackAvailability.length,
      wasDateChecked: apiPayload.wasDateChecked,
      lastBasicCheckAt: checkTimes.lastBasicCheckAt || apiPayload.lastBasicCheckAt,
      lastDetailedCheckAt: checkTimes.lastDetailedCheckAt || apiPayload.lastDetailedCheckAt,
      statusReason: uiResolved.statusReason,
      uiDisplay: uiResolved.uiDisplay,
      uiMessage: uiResolved.uiMessage,
      sampleSessions: apiPayload.sessions.slice(0, 5).map(s => ({
        key: s.key,
        time: s.time,
        level: s.level,
        waveSide: s.waveSide,
        available: s.available,
        slots: s.slots,
        capacity: s.capacity,
        estimatedBooked: s.estimatedBooked,
        fillRate: s.fillRate,
        priceText: s.priceText,
        lastBasicCheckAt: s.lastBasicCheckAt,
        lastDetailedCheckAt: s.lastDetailedCheckAt,
      })),
      scrapeInProgress,
      persistedDatesChecked: [...persistedDatesChecked].sort(),
      datesCheckedEmpty: [...datesCheckedEmpty].sort(),
      relevantScrapeRuns: [],
      latestSnapshotsForDate: [],
    };

    if (supabase) {
      try {
        const { count } = await supabase
          .from('availability_snapshots')
          .select('*', { count: 'exact', head: true })
          .eq('iso_date', isoDate);
        payload.availabilitySnapshotsCountForDate = count ?? 0;
      } catch {}

      const { data: runs, error: runsError } = await supabase
        .from('scrape_runs')
        .select('id, tier, started_at, finished_at, success, sessions_found, dates_covered, missing_dates, coverage_percent, error')
        .order('started_at', { ascending: false })
        .limit(30);
      if (runsError) throw runsError;
      payload.relevantScrapeRuns = (runs || []).filter((run) => {
        if (Array.isArray(run.missing_dates) && !run.missing_dates.includes(isoDate)) {
          return run.success && run.dates_covered != null;
        }
        return true;
      }).slice(0, 10);

      const { data: snaps, error: snapsError } = await supabase
        .from('availability_snapshots')
        .select('scraped_at, session_key, iso_date, start_time, session_type, wave_side, available, slots_available, capacity, price_text, snapshot_type, source_tier')
        .eq('iso_date', isoDate)
        .order('scraped_at', { ascending: false })
        .limit(12);
      if (snapsError) throw snapsError;
      payload.latestSnapshotsForDate = snaps || [];
      payload.latestAvailabilitySnapshots = snaps || [];
    }

    res.json(payload);
  } catch (e) {
    res.status(500).json({ isoDate, error: e.message });
  }
});

app.post('/api/admin/enrich-date', async (req, res) => {
  const isoDate = normalizeIsoDateParam(req.body?.isoDate || req.body?.iso_date);
  if (!isoDate) {
    return res.status(400).json({ error: 'isoDate required (YYYY-MM-DD)' });
  }

  try {
    await ensureSessionsForStatus();
    const dateSessions = sessionsForDate(isoDate).filter(s => s.available);
    if (!dateSessions.length) {
      return res.json({
        isoDate,
        sessionsAttempted: 0,
        sessionsUpdatedWithSlots: 0,
        sessionsUpdatedWithCapacity: 0,
        sessionsUpdatedWithPrice: 0,
        errors: [{ error: 'no_open_sessions_for_date' }],
      });
    }

    await enqueueDateForEnrichment(isoDate, { priority: 1, reason: 'admin_enrich_date' });
    const result = await runDetailEnrichment({
      isoDate,
      sessions: dateSessions,
      reason: 'admin_enrich_date',
    });

    res.json({
      isoDate,
      sessionsAttempted: result.sessionsAttempted ?? dateSessions.length,
      sessionsUpdatedWithSlots: result.sessionsUpdatedWithSlots ?? 0,
      sessionsUpdatedWithCapacity: result.sessionsUpdatedWithCapacity ?? 0,
      sessionsUpdatedWithPrice: result.sessionsUpdatedWithPrice ?? 0,
      errors: result.errors ?? [],
      skipped: result.skipped ?? false,
    });
  } catch (e) {
    res.status(500).json({ isoDate, error: e.message, errors: [{ error: e.message }] });
  }
});

app.get('/api/debug/scrape', (_req, res) => {
  const coverage = computeDateCoverage();
  res.json({
    scrapeInProgress,
    sessionsCount: sessions.length,
    currentSessionsCount: sessionsByKey.size,
    snapshotRowsInsertedLastRun: lastSnapshotRowsInsertedLastRun,
    lastScrapeAttempt,
    lastScrapeError,
    lastScrapeErrorStackPreview: stackPreview(lastScrapeErrorStack),
    lastSuccessfulScrape,
    dataSource,
    supabaseConfigured,
    effectiveWeeksAhead,
    lastTierRun: { ...lastTierRun },
    lastFullCoverageScrape,
    tierCoverage: tierCoverageSummary(),
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

app.get('/api/watchlist', async (req, res) => {
  const userKey = req.query.user_key;
  if (!userKey) return res.status(400).json({ error: 'user_key required' });
  await ensureWatchlistForUser(userKey);
  const items = watchlistForUser(userKey);
  res.json({
    items,
    user_key: userKey,
    watchlistCount: items.length,
    watchlistRowsLoaded: items.length,
    watchlistLastError,
    supabaseConfigured,
    supabaseAvailable: !!supabase,
  });
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
    res.json({ ok: true, item: enrichWatchItemForClient(saved), watchlistLastError });
  } catch (e) {
    watchlistLastError = e.message;
    res.status(500).json({ error: e.message, watchlistLastError });
  }
});

app.post('/api/watchlist/sync', async (req, res) => {
  const { user_key, ntfy_topic, items, replace = false } = req.body || {};
  if (!user_key) return res.status(400).json({ error: 'user_key required' });
  const incoming = asSessionArray(items);

  if (replace && incoming.length === 0) {
    return res.status(400).json({
      error: 'Refusing to replace watchlist with an empty list — pass replace:true only when intentional',
    });
  }

  const synced = [];
  for (const item of incoming) {
    const row = buildWatchRow({ ...item, user_key, ntfy_topic: item.ntfy_topic || ntfy_topic });
    if (!row) continue;
    const isNew = !watchItems.some(
      w => w.user_key === row.user_key && w.session_key === row.session_key
    );
    const saved = await upsertWatchItem(row, { isNew });
    synced.push(enrichWatchItemForClient(saved));
  }

  if (replace === true && incoming.length > 0) {
    const incomingKeys = new Set(incoming.map(i => i.session_key || i.key));
    for (const w of [...watchItems]) {
      if (w.user_key === user_key && !incomingKeys.has(w.session_key)) {
        await deactivateWatchItem(w.id, user_key);
      }
    }
  }

  await ensureWatchlistForUser(user_key);
  res.json({
    ok: true,
    items: watchlistForUser(user_key),
    syncedCount: synced.length,
    watchlistLastError,
  });
});

app.delete('/api/watchlist/:id', async (req, res) => {
  const userKey = req.query.user_key || req.body?.user_key;
  if (!userKey) return res.status(400).json({ error: 'user_key required' });
  try {
    await deactivateWatchItem(req.params.id, userKey);
    for (const key of [...lastAlertState.keys()]) {
      if (key.startsWith(`${userKey}:`)) lastAlertState.delete(key);
    }
    console.log(`  🗑  Removed watch ${req.params.id}`);
    res.json({ ok: true, watchlistLastError });
  } catch (e) {
    res.status(500).json({ error: e.message, watchlistLastError });
  }
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
    await auditSupabaseSchema();
    const loadedCurrent = await loadCurrentSessionsFromSupabase();
    if (!loadedCurrent) {
      await loadLatestSnapshotFromSupabase();
    } else {
      await mergeBroadSnapshotIntoStore();
    }
    await loadWatchlistFromSupabase();
    await refreshCoverageFlags();
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
cron.schedule(`*/${CHECK_MINS} * * * *`, () => runDetailEnrichmentByPriority(1).catch(console.error));
cron.schedule('*/30 * * * *', () => runTierScrape(2).catch(console.error));
cron.schedule(`*/${ENRICHMENT_TIER2_EVERY_MINS} * * * *`, () => runDetailEnrichmentByPriority(2).catch(console.error));
cron.schedule('0 */6 * * *', () => {
  runTierScrape(3).catch(console.error);
  setTimeout(() => runDetailEnrichmentByPriority(3).catch(console.error), 90_000);
});
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
    console.log(`Detail enrichment P1 (watched/today/selected): every ${CHECK_MINS} min`);
    console.log('Tier 2 (this week):              every 30 min');
    console.log(`Detail enrichment P2 (next 7 days): every ${ENRICHMENT_TIER2_EVERY_MINS} min`);
    console.log('Tier 3 (weeks 2–3):              every 6 hours (+ detail P3)');
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
    startupCoverageCheck().catch(console.error);
  });
}

startServer().catch((e) => {
  console.error('Server startup failed:', e.message);
  process.exit(1);
});
