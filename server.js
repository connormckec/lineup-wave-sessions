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
const CHECK_MINS      = Math.max(1, parseInt(process.env.CHECK_EVERY_MINS || '5', 10) || 5);
const MAX_SLOT_CHECKS = parseInt(process.env.MAX_SLOT_CHECKS || '50', 10);
const SLOT_CACHE_STALE_CYCLES = parseInt(process.env.SLOT_CACHE_STALE_CYCLES || '3', 10);
const DETAIL_ENRICH_MAX_PER_RUN = parseInt(process.env.DETAIL_ENRICH_MAX_PER_RUN || '25', 10);
const ENRICHMENT_STALE_HOURS = parseInt(process.env.ENRICHMENT_STALE_HOURS || '6', 10);
const ENRICHMENT_TIER2_EVERY_MINS = parseInt(process.env.ENRICHMENT_TIER2_EVERY_MINS || '45', 10);
const ENRICHMENT_TIER3_STALE_HOURS = parseInt(process.env.ENRICHMENT_TIER3_STALE_HOURS || '12', 10);
const ENRICHMENT_DELAY_MS = parseInt(process.env.ENRICHMENT_DELAY_MS || '350', 10);
const ENRICHMENT_BROWSER_IDLE_MS = parseInt(process.env.ENRICHMENT_BROWSER_IDLE_MS || '300000', 10);
const ENRICHMENT_P1_OFFSET_MS = parseInt(process.env.ENRICHMENT_P1_OFFSET_MS || '120000', 10);
const SCRAPE_WEEKS_AHEAD = parseInt(process.env.SCRAPE_WEEKS_AHEAD || '4', 10);
const SUPABASE_URL              = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
const EXCLUDED_LEVELS    = ['Cabanas', 'Beach Pass'];
const EXCLUDED_WAVES     = [5, 6];
const BOOKING_TZ = 'America/New_York';
const MAX_BOOKING_HORIZON_DAYS = parseInt(process.env.MAX_BOOKING_HORIZON_DAYS || '120', 10);

let lastTier1DurationMs = null;
let lastTier2DurationMs = null;
let lastTier3DurationMs = null;
let lastApiSessionsDurationMs = null;
let lastSupabaseDateQueryMs = null;
let scrapeMetaCachedAt = 0;
const lastTierDurationMs = { 1: null, 2: null, 3: null, 4: null };
const lastTierError = { 1: null, 2: null, 3: null, 4: null };
const SCRAPE_META_TTL_MS = 60_000;
const API_SESSIONS_DURATION_SAMPLES = [];

const SCRAPE_OPTS = { excludedLevels: EXCLUDED_LEVELS, excludedWaves: EXCLUDED_WAVES };

const TIER_CONFIG = {
  1: { label: 'today/tomorrow', slotCounts: true,  weekStart: 0, weekEnd: 0, minDay: 0,  maxDay: 1 },
  2: { label: 'booking window', slotCounts: false, weekStart: 0, weekEnd: null, minDay: 2,  maxDay: null },
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
let currentScrapeTier  = null;
let currentScrapeStartedAt = null;
let detailEnrichmentInProgress = false;
let lastDetailEnrichmentAt = null;
let lastDetailEnrichmentError = null;
let lastRequestedDateForEnrichment = null;
let enrichmentQueuePendingCount = 0;
let enrichmentQueueRunningCount = 0;
let fallbackAvailableCached = false;
const enrichmentQueueMemory = new Map();
const sessionsNeedingDetailAfterBasic = new Set();
let enrichmentBrowserPool = null;
let enrichmentBrowserLastUsed = 0;
let enrichmentNetworkCapture = null;
let detailModalLifecycleState = null;

function createEmptyModalLifecycleState() {
  return {
    lastModalTextHash: null,
    lastModalText: null,
    lastSessionKey: null,
    modalLifecycleSamples: [],
    tileClickSamples: [],
  };
}

function resetDetailModalLifecycleState() {
  detailModalLifecycleState = createEmptyModalLifecycleState();
  return detailModalLifecycleState;
}

function hashModalText(text) {
  if (!text) return null;
  const s = String(text).replace(/\s+/g, ' ').trim();
  if (!s) return null;
  let h = 0;
  for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  return `m${Math.abs(h)}`;
}

function modalAssociationVerifiedOnSession(s) {
  if (!s) return false;
  if (s.modalAssociationVerified === true || s.raw?.modalAssociationVerified === true) return true;
  return sessionDetailVerified(s);
}

function isInvalidCheckedWithSlotsStatus(s) {
  const st = normalizeDetailStatus(s?.detailStatus || s?.detail_status);
  if (st !== 'checked_with_slots') return false;
  return s?.slots == null || s?.capacity == null || !modalAssociationVerifiedOnSession(s);
}

function reconcileDetailStatusForSession(s) {
  const st = normalizeDetailStatus(s?.detailStatus || s?.detail_status);
  if (st !== 'checked_with_slots') return st;
  if (s?.slots == null || s?.capacity == null) {
    if (s?.staleModalDetected || s?.raw?.staleModalDetected) return 'failed_modal_stale';
    if (!modalAssociationVerifiedOnSession(s)) return 'failed_modal_mismatch';
    return 'checked_available_no_slot_count';
  }
  if (!modalAssociationVerifiedOnSession(s)) return 'failed_modal_mismatch';
  return st;
}

const enrichmentMetrics = {
  lastRunAt: null,
  lastDurationMs: null,
  lastRunStats: null,
  recentErrors: [],
  runsCompleted: 0,
  averageDurationMs: null,
};
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
let lastWeeksScraped = 0;
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
let backgroundCollectorEnabled = process.env.BACKGROUND_COLLECTOR_ENABLED !== 'false';
let scrapeScheduleEnabled = false;
const collectorState = {
  schedulerStartedAt: null,
  initialScrapeScheduled: false,
  tier1Interval: `*/${CHECK_MINS} * * * *`,
  tier2Interval: '*/30 * * * *',
  tier3Interval: '0 */6 * * *',
  tier1NextRunAt: null,
  tier2NextRunAt: null,
  tier3NextRunAt: null,
  tier1LastAttemptAt: null,
  tier1LastCompletedAt: null,
  tier1LastSkippedAt: null,
  tier1LastSkipReason: null,
  tier1LastError: null,
  tier1TargetDates: null,
  tier1LastResult: null,
  tier2LastAttemptAt: null,
  tier2LastCompletedAt: null,
  tier2LastSkippedAt: null,
  tier2LastSkipReason: null,
  tier2LastError: null,
  tier2LastResult: null,
  skippedRuns: [],
  cronTasks: {},
  dateNavigationByDate: {},
  dateCoverageAttempts: {},
  lastDateRangeBackfill: null,
  discoveredAvailableDates: [],
  lastDiscoveryAt: null,
  discoveryDiagnostics: null,
  lastBackfillAvailableDatesResult: null,
};
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
    weeksScraped: lastWeeksScraped ?? 0,
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
    if (meta.weeksScraped != null) lastWeeksScraped = meta.weeksScraped ?? 0;
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

    if (!openSessions.length) {
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

async function queryCurrentSessionsForDate(isoDate, { mergeIntoStore = true } = {}) {
  if (!isoDate) return { sessions: [], dataSource: 'none', isFallback: false, queryMs: 0 };

  const queryStarted = Date.now();
  const finish = (result) => {
    const queryMs = Date.now() - queryStarted;
    lastSupabaseDateQueryMs = queryMs;
    return { ...result, queryMs };
  };

  if (!supabase) {
    return finish({
      sessions: sessionsForDate(isoDate),
      dataSource: normalizeDataSource(dataSource),
      isFallback: String(dataSource).includes('fallback'),
    });
  }

  const tableMissing = supabaseSchemaHealth.checkedAt && !supabaseSchemaHealth.currentSessionsAvailable;
  if (!tableMissing) {
    try {
      const { data, error } = await supabase
        .from('current_sessions')
        .select('*')
        .eq('park', PARK)
        .eq('iso_date', isoDate)
        .order('start_ts', { ascending: true });
      if (!error && data?.length) {
        const dateSessions = data.map(currentRowToSession).filter(s => s?.key);
        if (mergeIntoStore) mergeSessionsIntoStore(dateSessions);
        return finish({
          sessions: dateSessions,
          dataSource: 'supabase/current_sessions',
          isFallback: false,
        });
      }
      if (error && isMissingTableError(error)) {
        supabaseSchemaHealth.currentSessionsAvailable = false;
      } else if (error) {
        throw error;
      }
    } catch (e) {
      if (!isMissingTableError(e)) console.warn(`  current_sessions date query ${isoDate}:`, e.message);
    }
  }

  const fromSnapshot = await loadSessionsForDateFromSnapshotBlob(isoDate);
  if (fromSnapshot.length) {
    if (mergeIntoStore) mergeSessionsIntoStore(fromSnapshot);
    return finish({
      sessions: fromSnapshot,
      dataSource: 'supabase/scrape_snapshots_fallback',
      isFallback: true,
    });
  }

  const fromAvailability = await loadSessionsForDateFromAvailabilitySnapshots(isoDate);
  if (fromAvailability.length) {
    if (mergeIntoStore) mergeSessionsIntoStore(fromAvailability);
    return finish({
      sessions: fromAvailability,
      dataSource: 'supabase/availability_snapshots_fallback',
      isFallback: true,
    });
  }

  const mem = sessionsForDate(isoDate);
  if (mem.length) {
    return finish({
      sessions: mem,
      dataSource: normalizeDataSource(dataSource),
      isFallback: String(dataSource).includes('fallback'),
    });
  }

  return finish({ sessions: [], dataSource: 'none', isFallback: false });
}

async function loadSessionsForDateAllSources(isoDate, { skipPrimary = false, mergeIntoStore = true } = {}) {
  if (!isoDate) {
    return { sessions: [], dataSource: 'none', isFallback: false };
  }

  if (!skipPrimary) {
    return queryCurrentSessionsForDate(isoDate, { mergeIntoStore });
  }

  const fromSnapshot = await loadSessionsForDateFromSnapshotBlob(isoDate);
  if (fromSnapshot.length) {
    if (mergeIntoStore) mergeSessionsIntoStore(fromSnapshot);
    return {
      sessions: fromSnapshot,
      dataSource: 'supabase/scrape_snapshots_fallback',
      isFallback: true,
    };
  }

  const fromAvailability = await loadSessionsForDateFromAvailabilitySnapshots(isoDate);
  if (fromAvailability.length) {
    if (mergeIntoStore) mergeSessionsIntoStore(fromAvailability);
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
    const upsertResult = await upsertCurrentSessionsToSupabase(toUpsert, 0, { scrapeKind: 'basic' });
    rowsUpserted = upsertResult.rowsUpserted;
    if (upsertResult.error) errors.push(upsertResult.error);
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
      console.log('  Backfill found nothing — Tier 1/Tier 2 deferred (120s check, Tier 1 first)');
      setTimeout(() => {
        if (scrapeInProgress) return;
        if (!lastTierRun[1]) {
          runScheduledTier(1, { reason: 'startup_backfill_tier1_first' }).catch(console.error);
          return;
        }
        runScheduledTier(2, { reason: 'startup_backfill_tier2' }).catch(console.error);
      }, 120_000);
    }
    return result;
  }

  if (sparse && !scrapeInProgress) {
    console.log('  Sparse coverage, no fallback — Tier 2 deferred until Tier 1 completes (120s check)');
    setTimeout(() => {
      if (scrapeInProgress) return;
      if (!lastTierRun[1]) {
        console.log('  Tier 1 not completed yet — running Tier 1 first');
        runScheduledTier(1, { reason: 'startup_coverage_tier1_first' }).catch(console.error);
        return;
      }
      runScheduledTier(2, { reason: 'startup_coverage_tier2' }).catch(console.error);
    }, 120_000);
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

async function buildAvailableDatesCoverageStatuses() {
  const dates = expectedAvailableBookingDates();
  const dbStats = await queryCurrentSessionsByDateFromDb();
  const rowsByDate = {};

  if (supabase) {
    try {
      const start = dates[0];
      const end = dates[dates.length - 1];
      if (start && end) {
        const { data, error } = await supabase
          .from('current_sessions')
          .select('*')
          .eq('park', PARK)
          .gte('iso_date', start)
          .lte('iso_date', end);
        if (!error) {
          for (const row of data || []) {
            const s = currentRowToSession(row);
            const dk = sessionDateKey(s);
            if (!dk) continue;
            if (!rowsByDate[dk]) rowsByDate[dk] = [];
            rowsByDate[dk].push(s);
          }
        }
      }
    } catch (e) {
      console.warn('  buildAvailableDatesCoverageStatuses db query failed:', e.message);
    }
  }

  for (const s of allStoredSessions()) {
    const dk = sessionDateKey(s);
    if (!dates.includes(dk)) continue;
    if (!rowsByDate[dk]) rowsByDate[dk] = [];
    if (!rowsByDate[dk].some(x => x.key === s.key)) rowsByDate[dk].push(s);
  }

  return dates.map((isoDate) => {
    const rows = rowsByDate[isoDate] || [];
    const basicRowCount = dbStats.byDate[isoDate] || rows.length;
    const verified = rows.filter(s => sessionDetailVerified(s));
    const suppressed = rows.filter(s => {
      const st = effectiveDetailStatus(s);
      return isDetailFailureStatus(st)
        || st === 'checked_available_no_slot_count'
        || st === 'checked_open_no_slots_visible'
        || isInvalidCheckedWithSlotsStatus(s);
    });
    const attempt = collectorState.dateCoverageAttempts[isoDate]
      || collectorState.dateNavigationByDate[isoDate]
      || null;
    const lastBasic = rows.reduce((max, s) => {
      const t = s.lastBasicCheckAt || s.last_basic_check_at;
      return t && (!max || t > max) ? t : max;
    }, null);
    const lastDetailed = rows.reduce((max, s) => {
      const t = s.lastDetailedCheckAt || s.last_detailed_check_at;
      return t && (!max || t > max) ? t : max;
    }, null);
    const lastSuccess = verified.length
      ? verified.reduce((max, s) => {
        const t = s.lastDetailedCheckAt || s.last_detailed_check_at;
        return t && (!max || t > max) ? t : max;
      }, null)
      : (basicRowCount > 0 ? lastBasic : null);

    let failureReason = null;
    if (attempt?.failureReason) failureReason = attempt.failureReason;
    else if (attempt?.navigationError) failureReason = attempt.navigationError;
    else if (basicRowCount === 0 && persistedDatesChecked.has(isoDate)) failureReason = 'checked_empty_no_sessions';
    else if (basicRowCount === 0) failureReason = 'not_checked_or_no_rows';
    else if (verified.length === 0 && rows.some(s => isDetailFailureStatus(effectiveDetailStatus(s)))) {
      failureReason = 'detail_verification_failed';
    }

    return {
      isoDate,
      hasBasicRows: basicRowCount > 0,
      basicRowCount,
      hasVerifiedDetails: verified.length > 0,
      verifiedDetailCount: verified.length,
      suppressedDetailCount: suppressed.length,
      failureReason,
      lastAttemptAt: attempt?.recordedAt || lastDetailed || lastBasic || null,
      lastSuccessAt: lastSuccess,
      attempted: persistedDatesChecked.has(isoDate) || !!attempt,
      navigation: attempt,
    };
  });
}

async function buildCoverageDebugPayload() {
  const discovered = getDiscoveredAvailableDates();
  const expected = expectedAvailableBookingDates();
  const scrapeWindowExpected = expectedDatesInScrapeWindow();
  const dbStats = await queryCurrentSessionsByDateFromDb();
  const currentMap = dbStats.byDate;
  const availByDate = await queryAvailabilitySnapshotsByDate();
  const snapMeta = await fetchScrapeSnapshotMeta();
  const snapDates = Object.keys(snapMeta.scrapeSnapshotsByDate || {});

  const datesInCurrentSessions = expected.filter(d => (currentMap[d] || 0) > 0).sort();
  const missingDatesFromCurrentSessions = discovered.length
    ? discovered.filter(d => !(currentMap[d] > 0))
    : expected.filter(d => !(currentMap[d] > 0));
  const datesWithBasicRows = (discovered.length ? discovered : expected)
    .filter(d => (currentMap[d] || 0) > 0)
    .sort();

  let datesWithVerifiedDetails = [];
  if (discovered.length || expected.length) {
    const checkDates = discovered.length ? discovered : expected;
    const rowsByDate = {};
    for (const s of allStoredSessions()) {
      const dk = sessionDateKey(s);
      if (!checkDates.includes(dk)) continue;
      if (!rowsByDate[dk]) rowsByDate[dk] = [];
      rowsByDate[dk].push(s);
    }
    datesWithVerifiedDetails = checkDates.filter(d =>
      (rowsByDate[d] || []).some(s => sessionDetailVerified(s)),
    ).sort();
  }

  const missingDiscoveredDates = discovered.filter(d => !(currentMap[d] > 0));
  const datesAttempted = (discovered.length ? discovered : expected).filter(d =>
    persistedDatesChecked.has(d) || !!collectorState.dateCoverageAttempts[d],
  );
  const datesSucceeded = (discovered.length ? discovered : expected).filter(d =>
    (currentMap[d] > 0) || (datesCheckedEmpty.has(d) && persistedDatesChecked.has(d)),
  );
  const datesFailed = datesAttempted.filter(d => {
    const reason = collectorState.dateCoverageAttempts[d]?.failureReason;
    return !(currentMap[d] > 0)
      && !datesCheckedEmpty.has(d)
      && reason
      && reason !== 'checked_empty_no_sessions_on_site';
  });

  const failureReasonCounts = {};
  for (const d of datesAttempted) {
    const reason = collectorState.dateCoverageAttempts[d]?.failureReason
      || collectorState.dateCoverageAttempts[d]?.navigationError;
    if (reason) failureReasonCounts[reason] = (failureReasonCounts[reason] || 0) + 1;
  }

  const coverage = computeDateCoverage();
  const detailStats = detailCoverageStats();
  const sparse = isCurrentSessionsSparse();
  const fallback = await checkFallbackAvailable();

  let recommendedAction = 'none';
  if (!discovered.length) recommendedAction = 'POST /api/admin/backfill-available-dates';
  else if (sparse && fallback) recommendedAction = 'POST /api/admin/backfill-current-sessions';
  else if (sparse) recommendedAction = 'POST /api/admin/backfill-available-dates or wait for Tier 2 scrape';
  else if (missingDiscoveredDates.length) recommendedAction = 'POST /api/admin/backfill-available-dates';

  return {
    parkTodayIso: getParkTodayIso(),
    maxHorizonDays: MAX_BOOKING_HORIZON_DAYS,
    maxHorizonDate: maxHorizonDateKey(),
    discoveredAvailableDates: discovered,
    discoveredAvailableDatesCount: discovered.length,
    lastDiscoveryAt: collectorState.lastDiscoveryAt,
    discoveryDiagnostics: collectorState.discoveryDiagnostics,
    datesWithBasicRows,
    datesWithVerifiedDetails,
    missingDiscoveredDates,
    datesAttempted,
    datesAttemptedCount: datesAttempted.length,
    datesSucceeded,
    datesSucceededCount: datesSucceeded.length,
    datesFailed,
    datesFailedCount: datesFailed.length,
    failureReasonCounts,
    lastBackfillAvailableDatesResult: collectorState.lastBackfillAvailableDatesResult,
    dateStatuses: await buildAvailableDatesCoverageStatuses(),
    scrapeWeeksAhead: SCRAPE_WEEKS_AHEAD,
    effectiveWeeksAhead,
    expectedDates: expected,
    expectedDatesCount: expected.length,
    scrapeWindowExpectedDates: scrapeWindowExpected,
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
    lastDateRangeBackfill: collectorState.lastDateRangeBackfill,
    scrapeInProgress,
    backgroundCollectorEnabled,
    scrapeScheduleEnabled: !!scrapeScheduleEnabled,
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

async function buildSessionsForDatePayload(isoDate, { mergeIntoStore = false } = {}) {
  const chain = await loadSessionsForDateFromSupabaseFast(isoDate, { mergeIntoStore });
  const { sessions: dateSessions, dataSource: src, schemaError, isFallback = false, queryMs } = chain;

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
  const sanitizedSessions = dateSessions.map(s => sanitizeSessionForApi(s));
  return {
    isoDate,
    sessions: sanitizedSessions,
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
    supabaseQueryMs: queryMs ?? lastSupabaseDateQueryMs,
  };
}

async function loadSessionsForDateFromSupabaseFast(isoDate, { mergeIntoStore = false } = {}) {
  if (!isoDate) {
    return { sessions: [], dataSource: 'none', schemaError: null, isFallback: false, queryMs: 0 };
  }

  if (!supabase) {
    const chain = await queryCurrentSessionsForDate(isoDate, { mergeIntoStore });
    return { ...chain, schemaError: null };
  }

  if (!supabaseSchemaHealth.checkedAt) {
    auditSupabaseSchema().catch(console.error);
  }

  if (!supabaseSchemaHealth.currentSessionsAvailable && supabaseSchemaHealth.checkedAt) {
    return loadSessionsForDateFallback(isoDate, formatSchemaError('current_sessions'));
  }

  try {
    const chain = await queryCurrentSessionsForDate(isoDate, { mergeIntoStore });
    if (chain.sessions.length || chain.isFallback) {
      supabaseSchemaHealth.currentSessionsAvailable = true;
      return { ...chain, schemaError: null };
    }

    if (persistedDatesChecked.has(isoDate) || datesCheckedEmpty.has(isoDate)) {
      return { sessions: [], dataSource: 'supabase/current_sessions', schemaError: null, isFallback: false, queryMs: chain.queryMs };
    }

    return { sessions: [], dataSource: 'supabase/current_sessions', schemaError: null, isFallback: false, queryMs: chain.queryMs };
  } catch (e) {
    if (isMissingTableError(e)) {
      return loadSessionsForDateFallback(isoDate, formatSchemaError('current_sessions'));
    }
    throw e;
  }
}

function sessionsMissingDetails(sessions) {
  return asSessionArray(sessions).filter(s => s?.available && !sessionHasDetailedData(s));
}

function isTodayOrTomorrowIso(isoDate) {
  const days = daysFromToday(isoDate);
  return days >= 0 && days <= 1;
}

function scheduleBackgroundDateDetail(isoDate, sessions, { reason = 'api_date_request' } = {}) {
  if (!isoDate) return;
  const needing = sessionsMissingDetails(sessions);
  if (!needing.length) return;
  if (scrapeInProgress || detailEnrichmentInProgress) {
    const priority = isTodayOrTomorrowIso(isoDate) ? 1 : 2;
    enqueueSessionsForEnrichment(needing, { priority, reason: `${reason}:${isoDate}` }).catch(console.error);
    return;
  }
  const priority = isTodayOrTomorrowIso(isoDate) ? 1 : enrichmentPriorityForSession(needing[0]);
  setImmediate(() => {
    enqueueSessionsForEnrichment(needing, { priority, reason: `${reason}:${isoDate}` })
      .then(() => runDetailEnrichment({ isoDate, sessions: needing, reason: `${reason}:${isoDate}` }))
      .catch(console.error);
  });
}

function buildDateDetailDiagnostics(isoDate, sessions) {
  const list = asSessionArray(sessions);
  const available = list.filter(s => s.available);
  const withSlots = list.filter(s => s.slots != null);
  const withCapacity = list.filter(s => s.capacity != null);
  const withBooked = list.filter(s => s.estimatedBooked != null);
  const failedDetails = list.filter(s => {
    const st = effectiveDetailStatus(s);
    return isDetailFailureStatus(st);
  });
  const unknownDetails = list.filter(s => isDetailUnknownStatus(s.detailStatus || s.detail_status)
    && !isDetailFailureStatus(effectiveDetailStatus(s)));
  const checkedOpenNoSlots = list.filter(s => {
    const st = effectiveDetailStatus(s);
    return st === 'checked_open_no_slots_visible' && s.available && s.slots == null;
  });
  const unavailable = available.filter(s => {
    const status = effectiveDetailStatus(s);
    return !sessionHasDetailedData(s) && (isDetailFailureStatus(status) || s.detailError || s.detail_error);
  });
  const pending = available.filter(s => {
    const st = effectiveDetailStatus(s);
    return !sessionHasDetailedData(s) && !isDetailFailureStatus(st)
      && st !== 'checked_open_no_slots_visible' && st !== 'unknown';
  });
  const checkTimes = dateCheckTimestamps(list);
  const detailStatusSummary = {};
  for (const s of list) {
    const st = effectiveDetailStatus(s);
    detailStatusSummary[st] = (detailStatusSummary[st] || 0) + 1;
  }

  let detailsUnavailableReason = null;
  if (unavailable.length) {
    const errors = [...new Set(unavailable.map(s => s.detailError || s.detail_error || 'no_details').filter(Boolean))];
    detailsUnavailableReason = errors.length ? errors.join('; ') : 'detail scrape returned no slots/capacity';
  } else if (pending.length) {
    detailsUnavailableReason = 'details pending — enrichment not completed yet';
  } else if (!available.length) {
    detailsUnavailableReason = 'no open sessions on this date';
  }

  const tier1RunForDate = lastTierRun[1] && isTodayOrTomorrowIso(isoDate);
  const minutesSinceTier1 = lastTierRun[1]
    ? Math.round((Date.now() - new Date(lastTierRun[1]).getTime()) / 60_000)
    : null;

  const failedDetailsSample = failedDetails.slice(0, 12).map(sessionDetailDiagnosticsFields);
  const unknownDetailsSample = unknownDetails.slice(0, 12).map(sessionDetailDiagnosticsFields);
  const failedCookieOverlay = list.filter(s => effectiveDetailStatus(s) === 'failed_cookie_overlay');
  const failedCookieOverlaySample = failedCookieOverlay.slice(0, 12).map(sessionDetailDiagnosticsFields);
  const checkedOpenNoSlotsSample = checkedOpenNoSlots.slice(0, 12).map(sessionDetailDiagnosticsFields);

  const checkedButNoSlotsSample = [
    ...checkedOpenNoSlots,
    ...available.filter(s => {
      const st = effectiveDetailStatus(s);
      return (st === 'checked' || st === 'checked_open_no_slots_visible') && s.slots == null && s.capacity == null;
    }),
  ]
    .filter((s, i, arr) => arr.findIndex(x => x.key === s.key) === i)
    .slice(0, 12)
    .map(sessionDetailDiagnosticsFields);

  return {
    sessionsCount: list.length,
    sessionsWithSlotsCount: withSlots.length,
    sessionsWithCapacityCount: withCapacity.length,
    sessionsWithEstimatedBookedCount: withBooked.length,
    sessionsWithDetailsUnavailableCount: unavailable.length,
    sessionsDetailsPendingCount: pending.length,
    failedDetailsCount: failedDetails.length,
    unknownDetailsCount: unknownDetails.length,
    failedDetailsSample,
    unknownDetailsSample,
    failedCookieOverlayCount: failedCookieOverlay.length,
    failedCookieOverlaySample,
    checkedOpenNoSlotsCount: checkedOpenNoSlots.length,
    checkedOpenNoSlotsSample,
    checkedButNoSlotsCount: checkedButNoSlotsSample.length,
    checkedButNoSlotsSample,
    lastBasicCheckAt: checkTimes.lastBasicCheckAt,
    lastDetailedCheckAt: checkTimes.lastDetailedCheckAt,
    detailStatusSummary,
    sampleSessionsMissingDetails: available.filter(s => !sessionHasDetailedData(s)).slice(0, 8).map(s => ({
      key: s.key,
      time: s.time,
      level: s.level,
      detailStatus: s.detailStatus || s.detail_status,
      detailError: s.detailError || s.detail_error,
      lastBasicCheckAt: s.lastBasicCheckAt,
      lastDetailedCheckAt: s.lastDetailedCheckAt,
      slots: s.slots,
    })),
    detailsUnavailableReason,
    tier1DetailScrapeExpected: isTodayOrTomorrowIso(isoDate),
    tier1HasRunRecently: tier1RunForDate && minutesSinceTier1 != null && minutesSinceTier1 <= CHECK_MINS * 2,
    lastTier1Scrape: lastTierRun[1],
    minutesSinceTier1Scrape: minutesSinceTier1,
    lastTier1DurationMs,
    latestDetailEnrichmentErrors: enrichmentMetrics.recentErrors.slice(-5),
    dateNavigationDiagnostics: collectorState.dateNavigationByDate[isoDate]
      || collectorState.dateCoverageAttempts[isoDate]
      || null,
    ...cookieDiagnosticsPayload(),
    ...buildDetailAssociationDiagnostics(isoDate, list),
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

function buildTopDetailErrors(errors, limit = 5) {
  const counts = {};
  for (const e of errors || []) {
    const key = e.error || e.message || 'unknown';
    counts[key] = (counts[key] || 0) + 1;
  }
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([error, count]) => ({ error, count }));
}

async function buildSessionDebugPayload(sessionKey) {
  await ensureSessionsForStatus();
  let row = sessionsByKey.get(sessionKey) || null;
  if (!row && supabase) {
    const { data, error } = await supabase
      .from('current_sessions')
      .select('*')
      .eq('park', PARK)
      .eq('session_key', sessionKey)
      .maybeSingle();
    if (!error && data) row = currentRowToSession(data);
  }
  if (!row) {
    return { sessionKey, found: false, error: 'session_not_found' };
  }

  let recentSnapshots = [];
  if (supabase) {
    const { data: snaps } = await supabase
      .from('availability_snapshots')
      .select('scraped_at, snapshot_type, slots_available, capacity, estimated_booked, fill_rate, price_text, available, raw')
      .eq('session_key', sessionKey)
      .order('scraped_at', { ascending: false })
      .limit(12);
    recentSnapshots = snaps || [];
  }

  const detailStatus = effectiveDetailStatus(row);
  const parseSource = row.detailRawText || row.raw?.detailRawText || row.detailRawTileText || row.raw?.tileText || row.tileText || '';
  const parserOutput = row.detailParseOutput || row.raw?.detailParseOutput || buildParserOutputFromText(parseSource);

  const recentDetailAttempts = [];
  const attemptAt = row.lastDetailedCheckAt || row.last_detailed_check_at;
  if (attemptAt) {
    recentDetailAttempts.push({
      at: attemptAt,
      detail_status: detailStatus,
      detail_error: row.detailError || row.detail_error,
      failed_selector: row.detailFailedSelector || row.raw?.detailFailedSelector || null,
    });
  }
  if (isDetailFailureStatus(detailStatus) || detailStatus === 'unknown') {
    recentDetailAttempts.push(...recentDetailAttempts);
  }

  return {
    sessionKey,
    found: true,
    currentSession: row,
    recentAvailabilitySnapshots: recentSnapshots,
    recentDetailErrors: isDetailFailureStatus(detailStatus)
      ? recentDetailAttempts
      : [],
    recentDetailAttempts,
    latestDetailAttempt: {
      rawText: truncateDetailText(row.detailRawText || row.raw?.detailRawText || null, 1500),
      rawTileText: truncateDetailText(row.detailRawTileText || row.raw?.tileText || row.tileText || null, 800),
      capturedAt: attemptAt,
      failedSelector: row.detailFailedSelector || row.raw?.detailFailedSelector || null,
    },
    parserOutput,
    parseResult: {
      detailStatus,
      detailError: row.detailError || row.detail_error,
      parseReason: row.detailParseReason || row.raw?.detailParseReason || parserOutput.parse_reason,
      slots: row.slots,
      capacity: row.capacity,
      estimatedBooked: row.estimatedBooked,
      fillRate: row.fillRate,
      priceText: row.priceText,
      available: row.available,
      ...parserOutput,
    },
    reasonForDetailStatus: reasonForDetailStatus(row),
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
    detailRawText: raw.detailRawText ?? null,
    detailRawTileText: raw.detailRawTileText ?? null,
    detailParseReason: raw.detailParseReason ?? null,
    detailParseOutput: raw.detailParseOutput ?? null,
    detailVerified: raw.detailVerified ?? false,
    detailConfidence: raw.detailConfidence ?? null,
    detailSourceSessionKey: raw.detailSourceSessionKey ?? null,
    detailSourceIsoDate: raw.detailSourceIsoDate ?? null,
    detailSourceStartTime: raw.detailSourceStartTime ?? null,
    detailSourceSessionType: raw.detailSourceSessionType ?? null,
    detailSourceWaveSide: raw.detailSourceWaveSide ?? null,
    tileText: raw.tileText ?? null,
  };
}

function sessionToCurrentRow(s, sourceTier, { scrapeKind = 'basic' } = {}) {
  const now = new Date().toISOString();
  const metrics = computeSessionMetrics(
    s.slots ?? null,
    s.capacity ?? null,
    s.level,
    { inferCapacityFromLevel: false },
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
    raw: {
      ...s,
      detailWarning: s.detailWarning ?? metrics.detailWarning ?? null,
      detailRawText: s.detailRawText ?? null,
      detailRawTileText: s.detailRawTileText ?? null,
      detailParseReason: s.detailParseReason ?? null,
      detailParseOutput: s.detailParseOutput ?? null,
      detailFailedSelector: s.detailFailedSelector ?? null,
      detailVerified: s.detailVerified ?? false,
      detailConfidence: s.detailConfidence ?? null,
      detailSourceSessionKey: s.detailSourceSessionKey ?? null,
      detailSourceIsoDate: s.detailSourceIsoDate ?? null,
      detailSourceStartTime: s.detailSourceStartTime ?? null,
      detailSourceSessionType: s.detailSourceSessionType ?? null,
      detailSourceWaveSide: s.detailSourceWaveSide ?? null,
      modalAssociationVerified: s.modalAssociationVerified ?? false,
      modalDiagnosticRawText: s.modalDiagnosticRawText ?? null,
      modalMismatchReason: s.modalMismatchReason ?? null,
      staleModalDetected: s.staleModalDetected ?? false,
      previousModalTextHash: s.previousModalTextHash ?? null,
      currentModalTextHash: s.currentModalTextHash ?? null,
      tileClickDiagnostics: s.tileClickDiagnostics ?? null,
      tileText: s.tileText ?? s.detailRawTileText ?? null,
    },
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
    scrapeScheduleEnabled: !!scrapeScheduleEnabled,
    backgroundCollectorEnabled,
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
  if (!supabaseSchemaHealth.checkedAt) await auditSupabaseSchema();
  await loadCurrentSessionsFromSupabase({ reloadMeta: !scrapeInProgress });
  if (!sessions.length) await loadLatestSnapshotFromSupabase();
  if (!lastSuccessfulScrape) await loadScrapeMetaFromSupabase();
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
  const writeResult = {
    rowsUpserted: 0,
    sessionsEligibleForUpsert: 0,
    sessionsSkippedBeforeUpsert: 0,
    skipReasons: {},
    error: null,
    upsertError: null,
    sampleSessionBeforeUpsert: null,
    sampleSkippedSessions: [],
    supabaseConfigured: !!supabase,
  };

  if (!supabase) {
    writeResult.error = 'supabase_not_configured';
    writeResult.upsertError = writeResult.error;
    return writeResult;
  }

  const { eligible, skipped, skipReasons } = classifySessionsForUpsert(scrapedSessions);
  writeResult.sessionsEligibleForUpsert = eligible.length;
  writeResult.sessionsSkippedBeforeUpsert = skipped.length;
  writeResult.skipReasons = skipReasons;
  writeResult.sampleSkippedSessions = skipped.slice(0, 5).map((item) => ({
    session_key: item.key || item.session?.key || null,
    iso_date: item.iso_date || sessionDateKey(item.session) || null,
    start_time: item.session?.time || null,
    session_type: item.session?.level || null,
    reason: item.reason,
  }));

  if (!eligible.length) {
    writeResult.error = skipped.length ? 'all_sessions_skipped_before_upsert' : 'empty_batch';
    writeResult.upsertError = writeResult.error;
    return writeResult;
  }

  try {
    const rows = eligible.map((s) => {
      const existing = sessionsByKey.get(s.key);
      const kind = scrapeKind === 'detailed' || sessionHasDetailedData(s) ? 'detailed' : 'basic';
      const merged = mergeSessionFieldsForUpsert(s, existing, { scrapeKind: kind });
      sessionsByKey.set(s.key, merged);
      return sanitizeCurrentSessionRow(sessionToCurrentRow(merged, sourceTier, { scrapeKind: kind }));
    });

    writeResult.sampleSessionBeforeUpsert = rows[0] ? {
      session_key: rows[0].session_key,
      iso_date: rows[0].iso_date,
      start_time: rows[0].start_time,
      session_type: rows[0].session_type,
      wave_side: rows[0].wave_side,
      available: rows[0].available,
      slots_available: rows[0].slots_available,
      detail_status: rows[0].detail_status,
    } : null;

    let upserted = 0;
    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const { error } = await supabase
        .from('current_sessions')
        .upsert(chunk, { onConflict: 'park,session_key' });
      if (error) {
        writeResult.upsertError = error.message;
        writeResult.error = error.message;
        writeResult.errorDetails = {
          code: error.code,
          hint: error.hint,
          details: error.details,
          chunkStart: i,
          chunkSize: chunk.length,
        };
        console.error('  Supabase current_sessions upsert failed:', error.message, error.details || '');
        throw error;
      }
      upserted += chunk.length;
    }
    rebuildSessionsArray();
    writeResult.rowsUpserted = upserted;
    console.log(`  Supabase: upserted ${upserted} current_sessions row(s) (${scrapeKind})`);
    return writeResult;
  } catch (e) {
    writeResult.error = writeResult.error || e.message;
    writeResult.upsertError = writeResult.upsertError || e.message;
    console.error('  Supabase current_sessions upsert failed:', e.message);
    return writeResult;
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

function truncateDetailText(str, max = 2000) {
  if (!str) return null;
  const s = String(str);
  return s.length > max ? `${s.slice(0, max)}…` : s;
}

function isDetailFailureStatus(status) {
  return status === 'failed' || (typeof status === 'string' && status.startsWith('failed_'));
}

function isDetailSuccessStatus(status) {
  if (!status) return false;
  return status.startsWith('checked_') || status === 'checked';
}

function parseDetailAvailabilityFromText(text) {
  if (!text) return null;
  if (isCookieBannerText(text)) return null;
  const t = String(text).replace(/\s+/g, ' ').trim();
  const low = t.toLowerCase();

  if (/\bsold\s*out\b/i.test(t) || /\bpacked\b/i.test(low)
    || /\bunavailable\b/i.test(low)
    || /\bno\s+spots?\s+(left|available|remaining)\b/i.test(t)
    || /\bfull(?:y)?\s+booked\b/i.test(t)) {
    return { packed: true, slots: 0, parseReason: 'text_packed_or_sold_out' };
  }

  const onlyLeft = t.match(/\bonly\s+(\d+)\s+left\b/i);
  if (onlyLeft) {
    return { slots: parseInt(onlyLeft[1], 10), parseReason: 'text_only_n_left' };
  }

  const spotsLeft = t.match(/(\d+)\s+spots?\s+left\b/i);
  if (spotsLeft) {
    return { slots: parseInt(spotsLeft[1], 10), parseReason: 'text_spots_left' };
  }

  const spotLeft = t.match(/(\d+)\s+spot\s+left\b/i);
  if (spotLeft) {
    return { slots: parseInt(spotLeft[1], 10), parseReason: 'text_spot_left' };
  }

  if (/\bspots?\s+left\b/i.test(t) && !/\d+\s+spots?\s+left/i.test(t)) {
    return { openNoCount: true, parseReason: 'text_spots_left_no_number' };
  }

  const nLeft = t.match(/\b(\d+)\s+left\b/i);
  if (nLeft) {
    return { slots: parseInt(nLeft[1], 10), parseReason: 'text_n_left' };
  }

  const frac = t.match(/(\d+)\s*\/\s*(\d+)(?:\s+booked)?/i);
  if (frac) {
    const booked = parseInt(frac[1], 10);
    const cap = parseInt(frac[2], 10);
    if (Number.isFinite(booked) && Number.isFinite(cap) && cap > 0) {
      const slots = cap - booked;
      if (slots >= 0) {
        return {
          slots,
          capacity: cap,
          estimatedBooked: booked,
          parseReason: 'text_fraction_booked',
        };
      }
    }
  }

  const ofMatch = t.match(/(\d+)\s+of\s+(\d+)/i);
  if (ofMatch) {
    const booked = parseInt(ofMatch[1], 10);
    const cap = parseInt(ofMatch[2], 10);
    if (Number.isFinite(booked) && Number.isFinite(cap) && cap > 0) {
      const slots = cap - booked;
      if (slots >= 0) {
        return {
          slots,
          capacity: cap,
          estimatedBooked: booked,
          parseReason: 'text_n_of_capacity',
        };
      }
    }
  }

  if (/\bopen\b/i.test(t) || /\bavailable\b/i.test(t)) {
    return { openNoCount: true, parseReason: 'text_open_or_available' };
  }

  return null;
}

function parseCapacityFromDetailText(text) {
  if (!text) return null;
  const capMatch = String(text).match(/(?:capacity|max(?:imum)?(?:\s+capacity)?)\s*[:\s]*(\d+)/i);
  if (capMatch) return parseInt(capMatch[1], 10);
  return null;
}

function inferDetailStatusFromPayload(details) {
  if (!details) return 'failed_parse';
  if (details.detailStatus) return details.detailStatus;
  if (details.failureType) return details.failureType;
  if (details.packed || details.slots === 0) return 'checked_packed';
  if (details.openNoCount) {
    return details.parseReason === 'text_open_or_available'
      ? 'checked_available_no_slot_count'
      : 'checked_open_no_slots_visible';
  }
  if (details.slots != null && details.verified !== false) return 'checked_with_slots';
  if (details.rawModalText && String(details.rawModalText).trim().length > 8) return 'failed_parse';
  return 'failed_parse';
}

function ensureDetailStatusRecorded(entry, fallback = 'failed_parse') {
  const st = normalizeDetailStatus(entry?.detailStatus);
  if (!st || st === 'unknown' || st === 'checking') {
    entry.detailStatus = fallback;
    if (!entry.detailError) entry.detailError = 'detail_status_missing_after_attempt';
  }
}

function sessionLookupContext(session) {
  if (!session) return {};
  const raw = session.raw && typeof session.raw === 'object' ? session.raw : {};
  return {
    ts: Number(session.ts),
    wave: Number(session.wave),
    key: session.key,
    time: session.time,
    level: session.level || session.session_type,
    waveSide: session.waveSide,
    isoDate: session.isoDate || session.dateKey,
    tileText: session.tileText || session.detailRawTileText || raw.tileText || null,
    tileColumnIndex: session.tileColumnIndex ?? raw.tileColumnIndex ?? null,
    tileClassName: session.tileClassName ?? raw.tileClassName ?? null,
  };
}

function isCookieBannerText(text) {
  if (!text) return false;
  const low = String(text).toLowerCase();
  return (low.includes('cookie') && (low.includes('allow') || low.includes('refuse') || low.includes('consent')))
    || low.includes('this website uses cookies')
    || (low.includes('refuse cookies') && low.includes('allow cookies'));
}

function modalTextLooksLikeSessionDetail(text, session) {
  if (!text || isCookieBannerText(text)) return false;
  const validation = validateModalAssociation(session, text);
  if (validation.confidence === 'mismatch') return false;
  const low = text.toLowerCase();
  if (/session level|from\s*:|qty|spot|book|packed|sold out|\$\d/i.test(text)) return true;
  if (session?.level && low.includes(String(session.level).toLowerCase())) return true;
  if (session?.time) {
    const t = String(session.time).toLowerCase().replace(/\s+/g, ' ').trim();
    if (t && t !== '?' && low.includes(t.replace(/\s/g, ''))) return true;
  }
  return text.trim().length > 40 && !isCookieBannerText(text);
}

const MODAL_MONTHS = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
};

function normalizeTimeToken(t) {
  if (!t) return null;
  const s = String(t).toLowerCase().replace(/\s+/g, ' ').trim();
  const m = s.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)/);
  if (!m) return s.replace(/\s/g, '');
  let hr = parseInt(m[1], 10);
  const min = m[2] ? parseInt(m[2], 10) : 0;
  if (m[3] === 'pm' && hr !== 12) hr += 12;
  if (m[3] === 'am' && hr === 12) hr = 0;
  return `${String(hr).padStart(2, '0')}:${String(min).padStart(2, '0')}`;
}

function parseModalDateLabel(text) {
  if (!text) return null;
  const m = String(text).match(/\b([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})\b/);
  if (!m) return null;
  const mon = MODAL_MONTHS[m[1].slice(0, 3).toLowerCase()];
  if (mon == null) return null;
  const day = parseInt(m[2], 10);
  const year = parseInt(m[3], 10);
  if (!Number.isFinite(day) || !Number.isFinite(year)) return null;
  return `${year}-${String(mon + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

function parseModalSessionType(text) {
  if (!text) return null;
  const t = String(text);
  const patterns = [
    'Expert Turns', 'Pro Turns', 'Progressive', 'Lesson Only',
    'Cruiser', 'Beginner', 'Intermediate', 'Advanced',
  ];
  for (const p of patterns) {
    if (new RegExp(`\\b${p.replace(/\s+/g, '\\s+')}\\b`, 'i').test(t)) return p;
  }
  const m = t.match(/Session level\s*:?\s*([^|<\n]+)/i);
  return m ? m[1].replace(/\s+/g, ' ').trim() : null;
}

function parseModalWaveSide(text) {
  if (!text) return null;
  const t = String(text);
  const m = t.match(/\b(Left|Right)\s+(Wave(?:\s+Sessions)?|Lesson)\b/i);
  if (m) {
    const side = m[1];
    const kind = /lesson/i.test(m[2]) ? 'Lesson' : 'Wave';
    return `${side} ${kind}`;
  }
  return null;
}

function parseModalIdentityFromText(text) {
  if (!text) return {};
  const t = String(text).replace(/\s+/g, ' ').trim();
  const isoDate = parseModalDateLabel(t);
  const timeMatch = t.match(/\bat\s+([\d]{1,2}(?::\d{2})?\s*[ap]m)\b/i)
    || t.match(/\b([\d]{1,2}(?::\d{2})?\s*[ap]m)\b/i);
  const startTime = timeMatch ? timeMatch[1].replace(/\s+/g, ' ').trim().toLowerCase() : null;
  return {
    isoDate,
    startTime,
    sessionType: parseModalSessionType(t),
    waveSide: parseModalWaveSide(t),
    dateLabel: isoDate ? t.match(/\b[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4}\b/i)?.[0] || null : null,
  };
}

function normalizeLevelToken(level) {
  return String(level || '').toLowerCase().replace(/\s+/g, ' ').trim();
}

function levelsMatch(expected, found) {
  if (!expected || !found) return !expected && !found;
  const e = normalizeLevelToken(expected);
  const f = normalizeLevelToken(found);
  return e === f || f.includes(e) || e.includes(f);
}

function waveSidesMatch(expected, found) {
  if (!expected || !found) return !expected && !found;
  const e = normalizeLevelToken(expected);
  const f = normalizeLevelToken(found);
  return e === f || f.includes(e) || e.includes(f);
}

function validateModalAssociation(session, rawModalText) {
  const parsed = parseModalIdentityFromText(rawModalText);
  const expected = {
    isoDate: session?.isoDate || session?.dateKey || null,
    startTime: session?.time || null,
    sessionType: session?.level || session?.session_type || null,
    waveSide: session?.waveSide || null,
    sessionKey: session?.key || null,
  };
  const mismatches = [];

  if (parsed.isoDate && expected.isoDate && parsed.isoDate !== expected.isoDate) {
    mismatches.push({ field: 'isoDate', expected: expected.isoDate, found: parsed.isoDate });
  }
  if (parsed.startTime && expected.startTime) {
    if (normalizeTimeToken(parsed.startTime) !== normalizeTimeToken(expected.startTime)) {
      mismatches.push({ field: 'startTime', expected: expected.startTime, found: parsed.startTime });
    }
  }
  if (parsed.sessionType && expected.sessionType && !levelsMatch(expected.sessionType, parsed.sessionType)) {
    mismatches.push({ field: 'sessionType', expected: expected.sessionType, found: parsed.sessionType });
  }
  if (parsed.waveSide && expected.waveSide && !waveSidesMatch(expected.waveSide, parsed.waveSide)) {
    mismatches.push({ field: 'waveSide', expected: expected.waveSide, found: parsed.waveSide });
  }

  if (mismatches.length) {
    return { match: false, confidence: 'mismatch', parsedFromModal: parsed, mismatches, expected };
  }
  const hasIdentity = !!(parsed.isoDate && parsed.startTime);
  if (hasIdentity) {
    return { match: true, confidence: 'exact_match', parsedFromModal: parsed, mismatches: [], expected };
  }
  return { match: false, confidence: 'weak_match', parsedFromModal: parsed, mismatches: [{ field: 'identity', expected: 'date_and_time', found: 'insufficient' }], expected };
}

function sessionDetailVerified(s) {
  if (!s) return false;
  return s.detailVerified === true || s.raw?.detailVerified === true;
}

function isDefaultLikeDetailValues(slots, capacity, estimatedBooked) {
  if (slots === 10 && capacity === 12 && estimatedBooked === 2) return true;
  if (slots === 10 && capacity === 12) return true;
  if (slots === 10 && estimatedBooked === 2) return true;
  return false;
}

function applyDetailSourceFields(entry, session, validation) {
  const src = validation?.parsedFromModal || {};
  entry.detailSourceSessionKey = session?.key || null;
  entry.detailSourceIsoDate = src.isoDate || session?.isoDate || session?.dateKey || null;
  entry.detailSourceStartTime = src.startTime || session?.time || null;
  entry.detailSourceSessionType = src.sessionType || session?.level || null;
  entry.detailSourceWaveSide = src.waveSide || session?.waveSide || null;
}

function clearUnverifiedDetailMetrics(entry) {
  entry.slots = null;
  entry.capacity = null;
  entry.estimatedBooked = null;
  entry.fillRate = null;
  entry.priceText = null;
  entry.priceMin = null;
  entry.priceMax = null;
  entry.detailVerified = false;
}

function preservePriorVerifiedDetailFields(entry, prior) {
  if (!prior || prior.key !== entry.key || !sessionDetailVerified(prior)) return;
  for (const field of [
    'slots', 'capacity', 'estimatedBooked', 'fillRate',
    'priceText', 'priceMin', 'priceMax', 'currency', 'available',
    'detailVerified', 'detailConfidence',
    'detailSourceSessionKey', 'detailSourceIsoDate', 'detailSourceStartTime',
    'detailSourceSessionType', 'detailSourceWaveSide',
    'detailStatus',
  ]) {
    if (prior[field] != null) entry[field] = prior[field];
  }
}

function sanitizeSessionForApi(s, { debug = false } = {}) {
  if (!s) return s;
  const out = { ...s };
  const verified = sessionDetailVerified(s);
  const parserOutput = out.detailParseOutput || out.raw?.detailParseOutput
    || buildParserOutputFromText(out.detailRawText || out.raw?.detailRawText || '');

  if (!debug) {
    if (!verified) {
      if (out.detailStatus !== 'checked_packed' || !verified) {
        out.slots = null;
        out.capacity = null;
        out.estimatedBooked = null;
        out.fillRate = null;
        out.priceText = null;
        out.priceMin = null;
        out.priceMax = null;
      }
      if (!modalAssociationVerifiedOnSession(s)) {
        out.detailRawText = null;
      }
    } else if (isDefaultLikeDetailValues(out.slots, out.capacity, out.estimatedBooked)) {
      out.slots = null;
      out.capacity = null;
      out.estimatedBooked = null;
      out.fillRate = null;
      out.detailVerified = false;
      out.detailConfidence = 'default_suppressed';
    }
  }

  out.detailStatus = reconcileDetailStatusForSession(out);
  out.modalAssociationVerified = modalAssociationVerifiedOnSession(s);
  out.modalDiagnosticRawText = s.modalDiagnosticRawText || s.raw?.modalDiagnosticRawText || null;
  out.staleModalDetected = s.staleModalDetected === true || s.raw?.staleModalDetected === true;
  out.previousModalTextHash = s.previousModalTextHash || s.raw?.previousModalTextHash || null;
  out.currentModalTextHash = s.currentModalTextHash || s.raw?.currentModalTextHash || null;

  out.detailParseOutput = parserOutput;
  out.detailVerified = verified;
  out.detailConfidence = out.detailConfidence || out.raw?.detailConfidence || (verified ? 'exact_match' : null);
  out.detailSourceSessionKey = out.detailSourceSessionKey || out.raw?.detailSourceSessionKey || null;
  out.detailSourceIsoDate = out.detailSourceIsoDate || out.raw?.detailSourceIsoDate || null;
  out.detailSourceStartTime = out.detailSourceStartTime || out.raw?.detailSourceStartTime || null;
  out.detailSourceSessionType = out.detailSourceSessionType || out.raw?.detailSourceSessionType || null;
  out.detailSourceWaveSide = out.detailSourceWaveSide || out.raw?.detailSourceWaveSide || null;
  return out;
}

function buildDetailAssociationDiagnostics(isoDate, sessions) {
  const list = asSessionArray(sessions);
  const modalMismatch = [];
  const defaultLike = [];
  const unparsedButDisplayed = [];
  const dateDiffers = [];
  const timeDiffers = [];
  const staleModal = [];
  const tileMismatch = [];
  const checkedWithSlotsButNull = [];
  const detailValueGroups = new Map();
  const rawModalGroups = new Map();
  const associationSamples = [];
  const modalLifecycleSamples = [];
  const tileClickSamples = [];

  for (const s of list) {
    const rawModal = s.detailRawText || s.raw?.detailRawText || s.modalDiagnosticRawText || s.raw?.modalDiagnosticRawText || '';
    const parserOutput = s.detailParseOutput || s.raw?.detailParseOutput
      || buildParserOutputFromText(rawModal);
    const validation = rawModal ? validateModalAssociation(s, rawModal) : null;

    if (validation?.confidence === 'mismatch') {
      modalMismatch.push({
        key: s.key,
        isoDate: s.isoDate || s.dateKey,
        time: s.time,
        sessionType: s.level,
        waveSide: s.waveSide,
        detailStatus: effectiveDetailStatus(s),
        mismatches: validation.mismatches,
        rawModalSample: truncateDetailText(rawModal, 200),
      });
    }

    if (isDefaultLikeDetailValues(s.slots, s.capacity, s.estimatedBooked)) {
      defaultLike.push({
        key: s.key, time: s.time, sessionType: s.level,
        slots: s.slots, capacity: s.capacity, estimatedBooked: s.estimatedBooked,
        detailVerified: sessionDetailVerified(s),
      });
    }

    if ((s.slots != null || s.capacity != null)
      && parserOutput.parsed_slots_available == null
      && parserOutput.parsed_capacity == null
      && !sessionDetailVerified(s)) {
      unparsedButDisplayed.push({
        key: s.key, time: s.time, slots: s.slots, capacity: s.capacity,
        parseReason: parserOutput.parse_reason,
      });
    }

    const parsedId = parseModalIdentityFromText(rawModal);
    if (parsedId.isoDate && (s.isoDate || s.dateKey) && parsedId.isoDate !== (s.isoDate || s.dateKey)) {
      dateDiffers.push({ key: s.key, expected: s.isoDate || s.dateKey, found: parsedId.isoDate });
    }
    if (parsedId.startTime && s.time && normalizeTimeToken(parsedId.startTime) !== normalizeTimeToken(s.time)) {
      timeDiffers.push({ key: s.key, expected: s.time, found: parsedId.startTime });
    }

    const dvKey = `${s.slots}|${s.capacity}|${s.estimatedBooked}|${s.priceText || ''}`;
    if (s.slots != null || s.capacity != null) {
      if (!detailValueGroups.has(dvKey)) detailValueGroups.set(dvKey, []);
      detailValueGroups.get(dvKey).push(s.key);
    }
    if (isInvalidCheckedWithSlotsStatus(s)) {
      checkedWithSlotsButNull.push({
        key: s.key,
        time: s.time,
        sessionType: s.level,
        detailStatus: s.detailStatus || s.detail_status,
        slots: s.slots,
        capacity: s.capacity,
        modalAssociationVerified: modalAssociationVerifiedOnSession(s),
      });
    }

    if (effectiveDetailStatus(s) === 'failed_modal_stale' || s.staleModalDetected || s.raw?.staleModalDetected) {
      staleModal.push({
        key: s.key, time: s.time, sessionType: s.level,
        previousModalTextHash: s.previousModalTextHash || s.raw?.previousModalTextHash,
        currentModalTextHash: s.currentModalTextHash || s.raw?.currentModalTextHash,
      });
    }

    if (effectiveDetailStatus(s) === 'failed_tile_mismatch') {
      tileMismatch.push({
        key: s.key,
        time: s.time,
        tileClickDiagnostics: s.tileClickDiagnostics || s.raw?.tileClickDiagnostics || null,
      });
    }

    if (s.tileClickDiagnostics || s.raw?.tileClickDiagnostics) {
      tileClickSamples.push({
        key: s.key,
        ...(s.tileClickDiagnostics || s.raw?.tileClickDiagnostics),
      });
    }

    if (s.staleModalDetected || s.raw?.staleModalDetected || s.modalDiagnosticRawText || s.raw?.modalDiagnosticRawText) {
      modalLifecycleSamples.push({
        key: s.key,
        time: s.time,
        staleModalDetected: s.staleModalDetected || s.raw?.staleModalDetected || false,
        previousModalTextHash: s.previousModalTextHash || s.raw?.previousModalTextHash || null,
        currentModalTextHash: s.currentModalTextHash || s.raw?.currentModalTextHash || null,
        modalMismatchReason: s.modalMismatchReason || s.raw?.modalMismatchReason || null,
        modalAssociationVerified: modalAssociationVerifiedOnSession(s),
      });
    }

    if (rawModal) {
      const rmKey = rawModal.slice(0, 120);
      if (!rawModalGroups.has(rmKey)) rawModalGroups.set(rmKey, []);
      rawModalGroups.get(rmKey).push(s.key);
    }

    if (validation || rawModal) {
      associationSamples.push({
        key: s.key,
        isoDate: s.isoDate || s.dateKey,
        time: s.time,
        sessionType: s.level,
        waveSide: s.waveSide,
        detailVerified: sessionDetailVerified(s),
        detailConfidence: s.detailConfidence || s.raw?.detailConfidence || null,
        detailStatus: effectiveDetailStatus(s),
        validationConfidence: validation?.confidence || null,
        mismatches: validation?.mismatches || [],
      });
    }
  }

  const duplicateDetailValueGroups = [...detailValueGroups.entries()]
    .filter(([, keys]) => keys.length > 1)
    .map(([values, keys]) => ({ values, sessionKeys: keys, count: keys.length }))
    .slice(0, 12);

  const duplicateRawModalGroups = [...rawModalGroups.entries()]
    .filter(([, keys]) => keys.length > 1)
    .map(([rawSample, keys]) => ({ rawSample: rawSample.slice(0, 120), sessionKeys: keys, count: keys.length }))
    .slice(0, 12);

  const checkedWithSlots = list.filter(s =>
    effectiveDetailStatus(s) === 'checked_with_slots'
    && sessionDetailVerified(s)
    && s.slots != null
    && s.capacity != null,
  );
  const checkedAvailableNoSlot = list.filter(s => effectiveDetailStatus(s) === 'checked_available_no_slot_count');
  const rowsWithDetailsUnavailable = list.filter(s => {
    if (!s.available) return false;
    const st = effectiveDetailStatus(s);
    return !sessionHasDetailedData(s)
      && (isDetailFailureStatus(st)
        || st === 'checked_available_no_slot_count'
        || st === 'checked_open_no_slots_visible');
  });

  return {
    modalMismatchCount: modalMismatch.length,
    failedModalMismatchSample: modalMismatch.slice(0, 12),
    staleModalCount: staleModal.length,
    failedModalStaleSample: staleModal.slice(0, 12),
    tileMismatchCount: tileMismatch.length,
    failedTileMismatchSample: tileMismatch.slice(0, 12),
    checkedWithSlotsCount: checkedWithSlots.length,
    checkedAvailableNoSlotCount: checkedAvailableNoSlot.length,
    rowsWithDetailsUnavailable: rowsWithDetailsUnavailable.length,
    rowsWithCheckedWithSlotsButNullValuesSample: checkedWithSlotsButNull.slice(0, 12),
    rowsWithDefaultLikeDetailsSample: defaultLike.slice(0, 12),
    rowsWithUnparsedButDisplayedSlotsSample: unparsedButDisplayed.slice(0, 12),
    rowsWhereDetailRawTextDateDiffersFromIsoDate: dateDiffers.slice(0, 12),
    rowsWhereDetailRawTextTimeDiffersFromStartTime: timeDiffers.slice(0, 12),
    duplicateDetailValueGroups,
    duplicateRawModalGroups,
    detailAssociationSamples: associationSamples.slice(0, 20),
    modalLifecycleSamples: modalLifecycleSamples.slice(0, 20),
    tileClickSamples: tileClickSamples.slice(0, 20),
  };
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
  if (!sessionDetailVerified(s)) return false;
  if (s.slots === 0) return true;
  return s.slots != null || s.capacity != null || s.priceText != null || s.priceMin != null;
}

function sessionNeedsDetailEnrichment(s, maxAgeHours = ENRICHMENT_STALE_HOURS) {
  if (!s?.key) return false;
  const watchKeys = watchedSessionKeys();
  if (!s.available && !watchKeys.has(s.key)) return false;
  if (!sessionHasDetailedData(s)) return true;
  if (!s.lastDetailedCheckAt) return true;
  const ageMs = Date.now() - new Date(s.lastDetailedCheckAt).getTime();
  return ageMs > maxAgeHours * 3_600_000;
}

function detailStaleMaxAgeHours(priority) {
  if (priority === 1) return Math.max(CHECK_MINS / 60, 5 / 60);
  if (priority === 2) return Math.max(ENRICHMENT_TIER2_EVERY_MINS / 60, 0.75);
  return ENRICHMENT_TIER3_STALE_HOURS;
}

function sessionQualifiesForDetailEnrichment(s, { availabilityChanged = false, force = false } = {}) {
  if (!s?.key) return false;
  if (force) return s.available !== false;
  const watchKeys = watchedSessionKeys();
  const priority = enrichmentPriorityForSession(s);
  const days = daysFromToday(s.isoDate || s.dateKey || todayDateKey());

  if (!s.available && !watchKeys.has(s.key)) return false;
  if (availabilityChanged || sessionsNeedingDetailAfterBasic.has(s.key)) return true;
  if (watchKeys.has(s.key)) return sessionNeedsDetailEnrichment(s, detailStaleMaxAgeHours(1));
  if (days <= 2) return sessionNeedsDetailEnrichment(s, detailStaleMaxAgeHours(1));
  if (!sessionHasDetailedData(s)) return true;
  return sessionNeedsDetailEnrichment(s, detailStaleMaxAgeHours(priority));
}

function sessionHourFromSession(s) {
  if (s?.time) {
    const m = String(s.time).match(/(\d{1,2})/);
    if (m) return parseInt(m[1], 10);
  }
  if (s?.ts) {
    try {
      return new Date(Number(s.ts) * 1000).getHours();
    } catch {}
  }
  return null;
}

function groupSessionsByWeekOffset(sessions) {
  const groups = new Map();
  for (const s of asSessionArray(sessions)) {
    const dk = s.isoDate || s.dateKey;
    const wo = Math.max(0, Math.floor(daysFromToday(dk || todayDateKey()) / 7));
    if (!groups.has(wo)) groups.set(wo, []);
    groups.get(wo).push(s);
  }
  return [...groups.entries()].sort((a, b) => a[0] - b[0]);
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
    if (sessionDetailVerified(existing)) {
      merged.detailVerified = existing.detailVerified;
      merged.detailConfidence = existing.detailConfidence;
      merged.detailSourceSessionKey = existing.detailSourceSessionKey;
      merged.detailSourceIsoDate = existing.detailSourceIsoDate;
      merged.detailSourceStartTime = existing.detailSourceStartTime;
      merged.detailSourceSessionType = existing.detailSourceSessionType;
      merged.detailSourceWaveSide = existing.detailSourceWaveSide;
    }
    if (isDetailSuccessStatus(existing.detailStatus) && sessionHasDetailedData(merged)) {
      merged.detailStatus = existing.detailStatus;
    }
  }

  if (scrapeKind === 'basic') {
    merged.lastBasicCheckAt = now;
    if (!sessionHasDetailedData(merged) && merged.detailStatus !== 'checking') {
      merged.detailStatus = merged.detailStatus || 'pending';
    }
  }

  if (scrapeKind === 'detailed') {
    merged.lastDetailedCheckAt = incoming.lastDetailedCheckAt || now;
    const incomingStatus = incoming.detailStatus;
    const failed = isDetailFailureStatus(incomingStatus);
    if (failed && existing && sessionDetailVerified(existing) && !sessionDetailVerified(incoming)) {
      for (const field of preserveFields) {
        if (existing[field] != null) merged[field] = existing[field];
      }
      merged.detailVerified = existing.detailVerified;
      merged.detailConfidence = existing.detailConfidence;
      merged.detailSourceSessionKey = existing.detailSourceSessionKey;
      merged.detailSourceIsoDate = existing.detailSourceIsoDate;
      merged.detailSourceStartTime = existing.detailSourceStartTime;
      merged.detailSourceSessionType = existing.detailSourceSessionType;
      merged.detailSourceWaveSide = existing.detailSourceWaveSide;
    }
    if (failed) {
      merged.detailStatus = incomingStatus || merged.detailStatus;
      merged.detailError = incoming.detailError ?? merged.detailError;
    } else {
      merged.detailStatus = incomingStatus || merged.detailStatus;
      if (merged.detailWarning) merged.detailError = merged.detailWarning;
      else if (isDetailSuccessStatus(merged.detailStatus)) merged.detailError = null;
    }
    merged.detailStatus = reconcileDetailStatusForSession(merged);
  }

  return merged;
}

function detailCoverageStats() {
  const all = allStoredSessions();
  const withSlots = all.filter(s => s.slots != null);
  const withCapacity = all.filter(s => s.capacity != null);
  const withPrice = all.filter(s => s.priceText || s.priceMin != null);
  const needing = all.filter(s => sessionQualifiesForDetailEnrichment(s));
  const total = all.length || 1;
  return {
    sessionsWithSlotsCount: withSlots.length,
    sessionsWithCapacityCount: withCapacity.length,
    sessionsWithPriceCount: withPrice.length,
    sessionsNeedingDetailCount: needing.length,
    detailCoveragePercent: Math.round((withSlots.length / total) * 100),
  };
}

function applyModalTrustFields(entry, details, { trusted = false } = {}) {
  entry.modalAssociationVerified = trusted;
  entry.modalMismatchReason = details?.modalMismatchReason || details?.detailError || null;
  entry.staleModalDetected = details?.staleModalDetected === true;
  entry.previousModalTextHash = details?.previousModalTextHash ?? entry.previousModalTextHash ?? null;
  entry.currentModalTextHash = details?.currentModalTextHash ?? entry.currentModalTextHash ?? null;
  if (details?.tileClickDiagnostics) entry.tileClickDiagnostics = details.tileClickDiagnostics;

  if (trusted && details?.rawModalText) {
    entry.detailRawText = truncateDetailText(details.rawModalText);
    entry.modalDiagnosticRawText = null;
  } else {
    entry.detailRawText = null;
    if (details?.rawModalText) {
      entry.modalDiagnosticRawText = truncateDetailText(details.rawModalText);
    }
  }
}

function applyDetailPayloadToSession(entry, details, level, { prior = null, session = null } = {}) {
  if (!entry || !details) return { ok: false, category: 'failed' };

  const priorSnapshot = prior || { ...entry };
  const now = new Date().toISOString();
  const status = inferDetailStatusFromPayload(details);
  const sourceSession = session || entry;
  const rawModal = details.rawModalText || null;

  if (details.rawTileText) entry.detailRawTileText = truncateDetailText(details.rawTileText);
  if (details.parseReason) entry.detailParseReason = details.parseReason;
  if (details.failedSelector) entry.detailFailedSelector = details.failedSelector;
  entry.lastDetailedCheckAt = now;

  if (isDetailFailureStatus(status)) {
    entry.detailParseOutput = buildParserOutputFromText(details.rawModalText || details.rawTileText || '');
    applyModalTrustFields(entry, details, { trusted: false });
    entry.detailStatus = normalizeDetailStatus(status) || status;
    entry.detailError = details.detailError || status;
    entry.detailVerified = false;
    entry.detailConfidence = status === 'failed_modal_stale' ? 'mismatch'
      : status === 'failed_tile_mismatch' ? 'mismatch'
      : status === 'failed_modal_mismatch' ? 'mismatch'
      : 'default_suppressed';
    preservePriorVerifiedDetailFields(entry, priorSnapshot);
    if (!sessionDetailVerified(entry)) clearUnverifiedDetailMetrics(entry);
    ensureDetailStatusRecorded(entry, entry.detailStatus);
    return { ok: false, category: 'failed', status: entry.detailStatus };
  }

  entry.detailParseOutput = buildParserOutputFromText(details.rawModalText || details.rawTileText || '');

  if (rawModal) {
    const validation = details.modalValidation || validateModalAssociation(sourceSession, rawModal);
    entry.modalValidation = validation;
    applyDetailSourceFields(entry, sourceSession, validation);

    if (validation.confidence === 'mismatch') {
      applyModalTrustFields(entry, {
        ...details,
        modalMismatchReason: `modal_mismatch: ${validation.mismatches.map(m => `${m.field} expected ${m.expected} got ${m.found}`).join('; ')}`,
      }, { trusted: false });
      entry.detailStatus = 'failed_modal_mismatch';
      entry.detailError = entry.modalMismatchReason;
      entry.detailVerified = false;
      entry.detailConfidence = 'mismatch';
      clearUnverifiedDetailMetrics(entry);
      preservePriorVerifiedDetailFields(entry, priorSnapshot);
      ensureDetailStatusRecorded(entry, 'failed_modal_mismatch');
      return { ok: false, category: 'failed', status: 'failed_modal_mismatch', validation };
    }

    if (validation.confidence === 'weak_match' && (details.slots != null || details.capacity != null || details.slotsFromClicks != null)) {
      applyModalTrustFields(entry, {
        ...details,
        modalMismatchReason: 'modal_identity_not_confirmed',
      }, { trusted: false });
      entry.detailStatus = 'failed_modal_mismatch';
      entry.detailError = 'modal_identity_not_confirmed';
      entry.detailVerified = false;
      entry.detailConfidence = 'weak_match';
      clearUnverifiedDetailMetrics(entry);
      preservePriorVerifiedDetailFields(entry, priorSnapshot);
      ensureDetailStatusRecorded(entry, 'failed_modal_mismatch');
      return { ok: false, category: 'failed', status: 'failed_modal_mismatch', validation };
    }
  } else {
    applyModalTrustFields(entry, details, { trusted: false });
  }

  if (status === 'unknown') {
    entry.detailStatus = 'failed_parse';
    entry.detailError = details.detailError || details.parseReason || 'insufficient_detail_signal';
    entry.detailVerified = false;
    entry.detailConfidence = 'default_suppressed';
    clearUnverifiedDetailMetrics(entry);
    preservePriorVerifiedDetailFields(entry, priorSnapshot);
    ensureDetailStatusRecorded(entry, 'failed_parse');
    return { ok: false, category: 'failed', status: 'failed_parse' };
  }

  const parserOutput = entry.detailParseOutput || {};
  const hasParsedSlots = parserOutput.parsed_slots_available != null;
  const hasParsedCapacity = parserOutput.parsed_capacity != null;

  if (details.available === false || status === 'checked_packed' || details.packed || details.slots === 0) {
    entry.available = false;
    entry.slots = 0;
    if (details.capacity != null && hasParsedCapacity) entry.capacity = details.capacity;
    else entry.capacity = null;
    if (entry.capacity != null) {
      entry.estimatedBooked = entry.capacity;
      entry.fillRate = 1;
    } else {
      entry.estimatedBooked = null;
      entry.fillRate = null;
    }
    const validation = rawModal ? (details.modalValidation || validateModalAssociation(sourceSession, rawModal)) : null;
    const trusted = validation?.match && validation?.confidence === 'exact_match';
    applyModalTrustFields(entry, details, { trusted });
    if (details.price_text && trusted && parserOutput.parsed_price_text) {
      entry.priceText = details.price_text;
      entry.priceMin = details.price_min;
      entry.priceMax = details.price_max;
      entry.currency = details.currency || 'USD';
    }
    entry.detailStatus = 'checked_packed';
    entry.detailError = null;
    entry.detailVerified = trusted;
    entry.detailConfidence = trusted ? 'exact_match' : 'weak_match';
    return { ok: entry.detailVerified, category: 'packed' };
  }

  if (status === 'checked_open_no_slots_visible' || status === 'checked_available_no_slot_count' || details.openNoCount) {
    entry.available = true;
    entry.slots = null;
    entry.capacity = null;
    entry.estimatedBooked = null;
    entry.fillRate = null;
    const validation = rawModal ? (details.modalValidation || validateModalAssociation(sourceSession, rawModal)) : null;
    const trusted = validation?.match && validation?.confidence === 'exact_match';
    applyModalTrustFields(entry, details, { trusted });
    entry.detailStatus = status === 'checked_available_no_slot_count'
      ? 'checked_available_no_slot_count'
      : 'checked_open_no_slots_visible';
    entry.detailError = details.parseReason || null;
    entry.detailVerified = false;
    entry.detailConfidence = trusted ? 'exact_match' : 'weak_match';
    entry.priceText = null;
    entry.priceMin = null;
    entry.priceMax = null;
    return { ok: true, category: 'open_no_slots_visible' };
  }

  if (details.slots != null && hasParsedSlots) entry.slots = details.slots;
  else entry.slots = null;
  if (details.capacity != null && hasParsedCapacity) entry.capacity = details.capacity;
  else entry.capacity = null;

  attachSessionMetrics(entry, {
    slots: entry.slots,
    capacity: entry.capacity,
    price_text: hasParsedSlots && hasParsedCapacity ? details.price_text : null,
    price_min: hasParsedSlots && hasParsedCapacity ? details.price_min : null,
    price_max: hasParsedSlots && hasParsedCapacity ? details.price_max : null,
    currency: details.currency,
  }, level, { scrapeKind: 'detailed' });

  const validation = rawModal
    ? (details.modalValidation || validateModalAssociation(sourceSession, rawModal))
    : { match: false, confidence: 'weak_match' };
  const trusted = validation.match && validation.confidence === 'exact_match';
  applyModalTrustFields(entry, details, { trusted });

  if (!hasParsedSlots || !hasParsedCapacity || entry.slots == null || entry.capacity == null) {
    clearUnverifiedDetailMetrics(entry);
    entry.detailStatus = details.parseReason === 'text_open_or_available'
      ? 'checked_available_no_slot_count'
      : 'checked_open_no_slots_visible';
    entry.detailVerified = false;
    entry.detailConfidence = 'default_suppressed';
    return { ok: true, category: 'open_no_slots_visible' };
  }

  if (isDefaultLikeDetailValues(entry.slots, entry.capacity, entry.estimatedBooked)) {
    clearUnverifiedDetailMetrics(entry);
    entry.detailStatus = 'checked_open_no_slots_visible';
    entry.detailVerified = false;
    entry.detailConfidence = 'default_suppressed';
    return { ok: true, category: 'open_no_slots_visible' };
  }

  entry.detailVerified = trusted;
  entry.detailConfidence = trusted ? 'exact_match' : 'weak_match';

  if (!trusted) {
    clearUnverifiedDetailMetrics(entry);
    entry.detailStatus = 'failed_modal_mismatch';
    entry.detailError = entry.detailError || 'modal_not_verified_for_slots';
    return { ok: false, category: 'failed', status: 'failed_modal_mismatch' };
  }

  entry.detailStatus = 'checked_with_slots';
  entry.detailError = entry.detailWarning || null;
  ensureDetailStatusRecorded(entry, 'checked_with_slots');
  return { ok: true, category: 'with_slots' };
}

function preservePriorDetailFields(entry, prior) {
  preservePriorVerifiedDetailFields(entry, prior);
}

function applyDetailFailureToSession(entry, failure, { prior = null } = {}) {
  if (!entry) return { ok: false, category: 'failed' };
  const priorSnapshot = prior || { ...entry };
  const now = new Date().toISOString();
  const status = normalizeDetailStatus(failure?.detailStatus || failure?.failureType) || 'failed_parse';
  entry.lastDetailedCheckAt = now;
  entry.detailStatus = status;
  entry.detailError = failure?.detailError || status;
  entry.detailVerified = false;
  if (failure?.modalValidation) entry.modalValidation = failure.modalValidation;
  if (failure?.rawTileText) entry.detailRawTileText = truncateDetailText(failure.rawTileText);
  if (failure?.failedSelector) entry.detailFailedSelector = failure.failedSelector;
  if (failure?.parseReason) entry.detailParseReason = failure.parseReason;
  if (failure?.mismatches) entry.modalMismatches = failure.mismatches;
  applyModalTrustFields(entry, failure, { trusted: false });
  entry.detailParseOutput = buildParserOutputFromText(failure?.rawModalText || failure?.rawTileText || '');
  preservePriorVerifiedDetailFields(entry, priorSnapshot);
  if (!sessionDetailVerified(entry)) clearUnverifiedDetailMetrics(entry);
  ensureDetailStatusRecorded(entry, status);
  return { ok: false, category: 'failed', status: entry.detailStatus };
}

function incrementDetailFailureStats(stats, status) {
  const st = normalizeDetailStatus(status) || status;
  if (st === 'failed_modal_mismatch') stats.sessionsFailedModalMismatch = (stats.sessionsFailedModalMismatch || 0) + 1;
  else if (st === 'failed_modal_stale') stats.sessionsFailedModalStale = (stats.sessionsFailedModalStale || 0) + 1;
  else if (st === 'failed_tile_mismatch') stats.sessionsFailedTileMismatch = (stats.sessionsFailedTileMismatch || 0) + 1;
  else if (st === 'failed_cookie_overlay') stats.sessionsFailedCookieOverlay = (stats.sessionsFailedCookieOverlay || 0) + 1;
  else if (st === 'failed_parse') stats.sessionsFailedParse = (stats.sessionsFailedParse || 0) + 1;
  else if (st === 'failed_selector') stats.sessionsFailedSelector = (stats.sessionsFailedSelector || 0) + 1;
  else if (st === 'failed_modal_open') stats.sessionsFailedModalOpen = (stats.sessionsFailedModalOpen || 0) + 1;
  else if (st === 'failed_timeout') stats.sessionsTimedOut = (stats.sessionsTimedOut || 0) + 1;
  else stats.sessionsFailed = (stats.sessionsFailed || 0) + 1;
}

function attachCookieDiagnosticsToStats(stats) {
  Object.assign(stats, cookieDiagnosticsPayload());
  return stats;
}

function finalizeEnrichmentStats(stats) {
  const outcomeTotal = (stats.sessionsUpdatedWithSlots || 0)
    + (stats.sessionsMarkedPacked || 0)
    + (stats.sessionsCheckedOpenNoSlotsVisible || 0)
    + (stats.sessionsFailedCookieOverlay || 0)
    + (stats.sessionsFailedParse || 0)
    + (stats.sessionsFailedSelector || 0)
    + (stats.sessionsFailedModalOpen || 0)
    + (stats.sessionsTimedOut || 0)
    + (stats.sessionsFailed || 0)
    + (stats.sessionsUnchanged || 0);
  stats.outcomeTotal = outcomeTotal;
  stats.outcomeReconciles = stats.sessionsAttempted === 0 || outcomeTotal === stats.sessionsAttempted;
  attachCookieDiagnosticsToStats(stats);
  return stats;
}

function emptyEnrichmentStats({ skipped = false, skipReason = null, sessionsQueued = 0 } = {}) {
  return finalizeEnrichmentStats({
    skipped,
    skipReason,
    sessionsQueued,
    sessionsAttempted: 0,
    sessionsUpdatedWithSlots: 0,
    sessionsUpdatedWithCapacity: 0,
    sessionsUpdatedWithPrice: 0,
    sessionsMarkedPacked: 0,
    sessionsCheckedOpenNoSlotsVisible: 0,
    sessionsCheckedNoSlotsVisible: 0,
    sessionsFailed: 0,
    sessionsFailedParse: 0,
    sessionsFailedSelector: 0,
    sessionsFailedModalOpen: 0,
    sessionsFailedCookieOverlay: 0,
    sessionsTimedOut: 0,
    sessionsUnchanged: 0,
    unchangedReasons: [],
    errors: [],
  });
}

function sessionQualifiesForFailedFirstEnrich(s) {
  const st = effectiveDetailStatus(s);
  if (['failed_cookie_overlay', 'failed_parse', 'failed_selector', 'failed_modal_open', 'failed_timeout', 'failed_modal_mismatch', 'failed_modal_stale', 'failed_tile_mismatch'].includes(st)) return true;
  if (st === 'unknown' && (s.lastDetailedCheckAt || s.last_detailed_check_at)) return true;
  if (s.available !== false && !sessionHasDetailedData(s)) return true;
  return false;
}

function sortSessionsForFailedFirstEnrich(sessions) {
  const score = (s) => {
    const st = effectiveDetailStatus(s);
    if (st === 'failed_cookie_overlay') return 0;
    if (st === 'failed_selector') return 1;
    if (st === 'failed_parse') return 2;
    if (st === 'failed_timeout' || st === 'failed_modal_open') return 3;
    if (st === 'unknown' && (s.lastDetailedCheckAt || s.last_detailed_check_at)) return 4;
    if (!sessionHasDetailedData(s)) return 5;
    return 6;
  };
  return [...asSessionArray(sessions)].sort((a, b) => {
    const diff = score(a) - score(b);
    if (diff) return diff;
    return (a.ts || 0) - (b.ts || 0);
  });
}

function sortSessionsForDetailRetry(sessions) {
  const score = (s) => {
    const st = effectiveDetailStatus(s);
    if (isDetailFailureStatus(st)) return 0;
    if (s.available !== false && !sessionHasDetailedData(s)) return 1;
    if (st === 'pending' || st === 'unknown') return 2;
    if (st === 'checked_open_no_slots_visible') return 3;
    return 4;
  };
  return [...asSessionArray(sessions)].sort((a, b) => {
    const diff = score(a) - score(b);
    if (diff) return diff;
    return (a.ts || 0) - (b.ts || 0);
  });
}

function normalizeDetailStatus(status) {
  if (!status) return null;
  if (status === 'checked_packed_no_slots') return 'checked_packed';
  if (status === 'checked_no_slots_visible') return 'checked_open_no_slots_visible';
  return status;
}

function effectiveDetailStatus(s) {
  const raw = normalizeDetailStatus(s?.detailStatus || s?.detail_status);
  const reconciled = reconcileDetailStatusForSession(s);
  if (raw === 'checked_with_slots' && reconciled !== 'checked_with_slots') return reconciled;
  if (raw && raw !== 'unknown') return raw;
  const attempted = !!(s?.lastDetailedCheckAt || s?.last_detailed_check_at);
  if (!attempted) return 'unknown';
  if (s?.slots === 0 && s?.available === false && sessionDetailVerified(s)) return 'checked_packed';
  if (s?.slots != null && s?.capacity != null && sessionDetailVerified(s)) return 'checked_with_slots';
  const err = String(s?.detailError || s?.detail_error || '').toLowerCase();
  if (err.includes('modal_stale') || s?.staleModalDetected || s?.raw?.staleModalDetected) return 'failed_modal_stale';
  if (err.includes('tile_mismatch')) return 'failed_tile_mismatch';
  if (err.includes('modal_mismatch') || err.includes('modal_identity')) return 'failed_modal_mismatch';
  if (err.includes('cookie')) return 'failed_cookie_overlay';
  if (err.includes('tile not found') || err.includes('selector')) return 'failed_selector';
  if (err.includes('timeout')) return 'failed_timeout';
  if (err.includes('modal never')) return 'failed_modal_open';
  if (err.includes('target_date_not_visible')) return 'failed_selector';
  return 'failed_parse';
}

function isDetailUnknownStatus(status) {
  const st = normalizeDetailStatus(status);
  return !st || st === 'unknown';
}

function reasonForDetailStatus(s) {
  const st = effectiveDetailStatus(s);
  const err = s?.detailError || s?.detail_error;
  switch (st) {
    case 'unknown':
      return (s?.lastDetailedCheckAt || s?.last_detailed_check_at)
        ? 'detail_check_ran_but_status_not_recorded'
        : 'no_detail_check_yet';
    case 'checked_with_slots':
      return 'parsed_slots_available';
    case 'checked_packed':
      return 'parsed_sold_out_or_packed';
    case 'checked_open_no_slots_visible':
      return err || 'modal_open_session_open_no_slot_count';
    case 'checked_available_no_slot_count':
      return err || 'modal_open_available_no_slot_count';
    case 'failed_modal_mismatch':
      return err || 'modal_text_does_not_match_session_row';
    case 'failed_modal_stale':
      return err || 'modal_text_unchanged_after_tile_click';
    case 'failed_tile_mismatch':
      return err || 'clicked_tile_does_not_match_session_row';
    case 'failed_selector':
      return err || 'booking_tile_or_modal_selector_missing';
    case 'failed_cookie_overlay':
      return err || 'cookie_consent_overlay_blocked_modal';
    case 'failed_modal_open':
      return err || 'tile_clicked_modal_never_appeared';
    case 'failed_parse':
      return err || 'modal_text_present_but_unparsed';
    case 'failed_timeout':
      return err || 'playwright_timeout';
    case 'checking':
      return 'detail_check_in_progress';
    case 'pending':
      return 'detail_check_pending';
    default:
      return err || st || 'unknown';
  }
}

function buildParserOutputFromText(text) {
  const parsed = parseDetailAvailabilityFromText(text);
  const price = parsePriceFromText(text || '');
  let parsedAvailability = null;
  if (parsed?.packed) parsedAvailability = false;
  else if (parsed?.openNoCount) parsedAvailability = true;
  else if (parsed?.slots != null) parsedAvailability = true;
  return {
    parsed_availability: parsedAvailability,
    parsed_slots_available: parsed?.slots ?? null,
    parsed_capacity: parsed?.capacity ?? parseCapacityFromDetailText(text),
    parsed_price_text: price.price_text ?? null,
    parse_reason: parsed?.parseReason ?? null,
  };
}

function sessionDetailDiagnosticsFields(s) {
  const rawModal = s.detailRawText || s.raw?.detailRawText || null;
  const rawTile = s.detailRawTileText || s.raw?.tileText || s.tileText || null;
  const parseSource = rawModal || rawTile || '';
  const parserOutput = buildParserOutputFromText(parseSource);
  const st = effectiveDetailStatus(s);
  return {
    session_key: s.key,
    iso_date: s.isoDate || s.dateKey,
    start_time: s.time,
    session_type: s.level,
    wave_side: s.waveSide,
    detail_status: st,
    detail_error: s.detailError || s.detail_error,
    raw_tile_text: rawTile ? truncateDetailText(rawTile, 800) : null,
    raw_modal_or_detail_text: rawModal ? truncateDetailText(rawModal, 1500) : null,
    parsed_availability: parserOutput.parsed_availability ?? s.available,
    parsed_slots_available: parserOutput.parsed_slots_available ?? null,
    parsed_capacity: parserOutput.parsed_capacity ?? null,
    parsed_price_text: parserOutput.parsed_price_text ?? null,
    parse_reason: s.detailParseReason || s.raw?.detailParseReason || parserOutput.parse_reason,
    failed_selector: s.detailFailedSelector || s.raw?.detailFailedSelector || null,
    last_detailed_check_at: s.lastDetailedCheckAt || s.last_detailed_check_at,
    reason: reasonForDetailStatus(s),
  };
}

function attachSessionMetrics(entry, details, level, { scrapeKind = 'detailed' } = {}) {
  if (!entry) return;
  const slots = entry.slots ?? details?.slots ?? null;
  const capacityHint = details?.capacity ?? entry.capacity ?? null;
  const metrics = computeSessionMetrics(slots, capacityHint, level, {
    inferCapacityFromLevel: false,
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

function sanitizeNumericField(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function sanitizeRawForDb(raw) {
  if (!raw || typeof raw !== 'object') return {};
  try {
    const cleaned = { ...raw };
    delete cleaned._recheckReason;
    return JSON.parse(JSON.stringify(cleaned));
  } catch {
    return { key: raw.key, ts: raw.ts, wave: raw.wave };
  }
}

function sanitizeCurrentSessionRow(row) {
  return {
    ...row,
    slots_available: row.slots_available != null ? Math.trunc(Number(row.slots_available)) : null,
    capacity: row.capacity != null ? Math.trunc(Number(row.capacity)) : null,
    estimated_booked: row.estimated_booked != null ? Math.trunc(Number(row.estimated_booked)) : null,
    fill_rate: sanitizeNumericField(row.fill_rate),
    price_min: sanitizeNumericField(row.price_min),
    price_max: sanitizeNumericField(row.price_max),
    raw: sanitizeRawForDb(row.raw),
  };
}

function classifySessionsForUpsert(sessions) {
  const eligible = [];
  const skipped = [];
  const skipReasons = {};
  const bump = (reason) => { skipReasons[reason] = (skipReasons[reason] || 0) + 1; };

  for (const s of asSessionArray(sessions)) {
    if (!s?.key) {
      skipped.push({ session: s, reason: 'missing_session_key' });
      bump('missing_session_key');
      continue;
    }
    eligible.push(s);
  }
  return { eligible, skipped, skipReasons };
}

function classifySessionsForSnapshots(sessions) {
  const eligible = [];
  const skipped = [];
  const skipReasons = {};
  const bump = (reason) => { skipReasons[reason] = (skipReasons[reason] || 0) + 1; };

  for (const s of asSessionArray(sessions)) {
    if (!s?.key) {
      skipped.push({ session: s, reason: 'missing_session_key' });
      bump('missing_session_key');
      continue;
    }
    const isoDate = s.isoDate || s.dateKey || (s.ts
      ? new Intl.DateTimeFormat('en-CA', { timeZone: BOOKING_TZ }).format(new Date(Number(s.ts) * 1000))
      : null);
    if (!isoDate) {
      skipped.push({ key: s.key, session: s, reason: 'missing_iso_date' });
      bump('missing_iso_date');
      continue;
    }
    eligible.push({ ...s, isoDate, dateKey: s.dateKey || isoDate });
  }
  return { eligible, skipped, skipReasons };
}

async function saveAvailabilitySnapshotsToSupabase(scrapedSessions, sourceTier, { snapshotType = 'basic' } = {}) {
  const writeResult = {
    snapshotsInserted: 0,
    snapshotsEligible: 0,
    sessionsSkippedBeforeSnapshot: 0,
    skipReasons: {},
    error: null,
    snapshotInsertError: null,
    historySnapshotsEnabled: HISTORY_SNAPSHOTS_ENABLED,
    supabaseConfigured: !!supabase,
  };

  if (!supabase) {
    writeResult.error = 'supabase_not_configured';
    writeResult.snapshotInsertError = writeResult.error;
    return writeResult;
  }
  if (!HISTORY_SNAPSHOTS_ENABLED) {
    writeResult.error = 'history_snapshots_disabled';
    writeResult.snapshotInsertError = writeResult.error;
    return writeResult;
  }

  const { eligible, skipped, skipReasons } = classifySessionsForSnapshots(scrapedSessions);
  writeResult.snapshotsEligible = eligible.length;
  writeResult.sessionsSkippedBeforeSnapshot = skipped.length;
  writeResult.skipReasons = skipReasons;

  if (!eligible.length) {
    writeResult.error = skipped.length ? 'all_sessions_skipped_before_snapshot' : 'empty_batch';
    writeResult.snapshotInsertError = writeResult.error;
    return writeResult;
  }

  try {
    const scrapedAt = new Date().toISOString();
    const rows = eligible.map((s) => {
      const metrics = computeSessionMetrics(
        s.slots ?? null,
        s.capacity ?? null,
        s.level,
        { inferCapacityFromLevel: snapshotType === 'detailed' && s.slots != null },
      );
      const type = snapshotType === 'detailed' || sessionHasDetailedData(s) ? 'detailed' : 'basic';
      const hour = sessionHourFromSession(s);
      return sanitizeCurrentSessionRow({
        scraped_at: scrapedAt,
        park: PARK,
        session_key: s.key,
        iso_date: s.isoDate || s.dateKey,
        start_ts: s.ts,
        start_time: s.time,
        weekday: s.weekday || null,
        hour,
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
        raw: sanitizeRawForDb(s),
      });
    });

    for (let i = 0; i < rows.length; i += 500) {
      const chunk = rows.slice(i, i + 500);
      const { error } = await supabase.from('availability_snapshots').insert(chunk);
      if (error) {
        writeResult.snapshotInsertError = error.message;
        writeResult.error = error.message;
        writeResult.errorDetails = {
          code: error.code,
          hint: error.hint,
          details: error.details,
          chunkStart: i,
          chunkSize: chunk.length,
        };
        console.error('  Supabase availability snapshots failed:', error.message, error.details || '');
        throw error;
      }
    }

    lastHistorySnapshotSavedAt = scrapedAt;
    writeResult.snapshotsInserted = rows.length;
    console.log(`  Supabase: saved ${rows.length} availability snapshot row(s)`);
    return writeResult;
  } catch (e) {
    writeResult.error = writeResult.error || e.message;
    writeResult.snapshotInsertError = writeResult.snapshotInsertError || e.message;
    console.error('  Supabase availability snapshots failed:', e.message);
    return writeResult;
  }
}

async function persistTierScrapeResults(sessions, tier, { slotCountsAttempted = false, slotCountsError = null } = {}) {
  const merged = asSessionArray(sessions);
  const diagnostics = {
    sessionsFound: merged.length,
    sessionsEligibleForUpsert: 0,
    sessionsSkippedBeforeUpsert: 0,
    skipReasons: {},
    rowsUpserted: 0,
    upsertError: null,
    detailUpsertRows: 0,
    detailUpsertError: null,
    snapshotsEligible: 0,
    snapshotsInserted: 0,
    snapshotInsertError: null,
    sampleSessionBeforeUpsert: null,
    sampleSkippedSessions: [],
  };

  const basicWrite = await upsertCurrentSessionsToSupabase(merged, tier, { scrapeKind: 'basic' });
  diagnostics.sessionsEligibleForUpsert = basicWrite.sessionsEligibleForUpsert;
  diagnostics.sessionsSkippedBeforeUpsert = basicWrite.sessionsSkippedBeforeUpsert;
  diagnostics.skipReasons = { ...basicWrite.skipReasons };
  diagnostics.rowsUpserted = basicWrite.rowsUpserted;
  diagnostics.upsertError = basicWrite.upsertError || basicWrite.error || null;
  diagnostics.sampleSessionBeforeUpsert = basicWrite.sampleSessionBeforeUpsert;
  diagnostics.sampleSkippedSessions = basicWrite.sampleSkippedSessions;

  if (slotCountsAttempted && !slotCountsError && basicWrite.rowsUpserted > 0) {
    const detailWrite = await upsertCurrentSessionsToSupabase(merged, tier, { scrapeKind: 'detailed' });
    diagnostics.detailUpsertRows = detailWrite.rowsUpserted;
    diagnostics.detailUpsertError = detailWrite.upsertError || detailWrite.error || null;
    if (detailWrite.rowsUpserted > diagnostics.rowsUpserted) {
      diagnostics.rowsUpserted = detailWrite.rowsUpserted;
    }
  }

  const snapshotType = slotCountsAttempted && !slotCountsError ? 'detailed' : 'basic';
  const snapshotWrite = await saveAvailabilitySnapshotsToSupabase(merged, tier, { snapshotType });
  diagnostics.snapshotsEligible = snapshotWrite.snapshotsEligible;
  diagnostics.snapshotsInserted = snapshotWrite.snapshotsInserted;
  diagnostics.snapshotInsertError = snapshotWrite.snapshotInsertError || snapshotWrite.error || null;
  if (snapshotWrite.skipReasons && Object.keys(snapshotWrite.skipReasons).length) {
    diagnostics.skipReasons = {
      ...diagnostics.skipReasons,
      ...Object.fromEntries(Object.entries(snapshotWrite.skipReasons).map(([k, v]) => [`snapshot_${k}`, v])),
    };
  }

  return diagnostics;
}

// ── Detail enrichment (future dates: slots/capacity/price) ─────────────────────

function installBookingNetworkCapture(page) {
  const captured = [];
  page.on('response', async (response) => {
    try {
      const url = response.url();
      if (!/atlanticparksurf|wave7|booking|activity|agenda|session|slot|calendar|api/i.test(url)) return;
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
    ?? node.remainingSlots ?? node.remaining ?? node.qty ?? node.quantity
    ?? node.placesAvailable ?? node.places_available ?? node.spotsRemaining
    ?? node.spots_remaining ?? node.availablePlaces ?? null;
  const capacity = node.capacity ?? node.maxCapacity ?? node.maxQty ?? node.max
    ?? node.totalCapacity ?? node.total_capacity ?? null;

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

async function getSessionDetailsWithFallback(page, session, networkCapture) {
  const ctx = sessionLookupContext(session);
  const label = ctx.key || `${ctx.ts}_${ctx.wave}`;
  const fromNetwork = networkCapture ? extractDetailsFromNetworkCapture(networkCapture, ctx.ts) : null;
  if (fromNetwork && (fromNetwork.slots != null || fromNetwork.capacity != null || fromNetwork.price_text)) {
    console.log(`  [details ${label}] from network response (requires modal verification for verified status)`);
    fromNetwork.detailStatus = fromNetwork.slots === 0
      ? 'checked_packed'
      : 'checked_open_no_slots_visible';
    fromNetwork.parseReason = 'network_json_unverified';
    fromNetwork.verified = false;
    return fromNetwork;
  }
  return getSessionModalDetails(page, session);
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
    .filter(s => s?.key && sessionQualifiesForDetailEnrichment(s))
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

  let candidates = allStoredSessions().filter(s => sessionQualifiesForDetailEnrichment(s));
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
    return emptyEnrichmentStats({
      skipped: true,
      skipReason: scrapeInProgress ? 'scrape_in_progress' : 'detail_enrichment_busy',
    });
  }

  resetCookieDismissDiagnostics();
  resetDetailModalLifecycleState();
  const runStarted = Date.now();
  const stats = {
    isoDate: isoDate || null,
    skipped: false,
    skipReason: null,
    sessionsQueued: explicitSessions?.length || 0,
    sessionsAttempted: 0,
    sessionsUpdatedWithSlots: 0,
    sessionsUpdatedWithCapacity: 0,
    sessionsUpdatedWithPrice: 0,
    sessionsMarkedPacked: 0,
    sessionsCheckedOpenNoSlotsVisible: 0,
    sessionsCheckedNoSlotsVisible: 0,
    sessionsFailed: 0,
    sessionsFailedParse: 0,
    sessionsFailedSelector: 0,
    sessionsFailedModalOpen: 0,
    sessionsFailedCookieOverlay: 0,
    sessionsTimedOut: 0,
    sessionsUnchanged: 0,
    unchangedReasons: [],
    errors: [],
    detailRowsVerified: 0,
    detailRowsSuppressed: 0,
  };

  let keepBrowser = false;
  try {
    const toEnrich = explicitSessions?.length
      ? sortSessionsForDetailRetry(asSessionArray(explicitSessions)).slice(0, DETAIL_ENRICH_MAX_PER_RUN)
      : await pickSessionsForDetailEnrichment({ priority, isoDate });

    stats.sessionsQueued = toEnrich.length;

    if (!toEnrich.length) {
      stats.skipReason = 'no_sessions_to_enrich';
      return finalizeEnrichmentStats(stats);
    }

    console.log(`\n[${new Date().toLocaleTimeString()}] Detail enrichment (${reason}): ${toEnrich.length} session(s)`);

    const { page } = await acquireEnrichmentBrowser();
    const networkCapture = enrichmentNetworkCapture;
    await openBookingPage(page);

    const dateGroups = groupSessionsByIsoDate(toEnrich);
    let lastNavDate = null;
    let navDiag = null;
    stats.navigationDiagnostics = [];

    for (const [targetDate, sessionsOnDate] of dateGroups) {
      if (lastNavDate !== targetDate) {
        navDiag = await navigateCalendarToShowDate(page, targetDate);
        lastNavDate = targetDate;
        stats.navigationDiagnostics.push(navDiag);
        console.log(`  calendar nav for ${targetDate}: visible=${navDiag.targetDateVisible} clicks=${navDiag.clickedNextWeekCount} range=${navDiag.visibleWeekStart || '?'}..${navDiag.visibleWeekEnd || '?'}`);
      }

      if (!navDiag?.targetDateVisible) {
        for (const s of sessionsOnDate) {
          stats.sessionsAttempted++;
          await markQueueItemStatus(s.key, 'running', { incrementAttempt: true });
          const entry = { ...sessionsByKey.get(s.key), ...s };
          const prior = { ...entry };
          applyDetailFailureToSession(entry, {
            detailStatus: 'failed_selector',
            detailError: navDiag?.navigationError || 'target_date_not_visible',
            navigationDiagnostics: navDiag,
          }, { prior });
          sessionsByKey.set(s.key, entry);
          stats.detailRowsSuppressed++;
          incrementDetailFailureStats(stats, entry.detailStatus);
          await upsertCurrentSessionsToSupabase([entry], 0, { scrapeKind: 'detailed' });
          await markQueueItemStatus(s.key, 'pending', { error: entry.detailError });
        }
        continue;
      }

      for (const s of sessionsOnDate) {
        stats.sessionsAttempted++;
        await markQueueItemStatus(s.key, 'running', { incrementAttempt: true });

        const entry = { ...sessionsByKey.get(s.key), ...s };
        const prior = { ...entry };
        const priorStatus = effectiveDetailStatus(prior);
        const priorSlots = prior.slots;
        entry.detailStatus = 'checking';
        sessionsByKey.set(s.key, entry);
        sessionsNeedingDetailAfterBasic.delete(s.key);

        try {
          await dismissCookieBanner(page);
          const details = await getSessionDetailsWithFallback(page, s, networkCapture);
          if (!details || isDetailFailureStatus(normalizeDetailStatus(details.detailStatus || details.failureType))) {
            const err = details?.detailError || details?.detailStatus || 'no_details';
            const failStatus = normalizeDetailStatus(details?.detailStatus || details?.failureType) || 'failed_parse';
            stats.errors.push({ session_key: s.key, error: err, detail_status: failStatus });
            applyDetailFailureToSession(entry, details || {
              detailStatus: 'failed_parse',
              detailError: 'no_details',
            }, { prior });
            sessionsByKey.set(s.key, entry);
            stats.detailRowsSuppressed++;
            incrementDetailFailureStats(stats, entry.detailStatus);
            const newStatus = effectiveDetailStatus(entry);
            if (newStatus === priorStatus && priorSlots === entry.slots) {
              stats.sessionsUnchanged++;
              stats.unchangedReasons.push({
                session_key: s.key,
                reason: `still_${newStatus}`,
                prior_status: priorStatus,
                detail_error: entry.detailError || err,
              });
            }
            await upsertCurrentSessionsToSupabase([entry], 0, { scrapeKind: 'detailed' });
            await markQueueItemStatus(s.key, 'pending', { error: err });
            enrichmentMetrics.recentErrors.push({ at: new Date().toISOString(), session_key: s.key, error: err });
            continue;
          }

          const hadSlots = prior.slots != null;
          const hadCapacity = prior.capacity != null;
          const hadPrice = prior.priceText != null || prior.priceMin != null;

          const result = applyDetailPayloadToSession(entry, details, s.level, { prior, session: s });
          sessionsByKey.set(s.key, entry);

          if (sessionDetailVerified(entry)) stats.detailRowsVerified++;
          else if (isDetailFailureStatus(effectiveDetailStatus(entry)) || !sessionDetailVerified(entry)) stats.detailRowsSuppressed++;

          if (!hadSlots && entry.slots != null) stats.sessionsUpdatedWithSlots++;
          if (!hadCapacity && entry.capacity != null) stats.sessionsUpdatedWithCapacity++;
          if (!hadPrice && (entry.priceText || entry.priceMin != null)) stats.sessionsUpdatedWithPrice++;
          if (result.category === 'packed') stats.sessionsMarkedPacked++;
          if (result.category === 'open_no_slots_visible') {
            stats.sessionsCheckedOpenNoSlotsVisible++;
            stats.sessionsCheckedNoSlotsVisible++;
          }
          if (result.category === 'failed') incrementDetailFailureStats(stats, entry.detailStatus);

          const newStatus = effectiveDetailStatus(entry);
          const changed = newStatus !== priorStatus
            || priorSlots !== entry.slots
            || prior.capacity !== entry.capacity;
          if (!changed && result.category !== 'with_slots' && result.category !== 'packed' && result.category !== 'open_no_slots_visible') {
            stats.sessionsUnchanged++;
            stats.unchangedReasons.push({
              session_key: s.key,
              reason: `still_${newStatus}`,
              prior_status: priorStatus,
            });
          }

          await upsertCurrentSessionsToSupabase([entry], 0, { scrapeKind: 'detailed' });
          await saveAvailabilitySnapshotsToSupabase([entry], 0, { snapshotType: 'detailed' });

          await markQueueItemStatus(s.key, 'done');
          await page.waitForTimeout(ENRICHMENT_DELAY_MS);
        } catch (e) {
          stats.errors.push({ session_key: s.key, error: e.message });
          applyDetailFailureToSession(entry, {
            detailStatus: /timeout|timed out/i.test(e.message) ? 'failed_timeout' : 'failed_parse',
            detailError: e.message,
          }, { prior });
          sessionsByKey.set(s.key, entry);
          incrementDetailFailureStats(stats, entry.detailStatus);
          await upsertCurrentSessionsToSupabase([entry], 0, { scrapeKind: 'detailed' });
          await markQueueItemStatus(s.key, 'pending', { error: e.message });
          enrichmentMetrics.recentErrors.push({ at: new Date().toISOString(), session_key: s.key, error: e.message });
        }
      }
    }

    rebuildSessionsArray();
    keepBrowser = true;
    enrichmentBrowserLastUsed = Date.now();
    scheduleEnrichmentBrowserIdleClose();

    lastDetailEnrichmentAt = new Date().toISOString();
    lastDetailEnrichmentError = stats.errors.length ? `${stats.errors.length} session error(s)` : null;
    const durationMs = Date.now() - runStarted;
    enrichmentMetrics.lastRunAt = lastDetailEnrichmentAt;
    enrichmentMetrics.lastDurationMs = durationMs;
    enrichmentMetrics.lastRunStats = finalizeEnrichmentStats({ ...stats });
    enrichmentMetrics.runsCompleted += 1;
    enrichmentMetrics.averageDurationMs = enrichmentMetrics.averageDurationMs == null
      ? durationMs
      : Math.round((enrichmentMetrics.averageDurationMs * (enrichmentMetrics.runsCompleted - 1) + durationMs) / enrichmentMetrics.runsCompleted);
    if (enrichmentMetrics.recentErrors.length > 50) {
      enrichmentMetrics.recentErrors = enrichmentMetrics.recentErrors.slice(-50);
    }

    console.log(`  detail enrichment done (${durationMs}ms): ${stats.sessionsUpdatedWithSlots} slots, ${stats.sessionsFailedCookieOverlay} cookie, ${stats.sessionsFailedParse} parse`);
    return finalizeEnrichmentStats(stats);
  } catch (e) {
    lastDetailEnrichmentError = e.message;
    stats.errors.push({ error: e.message });
    enrichmentMetrics.recentErrors.push({ at: new Date().toISOString(), error: e.message });
    console.error('  detail enrichment failed:', e.message);
    await releaseEnrichmentBrowserPool();
    return finalizeEnrichmentStats(stats);
  } finally {
    releaseDetailEnrichmentLock();
    if (!keepBrowser) await releaseEnrichmentBrowserPool();
    await refreshEnrichmentQueueCounts();
  }
}

async function runDetailEnrichmentByPriority(priority) {
  return runDetailEnrichment({ priority, reason: `priority_${priority}` });
}

function tryAcquireScrapeLock(context = 'scrape', tier = null) {
  if (scrapeInProgress) {
    console.log(`  ${context} skipped — scrape already running (tier ${currentScrapeTier} since ${currentScrapeStartedAt})`);
    return false;
  }
  scrapeInProgress = true;
  currentScrapeTier = tier;
  currentScrapeStartedAt = new Date().toISOString();
  return true;
}

function releaseScrapeLock() {
  scrapeInProgress = false;
  currentScrapeTier = null;
  currentScrapeStartedAt = null;
}

function tierTargetDates(tier) {
  if (tier === 1) {
    const today = getParkTodayIso();
    return [today, addDaysToParkIso(today, 1)];
  }
  return expectedDatesForTier(tier);
}

function recordTierRunState(tier, report, { reason = 'scheduled' } = {}) {
  const now = new Date().toISOString();
  const p = `tier${tier}`;
  collectorState[`${p}LastAttemptAt`] = now;
  if (tier === 1) {
    collectorState.tier1TargetDates = report.targetDates || tierTargetDates(tier);
  }
  collectorState[`${p}LastResult`] = {
    at: now,
    reason,
    tier,
    started: report.started,
    completed: report.completed,
    skipped: report.skipped,
    skipReason: report.skipReason,
    targetDates: report.targetDates || tierTargetDates(tier),
    sessionsFound: report.sessionsFound ?? 0,
    rowsUpserted: report.rowsUpserted ?? 0,
    snapshotsInserted: report.snapshotsInserted ?? 0,
    sessionsEligibleForUpsert: report.sessionsEligibleForUpsert ?? 0,
    sessionsSkippedBeforeUpsert: report.sessionsSkippedBeforeUpsert ?? 0,
    skipReasons: report.skipReasons ?? {},
    upsertError: report.upsertError ?? null,
    snapshotsEligible: report.snapshotsEligible ?? 0,
    snapshotInsertError: report.snapshotInsertError ?? null,
    sampleSessionBeforeUpsert: report.sampleSessionBeforeUpsert ?? null,
    sampleSkippedSessions: report.sampleSkippedSessions ?? [],
    durationMs: report.durationMs ?? 0,
    error: report.error || report.upsertError || report.snapshotInsertError || report.errors?.[0]?.error || null,
    blockingScrapeTier: report.blockingScrapeTier ?? null,
    blockingScrapeStartedAt: report.blockingScrapeStartedAt ?? null,
    slotCountsError: report.slotCountsError ?? null,
  };
  if (report.skipped) {
    collectorState[`${p}LastSkippedAt`] = now;
    collectorState[`${p}LastSkipReason`] = report.skipReason;
  }
  if (report.completed) {
    collectorState[`${p}LastCompletedAt`] = now;
    collectorState[`${p}LastError`] = report.upsertError || report.snapshotInsertError || report.slotCountsError || null;
  }
  if (report.error || report.errors?.length) {
    collectorState[`${p}LastError`] = report.error || report.errors[0]?.error;
  }
}

function reasonTodayHasZeroSessions(isoDate) {
  const today = getParkTodayIso();
  if (isoDate !== today) return null;
  const memCount = sessionsForDate(isoDate).length;
  if (memCount > 0) return null;

  if (!lastTierRun[1]) {
    if (collectorState.tier1LastSkippedAt) {
      return `tier1_never_completed — last skip: ${collectorState.tier1LastSkipReason || 'unknown'} (${collectorState.tier1LastSkippedAt})`;
    }
    if (collectorState.tier1LastAttemptAt) {
      return `tier1_never_completed — last attempt ${collectorState.tier1LastAttemptAt} did not finish`;
    }
    return 'tier1_never_completed — POST /api/admin/run-tier1?wait=true';
  }
  if (lastTierError[1]) return `tier1_last_error: ${lastTierError[1]}`;
  if (datesCheckedEmpty.has(isoDate)) return 'tier1_checked_empty — booking site had no remaining sessions today';
  if (collectorState.tier1LastResult?.sessionsFound === 0) {
    return 'tier1_completed_but_found_zero_sessions_in_date_range';
  }
  if (!persistedDatesChecked.has(isoDate)) {
    return 'today_not_marked_checked_by_tier1_yet';
  }
  return 'no_saved_rows_for_today — tier1 may have run before sessions were added or rows were pruned';
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
  return watchItems.filter(w => w.active !== false && !isWatchItemPast(w));
}

function watchedSessionKeys() {
  return new Set(activeWatchItems().map(w => w.session_key));
}

async function expirePastWatchlistItems() {
  const expired = watchItems.filter(w => w.active !== false && isWatchItemPast(w));
  if (!expired.length) return 0;
  for (const w of expired) {
    try {
      await deactivateWatchItem(w.id, w.user_key);
    } catch (e) {
      console.warn(`  expire watch ${w.session_key}:`, e.message);
    }
  }
  console.log(`  expired ${expired.length} past watchlist item(s)`);
  return expired.length;
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
  if (isWatchItemPast(watch) || isSessionPast(session)) return;
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
  await expirePastWatchlistItems();
  const updatedSet = new Set(updatedKeys);
  const now = new Date().toISOString();

  for (const watch of activeWatchItems()) {
    const session = sessionsByKey.get(watch.session_key);
    if (!session) continue;
    if (isWatchItemPast(watch) || isSessionPast(session)) continue;

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
  const n = await reloadWatchlistFromSupabase();
  await expirePastWatchlistItems();
  return n;
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
    iso_date: iso_date || dateKey || live?.isoDate || live?.dateKey || (start_ts ?? ts ?? live?.ts ? dateKeyInBookingTz(new Date((start_ts ?? ts ?? live?.ts) * 1000)) : null),
    start_ts: start_ts ?? ts ?? live?.ts ?? null,
    wave_side: live?.waveSide || wave_side || waveSide || null,
    session_type: session_type || level || live?.level || null,
    start_time: time || live?.time || null,
    time: time || live?.time || null,
    date: date || live?.date || null,
    day_label: null,
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

const COOKIE_CONSENT_INIT_SCRIPT = `
(() => {
  try {
    const storageKeys = [
      'CookieConsent', 'cookieconsent_status', 'cookies_accepted', 'cookie_consent',
      'gdpr-consent', 'euconsent-v2', 'OptanonConsent', 'cookie-agreed', 'cookie_agreed',
      'allowCookies', 'acceptCookies', 'cookie_notice_accepted', 'cb-enabled', 'cookieControl',
      'cookieconsent', 'cookies-policy', 'cookie_policy', 'consent_status',
    ];
    const storageValues = ['true', 'allow', '1', 'yes', 'accepted', 'dismiss', 'all'];
    for (const key of storageKeys) {
      for (const val of storageValues) {
        try { localStorage.setItem(key, val); } catch {}
        try { sessionStorage.setItem(key, val); } catch {}
      }
    }
    const cookiePairs = [
      'CookieConsent=true', 'cookieconsent_status=allow', 'cookies_accepted=1',
      'cookie_consent=accepted', 'allowCookies=1', 'acceptCookies=true',
    ];
    for (const pair of cookiePairs) {
      try { document.cookie = pair + '; path=/; max-age=31536000; SameSite=Lax'; } catch {}
    }
  } catch {}
})();
`;

let cookieDismissDiagnostics = {
  cookieDismissAttempted: 0,
  cookieDismissSucceeded: 0,
  cookieBannerStillVisible: false,
  cookieClickMethod: null,
  modalTextAfterCookieDismissSample: null,
  lastAttempt: null,
  attempts: [],
};

function emptyCookieAttemptDiagnostics() {
  return {
    bannerElementTag: null,
    bannerElementClass: null,
    bannerElementId: null,
    buttonCandidates: [],
    clickedCandidate: null,
    beforeText: null,
    afterText: null,
    didTextDisappear: false,
    didElementDetach: false,
    activeElementAfterClick: null,
    iframeCount: 0,
    shadowHostCandidates: [],
    frameUsed: 'main',
    method: null,
    failureReason: null,
  };
}

function resetCookieDismissDiagnostics() {
  cookieDismissDiagnostics = {
    cookieDismissAttempted: 0,
    cookieDismissSucceeded: 0,
    cookieBannerStillVisible: false,
    cookieClickMethod: null,
    modalTextAfterCookieDismissSample: null,
    lastAttempt: null,
    attempts: [],
  };
}

function cookieDiagnosticsPayload() {
  const snap = cookieDiagnosticsSnapshot();
  return {
    cookieDismissAttempted: snap.cookieDismissAttempted,
    cookieDismissSucceeded: snap.cookieDismissSucceeded,
    cookieBannerStillVisible: snap.cookieBannerStillVisible,
    cookieClickMethod: snap.cookieClickMethod,
    modalTextAfterCookieDismissSample: snap.modalTextAfterCookieDismissSample,
    cookieDismissLastAttempt: snap.lastAttempt,
    cookieDismissAttempts: snap.attempts,
  };
}

function cookieDiagnosticsSnapshot() {
  return { ...cookieDismissDiagnostics, attempts: [...(cookieDismissDiagnostics.attempts || [])] };
}

function recordCookieAttempt(attempt) {
  cookieDismissDiagnostics.lastAttempt = attempt;
  cookieDismissDiagnostics.attempts = [...(cookieDismissDiagnostics.attempts || []), attempt].slice(-5);
}

async function setupBookingBrowserContext(context) {
  await context.addInitScript(COOKIE_CONSENT_INIT_SCRIPT);
}

async function isCookieBannerVisible(page) {
  return page.evaluate(() => {
    const body = (document.body?.innerText || '').replace(/\s+/g, ' ').toLowerCase();
    const hasCookieCopy = body.includes('this website uses cookies')
      || (body.includes('cookie') && (body.includes('allow cookies') || body.includes('refuse cookies')));
    const selectors = [
      '[class*="cookie" i]', '[id*="cookie" i]', '[class*="consent" i]', '[id*="consent" i]',
      '[class*="gdpr" i]', '[id*="gdpr" i]', '[class*="cc-" i]', '.cc-window', '#cookie-law-info-bar',
      '[class*="CookieConsent" i]', '#CybotCookiebotDialog', '#onetrust-banner-sdk',
    ];
    for (const sel of selectors) {
      for (const el of document.querySelectorAll(sel)) {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        const visible = style.display !== 'none'
          && style.visibility !== 'hidden'
          && parseFloat(style.opacity || '1') > 0.05
          && rect.width > 40
          && rect.height > 20;
        if (visible) return true;
      }
    }
    return hasCookieCopy && (body.includes('allow cookies') || body.includes('refuse cookies'));
  }).catch(() => false);
}

async function waitForCookieBannerGone(page, timeout = 6000) {
  await page.waitForFunction(() => {
    const body = (document.body?.innerText || '').replace(/\s+/g, ' ').toLowerCase();
    const hasBannerText = body.includes('this website uses cookies')
      || (body.includes('cookie') && body.includes('allow cookies') && body.includes('refuse cookies'));
    if (!hasBannerText) return true;
    const selectors = [
      '[class*="cookie" i]', '[id*="cookie" i]', '[class*="consent" i]', '[id*="consent" i]',
      '[class*="gdpr" i]', '.cc-window', '#cookie-law-info-bar', '#CybotCookiebotDialog', '#onetrust-banner-sdk',
    ];
    for (const sel of selectors) {
      for (const el of document.querySelectorAll(sel)) {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        const visible = style.display !== 'none'
          && style.visibility !== 'hidden'
          && parseFloat(style.opacity || '1') > 0.05
          && rect.width > 40
          && rect.height > 20;
        if (visible) return false;
      }
    }
    return !hasBannerText;
  }, { timeout }).catch(() => {});
}

async function collectCookieBannerContext(frame, frameLabel = 'main') {
  return frame.evaluate((label) => {
    const norm = (t) => String(t || '').replace(/\s+/g, ' ').trim();
    const low = (t) => norm(t).toLowerCase();
    const bodyText = norm(document.body?.innerText || '').slice(0, 600);

    function directText(el) {
      let t = '';
      for (const node of el.childNodes) {
        if (node.nodeType === Node.TEXT_NODE) t += node.textContent;
      }
      return norm(t);
    }

    function elementMeta(el) {
      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      const text = norm(el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || '');
      const ownText = directText(el) || text;
      return {
        tag: el.tagName.toLowerCase(),
        className: typeof el.className === 'string' ? el.className : null,
        id: el.id || null,
        text: text.slice(0, 140),
        ownText: ownText.slice(0, 80),
        visible: style.display !== 'none'
          && style.visibility !== 'hidden'
          && parseFloat(style.opacity || '1') > 0.05
          && rect.width > 0
          && rect.height > 0,
        enabled: !el.disabled,
        boundingBox: { x: Math.round(rect.x), y: Math.round(rect.y), width: Math.round(rect.width), height: Math.round(rect.height) },
        ariaLabel: el.getAttribute('aria-label'),
        role: el.getAttribute('role'),
        cursor: style.cursor,
        href: el.getAttribute('href'),
      };
    }

    function isClickableTag(el) {
      const tag = el.tagName.toLowerCase();
      if (['button', 'a', 'input'].includes(tag)) return true;
      if (el.getAttribute('role') === 'button') return true;
      if (el.hasAttribute('onclick')) return true;
      return window.getComputedStyle(el).cursor === 'pointer';
    }

    function isAllowLabel(text) {
      const t = low(text);
      return /\ballow cookies\b/.test(t) || /\baccept all\b/.test(t)
        || /\baccept cookies\b/.test(t) || t === 'accept' || t === 'agree' || t === 'ok';
    }

    function isRefuseLabel(text) {
      const t = low(text);
      return /\brefuse cookies\b/.test(t) || /\breject all\b/.test(t) || t === 'decline';
    }

    function findBannerElement(root) {
      const selectors = [
        '[class*="cookie" i]', '[id*="cookie" i]', '[class*="consent" i]', '[id*="consent" i]',
        '[class*="gdpr" i]', '.cc-window', '#cookie-law-info-bar', '#CybotCookiebotDialog',
        '#onetrust-banner-sdk', '[class*="CookieConsent" i]',
      ];
      for (const sel of selectors) {
        for (const el of root.querySelectorAll(sel)) {
          const meta = elementMeta(el);
          const text = low(meta.text);
          if (!meta.visible || meta.boundingBox.width < 40) continue;
          if (text.includes('cookie') || text.includes('consent') || text.includes('this website uses')) {
            return el;
          }
        }
      }
      for (const el of root.querySelectorAll('div, section, aside, footer, dialog, [role="dialog"], [role="alertdialog"]')) {
        const meta = elementMeta(el);
        const text = low(meta.text);
        if (!meta.visible || meta.boundingBox.width < 120) continue;
        if (text.includes('this website uses cookies') || (text.includes('cookie') && text.includes('allow cookies'))) {
          return el;
        }
      }
      return null;
    }

    function collectCandidates(root, bannerEl) {
      const CLICKABLE_SEL = 'button, a[href], a, [role="button"], input[type="button"], input[type="submit"], input[type="reset"]';
      const searchRoots = bannerEl ? [bannerEl] : [root];
      const out = [];
      const seen = new Set();

      for (const searchRoot of searchRoots) {
        for (const el of searchRoot.querySelectorAll(CLICKABLE_SEL)) {
          if (!isClickableTag(el) || seen.has(el)) continue;
          seen.add(el);
          const meta = elementMeta(el);
          if (!meta.visible || !meta.enabled) continue;
          const labels = [meta.text, meta.ownText, meta.ariaLabel].filter(Boolean);
          let preference = null;
          for (const lbl of labels) {
            if (isAllowLabel(lbl)) { preference = 'allow'; break; }
            if (isRefuseLabel(lbl)) { preference = 'refuse'; break; }
          }
          if (!preference) continue;
          out.push({ ...meta, preference, score: preference === 'allow'
            ? (/\ballow cookies\b/i.test(meta.ownText || meta.text) ? 100 : 70)
            : 40 });
        }
      }

      if (bannerEl) {
        for (const el of bannerEl.querySelectorAll('*')) {
          if (!isClickableTag(el) || seen.has(el)) continue;
          seen.add(el);
          const meta = elementMeta(el);
          if (!meta.visible) continue;
          const own = meta.ownText || meta.text;
          if (isAllowLabel(own) && !isRefuseLabel(own)) {
            out.push({ ...meta, preference: 'allow', score: /\ballow cookies\b/i.test(own) ? 95 : 75 });
          } else if (isRefuseLabel(own)) {
            out.push({ ...meta, preference: 'refuse', score: 35 });
          }
        }
      }

      out.sort((a, b) => b.score - a.score);
      return out.slice(0, 20);
    }

    function collectShadowHosts(root) {
      const hosts = [];
      root.querySelectorAll('*').forEach((el) => {
        if (el.shadowRoot) {
          hosts.push({
            tag: el.tagName.toLowerCase(),
            id: el.id || null,
            className: typeof el.className === 'string' ? el.className : null,
          });
        }
      });
      return hosts.slice(0, 12);
    }

    function walkShadowRoots(root, fn) {
      fn(root);
      root.querySelectorAll('*').forEach((el) => {
        if (el.shadowRoot) walkShadowRoots(el.shadowRoot, fn);
      });
    }

    let bannerEl = null;
    let buttonCandidates = [];
    walkShadowRoots(document, (root) => {
      if (bannerEl) return;
      bannerEl = findBannerElement(root);
      if (bannerEl) buttonCandidates = collectCandidates(root, bannerEl);
    });
    if (!buttonCandidates.length) {
      buttonCandidates = collectCandidates(document, bannerEl);
    }

    return {
      frameUsed: label,
      beforeText: bodyText,
      bannerElementTag: bannerEl?.tagName?.toLowerCase() || null,
      bannerElementClass: bannerEl && typeof bannerEl.className === 'string' ? bannerEl.className : null,
      bannerElementId: bannerEl?.id || null,
      buttonCandidates,
      shadowHostCandidates: collectShadowHosts(document),
      iframeCount: window.frames?.length ?? 0,
    };
  }, frameLabel).catch(() => ({
    frameUsed: frameLabel,
    beforeText: null,
    bannerElementTag: null,
    bannerElementClass: null,
    bannerElementId: null,
    buttonCandidates: [],
    shadowHostCandidates: [],
    iframeCount: 0,
  }));
}

async function clickCookieCandidateWithPlaywright(frame, candidate, methodPrefix) {
  const page = frame.page();
  const selParts = [];
  if (candidate.id) selParts.push(`#${CSS.escape(candidate.id)}`);
  else if (candidate.className) {
    const cls = candidate.className.split(/\s+/).filter(Boolean)[0];
    if (cls) selParts.push(`${candidate.tag}.${cls.replace(/([^\w-])/g, '\\$1')}`);
  }
  const locators = [
    candidate.id ? frame.locator(`#${candidate.id}`) : null,
    candidate.text ? frame.getByRole('button', { name: new RegExp(candidate.ownText || candidate.text, 'i') }).first() : null,
    candidate.text ? frame.locator(`${candidate.tag || 'button'}`).filter({ hasText: new RegExp('\\ballow cookies\\b', 'i') }).first() : null,
    selParts.length ? frame.locator(selParts[0]).first() : null,
  ].filter(Boolean);

  for (const locator of locators) {
    try {
      if (!await locator.count()) continue;
      if (!await locator.first().isVisible({ timeout: 400 }).catch(() => false)) continue;
      await locator.first().scrollIntoViewIfNeeded({ timeout: 2000 }).catch(() => {});
      await locator.first().hover({ timeout: 1500 }).catch(() => {});
      try {
        await locator.first().click({ timeout: 3000 });
      } catch {
        await locator.first().click({ timeout: 3000, force: true });
      }
      return `${methodPrefix}:playwright:${candidate.tag}:${candidate.id || candidate.className || candidate.text?.slice(0, 30)}`;
    } catch {}
  }
  return null;
}

async function clickCookieCandidateWithEvents(frame, candidate, preferAllow = true) {
  return frame.evaluate(({ candidate: c, preferAllow: allowFirst }) => {
    const norm = (t) => String(t || '').replace(/\s+/g, ' ').trim();
    const low = (t) => norm(t).toLowerCase();
    const isAllow = (t) => /\ballow cookies\b/i.test(t) || /\baccept all\b/i.test(t) || /\baccept cookies\b/i.test(t);
    const isRefuse = (t) => /\brefuse cookies\b/i.test(t) || /\breject all\b/i.test(t);

    function directText(el) {
      let t = '';
      for (const node of el.childNodes) {
        if (node.nodeType === Node.TEXT_NODE) t += node.textContent;
      }
      return norm(t);
    }

    function isClickable(el) {
      const tag = el.tagName.toLowerCase();
      if (['button', 'a', 'input'].includes(tag)) return true;
      if (el.getAttribute('role') === 'button') return true;
      if (el.hasAttribute('onclick')) return true;
      return window.getComputedStyle(el).cursor === 'pointer';
    }

    function matchesCandidate(el) {
      if (c.id && el.id === c.id) return true;
      if (c.className && el.className === c.className && el.tagName.toLowerCase() === c.tag) return true;
      const text = norm(el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || '');
      const own = directText(el) || text;
      return (c.ownText && own === c.ownText) || (c.text && text === c.text);
    }

    function dispatchClick(el) {
      const rect = el.getBoundingClientRect();
      const x = rect.left + rect.width / 2;
      const y = rect.top + rect.height / 2;
      const opts = { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y };
      el.dispatchEvent(new PointerEvent('pointerdown', opts));
      el.dispatchEvent(new MouseEvent('mousedown', opts));
      el.dispatchEvent(new PointerEvent('pointerup', opts));
      el.dispatchEvent(new MouseEvent('mouseup', opts));
      el.dispatchEvent(new MouseEvent('click', opts));
      if (typeof el.click === 'function') el.click();
    }

    function walkShadowRoots(root, fn) {
      fn(root);
      root.querySelectorAll('*').forEach((el) => {
        if (el.shadowRoot) walkShadowRoots(el.shadowRoot, fn);
      });
    }

    let target = null;
    walkShadowRoots(document, (root) => {
      if (target) return;
      for (const el of root.querySelectorAll('button, a, [role="button"], input[type="button"], input[type="submit"]')) {
        if (!isClickable(el)) continue;
        if (matchesCandidate(el)) { target = el; return; }
      }
    });

    if (!target && c) {
      walkShadowRoots(document, (root) => {
        if (target) return;
        const candidates = [];
        for (const el of root.querySelectorAll('button, a, [role="button"], input[type="button"], input[type="submit"]')) {
          if (!isClickable(el)) continue;
          const style = window.getComputedStyle(el);
          const rect = el.getBoundingClientRect();
          if (style.display === 'none' || rect.width < 2) continue;
          const text = norm(el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || '');
          const own = directText(el) || text;
          let score = 0;
          if (allowFirst && isAllow(own)) score = /\ballow cookies\b/i.test(own) ? 100 : 80;
          else if (!allowFirst && isRefuse(own)) score = 60;
          else if (allowFirst && isAllow(text) && !isRefuse(own)) score = 70;
          if (score) candidates.push({ el, score, own, text });
        }
        candidates.sort((a, b) => b.score - a.score);
        target = candidates[0]?.el || null;
      });
    }

    if (!target) return { ok: false, method: null, clickedCandidate: c };

    dispatchClick(target);
    const active = document.activeElement;
    return {
      ok: true,
      method: `dom_events:${target.tagName.toLowerCase()}:${target.id || directText(target) || norm(target.innerText).slice(0, 30)}`,
      clickedCandidate: {
        tag: target.tagName.toLowerCase(),
        className: typeof target.className === 'string' ? target.className : null,
        id: target.id || null,
        text: norm(target.innerText || target.textContent || '').slice(0, 140),
        ownText: directText(target).slice(0, 80),
      },
      activeElementAfterClick: active ? {
        tag: active.tagName?.toLowerCase(),
        id: active.id || null,
        className: typeof active.className === 'string' ? active.className : null,
      } : null,
    };
  }, { candidate, preferAllow }).catch(() => ({ ok: false, method: null, clickedCandidate: candidate }));
}

async function removeCookieOverlayLastResort(frame) {
  return frame.evaluate(() => {
    let removed = 0;
    const selectors = [
      '[class*="cookie" i]', '[id*="cookie" i]', '[class*="consent" i]', '[id*="consent" i]',
      '[class*="gdpr" i]', '.cc-window', '#cookie-law-info-bar', '#CybotCookiebotDialog',
      '#onetrust-banner-sdk', '[class*="CookieConsent" i]',
    ];
    const hideEl = (el) => {
      el.style.setProperty('display', 'none', 'important');
      el.style.setProperty('visibility', 'hidden', 'important');
      el.style.setProperty('opacity', '0', 'important');
      el.style.setProperty('pointer-events', 'none', 'important');
      try { el.remove(); } catch {}
      removed++;
    };
    for (const sel of selectors) {
      document.querySelectorAll(sel).forEach((el) => {
        const text = (el.innerText || el.textContent || '').toLowerCase();
        if (text.includes('cookie') || text.includes('consent') || text.includes('this website uses')) {
          hideEl(el);
        }
      });
    }
    document.querySelectorAll('div, section, aside, dialog, [role="dialog"]').forEach((el) => {
      const style = window.getComputedStyle(el);
      if (style.position !== 'fixed' && style.position !== 'sticky') return;
      const text = (el.innerText || el.textContent || '').toLowerCase();
      const rect = el.getBoundingClientRect();
      if (rect.width > 100 && rect.height > 40 && text.includes('cookie') && text.includes('allow')) {
        hideEl(el);
      }
    });
    document.querySelectorAll('[class*="overlay" i], [class*="backdrop" i]').forEach((el) => {
      const text = (el.innerText || el.textContent || '').toLowerCase();
      if (text.includes('cookie') || text.includes('allow cookies')) hideEl(el);
    });
    return removed;
  }).catch(() => 0);
}

async function attemptCookieDismissInFrame(frame, frameLabel, { preferAllow = true } = {}) {
  const attempt = { ...emptyCookieAttemptDiagnostics(), frameUsed: frameLabel };
  const ctx = await collectCookieBannerContext(frame, frameLabel);
  attempt.bannerElementTag = ctx.bannerElementTag;
  attempt.bannerElementClass = ctx.bannerElementClass;
  attempt.bannerElementId = ctx.bannerElementId;
  attempt.buttonCandidates = ctx.buttonCandidates;
  attempt.beforeText = ctx.beforeText;
  attempt.iframeCount = ctx.iframeCount;
  attempt.shadowHostCandidates = ctx.shadowHostCandidates;

  const allowCandidates = ctx.buttonCandidates.filter(c => c.preference === 'allow');
  const refuseCandidates = ctx.buttonCandidates.filter(c => c.preference === 'refuse');
  const ordered = preferAllow
    ? [...allowCandidates, ...refuseCandidates]
    : [...refuseCandidates, ...allowCandidates];

  for (const candidate of ordered.slice(0, 6)) {
    let method = await clickCookieCandidateWithPlaywright(frame, candidate, frameLabel);
    if (!method) {
      const ev = await clickCookieCandidateWithEvents(frame, candidate, preferAllow);
      if (ev.ok) {
        method = `${frameLabel}:${ev.method}`;
        attempt.clickedCandidate = ev.clickedCandidate || candidate;
        attempt.activeElementAfterClick = ev.activeElementAfterClick || null;
      }
    } else {
      attempt.clickedCandidate = candidate;
      attempt.method = method;
    }
    if (method) {
      attempt.method = method;
      await frame.page().waitForTimeout(600);
      await waitForCookieBannerGone(frame.page(), 4000);
      const afterCtx = await collectCookieBannerContext(frame, frameLabel);
      attempt.afterText = afterCtx.beforeText;
      const pageStillVisible = await isCookieBannerVisible(frame.page());
      attempt.didTextDisappear = !pageStillVisible;
      attempt.didElementDetach = !afterCtx.bannerElementId && !!ctx.bannerElementId;
      if (!pageStillVisible) {
        attempt.method = method;
        return { success: true, method, attempt };
      }
      attempt.failureReason = 'clicked_but_banner_still_visible';
    }
  }

  const evFallback = await clickCookieCandidateWithEvents(frame, null, preferAllow);
  if (evFallback.ok) {
    attempt.method = `${frameLabel}:${evFallback.method}`;
    attempt.clickedCandidate = evFallback.clickedCandidate;
    attempt.activeElementAfterClick = evFallback.activeElementAfterClick;
    await frame.page().waitForTimeout(600);
    await waitForCookieBannerGone(frame.page(), 4000);
    const afterCtx = await collectCookieBannerContext(frame, frameLabel);
    attempt.afterText = afterCtx.beforeText;
    attempt.didTextDisappear = !(await isCookieBannerVisible(frame.page()));
    if (attempt.didTextDisappear) {
      return { success: true, method: attempt.method, attempt };
    }
    attempt.failureReason = 'event_fallback_clicked_but_banner_still_visible';
  }

  attempt.failureReason = attempt.failureReason || 'no_clickable_candidate_matched';
  return { success: false, method: attempt.method, attempt };
}

async function clickCookieConsentButton(page) {
  const frames = page.frames();
  for (let i = 0; i < frames.length; i++) {
    const frame = frames[i];
    const label = i === 0 ? 'main' : `frame_${i}`;
    try {
      const hasCookieText = await frame.evaluate(() => {
        const t = (document.body?.innerText || '').toLowerCase();
        return t.includes('cookie') && (t.includes('allow cookies') || t.includes('refuse cookies') || t.includes('this website uses cookies'));
      }).catch(() => false);
      if (!hasCookieText && i > 0) continue;
      const result = await attemptCookieDismissInFrame(frame, label);
      recordCookieAttempt(result.attempt);
      if (result.success) return result.method;
    } catch {}
  }

  const mainResult = await attemptCookieDismissInFrame(page.mainFrame(), 'main_retry');
  recordCookieAttempt(mainResult.attempt);
  if (mainResult.success) return mainResult.method;

  const playwrightAttempts = [
    { method: 'playwright_role_allow_cookies', run: () => page.getByRole('button', { name: /allow cookies/i }).first() },
    { method: 'playwright_locator_allow_in_banner', run: () => page.locator('[class*="cookie" i] button, [id*="cookie" i] button, [class*="consent" i] button').filter({ hasText: /allow cookies/i }).first() },
    { method: 'playwright_role_refuse_cookies', run: () => page.getByRole('button', { name: /refuse cookies/i }).first() },
  ];
  for (const pw of playwrightAttempts) {
    try {
      const locator = pw.run();
      if (await locator.isVisible({ timeout: 500 }).catch(() => false)) {
        await locator.scrollIntoViewIfNeeded().catch(() => {});
        await locator.hover().catch(() => {});
        try { await locator.click({ timeout: 3000 }); } catch { await locator.click({ timeout: 3000, force: true }); }
        return pw.method;
      }
    } catch {}
  }
  return null;
}

async function dismissCookieBanner(page) {
  cookieDismissDiagnostics.cookieDismissAttempted++;
  const attemptStart = emptyCookieAttemptDiagnostics();
  try {
    if (!(await isCookieBannerVisible(page))) {
      cookieDismissDiagnostics.cookieBannerStillVisible = false;
      attemptStart.method = 'already_hidden';
      recordCookieAttempt(attemptStart);
      return { success: true, method: 'already_hidden' };
    }

    attemptStart.beforeText = await page.evaluate(() => (document.body?.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 600)).catch(() => null);
    attemptStart.iframeCount = page.frames().length;

    let method = await clickCookieConsentButton(page);
    if (method) {
      cookieDismissDiagnostics.cookieClickMethod = method;
      await page.waitForTimeout(500);
      await waitForCookieBannerGone(page, 5000);
    }

    let stillVisible = await isCookieBannerVisible(page);
    cookieDismissDiagnostics.cookieBannerStillVisible = stillVisible;
    if (!stillVisible) {
      cookieDismissDiagnostics.cookieDismissSucceeded++;
      attemptStart.method = method || 'unknown';
      attemptStart.didTextDisappear = true;
      attemptStart.afterText = await page.evaluate(() => (document.body?.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 600)).catch(() => null);
      recordCookieAttempt({ ...cookieDismissDiagnostics.lastAttempt, ...attemptStart });
      console.log(`  [cookie] dismissed via ${method || 'unknown'}`);
      return { success: true, method: method || 'unknown' };
    }

    if (method) {
      method = await clickCookieConsentButton(page);
      if (method) {
        cookieDismissDiagnostics.cookieClickMethod = `${cookieDismissDiagnostics.cookieClickMethod}|retry:${method}`;
        await waitForCookieBannerGone(page, 5000);
        stillVisible = await isCookieBannerVisible(page);
        cookieDismissDiagnostics.cookieBannerStillVisible = stillVisible;
        if (!stillVisible) {
          cookieDismissDiagnostics.cookieDismissSucceeded++;
          console.log(`  [cookie] dismissed on retry via ${method}`);
          return { success: true, method };
        }
      }
    }

    const removed = await removeCookieOverlayLastResort(page.mainFrame());
    for (const frame of page.frames().slice(1)) {
      await removeCookieOverlayLastResort(frame).catch(() => {});
    }
    await page.waitForTimeout(400);
    await waitForCookieBannerGone(page, 3000);
    stillVisible = await isCookieBannerVisible(page);
    cookieDismissDiagnostics.cookieBannerStillVisible = stillVisible;
    if (!stillVisible) {
      cookieDismissDiagnostics.cookieDismissSucceeded++;
      cookieDismissDiagnostics.cookieClickMethod = 'overlay_removed_last_resort';
      const last = cookieDismissDiagnostics.lastAttempt || attemptStart;
      last.method = 'overlay_removed_last_resort';
      last.afterText = await page.evaluate(() => (document.body?.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 600)).catch(() => null);
      last.didTextDisappear = true;
      last.failureReason = removed ? `removed_${removed}_overlay_nodes` : 'overlay_hidden';
      recordCookieAttempt(last);
      console.log(`  [cookie] overlay removed last resort (${removed} node(s))`);
      return { success: true, method: 'overlay_removed_last_resort' };
    }

    attemptStart.afterText = await page.evaluate(() => (document.body?.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 600)).catch(() => null);
    attemptStart.failureReason = cookieDismissDiagnostics.lastAttempt?.failureReason || 'banner_still_visible_after_all_attempts';
    attemptStart.clickedCandidate = cookieDismissDiagnostics.lastAttempt?.clickedCandidate || null;
    attemptStart.buttonCandidates = cookieDismissDiagnostics.lastAttempt?.buttonCandidates || [];
    recordCookieAttempt(attemptStart);
  } catch (e) {
    console.log(`  [cookie] dismiss failed: ${e.message}`);
    attemptStart.failureReason = e.message;
    recordCookieAttempt(attemptStart);
  }
  cookieDismissDiagnostics.cookieBannerStillVisible = await isCookieBannerVisible(page).catch(() => true);
  return { success: false, method: cookieDismissDiagnostics.cookieClickMethod };
}

async function readAnyVisibleModalText(page) {
  return page.evaluate(() => {
    const el = document.querySelector('.modal.in, .modal.show, [role="dialog"].show, [role="dialog"][aria-modal="true"]');
    if (!el) return { text: '', visible: false };
    const style = window.getComputedStyle(el);
    const visible = style.display !== 'none' && style.visibility !== 'hidden' && el.offsetParent !== null;
    if (!visible) return { text: '', visible: false };
    return { text: (el.innerText || '').replace(/\s+/g, ' ').trim(), visible: true };
  });
}

async function waitForModalTextAbsent(page, previousHash, timeoutMs = 4000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const snap = await readAnyVisibleModalText(page);
    const h = hashModalText(snap.text);
    if (!snap.visible || !snap.text || snap.text.length < 8) return true;
    if (previousHash && h !== previousHash) return true;
    await page.waitForTimeout(150);
  }
  return false;
}

async function waitForModalTextChange(page, previousHash, label, timeoutMs = 6000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const snap = await readAnyVisibleModalText(page);
    const h = hashModalText(snap.text);
    if (snap.text && snap.text.length > 8 && h && h !== previousHash) {
      console.log(`  [getSessionModalDetails ${label}] modal text changed (${previousHash || 'none'} → ${h})`);
      return { text: snap.text, hash: h, changed: true };
    }
    await page.waitForTimeout(180);
  }
  const snap = await readAnyVisibleModalText(page);
  const h = hashModalText(snap.text);
  console.log(`  [getSessionModalDetails ${label}] modal text did not change (hash=${h || 'none'})`);
  return { text: snap.text || '', hash: h, changed: false };
}

function validateTileMatchesSession(tileMeta, session) {
  const ctx = sessionLookupContext(session);
  const mismatches = [];
  if (Number.isFinite(ctx.ts) && tileMeta?.elTs && tileMeta.elTs !== ctx.ts) {
    mismatches.push({ field: 'ts', expected: ctx.ts, found: tileMeta.elTs });
  }
  if (ctx.time && tileMeta?.elTime && normalizeTimeToken(tileMeta.elTime) !== normalizeTimeToken(ctx.time)) {
    mismatches.push({ field: 'time', expected: ctx.time, found: tileMeta.elTime });
  }
  if (ctx.level && tileMeta?.elLevel && !levelsMatch(ctx.level, tileMeta.elLevel)) {
    mismatches.push({ field: 'sessionType', expected: ctx.level, found: tileMeta.elLevel });
  }
  if (ctx.waveSide && tileMeta?.elWaveSide && !waveSidesMatch(ctx.waveSide, tileMeta.elWaveSide)) {
    mismatches.push({ field: 'waveSide', expected: ctx.waveSide, found: tileMeta.elWaveSide });
  }
  return { match: mismatches.length === 0, mismatches, score: tileMeta?.score ?? null };
}

async function extractTileMeta(tile, score = null) {
  return tile.evaluate((node, tileScore) => {
    const rect = node.getBoundingClientRect();
    const cls = node.className || '';
    const title = node.dataset.originalTitle || '';
    const text = (node.innerText || node.textContent || '').replace(/\s+/g, ' ').trim();
    const wm = cls.match(/booking-agenda-clickable_(\d+)_(\d+)/);
    const lm = title.match(/Session level\s*:<\/b>\s*([^<]+)/i);
    const fm = title.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
    const td = node.closest('td');
    let elWaveSide = '';
    if (td) {
      const table = td.closest('table');
      const headerRow = table?.querySelector('thead tr') || table?.querySelector('tr');
      elWaveSide = (headerRow?.cells?.[td.cellIndex]?.textContent || '').replace(/\s+/g, ' ').trim();
    }
    return {
      className: cls,
      text: text.slice(0, 120),
      boundingBox: { x: Math.round(rect.x), y: Math.round(rect.y), width: Math.round(rect.width), height: Math.round(rect.height) },
      elTs: wm ? +wm[1] : null,
      elWave: wm ? +wm[2] : null,
      elTime: fm ? fm[1].replace(/\s+/g, ' ').trim().toLowerCase() : '',
      elLevel: lm ? lm[1].replace(/\s+/g, ' ').trim() : '',
      elWaveSide: elWaveSide,
      score: tileScore,
    };
  }, score);
}

async function readModalText(page, modal) {
  return modal.evaluate(() => {
    const modalEl = document.querySelector('.modal.in, .modal.show, [role="dialog"].show, [role="dialog"][aria-modal="true"]');
    const el = modalEl || document.querySelector('.modal.in, .modal.show, .modal, [role="dialog"]');
    if (!el) return { text: '', maxQty: null };
    const text = (el.innerText || '').replace(/\s+/g, ' ').trim();
    const qty = el.querySelector('input.qty-info') || document.querySelector('input.qty-info');
    const maxAttr = qty?.getAttribute('max');
    const maxQty = maxAttr ? parseInt(maxAttr, 10) : null;
    return { text, maxQty: Number.isFinite(maxQty) && maxQty > 0 ? maxQty : null };
  });
}

async function clickSessionTileForModal(page, session, label, lifecycleState = detailModalLifecycleState) {
  const state = lifecycleState || resetDetailModalLifecycleState();
  await dismissCookieBanner(page);
  await waitForCookieBannerGone(page, 4000);

  const previousModalTextHash = state.lastModalTextHash;
  if (await isModalVisible(page)) {
    state.modalLifecycleSamples.push({
      sessionKey: session.key,
      phase: 'pre_close_modal_visible',
      previousModalTextHash,
    });
    await closeModal(page, label);
    await waitForModalGone(page, 5000);
  }
  await waitForModalTextAbsent(page, previousModalTextHash, 4000);

  const tileResult = await findSessionTileWithDiagnostics(page, session, label);
  const tileClickDiagnostics = tileResult.tileClickDiagnostics;
  state.tileClickSamples.push({ sessionKey: session.key, ...tileClickDiagnostics });

  if (!tileResult.tile) {
    return {
      tile: null,
      tileSel: tileResult.selector,
      tileMethod: tileResult.method,
      modal: null,
      rawTileText: null,
      tileClickDiagnostics,
    };
  }

  const tileValidation = validateTileMatchesSession(tileResult.tileMeta, session);
  if (!tileValidation.match) {
    console.log(`  [getSessionModalDetails ${label}] tile mismatch — ${tileValidation.mismatches.map(m => m.field).join(', ')}`);
    return {
      tile: tileResult.tile,
      tileSel: tileResult.selector,
      tileMethod: tileResult.method,
      modal: null,
      rawTileText: tileResult.tileMeta?.text || null,
      tileClickDiagnostics,
      tileValidation,
      detailStatus: 'failed_tile_mismatch',
      failureType: 'failed_tile_mismatch',
      detailError: `tile_mismatch: ${tileValidation.mismatches.map(m => `${m.field} expected ${m.expected} got ${m.found}`).join('; ')}`,
    };
  }

  const rawTileText = tileResult.tileMeta?.text
    || await tileResult.tile.evaluate(el => (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim()).catch(() => '');
  console.log(`  [getSessionModalDetails ${label}] clicking tile (${tileResult.method}, score=${tileResult.tileMeta?.score ?? 'n/a'})`);

  await tileResult.tile.click({ timeout: 10_000 });
  await page.waitForTimeout(250);

  let modal = await waitForModal(page, label);
  let rawModalText = '';
  if (modal) {
    try {
      const meta = await readModalText(page, modal);
      rawModalText = meta.text || '';
    } catch {}
  } else {
    rawModalText = (await readAnyVisibleModalText(page)).text || '';
  }

  let currentModalTextHash = hashModalText(rawModalText);
  let staleModalDetected = !!(previousModalTextHash && currentModalTextHash && currentModalTextHash === previousModalTextHash && rawModalText.length > 20);

  if (staleModalDetected) {
    console.log(`  [getSessionModalDetails ${label}] stale modal detected (hash=${currentModalTextHash}) — retrying after force close`);
    await closeModal(page, label);
    await waitForModalGone(page, 5000);
    await waitForModalTextAbsent(page, previousModalTextHash, 4000);
    await tileResult.tile.click({ timeout: 10_000 });
    await page.waitForTimeout(300);
    modal = await waitForModal(page, label);
    const change = await waitForModalTextChange(page, previousModalTextHash, label, 6000);
    rawModalText = change.text || '';
    currentModalTextHash = change.hash;
    staleModalDetected = !!(previousModalTextHash && currentModalTextHash && currentModalTextHash === previousModalTextHash && rawModalText.length > 20);
  } else if (previousModalTextHash && rawModalText.length > 20) {
    const change = await waitForModalTextChange(page, previousModalTextHash, label, 2000);
    if (change.changed) {
      rawModalText = change.text;
      currentModalTextHash = change.hash;
    }
  }

  state.modalLifecycleSamples.push({
    sessionKey: session.key,
    phase: staleModalDetected ? 'post_click_stale' : 'post_click',
    previousModalTextHash,
    currentModalTextHash,
    staleModalDetected,
  });

  if (staleModalDetected) {
    await closeModal(page, label);
    await waitForModalGone(page, 3000);
    return {
      tile: tileResult.tile,
      tileSel: tileResult.selector,
      tileMethod: tileResult.method,
      modal,
      rawTileText,
      rawModalText,
      tileClickDiagnostics,
      staleModalDetected: true,
      previousModalTextHash,
      currentModalTextHash,
      detailStatus: 'failed_modal_stale',
      failureType: 'failed_modal_stale',
      detailError: 'modal_text_unchanged_after_tile_click',
    };
  }

  if (rawModalText.length > 20) {
    state.lastModalTextHash = currentModalTextHash;
    state.lastModalText = rawModalText;
    state.lastSessionKey = session.key;
  }

  return {
    tile: tileResult.tile,
    tileSel: tileResult.selector,
    tileMethod: tileResult.method,
    modal,
    rawTileText,
    rawModalText,
    tileClickDiagnostics,
    staleModalDetected: false,
    previousModalTextHash,
    currentModalTextHash,
  };
}

function cookieOverlayFailure(rawTileText, rawModalText) {
  return {
    detailStatus: 'failed_cookie_overlay',
    failureType: 'failed_cookie_overlay',
    detailError: 'cookie consent overlay blocked session modal',
    parseReason: 'cookie_banner_text',
    rawTileText,
    rawModalText: truncateDetailText(rawModalText, 1500),
  };
}
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

function buildDetailPayloadFromParsed(parsed, { rawTileText, rawModalText, capacityFromModal, priceInfo, slotsFromClicks, modalValidation = null, session = null } = {}) {
  const textSource = rawModalText || rawTileText || '';
  const price = priceInfo || parsePriceFromText(textSource);
  const capacity = parsed?.capacity ?? null;
  const parserOutput = buildParserOutputFromText(textSource);

  const baseMeta = {
    rawTileText,
    rawModalText,
    modalValidation,
    parserOutput,
    verified: modalValidation?.match && modalValidation?.confidence === 'exact_match',
  };

  if (parsed?.packed) {
    return {
      slots: 0,
      available: false,
      packed: true,
      capacity: parsed.capacity ?? capacity,
      ...price,
      detailStatus: 'checked_packed',
      parseReason: parsed.parseReason,
      ...baseMeta,
    };
  }

  if (parsed?.slots != null) {
    return {
      slots: parsed.slots,
      capacity: parsed.capacity ?? capacity,
      estimatedBooked: parsed.estimatedBooked,
      ...price,
      detailStatus: 'checked_with_slots',
      parseReason: parsed.parseReason,
      ...baseMeta,
    };
  }

  if (parsed?.openNoCount) {
    const status = parsed.parseReason === 'text_open_or_available'
      ? 'checked_available_no_slot_count'
      : 'checked_open_no_slots_visible';
    return {
      openNoCount: true,
      ...price,
      detailStatus: status,
      parseReason: parsed.parseReason,
      ...baseMeta,
    };
  }

  if (slotsFromClicks != null && slotsFromClicks > 0) {
    return {
      openNoCount: true,
      detailStatus: 'checked_available_no_slot_count',
      parseReason: 'plus_clicks_unverified_no_text_parse',
      slotsFromClicks,
      ...baseMeta,
    };
  }

  if (slotsFromClicks === 0) {
    const modalParsed = parseDetailAvailabilityFromText(rawModalText || '');
    if (modalParsed?.packed) {
      return {
        slots: 0,
        available: false,
        packed: true,
        capacity: modalParsed.capacity ?? capacity,
        ...price,
        detailStatus: 'checked_packed',
        parseReason: modalParsed.parseReason || 'plus_zero_packed',
        ...baseMeta,
      };
    }
    if (modalParsed?.openNoCount || /\bopen\b/i.test(rawModalText || '') || /\bavailable\b/i.test(rawModalText || '')) {
      const status = modalParsed?.parseReason === 'text_open_or_available'
        ? 'checked_available_no_slot_count'
        : 'checked_open_no_slots_visible';
      return {
        openNoCount: true,
        ...price,
        detailStatus: status,
        parseReason: modalParsed?.parseReason || 'plus_zero_open_no_count',
        ...baseMeta,
      };
    }
    if (rawModalText && rawModalText.trim().length > 8) {
      return {
        detailStatus: 'failed_parse',
        failureType: 'failed_parse',
        detailError: 'modal text present but slot count not parsed',
        parseReason: 'no_matching_patterns',
        ...baseMeta,
        ...parserOutput,
      };
    }
  }

  return null;
}

async function findSessionTile(page, session, label) {
  const result = await findSessionTileWithDiagnostics(page, session, label);
  return {
    tile: result.tile,
    selector: result.selector,
    method: result.method,
    tilePreview: result.tileMeta?.text,
    tileMeta: result.tileMeta,
    tileClickDiagnostics: result.tileClickDiagnostics,
    candidatesCount: result.candidatesCount,
    selectedIndex: result.selectedIndex,
  };
}

async function findSessionTileWithDiagnostics(page, session, label) {
  const ctx = sessionLookupContext(session);
  const ts = ctx.ts;
  const wave = ctx.wave;

  const selectors = [
    `div[class*="booking-agenda-clickable_${ts}_${wave}"]`,
    `[class*="booking-agenda-clickable_${ts}_${wave}"]`,
    `div.dynamic-cal-booking-ts[class*="_${ts}_${wave}"]`,
  ];
  if (ctx.tileClassName) {
    const safeClass = ctx.tileClassName.split(/\s+/).find(c => c.includes('booking-agenda-clickable'));
    if (safeClass) selectors.unshift(`div.${safeClass.replace(/([^\w-])/g, '\\$1')}`);
  }

  for (const sel of selectors) {
    const tile = await page.$(sel).catch(() => null);
    if (tile) {
      const tileMeta = await extractTileMeta(tile, 200);
      return {
        tile,
        selector: sel,
        method: 'css_selector',
        tileMeta,
        candidatesCount: 1,
        selectedIndex: 0,
        tileClickDiagnostics: buildTileClickDiagnostics(session, tileMeta, 0, 1, 'css_selector', sel),
      };
    }
  }

  try {
    const scored = await page.evaluate(({ ctx: c }) => {
      const normTime = (t) => String(t || '').toLowerCase().replace(/\s+/g, ' ').trim();
      const normLevel = (l) => String(l || '').toLowerCase().replace(/\s+/g, ' ').trim();
      const normSide = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim();
      const normText = (t) => String(t || '').toLowerCase().replace(/\s+/g, ' ').trim();

      const targetTime = normTime(c.time);
      const targetLevel = normLevel(c.level);
      const targetSide = normSide(c.waveSide);
      const targetTileText = normText(c.tileText);
      const targetIso = c.isoDate;

      function columnHeader(el) {
        const td = el.closest('td');
        if (!td) return '';
        const table = td.closest('table');
        const headerRow = table?.querySelector('thead tr') || table?.querySelector('tr');
        return normText(headerRow?.cells?.[td.cellIndex]?.textContent);
      }

      const candidates = [];
      for (const el of document.querySelectorAll('div.dynamic-cal-booking-ts[data-original-title]')) {
        const cls = el.className || '';
        const title = el.dataset.originalTitle || '';
        const wm = cls.match(/booking-agenda-clickable_(\d+)_(\d+)/);
        if (!wm) continue;
        const elTs = +wm[1];
        const elWave = +wm[2];
        const lm = title.match(/Session level\s*:<\/b>\s*([^<]+)/i);
        const fm = title.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
        const elLevel = lm ? normLevel(lm[1]) : '';
        const elTime = fm ? normTime(fm[1]) : '';
        const elText = normText((el.innerText || el.textContent || title).replace(/<[^>]+>/g, ' '));
        const hdr = columnHeader(el);

        let score = 0;
        if (Number.isFinite(c.ts) && elTs === c.ts) score += 100;
        else if (Number.isFinite(c.ts) && Math.abs(elTs - c.ts) <= 60) score += 40;
        if (Number.isFinite(c.wave) && elWave === c.wave) score += 50;
        if (targetTime && elTime && targetTime === elTime) score += 45;
        else if (targetTime && elTime && targetTime.replace(/\s/g, '') === elTime.replace(/\s/g, '')) score += 35;
        if (targetLevel && elLevel && (targetLevel === elLevel || elLevel.includes(targetLevel) || targetLevel.includes(elLevel))) score += 35;
        if (targetSide && (hdr.includes(targetSide) || elText.includes(targetSide))) score += 30;
        if (targetTileText && elText) {
          if (elText === targetTileText) score += 50;
          else if (elText.includes(targetTileText) || targetTileText.includes(elText)) score += 30;
        }
        if (c.tileClassName && cls === c.tileClassName) score += 80;
        if (cls.includes(`_${c.ts}_${c.wave}`)) score += 60;

        candidates.push({ score, elTs, elWave, elTime, elLevel, elWaveSide: hdr, className: cls, text: elText.slice(0, 120) });
      }

      candidates.sort((a, b) => b.score - a.score);
      return { candidates, bestIndex: candidates.length ? 0 : -1, bestScore: candidates[0]?.score || 0 };
    }, { ctx });

    const handle = await page.evaluateHandle(({ ctx: c, bestScore }) => {
      const normTime = (t) => String(t || '').toLowerCase().replace(/\s+/g, ' ').trim();
      const normLevel = (l) => String(l || '').toLowerCase().replace(/\s+/g, ' ').trim();
      const normText = (t) => String(t || '').toLowerCase().replace(/\s+/g, ' ').trim();
      const targetTime = normTime(c.time);
      const targetLevel = normLevel(c.level);
      const targetSide = String(c.waveSide || '').toLowerCase();
      const targetTileText = normText(c.tileText);

      function columnHeader(el) {
        const td = el.closest('td');
        if (!td) return '';
        const table = td.closest('table');
        const headerRow = table?.querySelector('thead tr') || table?.querySelector('tr');
        return normText(headerRow?.cells?.[td.cellIndex]?.textContent);
      }

      const candidates = [];
      for (const el of document.querySelectorAll('div.dynamic-cal-booking-ts[data-original-title]')) {
        const cls = el.className || '';
        const title = el.dataset.originalTitle || '';
        const wm = cls.match(/booking-agenda-clickable_(\d+)_(\d+)/);
        if (!wm) continue;
        const elTs = +wm[1];
        const elWave = +wm[2];
        const lm = title.match(/Session level\s*:<\/b>\s*([^<]+)/i);
        const fm = title.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
        const elLevel = lm ? normLevel(lm[1]) : '';
        const elTime = fm ? normTime(fm[1]) : '';
        const elText = normText((el.innerText || el.textContent || title).replace(/<[^>]+>/g, ' '));
        const hdr = columnHeader(el);
        let score = 0;
        if (Number.isFinite(c.ts) && elTs === c.ts) score += 100;
        if (Number.isFinite(c.wave) && elWave === c.wave) score += 50;
        if (targetTime && elTime && targetTime === elTime) score += 45;
        if (targetLevel && elLevel && (targetLevel === elLevel || elLevel.includes(targetLevel) || targetLevel.includes(elLevel))) score += 35;
        if (targetSide && (hdr.includes(targetSide) || elText.includes(targetSide))) score += 30;
        if (targetTileText && elText && (elText === targetTileText || elText.includes(targetTileText))) score += 30;
        if (cls.includes(`_${c.ts}_${c.wave}`)) score += 60;
        if (score >= bestScore - 5) candidates.push({ el, score });
      }
      candidates.sort((a, b) => b.score - a.score);
      return candidates[0]?.el || null;
    }, { ctx, bestScore: scored.bestScore });

    const el = handle.asElement();
    if (el) {
      const tileMeta = await extractTileMeta(el, scored.bestScore);
      tileMeta.elTs = tileMeta.elTs ?? scored.candidates[0]?.elTs ?? null;
      tileMeta.elWave = tileMeta.elWave ?? scored.candidates[0]?.elWave ?? null;
      tileMeta.elTime = tileMeta.elTime || scored.candidates[0]?.elTime || '';
      tileMeta.elLevel = tileMeta.elLevel || scored.candidates[0]?.elLevel || '';
      tileMeta.elWaveSide = tileMeta.elWaveSide || scored.candidates[0]?.elWaveSide || '';
      console.log(`  [getSessionModalDetails ${label}] tile found via scored DOM scan (score=${scored.bestScore}, candidates=${scored.candidates.length})`);
      return {
        tile: el,
        selector: 'scored_dom_scan',
        method: 'scored_dom_scan',
        tileMeta,
        candidatesCount: scored.candidates.length,
        selectedIndex: scored.bestIndex,
        tileClickDiagnostics: buildTileClickDiagnostics(session, tileMeta, scored.bestIndex, scored.candidates.length, 'scored_dom_scan', 'scored_dom_scan'),
      };
    }
    await handle.dispose();
  } catch (e) {
    console.log(`  [getSessionModalDetails ${label}] scored DOM scan failed: ${e.message}`);
  }

  return {
    tile: null,
    selector: selectors[0],
    method: 'not_found',
    tileMeta: null,
    candidatesCount: 0,
    selectedIndex: -1,
    tileClickDiagnostics: buildTileClickDiagnostics(session, null, -1, 0, 'not_found', selectors[0]),
  };
}

function buildTileClickDiagnostics(session, tileMeta, selectedIndex, candidatesCount, method, selector) {
  return {
    expectedSessionKey: session?.key || null,
    expectedTileClassName: session?.tileClassName || session?.raw?.tileClassName || null,
    expectedIsoDate: session?.isoDate || session?.dateKey || null,
    expectedTime: session?.time || null,
    expectedSessionType: session?.level || null,
    expectedWaveSide: session?.waveSide || null,
    actualClickedTileClass: tileMeta?.className || null,
    actualClickedTileText: tileMeta?.text || null,
    actualClickedTileBoundingBox: tileMeta?.boundingBox || null,
    clickedTileIndexAmongCandidates: selectedIndex,
    tileMatchScore: tileMeta?.score ?? null,
    candidateTilesCount: candidatesCount,
    tileMethod: method,
    tileSelector: selector,
  };
}

async function waitForModalGone(page, timeoutMs = 3000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (!(await isModalVisible(page))) return true;
    await page.waitForTimeout(150);
  }
  return !(await isModalVisible(page));
}

function wrapModalDetailPayload(session, rawModalText, rawTileText, payload) {
  if (!payload) return payload;
  if (!rawModalText) return payload;
  const modalValidation = validateModalAssociation(session, rawModalText);
  payload.modalValidation = modalValidation;
  payload.rawModalText = rawModalText;
  payload.rawTileText = rawTileText;
  if (modalValidation.confidence === 'mismatch') {
    return {
      detailStatus: 'failed_modal_mismatch',
      failureType: 'failed_modal_mismatch',
      detailError: `modal_mismatch: ${modalValidation.mismatches.map(m => `${m.field} expected ${m.expected} got ${m.found}`).join('; ')}`,
      rawTileText,
      rawModalText: truncateDetailText(rawModalText, 1500),
      modalValidation,
      mismatches: modalValidation.mismatches,
      parseReason: 'failed_modal_mismatch',
    };
  }
  return buildDetailPayloadFromParsed(
    payload.slots != null || payload.packed ? {
      slots: payload.slots,
      capacity: payload.capacity,
      estimatedBooked: payload.estimatedBooked,
      packed: payload.packed,
      openNoCount: payload.openNoCount,
      parseReason: payload.parseReason,
    } : parseDetailAvailabilityFromText(rawModalText),
    {
      rawTileText,
      rawModalText,
      priceInfo: payload.price_text ? {
        price_text: payload.price_text,
        price_min: payload.price_min,
        price_max: payload.price_max,
        currency: payload.currency,
      } : parsePriceFromText(rawModalText || rawTileText),
      slotsFromClicks: payload.slotsFromClicks,
      modalValidation,
      session,
    },
  ) || payload;
}

async function getSessionModalDetails(page, session, { cookieRetryAllowed = true, lifecycleState = detailModalLifecycleState } = {}) {
  const ctx = sessionLookupContext(session);
  const label = ctx.key || `${ctx.ts}_${ctx.wave}`;
  console.log(`\n[getSessionModalDetails ${label}] starting`);

  await dismissCookieBanner(page);
  await waitForCookieBannerGone(page, 4000);

  try {
    const firstOpen = await clickSessionTileForModal(page, session, label, lifecycleState);
    if (firstOpen.detailStatus === 'failed_tile_mismatch' || firstOpen.detailStatus === 'failed_modal_stale') {
      return {
        detailStatus: firstOpen.detailStatus,
        failureType: firstOpen.failureType,
        detailError: firstOpen.detailError,
        rawTileText: firstOpen.rawTileText || ctx.tileText || null,
        rawModalText: firstOpen.rawModalText || null,
        tileClickDiagnostics: firstOpen.tileClickDiagnostics,
        staleModalDetected: firstOpen.staleModalDetected === true,
        previousModalTextHash: firstOpen.previousModalTextHash || null,
        currentModalTextHash: firstOpen.currentModalTextHash || null,
        tileValidation: firstOpen.tileValidation || null,
        parseReason: firstOpen.detailStatus,
      };
    }
    if (!firstOpen.tile) {
      console.log(`  [getSessionModalDetails ${label}] tile not found (${firstOpen.tileSel})`);
      return {
        detailStatus: 'failed_selector',
        failureType: 'failed_selector',
        detailError: `tile not found (${firstOpen.tileSel})`,
        failedSelector: firstOpen.tileSel,
        rawTileText: ctx.tileText || null,
        rawModalText: null,
        parseReason: 'tile_not_found',
      };
    }

    const rawTileText = firstOpen.rawTileText || ctx.tileText || '';
    console.log(`  [getSessionModalDetails ${label}] tile found via ${firstOpen.tileMethod}, text="${rawTileText.slice(0, 80)}"`);

    const tileClickDiagnostics = firstOpen.tileClickDiagnostics || null;
    const lifecycleMeta = {
      tileClickDiagnostics,
      previousModalTextHash: firstOpen.previousModalTextHash || null,
      currentModalTextHash: firstOpen.currentModalTextHash || null,
      staleModalDetected: firstOpen.staleModalDetected === true,
    };

    const tileParsed = parseDetailAvailabilityFromText(rawTileText);
    if (tileParsed?.packed) {
      const payload = buildDetailPayloadFromParsed(tileParsed, { rawTileText, rawModalText: null, priceInfo: parsePriceFromText(rawTileText) });
      console.log(`  [getSessionModalDetails ${label}] packed from tile text`);
      return payload;
    }
    if (tileParsed?.slots != null) {
      const payload = buildDetailPayloadFromParsed(tileParsed, { rawTileText, rawModalText: null, priceInfo: parsePriceFromText(rawTileText) });
      console.log(`  [getSessionModalDetails ${label}] ${payload.slots} slot(s) from tile text`);
      return payload;
    }

    let modal = firstOpen.modal;
    if (!modal) {
      console.log(`  [getSessionModalDetails ${label}] abort — modal never appeared`);
      if (tileParsed?.openNoCount) {
        return buildDetailPayloadFromParsed(tileParsed, { rawTileText, rawModalText: rawTileText, priceInfo: parsePriceFromText(rawTileText) });
      }
      return {
        detailStatus: 'failed_modal_open',
        failureType: 'failed_modal_open',
        detailError: 'modal never appeared after tile click',
        rawTileText,
        rawModalText: null,
      };
    }

    const screenshotPath = path.join(__dirname, 'debug-modal.png');
    if (process.env.DEBUG_MODAL === '1') {
      await page.screenshot({ path: screenshotPath });
      console.log(`  [getSessionModalDetails ${label}] debug screenshot saved → ${screenshotPath}`);
    }

    let rawModalText = firstOpen.rawModalText || '';
    let capacityFromModal = null;
    if (!rawModalText && modal) {
      try {
        const meta = await readModalText(page, modal);
        rawModalText = meta.text || '';
        capacityFromModal = meta.maxQty;
      } catch (pe) {
        console.log(`  [getSessionModalDetails ${label}] modal text read skipped: ${pe.message}`);
      }
    } else if (modal) {
      try {
        const meta = await readModalText(page, modal);
        capacityFromModal = meta.maxQty;
      } catch {}
    }

    const bannerStillVisible = await isCookieBannerVisible(page);
    if (isCookieBannerText(rawModalText) || (bannerStillVisible && !modalTextLooksLikeSessionDetail(rawModalText, session))) {
      console.log(`  [getSessionModalDetails ${label}] cookie overlay detected in modal text`);
      await closeModal(page, label);
      if (cookieRetryAllowed) {
        await dismissCookieBanner(page);
        await waitForCookieBannerGone(page, 6000);
        const retryOpen = await clickSessionTileForModal(page, session, label);
        modal = retryOpen.modal;
        if (modal) {
          try {
            const meta = await readModalText(page, modal);
            rawModalText = meta.text || '';
            capacityFromModal = meta.maxQty;
            cookieDismissDiagnostics.modalTextAfterCookieDismissSample = truncateDetailText(rawModalText, 300);
          } catch (pe) {
            console.log(`  [getSessionModalDetails ${label}] retry modal read failed: ${pe.message}`);
          }
        }
        if (!isCookieBannerText(rawModalText) && modalTextLooksLikeSessionDetail(rawModalText, session)) {
          console.log(`  [getSessionModalDetails ${label}] session modal recovered after cookie dismiss`);
        } else if (isCookieBannerText(rawModalText)) {
          return cookieOverlayFailure(rawTileText, rawModalText);
        } else if (!modal) {
          return cookieOverlayFailure(rawTileText, rawModalText || 'cookie dismissed but session modal missing');
        }
      } else {
        return cookieOverlayFailure(rawTileText, rawModalText);
      }
    }

    const modalParsed = parseDetailAvailabilityFromText(rawModalText);
    const priceInfo = parsePriceFromText(rawModalText || rawTileText);
    const modalValidation = validateModalAssociation(session, rawModalText);

    if (modalValidation.confidence === 'mismatch') {
      console.log(`  [getSessionModalDetails ${label}] modal mismatch — ${modalValidation.mismatches.map(m => m.field).join(', ')}`);
      await closeModal(page, label);
      await waitForModalGone(page, 3000);
      return {
        detailStatus: 'failed_modal_mismatch',
        failureType: 'failed_modal_mismatch',
        detailError: `modal_mismatch: ${modalValidation.mismatches.map(m => `${m.field} expected ${m.expected} got ${m.found}`).join('; ')}`,
        rawTileText,
        rawModalText: truncateDetailText(rawModalText, 1500),
        modalValidation,
        mismatches: modalValidation.mismatches,
        parseReason: 'failed_modal_mismatch',
        ...lifecycleMeta,
        modalMismatchReason: `modal_mismatch: ${modalValidation.mismatches.map(m => m.field).join(', ')}`,
      };
    }

    if (modalParsed?.packed) {
      const payload = wrapModalDetailPayload(session, rawModalText, rawTileText, buildDetailPayloadFromParsed(modalParsed, { rawTileText, rawModalText, priceInfo, modalValidation, session }));
      console.log(`  [getSessionModalDetails ${label}] packed from modal text`);
      await closeModal(page, label);
      await waitForModalGone(page, 3000);
      return payload;
    }
    if (modalParsed?.slots != null) {
      const payload = wrapModalDetailPayload(session, rawModalText, rawTileText, buildDetailPayloadFromParsed(modalParsed, { rawTileText, rawModalText, priceInfo, modalValidation, session }));
      console.log(`  [getSessionModalDetails ${label}] ${payload?.slots ?? 'n/a'} slot(s) from modal text`);
      await closeModal(page, label);
      await waitForModalGone(page, 3000);
      return payload;
    }
    if (modalParsed?.openNoCount) {
      const payload = wrapModalDetailPayload(session, rawModalText, rawTileText, buildDetailPayloadFromParsed(modalParsed, { rawTileText, rawModalText, priceInfo, modalValidation, session }));
      console.log(`  [getSessionModalDetails ${label}] open with no slot count visible`);
      await closeModal(page, label);
      await waitForModalGone(page, 3000);
      return payload;
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

    const payload = wrapModalDetailPayload(session, rawModalText, rawTileText, buildDetailPayloadFromParsed(null, {
      rawTileText,
      rawModalText,
      priceInfo,
      slotsFromClicks: n,
      modalValidation,
      session,
    }));

    await closeModal(page, label);
    await waitForModalGone(page, 3000);

    if (payload) {
      console.log(`  [getSessionModalDetails ${label}] result: ${payload.detailStatus} slots=${payload.slots ?? 'n/a'}`);
      return payload;
    }

    console.log(`  [getSessionModalDetails ${label}] could not parse slots from modal`);
    if (isCookieBannerText(rawModalText)) {
      return cookieOverlayFailure(rawTileText, rawModalText);
    }

    return {
      detailStatus: 'failed_parse',
      failureType: 'failed_parse',
      detailError: rawModalText && rawModalText.trim().length > 8
        ? 'modal opened but slot count could not be parsed'
        : 'modal opened with insufficient text to classify',
      rawTileText,
      rawModalText: truncateDetailText(rawModalText, 1500),
      parseReason: 'no_matching_patterns',
      ...buildParserOutputFromText(rawModalText),
    };

  } catch (e) {
    const isTimeout = /timeout|timed out/i.test(e.message);
    console.error(`  [getSessionModalDetails ${label}] ERROR: ${e.message}`);
    try { await closeModal(page, label); } catch (ce) {
      console.error(`  [getSessionModalDetails ${label}] close after error failed: ${ce.message}`);
    }
    return {
      detailStatus: isTimeout ? 'failed_timeout' : 'failed_parse',
      failureType: isTimeout ? 'failed_timeout' : 'failed_parse',
      detailError: e.message,
      rawTileText: ctx.tileText || null,
      rawModalText: null,
    };
  }
}

async function getSlotCount(page, ts, wave, session = null) {
  const s = session || { ts, wave, key: `${ts}_${wave}` };
  const details = await getSessionModalDetails(page, s);
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
    const isoDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(d);
    const displayDate = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', timeZone: 'America/New_York' });
    const weekday = d.toLocaleDateString('en-US', { weekday: 'long', timeZone: 'America/New_York' });
    const dayLabel = displayDate;
    const sideInfo = resolveWaveSide(el, wave, level, t);
    const td = el.closest('td');
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
      tileClassName: cls,
      tileColumnIndex: td ? td.cellIndex : null,
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

  lastWeeksScraped = Math.max(lastWeeksScraped ?? 0, weeksScraped);
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
  const tomorrowKey = addDaysToParkIso(todayKey, 1);
  const reasonScore = { watched: 0, opened: 1, deferred: 2, stale: 3, uncached: 4 };

  function dayScore(s) {
    const dk = s.dateKey || s.isoDate;
    if (dk === todayKey) return 0;
    if (dk === tomorrowKey) return 1;
    return 2;
  }

  return batch.sort((a, b) => {
    const ra = a._recheckReason || 'uncached';
    const rb = b._recheckReason || 'uncached';
    const rDiff = (reasonScore[ra] ?? 5) - (reasonScore[rb] ?? 5);
    if (rDiff) return rDiff;
    const dDiff = dayScore(a) - dayScore(b);
    if (dDiff) return dDiff;
    return a.ts - b.ts;
  });
}

async function fillSlotCounts(page, batch, byKey, prevByKey, stats, { networkCapture = null } = {}) {
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
    const dk = s.dateKey || s.isoDate;
    const highPriority = daysFromToday(dk) <= 1 || watchKeys.has(s.key);

    if (slotChecksThisCycle >= MAX_SLOT_CHECKS && !highPriority) {
      deferredThisCycle.add(s.key);
      const cached = slotCache[s.key];
      if (cached?.slots != null) entry.slots = cached.slots;
      continue;
    }

    const details = await getSessionDetailsWithFallback(page, s, networkCapture);
    const prior = { ...entry, ...(prevByKey.get(s.key) || {}) };
    try {
      if (details && !isDetailFailureStatus(normalizeDetailStatus(details.detailStatus || details.failureType))) {
        applyDetailPayloadToSession(entry, details, s.level, { prior });
      } else {
        applyDetailFailureToSession(entry, details || {
          detailStatus: 'failed_parse',
          detailError: 'no_details',
        }, { prior });
      }
    } catch (slotErr) {
      console.error(`  slot check failed ${s.key}: ${slotErr.message}`);
      applyDetailFailureToSession(entry, {
        detailStatus: /timeout|timed out/i.test(slotErr.message) ? 'failed_timeout' : 'failed_parse',
        detailError: slotErr.message,
      }, { prior });
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

function getParkTodayIso() {
  return todayDateKey();
}

function addDaysToParkIso(isoDate, days) {
  const [y, m, d] = String(isoDate).slice(0, 10).split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d + days, 12, 0, 0));
  return dateKeyInBookingTz(dt);
}

function getRelativeDateLabel(isoDate) {
  if (!isoDate) return '';
  const today = getParkTodayIso();
  if (isoDate === today) return 'Today';
  if (isoDate === addDaysToParkIso(today, 1)) return 'Tomorrow';
  const [y, mo, da] = isoDate.split('-').map(Number);
  const ref = new Date(Date.UTC(y, mo - 1, da, 17, 0, 0));
  return new Intl.DateTimeFormat('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    timeZone: BOOKING_TZ,
  }).format(ref);
}

function isSessionPast(session, now = new Date()) {
  const ts = session?.start_ts ?? session?.ts ?? session?.startTs;
  if (ts == null) return false;
  const startMs = Number(ts) * 1000;
  if (!Number.isFinite(startMs)) return false;
  const durationMs = (session?.durationMinutes || 90) * 60 * 1000;
  return now.getTime() > startMs + durationMs;
}

function normalizeSessionDate(session) {
  if (!session) return session;
  const iso = sessionDateKey(session) || isoDateFromRow(session) || null;
  return {
    ...session,
    isoDate: iso || session.isoDate || session.iso_date || null,
    dateKey: iso || session.dateKey || session.iso_date || null,
  };
}

function isWatchItemPast(w, now = new Date()) {
  const ts = w?.start_ts ?? w?.startTs;
  if (ts == null) return false;
  return isSessionPast({ ts, durationMinutes: 90 }, now);
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

function maxHorizonDateKey(maxHorizonDays = MAX_BOOKING_HORIZON_DAYS) {
  const today = getParkTodayIso();
  const d = parseDateKey(today);
  d.setDate(d.getDate() + maxHorizonDays);
  return dateKeyFromDate(d);
}

function getDiscoveredAvailableDates() {
  return collectorState.discoveredAvailableDates || [];
}

function expectedAvailableBookingDates() {
  const discovered = getDiscoveredAvailableDates();
  if (discovered.length) return discovered;
  const today = getParkTodayIso();
  const maxDate = maxHorizonDateKey();
  return [...new Set(
    allStoredSessions()
      .map(sessionDateKey)
      .filter(d => d && d >= today && d <= maxDate),
  )].sort();
}

function clampDateRangeToHorizon(startDate, endDate, maxHorizonDays = MAX_BOOKING_HORIZON_DAYS) {
  const today = getParkTodayIso();
  const maxDate = maxHorizonDateKey(maxHorizonDays);
  const start = startDate < today ? today : startDate;
  const finish = endDate > maxDate ? maxDate : endDate;
  if (start > finish) return { start: null, end: null, dates: [] };
  return { start, end: finish, dates: enumerateDateKeys(start, finish) };
}

function clampDateRangeToBookingWindow(startDate, endDate) {
  return clampDateRangeToHorizon(startDate, endDate);
}

function sessionInDateRange(s, startDate, endDate) {
  const dk = sessionDateKey(s);
  if (!dk) return false;
  return dk >= startDate && dk <= endDate;
}

function recordDateCoverageAttempt(isoDate, attempt) {
  if (!isoDate || !attempt) return;
  collectorState.dateCoverageAttempts[isoDate] = {
    ...attempt,
    recordedAt: new Date().toISOString(),
  };
  if (attempt.targetDateVisible != null || attempt.navigationError != null) {
    collectorState.dateNavigationByDate[isoDate] = attempt;
  }
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
    const fmt = (ts) => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date(ts * 1000));
    document.querySelectorAll('div.dynamic-cal-booking-ts').forEach(el => {
      const m = el.className.match(/booking-agenda-clickable_(\d+)_/);
      if (m) keys.add(fmt(+m[1]));
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

function groupSessionsByIsoDate(sessions) {
  const groups = new Map();
  for (const s of asSessionArray(sessions)) {
    const dk = s.isoDate || s.dateKey;
    if (!dk) continue;
    if (!groups.has(dk)) groups.set(dk, []);
    groups.get(dk).push(s);
  }
  return [...groups.entries()].sort((a, b) => a[0].localeCompare(b[0]));
}

async function navigateCalendarToShowDate(page, targetIsoDate) {
  const diag = {
    targetIsoDate: targetIsoDate || null,
    visibleWeekStart: null,
    visibleWeekEnd: null,
    visibleDateLabels: [],
    clickedNextWeekCount: 0,
    targetDateVisible: false,
    navigationError: null,
  };

  if (!targetIsoDate) {
    diag.navigationError = 'missing_target_iso_date';
    return diag;
  }

  let visible = await getVisibleDateKeysFromPage(page);
  diag.visibleDateLabels = visible;
  if (visible.length) {
    diag.visibleWeekStart = visible[0];
    diag.visibleWeekEnd = visible[visible.length - 1];
  }
  if (visible.includes(targetIsoDate)) {
    diag.targetDateVisible = true;
    return diag;
  }

  await openBookingPage(page);
  await dismissCookieBanner(page);
  visible = await getVisibleDateKeysFromPage(page);
  diag.visibleDateLabels = visible;
  if (visible.length) {
    diag.visibleWeekStart = visible[0];
    diag.visibleWeekEnd = visible[visible.length - 1];
  }
  if (visible.includes(targetIsoDate)) {
    diag.targetDateVisible = true;
    return diag;
  }

  const maxSteps = effectiveWeeksAhead + 3;
  for (let step = 0; step < maxSteps; step++) {
    if (!await advanceCalendarWeek(page)) break;
    diag.clickedNextWeekCount++;
    visible = await getVisibleDateKeysFromPage(page);
    diag.visibleDateLabels = visible;
    if (visible.length) {
      diag.visibleWeekStart = visible[0];
      diag.visibleWeekEnd = visible[visible.length - 1];
    }
    if (visible.includes(targetIsoDate)) {
      diag.targetDateVisible = true;
      return diag;
    }
  }

  for (let step = 0; step < maxSteps; step++) {
    if (!await retreatCalendarWeek(page)) break;
    visible = await getVisibleDateKeysFromPage(page);
    diag.visibleDateLabels = visible;
    if (visible.length) {
      diag.visibleWeekStart = visible[0];
      diag.visibleWeekEnd = visible[visible.length - 1];
    }
    if (visible.includes(targetIsoDate)) {
      diag.targetDateVisible = true;
      return diag;
    }
  }

  diag.navigationError = 'target_date_not_visible_after_navigation';
  return diag;
}

async function scrapeBasicSessionsForDates(page, dates, { tier = 0, sourceLabel = 'date_scrape' } = {}) {
  const allByKey = new Map();
  const dateResults = [];
  const datesSeen = new Set();

  for (const targetIsoDate of asSessionArray(dates)) {
    const navDiag = await navigateCalendarToShowDate(page, targetIsoDate);
    const dateResult = {
      targetIsoDate,
      visibleWeekStart: navDiag.visibleWeekStart,
      visibleWeekEnd: navDiag.visibleWeekEnd,
      visibleDateLabels: navDiag.visibleDateLabels,
      clickedNextWeekCount: navDiag.clickedNextWeekCount,
      targetDateVisible: navDiag.targetDateVisible,
      navigationError: navDiag.navigationError,
      sessionsFound: 0,
      rowsUpserted: 0,
      detailRowsVerified: 0,
      detailRowsSuppressed: 0,
      failureReason: null,
    };

    if (!navDiag.targetDateVisible) {
      dateResult.failureReason = navDiag.navigationError || 'target_date_not_visible';
      persistedDatesChecked.add(targetIsoDate);
      datesCheckedEmpty.add(targetIsoDate);
      recordDateCoverageAttempt(targetIsoDate, dateResult);
      dateResults.push(dateResult);
      console.log(`  [${sourceLabel}] ${targetIsoDate} skipped — calendar navigation failed`);
      continue;
    }

    await dismissCookieBanner(page).catch(() => {});
    const { pageSessions, visible, rawCount } = await absorbVisibleSessions(page, allByKey, datesSeen, 0);
    const onDate = [...allByKey.values()].filter(s => sessionDateKey(s) === targetIsoDate);
    dateResult.sessionsFound = onDate.length;
    dateResult.rawTilesOnPage = rawCount;
    dateResult.visibleDateLabels = visible.length ? visible : dateResult.visibleDateLabels;

    persistedDatesChecked.add(targetIsoDate);
    if (onDate.length === 0) {
      datesCheckedEmpty.add(targetIsoDate);
      dateResult.failureReason = 'checked_empty_no_sessions_on_site';
    } else {
      datesCheckedEmpty.delete(targetIsoDate);
    }

    recordDateCoverageAttempt(targetIsoDate, dateResult);
    dateResults.push(dateResult);
    console.log(`  [${sourceLabel}] ${targetIsoDate}: ${onDate.length} session(s), visible=${visible.join(', ')}`);
  }

  const batch = [...allByKey.values()].map(s => ({
    ...s,
    tier: tier || s.tier || 2,
    detailStatus: s.detailStatus || 'pending',
  }));

  return { batch, dateResults, datesSeen, sessionsFound: batch.length };
}

async function discoverAvailableBookingDates(page, { maxHorizonDays = MAX_BOOKING_HORIZON_DAYS } = {}) {
  const today = getParkTodayIso();
  const maxDate = maxHorizonDateKey(maxHorizonDays);
  const calendarDatesDiscovered = new Set();
  const discoveryDiagnostics = {
    parkTodayIso: today,
    maxHorizonDays,
    maxHorizonDate: maxDate,
    weeksScanned: 0,
    stoppedReason: null,
    lastVisibleWeekStart: null,
    lastVisibleWeekEnd: null,
  };

  const nav = await navigateCalendarToShowDate(page, today);
  if (!nav.targetDateVisible) {
    console.log(`  [discover] warning: today not visible (${nav.navigationError || 'unknown'}) — scanning from current week`);
  }
  await dismissCookieBanner(page).catch(() => {});

  const seenSigs = new Set();
  for (let guard = 0; guard < 52; guard++) {
    const visible = await getVisibleDateKeysFromPage(page);
    const fp = await getCalendarFingerprint(page);

    if (seenSigs.has(fp.sig) && fp.count > 0) {
      discoveryDiagnostics.stoppedReason = 'repeated_calendar_fingerprint';
      break;
    }
    seenSigs.add(fp.sig);
    discoveryDiagnostics.weeksScanned++;
    if (visible.length) {
      discoveryDiagnostics.lastVisibleWeekStart = visible[0];
      discoveryDiagnostics.lastVisibleWeekEnd = visible[visible.length - 1];
    }

    for (const d of visible) {
      if (d >= today && d <= maxDate) calendarDatesDiscovered.add(d);
    }

    if (visible.length && visible[visible.length - 1] >= maxDate) {
      discoveryDiagnostics.stoppedReason = 'max_horizon_reached';
      break;
    }

    if (!await canAdvanceCalendar(page)) {
      discoveryDiagnostics.stoppedReason = 'calendar_end_no_next_arrow';
      break;
    }
    if (!await advanceCalendarWeek(page)) {
      discoveryDiagnostics.stoppedReason = 'advance_failed';
      break;
    }
    await dismissCookieBanner(page).catch(() => {});
  }

  const dates = [...calendarDatesDiscovered].sort();
  collectorState.discoveredAvailableDates = dates;
  collectorState.lastDiscoveryAt = new Date().toISOString();
  collectorState.discoveryDiagnostics = discoveryDiagnostics;

  console.log(`  [discover] ${dates.length} calendar date(s) from ${today} through ${dates[dates.length - 1] || 'none'} (${discoveryDiagnostics.stoppedReason || 'done'})`);
  return { dates, diagnostics: discoveryDiagnostics };
}

async function scrapeAllAvailableBookingDates(page, {
  maxHorizonDays = MAX_BOOKING_HORIZON_DAYS,
  tier = 2,
  sourceLabel = 'available_dates_scrape',
  minDay = 0,
} = {}) {
  const today = getParkTodayIso();
  const maxDate = maxHorizonDateKey(maxHorizonDays);
  const allByKey = new Map();
  const datesSeen = new Set();
  const calendarDatesDiscovered = new Set();
  const discoveryDiagnostics = {
    parkTodayIso: today,
    maxHorizonDays,
    maxHorizonDate: maxDate,
    weeksScanned: 0,
    stoppedReason: null,
    lastVisibleWeekStart: null,
    lastVisibleWeekEnd: null,
  };

  const nav = await navigateCalendarToShowDate(page, today);
  if (!nav.targetDateVisible) {
    console.log(`  [${sourceLabel}] warning: today not visible (${nav.navigationError || 'unknown'}) — scraping from current week`);
  }
  await dismissCookieBanner(page).catch(() => {});

  const seenSigs = new Set();
  for (let guard = 0; guard < 52; guard++) {
    const { visible } = await absorbVisibleSessions(page, allByKey, datesSeen, discoveryDiagnostics.weeksScanned);
    const fp = await getCalendarFingerprint(page);

    if (seenSigs.has(fp.sig) && fp.count > 0) {
      discoveryDiagnostics.stoppedReason = 'repeated_calendar_fingerprint';
      break;
    }
    seenSigs.add(fp.sig);
    discoveryDiagnostics.weeksScanned++;
    if (visible.length) {
      discoveryDiagnostics.lastVisibleWeekStart = visible[0];
      discoveryDiagnostics.lastVisibleWeekEnd = visible[visible.length - 1];
    }

    for (const d of visible) {
      if (d >= today && d <= maxDate) calendarDatesDiscovered.add(d);
    }

    if (visible.length && visible[visible.length - 1] >= maxDate) {
      discoveryDiagnostics.stoppedReason = 'max_horizon_reached';
      break;
    }

    if (!await canAdvanceCalendar(page)) {
      discoveryDiagnostics.stoppedReason = 'calendar_end_no_next_arrow';
      break;
    }
    if (!await advanceCalendarWeek(page)) {
      discoveryDiagnostics.stoppedReason = 'advance_failed';
      break;
    }
    await dismissCookieBanner(page).catch(() => {});
  }

  const discoveredDates = [...calendarDatesDiscovered].sort();
  collectorState.discoveredAvailableDates = discoveredDates;
  collectorState.lastDiscoveryAt = new Date().toISOString();
  collectorState.discoveryDiagnostics = discoveryDiagnostics;

  const batch = [...allByKey.values()]
    .filter(s => {
      const dk = sessionDateKey(s);
      return dk && dk >= today && dk <= maxDate && daysFromToday(dk) >= minDay;
    })
    .map(s => ({
      ...s,
      tier: tier || s.tier || 2,
      detailStatus: s.detailStatus || 'pending',
    }));

  const dateResults = discoveredDates
    .filter(d => daysFromToday(d) >= minDay)
    .map((isoDate) => {
      const onDate = batch.filter(s => sessionDateKey(s) === isoDate);
      const result = {
        isoDate,
        targetDateVisible: true,
        sessionsFound: onDate.length,
        rowsUpserted: 0,
        snapshotsInserted: 0,
        verifiedDetailsWritten: 0,
        detailValuesSuppressed: 0,
        failureReason: onDate.length === 0 ? 'checked_empty_no_sessions_on_site' : null,
      };
      persistedDatesChecked.add(isoDate);
      if (onDate.length === 0) datesCheckedEmpty.add(isoDate);
      else datesCheckedEmpty.delete(isoDate);
      recordDateCoverageAttempt(isoDate, result);
      return result;
    });

  console.log(
    `  [${sourceLabel}] discovered ${discoveredDates.length} date(s), ${batch.length} session row(s) (${discoveryDiagnostics.stoppedReason || 'done'})`,
  );

  return {
    batch,
    dateResults,
    discoveredDates,
    datesSeen,
    discoveryDiagnostics,
    sessionsFound: batch.length,
  };
}

async function runBackfillAvailableDates({
  mode = 'both',
  maxHorizonDays,
  reason = 'admin_backfill_available_dates',
} = {}) {
  const horizon = Number.isFinite(Number(maxHorizonDays)) && Number(maxHorizonDays) > 0
    ? Math.min(Number(maxHorizonDays), MAX_BOOKING_HORIZON_DAYS * 2)
    : MAX_BOOKING_HORIZON_DAYS;

  const report = {
    mode,
    maxHorizonDays: horizon,
    discoveredAvailableDates: [],
    dateResults: [],
    sessionsFound: 0,
    rowsUpserted: 0,
    snapshotsInserted: 0,
    verifiedDetailsWritten: 0,
    detailValuesSuppressed: 0,
    discoveryDiagnostics: null,
    detail: null,
    durationMs: 0,
    skipped: false,
    skipReason: null,
    errors: [],
  };

  const started = Date.now();
  let launched = null;

  try {
    if (!tryAcquireScrapeLock(`available dates backfill (${reason})`, 0)) {
      report.skipped = true;
      report.skipReason = 'scrape_in_progress';
      return report;
    }

    if (mode === 'basic_only' || mode === 'both') {
      launched = await launchBrowser();
      const { page } = launched;
      await openBookingPage(page);

      const scrape = await scrapeAllAvailableBookingDates(page, {
        maxHorizonDays: horizon,
        tier: 2,
        sourceLabel: reason,
        minDay: 0,
      });

      report.discoveredAvailableDates = scrape.discoveredDates;
      report.discoveryDiagnostics = scrape.discoveryDiagnostics;
      report.sessionsFound = scrape.sessionsFound;
      report.dateResults = scrape.dateResults;

      if (scrape.batch.length) {
        mergeBatchIntoStore(scrape.batch, 2, { preserveSlots: true, scrapeKind: 'basic' });
        recordTierDateCoverage(new Set(scrape.discoveredDates));
        const writeDiag = await persistTierScrapeResults(scrape.batch, 2, { slotCountsAttempted: false });
        report.rowsUpserted = writeDiag.rowsUpserted;
        report.snapshotsInserted = writeDiag.snapshotsInserted;
        if (writeDiag.upsertError) report.errors.push({ error: writeDiag.upsertError, phase: 'upsert' });

        for (const dr of report.dateResults) {
          dr.rowsUpserted = scrape.batch.filter(s => sessionDateKey(s) === dr.isoDate).length;
          dr.snapshotsInserted = dr.rowsUpserted;
          recordDateCoverageAttempt(dr.isoDate, dr);
        }
      }

      await saveLatestSnapshotToSupabase();
    }

    if (mode === 'verified_detail' || mode === 'both') {
      await ensureSessionsForStatus();
      if (!report.discoveredAvailableDates.length) {
        report.discoveredAvailableDates = getDiscoveredAvailableDates();
      }
      if (!report.discoveredAvailableDates.length) {
        const today = getParkTodayIso();
        report.discoveredAvailableDates = [...new Set(
          allStoredSessions()
            .map(sessionDateKey)
            .filter(d => d && d >= today),
        )].sort();
      }

      const targets = report.discoveredAvailableDates.flatMap(d =>
        sessionsForDate(d).filter(s => s.available !== false),
      );

      if (!targets.length) {
        if (mode === 'verified_detail') {
          report.skipped = true;
          report.skipReason = 'no_open_sessions_discovered';
        }
      } else {
        const enrichStats = await runDetailEnrichment({
          sessions: targets,
          reason: `${reason}_detail`,
        });
        report.verifiedDetailsWritten = enrichStats.detailRowsVerified ?? 0;
        report.detailValuesSuppressed = enrichStats.detailRowsSuppressed ?? 0;
        report.detail = enrichStats;
        if (enrichStats.errors?.length) report.errors.push(...enrichStats.errors.slice(0, 5));

        if (!report.dateResults.length) {
          report.dateResults = report.discoveredAvailableDates.map((isoDate) => {
            const rows = sessionsForDate(isoDate);
            return {
              isoDate,
              targetDateVisible: true,
              sessionsFound: rows.length,
              rowsUpserted: rows.length,
              snapshotsInserted: 0,
              verifiedDetailsWritten: rows.filter(s => sessionDetailVerified(s)).length,
              detailValuesSuppressed: rows.filter(s => {
                const st = effectiveDetailStatus(s);
                return isDetailFailureStatus(st) || isInvalidCheckedWithSlotsStatus(s);
              }).length,
              failureReason: rows.length === 0 ? 'checked_empty_no_sessions_on_site' : null,
            };
          });
        } else {
          for (const dr of report.dateResults) {
            const rows = sessionsForDate(dr.isoDate);
            dr.verifiedDetailsWritten = rows.filter(s => sessionDetailVerified(s)).length;
            dr.detailValuesSuppressed = rows.filter(s => {
              const st = effectiveDetailStatus(s);
              return isDetailFailureStatus(st) || isInvalidCheckedWithSlotsStatus(s);
            }).length;
            recordDateCoverageAttempt(dr.isoDate, dr);
          }
        }
      }
    }

    await refreshCoverageFlags();
    report.durationMs = Date.now() - started;
    collectorState.lastBackfillAvailableDatesResult = { ...report, completedAt: new Date().toISOString() };
    return report;
  } catch (e) {
    report.errors.push({ error: e.message });
    report.durationMs = Date.now() - started;
    throw e;
  } finally {
    releaseScrapeLock();
    if (launched?.browser) await launched.browser.close().catch(() => {});
  }
}

async function runDateRangeBackfillBasic(startDate, endDate, { tier = 0, reason = 'backfill_date_range' } = {}) {
  const { start, end, dates } = clampDateRangeToBookingWindow(startDate, endDate);
  const report = {
    mode: 'basic_only',
    startDate: start,
    endDate: end,
    datesRequested: dates.length,
    dateResults: [],
    sessionsFound: 0,
    rowsUpserted: 0,
    snapshotsInserted: 0,
    upsertError: null,
    errors: [],
    durationMs: 0,
    skipped: false,
    skipReason: null,
  };

  if (!dates.length) {
    report.skipped = true;
    report.skipReason = 'empty_or_invalid_date_range';
    return report;
  }

  const started = Date.now();
  let launched = null;
  try {
    if (!tryAcquireScrapeLock(`date range backfill (${reason})`, 0)) {
      report.skipped = true;
      report.skipReason = 'scrape_in_progress';
      return report;
    }

    launched = await launchBrowser();
    const { page } = launched;
    await openBookingPage(page);

    const { batch, dateResults, sessionsFound } = await scrapeBasicSessionsForDates(page, dates, {
      tier,
      sourceLabel: reason,
    });
    report.dateResults = dateResults;
    report.sessionsFound = sessionsFound;

    if (batch.length) {
      mergeBatchIntoStore(batch, tier || 2, { preserveSlots: true, scrapeKind: 'basic' });
      recordTierDateCoverage(new Set(dates));
      const writeDiag = await persistTierScrapeResults(batch, tier || 2, { slotCountsAttempted: false });
      report.rowsUpserted = writeDiag.rowsUpserted;
      report.snapshotsInserted = writeDiag.snapshotsInserted;
      report.upsertError = writeDiag.upsertError;
      if (writeDiag.upsertError) report.errors.push({ error: writeDiag.upsertError, phase: 'upsert' });

      for (const dr of dateResults) {
        dr.rowsUpserted = batch.filter(s => sessionDateKey(s) === dr.targetIsoDate).length;
        recordDateCoverageAttempt(dr.targetIsoDate, dr);
      }
    }

    await saveLatestSnapshotToSupabase();
    await refreshCoverageFlags();
    report.durationMs = Date.now() - started;
    collectorState.lastDateRangeBackfill = { ...report, completedAt: new Date().toISOString() };
    return report;
  } catch (e) {
    report.errors.push({ error: e.message });
    report.durationMs = Date.now() - started;
    throw e;
  } finally {
    releaseScrapeLock();
    if (launched?.browser) await launched.browser.close().catch(() => {});
  }
}

async function runDateRangeBackfillVerifiedDetail(startDate, endDate, { reason = 'backfill_verified_detail' } = {}) {
  const { start, end, dates } = clampDateRangeToBookingWindow(startDate, endDate);
  const report = {
    mode: 'verified_detail',
    startDate: start,
    endDate: end,
    datesRequested: dates.length,
    sessionsAttempted: 0,
    detailRowsVerified: 0,
    detailRowsSuppressed: 0,
    sessionsUpdatedWithSlots: 0,
    errors: [],
    durationMs: 0,
    skipped: false,
    skipReason: null,
  };

  if (!dates.length) {
    report.skipped = true;
    report.skipReason = 'empty_or_invalid_date_range';
    return report;
  }

  const started = Date.now();
  await ensureSessionsForStatus();
  const targets = dates.flatMap(d => sessionsForDate(d).filter(s => s.available !== false));
  report.sessionsQueued = targets.length;

  if (!targets.length) {
    report.skipped = true;
    report.skipReason = 'no_open_sessions_in_range';
    report.durationMs = Date.now() - started;
    return report;
  }

  const enrichStats = await runDetailEnrichment({
    sessions: targets,
    reason,
  });

  report.sessionsAttempted = enrichStats.sessionsAttempted ?? 0;
  report.detailRowsVerified = enrichStats.detailRowsVerified ?? 0;
  report.detailRowsSuppressed = enrichStats.detailRowsSuppressed ?? 0;
  report.sessionsUpdatedWithSlots = enrichStats.sessionsUpdatedWithSlots ?? 0;
  report.errors = enrichStats.errors ?? [];
  report.durationMs = Date.now() - started;
  report.enrichmentStats = enrichStats;
  return report;
}

async function runDateRangeBackfill({ startDate, endDate, mode = 'both', reason = 'admin_backfill_date_range' } = {}) {
  const { start, end, dates } = clampDateRangeToBookingWindow(startDate, endDate);
  const combined = {
    mode,
    startDate: start,
    endDate: end,
    datesRequested: dates?.length || 0,
    basic: null,
    detail: null,
    durationMs: 0,
    skipped: false,
    skipReason: null,
  };

  if (!dates?.length) {
    combined.skipped = true;
    combined.skipReason = 'empty_or_invalid_date_range';
    return combined;
  }

  const started = Date.now();
  if (mode === 'basic_only' || mode === 'both') {
    combined.basic = await runDateRangeBackfillBasic(start, end, { reason: `${reason}_basic` });
    if (combined.basic.skipped && mode === 'basic_only') {
      combined.skipped = combined.basic.skipped;
      combined.skipReason = combined.basic.skipReason;
    }
  }

  if (mode === 'verified_detail' || mode === 'both') {
    combined.detail = await runDateRangeBackfillVerifiedDetail(start, end, {
      reason: `${reason}_detail`,
    });
  }

  combined.durationMs = Date.now() - started;
  collectorState.lastDateRangeBackfill = { ...combined, completedAt: new Date().toISOString() };
  return combined;
}

async function navigateToWeekOffset(page, weekOffset) {
  await openBookingPage(page);
  await dismissCookieBanner(page);
  await waitForCookieBannerGone(page, 4000);
  for (let w = 0; w < weekOffset; w++) {
    if (!await advanceCalendarWeek(page)) return false;
    await dismissCookieBanner(page).catch(() => {});
  }
  return true;
}

async function fillMissingDates(page, missingDates, allByKey, datesSeen) {
  if (!missingDates.length) return;
  console.log(`  filling ${missingDates.length} missing date(s): ${missingDates.join(', ')}`);

  for (const dateKey of missingDates) {
    if (datesSeen.has(dateKey)) continue;

    const navDiag = await navigateCalendarToShowDate(page, dateKey);
    recordDateCoverageAttempt(dateKey, {
      targetIsoDate: dateKey,
      ...navDiag,
      sessionsFound: 0,
      failureReason: navDiag.targetDateVisible ? null : (navDiag.navigationError || 'target_date_not_visible'),
    });

    if (!navDiag.targetDateVisible) {
      console.log(`  ⚠ ${dateKey} not visible after navigation (${navDiag.navigationError || 'unknown'})`);
      datesSeen.add(dateKey);
      persistedDatesChecked.add(dateKey);
      datesCheckedEmpty.add(dateKey);
      datesCheckedDuringScrape.add(dateKey);
      continue;
    }

    const localSeen = new Set(datesSeen);
    const { pageSessions, visible } = await absorbVisibleSessions(page, allByKey, localSeen, 0);
    for (const d of localSeen) datesSeen.add(d);
    const count = [...allByKey.values()].filter(s => sessionDateKey(s) === dateKey).length;
    if (count > 0 || visible.includes(dateKey)) {
      console.log(`  ✓ ${dateKey} found after targeted navigation (${count} session(s))`);
      if (count === 0) datesCheckedEmpty.add(dateKey);
      else datesCheckedEmpty.delete(dateKey);
    } else {
      console.log(`  ⚠ ${dateKey} visible on calendar but no sessions parsed`);
      datesCheckedEmpty.add(dateKey);
    }
    recordDateCoverageAttempt(dateKey, {
      targetIsoDate: dateKey,
      ...navDiag,
      sessionsFound: count,
      targetDateVisible: true,
    });
    datesSeen.add(dateKey);
    persistedDatesChecked.add(dateKey);
    datesCheckedDuringScrape.add(dateKey);
  }
}

function tierMaxDay(tier) {
  const cfg = TIER_CONFIG[tier];
  if (cfg.maxDay != null) return cfg.maxDay;
  if (tier === 2) {
    const discovered = getDiscoveredAvailableDates();
    if (discovered.length) {
      const last = discovered[discovered.length - 1];
      const endDays = daysFromToday(last);
      return Math.max(6, endDays >= 0 ? endDays : 0);
    }
    return Math.max(6, Math.min(MAX_BOOKING_HORIZON_DAYS, effectiveWeeksAhead * 7 - 1));
  }
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
  if (tier === 1 && effectiveWeeksAhead > 1) {
    endWeek = Math.max(endWeek, Math.min(1, effectiveWeeksAhead - 1));
  }
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
    weeksScraped: lastWeeksScraped ?? 0,
  };
}

function filterBatchForTier(batch, tier) {
  return asSessionArray(batch).filter(s => sessionInTier(s, tier));
}

function rebuildSessionsArray() {
  sessions = allStoredSessions().sort((a, b) => a.ts - b.ts || a.wave - b.wave);
}

function mergeBatchIntoStore(batch, tier, { preserveSlots = true, scrapeKind = null } = {}) {
  const now = new Date().toISOString();
  const updatedKeys = [];
  const cfg = TIER_CONFIG[tier];

  for (const raw of asSessionArray(batch)) {
    if (!sessionInTier(raw, tier)) continue;
    const existing = sessionsByKey.get(raw.key);
    const kind = scrapeKind || (cfg?.slotCounts && sessionHasDetailedData(raw) ? 'detailed' : 'basic');
    const merged = mergeSessionFieldsForUpsert(
      { ...(existing || {}), ...raw, tier, lastScraped: now },
      existing,
      { scrapeKind: kind },
    );
    if (existing && existing.available !== merged.available) {
      sessionsNeedingDetailAfterBasic.add(raw.key);
    } else if (existing && existing.available && merged.available
      && existing.slots != null && merged.slots != null && existing.slots !== merged.slots) {
      sessionsNeedingDetailAfterBasic.add(raw.key);
    }
    sessionsByKey.set(raw.key, merged);
    updatedKeys.push(raw.key);
    logWaveSideParse(merged);
  }

  rebuildSessionsArray();
  return updatedKeys;
}

async function configurePageForSpeed(page) {
  await page.route('**/*', (route) => {
    const type = route.request().resourceType();
    if (['image', 'font', 'media'].includes(type)) {
      return route.abort();
    }
    return route.continue();
  });
}

async function launchBrowser({ blockHeavyAssets = false } = {}) {
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
  });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125 Safari/537.36',
    timezoneId: BOOKING_TZ,
  });
  await setupBookingBrowserContext(context);
  const page = await context.newPage();
  if (blockHeavyAssets) await configurePageForSpeed(page);
  return { browser, page, context };
}

async function acquireEnrichmentBrowser() {
  if (enrichmentBrowserPool?.browser?.isConnected?.()) {
    enrichmentBrowserLastUsed = Date.now();
    if (!enrichmentNetworkCapture) {
      enrichmentNetworkCapture = installBookingNetworkCapture(enrichmentBrowserPool.page);
    }
    return enrichmentBrowserPool;
  }
  if (enrichmentBrowserPool?.browser) {
    await enrichmentBrowserPool.browser.close().catch(() => {});
  }
  const launched = await launchBrowser({ blockHeavyAssets: true });
  enrichmentBrowserPool = launched;
  enrichmentBrowserLastUsed = Date.now();
  enrichmentNetworkCapture = installBookingNetworkCapture(launched.page);
  return enrichmentBrowserPool;
}

async function releaseEnrichmentBrowserPool() {
  if (!enrichmentBrowserPool?.browser) return;
  try {
    await enrichmentBrowserPool.browser.close();
  } catch {}
  enrichmentBrowserPool = null;
  enrichmentNetworkCapture = null;
}

function scheduleEnrichmentBrowserIdleClose() {
  setTimeout(async () => {
    if (!enrichmentBrowserPool) return;
    if (Date.now() - enrichmentBrowserLastUsed >= ENRICHMENT_BROWSER_IDLE_MS) {
      await releaseEnrichmentBrowserPool();
    }
  }, ENRICHMENT_BROWSER_IDLE_MS + 1000);
}

async function buildEnrichmentDebugPayload() {
  await refreshEnrichmentQueueCounts();
  const all = allStoredSessions();
  const stale = all.filter(s => sessionQualifiesForDetailEnrichment(s));
  const missingSlots = all.filter(s => s.available && s.slots == null);
  const missingPrice = all.filter(s => s.available && !s.priceText && s.priceMin == null);
  const stats = detailCoverageStats();

  return {
    queuePending: enrichmentQueuePendingCount,
    queueRunning: enrichmentQueueRunningCount,
    lastDetailEnrichmentAt: lastDetailEnrichmentAt,
    lastDetailEnrichmentError: lastDetailEnrichmentError,
    detailEnrichmentInProgress,
    sessionsNeedingDetail: stale.length,
    sessionsMissingSlots: missingSlots.length,
    sessionsMissingPrice: missingPrice.length,
    sessionsWithStaleDetails: stale.length,
    detailCoveragePercent: stats.detailCoveragePercent,
    sessionsWithSlotsCount: stats.sessionsWithSlotsCount,
    sessionsWithCapacityCount: stats.sessionsWithCapacityCount,
    sessionsWithPriceCount: stats.sessionsWithPriceCount,
    lastRunDurationMs: enrichmentMetrics.lastDurationMs,
    averageEnrichmentDurationMs: enrichmentMetrics.averageDurationMs,
    runsCompleted: enrichmentMetrics.runsCompleted,
    lastRunStats: enrichmentMetrics.lastRunStats,
    recentErrors: enrichmentMetrics.recentErrors.slice(-10),
    cookieDismissAttempted: cookieDismissDiagnostics.cookieDismissAttempted,
    cookieDismissSucceeded: cookieDismissDiagnostics.cookieDismissSucceeded,
    cookieBannerStillVisible: cookieDismissDiagnostics.cookieBannerStillVisible,
    cookieClickMethod: cookieDismissDiagnostics.cookieClickMethod,
    modalTextAfterCookieDismissSample: cookieDismissDiagnostics.modalTextAfterCookieDismissSample,
    cookieDismissLastAttempt: cookieDismissDiagnostics.lastAttempt,
    cookieDismissAttempts: cookieDismissDiagnostics.attempts,
    enrichmentBrowserActive: !!enrichmentBrowserPool?.browser?.isConnected?.(),
    prioritySchedule: {
      p1: { everyMinutes: CHECK_MINS, staleHours: detailStaleMaxAgeHours(1) },
      p2: { everyMinutes: ENRICHMENT_TIER2_EVERY_MINS, staleHours: detailStaleMaxAgeHours(2) },
      p3: { everyHours: ENRICHMENT_TIER3_STALE_HOURS, tier3ScrapeEveryHours: 6 },
    },
    pendingAfterBasicChange: sessionsNeedingDetailAfterBasic.size,
  };
}

async function openBookingPage(page) {
  await dismissCookieBanner(page).catch(() => {});
  await page.goto(BOOKING, { waitUntil: 'networkidle', timeout: 30_000 });
  await page.waitForSelector('.dynamic-cal-booking-ts', { timeout: 15_000 });
  await dismissCookieBanner(page);
  await waitForCookieBannerGone(page, 6000);
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
  effectiveWeeksAhead = Math.max(1, Math.min(SCRAPE_WEEKS_AHEAD, detectedWeeks || SCRAPE_WEEKS_AHEAD));
  console.log(`  booking calendar: ${detectedWeeks} week(s) available on site`);
  console.log(`  SCRAPE_WEEKS_AHEAD=${SCRAPE_WEEKS_AHEAD} → effective lookahead ${effectiveWeeksAhead} week(s)`);
  if (effectiveWeeksAhead < SCRAPE_WEEKS_AHEAD) {
    console.log(`  capped SCRAPE_WEEKS_AHEAD at ${effectiveWeeksAhead} — site does not expose further weeks`);
  }
}


async function runTierScrape(tier, { reason = 'manual' } = {}) {
  const targetDates = tierTargetDates(tier);
  const report = {
    tier,
    reason,
    targetDates,
    started: true,
    completed: false,
    skipped: false,
    skipReason: null,
    sessionsFound: 0,
    rowsUpserted: 0,
    snapshotsInserted: 0,
    sessionsEligibleForUpsert: 0,
    sessionsSkippedBeforeUpsert: 0,
    skipReasons: {},
    upsertError: null,
    snapshotsEligible: 0,
    snapshotInsertError: null,
    sampleSessionBeforeUpsert: null,
    sampleSkippedSessions: [],
    durationMs: 0,
    errors: [],
    error: null,
    slotCountsError: null,
    blockingScrapeTier: null,
    blockingScrapeStartedAt: null,
  };

  if (!tryAcquireScrapeLock(`tier ${tier}`, tier)) {
    report.skipped = true;
    report.skipReason = 'scrape_in_progress';
    report.started = false;
    report.blockingScrapeTier = currentScrapeTier;
    report.blockingScrapeStartedAt = currentScrapeStartedAt;
    console.log(`[tier ${tier}] skipped — scrape already running (tier ${currentScrapeTier})`);
    collectorState.skippedRuns.push({
      tier,
      at: new Date().toISOString(),
      reason: 'scrape_in_progress',
      blockingScrapeTier: currentScrapeTier,
    });
    if (collectorState.skippedRuns.length > 30) collectorState.skippedRuns.shift();
    const skipRunId = await beginScrapeRun(tier);
    await finishScrapeRun(skipRunId, { success: false, error: `skipped: scrape_in_progress (tier ${currentScrapeTier})` });
    recordTierRunState(tier, report, { reason });
    return report;
  }

  const cfg = TIER_CONFIG[tier];
  const { startWeek, endWeek } = weeksForTier(tier);
  if (endWeek < startWeek || startWeek >= effectiveWeeksAhead) {
    const skipReason = `no weeks in range (offsets ${startWeek}–${endWeek}, effective=${effectiveWeeksAhead})`;
    console.log(`[tier ${tier}] skipped — ${skipReason}`);
    report.skipped = true;
    report.skipReason = skipReason;
    report.error = skipReason;
    collectorState.skippedRuns.push({ tier, at: new Date().toISOString(), reason: skipReason });
    if (collectorState.skippedRuns.length > 30) collectorState.skippedRuns.shift();
    const skipRunId = await beginScrapeRun(tier);
    await finishScrapeRun(skipRunId, { success: false, error: `skipped: ${skipReason}` });
    releaseScrapeLock();
    recordTierRunState(tier, report, { reason });
    return report;
  }

  const tierStarted = Date.now();
  console.log(`\n[${new Date().toLocaleTimeString()}] Tier ${tier} scrape (${cfg.label}, week offsets ${startWeek}–${endWeek}, dates ${targetDates.join(', ')})`);

  lastScrapeAttempt = new Date().toISOString();
  const scrapeRunId = await beginScrapeRun(tier);

  if (tier === 1) {
    checkCycle++;
    slotChecksThisCycle = 0;
    collectorState.tier1TargetDates = targetDates;
  }

  const slotStats = { cached: 0, rechecked: 0, byReason: {}, queueLogged: false };
  const prevByKey = new Map(sessions.map(s => [s.key, s]));
  let launched;
  let coverage = null;
  let rowsUpserted = 0;
  let snapshotsInserted = 0;

  try {
    launched = await launchBrowser();
    const { page } = launched;
    const networkCapture = cfg.slotCounts ? installBookingNetworkCapture(page) : null;
    await openBookingPage(page);

    const tierRequiredDates = targetDates.length ? targetDates : expectedDatesForTier(tier);
    let rawBatch;
    let rawTilesTotal = 0;
    let weeksScraped = 0;
    let datesSeen = new Set();

    if (tier === 2) {
      const avail = await scrapeAllAvailableBookingDates(page, {
        maxHorizonDays: MAX_BOOKING_HORIZON_DAYS,
        tier: 2,
        sourceLabel: `tier${tier}`,
        minDay: cfg.minDay,
      });
      rawBatch = avail.batch;
      datesSeen = avail.datesSeen;
      weeksScraped = avail.discoveryDiagnostics.weeksScraped;
      rawTilesTotal = rawBatch.length;
      report.dateRangeFill = avail.dateResults;
      report.availableDatesDiscovery = avail.discoveryDiagnostics;
      report.discoveredAvailableDates = avail.discoveredDates;
    } else {
      const paginated = await scrapePaginatedWeeks(page, startWeek, endWeek, { requiredDates: tierRequiredDates });
      rawBatch = paginated.sessions;
      rawTilesTotal = paginated.rawTilesTotal;
      weeksScraped = paginated.weeksScraped;
      datesSeen = paginated.datesSeen;

      if (tier >= 3) {
        const requiredInWindow = tierRequiredDates;
        const missingDates = requiredInWindow.filter(d => {
          const hasSessions = rawBatch.some(s => sessionDateKey(s) === d);
          return !hasSessions && (!datesSeen.has(d) || !hasSessions);
        });
        if (missingDates.length) {
          console.log(`  tier ${tier} targeted fill for ${missingDates.length} date(s): ${missingDates.join(', ')}`);
          const fill = await scrapeBasicSessionsForDates(page, missingDates, { tier, sourceLabel: `tier${tier}_fill` });
          report.dateRangeFill = fill.dateResults;
          for (const s of fill.batch) {
            if (!rawBatch.find(x => x.key === s.key)) rawBatch.push(s);
          }
          for (const d of fill.dateResults.filter(r => r.targetDateVisible).map(r => r.targetIsoDate)) {
            datesSeen.add(d);
          }
        }
      }
    }

    const batch = dedupeBatch(filterBatchForTier(rawBatch, tier));
    const byKey = new Map(batch.map(s => [s.key, s]));

    if (cfg.slotCounts) {
      try {
        await fillSlotCounts(page, batch, byKey, prevByKey, slotStats, { networkCapture });
        applySlotCacheFallback(byKey);
      } catch (slotErr) {
        report.slotCountsError = slotErr.message;
        console.error(`  tier ${tier} slot counts failed (continuing with basic session data):`, slotErr.message);
      }
    }

    const merged = [...byKey.values()];
    const updatedKeys = mergeBatchIntoStore(merged, tier, {
      preserveSlots: true,
      scrapeKind: cfg.slotCounts ? 'detailed' : 'basic',
    });
    if (datesSeen) recordTierDateCoverage(datesSeen);
    for (const d of tierRequiredDates) {
      datesCheckedDuringScrape.add(d);
      persistedDatesChecked.add(d);
    }

    if (cfg.slotCounts && !report.slotCountsError) {
      syncSlotCacheAvailability(merged);
      const reasonParts = Object.entries(slotStats.byReason).map(([k, n]) => `${n} ${k}`);
      console.log(`  slot counts: ${slotStats.cached} from cache, ${slotStats.rechecked} re-checked${reasonParts.length ? ` (${reasonParts.join(', ')})` : ''}`);
    }

    console.log(`  tier ${tier} summary: ${rawTilesTotal} tiles, ${weeksScraped} week(s), ${batch.length} in date range, ${updatedKeys.length} updated`);
    coverage = computeDateCoverage();
    console.log(`  tier ${tier} date coverage: ${coverage.earliestSessionDate || '?'} → ${coverage.latestSessionDate || '?'} (${coverage.uniqueDatesCount} days with sessions, ${coverage.sessionsCoveragePercent}% session coverage, ${coverage.coveragePercent}% dates checked)`);
    await processWatchAlertsAfterScrape(updatedKeys, { slotsAlerts: cfg.slotCounts && !report.slotCountsError });

    lastTierRun[tier] = new Date().toISOString();
    const durationMs = Date.now() - tierStarted;
    lastTierDurationMs[tier] = durationMs;
    lastTierError[tier] = report.slotCountsError || null;
    if (tier === 1) lastTier1DurationMs = durationMs;
    if (tier === 2) lastTier2DurationMs = durationMs;
    if (tier === 3) lastTier3DurationMs = durationMs;
    lastSuccessfulScrape = new Date().toISOString();
    lastCheck = lastSuccessfulScrape;
    if (!report.slotCountsError) {
      lastScrapeError = null;
      lastScrapeErrorStack = null;
    }
    hasFreshScrapeThisBoot = true;
    dataSource = supabaseConfigured ? 'supabase/current_sessions' : 'memory-fallback';

    const writeDiag = await persistTierScrapeResults(merged, tier, {
      slotCountsAttempted: cfg.slotCounts,
      slotCountsError: report.slotCountsError,
    });
    rowsUpserted = writeDiag.rowsUpserted;
    snapshotsInserted = writeDiag.snapshotsInserted;
    lastSnapshotRowsInsertedLastRun = snapshotsInserted;
    Object.assign(report, writeDiag);
    if (writeDiag.upsertError) {
      report.errors.push({ error: writeDiag.upsertError, phase: 'current_sessions_upsert' });
      report.error = report.error || writeDiag.upsertError;
      console.error(`  tier ${tier} current_sessions upsert failed: ${writeDiag.upsertError}`);
    }
    if (writeDiag.snapshotInsertError) {
      report.errors.push({ error: writeDiag.snapshotInsertError, phase: 'availability_snapshots' });
      if (!report.error) report.error = writeDiag.snapshotInsertError;
      console.error(`  tier ${tier} availability_snapshots insert failed: ${writeDiag.snapshotInsertError}`);
    }

    if (!cfg.slotCounts) {
      const needing = merged.filter(s => sessionQualifiesForDetailEnrichment(s));
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

    if (cfg.slotCounts && !report.slotCountsError) {
      const stillNeeding = merged.filter(s => s.available && !sessionHasDetailedData(s) && daysFromToday(sessionDateKey(s)) <= 1);
      if (stillNeeding.length) {
        console.log(`  tier 1 follow-up: ${stillNeeding.length} today/tomorrow session(s) still missing details`);
        setImmediate(() => {
          runDetailEnrichment({ sessions: stillNeeding, reason: 'tier1_followup' }).catch(console.error);
        });
      }
    }

    await finishScrapeRun(scrapeRunId, {
      success: true,
      sessionsFound: merged.length,
      datesCovered: coverage.coveredDatesCount,
      missingDates: coverage.missingDatesInScrapeWindow,
      coveragePercent: coverage.coveragePercent,
    });

    report.completed = true;
    report.sessionsFound = merged.length;
    report.rowsUpserted = rowsUpserted;
    report.snapshotsInserted = snapshotsInserted;
    report.durationMs = lastTierDurationMs[tier];
    updateTierNextRunEstimate(tier);
    recordTierRunState(tier, report, { reason });

  } catch (e) {
    recordScrapeError(e, `tier ${tier} scrape`);
    lastTierError[tier] = lastScrapeError;
    report.errors.push({ error: lastScrapeError });
    report.error = lastScrapeError;
    report.durationMs = Date.now() - tierStarted;
    lastTierDurationMs[tier] = report.durationMs;
    await saveScrapeErrorToSupabase(lastScrapeError);
    await finishScrapeRun(scrapeRunId, {
      success: false,
      sessionsFound: sessions.length,
      datesCovered: coverage?.coveredDatesCount ?? null,
      missingDates: coverage?.missingDatesInScrapeWindow ?? null,
      error: lastScrapeError,
      errorStack: lastScrapeErrorStack,
    });
    recordTierRunState(tier, report, { reason });
  } finally {
    releaseScrapeLock();
    if (launched?.browser) await launched.browser.close().catch(() => {});
  }

  return report;
}

let weekDetectionInProgress = false;

function tryAcquireWeekDetectionLock() {
  if (weekDetectionInProgress || scrapeInProgress) {
    console.log('  week detection skipped — scrape or detection already running');
    return false;
  }
  weekDetectionInProgress = true;
  return true;
}

function releaseWeekDetectionLock() {
  weekDetectionInProgress = false;
}

async function detectWeeksOnStartup() {
  if (!tryAcquireWeekDetectionLock()) return;

  let launched;
  try {
    launched = await launchBrowser();
    const detected = await detectAvailableWeeks(launched.page);
    updateEffectiveWeeksCap(detected);
  } catch (e) {
    console.error('week detection failed:', e.message);
    effectiveWeeksAhead = Math.max(1, SCRAPE_WEEKS_AHEAD);
  } finally {
    releaseWeekDetectionLock();
    if (launched?.browser) await launched.browser.close().catch(() => {});
  }
}

async function detectWeeksOnStartupWithTimeout(timeoutMs = 90_000) {
  try {
    await Promise.race([
      detectWeeksOnStartup(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('week detection timeout')), timeoutMs)),
    ]);
  } catch (e) {
    console.warn('  week detection:', e.message);
    effectiveWeeksAhead = Math.max(1, SCRAPE_WEEKS_AHEAD);
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
    lastRequestedDateForEnrichment = isoDate;
    const apiStarted = Date.now();
    const debugMode = req.query.debug === '1' || req.query.debug === 'true';
    try {
      const payload = await buildSessionsForDatePayload(isoDate, { mergeIntoStore: false });
      const apiDurationMs = Date.now() - apiStarted;
      lastApiSessionsDurationMs = apiDurationMs;
      API_SESSIONS_DURATION_SAMPLES.push(apiDurationMs);
      if (API_SESSIONS_DURATION_SAMPLES.length > 20) API_SESSIONS_DURATION_SAMPLES.shift();

      if (debugMode) {
        payload.sessions = payload.sessions.map(s => sanitizeSessionForApi(s, { debug: true }));
        payload.debugSessions = payload.sessions.map(s => ({
          key: s.key,
          isoDate: s.isoDate || s.dateKey,
          time: s.time,
          sessionType: s.level,
          waveSide: s.waveSide,
          slots: s.slots,
          capacity: s.capacity,
          estimatedBooked: s.estimatedBooked,
          detailStatus: effectiveDetailStatus(s),
          detailVerified: sessionDetailVerified(s),
          modalAssociationVerified: modalAssociationVerifiedOnSession(s),
          detailConfidence: s.detailConfidence || s.raw?.detailConfidence || null,
          detailRawText: truncateDetailText(s.detailRawText || s.raw?.detailRawText || null, 1500),
          modalDiagnosticRawText: truncateDetailText(s.modalDiagnosticRawText || s.raw?.modalDiagnosticRawText || null, 1500),
          modalMismatchReason: s.modalMismatchReason || s.raw?.modalMismatchReason || null,
          staleModalDetected: s.staleModalDetected === true || s.raw?.staleModalDetected === true,
          previousModalTextHash: s.previousModalTextHash || s.raw?.previousModalTextHash || null,
          currentModalTextHash: s.currentModalTextHash || s.raw?.currentModalTextHash || null,
          tileClickDiagnostics: s.tileClickDiagnostics || s.raw?.tileClickDiagnostics || null,
          detailParseOutput: s.detailParseOutput || s.raw?.detailParseOutput || buildParserOutputFromText(s.detailRawText || ''),
          detailSourceSessionKey: s.detailSourceSessionKey || s.raw?.detailSourceSessionKey || null,
          detailSourceIsoDate: s.detailSourceIsoDate || s.raw?.detailSourceIsoDate || null,
          detailSourceStartTime: s.detailSourceStartTime || s.raw?.detailSourceStartTime || null,
          detailSourceSessionType: s.detailSourceSessionType || s.raw?.detailSourceSessionType || null,
          detailSourceWaveSide: s.detailSourceWaveSide || s.raw?.detailSourceWaveSide || null,
        }));
        payload.detailAssociationDiagnostics = buildDetailAssociationDiagnostics(isoDate, payload.sessions);
      }

      res.json({ ...payload, apiDurationMs, supabaseQueryMs: payload.supabaseQueryMs ?? lastSupabaseDateQueryMs });
      scheduleBackgroundDateDetail(isoDate, payload.sessions, { reason: 'api_sessions_date' });
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

function reasonBrowseWouldShowNotChecked(isoDate, dateSessions, statusReason) {
  if (dateSessions?.length) return 'has_sessions — should render cards';
  if (statusReason === 'saved_sessions_found' || statusReason === 'fallback_sessions_found') {
    return 'statusReason indicates saved data but sessions array empty — client bug';
  }
  if (statusReason === 'checked_no_sessions') return 'date was checked and has zero sessions';
  if (statusReason === 'schema_error') return 'schema missing or query failed';
  if (statusReason === 'error') return 'fetch or server error';
  if (scrapeInProgress) return 'scrape in progress and no saved rows for date yet';
  const map = currentSessionsByDateMap();
  if (map[isoDate]) return `in-memory map has ${map[isoDate]} session(s) but date query returned none — iso_date mismatch?`;
  if (fallbackAvailableCached) return 'fallback sources exist globally but not for this date';
  if (!supabaseConfigured) return 'Supabase not configured and no in-memory sessions';
  if (supabaseSchemaHealth.checkedAt && !supabaseSchemaHealth.currentSessionsAvailable) {
    return 'current_sessions table unavailable — check fallback chain';
  }
  return 'no rows in current_sessions, scrape_snapshots, or availability_snapshots for this date';
}

async function buildBootDebugPayload(selectedDate = null) {
  const isoDate = normalizeIsoDateParam(selectedDate) || getParkTodayIso();
  await ensureSessionsForStatus();
  let datePayload = null;
  try {
    datePayload = await buildSessionsForDatePayload(isoDate);
  } catch (e) {
    datePayload = { error: e.message, sessionsCount: 0, statusReason: 'error' };
  }
  const map = currentSessionsByDateMap();
  return {
    serverNowUtc: new Date().toISOString(),
    serverNowEastern: new Intl.DateTimeFormat('en-US', {
      timeZone: BOOKING_TZ,
      dateStyle: 'full',
      timeStyle: 'long',
    }).format(new Date()),
    appTimezoneUsed: BOOKING_TZ,
    parkTodayIso: getParkTodayIso(),
    selectedDate: selectedDate || isoDate,
    selectedDateIso: isoDate,
    frontendExpectedTodayIso: getParkTodayIso(),
    currentSessionsCount: sessionsByKey.size,
    currentSessionsByDate: map,
    earliestSessionDate: computeDateCoverage().earliestSessionDate,
    latestSessionDate: computeDateCoverage().latestSessionDate,
    lastSuccessfulScrape,
    lastTier1Scrape: lastTierRun[1],
    lastTier2Scrape: lastTierRun[2],
    lastTier3Scrape: lastTierRun[3],
    scrapeInProgress,
    schemaHealth: schemaHealthPayload(),
    dataSource: normalizeDataSource(dataSource),
    hasFreshScrapeThisBoot,
    serverStartedAt,
    isColdStartLikely: !hasFreshScrapeThisBoot && (Date.now() - new Date(serverStartedAt).getTime()) < 300_000,
    timeSinceServerStartMs: Date.now() - new Date(serverStartedAt).getTime(),
    backgroundCollectorEnabled,
    scrapeScheduleEnabled: !!scrapeScheduleEnabled,
    lastTier1DurationMs,
    lastApiSessionsDurationMs,
    lastSupabaseDateQueryMs,
    averageApiSessionsDurationMs: API_SESSIONS_DURATION_SAMPLES.length
      ? Math.round(API_SESSIONS_DURATION_SAMPLES.reduce((a, b) => a + b, 0) / API_SESSIONS_DURATION_SAMPLES.length)
      : null,
    dateQuery: {
      isoDate,
      sessionsCount: datePayload?.sessionsCount ?? 0,
      statusReason: datePayload?.statusReason ?? null,
      dataSource: datePayload?.dataSource ?? null,
      isFallback: datePayload?.isFallback ?? false,
      hasSavedSessions: datePayload?.hasSavedSessions ?? false,
    },
    reasonBrowseWouldShowNotChecked: reasonBrowseWouldShowNotChecked(
      isoDate,
      datePayload?.sessions || [],
      datePayload?.statusReason,
    ),
    activeWatchlistCount: activeWatchItems().length,
    expiredWatchlistCandidates: watchItems.filter(w => w.active !== false && isWatchItemPast(w)).length,
  };
}

function uiReasonText(reason) {
  switch (reason) {
    case 'has_sessions': return 'Show saved sessions for this date';
    case 'checked_empty': return 'Show "No sessions found for this date"';
    case 'checking': return 'Show "Still checking this date…" while scrape runs';
    case 'not_checked': return 'Show "Not checked yet"';
    default: return reason || 'unknown';
  }
}

app.get('/api/debug/boot', async (req, res) => {
  try {
    const selectedDate = req.query.selected_date || req.query.selectedDate || req.query.date || null;
    const payload = await buildBootDebugPayload(selectedDate);
    res.json(payload);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/debug/coverage', async (_req, res) => {
  try {
    await ensureSessionsForStatus();
    const payload = await buildCoverageDebugPayload();
    res.json(payload);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/debug/enrichment', async (_req, res) => {
  try {
    await ensureSessionsForStatus();
    const payload = await buildEnrichmentDebugPayload();
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

app.post('/api/admin/backfill-date-range', async (req, res) => {
  const startDate = normalizeIsoDateParam(req.body?.startDate || req.body?.start_date);
  const endDate = normalizeIsoDateParam(req.body?.endDate || req.body?.end_date);
  const mode = req.body?.mode || 'both';
  const wait = req.body?.wait === true || req.query?.wait === 'true';

  if (!startDate || !endDate) {
    return res.status(400).json({ error: 'startDate and endDate required (YYYY-MM-DD)' });
  }
  if (!['basic_only', 'verified_detail', 'both'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be basic_only, verified_detail, or both' });
  }

  try {
    await ensureSessionsForStatus();

    if (scrapeInProgress && !wait) {
      return res.json({
        started: false,
        queued: false,
        skipped: true,
        skipReason: 'scrape_in_progress',
        startDate,
        endDate,
        mode,
        wait: false,
        message: 'Another scrape is running — retry with wait=true or check /api/debug/collector',
      });
    }

    if (wait && scrapeInProgress) {
      await new Promise((resolve) => {
        const poll = setInterval(() => {
          if (!scrapeInProgress) { clearInterval(poll); resolve(); }
        }, 1000);
        setTimeout(() => { clearInterval(poll); resolve(); }, 300_000);
      });
    }

    const runBackfill = () => runDateRangeBackfill({
      startDate,
      endDate,
      mode,
      reason: 'admin_backfill_date_range',
    }).then(async (result) => {
      await refreshCoverageFlags().catch(() => {});
      return result;
    });

    if (!wait) {
      setImmediate(() => {
        runBackfill().catch((err) => console.error('backfill-date-range error:', err));
      });
      return res.json({
        started: true,
        queued: true,
        wait: false,
        startDate,
        endDate,
        mode,
        maxHorizonDays: MAX_BOOKING_HORIZON_DAYS,
        message: 'Backfill queued — poll GET /api/debug/coverage or GET /api/debug/collector (lastDateRangeBackfill)',
      });
    }

    const result = await runBackfill();
    res.json({
      started: true,
      queued: false,
      wait: true,
      startDate,
      endDate,
      mode,
      maxHorizonDays: MAX_BOOKING_HORIZON_DAYS,
      ...result,
    });
  } catch (e) {
    res.status(500).json({
      error: e.message,
      errors: [{ error: e.message }],
      startDate,
      endDate,
      mode,
    });
  }
});

app.post('/api/admin/backfill-available-dates', async (req, res) => {
  const mode = req.body?.mode || 'both';
  const wait = req.body?.wait === true || req.query?.wait === 'true';
  const maxHorizonDays = req.body?.maxHorizonDays != null
    ? parseInt(req.body.maxHorizonDays, 10)
    : undefined;

  if (!['basic_only', 'verified_detail', 'both'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be basic_only, verified_detail, or both' });
  }

  try {
    await ensureSessionsForStatus();

    if (scrapeInProgress && !wait) {
      return res.json({
        started: false,
        queued: false,
        skipped: true,
        skipReason: 'scrape_in_progress',
        mode,
        wait: false,
        maxHorizonDays: maxHorizonDays ?? MAX_BOOKING_HORIZON_DAYS,
        message: 'Another scrape is running — retry with wait=true or check /api/debug/collector',
      });
    }

    if (wait && scrapeInProgress) {
      await new Promise((resolve) => {
        const poll = setInterval(() => {
          if (!scrapeInProgress) { clearInterval(poll); resolve(); }
        }, 1000);
        setTimeout(() => { clearInterval(poll); resolve(); }, 300_000);
      });
    }

    const runBackfill = () => runBackfillAvailableDates({
      mode,
      maxHorizonDays,
      reason: 'admin_backfill_available_dates',
    }).then(async (result) => {
      await refreshCoverageFlags().catch(() => {});
      return result;
    });

    if (!wait) {
      setImmediate(() => {
        runBackfill().catch((err) => console.error('backfill-available-dates error:', err));
      });
      return res.json({
        started: true,
        queued: true,
        wait: false,
        mode,
        maxHorizonDays: maxHorizonDays ?? MAX_BOOKING_HORIZON_DAYS,
        message: 'Backfill queued — poll GET /api/debug/coverage (discoveredAvailableDates, lastBackfillAvailableDatesResult)',
      });
    }

    const result = await runBackfill();
    res.json({
      started: true,
      queued: false,
      wait: true,
      mode,
      maxHorizonDays: maxHorizonDays ?? MAX_BOOKING_HORIZON_DAYS,
      ...result,
    });
  } catch (e) {
    res.status(500).json({
      error: e.message,
      errors: [{ error: e.message }],
      mode,
    });
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
    const detailDiag = buildDateDetailDiagnostics(isoDate, apiPayload.sessions);
    const payload = {
      isoDate,
      parkTodayIso: getParkTodayIso(),
      ...detailDiag,
      reasonBrowseWouldShowNotChecked: reasonBrowseWouldShowNotChecked(
        isoDate,
        apiPayload.sessions,
        apiPayload.statusReason,
      ),
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

      payload.scrapeRunsForDate = (runs || [])
        .filter((run) => run.tier === 1 || run.success)
        .slice(0, 10);
    }

    payload.wasTodayInTier1TargetDates = tierTargetDates(1).includes(isoDate);
    if (isoDate === getParkTodayIso()) {
      payload.reasonTodayHasZeroSessions = reasonTodayHasZeroSessions(isoDate);
      payload.tier1TargetDates = tierTargetDates(1);
      payload.tier1LastResult = collectorState.tier1LastResult;
      payload.tier1LastCompletedAt = collectorState.tier1LastCompletedAt || lastTierRun[1];
    }

    res.json(payload);
  } catch (e) {
    res.status(500).json({ isoDate, error: e.message });
  }
});

app.get('/api/debug/session/:sessionKey', async (req, res) => {
  const sessionKey = decodeURIComponent(req.params.sessionKey || '').trim();
  if (!sessionKey) {
    return res.status(400).json({ error: 'sessionKey required' });
  }
  try {
    const payload = await buildSessionDebugPayload(sessionKey);
    if (!payload.found) return res.status(404).json(payload);
    res.json(payload);
  } catch (e) {
    res.status(500).json({ sessionKey, error: e.message });
  }
});

app.post('/api/admin/repair-detail-data', async (req, res) => {
  const isoDate = normalizeIsoDateParam(req.body?.isoDate || req.body?.iso_date);
  const dryRun = req.body?.dryRun === true || req.query?.dryRun === 'true';
  try {
    await ensureSessionsForStatus();
    let candidates = allStoredSessions();
    if (isoDate) candidates = candidates.filter(s => (s.isoDate || s.dateKey) === isoDate);

    const repaired = [];
    const preserved = [];
    const skipped = [];

    for (const s of candidates) {
      const rawModal = s.detailRawText || s.raw?.detailRawText || s.modalDiagnosticRawText || s.raw?.modalDiagnosticRawText || '';
      const parserOutput = s.detailParseOutput || s.raw?.detailParseOutput
        || buildParserOutputFromText(rawModal);
      const validation = rawModal ? validateModalAssociation(s, rawModal) : null;
      const verified = sessionDetailVerified(s);
      const defaultLike = isDefaultLikeDetailValues(s.slots, s.capacity, s.estimatedBooked);
      const unparsedDisplayed = (s.slots != null || s.capacity != null)
        && parserOutput.parsed_slots_available == null
        && parserOutput.parsed_capacity == null;
      const mismatch = validation?.confidence === 'mismatch';
      const invalidCheckedWithSlots = isInvalidCheckedWithSlotsStatus(s);
      const stale = s.staleModalDetected || s.raw?.staleModalDetected
        || effectiveDetailStatus(s) === 'failed_modal_stale';

      if (verified && !defaultLike && !mismatch && !unparsedDisplayed && !invalidCheckedWithSlots && !stale) {
        preserved.push(s.key);
        continue;
      }
      if (!s.slots && !s.capacity && !s.estimatedBooked && !s.priceText && !rawModal
        && !invalidCheckedWithSlots && !stale && !mismatch) {
        skipped.push(s.key);
        continue;
      }

      const entry = { ...s };
      if (stale) {
        entry.detailStatus = 'failed_modal_stale';
        entry.detailError = entry.detailError || 'repaired_modal_stale';
        entry.detailConfidence = 'mismatch';
        entry.staleModalDetected = true;
      } else if (mismatch) {
        entry.detailStatus = 'failed_modal_mismatch';
        entry.detailError = entry.detailError || 'repaired_modal_mismatch';
        entry.detailConfidence = 'mismatch';
      } else if (invalidCheckedWithSlots) {
        entry.detailStatus = 'checked_available_no_slot_count';
        entry.detailConfidence = 'default_suppressed';
        entry.detailError = entry.detailError || 'repaired_invalid_checked_with_slots';
      } else if (defaultLike || unparsedDisplayed) {
        entry.detailStatus = entry.detailStatus === 'checked_with_slots'
          ? 'checked_available_no_slot_count'
          : entry.detailStatus;
        entry.detailConfidence = 'default_suppressed';
        entry.detailError = entry.detailError || 'repaired_unverified_defaults';
      }

      entry.detailVerified = false;
      entry.modalAssociationVerified = false;
      entry.detailRawText = null;
      if (rawModal) entry.modalDiagnosticRawText = truncateDetailText(rawModal, 1500);
      clearUnverifiedDetailMetrics(entry);
      repaired.push({
        key: s.key,
        isoDate: s.isoDate || s.dateKey,
        time: s.time,
        reason: stale ? 'modal_stale' : (mismatch ? 'modal_mismatch' : (invalidCheckedWithSlots ? 'invalid_checked_with_slots' : (defaultLike ? 'default_like' : 'unparsed_displayed'))),
        prior: { slots: s.slots, capacity: s.capacity, estimatedBooked: s.estimatedBooked, detailStatus: s.detailStatus || s.detail_status },
      });

      if (!dryRun) {
        sessionsByKey.set(entry.key, entry);
        await upsertCurrentSessionsToSupabase([entry], 0, { scrapeKind: 'detailed' });
      }
    }

    res.json({
      isoDate: isoDate || null,
      dryRun,
      candidatesCount: candidates.length,
      repairedCount: repaired.length,
      preservedVerifiedCount: preserved.length,
      skippedCount: skipped.length,
      repairedSample: repaired.slice(0, 20),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/admin/enrich-date', async (req, res) => {
  const isoDate = normalizeIsoDateParam(req.body?.isoDate || req.body?.iso_date);
  if (!isoDate) {
    return res.status(400).json({ error: 'isoDate required (YYYY-MM-DD)' });
  }
  const mode = req.body?.mode || req.query?.mode || 'failed_first';

  try {
    await ensureSessionsForStatus();
    const allForDate = sessionsForDate(isoDate);
    const openSessions = allForDate.filter(s => s.available !== false);
    const failedFirst = mode === 'failed_first';
    const retryTargets = failedFirst
      ? sortSessionsForFailedFirstEnrich(openSessions.filter(sessionQualifiesForFailedFirstEnrich))
      : sortSessionsForDetailRetry(openSessions.filter(s =>
        isDetailFailureStatus(effectiveDetailStatus(s)) || !sessionHasDetailedData(s),
      ));
    const toProcess = retryTargets.length ? retryTargets : sortSessionsForDetailRetry(openSessions);

    if (!openSessions.length) {
      return res.json({
        isoDate,
        mode,
        sessionsAttempted: 0,
        sessionsUpdatedWithSlots: 0,
        sessionsUpdatedWithCapacity: 0,
        sessionsUpdatedWithPrice: 0,
        sessionsMarkedPacked: 0,
        sessionsCheckedOpenNoSlotsVisible: 0,
        sessionsFailedParse: 0,
        sessionsFailedSelector: 0,
        sessionsFailedCookieOverlay: 0,
        sessionsTimedOut: 0,
        sessionsFailed: 0,
        topErrors: [],
        errors: [{ error: 'no_open_sessions_for_date' }],
      });
    }

    await enqueueDateForEnrichment(isoDate, { priority: 1, reason: `admin_enrich_date_${mode}` });
    const waitForResult = req.body?.wait === true || req.query?.wait === 'true';

    const enrichResponseFields = (result) => {
      const skipped = !!result.skipped;
      const skipReason = skipped ? (result.skipReason || result.reason || 'unknown_skip') : null;
      return {
        isoDate,
        mode,
        sessionsQueued: result.sessionsQueued ?? toProcess.length,
        sessionsAttempted: skipped ? 0 : (result.sessionsAttempted ?? 0),
        sessionsUpdatedWithSlots: result.sessionsUpdatedWithSlots ?? 0,
        sessionsUpdatedWithCapacity: result.sessionsUpdatedWithCapacity ?? 0,
        sessionsUpdatedWithPrice: result.sessionsUpdatedWithPrice ?? 0,
        sessionsMarkedPacked: result.sessionsMarkedPacked ?? 0,
        sessionsCheckedOpenNoSlotsVisible: result.sessionsCheckedOpenNoSlotsVisible ?? result.sessionsCheckedNoSlotsVisible ?? 0,
        sessionsFailedParse: result.sessionsFailedParse ?? 0,
        sessionsFailedSelector: result.sessionsFailedSelector ?? 0,
        sessionsFailedCookieOverlay: result.sessionsFailedCookieOverlay ?? 0,
        sessionsTimedOut: result.sessionsTimedOut ?? 0,
        sessionsFailed: result.sessionsFailed ?? 0,
        sessionsUnchanged: result.sessionsUnchanged ?? 0,
        outcomeTotal: result.outcomeTotal ?? 0,
        outcomeReconciles: result.outcomeReconciles ?? null,
        topErrors: buildTopDetailErrors(result.errors),
        errors: result.errors ?? [],
        unchangedReasons: result.unchangedReasons ?? [],
        skipped,
        skipReason,
        enrichDateSkipReason: skipReason,
        cookieDismissAttempted: result.cookieDismissAttempted ?? 0,
        cookieDismissSucceeded: result.cookieDismissSucceeded ?? 0,
        cookieBannerStillVisible: result.cookieBannerStillVisible ?? false,
        cookieClickMethod: result.cookieClickMethod ?? null,
        modalTextAfterCookieDismissSample: result.modalTextAfterCookieDismissSample ?? null,
        cookieDismissLastAttempt: result.cookieDismissLastAttempt ?? null,
        cookieDismissAttempts: result.cookieDismissAttempts ?? [],
      };
    };

    if (!waitForResult) {
      setImmediate(() => {
        runDetailEnrichment({
          isoDate,
          sessions: toProcess,
          reason: 'admin_enrich_date',
        }).catch(console.error);
      });
      return res.json({
        ...enrichResponseFields({ skipped: false, sessionsQueued: toProcess.length, sessionsAttempted: 0, errors: [] }),
        queued: true,
      });
    }

    const result = await runDetailEnrichment({
      isoDate,
      sessions: toProcess,
      reason: 'admin_enrich_date',
    });

    res.json(enrichResponseFields(result));
  } catch (e) {
    res.status(500).json({ isoDate, error: e.message, errors: [{ error: e.message }] });
  }
});

app.get('/api/debug/collector', async (_req, res) => {
  try {
    const payload = await buildCollectorDebugPayload();
    res.json(payload);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

async function adminRunTierHandler(tier, req, res) {
  const wait = req.body?.wait === true || req.query?.wait === 'true';
  const reason = req.body?.reason || `admin_run_tier${tier}`;
  try {
    if (scrapeInProgress && !wait) {
      return res.json({
        tier,
        started: false,
        completed: false,
        skipped: true,
        skipReason: 'scrape_in_progress',
        targetDates: tierTargetDates(tier),
        sessionsFound: 0,
        rowsUpserted: 0,
        snapshotsInserted: 0,
        durationMs: 0,
        error: null,
        blockingScrapeTier: currentScrapeTier,
        blockingScrapeStartedAt: currentScrapeStartedAt,
        message: 'Another scrape is running — retry with wait=true or check /api/debug/collector',
      });
    }
    if (wait && scrapeInProgress) {
      await new Promise((resolve) => {
        const poll = setInterval(() => {
          if (!scrapeInProgress) { clearInterval(poll); resolve(); }
        }, 1000);
        setTimeout(() => { clearInterval(poll); resolve(); }, 180_000);
      });
    }
    const report = await runScheduledTier(tier, { reason });
    res.json({
      tier,
      started: report.started !== false,
      completed: report.completed,
      skipped: report.skipped,
      skipReason: report.skipReason,
      targetDates: report.targetDates,
      sessionsFound: report.sessionsFound ?? 0,
      sessionsEligibleForUpsert: report.sessionsEligibleForUpsert ?? 0,
      sessionsSkippedBeforeUpsert: report.sessionsSkippedBeforeUpsert ?? 0,
      skipReasons: report.skipReasons ?? {},
      rowsUpserted: report.rowsUpserted ?? 0,
      upsertError: report.upsertError ?? null,
      snapshotsEligible: report.snapshotsEligible ?? 0,
      snapshotsInserted: report.snapshotsInserted ?? 0,
      snapshotInsertError: report.snapshotInsertError ?? null,
      sampleSessionBeforeUpsert: report.sampleSessionBeforeUpsert ?? null,
      sampleSkippedSessions: report.sampleSkippedSessions ?? [],
      durationMs: report.durationMs ?? 0,
      error: report.error || report.upsertError || report.snapshotInsertError || report.errors?.[0]?.error || report.slotCountsError || null,
      slotCountsError: report.slotCountsError ?? null,
      blockingScrapeTier: report.blockingScrapeTier ?? null,
      blockingScrapeStartedAt: report.blockingScrapeStartedAt ?? null,
      supabaseConfigured,
      message: report.completed ? 'completed' : (report.skipped ? 'skipped' : 'failed'),
    });
  } catch (e) {
    res.status(500).json({
      tier,
      started: true,
      completed: false,
      skipped: false,
      error: e.message,
      errors: [{ error: e.message }],
    });
  }
}

app.post('/api/admin/run-tier1', (req, res) => adminRunTierHandler(1, req, res));
app.post('/api/admin/run-tier2', (req, res) => adminRunTierHandler(2, req, res));
app.post('/api/admin/run-tier3', (req, res) => adminRunTierHandler(3, req, res));

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
    await expirePastWatchlistItems();
    await refreshCoverageFlags();
  } catch (e) {
    supabaseInitError = supabaseInitError || e.message;
    console.error('Supabase cache load failed:', e.message);
  }
}

function tierIntervalMinutes(tier) {
  if (tier === 1) return CHECK_MINS;
  if (tier === 2) return 30;
  if (tier === 3) return 360;
  return 1440;
}

function updateTierNextRunEstimate(tier) {
  const mins = tierIntervalMinutes(tier);
  const base = lastTierRun[tier] ? new Date(lastTierRun[tier]).getTime() : Date.now();
  const next = new Date(base + mins * 60_000).toISOString();
  if (tier === 1) collectorState.tier1NextRunAt = next;
  if (tier === 2) collectorState.tier2NextRunAt = next;
  if (tier === 3) collectorState.tier3NextRunAt = next;
}

function scheduleCronSafe(expression, handler, label) {
  if (!cron.validate(expression)) {
    console.error(`  Invalid cron expression for ${label}: ${expression}`);
    return null;
  }
  const task = cron.schedule(expression, handler, { scheduled: true, timezone: BOOKING_TZ });
  console.log(`  Scheduling ${label}: ${expression}`);
  return task;
}

async function runScheduledTier(tier, { reason = 'scheduled' } = {}) {
  console.log(`[collector] Tier ${tier} trigger (${reason})`);
  const report = await runTierScrape(tier, { reason });
  recordTierRunState(tier, report, { reason });
  if (report.skipped) {
    console.log(`[collector] Tier ${tier} skipped: ${report.skipReason}`);
  } else if (report.completed) {
    console.log(`[collector] Tier ${tier} completed in ${report.durationMs}ms — ${report.sessionsFound} sessions, ${report.rowsUpserted} upserted, ${report.snapshotsInserted} snapshots`);
  } else if (report.errors?.length || report.error) {
    console.error(`[collector] Tier ${tier} failed:`, report.error || report.errors[0]?.error);
  }
  return report;
}

function runTierScrapeAsync(tier, { reason = 'scheduled' } = {}) {
  runScheduledTier(tier, { reason }).catch((e) => console.error(`[collector] Tier ${tier} error:`, e.message));
}

async function buildCollectorDebugPayload() {
  const msSinceStart = Date.now() - new Date(serverStartedAt).getTime();
  const minutesSinceTier1 = lastTierRun[1]
    ? Math.round((Date.now() - new Date(lastTierRun[1]).getTime()) / 60_000)
    : null;
  const minutesSinceTier2 = lastTierRun[2]
    ? Math.round((Date.now() - new Date(lastTierRun[2]).getTime()) / 60_000)
    : null;
  const minutesSinceTier3 = lastTierRun[3]
    ? Math.round((Date.now() - new Date(lastTierRun[3]).getTime()) / 60_000)
    : null;

  let recentScrapeRuns = [];
  let availabilitySnapshotsByDateLast24h = {};
  if (supabase) {
    try {
      const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
      const { data: runs } = await supabase
        .from('scrape_runs')
        .select('id, tier, started_at, finished_at, success, sessions_found, error')
        .order('started_at', { ascending: false })
        .limit(15);
      recentScrapeRuns = runs || [];

      const { data: snaps } = await supabase
        .from('availability_snapshots')
        .select('iso_date')
        .gte('scraped_at', since);
      for (const row of snaps || []) {
        const dk = String(row.iso_date).slice(0, 10);
        availabilitySnapshotsByDateLast24h[dk] = (availabilitySnapshotsByDateLast24h[dk] || 0) + 1;
      }
    } catch (e) {
      console.warn('  collector debug query failed:', e.message);
    }
  }

  let recommendedAction = 'none';
  if (!scrapeScheduleEnabled) {
    recommendedAction = 'Scheduler not started — check server logs for cron errors';
  } else if (lastTierRun[1] == null && minutesSinceTier1 == null && msSinceStart > 10 * 60_000) {
    recommendedAction = 'Tier 1 never ran — POST /api/admin/run-tier1 or check Playwright/Chromium logs';
  } else if (minutesSinceTier1 != null && minutesSinceTier1 > CHECK_MINS * 3) {
    recommendedAction = 'Tier 1 stale — verify Railway App Sleep is disabled and run POST /api/admin/run-tier1';
  } else if (isCurrentSessionsSparse()) {
    recommendedAction = 'Coverage sparse — Tier 2 should run soon; POST /api/admin/run-tier2 if needed';
  }

  return {
    serverStartedAt,
    minutesSinceServerStart: Math.round(msSinceStart / 60_000),
    backgroundCollectorEnabled,
    scrapeScheduleEnabled: !!scrapeScheduleEnabled,
    schedulerStartedAt: collectorState.schedulerStartedAt,
    tier1IntervalConfigured: !!collectorState.cronTasks.tier1,
    tier1IntervalCron: collectorState.tier1Interval,
    tier2IntervalConfigured: !!collectorState.cronTasks.tier2,
    tier2IntervalCron: collectorState.tier2Interval,
    tier3IntervalConfigured: !!collectorState.cronTasks.tier3,
    tier3IntervalCron: collectorState.tier3Interval,
    tier1NextRunAt: collectorState.tier1NextRunAt,
    tier2NextRunAt: collectorState.tier2NextRunAt,
    tier3NextRunAt: collectorState.tier3NextRunAt,
    tier1LastAttemptAt: collectorState.tier1LastAttemptAt,
    tier1LastCompletedAt: collectorState.tier1LastCompletedAt || lastTierRun[1],
    tier1LastSkippedAt: collectorState.tier1LastSkippedAt,
    tier1LastSkipReason: collectorState.tier1LastSkipReason,
    tier1LastError: collectorState.tier1LastError || lastTierError[1],
    tier1TargetDates: collectorState.tier1TargetDates || tierTargetDates(1),
    tier1LastResult: collectorState.tier1LastResult,
    initialScrapeScheduled: collectorState.initialScrapeScheduled,
    lastTier1Scrape: lastTierRun[1],
    minutesSinceLastTier1: minutesSinceTier1,
    lastTier2Scrape: lastTierRun[2],
    minutesSinceLastTier2: minutesSinceTier2,
    lastTier3Scrape: lastTierRun[3],
    minutesSinceLastTier3: minutesSinceTier3,
    lastTier1DurationMs,
    lastTier2DurationMs,
    lastTier3DurationMs,
    lastTier1Error: lastTierError[1],
    lastTier2Error: lastTierError[2],
    lastTier3Error: lastTierError[3],
    scrapeInProgress,
    currentScrapeTier,
    currentScrapeStartedAt,
    weekDetectionInProgress,
    lastScrapeError,
    lastSuccessfulScrape,
    effectiveWeeksAhead,
    parkTodayIso: getParkTodayIso(),
    skippedRunsRecent: collectorState.skippedRuns.slice(-10),
    recentScrapeRuns,
    currentSessionsByDate: currentSessionsByDateMap(),
    availabilitySnapshotsByDateLast24h,
    likelySleepingOrRestarted: scrapeScheduleEnabled && lastTierRun[1] == null && msSinceStart > CHECK_MINS * 4 * 60_000,
    railwayNote: 'Disable Railway Serverless/App Sleep for continuous scraping.',
    recommendedAction,
    lastApiSessionsDurationMs,
    lastSupabaseDateQueryMs,
  };
}

function startBackgroundCollector() {
  if (!backgroundCollectorEnabled) {
    scrapeScheduleEnabled = false;
    console.log('Background collector disabled (BACKGROUND_COLLECTOR_ENABLED=false)');
    return;
  }

  if (collectorState.schedulerStartedAt) {
    console.log('Background collector already started');
    return;
  }

  collectorState.schedulerStartedAt = new Date().toISOString();
  collectorState.tier1Interval = `*/${CHECK_MINS} * * * *`;
  collectorState.tier2Interval = '*/30 * * * *';
  collectorState.tier3Interval = '0 */6 * * *';

  console.log('\n── Background collector enabled ──');
  console.log(`  Scheduling Tier 1 every ${CHECK_MINS} minutes (today/tomorrow + slots)`);
  console.log('  Scheduling Tier 2 every 30 minutes (next 7 days)');
  console.log('  Scheduling Tier 3 every 6 hours (weeks 2–3)');

  collectorState.cronTasks.tier1 = scheduleCronSafe(
    collectorState.tier1Interval,
    () => runTierScrapeAsync(1, { reason: 'cron_tier1' }),
    'Tier 1',
  );
  collectorState.cronTasks.tier2 = scheduleCronSafe(
    collectorState.tier2Interval,
    () => runTierScrapeAsync(2, { reason: 'cron_tier2' }),
    'Tier 2',
  );
  collectorState.cronTasks.tier3 = scheduleCronSafe(
    collectorState.tier3Interval,
    () => runTierScrapeAsync(3, { reason: 'cron_tier3' }),
    'Tier 3',
  );
  collectorState.cronTasks.tier4 = scheduleCronSafe(
    '0 0 * * *',
    () => {
      detectWeeksOnStartupWithTimeout().then(() => runTierScrapeAsync(4, { reason: 'cron_tier4' })).catch(console.error);
    },
    'Tier 4 daily',
  );
  collectorState.cronTasks.enrichP1 = scheduleCronSafe(
    collectorState.tier1Interval,
    () => setTimeout(() => runDetailEnrichmentByPriority(1).catch(console.error), ENRICHMENT_P1_OFFSET_MS),
    'Detail enrichment P1',
  );
  collectorState.cronTasks.enrichP2 = scheduleCronSafe(
    `*/${ENRICHMENT_TIER2_EVERY_MINS} * * * *`,
    () => runDetailEnrichmentByPriority(2).catch(console.error),
    'Detail enrichment P2',
  );
  collectorState.cronTasks.enrichP3 = scheduleCronSafe(
    '0 */12 * * *',
    () => runDetailEnrichmentByPriority(3).catch(console.error),
    'Detail enrichment P3',
  );

  scrapeScheduleEnabled = !!(collectorState.cronTasks.tier1 && collectorState.cronTasks.tier2 && collectorState.cronTasks.tier3);
  if (!scrapeScheduleEnabled) {
    console.error('  Background collector FAILED to schedule — check cron expressions');
    return;
  }

  updateTierNextRunEstimate(1);
  updateTierNextRunEstimate(2);
  updateTierNextRunEstimate(3);

  collectorState.initialScrapeScheduled = true;
  console.log('  Initial scrape scheduled — Tier 1 in 10s (priority), week detection in parallel');

  setTimeout(() => runTierScrapeAsync(1, { reason: 'startup_tier1' }), 10_000);

  setTimeout(() => {
    detectWeeksOnStartupWithTimeout().catch(console.error);
  }, 5_000);

  setTimeout(() => {
    if (isCurrentSessionsSparse() && !lastTierRun[1] && !scrapeInProgress) {
      console.log('  Coverage sparse and Tier 1 not done — re-triggering Tier 1');
      runTierScrapeAsync(1, { reason: 'startup_tier1_retry' });
    } else if (isCurrentSessionsSparse() && lastTierRun[1] && !scrapeInProgress) {
      console.log('  Coverage sparse — scheduling startup Tier 2');
      runTierScrapeAsync(2, { reason: 'startup_tier2_sparse' });
    }
  }, 120_000);

  console.log('── Background collector ready ──\n');
}

// ── Boot: tiered cron schedules ───────────────────────────────────────────────
function bootstrapInBackground() {
  startBackgroundCollector();
}

async function startServer() {
  await loadPersistedData();
  if (sessions.length) {
    console.log(`Serving ${sessions.length} saved session(s) (${dataSource}) — background scrapes will refresh in place`);
  }

  app.listen(PORT, () => {
    console.log(`\nAP Session Watcher running on :${PORT}`);
    console.log(`Express ready — API responds immediately; collector starts in background`);
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
