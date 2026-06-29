'use strict';
const express  = require('express');
const { chromium } = require('playwright');
const cron     = require('node-cron');
const path     = require('path');
const fs       = require('fs');
const crypto   = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const WebSocket = require('ws');

const pkg = require('./package.json');
const { parseCalendarFixtureDom } = require('./lib/calendar-fixture-tile-parser');
const { scrapeCalendarFixtureDom } = require('./lib/calendar-fixture-dom-scraper');
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
const THRESHOLD_SCAN_MAX_DEFAULT = parseInt(process.env.THRESHOLD_SCAN_MAX || '20', 10);
const THRESHOLD_SCAN_MIN_DEFAULT = 1;
const THRESHOLD_FILTER_SETTLE_MS = parseInt(process.env.THRESHOLD_FILTER_SETTLE_MS || '1200', 10);
const THRESHOLD_SCAN_MAX_WEEKS_PER_RUN = Math.max(1, parseInt(process.env.THRESHOLD_SCAN_MAX_WEEKS_PER_RUN || '1', 10));
const THRESHOLD_SCAN_MAX_THRESHOLDS_PER_PAGE = Math.max(1, parseInt(process.env.THRESHOLD_SCAN_MAX_THRESHOLDS_PER_PAGE || '20', 10));
const THRESHOLD_SCAN_PAGE_TIMEOUT_MS = parseInt(process.env.THRESHOLD_SCAN_PAGE_TIMEOUT_MS || '45000', 10);
const THRESHOLD_DEBUG_ARTIFACTS_DIR = path.join(__dirname, 'debug-threshold-scans');
const THRESHOLD_SCAN_RECYCLE_BROWSER_EACH_WEEK = process.env.THRESHOLD_SCAN_RECYCLE_BROWSER_EACH_WEEK !== 'false';
const THRESHOLD_SCAN_THRESHOLD_BATCH_SIZE = Math.max(1, parseInt(process.env.THRESHOLD_SCAN_THRESHOLD_BATCH_SIZE || '5', 10));
const THRESHOLD_FILTER_TIMEOUT_MS = parseInt(process.env.THRESHOLD_FILTER_TIMEOUT_MS || '20000', 10);
const THRESHOLD_TILE_SCRAPE_TIMEOUT_MS = parseInt(process.env.THRESHOLD_TILE_SCRAPE_TIMEOUT_MS || '15000', 10);
const BOOKING_PAGE_TIMEOUT_MS = parseInt(process.env.BOOKING_PAGE_TIMEOUT_MS || '45000', 10);
const SCRAPE_LOCK_MAX_MS = parseInt(process.env.SCRAPE_LOCK_MAX_MS || '900000', 10);
const SCRAPE_LOCK_MANUAL_RELEASE_MIN_MS = parseInt(process.env.SCRAPE_LOCK_MANUAL_RELEASE_MIN_MS || '120', 10) * 1000;
const BROWSER_CLOSE_TIMEOUT_MS = parseInt(process.env.BROWSER_CLOSE_TIMEOUT_MS || '10000', 10);
const BACKGROUND_THRESHOLD_SCAN_ENABLED = process.env.BACKGROUND_THRESHOLD_SCAN_ENABLED === 'true';
const BACKGROUND_THRESHOLD_SCAN_EVERY_MINS = Math.max(15, parseInt(process.env.BACKGROUND_THRESHOLD_SCAN_EVERY_MINS || '60', 10));
const BACKGROUND_DETAIL_ENRICHMENT_ENABLED = process.env.BACKGROUND_DETAIL_ENRICHMENT_ENABLED === 'true';
const BOOKING    = 'https://booking.atlanticparksurf.com/activity-agenda';
const APP_URL    = process.env.APP_URL || BOOKING;
const CHECK_MINS      = Math.max(1, parseInt(process.env.CHECK_EVERY_MINS || '5', 10) || 5);
const MAX_SLOT_CHECKS = parseInt(process.env.MAX_SLOT_CHECKS || '50', 10);
const SLOT_CACHE_STALE_CYCLES = parseInt(process.env.SLOT_CACHE_STALE_CYCLES || '3', 10);
const DETAIL_ENRICH_MAX_PER_RUN = parseInt(process.env.DETAIL_ENRICH_MAX_PER_RUN || '25', 10);
const DETAIL_MAX_ATTEMPTS = parseInt(process.env.DETAIL_MAX_ATTEMPTS || '30', 10);
const DETAIL_RETRY_BASE_MS = parseInt(process.env.DETAIL_RETRY_BASE_MS || '300000', 10);
const DETAIL_RETRY_MAX_MS = parseInt(process.env.DETAIL_RETRY_MAX_MS || '21600000', 10);
const DETAIL_QUEUE_DRAIN_DELAY_MS = parseInt(process.env.DETAIL_QUEUE_DRAIN_DELAY_MS || '5000', 10);
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
  1: { label: 'today/tomorrow', slotCounts: false, weekStart: 0, weekEnd: 0, minDay: 0,  maxDay: 1 },
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
  detailQueueDrainScheduled: false,
  lastDetailQueueBatch: null,
  lastThresholdScanResult: null,
  lastPageCrashAt: null,
  lastPageCrashStage: null,
  lastThresholdScanWeek: null,
  thresholdScanBatchProgress: null,
  thresholdScanWeeksRemaining: [],
  thresholdScanPendingWeeks: [],
  thresholdScanCompletedWeeks: [],
  thresholdScanLastError: null,
  thresholdScanRecovered: false,
  lastScrapeCrashAt: null,
  lastScrapeCrashStage: null,
  lastScrapeCrashTier: null,
  scrapeLockReleasedAt: null,
  scrapeLockReleasedReason: null,
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
    discoveredAvailableDates: collectorState.discoveredAvailableDates || [],
    lastDiscoveryAt: collectorState.lastDiscoveryAt || null,
    lastBackfillAvailableDatesResult: collectorState.lastBackfillAvailableDatesResult
      ? {
        completedAt: collectorState.lastBackfillAvailableDatesResult.completedAt || null,
        discoveredAvailableDates: collectorState.lastBackfillAvailableDatesResult.discoveredAvailableDates || [],
        discoveryDiagnostics: collectorState.lastBackfillAvailableDatesResult.discoveryDiagnostics || null,
      }
      : null,
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
    if (Array.isArray(meta.discoveredAvailableDates) && meta.discoveredAvailableDates.length) {
      collectorState.discoveredAvailableDates = [...meta.discoveredAvailableDates].sort();
    }
    if (meta.lastDiscoveryAt) collectorState.lastDiscoveryAt = meta.lastDiscoveryAt;
    if (meta.lastBackfillAvailableDatesResult) {
      collectorState.lastBackfillAvailableDatesResult = meta.lastBackfillAvailableDatesResult;
      const bfDates = meta.lastBackfillAvailableDatesResult.discoveredAvailableDates;
      if (Array.isArray(bfDates) && bfDates.length && !collectorState.discoveredAvailableDates.length) {
        collectorState.discoveredAvailableDates = [...bfDates].sort();
      }
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
  const dbStats = await queryCurrentSessionsByDateFromDb();
  const currentMap = dbStats.byDate;
  const discovery = resolveDiscoveredAvailableDates({ dbByDate: currentMap });
  const discovered = discovery.dates;
  const expected = discovered.length ? discovered : deriveDiscoveredDatesFromCurrentSessions(currentMap);
  const scrapeWindowExpected = expectedDatesInScrapeWindow();
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
  if (discovery.source === 'none' && datesWithBasicRows.length) {
    recommendedAction = 'discovered dates derived from current_sessions — run POST /api/admin/backfill-available-dates to refresh discovery';
  } else if (!discovered.length) recommendedAction = 'POST /api/admin/backfill-available-dates';
  else if (sparse && fallback) recommendedAction = 'POST /api/admin/backfill-current-sessions';
  else if (sparse) recommendedAction = 'POST /api/admin/backfill-available-dates or wait for Tier 2 scrape';
  else if (missingDiscoveredDates.length) recommendedAction = 'POST /api/admin/backfill-available-dates';

  const detailCounts = computeDetailCountsByDate(discovered.length ? discovered : expected);
  const thresholdCoverage = computeThresholdCoverageByDate(discovered.length ? discovered : expected);

  return {
    parkTodayIso: getParkTodayIso(),
    maxHorizonDays: MAX_BOOKING_HORIZON_DAYS,
    maxHorizonDate: maxHorizonDateKey(),
    discoveredAvailableDates: discovered,
    discoveredAvailableDatesSource: discovery.source,
    discoveredAvailableDatesCount: discovered.length,
    lastDiscoveryRunAt: discovery.lastDiscoveryRunAt,
    lastBackfillAvailableDatesResult: collectorState.lastBackfillAvailableDatesResult,
    datesWithBasicRows,
    datesWithVerifiedDetails,
    ...detailCounts,
    missingDiscoveredDates,
    datesAttempted,
    datesAttemptedCount: datesAttempted.length,
    datesSucceeded,
    datesSucceededCount: datesSucceeded.length,
    datesFailed,
    datesFailedCount: datesFailed.length,
    failureReasonCounts,
    discoveryDiagnostics: collectorState.discoveryDiagnostics,
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
    ...thresholdCoverage,
    lastThresholdScanResult: collectorState.lastThresholdScanResult,
    ...thresholdStabilityDebugPayload(),
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
  if (reason === 'api_sessions_date' || String(reason).startsWith('api_sessions')) return;
  if (!BACKGROUND_DETAIL_ENRICHMENT_ENABLED) return;
  const needing = sessionsMissingDetails(sessions).filter(s => sessionDetailQueueEligible(s));
  if (!needing.length) return;
  const priority = isTodayOrTomorrowIso(isoDate) ? 1 : 2;
  enqueueSessionsForEnrichment(needing, { priority, reason: `${reason}:${isoDate}` }).catch(console.error);
  scheduleDetailQueueDrain({ reason: `${reason}:${isoDate}`, limit: Math.min(needing.length, DETAIL_ENRICH_MAX_PER_RUN) });
}

function detailAttemptCountOnSession(s) {
  if (!s) return 0;
  return s.detailAttemptCount ?? s.detail_attempt_count ?? s.raw?.detailAttemptCount ?? 0;
}

function lastDetailAttemptAtOnSession(s) {
  return s?.lastDetailAttemptAt ?? s?.last_detail_attempt_at ?? s?.raw?.lastDetailAttemptAt ?? null;
}

function nextDetailRetryAtOnSession(s) {
  return s?.nextDetailRetryAt ?? s?.next_detail_retry_at ?? s?.raw?.nextDetailRetryAt ?? null;
}

function computeNextDetailRetryAt(attemptCount) {
  const delay = Math.min(DETAIL_RETRY_BASE_MS * (2 ** Math.min(attemptCount, 8)), DETAIL_RETRY_MAX_MS);
  return new Date(Date.now() + delay).toISOString();
}

function isDetailRetryableFailureStatus(status) {
  if (!status || status === 'pending' || status === 'unknown') return true;
  if (status === 'checking') return false;
  if (status === 'checked_available_no_slot_count' || status === 'checked_open_no_slots_visible') return true;
  return isDetailFailureStatus(status);
}

function isDetailPermanentFailure(s) {
  if (!s?.key) return false;
  if (sessionDetailVerified(s)) return false;
  return detailAttemptCountOnSession(s) >= DETAIL_MAX_ATTEMPTS;
}

function isDetailReadyForQueueRetry(s) {
  const nextRetry = nextDetailRetryAtOnSession(s);
  if (!nextRetry) return true;
  return new Date(nextRetry).getTime() <= Date.now();
}

function sessionDetailQueueEligible(s, { force = false, availabilityChanged = false } = {}) {
  if (!s?.key) return false;
  if (force) return s.available !== false;

  const watchKeys = watchedSessionKeys();
  const priority = enrichmentPriorityForSession(s);
  const days = daysFromToday(s.isoDate || s.dateKey || todayDateKey());
  const status = effectiveDetailStatus(s);

  if (thresholdSlotsTrusted(s) && !watchKeys.has(s.key)) return false;
  if (!BACKGROUND_DETAIL_ENRICHMENT_ENABLED && !watchKeys.has(s.key) && !force) return false;

  if (!s.available && !watchKeys.has(s.key)) return false;
  if (status === 'checking') return false;
  if (isDetailPermanentFailure(s)) return false;
  if (!isDetailReadyForQueueRetry(s)) return false;

  if (sessionDetailVerified(s) && sessionHasDetailedData(s)) {
    if (!sessionNeedsDetailEnrichment(s, detailStaleMaxAgeHours(priority))) return false;
  }

  if (availabilityChanged || sessionsNeedingDetailAfterBasic.has(s.key)) return true;
  if (watchKeys.has(s.key)) {
    return !sessionDetailVerified(s) || sessionNeedsDetailEnrichment(s, detailStaleMaxAgeHours(1));
  }
  if (days <= 2) {
    return !sessionDetailVerified(s) || sessionNeedsDetailEnrichment(s, detailStaleMaxAgeHours(1));
  }
  if (!sessionHasDetailedData(s)) return true;
  if (isDetailRetryableFailureStatus(status)) return true;
  if (status === 'pending' || !status) return true;
  return sessionNeedsDetailEnrichment(s, detailStaleMaxAgeHours(priority));
}

function sessionQualifiesForDetailEnrichment(s, opts = {}) {
  return sessionDetailQueueEligible(s, opts);
}

function recordDetailAttemptOnSession(entry, { success = false } = {}) {
  if (!entry) return;
  const now = new Date().toISOString();
  const count = detailAttemptCountOnSession(entry) + 1;
  entry.detailAttemptCount = count;
  entry.detail_attempt_count = count;
  entry.lastDetailAttemptAt = now;
  entry.last_detail_attempt_at = now;
  if (success || sessionDetailVerified(entry)) {
    entry.nextDetailRetryAt = null;
    entry.next_detail_retry_at = null;
  } else {
    entry.nextDetailRetryAt = computeNextDetailRetryAt(count);
    entry.next_detail_retry_at = entry.nextDetailRetryAt;
  }
}

function buildDateDetailQueueDiagnostics(isoDate, sessions) {
  const list = asSessionArray(sessions);
  const open = list.filter(s => s.available !== false);
  const verified = open.filter(s => sessionDetailVerified(s));
  const pending = open.filter(s => {
    const st = effectiveDetailStatus(s);
    return (st === 'pending' || !st || st === 'unknown') && !sessionDetailVerified(s);
  });
  const failed = open.filter(s => isDetailFailureStatus(effectiveDetailStatus(s)));
  const retryableFailed = failed.filter(s => !isDetailPermanentFailure(s));
  const permanentFailed = failed.filter(s => isDetailPermanentFailure(s));
  const queueEligible = open.filter(s => sessionDetailQueueEligible(s));
  const withVerifiedSlots = verified.filter(s => s.slots != null);

  return {
    sessionsCount: list.length,
    openSessionsCount: open.length,
    detailVerifiedCount: verified.length,
    detailPendingCount: pending.length,
    detailRetryableFailedCount: retryableFailed.length,
    detailPermanentFailedCount: permanentFailed.length,
    sessionsWithSlotsCount: withVerifiedSlots.length,
    detailQueueEligibleCount: queueEligible.length,
    nextDetailRetrySample: open
      .filter(s => nextDetailRetryAtOnSession(s))
      .sort((a, b) => (nextDetailRetryAtOnSession(a) || '').localeCompare(nextDetailRetryAtOnSession(b) || ''))
      .slice(0, 8)
      .map(s => ({
        ...sessionDetailDiagnosticsFields(s),
        detail_verified: sessionDetailVerified(s),
        detail_attempt_count: detailAttemptCountOnSession(s),
        next_detail_retry_at: nextDetailRetryAtOnSession(s),
        slots: s.slots,
        capacity: s.capacity,
      })),
    verifiedDetailSample: verified.slice(0, 8).map(s => ({
      ...sessionDetailDiagnosticsFields(s),
      detail_verified: true,
      detail_confidence: s.detailConfidence || s.raw?.detailConfidence || null,
      slots: s.slots,
      capacity: s.capacity,
      estimatedBooked: s.estimatedBooked,
    })),
    failedDetailSample: failed.slice(0, 8).map(s => ({
      ...sessionDetailDiagnosticsFields(s),
      detail_attempt_count: detailAttemptCountOnSession(s),
      next_detail_retry_at: nextDetailRetryAtOnSession(s),
      detail_verified: false,
    })),
  };
}

function computeDetailCountsByDate(dates) {
  const verifiedDetailCountsByDate = {};
  const pendingDetailCountsByDate = {};
  const failedDetailCountsByDate = {};
  for (const d of asSessionArray(dates)) {
    const rows = allStoredSessions().filter(s => sessionDateKey(s) === d);
    verifiedDetailCountsByDate[d] = rows.filter(s => sessionDetailVerified(s)).length;
    pendingDetailCountsByDate[d] = rows.filter(s => {
      const st = effectiveDetailStatus(s);
      return s.available !== false && (st === 'pending' || !st || st === 'unknown') && !sessionDetailVerified(s);
    }).length;
    failedDetailCountsByDate[d] = rows.filter(s =>
      s.available !== false && isDetailFailureStatus(effectiveDetailStatus(s)),
    ).length;
  }
  return { verifiedDetailCountsByDate, pendingDetailCountsByDate, failedDetailCountsByDate };
}

function scheduleDetailQueueDrain({ reason = 'queue_drain', limit = DETAIL_ENRICH_MAX_PER_RUN, delayMs = DETAIL_QUEUE_DRAIN_DELAY_MS } = {}) {
  if (collectorState.detailQueueDrainScheduled) return;
  collectorState.detailQueueDrainScheduled = true;
  setTimeout(() => {
    collectorState.detailQueueDrainScheduled = false;
    processDetailEnrichmentQueue({ limit, reason }).catch(console.error);
  }, delayMs);
}

async function processDetailEnrichmentQueue({ isoDate = null, limit = DETAIL_ENRICH_MAX_PER_RUN, reason = 'detail_queue' } = {}) {
  await ensureSessionsForStatus();
  let targets;
  if (isoDate) {
    targets = sessionsForDate(isoDate)
      .filter(s => s.available !== false && sessionDetailQueueEligible(s))
      .sort((a, b) => enrichmentPriorityForSession(a) - enrichmentPriorityForSession(b))
      .slice(0, limit);
    if (targets.length) {
      await enqueueSessionsForEnrichment(targets, {
        priority: enrichmentPriorityForSession(targets[0]),
        reason: `${reason}:${isoDate}`,
      });
    }
  } else {
    targets = await pickSessionsForDetailEnrichment({ isoDate, limit });
  }

  if (!targets.length) {
    return emptyEnrichmentStats({ skipReason: 'no_queue_eligible_sessions', isoDate });
  }

  return runDetailEnrichment({ isoDate, sessions: targets, reason });
}

async function processAllAvailableDetailQueue({ limitPerDate = 20, reason = 'admin_enrich_all_available' } = {}) {
  await ensureSessionsForStatus();
  const dates = getDiscoveredAvailableDates();
  const fallbackDates = dates.length
    ? dates
    : [...new Set(allStoredSessions().map(sessionDateKey).filter(Boolean))].sort();
  const report = {
    limitPerDate,
    datesProcessed: 0,
    dateResults: [],
    totalSessionsAttempted: 0,
    totalDetailRowsVerified: 0,
    totalDetailRowsSuppressed: 0,
    durationMs: 0,
    skipped: false,
    skipReason: null,
  };
  const started = Date.now();

  for (const isoDate of fallbackDates) {
    const openOnDate = sessionsForDate(isoDate).filter(s => s.available !== false);
    if (!openOnDate.length) continue;
    await enqueueSessionsForEnrichment(
      openOnDate.filter(s => sessionDetailQueueEligible(s)),
      { priority: enrichmentPriorityForSession(openOnDate[0]), reason: `${reason}:${isoDate}` },
    );
    const result = await processDetailEnrichmentQueue({ isoDate, limit: limitPerDate, reason: `${reason}:${isoDate}` });
    report.dateResults.push({ isoDate, ...result });
    report.totalSessionsAttempted += result.sessionsAttempted ?? 0;
    report.totalDetailRowsVerified += result.detailRowsVerified ?? 0;
    report.totalDetailRowsSuppressed += result.detailRowsSuppressed ?? 0;
    report.datesProcessed++;
    if (result.skipped && (result.skipReason === 'scrape_in_progress' || result.skipReason === 'detail_enrichment_busy')) {
      report.skipped = true;
      report.skipReason = result.skipReason;
      break;
    }
  }

  report.durationMs = Date.now() - started;
  collectorState.lastDetailQueueBatch = { ...report, completedAt: new Date().toISOString() };
  return report;
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
    ...buildDateDetailQueueDiagnostics(isoDate, list),
    ...buildDateThresholdDiagnostics(isoDate, list),
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
    detailAttemptCount: raw.detailAttemptCount ?? raw.detail_attempt_count ?? 0,
    lastDetailAttemptAt: raw.lastDetailAttemptAt ?? raw.last_detail_attempt_at ?? null,
    nextDetailRetryAt: raw.nextDetailRetryAt ?? raw.next_detail_retry_at ?? null,
    detailSourceSessionKey: raw.detailSourceSessionKey ?? null,
    detailSourceIsoDate: raw.detailSourceIsoDate ?? null,
    detailSourceStartTime: raw.detailSourceStartTime ?? null,
    detailSourceSessionType: raw.detailSourceSessionType ?? null,
    detailSourceWaveSide: raw.detailSourceWaveSide ?? null,
    tileText: raw.tileText ?? null,
    thresholdInferredSlots: raw.thresholdInferredSlots ?? null,
    thresholdMaxVisible: raw.thresholdMaxVisible ?? null,
    thresholdScanVerified: raw.thresholdScanVerified ?? false,
    thresholdScanAt: raw.thresholdScanAt ?? null,
    thresholdScanMaxTested: raw.thresholdScanMaxTested ?? null,
    thresholdScanMethod: raw.thresholdScanMethod ?? null,
    thresholdConfidence: raw.thresholdConfidence ?? null,
    thresholdDiagnostics: raw.thresholdDiagnostics ?? null,
    modalSlots: raw.modalSlots ?? null,
    thresholdSlots: raw.thresholdSlots ?? null,
    slotsAgree: raw.slotsAgree ?? null,
    available_entries: raw.available_entries ?? null,
    slot_status: raw.slot_status ?? null,
    slot_source: raw.slot_source ?? null,
    threshold_scan_verified: raw.threshold_scan_verified ?? false,
    threshold_scan_at: raw.threshold_scan_at ?? null,
    expectedCapacity: raw.expectedCapacity ?? null,
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
      detailAttemptCount: s.detailAttemptCount ?? s.detail_attempt_count ?? 0,
      lastDetailAttemptAt: s.lastDetailAttemptAt ?? s.last_detail_attempt_at ?? null,
      nextDetailRetryAt: s.nextDetailRetryAt ?? s.next_detail_retry_at ?? null,
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
      thresholdInferredSlots: s.thresholdInferredSlots ?? null,
      thresholdMaxVisible: s.thresholdMaxVisible ?? null,
      thresholdScanVerified: s.thresholdScanVerified ?? false,
      thresholdScanAt: s.thresholdScanAt ?? null,
      thresholdScanMaxTested: s.thresholdScanMaxTested ?? null,
      thresholdScanMethod: s.thresholdScanMethod ?? null,
      thresholdConfidence: s.thresholdConfidence ?? null,
      thresholdDiagnostics: s.thresholdDiagnostics ?? null,
      modalSlots: s.modalSlots ?? null,
      thresholdSlots: s.thresholdSlots ?? null,
      slotsAgree: s.slotsAgree ?? null,
      available_entries: s.available_entries ?? null,
      slot_status: s.slot_status ?? null,
      slot_source: s.slot_source ?? null,
      threshold_scan_verified: s.threshold_scan_verified ?? false,
      threshold_scan_at: s.threshold_scan_at ?? null,
      expectedCapacity: s.expectedCapacity ?? null,
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

function bodyHasConsentBannerCopy(text) {
  if (!text) return false;
  const low = String(text).replace(/\s+/g, ' ').trim().toLowerCase();
  if (low.includes('this website uses cookies')) return true;
  if (low.includes('allow cookies') && low.includes('refuse cookies')) return true;
  if (low.includes('allow cookies') && low.includes('refuse')) return true;
  if (low.includes('accept cookies') && low.includes('cookie')) return true;
  return false;
}

function isWeakCookiePolicyNavOnly(text) {
  if (!text) return false;
  const low = String(text).replace(/\s+/g, ' ').trim().toLowerCase();
  if (!low.includes('cookie')) return false;
  if (bodyHasConsentBannerCopy(low)) return false;
  return low.includes('cookie policy') || /^cookie policy menu\b/.test(low);
}

function isCookieBannerText(text) {
  if (!text) return false;
  if (isWeakCookiePolicyNavOnly(text)) return false;
  const low = String(text).toLowerCase();
  return bodyHasConsentBannerCopy(low)
    || (low.includes('refuse cookies') && low.includes('allow cookies'));
}

function cookieConsentBannerVisibleScript() {
  return () => {
    function norm(t) {
      return String(t || '').replace(/\s+/g, ' ').trim().toLowerCase();
    }
    function hasConsentCopy(text) {
      const t = norm(text);
      if (t.includes('this website uses cookies')) return true;
      if (t.includes('allow cookies') && t.includes('refuse cookies')) return true;
      if (t.includes('allow cookies') && t.includes('refuse')) return true;
      if (t.includes('accept cookies') && t.includes('cookie')) return true;
      return false;
    }
    function isPolicyNavOnly(text) {
      const t = norm(text);
      if (!t.includes('cookie')) return false;
      if (hasConsentCopy(t)) return false;
      return t.includes('cookie policy');
    }
    function elementVisible(el) {
      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style.display !== 'none'
        && style.visibility !== 'hidden'
        && parseFloat(style.opacity || '1') > 0.05
        && rect.width > 40
        && rect.height > 20;
    }
    function hasConsentButtons(root) {
      let hasAllow = false;
      let hasRefuse = false;
      for (const el of root.querySelectorAll('button, a, [role="button"], input[type="button"], input[type="submit"]')) {
        if (!elementVisible(el)) continue;
        const t = norm(el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || '');
        if (/\ballow cookies\b/.test(t) || /\baccept cookies\b/.test(t) || t === 'accept') hasAllow = true;
        if (/\brefuse cookies\b/.test(t) || /\breject all\b/.test(t)) hasRefuse = true;
      }
      return hasAllow && hasRefuse;
    }

    const selectors = [
      '[class*="cookie" i]', '[id*="cookie" i]', '[class*="consent" i]', '[id*="consent" i]',
      '[class*="gdpr" i]', '[id*="gdpr" i]', '[class*="cc-" i]', '.cc-window', '#cookie-law-info-bar',
      '[class*="CookieConsent" i]', '#CybotCookiebotDialog', '#onetrust-banner-sdk',
      'dialog', '[role="dialog"]', '[role="alertdialog"]',
    ];

    for (const sel of selectors) {
      for (const el of document.querySelectorAll(sel)) {
        if (!elementVisible(el)) continue;
        const text = el.innerText || el.textContent || '';
        if (isPolicyNavOnly(text)) continue;
        if (hasConsentCopy(text)) return true;
        if (hasConsentButtons(el)) return true;
      }
    }

    const body = document.body?.innerText || '';
    if (isPolicyNavOnly(body) && !hasConsentCopy(body)) return false;
    return hasConsentCopy(body) && (norm(body).includes('allow cookies') || norm(body).includes('refuse cookies'));
  };
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

const THRESHOLD_SESSION_FIELDS = [
  'thresholdInferredSlots',
  'thresholdMaxVisible',
  'thresholdScanVerified',
  'thresholdScanAt',
  'thresholdScanMaxTested',
  'thresholdScanMethod',
  'thresholdConfidence',
  'thresholdDiagnostics',
  'modalSlots',
  'thresholdSlots',
  'slotsAgree',
  'available_entries',
  'slot_status',
  'slot_source',
  'threshold_scan_verified',
  'threshold_scan_at',
  'expectedCapacity',
];

function thresholdFieldOnSession(s, field) {
  if (!s) return null;
  if (s[field] != null) return s[field];
  if (s.raw && s.raw[field] != null) return s.raw[field];
  return null;
}

function thresholdConfidenceOnSession(s) {
  return thresholdFieldOnSession(s, 'thresholdConfidence');
}

function sessionThresholdScanVerified(s) {
  if (!s) return false;
  return s.thresholdScanVerified === true
    || s.raw?.thresholdScanVerified === true
    || s.threshold_scan_verified === true
    || s.raw?.threshold_scan_verified === true;
}

function getThresholdInferredSlots(s) {
  const v = thresholdFieldOnSession(s, 'thresholdInferredSlots');
  return v == null ? null : Number(v);
}

function getThresholdScanMaxTested(s) {
  const v = thresholdFieldOnSession(s, 'thresholdScanMaxTested');
  return v == null ? THRESHOLD_SCAN_MAX_DEFAULT : Number(v);
}

function thresholdSlotsTrusted(s) {
  if (!sessionThresholdScanVerified(s)) return false;
  const slotSource = thresholdFieldOnSession(s, 'slot_source');
  if (slotSource && slotSource !== 'entries_left_threshold_scan') return false;
  const conf = thresholdConfidenceOnSession(s) || thresholdFieldOnSession(s, 'slot_status');
  return conf === 'exact' || conf === 'at_least';
}

function normalizeTimeForMatch(time) {
  if (!time || time === '?') return null;
  return String(time).replace(/\s+/g, ' ').trim().toLowerCase();
}

function resolveTrustedSlotDisplay(s) {
  const modalVerified = sessionDetailVerified(s);
  const modalSlots = modalVerified && !isDefaultLikeDetailValues(s.slots, s.capacity, s.estimatedBooked)
    ? s.slots
    : null;

  if (thresholdSlotsTrusted(s)) {
    const inferred = getThresholdInferredSlots(s)
      ?? thresholdFieldOnSession(s, 'available_entries');
    const conf = thresholdConfidenceOnSession(s)
      || thresholdFieldOnSession(s, 'slot_status');
    const maxTested = getThresholdScanMaxTested(s);
    const slotSource = thresholdFieldOnSession(s, 'slot_source') || 'entries_left_threshold_scan';
    if (inferred != null) {
      return {
        slots: inferred,
        source: slotSource === 'entries_left_threshold_scan' ? 'entries_left_threshold_scan' : 'threshold',
        confidence: conf,
        atLeast: conf === 'at_least',
        slotsDisplay: conf === 'at_least' ? `${maxTested}+` : String(inferred),
        thresholdScanMaxTested: maxTested,
      };
    }
  }

  if (modalVerified && modalSlots != null) {
    return {
      slots: modalSlots,
      source: 'modal',
      confidence: 'exact',
      atLeast: false,
      slotsDisplay: String(modalSlots),
      thresholdScanMaxTested: null,
    };
  }

  return {
    slots: null,
    source: null,
    confidence: null,
    atLeast: false,
    slotsDisplay: null,
    thresholdScanMaxTested: null,
  };
}

function slotsComparisonFields(s) {
  const modalSlots = sessionDetailVerified(s) && s.slots != null ? s.slots : null;
  const thresholdSlots = thresholdSlotsTrusted(s) ? getThresholdInferredSlots(s) : null;
  let slotsAgree = null;
  if (modalSlots != null && thresholdSlots != null) {
    if (thresholdConfidenceOnSession(s) === 'at_least') {
      slotsAgree = modalSlots >= thresholdSlots;
    } else {
      slotsAgree = modalSlots === thresholdSlots;
    }
  }
  return { modalSlots, thresholdSlots, slotsAgree };
}

function thresholdDiagnosticsFields(s) {
  const cmp = slotsComparisonFields(s);
  return {
    key: s.key,
    isoDate: sessionDateKey(s),
    time: s.time,
    level: s.level,
    waveSide: s.waveSide,
    thresholdInferredSlots: getThresholdInferredSlots(s),
    thresholdMaxVisible: thresholdFieldOnSession(s, 'thresholdMaxVisible'),
    thresholdConfidence: thresholdConfidenceOnSession(s),
    thresholdScanVerified: sessionThresholdScanVerified(s),
    thresholdScanAt: thresholdFieldOnSession(s, 'thresholdScanAt'),
    thresholdScanMaxTested: getThresholdScanMaxTested(s),
    thresholdDiagnostics: thresholdFieldOnSession(s, 'thresholdDiagnostics'),
    ...cmp,
  };
}

function inferThresholdSlotsFromMaxVisible(maxVisible, maxTested, { inBasicScrape = false } = {}) {
  if (!maxVisible || maxVisible < 1) {
    if (inBasicScrape) {
      return {
        thresholdInferredSlots: null,
        thresholdMaxVisible: 0,
        thresholdConfidence: 'no_match',
        thresholdScanVerified: false,
        reason: 'not_available_under_filter',
      };
    }
    return {
      thresholdInferredSlots: null,
      thresholdMaxVisible: null,
      thresholdConfidence: 'no_match',
      thresholdScanVerified: false,
      reason: 'not_seen_at_any_threshold',
    };
  }
  if (maxVisible >= maxTested) {
    return {
      thresholdInferredSlots: maxVisible,
      thresholdMaxVisible: maxVisible,
      thresholdConfidence: 'at_least',
      thresholdScanVerified: true,
      reason: 'visible_through_max_threshold',
    };
  }
  return {
    thresholdInferredSlots: maxVisible,
    thresholdMaxVisible: maxVisible,
    thresholdConfidence: 'exact',
    thresholdScanVerified: true,
    reason: 'disappeared_after_threshold',
  };
}

function applyThresholdFieldsToSession(entry, inference, {
  maxTested,
  diagnostics = null,
  overwriteModalSlots = false,
} = {}) {
  const now = new Date().toISOString();
  entry.thresholdInferredSlots = inference.thresholdInferredSlots ?? null;
  entry.thresholdMaxVisible = inference.thresholdMaxVisible ?? null;
  entry.thresholdScanVerified = inference.thresholdScanVerified === true;
  entry.thresholdScanAt = now;
  entry.thresholdScanMaxTested = maxTested;
  entry.thresholdScanMethod = 'entries_left_filter';
  entry.thresholdConfidence = inference.thresholdConfidence || 'failed';
  entry.thresholdDiagnostics = diagnostics || inference.reason || null;
  entry.expectedCapacity = inference.expectedCapacity ?? null;

  if (thresholdSlotsTrusted(entry)) {
    entry.available_entries = entry.thresholdInferredSlots;
    entry.slot_status = entry.thresholdConfidence === 'at_least' ? 'at_least' : 'exact';
    entry.slot_source = 'entries_left_threshold_scan';
    entry.threshold_scan_verified = true;
    entry.threshold_scan_at = now;
  } else {
    entry.available_entries = null;
    entry.slot_status = entry.thresholdConfidence === 'no_match' ? 'no_match' : null;
    entry.slot_source = null;
    entry.threshold_scan_verified = false;
    entry.threshold_scan_at = now;
  }

  const modalVerified = sessionDetailVerified(entry);
  const modalSlots = modalVerified ? entry.slots : null;
  const thresholdSlots = thresholdSlotsTrusted(entry) ? entry.thresholdInferredSlots : null;
  entry.modalSlots = modalSlots;
  entry.thresholdSlots = thresholdSlots;
  entry.slotsAgree = slotsComparisonFields(entry).slotsAgree;

  if (!modalVerified && thresholdSlotsTrusted(entry) && overwriteModalSlots !== false) {
    if (entry.thresholdConfidence === 'exact' || entry.thresholdConfidence === 'at_least') {
      entry.slots = entry.thresholdInferredSlots;
    }
  }
}

function matchThresholdTileToSessions(thresholdTile, candidates) {
  const list = asSessionArray(candidates);
  if (!thresholdTile?.key) return { confidence: 'failed', reason: 'missing_threshold_tile_key' };

  const keyMatches = list.filter(s => s.key === thresholdTile.key);
  if (keyMatches.length === 1) {
    return { session: keyMatches[0], confidence: 'exact_key', matchMethod: 'session_key' };
  }
  if (keyMatches.length > 1) {
    return {
      confidence: 'ambiguous',
      reason: 'duplicate_session_key',
      candidates: keyMatches,
      sample: keyMatches.slice(0, 3).map(s => ({ key: s.key, time: s.time, level: s.level, waveSide: s.waveSide })),
    };
  }

  const tileTime = normalizeTimeForMatch(thresholdTile.timeLabel || thresholdTile.time);
  const tileCode = thresholdTile.sessionCode || levelToSessionCode(thresholdTile.level);
  const identityMatches = list.filter((s) => {
    const sameDate = sessionDateKey(s) === thresholdTile.isoDate;
    const sameTime = tileTime && normalizeTimeForMatch(s.time) === tileTime;
    const sameType = (s.level || '').trim().toLowerCase() === (thresholdTile.level || '').trim().toLowerCase();
    const sameCode = tileCode && levelToSessionCode(s.level) === tileCode;
    const sameSide = !s.waveSide || !thresholdTile.waveSide
      || normalizeWaveSideShort(s.waveSide) === normalizeWaveSideShort(thresholdTile.waveSide);
    return sameDate && sameTime && (sameType || sameCode) && sameSide;
  });

  if (identityMatches.length === 1) {
    return {
      session: identityMatches[0],
      confidence: 'identity_match',
      matchMethod: 'iso_date_time_type_side',
    };
  }
  if (identityMatches.length > 1) {
    return {
      confidence: 'ambiguous',
      reason: 'multiple_identity_matches',
      candidates: identityMatches,
      sample: identityMatches.slice(0, 3).map(s => ({
        key: s.key,
        time: s.time,
        level: s.level,
        waveSide: s.waveSide,
        tileText: s.tileText,
      })),
    };
  }

  return { confidence: 'no_match', reason: 'no_matching_basic_session' };
}

function buildDateThresholdDiagnostics(isoDate, sessions) {
  const list = asSessionArray(sessions);
  const verified = list.filter(s => sessionThresholdScanVerified(s));
  const exact = list.filter(s => thresholdConfidenceOnSession(s) === 'exact');
  const atLeast = list.filter(s => thresholdConfidenceOnSession(s) === 'at_least');
  const ambiguous = list.filter(s => thresholdConfidenceOnSession(s) === 'ambiguous');
  const noMatch = list.filter(s => thresholdConfidenceOnSession(s) === 'no_match');
  const comparisons = list
    .filter(s => sessionDetailVerified(s) || sessionThresholdScanVerified(s))
    .map((s) => ({ key: s.key, ...slotsComparisonFields(s) }));
  const disagreementSample = comparisons.filter(c => c.slotsAgree === false).slice(0, 8);

  return {
    thresholdScanVerifiedCount: verified.length,
    thresholdExactCount: exact.length,
    thresholdAtLeastCount: atLeast.length,
    thresholdAmbiguousCount: ambiguous.length,
    thresholdNoMatchCount: noMatch.length,
    thresholdScanSample: verified.slice(0, 8).map(thresholdDiagnosticsFields),
    thresholdAmbiguousSample: ambiguous.slice(0, 8).map(thresholdDiagnosticsFields),
    thresholdDuplicateMatchSample: ambiguous
      .filter(s => (thresholdFieldOnSession(s, 'thresholdDiagnostics') || '').includes('duplicate'))
      .slice(0, 8)
      .map(thresholdDiagnosticsFields),
    modalThresholdComparisonSample: comparisons.slice(0, 8),
    disagreementSample,
  };
}

function computeThresholdCoverageByDate(dates) {
  const checkDates = asSessionArray(dates);
  const datesWithThresholdScans = [];
  const thresholdCountsByDate = {};
  for (const isoDate of checkDates) {
    const rows = sessionsForDate(isoDate);
    const scanned = rows.filter(s => thresholdFieldOnSession(s, 'thresholdScanAt'));
    if (!scanned.length) continue;
    datesWithThresholdScans.push(isoDate);
    thresholdCountsByDate[isoDate] = {
      total: rows.length,
      thresholdScanVerified: rows.filter(s => sessionThresholdScanVerified(s)).length,
      exact: rows.filter(s => thresholdConfidenceOnSession(s) === 'exact').length,
      atLeast: rows.filter(s => thresholdConfidenceOnSession(s) === 'at_least').length,
      ambiguous: rows.filter(s => thresholdConfidenceOnSession(s) === 'ambiguous').length,
      noMatch: rows.filter(s => thresholdConfidenceOnSession(s) === 'no_match').length,
      lastThresholdScanAt: scanned
        .map(s => thresholdFieldOnSession(s, 'thresholdScanAt'))
        .filter(Boolean)
        .sort()
        .pop() || null,
    };
  }
  return { datesWithThresholdScans: datesWithThresholdScans.sort(), thresholdCountsByDate };
}

function thresholdStabilityDebugPayload() {
  return {
    lastPageCrashAt: collectorState.lastPageCrashAt,
    lastPageCrashStage: collectorState.lastPageCrashStage,
    lastScrapeCrashAt: collectorState.lastScrapeCrashAt,
    lastScrapeCrashStage: collectorState.lastScrapeCrashStage,
    lastScrapeCrashTier: collectorState.lastScrapeCrashTier,
    lastThresholdScanWeek: collectorState.lastThresholdScanWeek,
    thresholdScanBatchProgress: collectorState.thresholdScanBatchProgress,
    thresholdScanWeeksRemaining: collectorState.thresholdScanWeeksRemaining,
    thresholdScanPendingWeeks: collectorState.thresholdScanPendingWeeks,
    thresholdScanCompletedWeeks: collectorState.thresholdScanCompletedWeeks,
    thresholdScanLastError: collectorState.thresholdScanLastError,
    thresholdScanRecovered: collectorState.thresholdScanRecovered,
    thresholdScanMaxWeeksPerRun: THRESHOLD_SCAN_MAX_WEEKS_PER_RUN,
    thresholdScanRecycleBrowserEachWeek: THRESHOLD_SCAN_RECYCLE_BROWSER_EACH_WEEK,
    backgroundThresholdScanEnabled: BACKGROUND_THRESHOLD_SCAN_ENABLED,
    backgroundDetailEnrichmentEnabled: BACKGROUND_DETAIL_ENRICHMENT_ENABLED,
    currentScrapeAgeSeconds: getScrapeLockAgeSeconds(),
    scrapeLockMaxMs: SCRAPE_LOCK_MAX_MS,
    scrapeLockManualReleaseMinMs: SCRAPE_LOCK_MANUAL_RELEASE_MIN_MS,
    scrapeLockReleasedAt: collectorState.scrapeLockReleasedAt,
    scrapeLockReleasedReason: collectorState.scrapeLockReleasedReason,
  };
}

function isPlaywrightCrashError(err) {
  const msg = (err?.message || String(err || '')).toLowerCase();
  return msg.includes('target crashed')
    || msg.includes('page crashed')
    || (msg.includes('page.evaluate') && (
      msg.includes('crashed')
      || msg.includes('closed')
      || msg.includes('destroyed')
      || msg.includes('protocol error')
    ))
    || msg.includes('target closed')
    || msg.includes('target page, context or browser has been closed')
    || msg.includes('browser has been closed')
    || msg.includes('browser disconnected')
    || msg.includes('browser closed')
    || msg.includes('execution context was destroyed')
    || msg.includes('protocol error')
    || msg.includes('navigation timeout')
    || msg.includes('context disposed')
    || msg.includes('net::err');
}

function getScrapeLockAgeMs() {
  if (!scrapeInProgress || !currentScrapeStartedAt) return null;
  return Date.now() - new Date(currentScrapeStartedAt).getTime();
}

function getScrapeLockAgeSeconds() {
  const ageMs = getScrapeLockAgeMs();
  return ageMs == null ? null : Math.round(ageMs / 1000);
}

function releaseScrapeLockIfStale() {
  if (!scrapeInProgress) return false;
  const ageMs = getScrapeLockAgeMs();
  if (ageMs != null && ageMs > SCRAPE_LOCK_MAX_MS) {
    console.warn(`  releasing stale scrape lock (tier ${currentScrapeTier}, age ${Math.round(ageMs / 1000)}s)`);
    releaseScrapeLock();
    collectorState.scrapeLockReleasedReason = 'stale_timeout';
    collectorState.scrapeLockReleasedAt = new Date().toISOString();
    return true;
  }
  return false;
}

function recordPageCrash(stage, err, { weekKey = null, failureReason = 'failed_page_crash', tier = null } = {}) {
  const now = new Date().toISOString();
  const message = err?.message || String(err);
  collectorState.lastPageCrashAt = now;
  collectorState.lastPageCrashStage = stage;
  collectorState.lastScrapeCrashAt = now;
  collectorState.lastScrapeCrashStage = stage;
  collectorState.thresholdScanRecovered = false;
  if (weekKey) collectorState.lastThresholdScanWeek = weekKey;
  if (tier != null) collectorState.lastScrapeCrashTier = tier;
  lastScrapeError = message;
  console.error(`  [playwright crash] stage=${stage} tier=${tier ?? currentScrapeTier ?? 'n/a'} week=${weekKey || 'n/a'} reason=${failureReason}: ${message}`);
  return failureReason;
}

function handlePlaywrightFailure(err, stage, { tier = null, weekKey = null } = {}) {
  recordScrapeError(err, stage);
  if (isPlaywrightCrashError(err)) {
    recordPageCrash(stage, err, { tier, weekKey });
  }
}

function markThresholdScanRecovered() {
  if (collectorState.lastPageCrashAt) {
    collectorState.thresholdScanRecovered = true;
  }
}

async function safeCloseBrowser(launched, { timeoutMs = BROWSER_CLOSE_TIMEOUT_MS } = {}) {
  if (!launched) return;
  try {
    await withTimeout((async () => {
      if (launched.page) await launched.page.close().catch(() => {});
      if (launched.context) await launched.context.close().catch(() => {});
      if (launched.browser) await launched.browser.close().catch(() => {});
    })(), timeoutMs, 'safeCloseBrowser');
  } catch (e) {
    console.warn(`  safeCloseBrowser: ${e.message}`);
  }
}

async function finalizeScrapeSession(launched, { releaseLock = true } = {}) {
  if (releaseLock) releaseScrapeLock();
  await safeCloseBrowser(launched);
}

async function withTimeout(promise, ms, label = 'operation') {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label}_timeout_after_${ms}ms`)), ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function withPlaywrightGuard(fn, {
  stage = 'playwright',
  timeout = THRESHOLD_TILE_SCRAPE_TIMEOUT_MS,
  weekKey = null,
  tier = null,
  page = null,
  auditContext = null,
} = {}) {
  try {
    return await withTimeout(Promise.resolve().then(fn), timeout, stage);
  } catch (e) {
    if (page) {
      try {
        const diag = await collectPageDiagnostics(page, `${stage}_timeout`);
        const artifacts = await captureThresholdDebugArtifacts(page, auditContext, stage, {
          screenshot: true,
          html: true,
        });
        Object.assign(diag, artifacts);
        e.timeoutDiagnostics = diag;
        pushThresholdAuditStep(auditContext, 'timeout_or_success', {
          ok: false,
          reason: e.message,
          stage,
          ...diag,
        });
      } catch {}
    }
    if (isPlaywrightCrashError(e)) {
      recordPageCrash(stage, e, { weekKey, tier });
    }
    throw e;
  }
}

function isLocalThresholdDebugArtifactsEnabled() {
  return process.env.RAILWAY_ENVIRONMENT == null && process.env.RAILWAY_PROJECT_ID == null;
}

function createThresholdAuditContext(options = {}) {
  return {
    auditTrail: [],
    debug: {
      debug: options.debug === true,
      trace: options.trace === true,
      screenshot: options.screenshot === true,
      headed: options.headed === true,
    },
    startedAt: Date.now(),
    traceContext: null,
    tracePath: null,
  };
}

function pushThresholdAuditStep(auditContext, step, data = {}) {
  if (!auditContext?.auditTrail) return;
  auditContext.auditTrail.push({
    step,
    at: new Date().toISOString(),
    elapsedMs: Date.now() - (auditContext.startedAt || Date.now()),
    ...data,
  });
}

function applyPageDiagnosticsTo(target, diagnostics = {}) {
  if (!target || !diagnostics) return target;
  for (const key of [
    'currentUrl', 'pageTitle', 'bodyTextLength', 'bodyTextSample',
    'calendarReadySignals', 'pageDiagnosticError', 'pageAvailable', 'pageUnavailableReason',
    'diagnosticLabel', 'screenshotPath', 'htmlSnapshotPath',
  ]) {
    if (diagnostics[key] != null) target[key] = diagnostics[key];
  }
  return target;
}

function assertPlaywrightPage(page, label) {
  if (!page || typeof page.evaluate !== 'function') {
    throw new Error(`${label}: expected Playwright page, got ${Object.prototype.toString.call(page)} keys=${page && typeof page === 'object' ? Object.keys(page).join(',') : String(page)}`);
  }
}

async function collectPageDiagnostics(page, label = 'unknown') {
  const out = {
    diagnosticLabel: label,
    pageAvailable: !!page,
    currentUrl: null,
    pageTitle: null,
    bodyTextLength: null,
    bodyTextSample: null,
    calendarReadySignals: null,
  };

  if (!page) {
    out.pageAvailable = false;
    out.pageUnavailableReason = 'page_object_missing';
    return out;
  }

  assertPlaywrightPage(page, `collectPageDiagnostics(${label})`);

  try { out.currentUrl = page.url?.() || null; } catch {
    out.pageUnavailableReason = out.pageUnavailableReason || 'current_url_unavailable';
  }
  try { out.pageTitle = await page.title(); } catch {
    out.pageUnavailableReason = out.pageUnavailableReason || 'page_title_unavailable';
  }

  try {
    const body = await page.evaluate(() => {
      const text = document.body?.innerText || '';
      return {
        bodyTextLength: text.length,
        bodyTextSample: text.slice(0, 1500),
        calendarReadySignals: {
          hasMonthYearText: /\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b/i.test(text),
          hasLeftWaveSessionsText: /Left Wave Sessions/i.test(text),
          hasRightWaveSessionsText: /Right Wave Sessions/i.test(text),
          hasEntriesLeftText: /Entries left/i.test(text),
          hasWeekdayText: /\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b/i.test(text),
          hasTimeRows: /\b\d{1,2}\s?(am|pm)\b/i.test(text),
        },
      };
    });
    Object.assign(out, body);
  } catch (err) {
    out.pageDiagnosticError = String(err?.stack || err?.message || err);
  }

  return out;
}

async function captureThresholdDebugArtifacts(page, auditContext, label, { screenshot = false, html = false } = {}) {
  if (!page || !isLocalThresholdDebugArtifactsEnabled()) return {};
  const wantScreenshot = screenshot || auditContext?.debug?.screenshot;
  const wantHtml = html || auditContext?.debug?.debug;
  if (!wantScreenshot && !wantHtml) return {};

  const safeLabel = String(label || 'step').replace(/[^\w.-]+/g, '_').slice(0, 80);
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const dir = THRESHOLD_DEBUG_ARTIFACTS_DIR;
  const artifacts = {};

  try {
    fs.mkdirSync(dir, { recursive: true });
    if (wantScreenshot) {
      const screenshotPath = path.join(dir, `${ts}_${safeLabel}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
      if (fs.existsSync(screenshotPath)) artifacts.screenshotPath = screenshotPath;
    }
    if (wantHtml) {
      const htmlSnapshotPath = path.join(dir, `${ts}_${safeLabel}.html`);
      const content = await page.content().catch(() => '');
      if (content) {
        fs.writeFileSync(htmlSnapshotPath, content, 'utf8');
        artifacts.htmlSnapshotPath = htmlSnapshotPath;
      }
    }
  } catch (err) {
    artifacts.artifactCaptureError = String(err?.message || err);
  }
  return artifacts;
}

async function enrichFailureWithPageDiagnostics(page, auditContext, stage, err) {
  const diagnostics = err?.timeoutDiagnostics
    || (page
      ? await collectPageDiagnostics(page, `${stage}_catch`)
      : {
        pageAvailable: false,
        pageUnavailableReason: 'page_object_missing',
        diagnosticLabel: `${stage}_catch`,
      });
  const artifacts = await captureThresholdDebugArtifacts(page, auditContext, stage, {
    screenshot: true,
    html: true,
  });
  const merged = { ...diagnostics, ...artifacts };
  pushThresholdAuditStep(auditContext, 'timeout_or_success', {
    ok: false,
    reason: err?.message || String(err),
    stage,
    ...merged,
  });
  return merged;
}

async function startThresholdPlaywrightTrace(auditContext, context) {
  if (!auditContext?.debug?.trace || !isLocalThresholdDebugArtifactsEnabled() || !context?.tracing) return;
  try {
    fs.mkdirSync(THRESHOLD_DEBUG_ARTIFACTS_DIR, { recursive: true });
    await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
    auditContext.traceContext = context;
  } catch (err) {
    pushThresholdAuditStep(auditContext, 'trace_start_failed', {
      ok: false,
      error: String(err?.message || err),
    });
  }
}

async function stopThresholdPlaywrightTrace(auditContext, label = 'threshold_scan') {
  if (!auditContext?.traceContext?.tracing) return null;
  try {
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const tracePath = path.join(THRESHOLD_DEBUG_ARTIFACTS_DIR, `${ts}_${label}.zip`);
    await auditContext.traceContext.tracing.stop({ path: tracePath });
    auditContext.tracePath = tracePath;
    return tracePath;
  } catch {
    return null;
  }
}

function buildThresholdBatches(minThreshold, maxThreshold, batchSize = THRESHOLD_SCAN_THRESHOLD_BATCH_SIZE) {
  const minT = Math.max(1, Number(minThreshold) || THRESHOLD_SCAN_MIN_DEFAULT);
  const maxT = Math.max(minT, Math.min(Number(maxThreshold) || THRESHOLD_SCAN_MAX_DEFAULT, THRESHOLD_SCAN_MAX_THRESHOLDS_PER_PAGE));
  const batches = [];
  for (let start = minT; start <= maxT; start += batchSize) {
    const end = Math.min(start + batchSize - 1, maxT);
    batches.push({
      batchIndex: batches.length,
      minThreshold: start,
      maxThreshold: end,
      thresholds: Array.from({ length: end - start + 1 }, (_, i) => start + i),
    });
  }
  return batches;
}

function getMondayWeekStartIso(isoDate) {
  const [y, m, d] = isoDate.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  const day = dt.getUTCDay();
  const daysSinceMonday = (day + 6) % 7;
  dt.setUTCDate(dt.getUTCDate() - daysSinceMonday);
  return dt.toISOString().slice(0, 10);
}

const CALENDAR_MONTH_NAME_TO_INDEX = {
  january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
  july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
};

function isoDateFromUtcParts(year, monthIndex, day) {
  if (!year || monthIndex == null || !day) return null;
  const dt = new Date(Date.UTC(year, monthIndex, day));
  if (dt.getUTCFullYear() !== year || dt.getUTCMonth() !== monthIndex || dt.getUTCDate() !== day) {
    return null;
  }
  return dt.toISOString().slice(0, 10);
}

function parseMonthYearLabel(monthLabel) {
  const text = String(monthLabel || '').replace(/\s+/g, ' ').trim();
  const m = text.match(/^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})$/i);
  if (!m) return null;
  const month = CALENDAR_MONTH_NAME_TO_INDEX[m[1].toLowerCase()];
  if (month == null) return null;
  return { year: parseInt(m[2], 10), month };
}

function parseMonthYearFromIso(isoDate) {
  if (!isoDate || !/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) return null;
  const [year, month] = isoDate.split('-').map(Number);
  return { year, month: month - 1 };
}

function compareMonthYear(a, b) {
  if (!a || !b) return null;
  if (a.year !== b.year) return a.year < b.year ? -1 : 1;
  if (a.month !== b.month) return a.month < b.month ? -1 : 1;
  return 0;
}

function monthLabelMatchesIsoDate(monthLabel, isoDate) {
  const labelMonth = parseMonthYearLabel(monthLabel);
  const targetMonth = parseMonthYearFromIso(isoDate);
  return compareMonthYear(labelMonth, targetMonth) === 0;
}

function monthLabelMatchesNavigationTargets(monthLabel, { requestedIsoDate, navigationIsoDate } = {}) {
  if (!monthLabel) return false;
  return (requestedIsoDate && monthLabelMatchesIsoDate(monthLabel, requestedIsoDate))
    || (navigationIsoDate && monthLabelMatchesIsoDate(monthLabel, navigationIsoDate));
}

function shouldAbortNavigationForEmptyDayHeaders(diag, { requestedIsoDate, navigationIsoDate } = {}) {
  const headersEmpty = !(diag.visibleIsoDatesFromHeaders?.length || 0);
  const dayHeadersEmpty = !(diag.rawDayHeaderTexts?.length || 0);
  const monthMatches = monthLabelMatchesNavigationTargets(diag.rawMonthLabel, {
    requestedIsoDate,
    navigationIsoDate,
  });
  return headersEmpty && dayHeadersEmpty && monthMatches;
}

function navigationDirectionForEmptyHeaders(diag, validationTarget) {
  const visibleMonth = parseMonthYearLabel(diag.rawMonthLabel);
  const targetMonth = parseMonthYearFromIso(validationTarget);
  if (!visibleMonth || !targetMonth) return null;
  const cmp = compareMonthYear(visibleMonth, targetMonth);
  if (cmp === 0) return 'stop';
  if (cmp < 0) return 'next';
  return 'prev';
}

function navigationDirectionForVisibleHeaders(visibleHeaders, validationTarget) {
  if (!visibleHeaders?.length || !validationTarget) return null;
  if (visibleHeaders.includes(validationTarget)) return null;
  const min = visibleHeaders[0];
  const max = visibleHeaders[visibleHeaders.length - 1];
  if (validationTarget < min) return 'prev';
  if (validationTarget > max) return 'next';
  return null;
}

function parseVisibleWeekFromMonthAndDayHeaders(monthLabel, dayHeaders) {
  const monthCtx = parseMonthYearLabel(monthLabel);
  if (!monthCtx || !Array.isArray(dayHeaders) || !dayHeaders.length) return [];

  let year = monthCtx.year;
  let month = monthCtx.month;
  let prevDay = null;
  const out = [];

  for (const header of dayHeaders) {
    const day = Number(header?.day);
    if (!Number.isFinite(day) || day < 1 || day > 31) continue;
    if (prevDay != null && day < prevDay) {
      month += 1;
      if (month > 11) {
        month = 0;
        year += 1;
      }
    }
    const iso = isoDateFromUtcParts(year, month, day);
    if (iso) out.push(iso);
    prevDay = day;
  }
  return out;
}

const SESSION_CODE_BY_LEVEL = {
  'advanced trick': 'AT',
  'advanced tricks': 'AT',
  'advanced': 'AT',
  'advanced beginner': 'AB',
  'expert trick': 'ET',
  'expert tricks': 'ET',
  'expert': 'ET',
  'expert beginner': 'EB',
  'progressive': 'PRG',
  'intermediate': 'INT',
  'pro turns': 'PT',
  'pro turn': 'PT',
  'progressive beginner': 'PB',
  'beginner': 'BGN',
  'cruiser': 'CRU',
  'lesson only': 'LO',
};

const EXPECTED_CAPACITY_BY_CODE = {
  PT: 10,
  PRG: 18,
};

const DEFAULT_EXPECTED_CAPACITY = 12;

function levelToSessionCode(level) {
  const norm = String(level || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!norm) return null;
  if (SESSION_CODE_BY_LEVEL[norm]) return SESSION_CODE_BY_LEVEL[norm];
  for (const [key, code] of Object.entries(SESSION_CODE_BY_LEVEL)) {
    if (norm.includes(key) || key.includes(norm)) return code;
  }
  const abbrev = norm.split(/\s+/).map(w => w[0]).join('').toUpperCase();
  return abbrev.length >= 2 && abbrev.length <= 4 ? abbrev : null;
}

function expectedCapacityForSessionCode(sessionCode, level) {
  const code = sessionCode || levelToSessionCode(level);
  if (code && EXPECTED_CAPACITY_BY_CODE[code] != null) return EXPECTED_CAPACITY_BY_CODE[code];
  if (code) return DEFAULT_EXPECTED_CAPACITY;
  return null;
}

function normalizeWaveSideShort(waveSide) {
  const low = String(waveSide || '').toLowerCase();
  if (/\bleft\b/.test(low)) return 'left';
  if (/\bright\b/.test(low)) return 'right';
  return null;
}

function makeThresholdIdentityKey(tile) {
  const side = normalizeWaveSideShort(tile.waveSide);
  const time = normalizeTimeForMatch(tile.timeLabel || tile.time);
  const code = tile.sessionCode || levelToSessionCode(tile.level || tile.sessionName);
  return `${tile.isoDate}|${time || '?'}|${side || '?'}|${code || '?'}`;
}

function buildThresholdsSeenForIdentity(identityKey, visibleByThreshold) {
  const seen = [];
  for (const [threshold, keys] of visibleByThreshold.entries()) {
    if (keys.has(identityKey)) seen.push(threshold);
  }
  return seen.sort((a, b) => a - b);
}

function inferSlotsFromThresholdPresence(thresholdsSeen, maxTested, {
  sessionCode = null,
  level = null,
  inBasicScrape = false,
} = {}) {
  const seen = [...new Set((thresholdsSeen || []).filter(t => Number.isFinite(t) && t >= 1))].sort((a, b) => a - b);
  if (!seen.length) {
    return inferThresholdSlotsFromMaxVisible(0, maxTested, { inBasicScrape });
  }
  const maxThresholdSeen = Math.max(...seen);
  const inference = inferThresholdSlotsFromMaxVisible(maxThresholdSeen, maxTested, { inBasicScrape });
  const expectedCapacity = expectedCapacityForSessionCode(sessionCode, level);
  if (expectedCapacity != null) inference.expectedCapacity = expectedCapacity;
  if (inference.thresholdConfidence === 'exact'
    && expectedCapacity != null
    && inference.thresholdInferredSlots != null
    && inference.thresholdInferredSlots > expectedCapacity) {
    return {
      thresholdInferredSlots: null,
      thresholdMaxVisible: maxThresholdSeen,
      thresholdConfidence: 'ambiguous',
      thresholdScanVerified: false,
      reason: 'exceeds_expected_capacity',
      expectedCapacity,
      thresholdsSeen: seen,
    };
  }
  inference.thresholdsSeen = seen;
  return inference;
}

function entriesLeftLabelMatchesThreshold(label, threshold) {
  if (!label) return false;
  const n = Number(threshold);
  if (!Number.isFinite(n)) return false;
  const text = String(label).replace(/\s+/g, ' ').trim();
  const patterns = [
    new RegExp(`entries?\\s*left\\s*:?\\s*${n}\\b`, 'i'),
    new RegExp(`at\\s*least\\s*${n}\\s*entries?\\s*left`, 'i'),
    new RegExp(`\\b${n}\\s*entries?\\s*left`, 'i'),
  ];
  return patterns.some((re) => re.test(text));
}

function entriesLeftSelectedLabelMatchesThreshold(label, threshold) {
  if (!label) return false;
  const n = Number(threshold);
  if (!Number.isFinite(n)) return false;
  const text = String(label).replace(/\s+/g, ' ').trim();
  return new RegExp(`^entries?\\s*left\\s*:\\s*${n}\\b`, 'i').test(text);
}

function exactAtLeastEntriesLeftOptionText(threshold) {
  const n = Math.max(1, Number(threshold) || 1);
  return `At least ${n} entries left`;
}

function expectedEntriesLeftSelectedLabel(threshold) {
  const n = Math.max(1, Number(threshold) || 1);
  return `Entries left : ${n}`;
}

function evaluateThresholdWriteSafety(report) {
  const scanned = (report.thresholdsScanned || Object.keys(report.visibleTileCountsByThreshold || {})
    .map((k) => Number(k))
    .filter((n) => Number.isFinite(n))
    .sort((a, b) => a - b));
  if (!scanned.length) {
    return {
      thresholdWriteSafe: false,
      thresholdWriteBlockReason: 'no_thresholds_scanned',
      statusReason: null,
    };
  }

  const presence = report.thresholdPresenceBySession || {};
  const identityKeys = Object.keys(presence);
  const maxScanned = Math.max(...scanned);
  let visibleAtAllCount = 0;
  for (const identityKey of identityKeys) {
    const seen = presence[identityKey] || [];
    const hasAll = scanned.every((t) => seen.includes(t));
    if (hasAll && seen.includes(1) && seen.includes(maxScanned)) visibleAtAllCount++;
  }
  const pctAtAll = identityKeys.length ? visibleAtAllCount / identityKeys.length : 0;

  const counts = scanned.map((t) => {
    const n = report.visibleTileCountsByThreshold?.[t];
    if (n != null) return n;
    const fromMap = report.visibleByThreshold?.[t];
    return typeof fromMap === 'number' ? fromMap : (fromMap?.size ?? 0);
  });
  let hasDecrease = false;
  for (let i = 1; i < counts.length; i++) {
    if (counts[i] < counts[i - 1]) hasDecrease = true;
  }
  const flatCounts = counts.length > 1 && counts.every((c) => c === counts[0] && c > 0);

  if (pctAtAll > 0.8 || (flatCounts && !hasDecrease)) {
    return {
      thresholdWriteSafe: false,
      thresholdWriteBlockReason: 'sessions_visible_at_all_thresholds',
      statusReason: 'threshold_filter_not_effective',
      pctVisibleAtAllThresholds: pctAtAll,
      hasTileCountDecrease: hasDecrease,
    };
  }

  return {
    thresholdWriteSafe: true,
    thresholdWriteBlockReason: null,
    statusReason: null,
    pctVisibleAtAllThresholds: pctAtAll,
    hasTileCountDecrease: hasDecrease,
  };
}

function sampleThresholdPresenceBySession(presenceBySession, limit = 12) {
  return Object.fromEntries(
    Object.entries(presenceBySession || {}).slice(0, limit),
  );
}

function classifyThresholdScanStatus(report) {
  if (report.statusReason === 'threshold_filter_not_effective') {
    return 'threshold_filter_not_effective';
  }
  if (report.statusReason === 'calendar_day_headers_not_parsed') {
    return 'calendar_day_headers_not_parsed';
  }
  if (report.targetDateVisibleFromHeaders === false) {
    const hasHeaders = (report.rawDayHeaderTexts?.length || 0) > 0
      || (report.visibleIsoDatesFromHeaders?.length || 0) > 0;
    if (!hasHeaders) return 'calendar_headers_not_ready';
    return 'date_not_visible_after_navigation';
  }
  if (report.thresholdScanStarted !== true) {
    return 'calendar_headers_not_ready';
  }
  if (report.thresholdWriteSafe === false) {
    return 'threshold_filter_not_effective';
  }
  if ((report.exactCount || 0) + (report.atLeastCount || 0) > 0) return 'scan_success_with_matches';
  if (report.emptyWeekButVisible || report.visibleTileCountAtThreshold1 === 0) {
    return 'visible_week_no_threshold_tiles';
  }
  if ((report.ambiguousCount || 0) > 0) return 'threshold_scan_ambiguous';
  return 'visible_week_no_threshold_tiles';
}

function buildThresholdHeaderDiagnostics(source = {}) {
  return {
    rawMonthLabel: source.rawMonthLabel ?? null,
    rawDayHeaderTexts: source.rawDayHeaderTexts || [],
    parsedDayHeaders: source.parsedDayHeaders || [],
    dayHeaderCandidateTexts: source.dayHeaderCandidateTexts || [],
    dayHeaderCandidateCount: source.dayHeaderCandidateCount ?? 0,
    dayHeaderParseSource: source.dayHeaderParseSource ?? null,
    bodyWeekdayTextSample: source.bodyWeekdayTextSample || [],
    bodyTextLinesSample: source.bodyTextLinesSample || [],
    weekdayLineMatches: source.weekdayLineMatches || [],
    combinedDayHeaderMatches: source.combinedDayHeaderMatches || [],
    headerParseStrategy: source.headerParseStrategy ?? null,
    visibleIsoDatesFromHeaders: source.visibleIsoDatesFromHeaders || [],
    headerScrapeAttempts: source.headerScrapeAttempts || [],
  };
}

function buildThresholdNavigationDiagnostics(source = {}) {
  return {
    targetIsoDate: source.targetIsoDate ?? source.navigationIsoDate ?? null,
    navigationIsoDate: source.navigationIsoDate ?? null,
    validateIsoDate: source.validateIsoDate ?? null,
    computedWeekStart: source.computedWeekStart ?? null,
    visibleWeekStart: source.visibleWeekStart ?? null,
    visibleWeekEnd: source.visibleWeekEnd ?? null,
    clickedNextWeekCount: source.clickedNextWeekCount ?? 0,
    targetDateVisibleFromHeaders: source.targetDateVisibleFromHeaders === true,
    targetDateVisible: source.targetDateVisible === true,
    navigationError: source.navigationError ?? null,
    currentUrl: source.currentUrl ?? null,
    pageTitle: source.pageTitle ?? null,
    bodyTextLength: source.bodyTextLength ?? null,
    bodyTextSample: source.bodyTextSample ?? null,
    calendarReadySignals: source.calendarReadySignals ?? null,
  };
}

function flattenThresholdScanApiResponse({
  scanResult = {},
  requestedIsoDate = null,
  computedWeekStart = null,
  navigationIsoDate = null,
  weekMode = true,
  dryRun = true,
  routeMeta = {},
} = {}) {
  const week = scanResult.dateResults?.[0] || scanResult;
  const nav = week.navigationDiagnostics || week.navigation || {};
  const header = week.headerDiagnostics || buildThresholdHeaderDiagnostics(week);
  const threshold = week.thresholdDiagnostics || {
    visibleByThreshold: week.visibleByThreshold || null,
    filterResults: week.filterResults || null,
    filterNormalization: week.filterNormalization || null,
    visibleTileCountAtThreshold1: week.visibleTileCountAtThreshold1 ?? null,
    thresholdPresenceBySession: week.thresholdPresenceBySession || null,
  };
  const flat = {
    ...routeMeta,
    ...scanResult,
    ...week,
    requestedIsoDate: requestedIsoDate || week.requestedIsoDate || routeMeta.isoDate || null,
    computedWeekStart: computedWeekStart || week.computedWeekStart || (requestedIsoDate ? getMondayWeekStartIso(requestedIsoDate) : null),
    navigationIsoDate: navigationIsoDate || week.navigationIsoDate || null,
    weekMode,
    dryRun,
    navigationDiagnostics: week.navigationDiagnostics || buildThresholdNavigationDiagnostics({ ...nav, ...week }),
    headerDiagnostics: week.headerDiagnostics || header,
    thresholdDiagnostics: week.thresholdDiagnostics || threshold,
    currentUrl: week.currentUrl ?? nav.currentUrl ?? null,
    pageTitle: week.pageTitle ?? nav.pageTitle ?? null,
    bodyTextLength: week.bodyTextLength ?? nav.bodyTextLength ?? null,
    bodyTextSample: week.bodyTextSample ?? nav.bodyTextSample ?? null,
    calendarReadySignals: week.calendarReadySignals ?? nav.calendarReadySignals ?? null,
    headerScrapeAttempts: week.headerScrapeAttempts ?? header.headerScrapeAttempts ?? [],
    rawMonthLabel: week.rawMonthLabel ?? header.rawMonthLabel ?? null,
    rawDayHeaderTexts: week.rawDayHeaderTexts ?? header.rawDayHeaderTexts ?? [],
    dayHeaderCandidateTexts: week.dayHeaderCandidateTexts ?? header.dayHeaderCandidateTexts ?? [],
    dayHeaderCandidateCount: week.dayHeaderCandidateCount ?? header.dayHeaderCandidateCount ?? 0,
    dayHeaderParseSource: week.dayHeaderParseSource ?? header.dayHeaderParseSource ?? null,
    bodyWeekdayTextSample: week.bodyWeekdayTextSample ?? header.bodyWeekdayTextSample ?? [],
    bodyTextLinesSample: week.bodyTextLinesSample ?? header.bodyTextLinesSample ?? [],
    weekdayLineMatches: week.weekdayLineMatches ?? header.weekdayLineMatches ?? [],
    combinedDayHeaderMatches: week.combinedDayHeaderMatches ?? header.combinedDayHeaderMatches ?? [],
    visibleIsoDatesFromHeaders: week.visibleIsoDatesFromHeaders ?? header.visibleIsoDatesFromHeaders ?? [],
    targetDateVisibleFromHeaders: week.targetDateVisibleFromHeaders === true,
    visibleTileCountAtThreshold1: week.visibleTileCountAtThreshold1 ?? null,
    earlyExitStage: week.earlyExitStage ?? scanResult.earlyExitStage ?? null,
    earlyExitReason: week.earlyExitReason ?? scanResult.earlyExitReason ?? null,
    headerParseError: week.headerParseError ?? nav.headerParseError ?? null,
    auditTrail: week.auditTrail ?? scanResult.auditTrail ?? [],
    tracePath: week.tracePath ?? scanResult.tracePath ?? null,
    pageAvailable: week.pageAvailable ?? null,
    pageUnavailableReason: week.pageUnavailableReason ?? null,
    thresholdWriteSafe: week.thresholdWriteSafe ?? null,
    thresholdWriteBlockReason: week.thresholdWriteBlockReason ?? null,
    thresholdStopReason: week.thresholdStopReason ?? null,
    thresholdScanMaxReached: week.thresholdScanMaxReached ?? null,
    filterDiagnosticsByThreshold: week.filterDiagnosticsByThreshold ?? [],
    selectedEntriesLeftLabelByThreshold: week.selectedEntriesLeftLabelByThreshold ?? {},
    visibleTileCountsByThreshold: week.visibleTileCountsByThreshold ?? {},
    tileIdentitySamplesByThreshold: week.tileIdentitySamplesByThreshold ?? {},
    thresholdPresenceBySessionSample: week.thresholdPresenceBySessionSample ?? {},
    statusReason: week.statusReason ?? scanResult.statusReason ?? null,
    error: week.error ?? scanResult.error ?? null,
    crashed: week.crashed ?? scanResult.crashed ?? false,
  };
  if (dryRun) {
    flat.debugResponseShape = true;
    flat.scanResultKeys = Object.keys(scanResult || {});
    flat.weekResultKeys = Object.keys(week || {});
  }
  return flat;
}

function buildWeekAnchorsFromDates(dates) {
  const weekMap = new Map();
  for (const isoDate of asSessionArray(dates).filter(Boolean).sort()) {
    const weekStart = getMondayWeekStartIso(isoDate);
    if (!weekMap.has(weekStart)) weekMap.set(weekStart, []);
    weekMap.get(weekStart).push(isoDate);
  }
  return [...weekMap.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([weekStart, weekDates]) => ({
      weekKey: weekStart,
      computedWeekStart: weekStart,
      anchorIsoDate: weekDates[0],
      requestedIsoDate: weekDates[weekDates.length - 1],
      weekDates,
    }));
}

function buildThresholdScanResumeQueue(dates, { resumeQueue = null, preferBasicSessions = true } = {}) {
  if (resumeQueue?.length) return resumeQueue;
  const pendingKeys = collectorState.thresholdScanPendingWeeks || [];
  if (pendingKeys.length) {
    return pendingKeys.map(weekKey => ({
      weekKey,
      computedWeekStart: weekKey,
      anchorIsoDate: weekKey,
    }));
  }
  let dateList = asSessionArray(dates).filter(Boolean);
  if (preferBasicSessions) {
    const today = getParkTodayIso();
    const fromStore = [...new Set(
      allStoredSessions()
        .map(sessionDateKey)
        .filter(d => d && d >= today),
    )];
    if (fromStore.length) {
      dateList = [...new Set([...dateList, ...fromStore])].sort();
    }
  }
  return buildWeekAnchorsFromDates(dateList);
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
  const trustedDisplay = resolveTrustedSlotDisplay(s);
  const parserOutput = out.detailParseOutput || out.raw?.detailParseOutput
    || buildParserOutputFromText(out.detailRawText || out.raw?.detailRawText || '');

  out.thresholdInferredSlots = getThresholdInferredSlots(s);
  out.thresholdMaxVisible = thresholdFieldOnSession(s, 'thresholdMaxVisible');
  out.thresholdScanVerified = sessionThresholdScanVerified(s);
  out.thresholdScanAt = thresholdFieldOnSession(s, 'thresholdScanAt');
  out.thresholdScanMaxTested = getThresholdScanMaxTested(s);
  out.thresholdScanMethod = thresholdFieldOnSession(s, 'thresholdScanMethod');
  out.thresholdConfidence = thresholdConfidenceOnSession(s);
  out.thresholdDiagnostics = thresholdFieldOnSession(s, 'thresholdDiagnostics');
  out.slotsSource = trustedDisplay.source;
  out.slotsAtLeast = trustedDisplay.atLeast;
  out.slotsDisplay = trustedDisplay.slotsDisplay;
  out.available_entries = thresholdFieldOnSession(s, 'available_entries') ?? getThresholdInferredSlots(s);
  out.slot_status = thresholdFieldOnSession(s, 'slot_status') ?? thresholdConfidenceOnSession(s);
  out.slot_source = thresholdFieldOnSession(s, 'slot_source');
  out.threshold_scan_verified = sessionThresholdScanVerified(s);
  out.threshold_scan_at = thresholdFieldOnSession(s, 'threshold_scan_at') ?? thresholdFieldOnSession(s, 'thresholdScanAt');
  const cmp = slotsComparisonFields(s);
  out.modalSlots = cmp.modalSlots;
  out.thresholdSlots = cmp.thresholdSlots;
  out.slotsAgree = cmp.slotsAgree;

  if (!debug) {
    if (trustedDisplay.source === 'threshold') {
      out.slots = trustedDisplay.slots;
      out.capacity = null;
      out.estimatedBooked = null;
      out.fillRate = null;
    } else if (verified) {
      if (isDefaultLikeDetailValues(out.slots, out.capacity, out.estimatedBooked)) {
        out.slots = null;
        out.capacity = null;
        out.estimatedBooked = null;
        out.fillRate = null;
        out.detailVerified = false;
        out.detailConfidence = 'default_suppressed';
      }
    } else {
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
    'detailAttemptCount', 'lastDetailAttemptAt', 'nextDetailRetryAt',
    ...THRESHOLD_SESSION_FIELDS,
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
  recordDetailAttemptOnSession(entry, { success: false });
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

function emptyCookieRequestDiagnostics() {
  return {
    cookieDismissAttempted: 0,
    cookieDismissSucceeded: 0,
    cookieBannerStillVisible: false,
    cookieClickMethod: null,
    modalTextAfterCookieDismissSample: null,
    cookieDismissLastAttempt: null,
    cookieDismissAttempts: [],
  };
}

function buildEnrichmentRunDiagnostics(stats = {}) {
  const attempted = (stats.sessionsAttempted ?? 0) > 0;
  const skippedBeforeAttempt = !!stats.skipped && !attempted;
  const activeBackground = scrapeInProgress || detailEnrichmentInProgress;

  if (skippedBeforeAttempt) {
    return {
      currentRequestDiagnostics: emptyCookieRequestDiagnostics(),
      activeRunDiagnostics: activeBackground
        ? { ...cookieDiagnosticsPayload(), source: 'active_background_run' }
        : null,
    };
  }

  return {
    currentRequestDiagnostics: cookieDiagnosticsPayload(),
    activeRunDiagnostics: null,
  };
}

function formatEnrichmentApiResponse(result, extra = {}) {
  const payload = { ...result, ...extra };
  const diag = buildEnrichmentRunDiagnostics(result);
  if (result?.skipped && (result.sessionsAttempted ?? 0) === 0) {
    delete payload.cookieDismissAttempted;
    delete payload.cookieDismissSucceeded;
    delete payload.cookieBannerStillVisible;
    delete payload.cookieClickMethod;
    delete payload.modalTextAfterCookieDismissSample;
    delete payload.cookieDismissLastAttempt;
    delete payload.cookieDismissAttempts;
  }
  return { ...payload, ...diag };
}

function finalizeEnrichmentStats(stats, { includeCookieDiagnostics = null } = {}) {
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
  const attachCookies = includeCookieDiagnostics ?? ((stats.sessionsAttempted ?? 0) > 0);
  if (attachCookies) attachCookieDiagnosticsToStats(stats);
  Object.assign(stats, buildEnrichmentRunDiagnostics(stats));
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
    detail_attempt_count: detailAttemptCountOnSession(s),
    last_detail_attempt_at: lastDetailAttemptAtOnSession(s),
    next_detail_retry_at: nextDetailRetryAtOnSession(s),
    detail_verified: sessionDetailVerified(s),
    detail_confidence: s.detailConfidence || s.raw?.detailConfidence || null,
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
      const type = snapshotType === 'entries_left_threshold'
        ? 'entries_left_threshold'
        : (snapshotType === 'detailed' || sessionHasDetailedData(s) ? 'detailed' : 'basic');
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
  await refreshEnrichmentQueueCounts();

  let candidates = allStoredSessions().filter(s => sessionDetailQueueEligible(s));
  if (isoDate) candidates = candidates.filter(s => sessionDateKey(s) === isoDate);
  if (priority != null) candidates = candidates.filter(s => enrichmentPriorityForSession(s) === priority);

  candidates.sort((a, b) => {
    const pDiff = enrichmentPriorityForSession(a) - enrichmentPriorityForSession(b);
    if (pDiff) return pDiff;
    const aAttempts = detailAttemptCountOnSession(a);
    const bAttempts = detailAttemptCountOnSession(b);
    if (aAttempts !== bAttempts) return aAttempts - bAttempts;
    const aRetry = nextDetailRetryAtOnSession(a) || '';
    const bRetry = nextDetailRetryAtOnSession(b) || '';
    if (aRetry !== bRetry) return aRetry.localeCompare(bRetry);
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
          recordDetailAttemptOnSession(entry, { success: sessionDetailVerified(entry) });
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
    const updatedKeys = toEnrich.map(s => s.key);
    await processWatchAlertsAfterScrape(updatedKeys, { slotsAlerts: true });
    const remainingEligible = allStoredSessions().filter(s => sessionDetailQueueEligible(s)).length;
    if (remainingEligible > 0) {
      scheduleDetailQueueDrain({ reason: `${reason}_continue`, limit: Math.min(remainingEligible, DETAIL_ENRICH_MAX_PER_RUN) });
    }
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
  await ensureSessionsForStatus();
  const targets = await pickSessionsForDetailEnrichment({ priority, limit: DETAIL_ENRICH_MAX_PER_RUN });
  if (!targets.length) {
    return emptyEnrichmentStats({ skipReason: 'no_queue_eligible_sessions' });
  }
  return runDetailEnrichment({ priority, sessions: targets, reason: `priority_${priority}` });
}

function tryAcquireScrapeLock(context = 'scrape', tier = null) {
  releaseScrapeLockIfStale();
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

  if (!chosen && slotsAlerts && sessionDetailVerified(session) && currSlots != null) {
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
    if (minsUntil != null && minsUntil > 0 && minsUntil <= lastCallMins
      && sessionDetailVerified(session) && currSlots != null && currSlots > 0) {
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
  return page.evaluate(cookieConsentBannerVisibleScript()).catch(() => false);
}

async function waitForCookieBannerGone(page, timeout = 6000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (!(await isCookieBannerVisible(page))) return;
    await page.waitForTimeout(200);
  }
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

    function hasConsentCopy(text) {
      const t = low(text);
      if (t.includes('this website uses cookies')) return true;
      if (t.includes('allow cookies') && t.includes('refuse cookies')) return true;
      if (t.includes('allow cookies') && t.includes('refuse')) return true;
      if (t.includes('accept cookies') && t.includes('cookie')) return true;
      return false;
    }

    function isPolicyNavOnly(text) {
      const t = low(text);
      if (!t.includes('cookie')) return false;
      if (hasConsentCopy(t)) return false;
      return t.includes('cookie policy');
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
          if (isPolicyNavOnly(text)) continue;
          if (hasConsentCopy(text)) return el;
          if (collectCandidates(root, el).length) return el;
        }
      }
      for (const el of root.querySelectorAll('div, section, aside, dialog, [role="dialog"], [role="alertdialog"]')) {
        const meta = elementMeta(el);
        const text = low(meta.text);
        if (!meta.visible || meta.boundingBox.width < 120) continue;
        if (isPolicyNavOnly(text)) continue;
        if (hasConsentCopy(text)) return el;
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

  attempt.failureReason = attempt.failureReason
    || (bodyHasConsentBannerCopy(ctx.beforeText) ? 'no_clickable_candidate_matched' : 'no_consent_banner_detected');
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
  const attemptStart = emptyCookieAttemptDiagnostics();
  try {
    if (!(await isCookieBannerVisible(page))) {
      cookieDismissDiagnostics.cookieBannerStillVisible = false;
      attemptStart.method = 'no_consent_banner_detected';
      attemptStart.failureReason = 'no_consent_banner_detected';
      recordCookieAttempt(attemptStart);
      return { success: true, method: 'no_consent_banner_detected' };
    }

    cookieDismissDiagnostics.cookieDismissAttempted++;
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

// Extract visible week dates from month selector + day column headers (not session tiles).
function scrapeCalendarHeaderDates() {
  const MONTH_NAMES = {
    january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
    july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
  };
  const DAY_HEADER_RE = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.?\s+(\d{1,2})$/i;
  const MONTH_YEAR_RE = /^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})$/i;
  const MONTH_YEAR_LOOSE_RE = /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b/i;
  const WEEKDAY_BODY_RE = /\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b/i;

  function normalizeText(text) {
    return String(text || '').replace(/\s+/g, ' ').trim();
  }

  function isoFromParts(year, monthIndex, day) {
    if (!year || monthIndex == null || !day) return null;
    const dt = new Date(Date.UTC(year, monthIndex, day));
    if (dt.getUTCFullYear() !== year || dt.getUTCMonth() !== monthIndex || dt.getUTCDate() !== day) {
      return null;
    }
    return dt.toISOString().slice(0, 10);
  }

  function parseMonthYearLabel(text) {
    const t = normalizeText(text);
    const exact = t.match(MONTH_YEAR_RE);
    if (exact) {
      const month = MONTH_NAMES[exact[1].toLowerCase()];
      return month == null ? null : { year: parseInt(exact[2], 10), month };
    }
    const loose = t.match(MONTH_YEAR_LOOSE_RE);
    if (loose) {
      const month = MONTH_NAMES[loose[1].toLowerCase()];
      return month == null ? null : { year: parseInt(loose[2], 10), month };
    }
    return null;
  }

  function parseDayHeaderText(text) {
    const t = normalizeText(text);
    const m = t.match(DAY_HEADER_RE);
    if (!m) return null;
    return {
      weekday: m[1].charAt(0).toUpperCase() + m[1].slice(1, 3).toLowerCase(),
      day: parseInt(m[2], 10),
      rawText: t,
    };
  }

  function isElementVisible(el) {
    if (!el || typeof el.getBoundingClientRect !== 'function') return false;
    const rect = el.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return false;
    const style = window.getComputedStyle(el);
    if (style.visibility === 'hidden' || style.display === 'none' || Number(style.opacity) === 0) {
      return false;
    }
    return true;
  }

  function findRawMonthLabel() {
    const selectors = 'button, .btn, a[role="button"], [class*="month"], .dropdown-toggle, span, div, label';
    for (const el of document.querySelectorAll(selectors)) {
      const t = normalizeText(el.textContent);
      if (!t || t.length > 40) continue;
      if (MONTH_YEAR_RE.test(t)) return t;
    }
    for (const el of document.querySelectorAll(selectors)) {
      const t = normalizeText(el.textContent);
      if (!t || t.length > 40) continue;
      const m = t.match(MONTH_YEAR_LOOSE_RE);
      if (m) return `${m[1]} ${m[2]}`;
    }
    return null;
  }

  function findLayoutAnchors() {
    let waveHeadingBottom = 0;
    let firstTimeRowTop = Infinity;
    for (const el of document.querySelectorAll('*')) {
      if (!isElementVisible(el)) continue;
      const t = normalizeText(el.textContent);
      if (/left wave sessions|right wave sessions/i.test(t) && t.length < 50) {
        const rect = el.getBoundingClientRect();
        if (rect.bottom > waveHeadingBottom) waveHeadingBottom = rect.bottom;
      }
      if (/^\d{1,2}(:\d{2})?\s*(am|pm)$/i.test(t) || /^0?6\s*am$/i.test(t) || /^06\s*am$/i.test(t)) {
        const rect = el.getBoundingClientRect();
        if (rect.top > 0 && rect.top < firstTimeRowTop) firstTimeRowTop = rect.top;
      }
    }
    return { waveHeadingBottom, firstTimeRowTop };
  }

  function passesLayoutFilter(candidate, layoutBounds = {}) {
    const {
      waveHeadingBottom = null,
      firstTimeRowTop = null,
      calendarTop = null,
      calendarBottom = null,
    } = layoutBounds || {};

    const box = candidate?.box || candidate?.rect || {};
    const top = Number.isFinite(box.top)
      ? box.top
      : (Number.isFinite(candidate?.top) ? candidate.top : null);
    const bottom = Number.isFinite(box.bottom)
      ? box.bottom
      : (top != null ? top + 1 : null);

    // If we do not have layout bounds, do not reject the candidate.
    // Layout filtering is a preference, not a hard requirement.
    if (top == null || bottom == null) return true;

    if (waveHeadingBottom != null && bottom < waveHeadingBottom) return false;
    if (firstTimeRowTop != null && top > firstTimeRowTop) return false;
    if (calendarTop != null && bottom < calendarTop) return false;
    if (calendarBottom != null && top > calendarBottom) return false;

    return true;
  }

  function makeCandidate(el, source, layoutBounds) {
    if (!isElementVisible(el)) return null;
    const rect = el.getBoundingClientRect();
    if (!passesLayoutFilter({ box: rect }, layoutBounds)) return null;
    const norm = normalizeText(el.innerText || el.textContent || '');
    if (!norm || norm.length > 40) return null;
    const parsed = parseDayHeaderText(norm);
    if (!parsed) return null;
    return {
      ...parsed,
      source,
      top: Math.round(rect.top),
      left: Math.round(rect.left),
    };
  }

  function collectBodyWeekdayTextSample() {
    const samples = [];
    const seen = new Set();
    for (const el of document.querySelectorAll('body *')) {
      if (!isElementVisible(el)) continue;
      const t = normalizeText(el.innerText || el.textContent || '');
      if (!t || t.length > 100) continue;
      const hasWeekday = WEEKDAY_BODY_RE.test(t);
      const hasDayNum = /\b\d{1,2}\b/.test(t);
      if (!hasWeekday && !hasDayNum) continue;
      if (!hasWeekday && hasDayNum) continue;
      if (seen.has(t)) continue;
      seen.add(t);
      samples.push(t);
      if (samples.length >= 30) break;
    }
    return samples;
  }

  function collectDayHeaderCandidates(layoutBounds) {
    const candidates = [];
    const pushUnique = (c) => {
      if (!c) return;
      const posKey = `${c.weekday.toLowerCase()}-${c.day}-${Math.round(c.left / 12)}-${Math.round(c.top / 12)}`;
      if (candidates.some(x => `${x.weekday.toLowerCase()}-${x.day}-${Math.round(x.left / 12)}-${Math.round(x.top / 12)}` === posKey)) {
        return;
      }
      candidates.push(c);
    };

    for (const table of document.querySelectorAll('table')) {
      const headerRow = table.querySelector('thead tr') || table.querySelector('tr');
      if (!headerRow) continue;
      for (let i = 1; i < headerRow.cells.length; i++) {
        pushUnique(makeCandidate(headerRow.cells[i], 'table_headers', layoutBounds));
      }
    }

    const domSelector = 'th, td, div, span, label, li, p, h1, h2, h3, h4, h5, h6, a, b, strong';
    for (const el of document.querySelectorAll(domSelector)) {
      const norm = normalizeText(el.innerText || el.textContent || '');
      if (!DAY_HEADER_RE.test(norm)) continue;
      pushUnique(makeCandidate(el, 'visible_dom_text', layoutBounds));
    }

    for (const el of document.querySelectorAll('*')) {
      if (!isElementVisible(el)) continue;
      if (el.children.length < 2) continue;
      const combined = normalizeText(el.innerText || el.textContent || '');
      if (!DAY_HEADER_RE.test(combined) || combined.length > 24) continue;
      const rect = el.getBoundingClientRect();
      if (rect.width < 2 || rect.height < 2) continue;
      if (!passesLayoutFilter({ box: rect }, layoutBounds)) continue;
      const parsed = parseDayHeaderText(combined);
      if (!parsed) continue;
      pushUnique({
        ...parsed,
        source: 'parent_combined_text',
        top: Math.round(rect.top),
        left: Math.round(rect.left),
      });
    }

    candidates.sort((a, b) => a.top - b.top || a.left - b.left);
    return candidates;
  }

  function filterCandidatesByLayout(candidates, layoutBounds = {}) {
    if (!Array.isArray(candidates)) return [];
    try {
      return candidates.filter((candidate) => passesLayoutFilter(candidate, layoutBounds));
    } catch (err) {
      return candidates;
    }
  }

  function firstCompleteWeekSequence(candidates, layoutBounds = {}) {
    const sorted = filterCandidatesByLayout(
      [...candidates].sort((a, b) => a.top - b.top || a.left - b.left),
      layoutBounds,
    );
    if (!sorted.length) return { parsed: [], rawTexts: [], parseSource: null };

    const monIdx = sorted.findIndex(c => c.weekday.toLowerCase().startsWith('mon'));
    if (monIdx >= 0) {
      const rowTop = sorted[monIdx].top;
      const sameRow = sorted.filter(c => Math.abs(c.top - rowTop) <= 10);
      const rowMonIdx = sameRow.findIndex(c => c.weekday.toLowerCase().startsWith('mon'));
      if (rowMonIdx >= 0 && sameRow.length >= rowMonIdx + 7) {
        const week = sameRow.slice(rowMonIdx, rowMonIdx + 7);
        const parseSource = week[0]?.source || 'visible_dom_text';
        return {
          parsed: week.map(c => ({ weekday: c.weekday, day: c.day, rawText: c.rawText })),
          rawTexts: week.map(c => c.rawText),
          parseSource,
        };
      }
      if (sorted.length >= monIdx + 7) {
        const week = sorted.slice(monIdx, monIdx + 7);
        const parseSource = week[0]?.source || 'visible_dom_text';
        return {
          parsed: week.map(c => ({ weekday: c.weekday, day: c.day, rawText: c.rawText })),
          rawTexts: week.map(c => c.rawText),
          parseSource,
        };
      }
    }

    const seen = new Set();
    const unique = [];
    for (const c of sorted) {
      const key = `${c.weekday.toLowerCase()}-${c.day}`;
      if (seen.has(key)) continue;
      seen.add(key);
      unique.push(c);
      if (unique.length >= 7) break;
    }
    const parseSource = unique[0]?.source || null;
    return {
      parsed: unique.map(c => ({ weekday: c.weekday, day: c.day, rawText: c.rawText })),
      rawTexts: unique.map(c => c.rawText),
      parseSource,
    };
  }

  function parseVisibleWeekFromMonthAndDayHeaders(monthLabel, dayHeaders) {
    const monthCtx = parseMonthYearLabel(monthLabel);
    if (!monthCtx || !dayHeaders.length) return [];

    let year = monthCtx.year;
    let month = monthCtx.month;
    let prevDay = null;
    const out = [];
    for (const header of dayHeaders) {
      const day = Number(header.day);
      if (!Number.isFinite(day)) continue;
      if (prevDay != null && day < prevDay) {
        month += 1;
        if (month > 11) {
          month = 0;
          year += 1;
        }
      }
      const iso = isoFromParts(year, month, day);
      if (iso) out.push(iso);
      prevDay = day;
    }
    return out;
  }

  const WEEKDAY_ONLY_RE = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.?$/i;
  const DAY_NUM_LINE_RE = /^(\d{1,2})$/;
  const WEEKDAY_ORDER = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];

  function normalizeBodyLines(bodyText) {
    return String(bodyText || '')
      .split(/\r?\n/)
      .map((line) => line.replace(/\s+/g, ' ').trim())
      .filter(Boolean);
  }

  function formatWeekdayToken(text) {
    const t = String(text || '').replace(/\./g, '').trim();
    return t.charAt(0).toUpperCase() + t.slice(1, 3).toLowerCase();
  }

  function parseDayHeadersFromBodyTextLines(bodyText) {
    const lines = normalizeBodyLines(bodyText);
    const bodyTextLinesSample = lines.slice(0, 80);
    const weekdayLineMatches = [];
    const combinedDayHeaderMatches = [];
    const tokens = [];

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (DAY_HEADER_RE.test(line)) {
        const parsed = parseDayHeaderText(line);
        if (parsed) {
          tokens.push({ ...parsed, source: 'body_text_lines' });
          combinedDayHeaderMatches.push(parsed.rawText);
          continue;
        }
      }
      if (WEEKDAY_ONLY_RE.test(line)) {
        weekdayLineMatches.push(line);
        const next = lines[i + 1];
        if (next && DAY_NUM_LINE_RE.test(next)) {
          const rawText = `${line} ${next}`.replace(/\s+/g, ' ');
          const parsed = parseDayHeaderText(rawText) || {
            weekday: formatWeekdayToken(line),
            day: parseInt(next, 10),
            rawText,
          };
          tokens.push({ ...parsed, source: 'body_text_lines_adjacent' });
          combinedDayHeaderMatches.push(parsed.rawText);
          i += 1;
        }
      }
    }

    let bestWeek = [];
    for (let start = 0; start < tokens.length; start++) {
      if (!tokens[start].weekday.toLowerCase().startsWith('mon')) continue;
      const week = [];
      let expect = 0;
      for (let j = start; j < tokens.length && week.length < 7; j++) {
        const w = tokens[j].weekday.toLowerCase().slice(0, 3);
        if (w === WEEKDAY_ORDER[expect]) {
          week.push(tokens[j]);
          expect += 1;
        } else if (week.length > 0) {
          break;
        }
      }
      if (week.length >= 7) {
        bestWeek = week.slice(0, 7);
        break;
      }
    }

    return {
      parsed: bestWeek.map((c) => ({ weekday: c.weekday, day: c.day, rawText: c.rawText })),
      rawTexts: bestWeek.map((c) => c.rawText),
      parseSource: bestWeek.length ? 'body_text_lines' : null,
      bodyTextLinesSample,
      weekdayLineMatches,
      combinedDayHeaderMatches,
    };
  }

  function buildCalendarReadySignals() {
    const bodyText = document.body?.innerText || '';
    return {
      hasMonthYearText: /\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b/i.test(bodyText),
      hasLeftWaveSessionsText: /Left Wave Sessions/i.test(bodyText),
      hasRightWaveSessionsText: /Right Wave Sessions/i.test(bodyText),
      hasWeekdayText: /\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b/i.test(bodyText),
      hasTimeRows: /\b\d{1,2}\s?(am|pm)\b/i.test(bodyText),
    };
  }

  try {
    const rawMonthLabel = findRawMonthLabel();
    const layoutAnchors = findLayoutAnchors();
    const dayHeaderCandidates = collectDayHeaderCandidates(layoutAnchors);
    const dayHeaderCandidateTexts = dayHeaderCandidates.map(c => c.rawText);
    const dayHeaderCandidateCount = dayHeaderCandidates.length;
    const bodyWeekdayTextSample = collectBodyWeekdayTextSample();
    let weekSequence = firstCompleteWeekSequence(dayHeaderCandidates, layoutAnchors);
    let bodyTextLinesSample = [];
    let weekdayLineMatches = [];
    let combinedDayHeaderMatches = [];

    if (!weekSequence.rawTexts?.length) {
      const bodyFallback = parseDayHeadersFromBodyTextLines(document.body?.innerText || '');
      bodyTextLinesSample = bodyFallback.bodyTextLinesSample || [];
      weekdayLineMatches = bodyFallback.weekdayLineMatches || [];
      combinedDayHeaderMatches = bodyFallback.combinedDayHeaderMatches || [];
      if (bodyFallback.rawTexts?.length) {
        weekSequence = bodyFallback;
      }
    }

    const rawDayHeaderTexts = weekSequence.rawTexts;
    const parsedDayHeaders = weekSequence.parsed;
    const dayHeaderParseSource = weekSequence.parseSource;
    const visibleIsoDatesFromHeaders = parseVisibleWeekFromMonthAndDayHeaders(rawMonthLabel, parsedDayHeaders);

    return {
      dates: visibleIsoDatesFromHeaders,
      visibleIsoDatesFromHeaders,
      rawMonthLabel,
      rawDayHeaderTexts,
      parsedDayHeaders,
      dayHeaderCandidateTexts,
      dayHeaderCandidateCount,
      dayHeaderParseSource,
      bodyWeekdayTextSample,
      bodyTextLinesSample,
      weekdayLineMatches,
      combinedDayHeaderMatches,
      headerParseStrategy: 'month_selector_plus_day_columns',
      headerParseError: null,
    };
  } catch (err) {
    const bodyText = document.body?.innerText || '';
    return {
      rawMonthLabel: null,
      rawDayHeaderTexts: [],
      parsedDayHeaders: [],
      visibleIsoDatesFromHeaders: [],
      targetDateVisibleFromHeaders: false,
      dayHeaderCandidateTexts: [],
      dayHeaderCandidateCount: 0,
      dayHeaderParseSource: null,
      headerParseError: String(err?.stack || err?.message || err),
      bodyTextLength: bodyText.length,
      bodyTextSample: bodyText.slice(0, 1000),
      calendarReadySignals: buildCalendarReadySignals(),
    };
  }
}

function normalizeBookingFiltersForThresholdScan() {
  const results = [];
  const clickOption = (selectEl, optionRegex) => {
    if (!selectEl) return false;
    for (const opt of selectEl.options) {
      const label = (opt.textContent || opt.label || '').replace(/\s+/g, ' ').trim();
      if (optionRegex.test(label)) {
        selectEl.value = opt.value;
        selectEl.dispatchEvent(new Event('change', { bubbles: true }));
        selectEl.dispatchEvent(new Event('input', { bubbles: true }));
        return label;
      }
    }
    return false;
  };

  const findSelectNear = (labelRegex) => {
    for (const sel of document.querySelectorAll('select')) {
      const id = sel.id || '';
      const name = sel.name || '';
      const label = sel.closest('label')?.textContent || '';
      const prev = sel.previousElementSibling?.textContent || '';
      const parentText = sel.parentElement?.textContent || '';
      const hay = `${id} ${name} ${label} ${prev} ${parentText}`.replace(/\s+/g, ' ');
      if (labelRegex.test(hay)) return sel;
    }
    for (const el of document.querySelectorAll('label, span, th, div, button')) {
      const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
      if (!labelRegex.test(t) || t.length > 40) continue;
      const sel = el.querySelector('select') || el.parentElement?.querySelector('select');
      if (sel) return sel;
    }
    return null;
  };

  const priceSel = findSelectNear(/price\s*category/i);
  const priceLabel = clickOption(priceSel, /^all$/i);
  if (priceLabel) results.push({ filter: 'price_category', value: priceLabel });

  const levelSel = findSelectNear(/session\s*level/i);
  const levelLabel = clickOption(levelSel, /^all$/i);
  if (levelLabel) results.push({ filter: 'session_level', value: levelLabel });

  const viewSel = findSelectNear(/view\s*by/i);
  const viewLabel = clickOption(viewSel, /levels/i);
  if (viewLabel) results.push({ filter: 'view_by', value: viewLabel });

  for (const el of document.querySelectorAll('button, label, span, a')) {
    const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
    if (/^levels$/i.test(t) || /^view by levels$/i.test(t)) {
      el.click();
      results.push({ filter: 'view_by_click', value: t });
      break;
    }
  }

  return { ok: true, normalized: results };
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

async function scrapeVisibleSessionsFromPage(page, options = {}) {
  assertPlaywrightPage(page, 'scrapeVisibleSessionsFromPage');
  return page.evaluate(scrapeVisibleSessions, { ...SCRAPE_OPTS, ...options });
}

function scrapeVisibleThresholdTilesStrict({ excludedLevels = [], excludedWaves = [], weekOffset = 0 } = {}) {
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
    return null;
  }

  function resolveWaveSide(el, wave, level, titleHtml) {
    const fromTitle = parseSideFromTitle(titleHtml, level);
    const fromColumn = parseSideFromColumn(el, level);
    const tileText = titleHtml.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
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

  function isThresholdTileDomVisible(el) {
    if (!el || typeof el.getBoundingClientRect !== 'function') return false;
    const rect = el.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return false;
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
    let node = el;
    while (node && node !== document.body) {
      const st = window.getComputedStyle(node);
      if (st.display === 'none' || st.visibility === 'hidden') return false;
      node = node.parentElement;
    }
    return true;
  }

  function isThresholdTileInCalendarGrid(el) {
    return !!el.closest('table, td, .agenda, .calendar, .dynamic-cal, [class*="booking"]');
  }

  const seen = new Set();
  const out = [];
  const allEls = document.querySelectorAll('div.dynamic-cal-booking-ts[data-original-title]');
  const rawCount = allEls.length;
  allEls.forEach((el) => {
    if (!isThresholdTileDomVisible(el)) return;
    if (!isThresholdTileInCalendarGrid(el)) return;
    if (el.querySelector('div.dynamic-cal-booking-ts[data-original-title]')) return;

    const cls = el.className;
    const t = el.dataset.originalTitle || '';
    const lm = t.match(/Session level\s*:<\/b>\s*([^<]+)/i);
    const wm = cls.match(/booking-agenda-clickable_(\d+)_(\d+)/);
    if (!lm || !wm) return;
    const level = lm[1].trim();
    const wave = +wm[2];
    if (excludedLevels.includes(level)) return;
    if (excludedWaves.includes(wave)) return;
    const ts = +wm[1];
    const key = `${ts}_${wave}`;
    if (seen.has(key)) return;
    seen.add(key);
    const fm = t.match(/From\s*:<\/b>\s*([\d:]+\s*[apm]+)/i);
    const d = new Date(ts * 1000);
    const isoDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(d);
    const displayDate = d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', timeZone: 'America/New_York' });
    const weekday = d.toLocaleDateString('en-US', { weekday: 'long', timeZone: 'America/New_York' });
    const sideInfo = resolveWaveSide(el, wave, level, t);
    const td = el.closest('td');
    out.push({
      key,
      ts,
      wave,
      level,
      available: !cls.includes('expired_timeslot'),
      time: fm ? fm[1].trim() : '?',
      date: displayDate,
      dayLabel: displayDate,
      dateKey: isoDate,
      isoDate,
      displayDate,
      weekday,
      waveSide: sideInfo.waveSide,
      waveSideSource: sideInfo.waveSideSource,
      sideParseRaw: sideInfo.sideParseRaw,
      tileText: sideInfo.tileText,
      tileClassName: cls,
      tileColumnIndex: td ? td.cellIndex : null,
      waveSideAmbiguous: sideInfo.waveSideAmbiguous,
      sideKey: `${ts}_${sideInfo.waveSide.toLowerCase().replace(/\s+/g, '-')}`,
      sessionType: level,
      weekOffset,
    });
  });
  return { sessions: out, rawCount, duplicateSkips: rawCount - out.length, visibleCount: out.length };
}

function isThresholdPlaceholderTile(tile) {
  const cls = tile.tileClassName || '';
  if (cls.includes('expired_timeslot')) return true;
  if (cls.includes('disabled') || cls.includes('unavailable') || cls.includes('empty')) return true;
  const text = (tile.tileText || '').replace(/\s+/g, ' ').trim();
  if (/^x$/i.test(text)) return true;
  if (!tile.level || String(tile.level).length < 2) return true;
  return false;
}

function enrichThresholdTile(tile) {
  const sessionCode = levelToSessionCode(tile.level);
  const sideShort = normalizeWaveSideShort(tile.waveSide);
  return {
    ...tile,
    sessionCode,
    sessionName: tile.level,
    timeLabel: tile.time,
    waveSideShort: sideShort,
    identityKey: makeThresholdIdentityKey({
      isoDate: tile.isoDate,
      timeLabel: tile.time,
      waveSide: tile.waveSide,
      sessionCode,
      level: tile.level,
    }),
  };
}

async function scrapeThresholdSessionTilesFromPage(page, options = {}) {
  assertPlaywrightPage(page, 'scrapeThresholdSessionTilesFromPage');
  const scrape = await page.evaluate(scrapeVisibleThresholdTilesStrict, { ...SCRAPE_OPTS, ...options });
  const sessions = (scrape.sessions || [])
    .filter(s => s.available && !isThresholdPlaceholderTile(s))
    .map(enrichThresholdTile);
  return {
    ...scrape,
    sessions,
    availableCount: sessions.length,
    visibleTileCount: sessions.length,
  };
}

async function getThresholdGridSnapshot(page) {
  assertPlaywrightPage(page, 'getThresholdGridSnapshot');
  return page.evaluate(() => {
    function readEntriesLeftFilterLabel() {
      for (const sel of document.querySelectorAll('select')) {
        const context = `${sel.name || ''} ${sel.id || ''} ${sel.closest('label')?.textContent || ''} ${sel.previousElementSibling?.textContent || ''}`.replace(/\s+/g, ' ');
        if (!/entries?\s*left/i.test(context)) continue;
        const opt = sel.options[sel.selectedIndex];
        if (opt) return (opt.textContent || opt.label || '').replace(/\s+/g, ' ').trim();
      }
      for (const el of document.querySelectorAll('button, span, label, div, a, th')) {
        const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
        if (/entries?\s*left\s*:/i.test(t) && t.length < 80) return t;
      }
      return null;
    }
    function hashBodyText(text) {
      let hash = 5381;
      const normalized = String(text || '').replace(/\s+/g, ' ').trim();
      for (let i = 0; i < normalized.length; i++) hash = ((hash << 5) + hash) + normalized.charCodeAt(i);
      return String(hash >>> 0);
    }
    function isVisible(el) {
      if (!el?.getBoundingClientRect) return false;
      const rect = el.getBoundingClientRect();
      if (rect.width < 2 || rect.height < 2) return false;
      const style = window.getComputedStyle(el);
      if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
      return true;
    }
    let visibleTileCount = 0;
    for (const el of document.querySelectorAll('div.dynamic-cal-booking-ts[data-original-title]')) {
      if (!isVisible(el)) continue;
      if (!el.closest('table, td, .agenda, .calendar, .dynamic-cal, [class*="booking"]')) continue;
      visibleTileCount += 1;
    }
    const bodyText = document.body?.innerText || '';
    return {
      bodyTextHash: hashBodyText(bodyText),
      visibleTileCount,
      entriesLeftLabel: readEntriesLeftFilterLabel(),
    };
  });
}

async function waitForThresholdGridChange(page, before, { timeoutMs = 8000, pollMs = 250 } = {}) {
  assertPlaywrightPage(page, 'waitForThresholdGridChange');
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const after = await getThresholdGridSnapshot(page);
    const gridChanged = after.bodyTextHash !== before.bodyTextHash
      || after.visibleTileCount !== before.visibleTileCount;
    if (gridChanged) {
      return { ...after, gridChanged: true, waitedMs: Date.now() - started };
    }
    await page.waitForTimeout(pollMs);
  }
  const after = await getThresholdGridSnapshot(page);
  return {
    ...after,
    gridChanged: after.bodyTextHash !== before.bodyTextHash
      || after.visibleTileCount !== before.visibleTileCount,
    waitedMs: Date.now() - started,
  };
}

async function applyEntriesLeftFilterWithVerification(page, threshold) {
  assertPlaywrightPage(page, 'applyEntriesLeftFilterWithVerification');
  const requestedThreshold = Number(threshold);
  const before = await getThresholdGridSnapshot(page);
  const changed = await setEntriesLeftFilter(page, requestedThreshold);
  let afterLabelSnap = await getThresholdGridSnapshot(page);
  const settle = await waitForThresholdGridChange(page, before, {
    timeoutMs: Math.max(THRESHOLD_FILTER_SETTLE_MS * 2, 6000),
  });
  afterLabelSnap = await getThresholdGridSnapshot(page);
  const afterEntriesLeftLabel = afterLabelSnap.entriesLeftLabel;
  const filterSetOk = entriesLeftLabelMatchesThreshold(afterEntriesLeftLabel, requestedThreshold);

  return {
    requestedThreshold,
    beforeEntriesLeftLabel: before.entriesLeftLabel,
    afterEntriesLeftLabel,
    filterSetOk,
    filterSetError: filterSetOk ? null : 'entries_left_label_mismatch',
    bodyTextHashBefore: before.bodyTextHash,
    bodyTextHashAfter: settle.bodyTextHash,
    visibleTileCountBefore: before.visibleTileCount,
    visibleTileCountAfter: settle.visibleTileCount,
    gridChanged: settle.gridChanged,
    filterApplyOk: changed?.ok === true,
    filterApplyMethod: changed?.method || null,
    filterApplyReason: changed?.reason || null,
    ok: filterSetOk,
    reason: filterSetOk ? null : (changed?.reason || 'entries_left_option_unavailable_or_not_selected'),
    threshold: requestedThreshold,
    waitedMs: settle.waitedMs,
  };
}

async function normalizeBookingFiltersOnPage(page) {
  return page.evaluate(normalizeBookingFiltersForThresholdScan);
}

async function getCalendarHeaderParseFromPage(page) {
  assertPlaywrightPage(page, 'getCalendarHeaderParseFromPage');
  return page.evaluate(scrapeCalendarHeaderDates);
}

async function collectThresholdPageDiagnostics(page, label = 'threshold_page') {
  return collectPageDiagnostics(page, label);
}

async function waitForThresholdCalendarShell(page, { timeoutMs = THRESHOLD_SCAN_PAGE_TIMEOUT_MS } = {}) {
  await page.waitForSelector('.dynamic-cal-booking-ts, table', { timeout: Math.min(timeoutMs, 20_000) }).catch(() => {});
  await page.waitForFunction(() => {
    const body = (document.body?.innerText || document.body?.textContent || '').replace(/\s+/g, ' ');
    return /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b/i.test(body)
      || /\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.?\s+\d{1,2}\b/i.test(body)
      || document.querySelectorAll('div.dynamic-cal-booking-ts').length > 0;
  }, { timeout: Math.min(timeoutMs, 15_000) }).catch(() => {});
}

async function scrapeCalendarHeadersWithRetry(page, { maxAttempts = 4, waitMs = 1500, auditContext = null } = {}) {
  const attempts = [];
  let lastHeaderParse = null;
  let lastPageDiagnostics = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const [headerParse, pageDiagnostics] = await Promise.all([
      getCalendarHeaderParseFromPage(page),
      collectPageDiagnostics(page, `header_scrape_attempt_${attempt}`),
    ]);
    lastHeaderParse = headerParse;
    lastPageDiagnostics = pageDiagnostics;
    const attemptRecord = {
      attempt,
      at: new Date().toISOString(),
      rawMonthLabel: headerParse?.rawMonthLabel ?? null,
      rawDayHeaderTexts: headerParse?.rawDayHeaderTexts || [],
      dayHeaderCandidateCount: headerParse?.dayHeaderCandidateCount ?? 0,
      visibleIsoDatesFromHeaders: headerParse?.visibleIsoDatesFromHeaders || [],
      headerParseError: headerParse?.headerParseError ?? null,
      currentUrl: pageDiagnostics?.currentUrl ?? null,
      bodyTextSample: pageDiagnostics?.bodyTextSample ?? null,
      calendarReadySignals: pageDiagnostics?.calendarReadySignals ?? null,
    };
    attempts.push(attemptRecord);
    pushThresholdAuditStep(auditContext, 'header_scrape_attempt', attemptRecord);
    const ready = (headerParse?.rawDayHeaderTexts?.length || 0) >= 7
      || (headerParse?.dayHeaderCandidateCount || 0) >= 7
      || (headerParse?.visibleIsoDatesFromHeaders?.length || 0) >= 7;
    if (ready) break;
    if (attempt < maxAttempts) await page.waitForTimeout(waitMs);
  }
  return {
    headerParse: lastHeaderParse,
    headerScrapeAttempts: attempts,
    pageDiagnostics: lastPageDiagnostics,
  };
}

async function getVisibleWeekDatesFromHeaders(page) {
  const result = await getCalendarHeaderParseFromPage(page);
  return result?.visibleIsoDatesFromHeaders || result?.dates || [];
}

async function blurActiveFilterDropdown(page) {
  await page.evaluate(() => {
    document.body.click();
    const grid = document.querySelector('table, .panel-body, .dynamic-cal-booking-ts');
    grid?.click();
  }).catch(() => {});
  await page.waitForTimeout(300);
}

function reconEntriesLeftControlSnapshot() {
  function normalizeText(text) {
    return String(text || '').replace(/\s+/g, ' ').trim();
  }

  function isVisible(el) {
    if (!el || typeof el.getBoundingClientRect !== 'function') return false;
    const rect = el.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return false;
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
    let node = el;
    while (node && node !== document.body) {
      const st = window.getComputedStyle(node);
      if (st.display === 'none' || st.visibility === 'hidden') return false;
      node = node.parentElement;
    }
    return true;
  }

  function boundingBox(el) {
    const rect = el.getBoundingClientRect();
    return {
      top: Math.round(rect.top),
      left: Math.round(rect.left),
      width: Math.round(rect.width),
      height: Math.round(rect.height),
    };
  }

  function selectorHint(el) {
    if (!el) return null;
    if (el.id) return `#${CSS.escape(el.id)}`;
    const tag = el.tagName ? el.tagName.toLowerCase() : 'node';
    const classes = [...(el.classList || [])].filter(Boolean).slice(0, 4);
    if (classes.length) return `${tag}.${classes.join('.')}`;
    const name = el.getAttribute?.('name');
    if (name) return `${tag}[name="${name}"]`;
    return tag;
  }

  function describeElement(el, source) {
    const text = normalizeText(el.innerText || el.textContent || '');
    return {
      text: text.slice(0, 160),
      role: el.getAttribute?.('role') || null,
      tagName: el.tagName || null,
      className: el.className || null,
      ariaLabel: el.getAttribute?.('aria-label') || null,
      id: el.id || null,
      name: el.getAttribute?.('name') || null,
      selectorHint: selectorHint(el),
      boundingBox: boundingBox(el),
      visible: isVisible(el),
      source,
    };
  }

  function readCurrentLabel() {
    for (const el of document.querySelectorAll('button, span, label, div, a, th')) {
      const t = normalizeText(el.innerText || el.textContent || '');
      if (/entries?\s*left\s*:/i.test(t) && t.length < 80) return t;
    }
    for (const sel of document.querySelectorAll('select')) {
      const context = `${sel.name || ''} ${sel.id || ''} ${sel.closest('label')?.textContent || ''} ${sel.previousElementSibling?.textContent || ''}`.replace(/\s+/g, ' ');
      if (/entries?\s*left/i.test(context)) {
        const opt = sel.options[sel.selectedIndex];
        return normalizeText(opt?.textContent || opt?.label || '');
      }
    }
    return null;
  }

  function collectControlCandidates() {
    const candidates = [];
    const seen = new Set();
    const push = (el, source) => {
      if (!el || seen.has(el)) return;
      seen.add(el);
      candidates.push(describeElement(el, source));
    };

    for (const sel of document.querySelectorAll('select')) {
      const context = `${sel.name || ''} ${sel.id || ''} ${sel.closest('label')?.textContent || ''} ${sel.previousElementSibling?.textContent || ''}`.replace(/\s+/g, ' ');
      const optionText = [...sel.options].map((o) => normalizeText(o.textContent || o.label || '')).join(' | ');
      if (/entries?\s*left/i.test(context) || /entries?\s*left/i.test(optionText)) {
        push(sel, 'select');
      }
    }

    for (const el of document.querySelectorAll('button, span, label, div, a, th, [role="button"], [role="combobox"], [role="listbox"]')) {
      const t = normalizeText(el.textContent || '');
      if (/entries?\s*left/i.test(t) && t.length < 120) push(el, 'text_match');
    }

    for (const el of document.querySelectorAll('[aria-label], [title]')) {
      const aria = normalizeText(el.getAttribute('aria-label') || el.getAttribute('title') || '');
      if (/entries?\s*left/i.test(aria)) push(el, 'aria_match');
    }

    return candidates.sort((a, b) => Number(b.visible) - Number(a.visible));
  }

  function collectOptionCandidates() {
    const options = [];
    const seen = new Set();
    const push = (el, source) => {
      if (!el || seen.has(el)) return;
      seen.add(el);
      const text = normalizeText(el.innerText || el.textContent || el.label || '');
      if (!text || text.length > 120) return;
      options.push({
        text: text.slice(0, 160),
        normalizedText: text,
        role: el.getAttribute?.('role') || null,
        tagName: el.tagName || null,
        className: el.className || null,
        ariaSelected: el.getAttribute?.('aria-selected') || el.selected === true || null,
        selectorHint: selectorHint(el),
        boundingBox: boundingBox(el),
        visible: isVisible(el),
        source,
      });
    };

    for (const sel of document.querySelectorAll('select')) {
      const context = `${sel.name || ''} ${sel.id || ''} ${sel.closest('label')?.textContent || ''}`.replace(/\s+/g, ' ');
      if (!/entries?\s*left/i.test(context) && ![...sel.options].some((o) => /entries?\s*left/i.test(o.textContent || ''))) continue;
      for (const opt of sel.options) push(opt, 'select_option');
    }

    const popupSelectors = '.dropdown-menu, .select2-results, .select2-dropdown, ul[role="listbox"], [role="menu"], .popover, .autocomplete, .ui-menu, .chosen-results';
    for (const popup of document.querySelectorAll(popupSelectors)) {
      if (!isVisible(popup)) continue;
      for (const el of popup.querySelectorAll('option, li, a, button, span, div[role="option"], [role="menuitem"]')) {
        push(el, 'popup_option');
      }
    }

    for (const el of document.querySelectorAll('option, li, a, button, span, div[role="option"], [role="menuitem"]')) {
      const t = normalizeText(el.textContent || '');
      if (/entries?\s*left/i.test(t) || /^at least \d+/i.test(t) || /^\d+\s*entries?\s*left/i.test(t)) {
        push(el, 'dom_option_scan');
      }
    }

    return options.sort((a, b) => Number(b.visible) - Number(a.visible));
  }

  function bodyTextSampleAroundEntriesLeft() {
    const bodyText = document.body?.innerText || '';
    const idx = bodyText.search(/entries?\s*left/i);
    if (idx < 0) return bodyText.slice(0, 1200);
    return bodyText.slice(Math.max(0, idx - 300), idx + 900);
  }

  function popupVisible() {
    const popupSelectors = '.dropdown-menu, .select2-results, .select2-dropdown, ul[role="listbox"], [role="menu"], .popover.open, .dropdown.open .dropdown-menu';
    for (const el of document.querySelectorAll(popupSelectors)) {
      if (isVisible(el)) return true;
    }
    for (const sel of document.querySelectorAll('select')) {
      const context = `${sel.name || ''} ${sel.id || ''}`.replace(/\s+/g, ' ');
      if (/entries?\s*left/i.test(context) && document.activeElement === sel) return true;
    }
    return false;
  }

  function findOpenClickTarget() {
    const scored = [];
    for (const el of document.querySelectorAll('button, span, label, div, a, th, select, [role="button"], [role="combobox"]')) {
      const t = normalizeText(el.textContent || '');
      const aria = normalizeText(el.getAttribute?.('aria-label') || '');
      if (!/entries?\s*left/i.test(t) && !/entries?\s*left/i.test(aria)) continue;
      if (!isVisible(el)) continue;
      let score = 0;
      if (/entries?\s*left\s*:/i.test(t)) score += 5;
      if (el.tagName === 'SELECT') score += 4;
      if (el.tagName === 'BUTTON') score += 3;
      if (el.getAttribute?.('role') === 'combobox') score += 3;
      if (el.classList?.contains('dropdown-toggle')) score += 2;
      scored.push({ el, score, selectorHint: selectorHint(el) });
    }
    scored.sort((a, b) => b.score - a.score);
    return scored[0] || null;
  }

  return {
    currentLabel: readCurrentLabel(),
    controlCandidates: collectControlCandidates(),
    openClickTarget: findOpenClickTarget()?.selectorHint || null,
  };
}

function reconEntriesLeftControlAfterOpen() {
  function normalizeText(text) {
    return String(text || '').replace(/\s+/g, ' ').trim();
  }
  function isVisible(el) {
    if (!el?.getBoundingClientRect) return false;
    const rect = el.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return false;
    const style = window.getComputedStyle(el);
    return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
  }
  function boundingBox(el) {
    const rect = el.getBoundingClientRect();
    return { top: Math.round(rect.top), left: Math.round(rect.left), width: Math.round(rect.width), height: Math.round(rect.height) };
  }
  function selectorHint(el) {
    if (el.id) return `#${CSS.escape(el.id)}`;
    const tag = el.tagName.toLowerCase();
    const classes = [...(el.classList || [])].slice(0, 4);
    return classes.length ? `${tag}.${classes.join('.')}` : tag;
  }
  const optionCandidates = [];
  const seen = new Set();
  const push = (el, source) => {
    if (!el || seen.has(el)) return;
    seen.add(el);
    const text = normalizeText(el.innerText || el.textContent || el.label || '');
    if (!text || text.length > 120) return;
    optionCandidates.push({
      text: text.slice(0, 160),
      normalizedText: text,
      role: el.getAttribute?.('role') || null,
      tagName: el.tagName || null,
      className: el.className || null,
      ariaSelected: el.getAttribute?.('aria-selected') || (el.selected === true ? 'true' : null),
      selectorHint: selectorHint(el),
      boundingBox: boundingBox(el),
      visible: isVisible(el),
      source,
    });
  };
  for (const sel of document.querySelectorAll('select')) {
    const context = `${sel.name || ''} ${sel.id || ''} ${sel.closest('label')?.textContent || ''}`.replace(/\s+/g, ' ');
    if (!/entries?\s*left/i.test(context) && ![...sel.options].some((o) => /entries?\s*left/i.test(o.textContent || ''))) continue;
    for (const opt of sel.options) push(opt, 'select_option');
  }
  const popupSelectors = '.dropdown-menu, .select2-results, .select2-dropdown, ul[role="listbox"], [role="menu"], .popover, .autocomplete, .ui-menu, .chosen-results';
  for (const popup of document.querySelectorAll(popupSelectors)) {
    for (const el of popup.querySelectorAll('option, li, a, button, span, div[role="option"], [role="menuitem"]')) push(el, 'popup_option');
  }
  for (const el of document.querySelectorAll('option, li, a, button, span, div[role="option"], [role="menuitem"]')) {
    const t = normalizeText(el.textContent || '');
    if (/entries?\s*left/i.test(t) || /^at least \d+/i.test(t) || /^\d+\s*entries?\s*left/i.test(t)) push(el, 'dom_option_scan');
  }
  const bodyText = document.body?.innerText || '';
  const idx = bodyText.search(/entries?\s*left/i);
  const bodyTextSampleAroundEntriesLeft = idx < 0 ? bodyText.slice(0, 1200) : bodyText.slice(Math.max(0, idx - 300), idx + 900);
  let popupVisibleFlag = false;
  for (const el of document.querySelectorAll(popupSelectors)) {
    if (isVisible(el)) { popupVisibleFlag = true; break; }
  }
  return {
    popupVisible: popupVisibleFlag,
    optionCandidates: optionCandidates.sort((a, b) => Number(b.visible) - Number(a.visible)),
    bodyTextSampleAroundEntriesLeft,
  };
}

async function openEntriesLeftControlForRecon(page) {
  assertPlaywrightPage(page, 'openEntriesLeftControlForRecon');
  return page.evaluate(() => {
    function normalizeText(text) {
      return String(text || '').replace(/\s+/g, ' ').trim();
    }
    function isVisible(el) {
      if (!el?.getBoundingClientRect) return false;
      const rect = el.getBoundingClientRect();
      if (rect.width < 2 || rect.height < 2) return false;
      const style = window.getComputedStyle(el);
      return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0;
    }
    const scored = [];
    for (const el of document.querySelectorAll('button, span, label, div, a, th, select, [role="button"], [role="combobox"]')) {
      const t = normalizeText(el.textContent || '');
      const aria = normalizeText(el.getAttribute?.('aria-label') || '');
      if (!/entries?\s*left/i.test(t) && !/entries?\s*left/i.test(aria)) continue;
      if (!isVisible(el)) continue;
      let score = 0;
      if (/entries?\s*left\s*:/i.test(t)) score += 5;
      if (el.tagName === 'SELECT') score += 4;
      if (el.tagName === 'BUTTON') score += 3;
      if (el.getAttribute?.('role') === 'combobox') score += 3;
      if (el.classList?.contains('dropdown-toggle')) score += 2;
      scored.push({ el, score });
    }
    scored.sort((a, b) => b.score - a.score);
    const target = scored[0]?.el || null;
    if (!target) return { ok: false, reason: 'no_click_target' };
    if (target.tagName === 'SELECT') {
      target.focus();
      target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
      target.dispatchEvent(new MouseEvent('click', { bubbles: true }));
      return { ok: true, openMethod: 'select_focus_click', tagName: target.tagName };
    }
    target.click();
    return { ok: true, openMethod: 'click', tagName: target.tagName, text: normalizeText(target.textContent || '').slice(0, 80) };
  });
}

function buildThresholdAvailabilityFromOptions(optionCandidates, thresholds) {
  const optionTexts = (optionCandidates || []).map((o) => o.normalizedText || o.text).filter(Boolean);
  return (thresholds || []).map((threshold) => {
    const n = Number(threshold);
    const patterns = [
      new RegExp(`entries?\\s*left\\s*:?\\s*${n}\\b`, 'i'),
      new RegExp(`at\\s*least\\s*${n}\\s*entries?\\s*left`, 'i'),
      new RegExp(`\\b${n}\\s*entries?\\s*left`, 'i'),
    ];
    const matchingOptionTexts = optionTexts.filter((text) => patterns.some((re) => re.test(text)));
    return {
      threshold: n,
      optionFound: matchingOptionTexts.length > 0,
      matchingOptionTexts,
    };
  });
}

async function scrapeEntriesLeftControlReconFromPage(page) {
  assertPlaywrightPage(page, 'scrapeEntriesLeftControlReconFromPage');
  const beforeOpen = await page.evaluate(reconEntriesLeftControlSnapshot);
  const openAttempt = await openEntriesLeftControlForRecon(page);
  await page.waitForTimeout(600);
  const afterOpen = await page.evaluate(reconEntriesLeftControlAfterOpen);
  const pageDiag = await collectPageDiagnostics(page, 'entries_left_control_recon');
  return {
    currentLabel: beforeOpen.currentLabel,
    controlCandidates: beforeOpen.controlCandidates,
    openClickTarget: beforeOpen.openClickTarget,
    openAttempt,
    afterOpen: {
      popupVisible: afterOpen.popupVisible,
      openMethod: openAttempt?.openMethod || null,
      optionCandidates: afterOpen.optionCandidates,
      bodyTextSampleAroundEntriesLeft: afterOpen.bodyTextSampleAroundEntriesLeft,
    },
    currentUrl: pageDiag.currentUrl,
    pageTitle: pageDiag.pageTitle,
  };
}

async function readEntriesLeftCurrentLabelFromPage(page) {
  assertPlaywrightPage(page, 'readEntriesLeftCurrentLabelFromPage');
  return page.evaluate(() => {
    function normalizeText(text) {
      return String(text || '').replace(/\s+/g, ' ').trim();
    }
    for (const el of document.querySelectorAll('button, span, label, div, a, th')) {
      const t = normalizeText(el.innerText || el.textContent || '');
      if (/entries?\s*left\s*:/i.test(t) && t.length < 80) return t;
    }
    for (const sel of document.querySelectorAll('select')) {
      const context = `${sel.name || ''} ${sel.id || ''} ${sel.closest('label')?.textContent || ''} ${sel.previousElementSibling?.textContent || ''}`.replace(/\s+/g, ' ');
      if (/entries?\s*left/i.test(context)) {
        const opt = sel.options[sel.selectedIndex];
        return normalizeText(opt?.textContent || opt?.label || '');
      }
    }
    return null;
  });
}

async function clickEntriesLeftOptionOnPage(page, optionText) {
  assertPlaywrightPage(page, 'clickEntriesLeftOptionOnPage');
  return page.evaluate((targetText) => {
    function normalizeText(text) {
      return String(text || '').replace(/\s+/g, ' ').trim();
    }
    function isVisible(el) {
      if (!el?.getBoundingClientRect) return false;
      const rect = el.getBoundingClientRect();
      if (rect.width < 2 || rect.height < 2) return false;
      const style = window.getComputedStyle(el);
      if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
      let node = el;
      while (node && node !== document.body) {
        const st = window.getComputedStyle(node);
        if (st.display === 'none' || st.visibility === 'hidden') return false;
        node = node.parentElement;
      }
      return true;
    }
    function selectorHint(el) {
      if (el.id) return `#${CSS.escape(el.id)}`;
      const tag = el.tagName.toLowerCase();
      const classes = [...(el.classList || [])].slice(0, 4);
      return classes.length ? `${tag}.${classes.join('.')}` : tag;
    }
    const wanted = normalizeText(targetText).toLowerCase();
    const matches = [];
    for (const el of document.querySelectorAll('option, li, a, button, span, div[role="option"], [role="menuitem"]')) {
      const text = normalizeText(el.innerText || el.textContent || el.label || '');
      if (text.toLowerCase() !== wanted) continue;
      if (!isVisible(el)) continue;
      matches.push(el);
    }
    if (!matches.length) {
      return { ok: false, reason: 'option_not_found', wanted: targetText };
    }
    matches.sort((a, b) => {
      const aNested = a.querySelector('option, li, a, button, span') ? 1 : 0;
      const bNested = b.querySelector('option, li, a, button, span') ? 1 : 0;
      return aNested - bNested;
    });
    const target = matches[0];
    target.click();
    return {
      ok: true,
      clickTargetText: normalizeText(target.innerText || target.textContent || target.label || ''),
      clickSelectorUsed: selectorHint(target),
      tagName: target.tagName,
    };
  }, optionText);
}

async function waitForEntriesLeftSelectedLabel(page, threshold, { timeoutMs = 8000, pollMs = 250 } = {}) {
  assertPlaywrightPage(page, 'waitForEntriesLeftSelectedLabel');
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const afterLabel = await readEntriesLeftCurrentLabelFromPage(page);
    if (entriesLeftSelectedLabelMatchesThreshold(afterLabel, threshold)) {
      return { ok: true, afterLabel, waitedMs: Date.now() - started };
    }
    await page.waitForTimeout(pollMs);
  }
  const afterLabel = await readEntriesLeftCurrentLabelFromPage(page);
  return {
    ok: entriesLeftSelectedLabelMatchesThreshold(afterLabel, threshold),
    afterLabel,
    waitedMs: Date.now() - started,
  };
}

async function dismissEntriesLeftPopup(page) {
  assertPlaywrightPage(page, 'dismissEntriesLeftPopup');
  await page.keyboard.press('Escape').catch(() => {});
  await blurActiveFilterDropdown(page);
}

async function setEntriesLeftThreshold(page, threshold, options = {}) {
  assertPlaywrightPage(page, 'setEntriesLeftThreshold');
  const requestedThreshold = Math.max(1, Number(threshold) || 1);
  const matchingOptionText = exactAtLeastEntriesLeftOptionText(requestedThreshold);
  const expectedAfterLabel = expectedEntriesLeftSelectedLabel(requestedThreshold);
  const labelWaitMs = options.labelWaitMs ?? Math.max(THRESHOLD_FILTER_SETTLE_MS * 2, 6000);

  const beforeLabel = await readEntriesLeftCurrentLabelFromPage(page);

  const openAttempt = await openEntriesLeftControlForRecon(page);
  if (!openAttempt?.ok) {
    return {
      requestedThreshold,
      beforeLabel,
      popupVisible: false,
      optionTexts: [],
      matchingOptionText,
      clickSelectorUsed: null,
      clickTargetText: null,
      afterLabel: beforeLabel,
      filterSetOk: false,
      filterSetError: 'entries_left_control_open_failed',
      openAttempt,
    };
  }

  await page.waitForTimeout(options.openWaitMs ?? 600);
  const afterOpen = await page.evaluate(reconEntriesLeftControlAfterOpen);
  const popupVisible = afterOpen.popupVisible === true;
  const optionTexts = [...new Set(
    (afterOpen.optionCandidates || []).map((o) => o.normalizedText || o.text).filter(Boolean),
  )];

  const hasMatchingOption = optionTexts.some(
    (text) => text.replace(/\s+/g, ' ').trim().toLowerCase() === matchingOptionText.toLowerCase(),
  );
  if (!hasMatchingOption) {
    await dismissEntriesLeftPopup(page);
    return {
      requestedThreshold,
      beforeLabel,
      popupVisible,
      optionTexts,
      matchingOptionText,
      clickSelectorUsed: null,
      clickTargetText: null,
      afterLabel: beforeLabel,
      filterSetOk: false,
      filterSetError: 'entries_left_option_not_found',
      expectedAfterLabel,
    };
  }

  const clickResult = await clickEntriesLeftOptionOnPage(page, matchingOptionText);
  if (!clickResult?.ok) {
    await dismissEntriesLeftPopup(page);
    return {
      requestedThreshold,
      beforeLabel,
      popupVisible,
      optionTexts,
      matchingOptionText,
      clickSelectorUsed: clickResult?.clickSelectorUsed ?? null,
      clickTargetText: clickResult?.clickTargetText ?? null,
      afterLabel: beforeLabel,
      filterSetOk: false,
      filterSetError: 'entries_left_option_not_found',
      expectedAfterLabel,
      clickResult,
    };
  }

  const labelWait = await waitForEntriesLeftSelectedLabel(page, requestedThreshold, {
    timeoutMs: labelWaitMs,
  });
  await dismissEntriesLeftPopup(page);

  const afterLabel = labelWait.afterLabel ?? await readEntriesLeftCurrentLabelFromPage(page);
  const filterSetOk = entriesLeftSelectedLabelMatchesThreshold(afterLabel, requestedThreshold);

  return {
    requestedThreshold,
    beforeLabel,
    popupVisible,
    optionTexts,
    matchingOptionText,
    clickSelectorUsed: clickResult.clickSelectorUsed ?? null,
    clickTargetText: clickResult.clickTargetText ?? null,
    afterLabel,
    expectedAfterLabel,
    filterSetOk,
    filterSetError: filterSetOk
      ? null
      : (clickResult.ok ? 'entries_left_option_clicked_but_label_unchanged' : 'entries_left_option_not_found'),
    labelWaitMs: labelWait.waitedMs ?? null,
    openAttempt,
    clickResult,
  };
}

async function runDebugEntriesLeftSelectionContract(page, thresholds) {
  const selectionResults = [];
  for (const threshold of thresholds) {
    const result = await setEntriesLeftThreshold(page, threshold);
    selectionResults.push(result);
    if (!result.filterSetOk) break;
    await page.waitForTimeout(400);
  }
  return selectionResults;
}

function scrapeCalendarGridContractSnapshot() {
  const SESSION_LIKE_RE = /\b(?:AT|AB|ET|EB|PRG|INT|PT|PB|BGN|X)\b/gi;
  const LEFT_WAVE = 'Left Wave Sessions';
  const RIGHT_WAVE = 'Right Wave Sessions';
  const WEEKDAY_HEADER_RE = /\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.\s*\d+\b/i;
  const ENTRIES_LEFT_DROPDOWN_LINE_RE = /^\s*at least \d+ entries?\s*left\s*$/i;

  function normalizeText(text) {
    return String(text || '').replace(/\s+/g, ' ').trim();
  }
  function hashText(text) {
    let hash = 5381;
    const normalized = normalizeText(text);
    for (let i = 0; i < normalized.length; i++) hash = ((hash << 5) + hash) + normalized.charCodeAt(i);
    return String(hash >>> 0);
  }
  function isVisible(el) {
    if (!el?.getBoundingClientRect) return false;
    const rect = el.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return false;
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
    let node = el;
    while (node && node !== document.body) {
      const st = window.getComputedStyle(node);
      if (st.display === 'none' || st.visibility === 'hidden') return false;
      node = node.parentElement;
    }
    return true;
  }
  function rawContainerText(el) {
    return String(el?.innerText || el?.textContent || '').replace(/\r/g, '');
  }
  function excludeEntriesLeftDropdownText(text) {
    const lines = String(text || '').replace(/\r/g, '').split('\n');
    const kept = lines.filter((line) => !ENTRIES_LEFT_DROPDOWN_LINE_RE.test(line.trim()));
    return kept.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  }
  function countSessionLikeTokens(text) {
    const matches = normalizeText(text).match(SESSION_LIKE_RE);
    return matches ? matches.length : 0;
  }
  function findLargestVisibleContainer(testFn) {
    let bestText = '';
    for (const el of document.querySelectorAll('div, section, main, table, tbody, article, [class*="agenda"], [class*="calendar"], [class*="dynamic-cal"]')) {
      if (!isVisible(el)) continue;
      const text = rawContainerText(el);
      if (!text || text.length < bestText.length) continue;
      if (!testFn(text)) continue;
      bestText = text;
    }
    return bestText;
  }
  function sliceBodyCalendarRegion(bodyRaw) {
    const leftIdx = bodyRaw.indexOf(LEFT_WAVE);
    if (leftIdx < 0) return { text: '', source: 'none' };

    let slice = bodyRaw.slice(leftIdx);
    const rightIdx = slice.indexOf(RIGHT_WAVE);
    if (rightIdx >= 0) {
      const afterRight = slice.slice(rightIdx);
      const endMarkers = [
        'Show only activities',
        'Show only events',
        'Entries left :',
        'At least 1 entries left',
        'Cookie',
        '©',
      ];
      let end = slice.length;
      for (const marker of endMarkers) {
        const markerIdx = afterRight.indexOf(marker);
        if (markerIdx > 80) end = Math.min(end, rightIdx + markerIdx);
      }
      slice = slice.slice(0, end);
      return { text: slice, source: 'body_slice_both_waves' };
    }

    const partialEndMarkers = ['Show only activities', 'Show only events', 'Entries left :'];
    let end = slice.length;
    for (const marker of partialEndMarkers) {
      const markerIdx = slice.indexOf(marker, LEFT_WAVE.length);
      if (markerIdx > 80) end = Math.min(end, markerIdx);
    }
    return { text: slice.slice(0, end), source: 'partial_wave_section' };
  }
  function extractCalendarGridText() {
    const bothWavesText = findLargestVisibleContainer(
      (text) => text.includes(LEFT_WAVE) && text.includes(RIGHT_WAVE),
    );
    if (bothWavesText.length >= 500) {
      return { text: bothWavesText, source: 'both_wave_sections_container' };
    }

    const leftAndWeekdayText = findLargestVisibleContainer(
      (text) => text.includes(LEFT_WAVE) && WEEKDAY_HEADER_RE.test(text),
    );
    if (leftAndWeekdayText.length >= 500) {
      return { text: leftAndWeekdayText, source: 'left_wave_and_weekday_headers_container' };
    }

    const bodyRaw = excludeEntriesLeftDropdownText(document.body?.innerText || document.body?.textContent || '');
    const bodySlice = sliceBodyCalendarRegion(bodyRaw.replace(/\r/g, ''));
    if (bodySlice.text.length >= 200) {
      return bodySlice;
    }

    if (bodyRaw.includes(LEFT_WAVE) || bodyRaw.includes(RIGHT_WAVE)) {
      const partial = bodyRaw.includes(LEFT_WAVE)
        ? bodyRaw.slice(bodyRaw.indexOf(LEFT_WAVE))
        : bodyRaw.slice(bodyRaw.indexOf(RIGHT_WAVE));
      return { text: partial, source: 'partial_wave_section' };
    }

    return { text: '', source: 'none' };
  }

  const bodyText = normalizeText(document.body?.innerText || document.body?.textContent || '');
  const extracted = extractCalendarGridText();
  const calendarGridTextRaw = excludeEntriesLeftDropdownText(extracted.text);
  const calendarGridTextLength = calendarGridTextRaw.length;
  const calendarGridTextSample = calendarGridTextRaw.slice(0, 800);
  const calendarGridTextForHash = normalizeText(calendarGridTextRaw);

  return {
    bodyTextHash: hashText(bodyText),
    calendarGridTextHash: hashText(calendarGridTextForHash),
    calendarGridTextLength,
    visibleSessionLikeTextCount: countSessionLikeTokens(calendarGridTextRaw),
    calendarGridTextSample,
    calendarGridTextSource: extracted.source,
  };
}

const GRID_CONTRACT_MIN_TEXT_LENGTH = 500;
const GRID_CONTRACT_MIN_SESSION_LIKE_COUNT = 20;
const GRID_CONTRACT_LENGTH_CHANGE_MIN = 50;

function isCalendarGridSnapshotValid(snap) {
  if (!snap) return false;
  return (snap.calendarGridTextLength ?? 0) >= GRID_CONTRACT_MIN_TEXT_LENGTH
    && (snap.visibleSessionLikeTextCount ?? 0) >= GRID_CONTRACT_MIN_SESSION_LIKE_COUNT;
}

function calendarGridTextLengthChangedMeaningfully(before, after) {
  const diff = Math.abs((after?.calendarGridTextLength ?? 0) - (before?.calendarGridTextLength ?? 0));
  return diff >= GRID_CONTRACT_LENGTH_CHANGE_MIN;
}

async function captureCalendarGridContractSnapshot(page, { dismissDropdown = true } = {}) {
  assertPlaywrightPage(page, 'captureCalendarGridContractSnapshot');
  if (dismissDropdown) {
    await dismissEntriesLeftPopup(page);
    await page.waitForTimeout(250);
  }
  const snap = await page.evaluate(scrapeCalendarGridContractSnapshot);
  return {
    ...snap,
    gridSnapshotValid: isCalendarGridSnapshotValid(snap),
  };
}

function calendarGridContractChanged(before, after) {
  if (!before || !after) return false;
  if (!isCalendarGridSnapshotValid(before) || !isCalendarGridSnapshotValid(after)) return false;
  return before.calendarGridTextHash !== after.calendarGridTextHash
    || before.visibleSessionLikeTextCount !== after.visibleSessionLikeTextCount
    || calendarGridTextLengthChangedMeaningfully(before, after);
}

function classifyCalendarGridChangeReason(before, after) {
  if (!isCalendarGridSnapshotValid(before) || !isCalendarGridSnapshotValid(after)) {
    return 'calendar_grid_snapshot_too_narrow';
  }
  if (before.calendarGridTextHash !== after.calendarGridTextHash) {
    return 'calendar_grid_text_hash_changed';
  }
  if (before.visibleSessionLikeTextCount !== after.visibleSessionLikeTextCount) {
    return 'visible_session_like_text_count_changed';
  }
  if (calendarGridTextLengthChangedMeaningfully(before, after)) {
    return 'calendar_grid_text_length_changed';
  }
  return null;
}

function resolveGridChangeOutcome(gridBefore, gridAfter, labelOk) {
  if (!isCalendarGridSnapshotValid(gridBefore) || !isCalendarGridSnapshotValid(gridAfter)) {
    return {
      gridChanged: false,
      gridChangeReason: 'calendar_grid_snapshot_too_narrow',
    };
  }
  const gridChanged = calendarGridContractChanged(gridBefore, gridAfter);
  if (gridChanged) {
    return {
      gridChanged: true,
      gridChangeReason: classifyCalendarGridChangeReason(gridBefore, gridAfter),
    };
  }
  return {
    gridChanged: false,
    gridChangeReason: labelOk ? 'entries_left_label_set_but_grid_unchanged' : 'entries_left_label_not_verified_after_grid_wait',
  };
}

function mapEntriesLeftSelectionContractFields(setResult) {
  return {
    beforeLabel: setResult?.beforeLabel ?? null,
    matchingOptionText: setResult?.matchingOptionText ?? null,
    afterLabel: setResult?.afterLabel ?? null,
    filterSetOk: setResult?.filterSetOk === true,
    filterSetError: setResult?.filterSetError ?? null,
  };
}

async function waitForCalendarGridChangeAfterThreshold(page, gridBefore, threshold, {
  timeoutMs = 8000,
  pollMs = 375,
} = {}) {
  assertPlaywrightPage(page, 'waitForCalendarGridChangeAfterThreshold');
  const started = Date.now();
  let lastAfter = gridBefore;

  while (Date.now() - started < timeoutMs) {
    const afterLabel = await readEntriesLeftCurrentLabelFromPage(page);
    const labelOk = entriesLeftSelectedLabelMatchesThreshold(afterLabel, threshold);
    const after = await captureCalendarGridContractSnapshot(page);
    lastAfter = after;
    if (labelOk) {
      const outcome = resolveGridChangeOutcome(gridBefore, after, labelOk);
      if (outcome.gridChanged) {
        return {
          gridAfter: after,
          gridChanged: true,
          gridChangeReason: outcome.gridChangeReason,
          waitedMs: Date.now() - started,
          labelOk,
        };
      }
      if (outcome.gridChangeReason === 'calendar_grid_snapshot_too_narrow') {
        return {
          gridAfter: after,
          gridChanged: false,
          gridChangeReason: outcome.gridChangeReason,
          waitedMs: Date.now() - started,
          labelOk,
        };
      }
    }
    await page.waitForTimeout(pollMs);
  }

  const afterLabel = await readEntriesLeftCurrentLabelFromPage(page);
  const labelOk = entriesLeftSelectedLabelMatchesThreshold(afterLabel, threshold);
  const gridAfter = lastAfter === gridBefore
    ? await captureCalendarGridContractSnapshot(page)
    : lastAfter;
  const outcome = resolveGridChangeOutcome(gridBefore, gridAfter, labelOk);

  return {
    gridAfter,
    gridChanged: outcome.gridChanged,
    gridChangeReason: outcome.gridChangeReason,
    waitedMs: Date.now() - started,
    labelOk,
  };
}

async function runEntriesLeftThresholdGridChangeContract(page, threshold, options = {}) {
  assertPlaywrightPage(page, 'runEntriesLeftThresholdGridChangeContract');
  const requestedThreshold = Math.max(1, Number(threshold) || 1);
  const gridBefore = await captureCalendarGridContractSnapshot(page);
  const setResult = await setEntriesLeftThreshold(page, requestedThreshold, options);
  const selection = mapEntriesLeftSelectionContractFields(setResult);

  if (!selection.filterSetOk) {
    const gridAfter = await captureCalendarGridContractSnapshot(page);
    const outcome = resolveGridChangeOutcome(gridBefore, gridAfter, false);
    return {
      requestedThreshold,
      selection,
      gridBefore,
      gridAfter,
      gridChanged: outcome.gridChanged,
      gridChangeReason: selection.filterSetError || outcome.gridChangeReason || 'selection_failed',
      waitedMs: 0,
    };
  }

  await dismissEntriesLeftPopup(page);
  await page.waitForTimeout(300);

  const waitResult = await waitForCalendarGridChangeAfterThreshold(
    page,
    gridBefore,
    requestedThreshold,
    {
      timeoutMs: options.gridWaitMs ?? 8000,
      pollMs: options.gridPollMs ?? 375,
    },
  );

  return {
    requestedThreshold,
    selection,
    gridBefore,
    gridAfter: waitResult.gridAfter,
    gridChanged: waitResult.gridChanged,
    gridChangeReason: waitResult.gridChangeReason,
    waitedMs: waitResult.waitedMs,
  };
}

async function runDebugEntriesLeftGridChangeContract(page, thresholds) {
  const gridChangeResults = [];
  for (const threshold of thresholds) {
    const result = await runEntriesLeftThresholdGridChangeContract(page, threshold);
    gridChangeResults.push(result);
    if (!result.selection?.filterSetOk) break;
    await page.waitForTimeout(400);
  }
  return gridChangeResults;
}

async function collectCalendarFixtureDomFromPage(page, fixtureMeta = {}) {
  assertPlaywrightPage(page, 'collectCalendarFixtureDomFromPage');
  const domFixture = await page.evaluate(scrapeCalendarFixtureDom);
  return {
    ...domFixture,
    threshold: fixtureMeta.threshold ?? null,
    isoDate: fixtureMeta.isoDate ?? null,
    navigation: fixtureMeta.navigation ?? null,
  };
}

function mapFixtureParseToTileParserResult(parseResult, domFixture, validationRaw = null) {
  const parserWarnings = Array.isArray(parseResult?.warnings) ? parseResult.warnings : [];
  const validationWarnings = Array.isArray(validationRaw?.warnings) ? validationRaw.warnings : [];
  const parseWarnings = [
    ...parserWarnings,
    ...validationWarnings.map((warning) => (
      typeof warning === 'string'
        ? { type: 'validation_warning', message: warning }
        : warning
    )),
  ];

  return {
    totalCandidateNodes: Number(
      domFixture?.summary?.sessionTileCount
      ?? parseResult?.meta?.sessionTileInputCount
      ?? 0,
    ),
    excludedCount: Number(parseResult?.excludedCount ?? 0),
    parsedCount: Number(parseResult?.parsedCount ?? 0),
    duplicateCount: Number(parseResult?.duplicateCount ?? 0),
    countsByDate: parseResult?.countsByDate && typeof parseResult.countsByDate === 'object'
      ? parseResult.countsByDate
      : {},
    countsByWaveSide: parseResult?.countsByWaveSide && typeof parseResult.countsByWaveSide === 'object'
      ? parseResult.countsByWaveSide
      : {},
    countsBySessionCode: parseResult?.countsBySessionCode && typeof parseResult.countsBySessionCode === 'object'
      ? parseResult.countsBySessionCode
      : {},
    parsedIdentitiesSample: Array.isArray(parseResult?.parsedIdentitiesSample)
      ? parseResult.parsedIdentitiesSample
      : [],
    excludedSamples: Array.isArray(parseResult?.excludedSamples)
      ? parseResult.excludedSamples
      : [],
    parseWarnings,
    spatialIndex: buildSpatialIndexFromDomFixture(domFixture),
  };
}

function buildSpatialIndexFromDomFixture(domFixture) {
  const summary = domFixture?.summary;
  if (!summary) return null;
  return {
    waveHeaderCount: summary.waveHeaderCount ?? null,
    dayHeaderCount: summary.dayHeaderCount ?? null,
    timeLabelCount: summary.timeLabelCount ?? null,
    sessionTileCount: summary.sessionTileCount ?? null,
    sessionTileLeftCount: summary.sessionTileLeftCount ?? null,
    sessionTileRightCount: summary.sessionTileRightCount ?? null,
    waveHeaders: summary.waveHeaders ?? [],
    hasLeftWaveHeader: summary.hasLeftWaveHeader === true,
    hasRightWaveHeader: summary.hasRightWaveHeader === true,
    byCategory: summary.byCategory ?? null,
  };
}

function buildGridSnapshotResponse(snap) {
  if (!snap) return null;
  return {
    bodyTextHash: snap.bodyTextHash ?? null,
    calendarGridTextHash: snap.calendarGridTextHash ?? null,
    calendarGridTextLength: Number(snap.calendarGridTextLength ?? 0),
    visibleSessionLikeTextCount: Number(snap.visibleSessionLikeTextCount ?? 0),
    calendarGridTextSample: snap.calendarGridTextSample ?? null,
    calendarGridTextSource: snap.calendarGridTextSource ?? null,
    gridSnapshotValid: snap.gridSnapshotValid === true || isCalendarGridSnapshotValid(snap),
  };
}

function assembleGate5TileParserContractPayload(parserRun) {
  const gridSnapshot = parserRun?.gridSnapshotRaw
    ? buildGridSnapshotResponse(parserRun.gridSnapshotRaw)
    : buildGridSnapshotResponse(parserRun?.gridSnapshot);

  const validationRaw = parserRun?.tileParserValidationRaw
    || buildTileParserContractValidation({
      thresholdSelection: parserRun?.thresholdSelection,
      gridSnapshot,
      tileParserResult: parserRun?.tileParserResult,
    });

  const tileParserResult = parserRun?.parseResult
    ? mapFixtureParseToTileParserResult(
      parserRun.parseResult,
      {
        summary: parserRun.domFixtureSummary,
        threshold: parserRun.parseResult?.meta?.threshold ?? null,
        isoDate: parserRun.parseResult?.meta?.isoDate ?? null,
      },
      validationRaw,
    )
    : (parserRun?.tileParserResult || emptyTileParserContractResult());

  const tileParserValidation = formatTileParserValidationForResponse(validationRaw);

  return {
    gridSnapshot,
    tileParserResult,
    tileParserValidation,
    tileParserContractOk: tileParserValidation?.ok === true,
    error: tileParserValidation?.ok
      ? null
      : resolveTileParserContractError(tileParserValidation, tileParserResult),
  };
}

function formatTileParserValidationForResponse(validation) {
  if (!validation) return null;
  const checks = validation.checks || {};
  return {
    ok: validation.ok === true,
    errors: validation.errors || [],
    validationErrors: validation.errors || [],
    warnings: validation.warnings || [],
    checks,
    ...checks,
  };
}

function emptyTileParserContractResult() {
  return {
    totalCandidateNodes: 0,
    excludedCount: 0,
    parsedCount: 0,
    duplicateCount: 0,
    parsedIdentitiesSample: [],
    excludedSamples: [],
    countsByDate: {},
    countsByWaveSide: {},
    countsBySessionCode: {},
    parseWarnings: [],
    spatialIndex: null,
  };
}

function normalizeGridSnapshotForResponse(snap) {
  return buildGridSnapshotResponse(snap);
}

function visibleCodeMatchesSessionCode(sourceText, sessionCode) {
  const compact = String(sourceText || '').replace(/\s+/g, '').toUpperCase();
  const m = compact.match(/^(AT|AB|ET|EB|PRG|INT|PT|PB|BGN)\*?$/);
  if (!m) return true;
  return m[1] === sessionCode;
}

function buildTileParserContractValidation({
  thresholdSelection,
  gridSnapshot,
  tileParserResult,
} = {}) {
  const errors = [];
  const warnings = [];

  if (!thresholdSelection?.filterSetOk) {
    errors.push('threshold_selection_failed');
  }
  if (!gridSnapshot?.gridSnapshotValid) {
    errors.push('grid_snapshot_invalid');
  }

  const parsedCount = tileParserResult?.parsedCount ?? 0;
  const leftCount = tileParserResult?.countsByWaveSide?.left ?? 0;
  const rightCount = tileParserResult?.countsByWaveSide?.right ?? 0;

  if (parsedCount <= 0) errors.push('parsed_count_zero');
  if (leftCount <= 0) errors.push('missing_left_wave');
  if (rightCount <= 0) errors.push('missing_right_wave');

  const parsedSample = tileParserResult?.parsedIdentitiesSample || [];
  const pbParsed = (tileParserResult?.countsBySessionCode?.PB ?? 0) > 0
    || parsedSample.some((p) => p.sessionCode === 'PB');
  if (!pbParsed) warnings.push('pb_not_parsed_in_sample');

  const excludedSamples = tileParserResult?.excludedSamples || [];
  const xExcluded = excludedSamples.some(
    (e) => e.reason === 'x_cell' || /^x$/i.test(String(e.sourceText || '').trim()),
  );
  if (!xExcluded && parsedCount > 0) warnings.push('x_exclusion_not_observed_in_samples');

  const mismatchWarnings = (tileParserResult?.parseWarnings || []).filter(
    (w) => w && typeof w === 'object' && w.type === 'title_visible_code_mismatch',
  );
  for (const mismatch of mismatchWarnings.slice(0, 10)) {
    warnings.push(`title_visible_code_mismatch:${mismatch.sourceText}:${mismatch.titleInferredCode}->${mismatch.visibleNormalizedCode}`);
  }

  const visibleCodeWins = parsedSample.every(
    (p) => visibleCodeMatchesSessionCode(p.sourceText, p.sessionCode),
  );
  if (!visibleCodeWins) errors.push('visible_code_does_not_match_session_code');

  const checks = {
    parsedCountPositive: parsedCount > 0,
    hasLeftWave: leftCount > 0,
    hasRightWave: rightCount > 0,
    pbParsed,
    xExcluded,
    visibleCodeWins,
  };

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    checks,
  };
}

function resolveTileParserContractError(validation, tileParserResult) {
  if (!validation) return 'tile_parser_contract_failed';
  if (validation.errors.includes('threshold_selection_failed')) return 'threshold_selection_failed';
  if (validation.errors.includes('grid_snapshot_invalid')) return 'calendar_grid_snapshot_too_narrow';
  if (validation.errors.includes('parsed_count_zero')) return 'tile_parser_no_identities_parsed';
  if (validation.errors.includes('missing_left_wave') || validation.errors.includes('missing_right_wave')) {
    return 'tile_parser_missing_wave_side_counts';
  }
  if (validation.errors.includes('visible_code_does_not_match_session_code')) {
    return 'tile_parser_visible_code_mismatch';
  }
  if ((tileParserResult?.parsedCount ?? 0) <= 0) return 'tile_parser_no_identities_parsed';
  return 'tile_parser_contract_failed';
}

async function runDebugEntriesLeftTileParserContract(page, threshold = 1, { isoDate, navigation } = {}) {
  const requestedThreshold = Math.max(1, Number(threshold) || 1);
  const setResult = await setEntriesLeftThreshold(page, requestedThreshold);
  const thresholdSelection = mapEntriesLeftSelectionContractFields(setResult);

  if (!thresholdSelection.filterSetOk) {
    const tileParserResult = emptyTileParserContractResult();
    const tileParserValidation = formatTileParserValidationForResponse(
      buildTileParserContractValidation({
        thresholdSelection,
        gridSnapshot: null,
        tileParserResult,
      }),
    );
    return {
      thresholdSelection,
      gridSnapshot: null,
      tileParserResult,
      tileParserValidation,
      error: resolveTileParserContractError(tileParserValidation, tileParserResult),
    };
  }

  await dismissEntriesLeftPopup(page);
  await page.waitForTimeout(300);
  const gridSnapshotRaw = await captureCalendarGridContractSnapshot(page);
  const gridSnapshot = buildGridSnapshotResponse(gridSnapshotRaw);

  const domFixture = await collectCalendarFixtureDomFromPage(page, {
    threshold: requestedThreshold,
    isoDate: isoDate || null,
    navigation: {
      rawMonthLabel: navigation?.rawMonthLabel ?? null,
      rawDayHeaderTexts: navigation?.rawDayHeaderTexts ?? [],
      visibleIsoDatesFromHeaders: navigation?.visibleIsoDatesFromHeaders ?? [],
      targetDateVisibleFromHeaders: navigation?.targetDateVisibleFromHeaders ?? null,
      currentUrl: navigation?.currentUrl ?? null,
    },
  });
  const parseResult = parseCalendarFixtureDom(domFixture);
  const tileParserValidationRaw = buildTileParserContractValidation({
    thresholdSelection,
    gridSnapshot,
    tileParserResult: mapFixtureParseToTileParserResult(parseResult, domFixture),
  });
  const tileParserResult = mapFixtureParseToTileParserResult(
    parseResult,
    domFixture,
    tileParserValidationRaw,
  );
  const tileParserValidation = formatTileParserValidationForResponse(tileParserValidationRaw);

  return {
    thresholdSelection,
    gridSnapshot,
    gridSnapshotRaw,
    parseResult,
    domFixtureSummary: domFixture.summary ?? null,
    tileParserResult,
    tileParserValidation,
    tileParserValidationRaw,
    error: tileParserValidation.ok
      ? null
      : resolveTileParserContractError(tileParserValidation, tileParserResult),
  };
}

function collectIdentityKeysFromParseResult(parseResult) {
  if (Array.isArray(parseResult?.identityKeys)) {
    return new Set(parseResult.identityKeys.filter(Boolean));
  }
  const keys = new Set();
  for (const entry of parseResult?.parsedIdentitiesSample || []) {
    if (entry?.identityKey) keys.add(entry.identityKey);
  }
  return keys;
}

function mergeTileByIdentityFromParseResult(tileByIdentity, parseResult) {
  for (const entry of parseResult?.parsedIdentitiesSample || []) {
    if (entry?.identityKey && !tileByIdentity.has(entry.identityKey)) {
      tileByIdentity.set(entry.identityKey, entry);
    }
  }
}

function thresholdCountsNonIncreasing(visibleTileCountsByThreshold, thresholds) {
  const counts = thresholds.map((t) => visibleTileCountsByThreshold[t] ?? null);
  if (counts.some((c) => c == null)) return false;
  for (let i = 1; i < counts.length; i += 1) {
    if (counts[i] > counts[i - 1]) return false;
  }
  return true;
}

function buildGate6InferenceSamples(inferences, confidence, limit = 12) {
  return inferences
    .filter((row) => row.inference?.thresholdConfidence === confidence)
    .slice(0, limit)
    .map((row) => ({
      identityKey: row.identityKey,
      thresholdsSeen: row.thresholdsSeen,
      availableEntries: row.inference?.thresholdInferredSlots ?? null,
      thresholdConfidence: row.inference?.thresholdConfidence,
      thresholdMaxVisible: row.inference?.thresholdMaxVisible ?? null,
      sessionCode: row.tile?.sessionCode ?? null,
      waveSide: row.tile?.waveSide ?? null,
      isoDate: row.tile?.isoDate ?? null,
      timeLabel: row.tile?.timeLabel ?? null,
    }));
}

async function runDebugEntriesLeftThresholdFixtureParse(page, threshold, { isoDate, navigation } = {}) {
  const requestedThreshold = Math.max(1, Number(threshold) || 1);
  const setResult = await setEntriesLeftThreshold(page, requestedThreshold);
  const thresholdSelection = mapEntriesLeftSelectionContractFields(setResult);

  if (!thresholdSelection.filterSetOk) {
    return {
      threshold,
      thresholdSelection,
      gridSnapshot: null,
      gridSnapshotRaw: null,
      parseResult: null,
      domFixture: null,
      tileParserResult: emptyTileParserContractResult(),
      tileParserValidation: formatTileParserValidationForResponse(
        buildTileParserContractValidation({
          thresholdSelection,
          gridSnapshot: null,
          tileParserResult: emptyTileParserContractResult(),
        }),
      ),
      tileParserContractOk: false,
      identityKeys: new Set(),
    };
  }

  await dismissEntriesLeftPopup(page);
  await page.waitForTimeout(300);
  const gridSnapshotRaw = await captureCalendarGridContractSnapshot(page);
  const gridSnapshot = buildGridSnapshotResponse(gridSnapshotRaw);

  const domFixture = await collectCalendarFixtureDomFromPage(page, {
    threshold: requestedThreshold,
    isoDate: isoDate || null,
    navigation: {
      rawMonthLabel: navigation?.rawMonthLabel ?? null,
      rawDayHeaderTexts: navigation?.rawDayHeaderTexts ?? [],
      visibleIsoDatesFromHeaders: navigation?.visibleIsoDatesFromHeaders ?? [],
      targetDateVisibleFromHeaders: navigation?.targetDateVisibleFromHeaders ?? null,
      currentUrl: navigation?.currentUrl ?? null,
    },
  });
  const parseResult = parseCalendarFixtureDom(domFixture);
  const tileParserValidationRaw = buildTileParserContractValidation({
    thresholdSelection,
    gridSnapshot,
    tileParserResult: mapFixtureParseToTileParserResult(parseResult, domFixture),
  });
  const tileParserResult = mapFixtureParseToTileParserResult(
    parseResult,
    domFixture,
    tileParserValidationRaw,
  );
  const tileParserValidation = formatTileParserValidationForResponse(tileParserValidationRaw);
  const identityKeys = collectIdentityKeysFromParseResult(parseResult);

  return {
    threshold: requestedThreshold,
    thresholdSelection,
    gridSnapshot,
    gridSnapshotRaw,
    parseResult,
    domFixture,
    tileParserResult,
    tileParserValidation,
    tileParserValidationRaw,
    tileParserContractOk: tileParserValidation?.ok === true,
    identityKeys,
  };
}

async function runDebugEntriesLeftThresholdInferenceContract(page, thresholds, { isoDate, navigation } = {}) {
  const thresholdList = (Array.isArray(thresholds) ? thresholds : [1, 2, 3])
    .map((t) => Math.max(1, Number(t) || 1))
    .filter((t) => Number.isFinite(t))
    .sort((a, b) => a - b);
  const maxTested = thresholdList.length ? Math.max(...thresholdList) : 3;

  const visibleByThreshold = new Map();
  const thresholdResults = [];
  const tileByIdentity = new Map();
  let stoppedEarly = false;
  let stopReason = null;

  for (const threshold of thresholdList) {
    const run = await runDebugEntriesLeftThresholdFixtureParse(page, threshold, { isoDate, navigation });
    thresholdResults.push({
      threshold: run.threshold,
      thresholdSelection: run.thresholdSelection,
      gridSnapshot: run.gridSnapshot,
      tileParserContractOk: run.tileParserContractOk,
      tileParserValidation: run.tileParserValidation,
      tileParserResult: {
        parsedCount: run.tileParserResult?.parsedCount ?? 0,
        identityKeyCount: run.identityKeys?.size ?? 0,
        countsByWaveSide: run.tileParserResult?.countsByWaveSide ?? {},
        countsBySessionCode: run.tileParserResult?.countsBySessionCode ?? {},
      },
    });

    if (!run.thresholdSelection?.filterSetOk) {
      stoppedEarly = true;
      stopReason = run.thresholdSelection?.filterSetError || 'threshold_selection_failed';
      break;
    }
    if (!run.tileParserContractOk) {
      stoppedEarly = true;
      stopReason = resolveTileParserContractError(run.tileParserValidation, run.tileParserResult)
        || 'tile_parser_contract_failed';
      break;
    }

    visibleByThreshold.set(threshold, run.identityKeys || new Set());
    if (run.parseResult) mergeTileByIdentityFromParseResult(tileByIdentity, run.parseResult);
    await page.waitForTimeout(400);
  }

  const visibleTileCountsByThreshold = Object.fromEntries(
    [...visibleByThreshold.entries()].map(([t, keys]) => [t, keys.size]),
  );

  const presenceByIdentity = buildThresholdPresenceMap(visibleByThreshold, maxTested);
  const thresholdPresenceBySession = {};
  const inferences = [];

  for (const [identityKey, thresholdsSeen] of presenceByIdentity.entries()) {
    const sortedSeen = [...thresholdsSeen].sort((a, b) => a - b);
    thresholdPresenceBySession[identityKey] = sortedSeen;
    const tile = tileByIdentity.get(identityKey) || null;
    const inference = inferSlotsFromThresholdPresence(sortedSeen, maxTested, {
      sessionCode: tile?.sessionCode ?? null,
    });
    inferences.push({
      identityKey,
      thresholdsSeen: sortedSeen,
      inference,
      tile,
    });
  }

  const exactInferences = inferences.filter((row) => row.inference?.thresholdConfidence === 'exact');
  const atLeastInferences = inferences.filter((row) => row.inference?.thresholdConfidence === 'at_least');
  const exactCount = exactInferences.length;
  const atLeastCount = atLeastInferences.length;
  const noMatchCount = inferences.filter((row) => row.inference?.thresholdConfidence === 'no_match').length;

  const writeSafety = evaluateThresholdWriteSafety({
    thresholdsScanned: thresholdList.filter((t) => visibleByThreshold.has(t)),
    visibleTileCountsByThreshold,
    thresholdPresenceBySession,
  });

  const selectionOk = thresholdResults.length > 0
    && thresholdResults.every((row) => row.thresholdSelection?.filterSetOk);
  const parserOk = thresholdResults.length > 0
    && thresholdResults.every((row) => row.tileParserContractOk);
  const countsNonIncreasing = thresholdCountsNonIncreasing(visibleTileCountsByThreshold, thresholdList);
  const hasInferenceResults = exactCount + atLeastCount > 0;
  const filterEffective = writeSafety.statusReason !== 'threshold_filter_not_effective';

  const inferenceContractOk = !stoppedEarly
    && selectionOk
    && parserOk
    && countsNonIncreasing
    && hasInferenceResults
    && filterEffective;

  let error = null;
  if (stoppedEarly) {
    error = stopReason;
  } else if (!selectionOk) {
    error = 'threshold_selection_failed';
  } else if (!parserOk) {
    error = 'tile_parser_contract_failed';
  } else if (!countsNonIncreasing) {
    error = 'visible_tile_counts_increased';
  } else if (!hasInferenceResults) {
    error = 'no_threshold_inferences';
  } else if (!filterEffective) {
    error = writeSafety.statusReason || 'threshold_filter_not_effective';
  }

  return {
    thresholdResults,
    visibleTileCountsByThreshold,
    thresholdPresenceBySessionSample: sampleThresholdPresenceBySession(thresholdPresenceBySession, 12),
    exactInferencesSample: buildGate6InferenceSamples(inferences, 'exact'),
    atLeastInferencesSample: buildGate6InferenceSamples(inferences, 'at_least'),
    exactCount,
    atLeastCount,
    noMatchCount,
    inferenceContractOk,
    thresholdFilterEffective: filterEffective,
    pctVisibleAtAllThresholds: writeSafety.pctVisibleAtAllThresholds ?? null,
    hasTileCountDecrease: writeSafety.hasTileCountDecrease ?? null,
    error,
  };
}

function entriesLeftDebugGateForMode(mode) {
  if (mode === 'threshold_inference_contract') return 6;
  if (mode === 'tile_parser_contract') return 5;
  if (mode === 'grid_change_contract') return 4;
  if (mode === 'selection_contract') return 3;
  return 2;
}

async function runDebugEntriesLeftControl({
  isoDate,
  weekMode = true,
  thresholds = [1, 2, 3],
  threshold = 1,
  dryRun = true,
  debug = true,
  mode = 'recon',
} = {}) {
  const gateNum = entriesLeftDebugGateForMode(mode);
  const requestedIsoDate = isoDate;
  const computedWeekStart = getMondayWeekStartIso(isoDate);
  const navigationIsoDate = weekMode ? computedWeekStart : isoDate;
  let launched = null;
  const started = Date.now();

  try {
    launched = await launchBrowser();
    await openBookingPageForThreshold(launched.page);
    await waitForThresholdCalendarShell(launched.page).catch(() => {});

    const nav = await navigateCalendarToShowDate(launched.page, navigationIsoDate, {
      headerOnly: true,
      validateIsoDate: requestedIsoDate,
      waitForShell: true,
    });

    if (nav.statusReason === 'calendar_day_headers_not_parsed' || nav.headerParseError) {
      return {
        gate: gateNum,
        mode,
        dryRun,
        debug,
        isoDate: requestedIsoDate,
        weekMode,
        thresholds,
        skipped: true,
        skipReason: nav.statusReason || 'header_parse_failed',
        navigation: nav,
        durationMs: Date.now() - started,
      };
    }

    if (!nav.targetDateVisibleFromHeaders) {
      return {
        gate: gateNum,
        mode,
        dryRun,
        debug,
        isoDate: requestedIsoDate,
        weekMode,
        thresholds,
        skipped: true,
        skipReason: 'target_date_not_visible',
        navigation: {
          rawMonthLabel: nav.rawMonthLabel,
          rawDayHeaderTexts: nav.rawDayHeaderTexts,
          visibleIsoDatesFromHeaders: nav.visibleIsoDatesFromHeaders,
          targetDateVisibleFromHeaders: nav.targetDateVisibleFromHeaders,
          currentUrl: nav.currentUrl,
        },
        durationMs: Date.now() - started,
      };
    }

    await normalizeBookingFiltersOnPage(launched.page).catch(() => {});
    await launched.page.waitForTimeout(500);

    const pageDiag = await collectPageDiagnostics(launched.page, 'entries_left_control');

    if (mode === 'selection_contract') {
      const selectionResults = await runDebugEntriesLeftSelectionContract(launched.page, thresholds);
      const allOk = selectionResults.length > 0 && selectionResults.every((r) => r.filterSetOk);
      return {
        gate: 3,
        mode,
        dryRun,
        debug,
        isoDate: requestedIsoDate,
        computedWeekStart,
        navigationIsoDate,
        weekMode,
        thresholds,
        rawMonthLabel: nav.rawMonthLabel,
        rawDayHeaderTexts: nav.rawDayHeaderTexts,
        visibleIsoDatesFromHeaders: nav.visibleIsoDatesFromHeaders,
        targetDateVisibleFromHeaders: nav.targetDateVisibleFromHeaders,
        currentUrl: pageDiag.currentUrl,
        pageTitle: pageDiag.pageTitle,
        selectionResults,
        selectionContractOk: allOk,
        durationMs: Date.now() - started,
        writesPerformed: false,
        thresholdWriteSafe: false,
        crashed: false,
        error: allOk ? null : (selectionResults.find((r) => !r.filterSetOk)?.filterSetError || 'selection_contract_failed'),
      };
    }

    if (mode === 'grid_change_contract') {
      const gridChangeResults = await runDebugEntriesLeftGridChangeContract(launched.page, thresholds);
      const selectionOk = gridChangeResults.length > 0
        && gridChangeResults.every((r) => r.selection?.filterSetOk);
      const snapshotsValid = gridChangeResults.every(
        (r) => r.gridBefore?.gridSnapshotValid && r.gridAfter?.gridSnapshotValid,
      );
      const gridEvidenceOk = gridChangeResults.every((r) => (
        r.gridChanged === true
        || r.gridChangeReason === 'entries_left_label_set_but_grid_unchanged'
      ));
      const contractOk = selectionOk && snapshotsValid && gridEvidenceOk;
      return {
        gate: 4,
        mode,
        dryRun,
        debug,
        isoDate: requestedIsoDate,
        computedWeekStart,
        navigationIsoDate,
        weekMode,
        thresholds,
        rawMonthLabel: nav.rawMonthLabel,
        rawDayHeaderTexts: nav.rawDayHeaderTexts,
        visibleIsoDatesFromHeaders: nav.visibleIsoDatesFromHeaders,
        targetDateVisibleFromHeaders: nav.targetDateVisibleFromHeaders,
        currentUrl: pageDiag.currentUrl,
        pageTitle: pageDiag.pageTitle,
        gridChangeResults,
        gridChangeContractOk: contractOk,
        durationMs: Date.now() - started,
        writesPerformed: false,
        thresholdWriteSafe: false,
        crashed: false,
        error: contractOk
          ? null
          : (
            gridChangeResults.find((r) => !r.selection?.filterSetOk)?.selection?.filterSetError
            || (!snapshotsValid ? 'calendar_grid_snapshot_too_narrow' : null)
            || gridChangeResults.find((r) => r.selection?.filterSetOk && r.gridChanged !== true && r.gridChangeReason !== 'entries_left_label_set_but_grid_unchanged')?.gridChangeReason
            || 'grid_change_contract_failed'
          ),
      };
    }

    if (mode === 'tile_parser_contract') {
      const parserThreshold = Math.max(1, Number(threshold) || 1);
      const parserRun = await runDebugEntriesLeftTileParserContract(launched.page, parserThreshold, {
        isoDate: requestedIsoDate,
        navigation: nav,
      });
      const assembled = assembleGate5TileParserContractPayload(parserRun);
      return {
        gate: 5,
        mode,
        dryRun,
        debug,
        isoDate: requestedIsoDate,
        computedWeekStart,
        navigationIsoDate,
        weekMode,
        threshold: parserThreshold,
        currentUrl: pageDiag.currentUrl,
        rawMonthLabel: nav.rawMonthLabel,
        rawDayHeaderTexts: nav.rawDayHeaderTexts,
        visibleIsoDatesFromHeaders: nav.visibleIsoDatesFromHeaders,
        targetDateVisibleFromHeaders: nav.targetDateVisibleFromHeaders,
        thresholdSelection: parserRun.thresholdSelection,
        gridSnapshot: assembled.gridSnapshot,
        tileParserContractOk: assembled.tileParserContractOk,
        tileParserValidation: assembled.tileParserValidation,
        tileParserResult: assembled.tileParserResult,
        durationMs: Date.now() - started,
        writesPerformed: false,
        thresholdWriteSafe: false,
        crashed: false,
        error: assembled.error,
      };
    }

    if (mode === 'threshold_inference_contract') {
      const inferenceThresholds = (Array.isArray(thresholds) && thresholds.length
        ? thresholds
        : [1, 2, 3]
      ).map((t) => Math.max(1, Number(t) || 1)).filter((t) => Number.isFinite(t)).sort((a, b) => a - b);
      const inferenceRun = await runDebugEntriesLeftThresholdInferenceContract(
        launched.page,
        inferenceThresholds,
        { isoDate: requestedIsoDate, navigation: nav },
      );
      return {
        gate: 6,
        mode,
        dryRun,
        debug,
        isoDate: requestedIsoDate,
        computedWeekStart,
        navigationIsoDate,
        weekMode,
        thresholds: inferenceThresholds,
        currentUrl: pageDiag.currentUrl,
        rawMonthLabel: nav.rawMonthLabel,
        rawDayHeaderTexts: nav.rawDayHeaderTexts,
        visibleIsoDatesFromHeaders: nav.visibleIsoDatesFromHeaders,
        targetDateVisibleFromHeaders: nav.targetDateVisibleFromHeaders,
        thresholdResults: inferenceRun.thresholdResults,
        visibleTileCountsByThreshold: inferenceRun.visibleTileCountsByThreshold,
        thresholdPresenceBySessionSample: inferenceRun.thresholdPresenceBySessionSample,
        exactInferencesSample: inferenceRun.exactInferencesSample,
        atLeastInferencesSample: inferenceRun.atLeastInferencesSample,
        exactCount: inferenceRun.exactCount,
        atLeastCount: inferenceRun.atLeastCount,
        noMatchCount: inferenceRun.noMatchCount,
        inferenceContractOk: inferenceRun.inferenceContractOk,
        durationMs: Date.now() - started,
        writesPerformed: false,
        thresholdWriteSafe: false,
        crashed: false,
        error: inferenceRun.error,
      };
    }

    const recon = await scrapeEntriesLeftControlReconFromPage(launched.page);
    const thresholdAvailability = buildThresholdAvailabilityFromOptions(
      recon.afterOpen.optionCandidates,
      thresholds,
    );

    return {
      gate: 2,
      mode: mode || 'recon',
      dryRun,
      debug,
      isoDate: requestedIsoDate,
      computedWeekStart,
      navigationIsoDate,
      weekMode,
      thresholds,
      thresholdAvailability,
      rawMonthLabel: nav.rawMonthLabel,
      rawDayHeaderTexts: nav.rawDayHeaderTexts,
      visibleIsoDatesFromHeaders: nav.visibleIsoDatesFromHeaders,
      targetDateVisibleFromHeaders: nav.targetDateVisibleFromHeaders,
      ...recon,
      durationMs: Date.now() - started,
      writesPerformed: false,
      thresholdWriteSafe: false,
    };
  } finally {
    await safeCloseBrowser(launched);
  }
}

async function setEntriesLeftFilter(page, minThreshold) {
  assertPlaywrightPage(page, 'setEntriesLeftFilter');
  await dismissCookieBanner(page).catch(() => {});
  const n = Math.max(1, Number(minThreshold) || 1);

  let changed = await page.evaluate((threshold) => {
    const patterns = [
      new RegExp(`entries?\\s*left\\s*:?\\s*${threshold}\\b`, 'i'),
      new RegExp(`at\\s*least\\s*${threshold}\\s*entries?\\s*left`, 'i'),
      new RegExp(`\\b${threshold}\\s*entries?\\s*left`, 'i'),
    ];

    const matchesLabel = (label) => patterns.some(re => re.test(label));

    for (const sel of document.querySelectorAll('select')) {
      for (const opt of sel.options) {
        const label = (opt.textContent || opt.label || '').replace(/\s+/g, ' ').trim();
        if (matchesLabel(label) || opt.value === String(threshold)) {
          sel.value = opt.value;
          sel.dispatchEvent(new Event('change', { bubbles: true }));
          sel.dispatchEvent(new Event('input', { bubbles: true }));
          return { ok: true, method: 'select', label, threshold };
        }
      }
    }

    const clickables = [...document.querySelectorAll('option, li, a, button, span, label, div')];
    for (const el of clickables) {
      const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
      if (!t || t.length > 80) continue;
      if (matchesLabel(t)) {
        el.click();
        return { ok: true, method: 'click', label: t, threshold };
      }
    }

    const filterLabels = [...document.querySelectorAll('label, span, th, div, button')].filter(el =>
      /entries?\s*left/i.test(el.textContent || ''),
    );
    for (const labelEl of filterLabels) {
      labelEl.click();
      const parent = labelEl.closest('form, .panel, .filter, .dropdown, .row, div') || document.body;
      for (const optEl of parent.querySelectorAll('option, li, a, button, span')) {
        const t = (optEl.textContent || '').replace(/\s+/g, ' ').trim();
        if (matchesLabel(t)) {
          optEl.click();
          return { ok: true, method: 'dropdown_click', label: t, threshold };
        }
      }
    }

    return { ok: false, reason: 'filter_option_not_found', threshold };
  }, n);

  if (!changed?.ok) {
    const loc = page.getByText(new RegExp(`Entries?\\s*left\\s*:?\\s*${n}\\b`, 'i')).first();
    if (await loc.count() && await loc.isVisible().catch(() => false)) {
      await loc.click().catch(() => {});
      changed = { ok: true, method: 'playwright_text', threshold: n };
    }
  }

  if (changed?.ok) {
    await blurActiveFilterDropdown(page);
    await page.waitForTimeout(THRESHOLD_FILTER_SETTLE_MS);
    await page.waitForFunction(() =>
      document.querySelectorAll('div.dynamic-cal-booking-ts').length >= 0,
    ).catch(() => {});
  }

  return changed || { ok: false, reason: 'filter_set_failed', threshold: n };
}

function buildThresholdPresenceMap(visibleByThreshold, maxTested) {
  const presenceByIdentity = new Map();
  for (let t = 1; t <= maxTested; t++) {
    const visible = visibleByThreshold.get(t) || new Set();
    for (const identityKey of visible) {
      if (!presenceByIdentity.has(identityKey)) presenceByIdentity.set(identityKey, []);
      presenceByIdentity.get(identityKey).push(t);
    }
  }
  return presenceByIdentity;
}

function matchThresholdResultsToSessions({
  presenceByIdentity,
  tileByIdentity,
  basicSessions,
  maxTested,
}) {
  const results = [];
  const ambiguousSamples = [];
  const matchedSessionKeys = new Set();
  const thresholdPresenceBySession = {};

  for (const [identityKey, thresholdsSeen] of presenceByIdentity.entries()) {
    const tile = tileByIdentity.get(identityKey);
    if (!tile) continue;
    thresholdPresenceBySession[identityKey] = thresholdsSeen.sort((a, b) => a - b);

    const weekCandidates = basicSessions.filter(s => sessionDateKey(s) === tile.isoDate);
    const match = matchThresholdTileToSessions(tile, weekCandidates);
    const inference = inferSlotsFromThresholdPresence(thresholdsSeen, maxTested, {
      sessionCode: tile.sessionCode,
      level: tile.level,
      inBasicScrape: false,
    });

    if (match.session) {
      matchedSessionKeys.add(match.session.key);
      results.push({
        session: match.session,
        tile,
        identityKey,
        thresholdsSeen,
        maxVisible: inference.thresholdMaxVisible,
        inference,
        matchConfidence: match.confidence,
        matchMethod: match.matchMethod,
        thresholdConfidence: inference.thresholdConfidence,
      });
    } else if (match.confidence === 'ambiguous') {
      ambiguousSamples.push({ identityKey, thresholdsSeen, ...match });
      results.push({
        session: null,
        tile,
        identityKey,
        thresholdsSeen,
        maxVisible: inference.thresholdMaxVisible,
        inference: {
          thresholdInferredSlots: null,
          thresholdMaxVisible: inference.thresholdMaxVisible,
          thresholdConfidence: 'ambiguous',
          thresholdScanVerified: false,
          reason: match.reason,
        },
        matchConfidence: 'ambiguous',
        ambiguousSample: match.sample,
      });
    }
  }

  for (const basic of basicSessions) {
    if (matchedSessionKeys.has(basic.key)) continue;
    const identityKey = makeThresholdIdentityKey({
      isoDate: sessionDateKey(basic),
      timeLabel: basic.time,
      waveSide: basic.waveSide,
      sessionCode: levelToSessionCode(basic.level),
      level: basic.level,
    });
    const thresholdsSeen = presenceByIdentity.get(identityKey) || [];
    const inference = inferSlotsFromThresholdPresence(thresholdsSeen, maxTested, {
      sessionCode: levelToSessionCode(basic.level),
      level: basic.level,
      inBasicScrape: true,
    });
    results.push({
      session: basic,
      tile: null,
      identityKey,
      thresholdsSeen,
      maxVisible: inference.thresholdMaxVisible || 0,
      inference,
      matchConfidence: inference.thresholdConfidence === 'no_match' ? 'no_match' : 'basic_only',
      thresholdConfidence: inference.thresholdConfidence,
    });
  }

  return { results, ambiguousSamples, thresholdPresenceBySession };
}

async function scanEntriesLeftThresholdsForWeek(page, {
  requestedIsoDate: requestedIsoDateInput = null,
  navigationIsoDate: navigationIsoDateInput = null,
  targetIsoDate = null,
  thresholds,
  minThreshold = THRESHOLD_SCAN_MIN_DEFAULT,
  maxThreshold = THRESHOLD_SCAN_MAX_DEFAULT,
  weekKey = null,
  auditContext = null,
} = {}) {
  const requestedIsoDate = requestedIsoDateInput || targetIsoDate;
  const navigationIsoDate = navigationIsoDateInput || requestedIsoDate;
  if (!requestedIsoDate) {
    return {
      requestedIsoDate: null,
      computedWeekStart: null,
      navigationIsoDate: null,
      statusReason: 'calendar_headers_not_ready',
      earlyExitStage: 'before_navigation',
      earlyExitReason: 'missing_requested_iso_date',
      error: 'missing_requested_iso_date',
      crashed: false,
      durationMs: 0,
    };
  }

  const minT = Math.max(1, Number(minThreshold) || THRESHOLD_SCAN_MIN_DEFAULT);
  const maxT = Math.max(minT, Math.min(Number(maxThreshold) || THRESHOLD_SCAN_MAX_DEFAULT, THRESHOLD_SCAN_MAX_THRESHOLDS_PER_PAGE));
  const thresholdBatches = buildThresholdBatches(minT, maxT, THRESHOLD_SCAN_THRESHOLD_BATCH_SIZE);
  const thresholdList = Array.isArray(thresholds) && thresholds.length
    ? thresholds.map(t => Number(t)).filter(t => Number.isFinite(t) && t >= minT && t <= maxT).sort((a, b) => a - b)
    : thresholdBatches.flatMap(b => b.thresholds);

  const computedWeekStart = getMondayWeekStartIso(requestedIsoDate);
  const report = {
    requestedIsoDate,
    computedWeekStart,
    navigationIsoDate,
    targetIsoDate: requestedIsoDate,
    weekKey: weekKey || computedWeekStart,
    minThreshold: minT,
    maxThreshold: maxT,
    thresholds: thresholdList,
    thresholdsScanned: thresholdList,
    thresholdBatches: thresholdBatches.length,
    navigation: null,
    navigationDiagnostics: null,
    headerDiagnostics: null,
    thresholdDiagnostics: null,
    visibleWeekStart: null,
    visibleWeekEnd: null,
    visibleIsoDatesFromHeaders: [],
    rawMonthLabel: null,
    rawDayHeaderTexts: [],
    parsedDayHeaders: [],
    dayHeaderCandidateTexts: [],
    dayHeaderCandidateCount: 0,
    dayHeaderParseSource: null,
    bodyWeekdayTextSample: [],
    bodyTextLinesSample: [],
    weekdayLineMatches: [],
    combinedDayHeaderMatches: [],
    headerParseStrategy: null,
    headerScrapeAttempts: [],
    currentUrl: null,
    pageTitle: null,
    bodyTextLength: null,
    bodyTextSample: null,
    calendarReadySignals: null,
    headerParseError: null,
    targetDateVisibleFromHeaders: false,
    targetDateVisible: false,
    visibleTileCountAtThreshold1: 0,
    emptyWeekButVisible: false,
    weekMarkedComplete: false,
    thresholdScanStarted: false,
    filterResults: [],
    filterDiagnosticsByThreshold: [],
    selectedEntriesLeftLabelByThreshold: {},
    visibleTileCountsByThreshold: {},
    tileIdentitySamplesByThreshold: {},
    thresholdStopReason: null,
    thresholdScanMaxReached: null,
    thresholdWriteSafe: null,
    thresholdWriteBlockReason: null,
    thresholdPresenceBySessionSample: {},
    filterNormalization: null,
    visibleByThreshold: {},
    thresholdPresenceBySession: {},
    survivalMap: {},
    inferred: [],
    ambiguousSamples: [],
    exactCount: 0,
    atLeastCount: 0,
    ambiguousCount: 0,
    noMatchCount: 0,
    sessionsMatched: 0,
    sessionsWritten: 0,
    statusReason: null,
    earlyExitStage: null,
    earlyExitReason: null,
    errors: [],
    durationMs: 0,
    method: 'entries_left_threshold_scan',
    batchProgress: [],
    crashed: false,
    error: null,
    auditTrail: auditContext?.auditTrail || [],
    pageAvailable: page != null,
    pageUnavailableReason: page ? null : 'page_object_missing',
  };

  const started = Date.now();
  try {
    pushThresholdAuditStep(auditContext, 'navigate_calendar_start', {
      ok: true,
      requestedIsoDate,
      computedWeekStart,
      navigationIsoDate,
      validateIsoDate: requestedIsoDate,
    });

    await waitForThresholdCalendarShell(page).catch(() => {});
    const bookingCalendarDiag = await collectPageDiagnostics(page, 'booking_calendar_wait');
    pushThresholdAuditStep(auditContext, 'booking_calendar_wait', { ok: true, ...bookingCalendarDiag });
    applyPageDiagnosticsTo(report, bookingCalendarDiag);

    const nav = await withPlaywrightGuard(
      () => navigateCalendarToShowDate(page, navigationIsoDate, {
        headerOnly: true,
        validateIsoDate: requestedIsoDate,
        waitForShell: true,
        auditContext,
      }),
      {
        stage: 'threshold_week_navigation',
        timeout: THRESHOLD_SCAN_PAGE_TIMEOUT_MS,
        weekKey: weekKey || computedWeekStart,
        page,
        auditContext,
      },
    );
    report.navigation = nav;
    report.navigationDiagnostics = buildThresholdNavigationDiagnostics(nav);
    report.visibleIsoDatesFromHeaders = nav.visibleIsoDatesFromHeaders || nav.visibleDateLabels || [];
    report.rawMonthLabel = nav.rawMonthLabel ?? null;
    report.rawDayHeaderTexts = nav.rawDayHeaderTexts || [];
    report.parsedDayHeaders = nav.parsedDayHeaders || [];
    report.dayHeaderCandidateTexts = nav.dayHeaderCandidateTexts || [];
    report.dayHeaderCandidateCount = nav.dayHeaderCandidateCount ?? 0;
    report.dayHeaderParseSource = nav.dayHeaderParseSource ?? null;
    report.bodyWeekdayTextSample = nav.bodyWeekdayTextSample || [];
    report.bodyTextLinesSample = nav.bodyTextLinesSample || [];
    report.weekdayLineMatches = nav.weekdayLineMatches || [];
    report.combinedDayHeaderMatches = nav.combinedDayHeaderMatches || [];
    report.headerParseStrategy = nav.headerParseStrategy ?? null;
    report.headerScrapeAttempts = nav.headerScrapeAttempts || [];
    report.currentUrl = nav.currentUrl ?? null;
    report.pageTitle = nav.pageTitle ?? null;
    report.bodyTextLength = nav.bodyTextLength ?? null;
    report.bodyTextSample = nav.bodyTextSample ?? null;
    report.calendarReadySignals = nav.calendarReadySignals ?? null;
    report.headerParseError = nav.headerParseError ?? null;
    report.headerDiagnostics = buildThresholdHeaderDiagnostics(report);
    report.visibleWeekStart = nav.visibleWeekStart;
    report.visibleWeekEnd = nav.visibleWeekEnd;
    report.targetDateVisibleFromHeaders = nav.targetDateVisibleFromHeaders === true;
    report.targetDateVisible = report.targetDateVisibleFromHeaders;

    if (nav.statusReason === 'calendar_day_headers_not_parsed') {
      report.statusReason = 'calendar_day_headers_not_parsed';
      report.earlyExitStage = 'after_header_parse';
      report.earlyExitReason = nav.navigationError || 'month_visible_but_day_headers_empty';
      report.error = 'calendar_day_headers_not_parsed';
      report.crashed = false;
      report.durationMs = Date.now() - started;
      return report;
    }

    if (nav.statusReason === 'calendar_headers_not_ready' && nav.navigationError === 'no_day_headers_parsed') {
      report.statusReason = 'calendar_headers_not_ready';
      report.earlyExitStage = 'after_header_parse';
      report.earlyExitReason = nav.navigationError;
      report.error = 'calendar_headers_not_ready';
      report.crashed = false;
      report.durationMs = Date.now() - started;
      return report;
    }

    if (report.headerParseError) {
      report.statusReason = 'calendar_headers_not_ready';
      report.earlyExitStage = 'after_navigation_before_header_parse';
      report.earlyExitReason = report.headerParseError;
      report.error = 'calendar_headers_not_ready';
      report.crashed = false;
      report.durationMs = Date.now() - started;
      return report;
    }

    const headersReady = (report.rawDayHeaderTexts?.length || 0) > 0
      || (report.visibleIsoDatesFromHeaders?.length || 0) > 0;
    if (!headersReady) {
      if (shouldAbortNavigationForEmptyDayHeaders(report, {
        requestedIsoDate,
        navigationIsoDate,
      }) && report.calendarReadySignals?.hasWeekdayText) {
        report.statusReason = 'calendar_day_headers_not_parsed';
        report.earlyExitStage = 'after_header_parse';
        report.earlyExitReason = 'month_visible_but_day_headers_empty';
        report.error = 'calendar_day_headers_not_parsed';
      } else {
        report.statusReason = 'calendar_headers_not_ready';
        report.earlyExitStage = 'after_header_parse';
        report.earlyExitReason = 'no_day_headers_parsed';
        report.error = 'calendar_headers_not_ready';
      }
      report.crashed = false;
      report.durationMs = Date.now() - started;
      return report;
    }

    if (!report.targetDateVisibleFromHeaders) {
      report.statusReason = 'date_not_visible_after_navigation';
      report.earlyExitStage = 'after_header_parse';
      report.earlyExitReason = nav.navigationError || 'requested_date_not_in_visible_headers';
      report.error = report.earlyExitReason;
      report.errors.push({
        error: nav.navigationError || 'target_date_not_visible_in_headers',
        failureReason: 'failed_navigation',
      });
      report.durationMs = Date.now() - started;
      return report;
    }

    report.thresholdScanStarted = true;
    report.filterNormalization = await withPlaywrightGuard(
      () => normalizeBookingFiltersOnPage(page),
      { stage: 'threshold_filter_normalize', timeout: THRESHOLD_FILTER_TIMEOUT_MS, weekKey: report.weekKey },
    );
    await page.waitForTimeout(500);

    const visibleByThreshold = new Map();
    const tileByIdentity = new Map();
    const filterResults = [];
    const filterDiagnosticsByThreshold = [];
    const selectedEntriesLeftLabelByThreshold = {};
    const visibleTileCountsByThreshold = {};
    const tileIdentitySamplesByThreshold = {};
    const thresholdsActuallyScanned = [];
    let thresholdStopReason = null;
    let thresholdScanMaxReached = null;

    for (const batch of thresholdBatches) {
      collectorState.thresholdScanBatchProgress = {
        weekKey: report.weekKey,
        batchIndex: batch.batchIndex + 1,
        totalBatches: thresholdBatches.length,
        thresholdRange: `${batch.minThreshold}-${batch.maxThreshold}`,
        targetIsoDate: requestedIsoDate,
      };

      for (const threshold of batch.thresholds) {
        if (thresholdStopReason) break;

        const filterDiag = await withPlaywrightGuard(
          () => applyEntriesLeftFilterWithVerification(page, threshold),
          { stage: 'threshold_filter_set', timeout: THRESHOLD_FILTER_TIMEOUT_MS, weekKey: report.weekKey, page, auditContext },
        );
        filterDiagnosticsByThreshold.push(filterDiag);
        filterResults.push({ threshold, batchIndex: batch.batchIndex, ...filterDiag });
        selectedEntriesLeftLabelByThreshold[threshold] = filterDiag.afterEntriesLeftLabel ?? null;

        if (!filterDiag.filterSetOk) {
          thresholdScanMaxReached = threshold > minT ? threshold - 1 : null;
          thresholdStopReason = 'entries_left_option_unavailable_or_not_selected';
          report.errors.push({
            threshold,
            error: filterDiag.filterSetError || filterDiag.reason || 'filter_set_failed',
          });
          break;
        }

        const scrape = await withPlaywrightGuard(
          () => scrapeThresholdSessionTilesFromPage(page, { weekOffset: 0 }),
          { stage: 'threshold_tile_scrape', timeout: THRESHOLD_TILE_SCRAPE_TIMEOUT_MS, weekKey: report.weekKey },
        );
        const identityKeys = new Set();
        const samples = [];
        for (const tile of scrape.sessions || []) {
          identityKeys.add(tile.identityKey);
          if (!tileByIdentity.has(tile.identityKey)) tileByIdentity.set(tile.identityKey, tile);
          if (samples.length < 5) {
            samples.push({
              identityKey: tile.identityKey,
              isoDate: tile.isoDate,
              time: tile.time,
              level: tile.level,
              waveSide: tile.waveSide,
            });
          }
        }
        visibleByThreshold.set(threshold, identityKeys);
        report.visibleByThreshold[threshold] = identityKeys.size;
        visibleTileCountsByThreshold[threshold] = identityKeys.size;
        tileIdentitySamplesByThreshold[threshold] = samples;
        thresholdsActuallyScanned.push(threshold);
        if (threshold === 1) report.visibleTileCountAtThreshold1 = identityKeys.size;
      }

      report.batchProgress.push({
        ...collectorState.thresholdScanBatchProgress,
        thresholdsTested: batch.thresholds.length,
        completedAt: new Date().toISOString(),
      });
      if (thresholdStopReason) break;
    }

    report.filterResults = filterResults;
    report.filterDiagnosticsByThreshold = filterDiagnosticsByThreshold;
    report.selectedEntriesLeftLabelByThreshold = selectedEntriesLeftLabelByThreshold;
    report.visibleTileCountsByThreshold = visibleTileCountsByThreshold;
    report.tileIdentitySamplesByThreshold = tileIdentitySamplesByThreshold;
    report.thresholdStopReason = thresholdStopReason;
    report.thresholdScanMaxReached = thresholdScanMaxReached
      ?? (thresholdsActuallyScanned.length ? Math.max(...thresholdsActuallyScanned) : null);
    report.thresholdsScanned = thresholdsActuallyScanned.length
      ? thresholdsActuallyScanned
      : thresholdList;

    const effectiveMaxTested = report.thresholdScanMaxReached || maxT;

    if (report.visibleTileCountAtThreshold1 === 0 && !thresholdStopReason) {
      report.emptyWeekButVisible = true;
      report.weekMarkedComplete = true;
      report.statusReason = 'visible_week_no_threshold_tiles';
      report.exactCount = 0;
      report.atLeastCount = 0;
      report.ambiguousCount = 0;
      report.noMatchCount = 0;
      report.sessionsMatched = 0;
      collectorState.lastThresholdScanWeek = report.weekKey;
      report.durationMs = Date.now() - started;
      return report;
    }

    const presenceByIdentity = buildThresholdPresenceMap(visibleByThreshold, effectiveMaxTested);
    report.survivalMap = Object.fromEntries(
      [...presenceByIdentity.entries()].map(([k, v]) => [k, Math.max(...v, 0)]),
    );

    const weekDates = report.visibleIsoDatesFromHeaders.length
      ? report.visibleIsoDatesFromHeaders
      : await withPlaywrightGuard(
        () => getVisibleWeekDatesFromHeaders(page),
        { stage: 'threshold_week_dates', timeout: THRESHOLD_TILE_SCRAPE_TIMEOUT_MS, weekKey: report.weekKey },
      );
    const basicSessions = weekDates.flatMap(d => sessionsForDate(d));
    const uniqueBasic = [...new Map(basicSessions.map(s => [s.key, s])).values()];

    const { results, ambiguousSamples, thresholdPresenceBySession } = matchThresholdResultsToSessions({
      presenceByIdentity,
      tileByIdentity,
      basicSessions: uniqueBasic,
      maxTested: effectiveMaxTested,
    });

    report.thresholdPresenceBySession = thresholdPresenceBySession;
    report.thresholdPresenceBySessionSample = sampleThresholdPresenceBySession(thresholdPresenceBySession);

    const writeSafety = evaluateThresholdWriteSafety(report);
    report.thresholdWriteSafe = writeSafety.thresholdWriteSafe;
    report.thresholdWriteBlockReason = writeSafety.thresholdWriteBlockReason;
    if (writeSafety.statusReason) {
      report.statusReason = writeSafety.statusReason;
      report.error = writeSafety.statusReason;
      report.earlyExitStage = 'threshold_parse';
      report.earlyExitReason = writeSafety.thresholdWriteBlockReason;
      report.exactCount = 0;
      report.atLeastCount = 0;
      report.ambiguousCount = results.filter(r => r.inference?.thresholdConfidence === 'ambiguous').length;
      report.noMatchCount = results.filter(r => r.inference?.thresholdConfidence === 'no_match' && r.session).length;
      report.sessionsMatched = 0;
      report.inferred = results.map(r => ({
        key: r.session?.key || null,
        identityKey: r.identityKey,
        isoDate: r.session ? sessionDateKey(r.session) : r.tile?.isoDate,
        time: r.session?.time || r.tile?.time,
        thresholdsSeen: r.thresholdsSeen || [],
        thresholdConfidence: r.inference?.thresholdConfidence,
        thresholdScanVerified: false,
      }));
      report.thresholdDiagnostics = {
        visibleByThreshold: report.visibleByThreshold,
        filterResults: report.filterResults,
        filterDiagnosticsByThreshold: report.filterDiagnosticsByThreshold,
        filterNormalization: report.filterNormalization,
        visibleTileCountAtThreshold1: report.visibleTileCountAtThreshold1,
        thresholdPresenceBySession: report.thresholdPresenceBySession,
        inferredCount: report.inferred.length,
        thresholdWriteSafe: false,
        thresholdWriteBlockReason: writeSafety.thresholdWriteBlockReason,
      };
      report.weekMarkedComplete = false;
      report.durationMs = Date.now() - started;
      return report;
    }

    report.inferred = results.map(r => ({
      key: r.session?.key || null,
      identityKey: r.identityKey,
      isoDate: r.session ? sessionDateKey(r.session) : r.tile?.isoDate,
      time: r.session?.time || r.tile?.time,
      timeLabel: r.tile?.timeLabel || r.session?.time,
      level: r.session?.level || r.tile?.level,
      sessionCode: r.tile?.sessionCode || levelToSessionCode(r.session?.level),
      waveSide: r.session?.waveSide || r.tile?.waveSide,
      thresholdsSeen: r.thresholdsSeen || [],
      maxVisible: r.maxVisible,
      thresholdInferredSlots: r.inference?.thresholdInferredSlots,
      thresholdConfidence: r.inference?.thresholdConfidence,
      thresholdScanVerified: r.inference?.thresholdScanVerified,
      matchConfidence: r.matchConfidence,
      matchMethod: r.matchMethod || null,
      ambiguousSample: r.ambiguousSample || null,
    }));
    report.ambiguousSamples = ambiguousSamples;
    report.exactCount = results.filter(r => r.inference?.thresholdConfidence === 'exact' && r.session).length;
    report.atLeastCount = results.filter(r => r.inference?.thresholdConfidence === 'at_least' && r.session).length;
    report.ambiguousCount = results.filter(r => r.inference?.thresholdConfidence === 'ambiguous').length;
    report.noMatchCount = results.filter(r => r.inference?.thresholdConfidence === 'no_match' && r.session).length;
    report.sessionsMatched = results.filter(r => r.session && r.inference?.thresholdScanVerified).length;
    report.weekMarkedComplete = true;
    report.thresholdWriteSafe = writeSafety.thresholdWriteSafe;
    report.thresholdDiagnostics = {
      visibleByThreshold: report.visibleByThreshold,
      filterResults: report.filterResults,
      filterDiagnosticsByThreshold: report.filterDiagnosticsByThreshold,
      filterNormalization: report.filterNormalization,
      visibleTileCountAtThreshold1: report.visibleTileCountAtThreshold1,
      thresholdPresenceBySession: report.thresholdPresenceBySession,
      inferredCount: report.inferred.length,
      thresholdWriteSafe: report.thresholdWriteSafe,
      thresholdWriteBlockReason: report.thresholdWriteBlockReason,
    };
    report.statusReason = classifyThresholdScanStatus(report);
    collectorState.lastThresholdScanWeek = report.weekKey;
    pushThresholdAuditStep(auditContext, 'timeout_or_success', {
      ok: true,
      reason: report.statusReason,
      stage: 'threshold_scan_complete',
      currentUrl: report.currentUrl,
      pageTitle: report.pageTitle,
      bodyTextLength: report.bodyTextLength,
      bodyTextSample: report.bodyTextSample,
      calendarReadySignals: report.calendarReadySignals,
    });
  } catch (e) {
    const failureReason = isPlaywrightCrashError(e)
      ? recordPageCrash('threshold_week_scan', e, { weekKey: weekKey || computedWeekStart })
      : 'failed_threshold_scan';
    const failureDiagnostics = await enrichFailureWithPageDiagnostics(
      page,
      auditContext,
      report.thresholdScanStarted ? 'threshold_parse' : 'after_navigation_before_header_parse',
      e,
    );
    applyPageDiagnosticsTo(report, failureDiagnostics);
    report.crashed = isPlaywrightCrashError(e);
    report.error = e.message;
    report.errors.push({ error: e.message, failureReason });
    if (!isPlaywrightCrashError(e)) {
      collectorState.thresholdScanLastError = e.message;
      report.earlyExitStage = report.thresholdScanStarted ? 'threshold_parse' : 'after_navigation_before_header_parse';
      report.earlyExitReason = e.message;
      report.statusReason = report.statusReason || 'calendar_headers_not_ready';
    }
  }

  report.auditTrail = auditContext?.auditTrail || report.auditTrail || [];
  report.pageAvailable = page != null;
  if (!page) report.pageUnavailableReason = report.pageUnavailableReason || 'page_object_missing';

  report.durationMs = Date.now() - started;
  return report;
}

async function applyThresholdScanReport(report, { dryRun = true, sourceTier = 2 } = {}) {
  const writeReport = {
    dryRun,
    sessionsEligible: 0,
    sessionsWritten: 0,
    rowsUpserted: 0,
    snapshotsInserted: 0,
    skippedAmbiguous: 0,
    skippedNoMatch: 0,
    skippedNoSession: 0,
    skippedWriteUnsafe: 0,
    errors: [],
  };

  if (report.thresholdWriteSafe === false || report.statusReason === 'threshold_filter_not_effective') {
    writeReport.skippedWriteUnsafe = (report.inferred || []).length;
    return writeReport;
  }

  const toWrite = [];
  for (const item of report.inferred || []) {
    if (!item.key) {
      writeReport.skippedNoSession++;
      continue;
    }
    const existing = sessionsByKey.get(item.key);
    if (!existing) {
      writeReport.skippedNoSession++;
      continue;
    }

    if (item.thresholdConfidence === 'ambiguous') {
      const entry = { ...existing };
      applyThresholdFieldsToSession(entry, {
        thresholdInferredSlots: null,
        thresholdMaxVisible: item.maxVisible,
        thresholdConfidence: 'ambiguous',
        thresholdScanVerified: false,
        reason: item.ambiguousSample ? 'ambiguous_match' : 'ambiguous',
      }, {
        maxTested: report.maxThreshold,
        diagnostics: item.ambiguousSample || item.matchConfidence,
        overwriteModalSlots: false,
      });
      if (!dryRun) toWrite.push(entry);
      writeReport.skippedAmbiguous++;
      continue;
    }

    if (item.thresholdConfidence === 'no_match') {
      const entry = { ...existing };
      applyThresholdFieldsToSession(entry, {
        thresholdInferredSlots: null,
        thresholdMaxVisible: item.maxVisible || 0,
        thresholdConfidence: 'no_match',
        thresholdScanVerified: false,
        reason: 'threshold_no_match',
      }, {
        maxTested: report.maxThreshold,
        diagnostics: 'not_visible_at_threshold_1',
        overwriteModalSlots: false,
      });
      if (!dryRun) toWrite.push(entry);
      writeReport.skippedNoMatch++;
      continue;
    }

    if (!item.thresholdScanVerified) continue;
    writeReport.sessionsEligible++;

    const entry = { ...existing };
    applyThresholdFieldsToSession(entry, {
      thresholdInferredSlots: item.thresholdInferredSlots,
      thresholdMaxVisible: item.maxVisible,
      thresholdConfidence: item.thresholdConfidence,
      thresholdScanVerified: true,
      reason: item.matchConfidence,
    }, {
      maxTested: report.maxThreshold,
      diagnostics: { matchConfidence: item.matchConfidence, matchMethod: item.matchMethod },
      overwriteModalSlots: !sessionDetailVerified(entry),
    });

    if (!dryRun) toWrite.push(entry);
  }

  if (!dryRun && toWrite.length) {
    mergeBatchIntoStore(toWrite, sourceTier, { preserveSlots: true, scrapeKind: 'basic' });
    const upsert = await upsertCurrentSessionsToSupabase(toWrite, sourceTier, { scrapeKind: 'basic' });
    writeReport.rowsUpserted = upsert.rowsUpserted || 0;
    if (upsert.error) writeReport.errors.push({ phase: 'upsert', error: upsert.error });

    const snapSessions = toWrite
      .filter(s => thresholdSlotsTrusted(s))
      .map(s => ({
        ...s,
        slots: s.thresholdInferredSlots,
      }));
    if (snapSessions.length) {
      const snap = await saveAvailabilitySnapshotsToSupabase(snapSessions, sourceTier, {
        snapshotType: 'entries_left_threshold',
      });
      writeReport.snapshotsInserted = snap.snapshotsInserted || 0;
      if (snap.error) writeReport.errors.push({ phase: 'snapshots', error: snap.error });
    }
    writeReport.sessionsWritten = toWrite.length;
  } else {
    writeReport.sessionsWritten = 0;
  }

  return writeReport;
}

async function runThresholdScanForWeek(page, options = {}) {
  const report = await scanEntriesLeftThresholdsForWeek(page, options);
  const write = await applyThresholdScanReport(report, {
    dryRun: options.dryRun !== false,
    sourceTier: options.sourceTier || 2,
  });
  const combined = {
    ...report,
    write,
    completedAt: new Date().toISOString(),
  };
  collectorState.lastThresholdScanResult = combined;
  if (!report.errors.some(e => e.failureReason === 'failed_page_crash')) {
    markThresholdScanRecovered();
  }
  return combined;
}

async function runThresholdScansChunked(dates, options = {}) {
  const {
    dryRun = true,
    minThreshold = THRESHOLD_SCAN_MIN_DEFAULT,
    maxThreshold = THRESHOLD_SCAN_MAX_DEFAULT,
    sourceTier = 2,
    maxWeeksPerRun = THRESHOLD_SCAN_MAX_WEEKS_PER_RUN,
    recycleBrowserEachWeek = THRESHOLD_SCAN_RECYCLE_BROWSER_EACH_WEEK,
    resumeQueue = null,
    requestedIsoDate: routeRequestedIsoDate = null,
    navigationIsoDate: routeNavigationIsoDate = null,
    computedWeekStart: routeComputedWeekStart = null,
    weekMode = true,
    debug = false,
    trace = false,
    screenshot = false,
    headed = false,
    auditContext: externalAuditContext = null,
  } = options;

  const auditContext = externalAuditContext || createThresholdAuditContext({
    debug,
    trace,
    screenshot,
    headed,
  });

  const pendingWeeks = buildThresholdScanResumeQueue(dates, { resumeQueue });
  collectorState.thresholdScanWeeksRemaining = pendingWeeks.map(w => w.weekKey);
  const weeksToProcess = pendingWeeks.slice(0, maxWeeksPerRun);
  const remainingAfter = pendingWeeks.slice(maxWeeksPerRun);

  const dateResults = [];
  const errors = [];
  let launched = null;
  let crashed = false;

  try {
    for (const week of weeksToProcess) {
      collectorState.lastThresholdScanWeek = week.weekKey;
      collectorState.thresholdScanBatchProgress = {
        weekKey: week.weekKey,
        batchIndex: 0,
        totalBatches: buildThresholdBatches(minThreshold, maxThreshold).length,
        targetIsoDate: week.anchorIsoDate,
      };

      try {
        if (recycleBrowserEachWeek || !launched) {
          await safeCloseBrowser(launched);
          const browserLaunchStarted = Date.now();
          launched = await launchBrowser({ headed: auditContext.debug.headed });
          await startThresholdPlaywrightTrace(auditContext, launched.context);
          pushThresholdAuditStep(auditContext, 'browser_launch', {
            ok: true,
            headed: auditContext.debug.headed,
            elapsedMs: Date.now() - browserLaunchStarted,
          });
          await openBookingPageForThreshold(launched.page, { auditContext });
          const pageOpenedDiag = await collectPageDiagnostics(launched.page, 'page_opened');
          pushThresholdAuditStep(auditContext, 'page_opened', { ok: true, ...pageOpenedDiag });
          await captureThresholdDebugArtifacts(launched.page, auditContext, 'page_opened', { screenshot: true });
        }

        const scanRequestedIsoDate = routeRequestedIsoDate
          || week.requestedIsoDate
          || week.weekDates?.[0]
          || week.anchorIsoDate;
        const scanComputedWeekStart = routeComputedWeekStart
          || week.computedWeekStart
          || getMondayWeekStartIso(scanRequestedIsoDate);
        const scanNavigationIsoDate = routeNavigationIsoDate
          || (weekMode ? scanComputedWeekStart : scanRequestedIsoDate);

        const result = await runThresholdScanForWeek(launched.page, {
          requestedIsoDate: scanRequestedIsoDate,
          navigationIsoDate: scanNavigationIsoDate,
          weekKey: week.weekKey || week.computedWeekStart,
          minThreshold,
          maxThreshold,
          dryRun,
          sourceTier,
          auditContext,
        });

        const isEmptyVisibleWeek = result.statusReason === 'visible_week_no_threshold_tiles';
        const weekFailed = !isEmptyVisibleWeek && result.errors?.some(e =>
          e.failureReason === 'failed_page_crash',
        );

        dateResults.push({
          isoDate: scanRequestedIsoDate,
          weekKey: result.weekKey || week.weekKey,
          computedWeekStart: result.computedWeekStart || scanComputedWeekStart,
          requestedIsoDate: result.requestedIsoDate || scanRequestedIsoDate,
          navigationIsoDate: result.navigationIsoDate || scanNavigationIsoDate,
          navigationDiagnostics: result.navigationDiagnostics,
          headerDiagnostics: result.headerDiagnostics,
          thresholdDiagnostics: result.thresholdDiagnostics,
          currentUrl: result.currentUrl,
          pageTitle: result.pageTitle,
          bodyTextLength: result.bodyTextLength,
          bodyTextSample: result.bodyTextSample,
          calendarReadySignals: result.calendarReadySignals,
          headerScrapeAttempts: result.headerScrapeAttempts,
          visibleIsoDatesFromHeaders: result.visibleIsoDatesFromHeaders,
          rawMonthLabel: result.rawMonthLabel,
          rawDayHeaderTexts: result.rawDayHeaderTexts,
          dayHeaderCandidateTexts: result.dayHeaderCandidateTexts,
          dayHeaderCandidateCount: result.dayHeaderCandidateCount,
          dayHeaderParseSource: result.dayHeaderParseSource,
          bodyWeekdayTextSample: result.bodyWeekdayTextSample,
          bodyTextLinesSample: result.bodyTextLinesSample,
          weekdayLineMatches: result.weekdayLineMatches,
          combinedDayHeaderMatches: result.combinedDayHeaderMatches,
          parsedDayHeaders: result.parsedDayHeaders,
          headerParseStrategy: result.headerParseStrategy,
          targetDateVisibleFromHeaders: result.targetDateVisibleFromHeaders,
          visibleTileCountAtThreshold1: result.visibleTileCountAtThreshold1,
          emptyWeekButVisible: result.emptyWeekButVisible,
          weekMarkedComplete: result.weekMarkedComplete,
          earlyExitStage: result.earlyExitStage,
          earlyExitReason: result.earlyExitReason,
          statusReason: result.statusReason,
          auditTrail: result.auditTrail || auditContext.auditTrail,
          pageAvailable: result.pageAvailable,
          pageUnavailableReason: result.pageUnavailableReason,
          headerParseError: result.headerParseError ?? null,
          thresholdWriteSafe: result.thresholdWriteSafe,
          thresholdWriteBlockReason: result.thresholdWriteBlockReason,
          thresholdStopReason: result.thresholdStopReason,
          thresholdScanMaxReached: result.thresholdScanMaxReached,
          filterDiagnosticsByThreshold: result.filterDiagnosticsByThreshold,
          selectedEntriesLeftLabelByThreshold: result.selectedEntriesLeftLabelByThreshold,
          visibleTileCountsByThreshold: result.visibleTileCountsByThreshold,
          tileIdentitySamplesByThreshold: result.tileIdentitySamplesByThreshold,
          thresholdPresenceBySessionSample: result.thresholdPresenceBySessionSample,
          exactCount: result.exactCount,
          atLeastCount: result.atLeastCount,
          ambiguousCount: result.ambiguousCount,
          noMatchCount: result.noMatchCount,
          thresholdPresenceBySession: result.thresholdPresenceBySession,
          targetDateVisible: result.targetDateVisible,
          sessionsMatched: result.sessionsMatched,
          write: result.write,
          errors: result.errors,
          crashed: weekFailed,
          error: weekFailed ? (result.errors.find(e => e.failureReason)?.failureReason || result.error) : result.error,
          failureReason: weekFailed ? (result.errors.find(e => e.failureReason)?.failureReason || 'failed_page_crash') : null,
        });

        if (weekFailed) {
          crashed = true;
          if (result.errors?.length) errors.push(...result.errors);
          remainingAfter.unshift(week, ...weeksToProcess.slice(weeksToProcess.indexOf(week) + 1));
          break;
        }

        if (!collectorState.thresholdScanCompletedWeeks.includes(result.computedWeekStart || result.weekKey || week.weekKey)) {
          collectorState.thresholdScanCompletedWeeks.push(result.computedWeekStart || result.weekKey || week.weekKey);
        }
        markThresholdScanRecovered();
      } catch (e) {
        const failureReason = isPlaywrightCrashError(e)
          ? recordPageCrash('threshold_week_chunk', e, { weekKey: week.weekKey })
          : 'failed_threshold_scan';
        const failureDiagnostics = await enrichFailureWithPageDiagnostics(
          launched?.page,
          auditContext,
          'threshold_week_chunk',
          e,
        );
        crashed = true;
        dateResults.push({
          isoDate: week.anchorIsoDate,
          weekKey: week.weekKey,
          requestedIsoDate: routeRequestedIsoDate || week.anchorIsoDate,
          computedWeekStart: routeComputedWeekStart || week.computedWeekStart || null,
          navigationIsoDate: routeNavigationIsoDate || null,
          crashed: true,
          failureReason,
          statusReason: isPlaywrightCrashError(e) ? null : 'calendar_headers_not_ready',
          earlyExitStage: 'threshold_week_chunk',
          earlyExitReason: e.message,
          error: e.message,
          auditTrail: auditContext.auditTrail,
          ...failureDiagnostics,
          errors: [{ error: e.message, failureReason }],
        });
        errors.push({ weekKey: week.weekKey, error: e.message, failureReason });
        remainingAfter.unshift(week, ...weeksToProcess.slice(weeksToProcess.indexOf(week) + 1));
        await safeCloseBrowser(launched);
        launched = null;
        break;
      } finally {
        if (recycleBrowserEachWeek) {
          await safeCloseBrowser(launched);
          launched = null;
        }
      }
    }
  } finally {
    await stopThresholdPlaywrightTrace(auditContext, 'threshold_scan');
    await safeCloseBrowser(launched);
    collectorState.thresholdScanPendingWeeks = remainingAfter.map(w => w.weekKey);
    collectorState.thresholdScanWeeksRemaining = remainingAfter.map(w => w.weekKey);
    if (!remainingAfter.length) {
      collectorState.thresholdScanBatchProgress = null;
    }
  }

  const summary = {
    weeksScanned: dateResults.filter(r => !r.skipped && !r.crashed).length,
    weeksRemaining: remainingAfter.length,
    weeksRequested: pendingWeeks.length,
    maxWeeksPerRun,
    resumable: remainingAfter.length > 0,
    crashed,
    dateResults,
    errors,
    pendingWeeks: remainingAfter,
    completedAt: new Date().toISOString(),
    auditTrail: auditContext.auditTrail,
    tracePath: auditContext.tracePath || null,
  };
  collectorState.lastThresholdScanResult = summary;
  return summary;
}

async function runThresholdScansForDates(page, dates, options = {}) {
  if (page) {
    const week = buildWeekAnchorsFromDates(dates)[0];
    if (!week) {
      return {
        weeksScanned: 0,
        weeksRemaining: 0,
        dateResults: [],
        errors: [{ error: 'no_dates_for_threshold_scan' }],
        completedAt: new Date().toISOString(),
      };
    }
    const result = await runThresholdScanForWeek(page, {
      ...options,
      targetIsoDate: week.anchorIsoDate,
      weekKey: week.weekKey,
    });
    return {
      weeksScanned: 1,
      weeksRemaining: 0,
      dateResults: [{
        isoDate: week.anchorIsoDate,
        weekKey: result.weekKey || week.weekKey,
        targetDateVisible: result.targetDateVisible,
        sessionsMatched: result.sessionsMatched,
        write: result.write,
        errors: result.errors,
      }],
      errors: result.errors || [],
      completedAt: new Date().toISOString(),
    };
  }
  return runThresholdScansChunked(dates, options);
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
async function advanceCalendarWeek(page, auditContext = null) {
  const before = await getCalendarFingerprint(page);
  const chevron = page.locator('.glyphicon-chevron-right').first();
  if (!await chevron.count()) {
    console.log('  next-week arrow not found');
    pushThresholdAuditStep(auditContext, 'navigation_click', {
      ok: false,
      direction: 'next',
      selectorUsed: '.glyphicon-chevron-right',
      beforeHeader: before.label || null,
      reason: 'selector_not_found',
    });
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
    pushThresholdAuditStep(auditContext, 'navigation_click', {
      ok: false,
      direction: 'next',
      selectorUsed: '.glyphicon-chevron-right',
      beforeHeader: before.label || null,
      afterHeader: after.label || null,
      reason: 'calendar_unchanged',
    });
    return false;
  }

  console.log(`  calendar advanced → "${after.label || 'n/a'}", ${after.count} tiles`);
  pushThresholdAuditStep(auditContext, 'navigation_click', {
    ok: true,
    direction: 'next',
    selectorUsed: '.glyphicon-chevron-right',
    beforeHeader: before.label || null,
    afterHeader: after.label || null,
  });
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
    const fp = await withPlaywrightGuard(
      () => getCalendarFingerprint(page),
      { stage: 'calendar_fingerprint', timeout: BOOKING_PAGE_TIMEOUT_MS },
    );
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
  return resolveDiscoveredAvailableDates().dates;
}

function deriveDiscoveredDatesFromCurrentSessions(dbByDate = null) {
  const today = getParkTodayIso();
  const maxDate = maxHorizonDateKey();
  const fromDb = dbByDate && typeof dbByDate === 'object'
    ? Object.keys(dbByDate).filter(d => (dbByDate[d] || 0) > 0 && d >= today && d <= maxDate)
    : [];
  const fromMemory = [...new Set(
    allStoredSessions()
      .map(sessionDateKey)
      .filter(d => d && d >= today && d <= maxDate),
  )];
  return [...new Set([...fromDb, ...fromMemory])].sort();
}

function resolveDiscoveredAvailableDates({ dbByDate = null } = {}) {
  const memory = (collectorState.discoveredAvailableDates || []).filter(Boolean);
  if (memory.length) {
    return {
      dates: [...memory].sort(),
      source: 'memory',
      lastDiscoveryRunAt: collectorState.lastDiscoveryAt || null,
    };
  }

  const backfill = collectorState.lastBackfillAvailableDatesResult;
  const backfillDates = backfill?.discoveredAvailableDates;
  if (Array.isArray(backfillDates) && backfillDates.length) {
    return {
      dates: [...backfillDates].sort(),
      source: 'last_backfill_result',
      lastDiscoveryRunAt: backfill.completedAt || collectorState.lastDiscoveryAt || null,
    };
  }

  const fromSessions = deriveDiscoveredDatesFromCurrentSessions(dbByDate);
  if (fromSessions.length) {
    return {
      dates: fromSessions,
      source: dbByDate ? 'db' : 'current_sessions_fallback',
      lastDiscoveryRunAt: collectorState.lastDiscoveryAt || lastSuccessfulScrape || null,
    };
  }

  return {
    dates: [],
    source: 'none',
    lastDiscoveryRunAt: collectorState.lastDiscoveryAt || null,
  };
}

function expectedAvailableBookingDates() {
  return resolveDiscoveredAvailableDates().dates;
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
  const result = await withPlaywrightGuard(
    () => page.evaluate(scrapeVisibleSessions, { ...SCRAPE_OPTS, weekOffset }),
    { stage: 'absorb_visible_sessions', timeout: BOOKING_PAGE_TIMEOUT_MS },
  );
  const pageSessions = asSessionArray(result?.sessions);
  const visible = await withPlaywrightGuard(
    () => getVisibleDateKeysFromPage(page),
    { stage: 'absorb_visible_dates', timeout: BOOKING_PAGE_TIMEOUT_MS },
  );
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

async function retreatCalendarWeek(page, auditContext = null) {
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
  if (after.sig === before.sig && after.firstTs === before.firstTs && after.count > 0) {
    pushThresholdAuditStep(auditContext, 'navigation_click', {
      ok: false,
      direction: 'prev',
      selectorUsed: '.glyphicon-chevron-left',
      beforeHeader: before.label || null,
      afterHeader: after.label || null,
      reason: 'calendar_unchanged',
    });
    return false;
  }
  console.log(`  calendar retreated → "${after.label || 'n/a'}", ${after.count} tiles`);
  pushThresholdAuditStep(auditContext, 'navigation_click', {
    ok: true,
    direction: 'prev',
    selectorUsed: '.glyphicon-chevron-left',
    beforeHeader: before.label || null,
    afterHeader: after.label || null,
  });
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

async function navigateCalendarToShowDate(page, navigationIsoDate, {
  headerOnly = false,
  validateIsoDate = null,
  waitForShell = true,
  auditContext = null,
} = {}) {
  const validationTarget = validateIsoDate || navigationIsoDate;
  const diag = {
    targetIsoDate: navigationIsoDate || null,
    navigationIsoDate: navigationIsoDate || null,
    validateIsoDate: validationTarget || null,
    computedWeekStart: navigationIsoDate ? getMondayWeekStartIso(navigationIsoDate) : null,
    visibleWeekStart: null,
    visibleWeekEnd: null,
    visibleDateLabels: [],
    visibleIsoDatesFromHeaders: [],
    rawMonthLabel: null,
    rawDayHeaderTexts: [],
    parsedDayHeaders: [],
    dayHeaderCandidateTexts: [],
    dayHeaderCandidateCount: 0,
    dayHeaderParseSource: null,
    bodyWeekdayTextSample: [],
    bodyTextLinesSample: [],
    weekdayLineMatches: [],
    combinedDayHeaderMatches: [],
    headerParseStrategy: null,
    headerScrapeAttempts: [],
    headerParseError: null,
    statusReason: null,
    targetDateVisibleFromHeaders: false,
    clickedNextWeekCount: 0,
    targetDateVisible: false,
    navigationError: null,
    headerOnly,
    currentUrl: null,
    pageTitle: null,
    bodyTextLength: null,
    bodyTextSample: null,
    calendarReadySignals: null,
  };

  if (!navigationIsoDate) {
    diag.navigationError = 'missing_target_iso_date';
    return diag;
  }

  function weekContainsTarget(headers) {
    if (!headers?.length || !validationTarget) return false;
    return headers.includes(validationTarget);
  }

  function finalizeDayHeaderParseFailure(d) {
    const hasWeekdayText = d.calendarReadySignals?.hasWeekdayText === true;
    d.navigationError = hasWeekdayText ? 'month_visible_but_day_headers_empty' : 'no_day_headers_parsed';
    d.statusReason = hasWeekdayText ? 'calendar_day_headers_not_parsed' : 'calendar_headers_not_ready';
    d.targetDateVisible = false;
    d.targetDateVisibleFromHeaders = false;
    pushThresholdAuditStep(auditContext, 'timeout_or_success', {
      ok: false,
      reason: d.navigationError,
      stage: 'after_header_parse',
      rawMonthLabel: d.rawMonthLabel,
      rawDayHeaderTexts: d.rawDayHeaderTexts,
      bodyTextLinesSample: d.bodyTextLinesSample,
      weekdayLineMatches: d.weekdayLineMatches,
      combinedDayHeaderMatches: d.combinedDayHeaderMatches,
    });
    return d;
  }

  function resolveNavigationDirection(headers) {
    if (headers?.length) {
      return navigationDirectionForVisibleHeaders(headers, validationTarget);
    }
    return navigationDirectionForEmptyHeaders(diag, validationTarget);
  }

  async function readVisibleDates() {
    if (waitForShell) await waitForThresholdCalendarShell(page).catch(() => {});
    const scraped = await scrapeCalendarHeadersWithRetry(page, { auditContext });
    const headerParse = scraped.headerParse || {};
    diag.headerScrapeAttempts = scraped.headerScrapeAttempts || [];
    if (scraped.pageDiagnostics) {
      diag.currentUrl = scraped.pageDiagnostics.currentUrl ?? null;
      diag.pageTitle = scraped.pageDiagnostics.pageTitle ?? null;
      diag.bodyTextLength = scraped.pageDiagnostics.bodyTextLength ?? null;
      diag.bodyTextSample = scraped.pageDiagnostics.bodyTextSample ?? null;
      diag.calendarReadySignals = scraped.pageDiagnostics.calendarReadySignals ?? null;
    }
    let headers = headerParse?.visibleIsoDatesFromHeaders || headerParse?.dates || [];
    if (!headers.length && !headerOnly) {
      headers = await getVisibleDateKeysFromPage(page);
    }
    diag.visibleIsoDatesFromHeaders = headers;
    diag.visibleDateLabels = headers;
    diag.rawMonthLabel = headerParse?.rawMonthLabel ?? null;
    diag.rawDayHeaderTexts = headerParse?.rawDayHeaderTexts || [];
    diag.parsedDayHeaders = headerParse?.parsedDayHeaders || [];
    diag.dayHeaderCandidateTexts = headerParse?.dayHeaderCandidateTexts || [];
    diag.dayHeaderCandidateCount = headerParse?.dayHeaderCandidateCount ?? 0;
    diag.dayHeaderParseSource = headerParse?.dayHeaderParseSource ?? null;
    diag.bodyWeekdayTextSample = headerParse?.bodyWeekdayTextSample || [];
    diag.bodyTextLinesSample = headerParse?.bodyTextLinesSample || [];
    diag.weekdayLineMatches = headerParse?.weekdayLineMatches || [];
    diag.combinedDayHeaderMatches = headerParse?.combinedDayHeaderMatches || [];
    diag.headerParseStrategy = headerParse?.headerParseStrategy ?? null;
    diag.headerParseError = headerParse?.headerParseError ?? null;
    if (headerParse?.headerParseError) {
      diag.bodyTextLength = headerParse.bodyTextLength ?? diag.bodyTextLength;
      diag.bodyTextSample = headerParse.bodyTextSample ?? diag.bodyTextSample;
      diag.calendarReadySignals = headerParse.calendarReadySignals ?? diag.calendarReadySignals;
    }
    if (headers.length) {
      diag.visibleWeekStart = headers[0];
      diag.visibleWeekEnd = headers[headers.length - 1];
    }
    diag.targetDateVisibleFromHeaders = weekContainsTarget(headers);
    diag.targetDateVisible = diag.targetDateVisibleFromHeaders;
    return headers;
  }

  let visible = await readVisibleDates();
  if (weekContainsTarget(visible)) return diag;
  if (shouldAbortNavigationForEmptyDayHeaders(diag, {
    requestedIsoDate: validationTarget,
    navigationIsoDate,
  })) {
    return finalizeDayHeaderParseFailure(diag);
  }

  await openBookingPage(page);
  await dismissCookieBanner(page);
  await waitForThresholdCalendarShell(page).catch(() => {});
  const reopenDiag = await collectPageDiagnostics(page, 'navigation_reopen_booking');
  pushThresholdAuditStep(auditContext, 'page_opened', { ok: true, reason: 'navigation_reopen', ...reopenDiag });
  visible = await readVisibleDates();
  if (weekContainsTarget(visible)) return diag;
  if (shouldAbortNavigationForEmptyDayHeaders(diag, {
    requestedIsoDate: validationTarget,
    navigationIsoDate,
  })) {
    return finalizeDayHeaderParseFailure(diag);
  }

  const maxSteps = effectiveWeeksAhead + 3;
  for (let step = 0; step < maxSteps; step++) {
    if (shouldAbortNavigationForEmptyDayHeaders(diag, {
      requestedIsoDate: validationTarget,
      navigationIsoDate,
    })) {
      return finalizeDayHeaderParseFailure(diag);
    }

    const direction = resolveNavigationDirection(visible);
    if (direction === 'stop') {
      return finalizeDayHeaderParseFailure(diag);
    }
    if (!direction) break;

    const clicked = direction === 'next'
      ? await advanceCalendarWeek(page, auditContext)
      : await retreatCalendarWeek(page, auditContext);
    if (!clicked) break;
    if (direction === 'next') diag.clickedNextWeekCount++;
    visible = await readVisibleDates();
    if (weekContainsTarget(visible)) return diag;
  }

  diag.navigationError = 'target_date_not_visible_after_navigation';
  diag.targetDateVisible = false;
  diag.targetDateVisibleFromHeaders = false;
  const finalDiag = await collectPageDiagnostics(page, 'navigation_exhausted');
  applyPageDiagnosticsTo(diag, finalDiag);
  pushThresholdAuditStep(auditContext, 'timeout_or_success', {
    ok: false,
    reason: diag.navigationError,
    stage: 'navigation_exhausted',
    ...finalDiag,
  });
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
  dryRun = false,
  minThreshold = THRESHOLD_SCAN_MIN_DEFAULT,
  maxThreshold = THRESHOLD_SCAN_MAX_DEFAULT,
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
    thresholdWeeksScanned: 0,
    thresholdSessionsMatched: 0,
    thresholdRowsUpserted: 0,
    thresholdSnapshotsInserted: 0,
    thresholdDryRun: dryRun,
    threshold: null,
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

    if (mode === 'basic_only' || mode === 'both' || mode === 'basic_plus_threshold') {
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

    if (mode === 'threshold_slots' || mode === 'basic_plus_threshold') {
      if (launched?.browser) {
        await safeCloseBrowser(launched);
        launched = null;
      }
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

      if (!report.discoveredAvailableDates.length) {
        if (mode === 'threshold_slots') {
          report.skipped = true;
          report.skipReason = 'no_dates_for_threshold_scan';
        }
      } else {
        const thresholdSummary = await runThresholdScansChunked(report.discoveredAvailableDates, {
          dryRun,
          minThreshold,
          maxThreshold,
          sourceTier: 2,
          maxWeeksPerRun: THRESHOLD_SCAN_MAX_WEEKS_PER_RUN,
        });
        report.threshold = thresholdSummary;
        report.thresholdWeeksScanned = thresholdSummary.weeksScanned || 0;
        report.thresholdWeeksRemaining = thresholdSummary.weeksRemaining || 0;
        report.thresholdResumable = thresholdSummary.resumable === true;
        report.thresholdSessionsMatched = (thresholdSummary.dateResults || [])
          .reduce((sum, dr) => sum + (dr.sessionsMatched || 0), 0);
        report.thresholdRowsUpserted = (thresholdSummary.dateResults || [])
          .reduce((sum, dr) => sum + (dr.write?.rowsUpserted || 0), 0);
        report.thresholdSnapshotsInserted = (thresholdSummary.dateResults || [])
          .reduce((sum, dr) => sum + (dr.write?.snapshotsInserted || 0), 0);
        if (thresholdSummary.errors?.length) {
          report.errors.push(...thresholdSummary.errors.slice(0, 5));
        }
      }
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
    releaseScrapeLock();
    handlePlaywrightFailure(e, 'backfill_available_dates');
    report.errors.push({ error: e.message });
    report.durationMs = Date.now() - started;
    throw e;
  } finally {
    releaseScrapeLock();
    void safeCloseBrowser(launched);
  }
}

async function runDateRangeBackfill({
  startDate,
  endDate,
  mode = 'both',
  dryRun = false,
  minThreshold = THRESHOLD_SCAN_MIN_DEFAULT,
  maxThreshold = THRESHOLD_SCAN_MAX_DEFAULT,
  reason = 'admin_backfill_date_range',
} = {}) {
  const { start, end, dates } = clampDateRangeToBookingWindow(startDate, endDate);
  const combined = {
    mode,
    startDate: start,
    endDate: end,
    datesRequested: dates?.length || 0,
    basic: null,
    detail: null,
    threshold: null,
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
  if (mode === 'basic_only' || mode === 'both' || mode === 'basic_plus_threshold') {
    combined.basic = await runDateRangeBackfillBasic(start, end, { reason: `${reason}_basic` });
    if (combined.basic.skipped && mode === 'basic_only') {
      combined.skipped = combined.basic.skipped;
      combined.skipReason = combined.basic.skipReason;
    }
  }

  if (mode === 'threshold_slots' || mode === 'basic_plus_threshold') {
    if (!tryAcquireScrapeLock(`date range threshold backfill (${reason})`, 0)) {
      combined.skipped = true;
      combined.skipReason = 'scrape_in_progress';
      combined.durationMs = Date.now() - started;
      return combined;
    }
    try {
      await ensureSessionsForStatus();
      combined.threshold = await runThresholdScansChunked(dates, {
        dryRun,
        minThreshold,
        maxThreshold,
        sourceTier: 2,
        maxWeeksPerRun: THRESHOLD_SCAN_MAX_WEEKS_PER_RUN,
      });
    } catch (e) {
      releaseScrapeLock();
      handlePlaywrightFailure(e, 'date_range_threshold_backfill');
      combined.threshold = { error: e.message, crashed: isPlaywrightCrashError(e) };
    } finally {
      releaseScrapeLock();
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
    releaseScrapeLock();
    handlePlaywrightFailure(e, 'date_range_backfill_basic');
    report.errors.push({ error: e.message });
    report.durationMs = Date.now() - started;
    throw e;
  } finally {
    releaseScrapeLock();
    void safeCloseBrowser(launched);
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

async function launchBrowser({ blockHeavyAssets = false, headed = false } = {}) {
  const browser = await chromium.launch({
    headless: !headed,
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
  const stale = all.filter(s => sessionDetailQueueEligible(s));
  const missingSlots = all.filter(s => s.available && !sessionDetailVerified(s));
  const missingPrice = all.filter(s => s.available && !s.priceText && s.priceMin == null && !sessionDetailVerified(s));
  const stats = detailCoverageStats();

  return {
    queuePending: enrichmentQueuePendingCount,
    queueRunning: enrichmentQueueRunningCount,
    detailQueueEligibleCount: stale.length,
    detailQueueDrainScheduled: collectorState.detailQueueDrainScheduled,
    lastDetailQueueBatch: collectorState.lastDetailQueueBatch || null,
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

async function openBookingPage(page, { timeout = BOOKING_PAGE_TIMEOUT_MS, waitUntil = 'networkidle' } = {}) {
  await dismissCookieBanner(page).catch(() => {});
  await withPlaywrightGuard(
    () => page.goto(BOOKING, { waitUntil, timeout }),
    { stage: 'open_booking_page_goto', timeout: timeout + 5000 },
  );
  await page.waitForSelector('.dynamic-cal-booking-ts', { timeout: Math.min(timeout, 20_000) });
  await dismissCookieBanner(page);
  await waitForCookieBannerGone(page, 6000);
}

async function openBookingPageForThreshold(page, { auditContext = null } = {}) {
  await openBookingPage(page, {
    timeout: THRESHOLD_SCAN_PAGE_TIMEOUT_MS,
    waitUntil: 'domcontentloaded',
  });
  await waitForThresholdCalendarShell(page);
  const diag = await collectPageDiagnostics(page, 'booking_calendar_wait');
  pushThresholdAuditStep(auditContext, 'booking_calendar_wait', { ok: true, ...diag });
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

    if (!cfg.slotCounts && BACKGROUND_DETAIL_ENRICHMENT_ENABLED) {
      const needing = merged.filter(s => sessionDetailQueueEligible(s));
      if (needing.length) {
        await enqueueSessionsForEnrichment(needing, {
          priority: tier === 2 ? 2 : 3,
          reason: `tier_${tier}_basic`,
        });
        scheduleDetailQueueDrain({ reason: `tier_${tier}_basic`, limit: DETAIL_ENRICH_MAX_PER_RUN });
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

    report.completed = true;
    report.sessionsFound = merged.length;
    report.rowsUpserted = rowsUpserted;
    report.snapshotsInserted = snapshotsInserted;
    report.durationMs = lastTierDurationMs[tier];
    updateTierNextRunEstimate(tier);
    recordTierRunState(tier, report, { reason });

  } catch (e) {
    releaseScrapeLock();
    handlePlaywrightFailure(e, `tier_${tier}_scrape`, { tier });
    lastTierError[tier] = lastScrapeError;
    report.errors.push({ error: lastScrapeError });
    report.error = lastScrapeError;
    report.durationMs = Date.now() - tierStarted;
    report.crashed = isPlaywrightCrashError(e);
    lastTierDurationMs[tier] = report.durationMs;
    recordTierRunState(tier, report, { reason });
    void saveScrapeErrorToSupabase(lastScrapeError).catch((err) => {
      console.error('  saveScrapeErrorToSupabase failed:', err.message);
    });
    void finishScrapeRun(scrapeRunId, {
      success: false,
      sessionsFound: sessions.length,
      datesCovered: coverage?.coveredDatesCount ?? null,
      missingDates: coverage?.missingDatesInScrapeWindow ?? null,
      error: lastScrapeError,
      errorStack: lastScrapeErrorStack,
    }).catch((err) => {
      console.error('  finishScrapeRun failed:', err.message);
    });
  } finally {
    releaseScrapeLock();
    void safeCloseBrowser(launched);
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
    handlePlaywrightFailure(e, 'week_detection');
    effectiveWeeksAhead = Math.max(1, SCRAPE_WEEKS_AHEAD);
  } finally {
    releaseWeekDetectionLock();
    void safeCloseBrowser(launched);
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
  if (!['basic_only', 'verified_detail', 'both', 'threshold_slots', 'basic_plus_threshold'].includes(mode)) {
    return res.status(400).json({
      error: 'mode must be basic_only, verified_detail, both, threshold_slots, or basic_plus_threshold',
    });
  }

  const dryRun = req.body?.dryRun === true;
  const minThreshold = req.body?.minThreshold != null
    ? parseInt(req.body.minThreshold, 10)
    : THRESHOLD_SCAN_MIN_DEFAULT;
  const maxThreshold = req.body?.maxThreshold != null
    ? parseInt(req.body.maxThreshold, 10)
    : THRESHOLD_SCAN_MAX_DEFAULT;

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
        dryRun,
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
      dryRun,
      minThreshold,
      maxThreshold,
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
  const dryRun = req.body?.dryRun === true;
  const minThreshold = req.body?.minThreshold != null
    ? parseInt(req.body.minThreshold, 10)
    : THRESHOLD_SCAN_MIN_DEFAULT;
  const maxThreshold = req.body?.maxThreshold != null
    ? parseInt(req.body.maxThreshold, 10)
    : THRESHOLD_SCAN_MAX_DEFAULT;

  if (!['basic_only', 'verified_detail', 'both', 'threshold_slots', 'basic_plus_threshold'].includes(mode)) {
    return res.status(400).json({
      error: 'mode must be basic_only, verified_detail, both, threshold_slots, or basic_plus_threshold',
    });
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
        dryRun,
        minThreshold,
        maxThreshold,
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
      dryRun,
      minThreshold,
      maxThreshold,
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
        dryRun,
        minThreshold,
        maxThreshold,
        message: 'Backfill queued — poll GET /api/debug/coverage (discoveredAvailableDates, lastBackfillAvailableDatesResult, lastThresholdScanResult)',
      });
    }

    const result = await runBackfill();
    res.json({
      started: true,
      queued: false,
      wait: true,
      mode,
      maxHorizonDays: maxHorizonDays ?? MAX_BOOKING_HORIZON_DAYS,
      dryRun,
      minThreshold,
      maxThreshold,
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

app.post('/api/admin/debug-entries-left-control', async (req, res) => {
  const isoDate = req.body?.isoDate;
  const weekMode = req.body?.weekMode !== false;
  const thresholds = Array.isArray(req.body?.thresholds) && req.body.thresholds.length
    ? req.body.thresholds.map((t) => Number(t)).filter((t) => Number.isFinite(t) && t >= 1)
    : [1, 2, 3];
  const threshold = req.body?.threshold != null
    ? Math.max(1, parseInt(req.body.threshold, 10) || 1)
    : 1;
  const wait = req.body?.wait === true || req.query?.wait === 'true';
  const dryRun = req.body?.dryRun !== false;
  const debug = req.body?.debug !== false;
  const modeRaw = req.body?.mode;
  const mode = modeRaw === 'selection_contract'
    || modeRaw === 'grid_change_contract'
    || modeRaw === 'tile_parser_contract'
    || modeRaw === 'threshold_inference_contract'
    ? modeRaw
    : 'recon';
  const gate = entriesLeftDebugGateForMode(mode);

  if (!isoDate || !/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) {
    return res.status(400).json({ error: 'isoDate must be YYYY-MM-DD' });
  }

  const runRecon = async () => {
    if (!tryAcquireScrapeLock('debug entries-left control recon', 0)) {
      return {
        skipped: true,
        skipReason: 'scrape_in_progress',
        gate,
        mode,
        isoDate,
        weekMode,
        dryRun,
      };
    }
    try {
      await ensureSessionsForStatus();
      return await runDebugEntriesLeftControl({
        isoDate,
        weekMode,
        thresholds,
        threshold,
        dryRun,
        debug,
        mode,
      });
    } finally {
      releaseScrapeLock();
    }
  };

  try {
    if (scrapeInProgress && !wait) {
      return res.json({
        started: false,
        skipped: true,
        skipReason: 'scrape_in_progress',
        gate,
        mode,
        isoDate,
        weekMode,
        dryRun,
        message: 'Another scrape is running — retry with wait=true',
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

    if (!wait) {
      setImmediate(() => {
        runRecon().catch((err) => console.error('debug-entries-left-control error:', err));
      });
      return res.json({
        started: true,
        queued: true,
        wait: false,
        gate,
        mode,
        isoDate,
        weekMode,
        thresholds,
        dryRun,
        message: mode === 'threshold_inference_contract'
          ? 'Entries-left threshold inference contract queued — retry with wait=true'
          : mode === 'tile_parser_contract'
          ? 'Entries-left tile parser contract queued — retry with wait=true'
          : mode === 'grid_change_contract'
            ? 'Entries-left grid-change contract queued — retry with wait=true'
            : mode === 'selection_contract'
              ? 'Entries-left selection contract queued — retry with wait=true'
              : 'Entries-left control recon queued — retry with wait=true',
      });
    }

    const result = await runRecon();
    res.json(result);
  } catch (e) {
    handlePlaywrightFailure(e, 'debug_entries_left_control', { weekKey: isoDate });
    res.status(500).json({
      gate,
      mode,
      error: e.message,
      isoDate,
      weekMode,
      dryRun,
      crashed: isPlaywrightCrashError(e),
    });
  }
});

app.post('/api/admin/scan-entries-left-thresholds', async (req, res) => {
  const isoDate = req.body?.isoDate;
  const weekMode = req.body?.weekMode !== false;
  const minThreshold = req.body?.minThreshold != null
    ? parseInt(req.body.minThreshold, 10)
    : THRESHOLD_SCAN_MIN_DEFAULT;
  const maxThreshold = req.body?.maxThreshold != null
    ? parseInt(req.body.maxThreshold, 10)
    : THRESHOLD_SCAN_MAX_DEFAULT;
  const wait = req.body?.wait === true || req.query?.wait === 'true';
  const dryRun = req.body?.dryRun !== false;
  const debug = req.body?.debug === true;
  const trace = req.body?.trace === true;
  const screenshot = req.body?.screenshot === true;
  const headed = req.body?.headed === true;

  if (!isoDate || !/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) {
    return res.status(400).json({ error: 'isoDate must be YYYY-MM-DD' });
  }

  const runScan = async () => {
    if (!tryAcquireScrapeLock('entries-left threshold scan', 0)) {
      return {
        skipped: true,
        skipReason: 'scrape_in_progress',
        isoDate,
        requestedIsoDate: isoDate,
        weekMode,
        dryRun,
      };
    }

    const requestedIsoDate = isoDate;
    const computedWeekStart = getMondayWeekStartIso(requestedIsoDate);
    const navigationIsoDate = weekMode ? computedWeekStart : requestedIsoDate;

    try {
      await ensureSessionsForStatus();
      const result = await runThresholdScansChunked([requestedIsoDate], {
        dryRun,
        minThreshold,
        maxThreshold,
        sourceTier: 2,
        maxWeeksPerRun: 1,
        requestedIsoDate,
        navigationIsoDate,
        computedWeekStart,
        weekMode,
        debug,
        trace,
        screenshot,
        headed,
      });

      await refreshCoverageFlags().catch(() => {});
      return flattenThresholdScanApiResponse({
        scanResult: result,
        requestedIsoDate,
        computedWeekStart,
        navigationIsoDate,
        weekMode,
        dryRun,
        routeMeta: {
          skipped: false,
          isoDate,
          minThreshold,
          maxThreshold,
          debug,
          trace,
          screenshot,
          headed,
        },
      });
    } catch (e) {
      releaseScrapeLock();
      handlePlaywrightFailure(e, 'admin_threshold_scan', { weekKey: isoDate });
      const failureReason = isPlaywrightCrashError(e) ? 'failed_page_crash' : 'failed_threshold_scan';
      return flattenThresholdScanApiResponse({
        scanResult: {},
        requestedIsoDate: isoDate,
        computedWeekStart: getMondayWeekStartIso(isoDate),
        navigationIsoDate: weekMode ? getMondayWeekStartIso(isoDate) : isoDate,
        weekMode,
        dryRun,
        routeMeta: {
          skipped: false,
          crashed: isPlaywrightCrashError(e),
          failureReason,
          error: e.message,
          isoDate,
          earlyExitStage: 'before_browser_launch',
          earlyExitReason: e.message,
          statusReason: isPlaywrightCrashError(e) ? null : 'calendar_headers_not_ready',
        },
      });
    } finally {
      releaseScrapeLock();
    }
  };

  try {
    if (scrapeInProgress && !wait) {
      return res.json({
        started: false,
        skipped: true,
        skipReason: 'scrape_in_progress',
        isoDate,
        weekMode,
        dryRun,
        message: 'Another scrape is running — retry with wait=true',
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

    if (!wait) {
      setImmediate(() => {
        runScan().catch((err) => console.error('scan-entries-left-thresholds error:', err));
      });
      return res.json({
        started: true,
        queued: true,
        wait: false,
        isoDate,
        weekMode,
        dryRun,
        minThreshold,
        maxThreshold,
        message: 'Threshold scan queued — poll GET /api/debug/coverage (lastThresholdScanResult)',
      });
    }

    const result = await runScan();
    res.json({
      started: true,
      queued: false,
      wait: true,
      ...result,
    });
  } catch (e) {
    res.status(500).json({ error: e.message, isoDate, weekMode, dryRun });
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

app.post('/api/admin/enrich-detail-queue', async (req, res) => {
  const isoDate = normalizeIsoDateParam(req.body?.isoDate || req.body?.iso_date);
  const limit = Math.min(
    Math.max(parseInt(req.body?.limit || req.query?.limit || '20', 10) || 20, 1),
    100,
  );
  const wait = req.body?.wait === true || req.query?.wait === 'true';

  if (!isoDate) {
    return res.status(400).json({ error: 'isoDate required (YYYY-MM-DD)' });
  }

  try {
    await ensureSessionsForStatus();
    const eligible = sessionsForDate(isoDate).filter(s => sessionDetailQueueEligible(s));

    if (!wait) {
      if (eligible.length) {
        await enqueueSessionsForEnrichment(eligible, {
          priority: enrichmentPriorityForSession(eligible[0]),
          reason: `admin_enrich_detail_queue:${isoDate}`,
        });
      }
      scheduleDetailQueueDrain({
        reason: `admin_enrich_detail_queue:${isoDate}`,
        limit,
        delayMs: 0,
      });
      return res.json({
        isoDate,
        limit,
        wait: false,
        queued: true,
        detailQueueEligibleCount: eligible.length,
        message: 'Detail queue drain scheduled — poll /api/debug/date/:isoDate',
      });
    }

    const result = await processDetailEnrichmentQueue({
      isoDate,
      limit,
      reason: 'admin_enrich_detail_queue',
    });
    res.json(formatEnrichmentApiResponse(result, {
      isoDate,
      limit,
      wait: true,
      detailQueueEligibleCount: eligible.length,
    }));
  } catch (e) {
    res.status(500).json({ isoDate, error: e.message, errors: [{ error: e.message }] });
  }
});

app.post('/api/admin/enrich-all-available-details', async (req, res) => {
  const limitPerDate = Math.min(
    Math.max(parseInt(req.body?.limitPerDate || req.query?.limitPerDate || '20', 10) || 20, 1),
    100,
  );
  const wait = req.body?.wait === true || req.query?.wait === 'true';

  try {
    await ensureSessionsForStatus();

    if (!wait) {
      setImmediate(() => {
        processAllAvailableDetailQueue({ limitPerDate, reason: 'admin_enrich_all_available' }).catch(console.error);
      });
      return res.json({
        limitPerDate,
        wait: false,
        queued: true,
        discoveredAvailableDates: getDiscoveredAvailableDates(),
        message: 'Processing all available dates in background — poll /api/debug/coverage',
      });
    }

    const result = await processAllAvailableDetailQueue({ limitPerDate, reason: 'admin_enrich_all_available' });
    res.json({
      limitPerDate,
      wait: true,
      discoveredAvailableDates: getDiscoveredAvailableDates(),
      ...result,
    });
  } catch (e) {
    res.status(500).json({ error: e.message, errors: [{ error: e.message }] });
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

app.post('/api/admin/release-scrape-lock', (req, res) => {
  const force = req.body?.force === true || req.query?.force === 'true';
  const reason = req.body?.reason || req.query?.reason || 'manual_recovery';
  const ageSeconds = getScrapeLockAgeSeconds();

  if (!scrapeInProgress) {
    return res.json({
      released: false,
      reason: 'not_locked',
      scrapeInProgress: false,
      currentScrapeAgeSeconds: null,
      scrapeLockMaxMs: SCRAPE_LOCK_MAX_MS,
    });
  }

  if (!force && ageSeconds != null && ageSeconds * 1000 < SCRAPE_LOCK_MANUAL_RELEASE_MIN_MS) {
    return res.status(409).json({
      released: false,
      reason: 'lock_too_young',
      scrapeInProgress: true,
      currentScrapeTier,
      currentScrapeStartedAt,
      currentScrapeAgeSeconds: ageSeconds,
      minReleaseAgeSeconds: Math.round(SCRAPE_LOCK_MANUAL_RELEASE_MIN_MS / 1000),
      scrapeLockMaxMs: SCRAPE_LOCK_MAX_MS,
      hint: 'Pass force=true to release immediately',
    });
  }

  const previousTier = currentScrapeTier;
  const previousStartedAt = currentScrapeStartedAt;
  releaseScrapeLock();
  collectorState.scrapeLockReleasedAt = new Date().toISOString();
  collectorState.scrapeLockReleasedReason = reason;
  collectorState.thresholdScanRecovered = true;

  res.json({
    released: true,
    reason,
    force,
    previousTier,
    previousStartedAt,
    previousScrapeAgeSeconds: ageSeconds,
    scrapeInProgress: false,
    scrapeLockMaxMs: SCRAPE_LOCK_MAX_MS,
    lastScrapeError,
    lastPageCrashAt: collectorState.lastPageCrashAt,
    lastPageCrashStage: collectorState.lastPageCrashStage,
  });
});

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
  if (scrapeInProgress) {
    const lockAgeSec = getScrapeLockAgeSeconds();
    if (lockAgeSec != null && lockAgeSec * 1000 > SCRAPE_LOCK_MANUAL_RELEASE_MIN_MS) {
      recommendedAction = `Scrape lock held ${lockAgeSec}s (tier ${currentScrapeTier}) — POST /api/admin/release-scrape-lock with force=true or wait for SCRAPE_LOCK_MAX_MS`;
    } else if (lockAgeSec != null) {
      recommendedAction = `Scrape in progress (tier ${currentScrapeTier}, ${lockAgeSec}s) — saved data still served from Supabase`;
    }
  } else if (!scrapeScheduleEnabled) {
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
    ...thresholdStabilityDebugPayload(),
  };
}

async function runBackgroundThresholdBatch() {
  if (!BACKGROUND_THRESHOLD_SCAN_ENABLED) return { skipped: true, skipReason: 'background_threshold_disabled' };
  if (scrapeInProgress || detailEnrichmentInProgress) {
    return { skipped: true, skipReason: 'scrape_or_enrichment_busy' };
  }
  if (!tryAcquireScrapeLock('background threshold batch', 0)) {
    return { skipped: true, skipReason: 'scrape_in_progress' };
  }

  try {
    await ensureSessionsForStatus();
    const discovery = resolveDiscoveredAvailableDates();
    const dates = discovery.dates?.length
      ? discovery.dates
      : [...new Set(allStoredSessions().map(sessionDateKey).filter(Boolean))].sort();

    if (!dates.length && !(collectorState.thresholdScanPendingWeeks || []).length) {
      return { skipped: true, skipReason: 'no_dates_for_threshold_scan' };
    }

    const result = await runThresholdScansChunked(dates, {
      dryRun: false,
      sourceTier: 2,
      maxWeeksPerRun: THRESHOLD_SCAN_MAX_WEEKS_PER_RUN,
    });
    collectorState.lastThresholdScanResult = { ...result, source: 'background_scheduler' };
    return { skipped: false, ...result };
  } catch (e) {
    releaseScrapeLock();
    handlePlaywrightFailure(e, 'background_threshold_batch');
    return { skipped: false, crashed: isPlaywrightCrashError(e), error: e.message };
  } finally {
    releaseScrapeLock();
  }
}

function runBackgroundThresholdBatchAsync() {
  runBackgroundThresholdBatch().catch((e) => console.error('[background threshold]', e.message));
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
  collectorState.cronTasks.enrichP1 = BACKGROUND_DETAIL_ENRICHMENT_ENABLED
    ? scheduleCronSafe(
      collectorState.tier1Interval,
      () => setTimeout(() => runDetailEnrichmentByPriority(1).catch(console.error), ENRICHMENT_P1_OFFSET_MS),
      'Detail enrichment P1',
    )
    : null;
  collectorState.cronTasks.enrichP2 = BACKGROUND_DETAIL_ENRICHMENT_ENABLED
    ? scheduleCronSafe(
      `*/${ENRICHMENT_TIER2_EVERY_MINS} * * * *`,
      () => runDetailEnrichmentByPriority(2).catch(console.error),
      'Detail enrichment P2',
    )
    : null;
  collectorState.cronTasks.enrichP3 = BACKGROUND_DETAIL_ENRICHMENT_ENABLED
    ? scheduleCronSafe(
      '0 */12 * * *',
      () => runDetailEnrichmentByPriority(3).catch(console.error),
      'Detail enrichment P3',
    )
    : null;

  if (BACKGROUND_THRESHOLD_SCAN_ENABLED) {
    const thresholdCron = `*/${BACKGROUND_THRESHOLD_SCAN_EVERY_MINS} * * * *`;
    collectorState.cronTasks.thresholdBatch = scheduleCronSafe(
      thresholdCron,
      () => runBackgroundThresholdBatchAsync(),
      'Threshold batch',
    );
    console.log(`  Scheduling threshold batch every ${BACKGROUND_THRESHOLD_SCAN_EVERY_MINS} minutes (max ${THRESHOLD_SCAN_MAX_WEEKS_PER_RUN} week(s)/run)`);
  }

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

  setInterval(() => {
    if (releaseScrapeLockIfStale()) {
      collectorState.thresholdScanRecovered = true;
    }
  }, 60_000);

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

const calendarFixtureHelpers = {
  launchBrowser,
  openBookingPageForThreshold,
  waitForThresholdCalendarShell,
  navigateCalendarToShowDate,
  normalizeBookingFiltersOnPage,
  setEntriesLeftThreshold,
  dismissEntriesLeftPopup,
  getMondayWeekStartIso,
  safeCloseBrowser,
  THRESHOLD_FILTER_SETTLE_MS,
};

if (require.main === module) {
  startServer().catch((e) => {
    console.error('Server startup failed:', e.message);
    process.exit(1);
  });
} else {
  module.exports = calendarFixtureHelpers;
}
