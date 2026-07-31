'use strict';

const adaptiveSchedule = require('./adaptive-threshold-schedule');
const thresholdDatePipeline = require('./threshold-date-pipeline');

const NEAR_TERM_SESSION_SELECT = [
  'session_key',
  'iso_date',
  'start_ts',
  'start_time',
  'available',
  'last_detailed_check_at',
  'raw',
].join(', ');

const DEFAULT_OPTIONAL_QUERY_TIMEOUT_MS = 8000;

function inferRowCount(value) {
  if (value == null) return null;
  if (typeof value === 'number') return value;
  if (Array.isArray(value)) return value.length;
  if (typeof value.count === 'number') return value.count;
  if (Array.isArray(value.data)) return value.data.length;
  if (Array.isArray(value.ready)) return value.ready.length;
  if (Array.isArray(value.dates)) return value.dates.length;
  if (Array.isArray(value.candidates)) return value.candidates.length;
  if (value.topCandidate) return 1;
  return null;
}

function createQueryInstrumenter(routeName) {
  const logEntry = (entry) => {
    console.log(JSON.stringify({ maintenanceRoute: routeName, ...entry }));
  };

  async function runInstrumentedQuery(queryName, fn) {
    const started = Date.now();
    try {
      const value = await fn();
      const elapsedMs = Date.now() - started;
      const rowCount = inferRowCount(value);
      logEntry({ queryName, elapsedMs, rowCount, ok: true });
      return { ok: true, value, elapsedMs, rowCount, queryName };
    } catch (err) {
      const elapsedMs = Date.now() - started;
      const message = err?.message || String(err);
      const code = err?.code || err?.error?.code || null;
      logEntry({ queryName, elapsedMs, rowCount: null, ok: false, error: message, code });
      return { ok: false, error: err, elapsedMs, rowCount: null, queryName, code, message };
    }
  }

  async function runOptionalQuery(queryName, fn, {
    timeoutMs = DEFAULT_OPTIONAL_QUERY_TIMEOUT_MS,
    required = false,
  } = {}) {
    let timer;
    const timeoutPromise = new Promise((resolve) => {
      timer = setTimeout(() => resolve(null), timeoutMs);
    });
    try {
      const raced = await Promise.race([
        runInstrumentedQuery(queryName, fn),
        timeoutPromise,
      ]);
      if (!raced) {
        const elapsedMs = timeoutMs;
        logEntry({
          queryName,
          elapsedMs,
          rowCount: null,
          ok: false,
          error: 'maintenance_query_timeout',
          code: '57014',
        });
        const warning = {
          queryName,
          code: 'maintenance_query_timeout',
          message: `Optional query "${queryName}" timed out after ${timeoutMs}ms`,
        };
        if (required) {
          return { ok: false, warning, value: null, elapsedMs };
        }
        return { ok: true, skipped: true, warning, value: null, elapsedMs };
      }
      if (!raced.ok && required) {
        return {
          ok: false,
          warning: {
            queryName,
            code: raced.code || 'query_error',
            message: raced.message || 'Query failed',
          },
          value: null,
          elapsedMs: raced.elapsedMs,
        };
      }
      if (!raced.ok) {
        return {
          ok: true,
          skipped: true,
          warning: {
            queryName,
            code: raced.code || 'query_error',
            message: raced.message || 'Query failed',
          },
          value: null,
          elapsedMs: raced.elapsedMs,
        };
      }
      return { ok: true, value: raced.value, elapsedMs: raced.elapsedMs, rowCount: raced.rowCount };
    } finally {
      clearTimeout(timer);
    }
  }

  return { runInstrumentedQuery, runOptionalQuery };
}

function computeNearTermSchedulingBounds(todayIso, addDaysFn) {
  return {
    minDate: addDaysFn(todayIso, -7),
    maxDate: addDaysFn(todayIso, 7),
  };
}

function rowToSchedulingSession(row) {
  const raw = row?.raw && typeof row.raw === 'object' ? row.raw : {};
  return {
    key: row.session_key,
    isoDate: row.iso_date || raw.isoDate || raw.dateKey || null,
    dateKey: row.iso_date || raw.dateKey || raw.isoDate || null,
    ts: row.start_ts ?? raw.ts ?? null,
    start_ts: row.start_ts ?? raw.ts ?? null,
    time: row.start_time || raw.time || null,
    available: row.available,
    lastDetailedCheckAt: row.last_detailed_check_at ?? raw.lastDetailedCheckAt ?? null,
    last_detailed_check_at: row.last_detailed_check_at ?? raw.lastDetailedCheckAt ?? null,
    durationMinutes: raw.durationMinutes ?? 90,
    threshold_scanned_at: raw.threshold_scanned_at ?? raw.threshold_scan_at ?? raw.thresholdScanAt ?? null,
    thresholdScanAt: raw.thresholdScanAt ?? raw.threshold_scanned_at ?? null,
    threshold_scan_verified: raw.threshold_scan_verified ?? raw.thresholdScanVerified ?? false,
    thresholdScanVerified: raw.thresholdScanVerified ?? raw.threshold_scan_verified ?? false,
    thresholdConfidence: raw.thresholdConfidence ?? raw.threshold_confidence ?? null,
    threshold_confidence: raw.threshold_confidence ?? raw.thresholdConfidence ?? null,
    slot_source: raw.slot_source ?? null,
    thresholdTrustedSuspendedAt: raw.thresholdTrustedSuspendedAt ?? null,
    raw,
  };
}

function collectDueScanFromSchedulingSessions(sessions, {
  watchKeys = new Set(),
  todayIso,
  daysFromTodayFn,
  now = new Date(),
} = {}) {
  const active = (sessions || []).filter((session) => !adaptiveSchedule.isSessionEnded(session, now));
  return adaptiveSchedule.collectDueDateScanCandidates(active, {
    watchKeys,
    todayIso,
    daysFromTodayFn,
    now,
  });
}

function compactTopDueDateCandidate(candidate) {
  if (!candidate) return null;
  return {
    isoDate: candidate.isoDate,
    watchedDueCount: candidate.watchedDueCount,
    generalDueCount: candidate.generalDueCount,
    mostOverdueMinutes: candidate.mostOverdueMinutes,
  };
}

function compactDueScanSummary(dueScan) {
  return {
    watchedDueCount: dueScan?.watchedDueCount || 0,
    generalDueCount: dueScan?.generalDueCount || 0,
    dueDateCount: dueScan?.candidates?.length || 0,
    topDueDateCandidate: compactTopDueDateCandidate(dueScan?.topCandidate || null),
  };
}

function aggregateCoverageRows(rows, {
  startDate,
  endDate,
  enumerateDateKeysFn,
  thresholdRowsTrustedFn,
  thresholdScannedAtFn,
  sessionThresholdVerifiedFn,
} = {}) {
  const byDate = new Map();
  for (const isoDate of enumerateDateKeysFn(startDate, endDate)) {
    byDate.set(isoDate, {
      isoDate,
      sessionsCount: 0,
      thresholdRows: 0,
      hasSavedSessions: false,
      hasThresholdCounts: false,
      latestThresholdScannedAt: null,
    });
  }

  for (const row of rows || []) {
    const isoDate = String(row.iso_date || '').slice(0, 10);
    if (!isoDate || !byDate.has(isoDate)) continue;
    const bucket = byDate.get(isoDate);
    bucket.sessionsCount += 1;
    bucket.hasSavedSessions = true;

    const session = rowToSchedulingSession(row);
    if (thresholdRowsTrustedFn(session) || sessionThresholdVerifiedFn(session)) {
      bucket.thresholdRows += 1;
      bucket.hasThresholdCounts = bucket.thresholdRows > 0;
    }
    const scannedAt = thresholdScannedAtFn(session);
    if (scannedAt && (!bucket.latestThresholdScannedAt || scannedAt > bucket.latestThresholdScannedAt)) {
      bucket.latestThresholdScannedAt = scannedAt;
    }
  }

  return [...byDate.values()];
}

function buildNearTermTickCompactResult({
  ok = true,
  action,
  reason = null,
  pass = 'near_term',
  job_id = null,
  sourceJobId = null,
  isoDate = null,
  selectedIsoDate = null,
  dueSummary = null,
  startDate = null,
  endDate = null,
  unhealthy = false,
  error = null,
} = {}) {
  const result = {
    ok,
    action,
    pass,
    startDate,
    endDate,
  };
  if (reason) result.reason = reason;
  if (error) result.error = error;
  if (job_id) result.job_id = job_id;
  if (sourceJobId) result.sourceJobId = sourceJobId;
  if (isoDate) result.isoDate = isoDate;
  if (selectedIsoDate) result.selectedIsoDate = selectedIsoDate;
  if (dueSummary) result.dueSummary = dueSummary;
  if (unhealthy) result.unhealthy = true;
  return result;
}

function buildBacklogFromDueSummary(dueSummary, queueCounts) {
  const dueScan = {
    watchedDueCount: dueSummary?.watchedDueCount || 0,
    generalDueCount: dueSummary?.generalDueCount || 0,
    candidates: dueSummary?.topDueDateCandidate ? [{ isoDate: dueSummary.topDueDateCandidate.isoDate }] : [],
  };
  return thresholdDatePipeline.buildBacklogHealthDiagnostics({
    dueScan,
    ...queueCounts,
  });
}

module.exports = {
  NEAR_TERM_SESSION_SELECT,
  DEFAULT_OPTIONAL_QUERY_TIMEOUT_MS,
  createQueryInstrumenter,
  inferRowCount,
  computeNearTermSchedulingBounds,
  rowToSchedulingSession,
  collectDueScanFromSchedulingSessions,
  compactTopDueDateCandidate,
  compactDueScanSummary,
  aggregateCoverageRows,
  buildNearTermTickCompactResult,
  buildBacklogFromDueSummary,
};
