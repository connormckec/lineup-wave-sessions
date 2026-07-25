'use strict';

const EXCLUDED_LEVELS = ['Cabanas', 'Beach Pass'];
const EXCLUDED_WAVES = [5, 6];
const DEFAULT_COVERAGE_CACHE_TTL_MS = 60_000;
const MAX_COVERAGE_RANGE_DAYS = 120;

function normalizeIsoDate(value) {
  const trimmed = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return null;
  const [year, month, day] = trimmed.split('-').map(Number);
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year
    || parsed.getUTCMonth() !== month - 1
    || parsed.getUTCDate() !== day
  ) {
    return null;
  }
  return trimmed;
}

function addDaysToIso(isoDate, days) {
  const [year, month, day] = isoDate.split('-').map(Number);
  const parsed = new Date(Date.UTC(year, month - 1, day + days));
  return parsed.toISOString().slice(0, 10);
}

function resolveCoverageDateRange(startDate, endDate, { todayIso, maxIso } = {}) {
  const start = normalizeIsoDate(startDate);
  const end = normalizeIsoDate(endDate);
  if (!start || !end) {
    return { ok: false, status: 400, error: 'invalid_start_or_end_date' };
  }
  if (start > end) {
    return { ok: false, status: 400, error: 'startDate_after_endDate' };
  }
  const clampedStart = todayIso && start < todayIso ? todayIso : start;
  const clampedEnd = maxIso && end > maxIso ? maxIso : end;
  if (clampedStart > clampedEnd) {
    return { ok: false, status: 400, error: 'range_outside_horizon' };
  }

  let dayCount = 0;
  let cur = clampedStart;
  while (cur <= clampedEnd) {
    dayCount += 1;
    if (dayCount > MAX_COVERAGE_RANGE_DAYS) {
      return { ok: false, status: 400, error: 'range_too_large', maxDays: MAX_COVERAGE_RANGE_DAYS };
    }
    cur = addDaysToIso(cur, 1);
  }

  return {
    ok: true,
    startDate: clampedStart,
    endDate: clampedEnd,
  };
}

function waveFromCoverageRow(row = {}) {
  if (row.wave != null) return Number(row.wave);
  const raw = row.raw && typeof row.raw === 'object' ? row.raw : {};
  if (raw.wave != null) return Number(raw.wave);
  return null;
}

function isSurfSessionCoverageRow(row = {}) {
  const level = row.session_type ?? row.level ?? null;
  const wave = waveFromCoverageRow(row);
  if (EXCLUDED_LEVELS.includes(level)) return false;
  if (Number.isFinite(wave) && EXCLUDED_WAVES.includes(wave)) return false;
  return true;
}

function aggregateCoverageByDate(rows = []) {
  const byDate = new Map();
  for (const row of rows) {
    const isoDate = normalizeIsoDate(row.iso_date ?? row.isoDate);
    if (!isoDate) continue;
    const entry = byDate.get(isoDate) || {
      sessionCount: 0,
      anyRows: 0,
      lastCheckedAt: null,
    };
    entry.anyRows += 1;
    const checkedAt = row.last_basic_check_at ?? row.lastBasicCheckAt ?? null;
    if (checkedAt && (!entry.lastCheckedAt || checkedAt > entry.lastCheckedAt)) {
      entry.lastCheckedAt = checkedAt;
    }
    if (isSurfSessionCoverageRow(row)) {
      entry.sessionCount += 1;
    }
    byDate.set(isoDate, entry);
  }
  return byDate;
}

function resolveDateCoverageStatus(isoDate, entry, checkedEmptySet, persistedCheckedSet) {
  const checkedEmpty = checkedEmptySet instanceof Set
    ? checkedEmptySet.has(isoDate)
    : (checkedEmptySet || []).includes(isoDate);
  const persistedChecked = persistedCheckedSet instanceof Set
    ? persistedCheckedSet.has(isoDate)
    : (persistedCheckedSet || []).includes(isoDate);
  const sessionCount = entry?.sessionCount ?? 0;

  if (sessionCount > 0) {
    return {
      isoDate,
      status: 'has_sessions',
      sessionCount,
      lastCheckedAt: entry?.lastCheckedAt ?? null,
    };
  }

  if (checkedEmpty || persistedChecked || (entry?.anyRows ?? 0) > 0) {
    return {
      isoDate,
      status: 'checked_empty',
      sessionCount: 0,
      lastCheckedAt: entry?.lastCheckedAt ?? null,
    };
  }

  return {
    isoDate,
    status: 'not_checked',
    sessionCount: null,
    lastCheckedAt: null,
  };
}

function buildDateCoverageList(startDate, endDate, aggregatedByDate, checkedEmptySet, persistedCheckedSet) {
  const dates = [];
  let cur = startDate;
  while (cur <= endDate) {
    dates.push(resolveDateCoverageStatus(
      cur,
      aggregatedByDate.get(cur),
      checkedEmptySet,
      persistedCheckedSet,
    ));
    cur = addDaysToIso(cur, 1);
  }
  return dates;
}

function shouldRefreshDateCoverage({
  lastFetchedAt = null,
  now = Date.now(),
  ttlMs = DEFAULT_COVERAGE_CACHE_TTL_MS,
  rangeKey = null,
  cachedRangeKey = null,
  force = false,
} = {}) {
  if (force) return true;
  if (!lastFetchedAt || !cachedRangeKey || cachedRangeKey !== rangeKey) return true;
  return now - lastFetchedAt >= ttlMs;
}

function pickerClassForCoverageStatus(status) {
  if (status === 'has_sessions') return 'has-sessions';
  if (status === 'checked_empty') return 'no-sessions checked-empty';
  return 'coverage-unknown';
}

function coverageStatusFromSelectedDateMeta({ statusReason, sessionsCount = 0, lastCheckedAt = null } = {}) {
  if (sessionsCount > 0 || statusReason === 'saved_sessions_found' || statusReason === 'fallback_sessions_found') {
    return {
      status: 'has_sessions',
      sessionCount: sessionsCount,
      lastCheckedAt,
    };
  }
  if (statusReason === 'checked_no_sessions') {
    return {
      status: 'checked_empty',
      sessionCount: 0,
      lastCheckedAt,
    };
  }
  if (statusReason === 'error' || statusReason === 'schema_error') {
    return {
      status: 'error',
      sessionCount: null,
      lastCheckedAt: null,
    };
  }
  return null;
}

function mergeSelectedDateCoverage(byDate, isoDate, selectedMeta = {}) {
  const mapped = coverageStatusFromSelectedDateMeta(selectedMeta);
  if (!mapped) return byDate;
  const next = { ...(byDate || {}) };
  next[isoDate] = {
    isoDate,
    status: mapped.status,
    sessionCount: mapped.sessionCount,
    lastCheckedAt: mapped.lastCheckedAt,
  };
  return next;
}

module.exports = {
  EXCLUDED_LEVELS,
  EXCLUDED_WAVES,
  DEFAULT_COVERAGE_CACHE_TTL_MS,
  MAX_COVERAGE_RANGE_DAYS,
  normalizeIsoDate,
  addDaysToIso,
  resolveCoverageDateRange,
  isSurfSessionCoverageRow,
  aggregateCoverageByDate,
  resolveDateCoverageStatus,
  buildDateCoverageList,
  shouldRefreshDateCoverage,
  pickerClassForCoverageStatus,
  coverageStatusFromSelectedDateMeta,
  mergeSelectedDateCoverage,
};
