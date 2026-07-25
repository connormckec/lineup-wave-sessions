'use strict';

const EXCLUDED_LEVELS = ['Cabanas', 'Beach Pass'];
const EXCLUDED_WAVES = [5, 6];
const DEFAULT_COVERAGE_CACHE_TTL_MS = 60_000;
const MAX_COVERAGE_RANGE_DAYS = 120;
const COVERAGE_QUERY_PAGE_SIZE = 500;
const COVERAGE_QUERY_MAX_ROWS = 50_000;

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

function coverageRowKey(row = {}) {
  const isoDate = normalizeIsoDate(row.iso_date ?? row.isoDate);
  const sessionKey = row.session_key ?? row.sessionKey ?? row.key ?? null;
  if (!isoDate || !sessionKey) return null;
  return `${isoDate}:${sessionKey}`;
}

function dedupeCoverageRowsBySessionKey(rows = []) {
  const seen = new Map();
  for (const row of rows) {
    const key = coverageRowKey(row);
    if (!key) continue;
    if (!seen.has(key)) seen.set(key, row);
  }
  return [...seen.values()];
}

async function fetchAllCoverageRowsPaginated(fetchPage, {
  pageSize = COVERAGE_QUERY_PAGE_SIZE,
  maxRows = COVERAGE_QUERY_MAX_ROWS,
} = {}) {
  let offset = 0;
  const rawRows = [];
  let complete = true;

  while (true) {
    const page = await fetchPage({ offset, limit: pageSize });
    if (!Array.isArray(page)) {
      complete = false;
      break;
    }
    rawRows.push(...page);
    if (page.length < pageSize) break;
    offset += pageSize;
    if (rawRows.length >= maxRows) {
      complete = false;
      break;
    }
  }

  return {
    rows: dedupeCoverageRowsBySessionKey(rawRows),
    rowCountScanned: rawRows.length,
    complete,
  };
}

function paginateRowsForTest(allRows, pageSize = COVERAGE_QUERY_PAGE_SIZE) {
  const pages = [];
  for (let offset = 0; offset < allRows.length; offset += pageSize) {
    pages.push(allRows.slice(offset, offset + pageSize));
  }
  if (!pages.length) pages.push([]);
  return pages;
}

async function fetchAllCoverageRowsFromPages(pages, pageSize = COVERAGE_QUERY_PAGE_SIZE) {
  return fetchAllCoverageRowsPaginated(async ({ offset, limit }) => {
    const pageIndex = Math.floor(offset / limit);
    return pages[pageIndex] || [];
  }, { pageSize });
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

function resolveDateCoverageStatus(isoDate, entry, checkedEmptySet, persistedCheckedSet, options = {}) {
  const queryComplete = options.queryComplete !== false;
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

  if (
    queryComplete
    && (checkedEmpty || persistedChecked || (entry?.anyRows ?? 0) > 0)
  ) {
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

function buildDateCoverageList(
  startDate,
  endDate,
  aggregatedByDate,
  checkedEmptySet,
  persistedCheckedSet,
  options = {},
) {
  const dates = [];
  let cur = startDate;
  while (cur <= endDate) {
    dates.push(resolveDateCoverageStatus(
      cur,
      aggregatedByDate.get(cur),
      checkedEmptySet,
      persistedCheckedSet,
      options,
    ));
    cur = addDaysToIso(cur, 1);
  }
  return dates;
}

function buildDateCoveragePayload({
  startDate,
  endDate,
  rows = [],
  queryComplete = true,
  rowCountScanned = rows.length,
  checkedEmptySet,
  persistedCheckedSet,
  fetchedAt = new Date().toISOString(),
} = {}) {
  const aggregatedByDate = aggregateCoverageByDate(rows);
  const dates = buildDateCoverageList(
    startDate,
    endDate,
    aggregatedByDate,
    checkedEmptySet,
    persistedCheckedSet,
    { queryComplete },
  );
  return {
    ok: true,
    startDate,
    endDate,
    fetchedAt,
    generatedAt: fetchedAt,
    complete: queryComplete,
    rowCountScanned,
    dates,
  };
}

function mergeCoverageByDatePreservingConfirmed(prevByDate = {}, nextByDate = {}) {
  const merged = { ...prevByDate };
  for (const [isoDate, row] of Object.entries(nextByDate || {})) {
    const prev = merged[isoDate];
    if (prev?.status === 'has_sessions' && row?.status !== 'has_sessions') {
      continue;
    }
    merged[isoDate] = row;
  }
  return merged;
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
  COVERAGE_QUERY_PAGE_SIZE,
  COVERAGE_QUERY_MAX_ROWS,
  normalizeIsoDate,
  addDaysToIso,
  resolveCoverageDateRange,
  isSurfSessionCoverageRow,
  coverageRowKey,
  dedupeCoverageRowsBySessionKey,
  fetchAllCoverageRowsPaginated,
  paginateRowsForTest,
  fetchAllCoverageRowsFromPages,
  aggregateCoverageByDate,
  resolveDateCoverageStatus,
  buildDateCoverageList,
  buildDateCoveragePayload,
  mergeCoverageByDatePreservingConfirmed,
  shouldRefreshDateCoverage,
  pickerClassForCoverageStatus,
  coverageStatusFromSelectedDateMeta,
  mergeSelectedDateCoverage,
};
