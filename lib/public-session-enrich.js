'use strict';

const PUBLIC_ENRICH_ALLOWED_BODY_KEYS = new Set(['isoDate']);
const PUBLIC_ENRICH_IP_WINDOW_MS = 60_000;
const PUBLIC_ENRICH_IP_MAX = 12;
const PUBLIC_ENRICH_DATE_COOLDOWN_MS = 5 * 60_000;
const PUBLIC_ENRICH_RECENT_DETAIL_MS = 5 * 60_000;

function normalizeIsoDateParam(raw) {
  const value = String(raw || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const [year, month, day] = value.split('-').map(Number);
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year
    || parsed.getUTCMonth() !== month - 1
    || parsed.getUTCDate() !== day
  ) {
    return null;
  }
  return value;
}

function createPublicEnrichRateLimiter({ now = Date.now() } = {}) {
  return {
    ipHits: new Map(),
    dateCooldownUntil: new Map(),
    now,
  };
}

function recordPublicEnrichIpHit(state, ip, { now = Date.now(), windowMs = PUBLIC_ENRICH_IP_WINDOW_MS, max = PUBLIC_ENRICH_IP_MAX } = {}) {
  const key = String(ip || 'unknown');
  const bucket = (state.ipHits.get(key) || []).filter((ts) => now - ts < windowMs);
  bucket.push(now);
  state.ipHits.set(key, bucket);
  return bucket.length > max;
}

function markPublicEnrichDateCooldown(state, isoDate, { now = Date.now(), cooldownMs = PUBLIC_ENRICH_DATE_COOLDOWN_MS } = {}) {
  state.dateCooldownUntil.set(isoDate, now + cooldownMs);
}

function isPublicEnrichDateCoolingDown(state, isoDate, { now = Date.now() } = {}) {
  const until = state.dateCooldownUntil.get(isoDate);
  return until != null && until > now;
}

function validatePublicEnrichDateBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return { ok: false, status: 400, error: 'invalid_body' };
  }
  const keys = Object.keys(body);
  if (keys.length !== 1 || !PUBLIC_ENRICH_ALLOWED_BODY_KEYS.has(keys[0])) {
    return { ok: false, status: 400, error: 'only_isoDate_allowed' };
  }
  const isoDate = normalizeIsoDateParam(body.isoDate);
  if (!isoDate) {
    return { ok: false, status: 400, error: 'invalid_isoDate' };
  }
  return { ok: true, isoDate };
}

function isIsoDateWithinHorizon(isoDate, { todayIso, maxIso }) {
  if (!isoDate || !todayIso || !maxIso) return false;
  return isoDate >= todayIso && isoDate <= maxIso;
}

function resolvePublicEnrichStatus({
  isoDate,
  state,
  detailEnrichmentInProgress = false,
  openSessionCount = 0,
  allRecentlyDetailed = false,
  now = Date.now(),
}) {
  if (openSessionCount <= 0) return { status: 'no_open_sessions', httpStatus: 200 };
  if (allRecentlyDetailed) return { status: 'recently_checked', httpStatus: 200 };
  if (detailEnrichmentInProgress || isPublicEnrichDateCoolingDown(state, isoDate, { now })) {
    return { status: 'already_running', httpStatus: 200 };
  }
  return { status: 'accepted', httpStatus: 202 };
}

function sessionsRecentlyDetailed(sessions, { now = Date.now(), recentMs = PUBLIC_ENRICH_RECENT_DETAIL_MS } = {}) {
  const open = (sessions || []).filter((s) => s.available !== false);
  if (!open.length) return false;
  return open.every((s) => {
    const at = s.lastDetailedCheckAt || s.last_detailed_check_at;
    if (!at) return false;
    return now - new Date(at).getTime() < recentMs;
  });
}

function resolveSchedulingMode({
  inProcessMaintenanceSchedulerEnabled = false,
  inlineThresholdWorkerEnabled = false,
} = {}) {
  const maintenanceScheduler = inProcessMaintenanceSchedulerEnabled ? 'in_process' : 'railway_cron';
  const thresholdWorker = inlineThresholdWorkerEnabled ? 'in_process' : 'railway_cron';
  const mode = (inProcessMaintenanceSchedulerEnabled || inlineThresholdWorkerEnabled)
    ? 'in_process'
    : 'railway_crons';
  const warnings = [];
  if (inProcessMaintenanceSchedulerEnabled) {
    warnings.push('IN_PROCESS_MAINTENANCE_SCHEDULER_ENABLED=true may duplicate Railway maintenance-tick-cron');
  }
  if (inlineThresholdWorkerEnabled) {
    warnings.push('INLINE_THRESHOLD_WORKER_ENABLED=true may duplicate Railway threshold-worker-cron');
  }
  return {
    mode,
    maintenanceScheduler,
    thresholdWorker,
    warnings,
  };
}

module.exports = {
  PUBLIC_ENRICH_ALLOWED_BODY_KEYS,
  PUBLIC_ENRICH_IP_WINDOW_MS,
  PUBLIC_ENRICH_IP_MAX,
  PUBLIC_ENRICH_DATE_COOLDOWN_MS,
  normalizeIsoDateParam,
  createPublicEnrichRateLimiter,
  recordPublicEnrichIpHit,
  markPublicEnrichDateCooldown,
  isPublicEnrichDateCoolingDown,
  validatePublicEnrichDateBody,
  isIsoDateWithinHorizon,
  resolvePublicEnrichStatus,
  sessionsRecentlyDetailed,
  resolveSchedulingMode,
};
