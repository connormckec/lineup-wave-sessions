'use strict';

const datePipeline = require('./threshold-date-pipeline');

const MS_PER_MINUTE = 60 * 1000;
const MS_PER_HOUR = 60 * MS_PER_MINUTE;

const INVENTORY_TARGET_MINUTES_WATCHED = [
  { maxHoursUntilStart: 72, targetMinutes: 5 },
  { maxHoursUntilStart: 7 * 24, targetMinutes: 15 },
  { maxHoursUntilStart: Infinity, targetMinutes: 60 },
];

const INVENTORY_TARGET_MINUTES_GENERAL = [
  { maxHoursUntilStart: 24, targetMinutes: 10 },
  { maxHoursUntilStart: 72, targetMinutes: 30 },
  { maxHoursUntilStart: 7 * 24, targetMinutes: 120 },
  { maxHoursUntilStart: Infinity, targetMinutes: 360 },
];

const PRICE_TARGET_MINUTES = [
  { maxHoursUntilStart: 72, targetMinutes: 45 },
  { maxHoursUntilStart: 7 * 24, targetMinutes: 150 },
  { maxHoursUntilStart: Infinity, targetMinutes: 360 },
];

const HEARTBEAT_TARGET_MINUTES = [
  { maxHoursUntilStart: 24, targetMinutes: 30 },
  { maxHoursUntilStart: 72, targetMinutes: 120 },
  { maxHoursUntilStart: 7 * 24, targetMinutes: 360 },
  { maxHoursUntilStart: Infinity, targetMinutes: 720 },
];

const NEAR_TERM_DATE_SCAN_MAX_HOURS_GENERAL = 72;
const NEAR_TERM_DATE_SCAN_MAX_HOURS_WATCHED = 7 * 24;

function resolveTargetMinutes(hoursUntilStart, table) {
  const hours = Number(hoursUntilStart);
  if (!Number.isFinite(hours)) return table[table.length - 1].targetMinutes;
  for (const row of table) {
    if (hours <= row.maxHoursUntilStart) return row.targetMinutes;
  }
  return table[table.length - 1].targetMinutes;
}

function hoursUntilSessionStart(session, now = new Date()) {
  const ts = session?.ts ?? session?.start_ts ?? session?.startTs;
  if (ts == null) return null;
  return (Number(ts) * 1000 - now.getTime()) / MS_PER_HOUR;
}

function isSessionEnded(session, now = new Date()) {
  const ts = session?.ts ?? session?.start_ts ?? session?.startTs;
  if (ts == null) return false;
  const startMs = Number(ts) * 1000;
  if (!Number.isFinite(startMs)) return false;
  const durationMs = (session?.durationMinutes || 90) * MS_PER_MINUTE;
  return now.getTime() > startMs + durationMs;
}

function resolveInventoryTargetAgeMinutes({ watched = false, hoursUntilStart } = {}) {
  const table = watched ? INVENTORY_TARGET_MINUTES_WATCHED : INVENTORY_TARGET_MINUTES_GENERAL;
  return resolveTargetMinutes(hoursUntilStart, table);
}

function resolvePriceTargetAgeMinutes({ hoursUntilStart } = {}) {
  return resolveTargetMinutes(hoursUntilStart, PRICE_TARGET_MINUTES);
}

function resolveHeartbeatIntervalMinutes({ hoursUntilStart } = {}) {
  return resolveTargetMinutes(hoursUntilStart, HEARTBEAT_TARGET_MINUTES);
}

function resolveHeartbeatIntervalMs(options = {}) {
  return resolveHeartbeatIntervalMinutes(options) * MS_PER_MINUTE;
}

function getThresholdScannedAt(session) {
  return session?.threshold_scanned_at
    ?? session?.thresholdScanAt
    ?? session?.threshold_scan_at
    ?? session?.raw?.threshold_scanned_at
    ?? session?.raw?.thresholdScanAt
    ?? null;
}

function getPriceVerifiedAt(session) {
  return session?.lastDetailedCheckAt
    ?? session?.last_detailed_check_at
    ?? session?.raw?.lastDetailedCheckAt
    ?? null;
}

function ageMsSince(timestamp, now = new Date()) {
  if (!timestamp) return null;
  const ts = new Date(timestamp).getTime();
  if (!Number.isFinite(ts)) return null;
  return Math.max(0, now.getTime() - ts);
}

function isDueByTargetAge(lastCheckedAt, targetMinutes, now = new Date()) {
  const ageMs = ageMsSince(lastCheckedAt, now);
  if (ageMs == null) return true;
  return ageMs >= targetMinutes * MS_PER_MINUTE;
}

function isSessionEligibleForInventorySchedule(session, { watched = false, now = new Date() } = {}) {
  if (!session?.key) return false;
  if (isSessionEnded(session, now)) return false;
  if (watched) return true;
  return session.available !== false;
}

function evaluateInventorySchedule(session, {
  watched = false,
  now = new Date(),
} = {}) {
  const hoursUntilStart = hoursUntilSessionStart(session, now);
  if (hoursUntilStart != null && hoursUntilStart < 0 && isSessionEnded(session, now)) {
    return { eligible: false, reason: 'session_ended' };
  }
  if (!isSessionEligibleForInventorySchedule(session, { watched, now })) {
    return { eligible: false, reason: 'not_eligible' };
  }
  const targetMinutes = resolveInventoryTargetAgeMinutes({ watched, hoursUntilStart });
  const lastSuccessfulCheck = getThresholdScannedAt(session);
  const actualAgeMinutes = ageMsSince(lastSuccessfulCheck, now) == null
    ? null
    : Math.round(ageMsSince(lastSuccessfulCheck, now) / MS_PER_MINUTE);
  const due = isDueByTargetAge(lastSuccessfulCheck, targetMinutes, now);
  const nextDueAt = lastSuccessfulCheck
    ? new Date(new Date(lastSuccessfulCheck).getTime() + targetMinutes * MS_PER_MINUTE).toISOString()
    : now.toISOString();
  return {
    eligible: true,
    due,
    cadenceSource: watched ? 'watch_priority' : 'general_proximity',
    targetFreshnessMinutes: targetMinutes,
    actualFreshnessMinutes: actualAgeMinutes,
    lastSuccessfulCheck,
    nextDueAt,
    hoursUntilStart: hoursUntilStart == null ? null : Number(hoursUntilStart.toFixed(2)),
    scanMode: 'date_threshold_write',
    estimatedAlertDetectionDelayMinutes: Math.ceil(targetMinutes / 2),
  };
}

function evaluatePriceSchedule(session, {
  watched = false,
  now = new Date(),
  force = false,
  inventoryChanged = false,
} = {}) {
  if (!session?.key || isSessionEnded(session, now)) {
    return { eligible: false, due: false, reason: 'session_ended' };
  }
  const hoursUntilStart = hoursUntilSessionStart(session, now);
  const targetMinutes = resolvePriceTargetAgeMinutes({ hoursUntilStart });
  const lastSuccessfulCheck = getPriceVerifiedAt(session);
  const actualAgeMinutes = ageMsSince(lastSuccessfulCheck, now) == null
    ? null
    : Math.round(ageMsSince(lastSuccessfulCheck, now) / MS_PER_MINUTE);
  const due = force
    || inventoryChanged
    || isDueByTargetAge(lastSuccessfulCheck, targetMinutes, now);
  return {
    eligible: true,
    due,
    cadenceSource: watched ? 'watch_priority' : 'general_proximity',
    targetFreshnessMinutes: targetMinutes,
    actualFreshnessMinutes: actualAgeMinutes,
    lastSuccessfulCheck,
    nextDueAt: lastSuccessfulCheck
      ? new Date(new Date(lastSuccessfulCheck).getTime() + targetMinutes * MS_PER_MINUTE).toISOString()
      : now.toISOString(),
    hoursUntilStart: hoursUntilStart == null ? null : Number(hoursUntilStart.toFixed(2)),
    scanMode: 'detail_modal_price',
    estimatedAlertDetectionDelayMinutes: Math.ceil(targetMinutes / 2),
    forced: force === true || inventoryChanged === true,
  };
}

function evaluateHeartbeatSchedule({ hoursUntilStart, lastObservationAt, now = new Date() }) {
  const targetMinutes = resolveHeartbeatIntervalMinutes({ hoursUntilStart });
  const due = isDueByTargetAge(lastObservationAt, targetMinutes, now);
  return {
    due,
    targetFreshnessMinutes: targetMinutes,
    actualFreshnessMinutes: ageMsSince(lastObservationAt, now) == null
      ? null
      : Math.round(ageMsSince(lastObservationAt, now) / MS_PER_MINUTE),
    nextDueAt: lastObservationAt
      ? new Date(new Date(lastObservationAt).getTime() + targetMinutes * MS_PER_MINUTE).toISOString()
      : now.toISOString(),
  };
}

function isDateWithinNearTermDateScan({ isoDate, watched = false, todayIso, daysFromTodayFn }) {
  const daysAhead = daysFromTodayFn(isoDate, todayIso);
  if (!Number.isFinite(daysAhead)) return false;
  if (daysAhead < 0) return watched;
  const maxHours = watched ? NEAR_TERM_DATE_SCAN_MAX_HOURS_WATCHED : NEAR_TERM_DATE_SCAN_MAX_HOURS_GENERAL;
  return daysAhead * 24 <= maxHours;
}

function collectDueDateScanCandidates(sessions, {
  watchKeys = new Set(),
  now = new Date(),
  todayIso,
  daysFromTodayFn,
} = {}) {
  const byDate = new Map();
  let watchedDueCount = 0;
  let generalDueCount = 0;

  for (const session of sessions || []) {
    const watched = watchKeys.has(session.key);
    const evalResult = evaluateInventorySchedule(session, { watched, now });
    if (!evalResult.eligible || !evalResult.due) continue;

    const isoDate = session.isoDate || session.dateKey || session.iso_date;
    if (!isoDate) continue;
    const nearTerm = isDateWithinNearTermDateScan({
      isoDate,
      watched,
      todayIso,
      daysFromTodayFn,
    });
    if (!nearTerm) continue;

    if (watched) watchedDueCount += 1;
    else generalDueCount += 1;

    if (!byDate.has(isoDate)) {
      byDate.set(isoDate, {
        isoDate,
        watchedDueCount: 0,
        generalDueCount: 0,
        mostOverdueMinutes: 0,
        sessions: [],
      });
    }
    const bucket = byDate.get(isoDate);
    if (watched) bucket.watchedDueCount += 1;
    else bucket.generalDueCount += 1;
    const overdue = evalResult.actualFreshnessMinutes == null
      ? evalResult.targetFreshnessMinutes
      : Math.max(0, evalResult.actualFreshnessMinutes - evalResult.targetFreshnessMinutes);
    bucket.mostOverdueMinutes = Math.max(bucket.mostOverdueMinutes, overdue);
    bucket.sessions.push({
      sessionKey: session.key,
      watched,
      ...evalResult,
    });
  }

  const candidates = datePipeline.sortDateScanCandidates([...byDate.values()], {
    daysFromTodayFn,
    todayIso,
  });

  return {
    watchedDueCount,
    generalDueCount,
    candidates,
    topCandidate: candidates[0] || null,
  };
}

function buildScheduleDiagnosticsSummary(evaluations = []) {
  const due = evaluations.filter((row) => row.inventory?.due);
  return {
    sessionsEvaluated: evaluations.length,
    watchedDueCount: due.filter((row) => row.watched).length,
    generalDueCount: due.filter((row) => !row.watched).length,
    sample: evaluations.slice(0, 25),
  };
}

module.exports = {
  MS_PER_MINUTE,
  MS_PER_HOUR,
  INVENTORY_TARGET_MINUTES_WATCHED,
  INVENTORY_TARGET_MINUTES_GENERAL,
  PRICE_TARGET_MINUTES,
  HEARTBEAT_TARGET_MINUTES,
  NEAR_TERM_DATE_SCAN_MAX_HOURS_GENERAL,
  NEAR_TERM_DATE_SCAN_MAX_HOURS_WATCHED,
  hoursUntilSessionStart,
  isSessionEnded,
  resolveInventoryTargetAgeMinutes,
  resolvePriceTargetAgeMinutes,
  resolveHeartbeatIntervalMinutes,
  resolveHeartbeatIntervalMs,
  isSessionEligibleForInventorySchedule,
  evaluateInventorySchedule,
  evaluatePriceSchedule,
  evaluateHeartbeatSchedule,
  isDateWithinNearTermDateScan,
  collectDueDateScanCandidates,
  buildScheduleDiagnosticsSummary,
  isDueByTargetAge,
  getThresholdScannedAt,
  getPriceVerifiedAt,
};
