'use strict';

const THRESHOLD_SCAN_JOB_MODE_DATE = 'date_threshold_write';
const THRESHOLD_SCAN_JOB_MODE_WEEK = 'threshold_week_write_contract';
const THRESHOLD_SCAN_JOB_MODE_APPLY = 'threshold_week_apply_prepared';

const MAX_OVERDUE_PRIORITY_MINUTES = 180;
const NEAR_TERM_TICK_INTERVAL_MS = 5 * 60 * 1000;

function resolvePreparedZeroReason({
  fullScanContractOk = false,
  exactInferenceCount = 0,
  preparedUpdatesCount = 0,
  dateResults = [],
} = {}) {
  if (fullScanContractOk !== true) return null;
  if (preparedUpdatesCount > 0) return null;
  if (exactInferenceCount === 0) return 'no_exact_inferences';
  const ambiguityCount = dateResults.reduce(
    (sum, row) => sum + (row.atLeastCount || 0) + (row.noMatchCount || 0),
    0,
  );
  if (ambiguityCount > 0 && exactInferenceCount === 0) return 'ambiguous_only';
  return 'no_session_matches';
}

function resolvePreparedUpdatesCountFromResults(resultsJson = {}) {
  const explicit = resultsJson.preparedUpdatesCount;
  if (explicit != null && Number.isFinite(Number(explicit))) {
    return Number(explicit);
  }
  const byDate = resultsJson.preparedUpdatesByDate;
  if (byDate && typeof byDate === 'object') {
    return Object.values(byDate).reduce((sum, rows) => sum + (rows?.length || 0), 0);
  }
  if (Array.isArray(resultsJson.preparedUpdates)) {
    return resultsJson.preparedUpdates.length;
  }
  return null;
}

function validateDateScanResultContract(resultsJson = {}) {
  const errors = [];
  if (resultsJson.mode !== THRESHOLD_SCAN_JOB_MODE_DATE) {
    errors.push('mode_not_date');
  }
  if (resultsJson.stage == null) errors.push('stage_null');
  if (resultsJson.fullScanContractOk == null) errors.push('fullScanContractOk_null');
  if (resultsJson.preparedUpdatesCount == null) errors.push('preparedUpdatesCount_null');
  if (resultsJson.fullScanContractOk === true && !resultsJson.preparedScanCompletedAt) {
    errors.push('preparedScanCompletedAt_missing');
  }
  if (!resultsJson.targetIsoDate && !(resultsJson.targetDates || []).length) {
    errors.push('targetIsoDate_missing');
  }
  return { ok: errors.length === 0, errors };
}

function isDateScanOperationallyComplete(resultsJson = {}) {
  const validation = validateDateScanResultContract(resultsJson);
  if (!validation.ok) return false;
  if (resultsJson.fullScanContractOk !== true) return false;
  return true;
}

function isDryScanReadyToApply(resultsJson = {}, { requirePreparedRows = true } = {}) {
  if (resultsJson.fullScanContractOk !== true) return false;
  if (resultsJson.error || resultsJson.resultsError) return false;
  const preparedUpdatesCount = resolvePreparedUpdatesCountFromResults(resultsJson);
  if (preparedUpdatesCount == null) return false;
  if (requirePreparedRows && preparedUpdatesCount <= 0) return false;
  if (!resultsJson.preparedScanCompletedAt) return false;
  return true;
}

function computeDateCandidatePriority(bucket, { daysFromTodayFn, todayIso }) {
  const watched = bucket.watchedDueCount > 0;
  const daysAhead = daysFromTodayFn(bucket.isoDate, todayIso);
  const futureDays = Number.isFinite(daysAhead) ? daysAhead : 999;
  const boundedOverdue = Math.min(bucket.mostOverdueMinutes || 0, MAX_OVERDUE_PRIORITY_MINUTES);
  let proximityScore;
  if (watched) {
    proximityScore = futureDays < 0 ? Math.abs(futureDays) : futureDays;
  } else if (futureDays < 0) {
    proximityScore = 500 + Math.abs(futureDays);
  } else {
    proximityScore = futureDays;
  }
  return {
    watchedTier: watched ? 0 : 1,
    proximityScore,
    boundedOverdueMinutes: boundedOverdue,
    sortKey: [
      watched ? 0 : 1,
      proximityScore,
      -boundedOverdue,
      bucket.isoDate,
    ],
  };
}

function sortDateScanCandidates(candidates, { daysFromTodayFn, todayIso }) {
  return [...(candidates || [])].sort((a, b) => {
    const pa = computeDateCandidatePriority(a, { daysFromTodayFn, todayIso });
    const pb = computeDateCandidatePriority(b, { daysFromTodayFn, todayIso });
    for (let i = 0; i < pa.sortKey.length; i += 1) {
      if (pa.sortKey[i] !== pb.sortKey[i]) return pa.sortKey[i] - pb.sortKey[i];
    }
    return 0;
  });
}

function summarizeNearTermTickResult(result = {}) {
  return {
    ok: result.ok !== false,
    action: result.action || null,
    reason: result.reason || null,
    pass: result.pass || null,
    job_id: result.job_id || result.jobId || null,
    sourceJobId: result.sourceJobId || null,
    isoDate: result.isoDate || result.selectedIsoDate || null,
  };
}

function createNearTermSchedulerState() {
  return {
    lastNearTermTickAt: null,
    lastNearTermTickSource: 'unknown',
    lastNearTermTickResult: null,
    consecutiveMissedNearTermTicks: 0,
    expectedIntervalMs: NEAR_TERM_TICK_INTERVAL_MS,
  };
}

function recordNearTermTick(state, result, source = 'unknown') {
  if (!state) return;
  state.lastNearTermTickAt = new Date().toISOString();
  state.lastNearTermTickSource = source;
  state.lastNearTermTickResult = summarizeNearTermTickResult(result);
  state.consecutiveMissedNearTermTicks = 0;
}

function buildNearTermSchedulerDiagnostics(state, {
  inProcessMaintenanceSchedulerEnabled = false,
  inlineThresholdWorkerEnabled = false,
} = {}) {
  const now = Date.now();
  const lastAt = state?.lastNearTermTickAt ? new Date(state.lastNearTermTickAt).getTime() : null;
  const intervalMs = state?.expectedIntervalMs || NEAR_TERM_TICK_INTERVAL_MS;
  let consecutiveMissed = state?.consecutiveMissedNearTermTicks || 0;
  if (lastAt && now - lastAt > intervalMs * 1.5) {
    consecutiveMissed = Math.max(consecutiveMissed, Math.floor((now - lastAt) / intervalMs) - 1);
  }
  const ownership = inProcessMaintenanceSchedulerEnabled ? 'in_process' : 'external_cron';
  return {
    enabled: true,
    ownership,
    inProcessMaintenanceSchedulerEnabled,
    inlineThresholdWorkerEnabled,
    expectedExternalNearTermCron: 'POST /api/admin/maintenance/near-term-tick every 5 minutes',
    lastNearTermTickAt: state?.lastNearTermTickAt || null,
    lastNearTermTickSource: state?.lastNearTermTickSource || 'unknown',
    lastNearTermTickResult: state?.lastNearTermTickResult || null,
    nextExpectedNearTermTickAt: lastAt
      ? new Date(lastAt + intervalMs).toISOString()
      : null,
    consecutiveMissedNearTermTicks: consecutiveMissed,
    dualSchedulerRisk: inProcessMaintenanceSchedulerEnabled,
  };
}

function buildDatePipelineDiagnostics({
  latestDateJob = null,
  applyJob = null,
  readyToApply = false,
} = {}) {
  const resultsJson = latestDateJob?.results_json || {};
  const applyResults = applyJob?.results_json || {};
  const createdAt = latestDateJob?.created_at ? new Date(latestDateJob.created_at).getTime() : null;
  const completedAt = latestDateJob?.completed_at ? new Date(latestDateJob.completed_at).getTime() : null;
  return {
    latestDateScanJobId: latestDateJob?.id || null,
    targetIsoDate: resultsJson.targetIsoDate || resultsJson.targetDates?.[0] || latestDateJob?.dates?.[0] || null,
    status: latestDateJob?.status || null,
    stage: resultsJson.stage ?? null,
    exactInferenceCount: resultsJson.exactCount ?? null,
    preparedUpdatesCount: resolvePreparedUpdatesCountFromResults(resultsJson),
    preparedZeroReason: resultsJson.preparedZeroReason ?? null,
    readyToApply,
    applyJobId: applyJob?.id || null,
    applyStatus: applyJob?.status || null,
    trustedRowsWritten: applyResults.rowsWrittenSuccessfully ?? applyResults.rowsWritten ?? null,
    verificationTimestampsAdvanced: applyResults.rowsWrittenSuccessfully ?? applyResults.rowsWritten ?? null,
    totalDurationMs: createdAt && completedAt ? completedAt - createdAt : null,
    fullScanContractOk: resultsJson.fullScanContractOk === true,
    resultsError: resultsJson.resultsError || resultsJson.error || latestDateJob?.error || null,
    operationallyComplete: isDateScanOperationallyComplete(resultsJson),
  };
}

function buildBacklogHealthDiagnostics({
  dueScan = {},
  queuedDateScans = 0,
  runningDateScans = 0,
  readyDateApplies = 0,
  queuedDateApplies = 0,
  runningDateApplies = 0,
  oldestTrustedInventoryAgeMinutes = null,
  oldestWatchedInventoryAgeMinutes = null,
  oldestGeneralNearTermInventoryAgeMinutes = null,
} = {}) {
  const dueDateCount = dueScan.candidates?.length || 0;
  const watchedDueDateCount = dueScan.candidates?.filter((row) => row.watchedDueCount > 0).length || 0;
  const generalDueDateCount = dueDateCount - watchedDueDateCount;
  const dueSessions = (dueScan.watchedDueCount || 0) + (dueScan.generalDueCount || 0);
  const pipelineIdle = queuedDateScans + runningDateScans + queuedDateApplies + runningDateApplies === 0;
  const unhealthy = dueSessions > 0 && pipelineIdle && readyDateApplies === 0;
  return {
    dueDateCount,
    watchedDueDateCount,
    generalDueDateCount,
    dueSessionCount: dueSessions,
    oldestTrustedInventoryAgeMinutes,
    oldestWatchedInventoryAgeMinutes,
    oldestGeneralNearTermInventoryAgeMinutes,
    queuedDateScans,
    runningDateScans,
    readyDateApplies,
    queuedDateApplies,
    runningDateApplies,
    unhealthy,
    unhealthyReason: unhealthy ? 'due_backlog_with_idle_pipeline' : null,
  };
}

module.exports = {
  THRESHOLD_SCAN_JOB_MODE_DATE,
  THRESHOLD_SCAN_JOB_MODE_WEEK,
  THRESHOLD_SCAN_JOB_MODE_APPLY,
  MAX_OVERDUE_PRIORITY_MINUTES,
  NEAR_TERM_TICK_INTERVAL_MS,
  resolvePreparedZeroReason,
  resolvePreparedUpdatesCountFromResults,
  validateDateScanResultContract,
  isDateScanOperationallyComplete,
  isDryScanReadyToApply,
  computeDateCandidatePriority,
  sortDateScanCandidates,
  createNearTermSchedulerState,
  recordNearTermTick,
  buildNearTermSchedulerDiagnostics,
  buildDatePipelineDiagnostics,
  buildBacklogHealthDiagnostics,
  summarizeNearTermTickResult,
};
