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

const FAR_GENERAL_SCAN_BUDGET_PER_HOUR = 1;
const FAR_GENERAL_SCAN_BUDGET_MS = 60 * 60 * 1000;
const FUTURE_HORIZON_FAIRNESS_WINDOW_MS = 60 * 60 * 1000;
const BEYOND_72_HOURS = 72;
const GENERAL_SCAN_ADMISSION_WINDOW_MINUTES = 15;
const GENERAL_SCAN_ADMISSION_MAX_PER_WINDOW = 1;
const GENERAL_SCAN_ADMISSION_WINDOW_MS = GENERAL_SCAN_ADMISSION_WINDOW_MINUTES * 60 * 1000;

function resolveHorizonSelectionBand(bucket, { daysFromTodayFn, todayIso }) {
  const hours = bucket.earliestHoursUntilStart;
  if (hours != null && Number.isFinite(hours)) {
    if (hours <= BEYOND_72_HOURS) return '<=72h';
    if (hours <= 168) return '72-168h';
    if (hours <= 14 * 24) return '8-14d';
    return '>14d';
  }
  const daysAhead = daysFromTodayFn(bucket.isoDate, todayIso);
  if (!Number.isFinite(daysAhead)) return '>14d';
  if (daysAhead <= 3) return '<=72h';
  if (daysAhead <= 7) return '72-168h';
  if (daysAhead <= 14) return '8-14d';
  return '>14d';
}

function isBeyond72HourGeneralBucket(bucket, { daysFromTodayFn, todayIso }) {
  if (bucket.watchedDueCount > 0) return false;
  const hours = bucket.earliestHoursUntilStart;
  if (hours != null && Number.isFinite(hours)) return hours > BEYOND_72_HOURS;
  const daysAhead = daysFromTodayFn(bucket.isoDate, todayIso);
  return Number.isFinite(daysAhead) && daysAhead > 3;
}

function countOverdueCandidatesByHorizonBand(candidates, { daysFromTodayFn, todayIso }) {
  const counts = {
    '<=72h': 0,
    '72-168h': 0,
    '8-14d': 0,
    '>14d': 0,
  };
  for (const bucket of candidates || []) {
    if (bucket.watchedDueCount > 0) continue;
    const band = resolveHorizonSelectionBand(bucket, { daysFromTodayFn, todayIso });
    if (counts[band] != null) counts[band] += 1;
  }
  return counts;
}

function oldestThresholdVerificationMs(bucket) {
  let oldestMs = null;
  let hasNeverVerified = false;
  for (const row of bucket.sessions || []) {
    const ts = row.lastSuccessfulCheck ?? row.threshold_scanned_at ?? null;
    if (!ts) {
      hasNeverVerified = true;
      continue;
    }
    const ms = new Date(ts).getTime();
    if (!Number.isFinite(ms)) {
      hasNeverVerified = true;
      continue;
    }
    oldestMs = oldestMs == null ? ms : Math.min(oldestMs, ms);
  }
  if (hasNeverVerified) return null;
  return oldestMs;
}

function resolveGeneralPriorityBand(bucket, { daysFromTodayFn, todayIso }) {
  const hours = bucket.earliestHoursUntilStart;
  if (hours != null && Number.isFinite(hours)) {
    if (hours <= 72) return 2;
    if (hours <= 168) return 3;
    if (hours <= 14 * 24) return 4;
    return 5;
  }
  const daysAhead = daysFromTodayFn(bucket.isoDate, todayIso);
  if (!Number.isFinite(daysAhead)) return 5;
  if (daysAhead <= 3) return 2;
  if (daysAhead <= 7) return 3;
  if (daysAhead <= 14) return 4;
  return 5;
}

function pickRepresentativeSession(bucket) {
  return (bucket.sessions || []).reduce((best, row) => {
    const overdue = row.actualFreshnessMinutes == null
      ? row.targetFreshnessMinutes
      : Math.max(0, row.actualFreshnessMinutes - row.targetFreshnessMinutes);
    if (!best) return row;
    const bestOverdue = best.actualFreshnessMinutes == null
      ? best.targetFreshnessMinutes
      : Math.max(0, best.actualFreshnessMinutes - best.targetFreshnessMinutes);
    return overdue > bestOverdue ? row : best;
  }, null);
}

function computeDateCandidatePriority(bucket, { daysFromTodayFn, todayIso }) {
  const watched = bucket.watchedDueCount > 0;
  const representative = pickRepresentativeSession(bucket);
  const freshness = freshnessFromRepresentative(representative);
  const generalPriorityBand = watched ? 1 : resolveGeneralPriorityBand(bucket, { daysFromTodayFn, todayIso });
  return {
    watchedTier: watched ? 0 : 1,
    generalPriorityBand,
    neverVerified: freshness.neverVerified,
    normalizedOverdueRatio: freshness.normalizedOverdueRatio,
    boundedOverdueMinutes: Math.min(bucket.mostOverdueMinutes || 0, MAX_OVERDUE_PRIORITY_MINUTES),
    sortKey: [
      watched ? 0 : 1,
      generalPriorityBand,
      -internalOverdueSortScore(freshness),
      bucket.isoDate,
    ],
  };
}

function isFarGeneralDueCandidate(bucket, { daysFromTodayFn, todayIso }) {
  if (bucket.watchedDueCount > 0) return false;
  const hours = bucket.earliestHoursUntilStart;
  if (hours != null && Number.isFinite(hours)) return hours > 168;
  const daysAhead = daysFromTodayFn(bucket.isoDate, todayIso);
  return Number.isFinite(daysAhead) && daysAhead > 7;
}

function evaluateGeneralScanAdmission({
  recentGeneralEnqueueCount = 0,
  lastGeneralEnqueueAt = null,
  nowMs = Date.now(),
  windowMs = GENERAL_SCAN_ADMISSION_WINDOW_MS,
  budgetPerWindow = GENERAL_SCAN_ADMISSION_MAX_PER_WINDOW,
} = {}) {
  if (recentGeneralEnqueueCount < budgetPerWindow) {
    return {
      allowed: true,
      recentGeneralEnqueueCount,
      lastGeneralEnqueueAt,
      nextEligibleGeneralScanAt: null,
      reason: null,
      windowMinutes: GENERAL_SCAN_ADMISSION_WINDOW_MINUTES,
      maxPerWindow: GENERAL_SCAN_ADMISSION_MAX_PER_WINDOW,
    };
  }
  const lastMs = lastGeneralEnqueueAt ? new Date(lastGeneralEnqueueAt).getTime() : nowMs;
  return {
    allowed: false,
    recentGeneralEnqueueCount,
    lastGeneralEnqueueAt,
    nextEligibleGeneralScanAt: new Date(lastMs + windowMs).toISOString(),
    reason: 'general_scan_budget_exhausted',
    windowMinutes: GENERAL_SCAN_ADMISSION_WINDOW_MINUTES,
    maxPerWindow: GENERAL_SCAN_ADMISSION_MAX_PER_WINDOW,
  };
}

function evaluateFutureHorizonFairness({
  overdueBeyond72Count = 0,
  recentBeyond72GeneralScanCount = 0,
  lastBeyond72GeneralScanAt = null,
  nowMs = Date.now(),
  windowMs = FUTURE_HORIZON_FAIRNESS_WINDOW_MS,
} = {}) {
  const needsBeyond72ScanThisWindow = overdueBeyond72Count > 0 && recentBeyond72GeneralScanCount < 1;
  const lastMs = lastBeyond72GeneralScanAt ? new Date(lastBeyond72GeneralScanAt).getTime() : null;
  return {
    overdueBeyond72Count,
    recentBeyond72GeneralScanCount,
    lastBeyond72GeneralScanAt,
    needsBeyond72ScanThisWindow,
    nextFutureFairnessEligibilityAt: needsBeyond72ScanThisWindow
      ? null
      : (lastMs ? new Date(lastMs + windowMs).toISOString() : null),
    windowMinutes: Math.round(windowMs / (60 * 1000)),
    blockedReason: overdueBeyond72Count <= 0
      ? 'no_overdue_beyond_72h'
      : (recentBeyond72GeneralScanCount >= 1 ? 'future_fairness_satisfied_this_window' : null),
  };
}

function sortFutureFairnessCandidates(candidates) {
  return [...(candidates || [])].sort((a, b) => {
    const freshnessA = freshnessFromRepresentative(pickRepresentativeSession(a));
    const freshnessB = freshnessFromRepresentative(pickRepresentativeSession(b));
    const overdueCmp = compareOverdueFreshnessPriority(freshnessA, freshnessB);
    if (overdueCmp !== 0) return overdueCmp;

    const oldestA = oldestThresholdVerificationMs(a);
    const oldestB = oldestThresholdVerificationMs(b);
    if (oldestA == null && oldestB != null) return -1;
    if (oldestA != null && oldestB == null) return 1;
    if (oldestA != null && oldestB != null && oldestA !== oldestB) return oldestA - oldestB;

    return String(a.isoDate).localeCompare(String(b.isoDate));
  });
}

function trySelectFirstEligible(sorted, {
  daysFromTodayFn,
  todayIso,
  recentFarGeneralScanCount = 0,
  farGeneralScanBudget = FAR_GENERAL_SCAN_BUDGET_PER_HOUR,
  generalScanAdmission = null,
} = {}) {
  let deferredFarCandidate = null;
  let deferredGeneralAdmissionCandidate = null;
  for (const candidate of sorted) {
    const isGeneral = candidate.watchedDueCount <= 0;
    if (isGeneral && generalScanAdmission?.allowed === false) {
      if (!deferredGeneralAdmissionCandidate) deferredGeneralAdmissionCandidate = candidate;
      continue;
    }
    if (isFarGeneralDueCandidate(candidate, { daysFromTodayFn, todayIso })
      && recentFarGeneralScanCount >= farGeneralScanBudget) {
      if (!deferredFarCandidate) deferredFarCandidate = candidate;
      continue;
    }
    return {
      selected: candidate,
      deferredFarCandidate,
      deferredGeneralAdmissionCandidate,
      deferredReason: null,
    };
  }
  return {
    selected: null,
    deferredFarCandidate,
    deferredGeneralAdmissionCandidate,
    deferredReason: deferredGeneralAdmissionCandidate
      ? (generalScanAdmission?.reason || 'general_scan_budget_exhausted')
      : (deferredFarCandidate ? 'far_general_hourly_budget_exhausted' : null),
  };
}

function resolveSelectionPath(selected, { daysFromTodayFn, todayIso }) {
  if (!selected) return null;
  if (selected.watchedDueCount > 0) return 'watched_priority';
  return 'near_term_priority';
}

function candidatePassesGeneralGuards(candidate, {
  daysFromTodayFn,
  todayIso,
  recentFarGeneralScanCount = 0,
  farGeneralScanBudget = FAR_GENERAL_SCAN_BUDGET_PER_HOUR,
  generalScanAdmission = null,
} = {}) {
  const isGeneral = candidate.watchedDueCount <= 0;
  if (isGeneral && generalScanAdmission?.allowed === false) return false;
  if (isFarGeneralDueCandidate(candidate, { daysFromTodayFn, todayIso })
    && recentFarGeneralScanCount >= farGeneralScanBudget) {
    return false;
  }
  return true;
}

function selectDueDateScanCandidate(candidates, {
  daysFromTodayFn,
  todayIso,
  recentFarGeneralScanCount = 0,
  farGeneralScanBudget = FAR_GENERAL_SCAN_BUDGET_PER_HOUR,
  generalScanAdmission = null,
  recentBeyond72GeneralScanCount = 0,
  lastBeyond72GeneralScanAt = null,
  nowMs = Date.now(),
} = {}) {
  const sorted = sortDateScanCandidates(candidates, { daysFromTodayFn, todayIso });
  const overdueBeyond72 = sorted.filter((candidate) => isBeyond72HourGeneralBucket(
    candidate,
    { daysFromTodayFn, todayIso },
  ));
  const overdueByHorizonBand = countOverdueCandidatesByHorizonBand(candidates, {
    daysFromTodayFn,
    todayIso,
  });
  const futureFairness = evaluateFutureHorizonFairness({
    overdueBeyond72Count: overdueBeyond72.length,
    recentBeyond72GeneralScanCount,
    lastBeyond72GeneralScanAt,
    nowMs,
  });

  const normal = trySelectFirstEligible(sorted, {
    daysFromTodayFn,
    todayIso,
    recentFarGeneralScanCount,
    farGeneralScanBudget,
    generalScanAdmission,
  });

  const guardOptions = {
    daysFromTodayFn,
    todayIso,
    recentFarGeneralScanCount,
    farGeneralScanBudget,
    generalScanAdmission,
  };

  if (normal.selected
    && isBeyond72HourGeneralBucket(normal.selected, { daysFromTodayFn, todayIso })) {
    return {
      ...normal,
      selectionPath: resolveSelectionPath(normal.selected, { daysFromTodayFn, todayIso }),
      futureFairness: {
        ...futureFairness,
        applied: false,
        satisfiedByNormalSelection: true,
      },
      overdueByHorizonBand,
    };
  }

  const shouldApplyFutureFairness = futureFairness.needsBeyond72ScanThisWindow
    && overdueBeyond72.length > 0
    && (
      !normal.selected
      || (normal.selected.watchedDueCount <= 0
        && resolveGeneralPriorityBand(normal.selected, { daysFromTodayFn, todayIso }) === 2)
    );

  if (shouldApplyFutureFairness) {
    for (const candidate of sortFutureFairnessCandidates(overdueBeyond72)) {
      if (!candidatePassesGeneralGuards(candidate, guardOptions)) continue;
      return {
        selected: candidate,
        deferredFarCandidate: normal.deferredFarCandidate,
        deferredGeneralAdmissionCandidate: normal.deferredGeneralAdmissionCandidate,
        deferredReason: null,
        selectionPath: 'future_horizon_fairness',
        futureFairness: {
          ...futureFairness,
          applied: true,
          blockedReason: null,
        },
        overdueByHorizonBand,
      };
    }
    return {
      ...normal,
      selectionPath: resolveSelectionPath(normal.selected, { daysFromTodayFn, todayIso }),
      futureFairness: {
        ...futureFairness,
        applied: false,
        blockedReason: normal.deferredGeneralAdmissionCandidate
          ? (generalScanAdmission?.reason || 'general_scan_budget_exhausted')
          : (normal.deferredFarCandidate ? 'far_general_hourly_budget_exhausted' : 'future_fairness_candidates_blocked'),
      },
      overdueByHorizonBand,
    };
  }

  return {
    ...normal,
    selectionPath: resolveSelectionPath(normal.selected, { daysFromTodayFn, todayIso }),
    futureFairness: {
      ...futureFairness,
      applied: false,
      satisfiedByNormalSelection: false,
    },
    overdueByHorizonBand,
  };
}

function evaluateOverdueFreshness({ targetFreshnessMinutes, actualFreshnessMinutes } = {}) {
  const target = Number(targetFreshnessMinutes);
  if (!Number.isFinite(target) || target <= 0) {
    return { neverVerified: false, normalizedOverdueRatio: 1 };
  }
  if (actualFreshnessMinutes == null) {
    return { neverVerified: true, normalizedOverdueRatio: null };
  }
  const actual = Number(actualFreshnessMinutes);
  if (!Number.isFinite(actual)) {
    return { neverVerified: true, normalizedOverdueRatio: null };
  }
  const overdue = Math.max(0, actual - target);
  return {
    neverVerified: false,
    normalizedOverdueRatio: overdue / target,
  };
}

function internalOverdueSortScore(freshness) {
  if (freshness?.neverVerified) return Number.MAX_SAFE_INTEGER;
  const ratio = Number(freshness?.normalizedOverdueRatio);
  return Number.isFinite(ratio) ? ratio : 0;
}

function compareOverdueFreshnessPriority(freshnessA, freshnessB) {
  const neverA = freshnessA?.neverVerified === true;
  const neverB = freshnessB?.neverVerified === true;
  if (neverA !== neverB) return neverA ? -1 : 1;
  const ratioA = internalOverdueSortScore(freshnessA);
  const ratioB = internalOverdueSortScore(freshnessB);
  if (ratioA !== ratioB) return ratioB - ratioA;
  return 0;
}

function computeNormalizedOverdueRatio({ targetFreshnessMinutes, actualFreshnessMinutes } = {}) {
  return evaluateOverdueFreshness({ targetFreshnessMinutes, actualFreshnessMinutes }).normalizedOverdueRatio;
}

function freshnessFromRepresentative(representative) {
  return evaluateOverdueFreshness({
    targetFreshnessMinutes: representative?.targetFreshnessMinutes,
    actualFreshnessMinutes: representative?.actualFreshnessMinutes,
  });
}

function sortDateScanCandidates(candidates, { daysFromTodayFn, todayIso }) {
  return [...(candidates || [])].sort((a, b) => {
    const pa = computeDateCandidatePriority(a, { daysFromTodayFn, todayIso });
    const pb = computeDateCandidatePriority(b, { daysFromTodayFn, todayIso });
    for (let i = 0; i < pa.sortKey.length - 1; i += 1) {
      if (pa.sortKey[i] !== pb.sortKey[i]) return pa.sortKey[i] - pb.sortKey[i];
    }
    return String(pa.sortKey[pa.sortKey.length - 1]).localeCompare(String(pb.sortKey[pb.sortKey.length - 1]));
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
  FAR_GENERAL_SCAN_BUDGET_PER_HOUR,
  FAR_GENERAL_SCAN_BUDGET_MS,
  FUTURE_HORIZON_FAIRNESS_WINDOW_MS,
  BEYOND_72_HOURS,
  GENERAL_SCAN_ADMISSION_WINDOW_MINUTES,
  GENERAL_SCAN_ADMISSION_MAX_PER_WINDOW,
  GENERAL_SCAN_ADMISSION_WINDOW_MS,
  evaluateGeneralScanAdmission,
  evaluateFutureHorizonFairness,
  evaluateOverdueFreshness,
  compareOverdueFreshnessPriority,
  internalOverdueSortScore,
  resolveHorizonSelectionBand,
  isBeyond72HourGeneralBucket,
  countOverdueCandidatesByHorizonBand,
  sortFutureFairnessCandidates,
  oldestThresholdVerificationMs,
  resolveGeneralPriorityBand,
  isFarGeneralDueCandidate,
  selectDueDateScanCandidate,
  trySelectFirstEligible,
  candidatePassesGeneralGuards,
  resolveSelectionPath,
  computeDateCandidatePriority,
  computeNormalizedOverdueRatio,
  sortDateScanCandidates,
  createNearTermSchedulerState,
  recordNearTermTick,
  buildNearTermSchedulerDiagnostics,
  buildDatePipelineDiagnostics,
  buildBacklogHealthDiagnostics,
  summarizeNearTermTickResult,
};
