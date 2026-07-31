'use strict';

const THRESHOLD_SCAN_JOB_MODE_DATE = 'date_threshold_write';
const THRESHOLD_SCAN_JOB_MODE_WEEK = 'threshold_week_write_contract';
const THRESHOLD_SCAN_JOB_MODE_APPLY = 'threshold_week_apply_prepared';

function parseDateKey(dateKey) {
  const [y, m, d] = String(dateKey).slice(0, 10).split('-').map(Number);
  return Date.UTC(y, m - 1, d, 12, 0, 0);
}

function daysFromToday(dateKey, todayIso) {
  if (!dateKey || !todayIso) return 999;
  return Math.round((parseDateKey(dateKey) - parseDateKey(todayIso)) / 86_400_000);
}

function jobAnchorIsoDate(job) {
  const dates = job?.results_json?.targetDates || job?.dates || [];
  if (Array.isArray(dates) && dates.length) {
    return [...dates].map(String).sort()[0];
  }
  return job?.results_json?.weekStart || null;
}

function thresholdScanJobNeedsScrapeLock(job) {
  const mode = job?.mode || THRESHOLD_SCAN_JOB_MODE_DATE;
  return mode !== THRESHOLD_SCAN_JOB_MODE_APPLY;
}

function jobTargetIsoDate(job) {
  const dates = job?.dates || job?.results_json?.targetDates || [];
  if (Array.isArray(dates) && dates.length) return String(dates[0]).slice(0, 10);
  return job?.results_json?.targetIsoDate || job?.results_json?.weekStart || null;
}

function getDateScanJobClaimBucket(job, todayIso) {
  const watchedDueCount = Number(job?.results_json?.watchedDueCount ?? 0);
  if (watchedDueCount > 0) return 1;
  const isoDate = jobTargetIsoDate(job);
  const daysAhead = isoDate ? daysFromToday(isoDate, todayIso) : 999;
  if (daysAhead <= 3) return 2;
  if (daysAhead <= 7) return 3;
  if (daysAhead <= 14) return 4;
  return 5;
}

function getThresholdScanJobPriorityBucket(job, todayIso) {
  const mode = job?.mode || THRESHOLD_SCAN_JOB_MODE_DATE;
  if (mode === THRESHOLD_SCAN_JOB_MODE_APPLY) return 0;
  if (mode === THRESHOLD_SCAN_JOB_MODE_DATE) {
    return getDateScanJobClaimBucket(job, todayIso);
  }
  const anchor = jobAnchorIsoDate(job);
  const daysAhead = anchor ? daysFromToday(anchor, todayIso) : 999;
  return daysAhead <= 21 ? 6 : 7;
}

function compareThresholdScanJobPriority(a, b, todayIso) {
  const bucketA = getThresholdScanJobPriorityBucket(a, todayIso);
  const bucketB = getThresholdScanJobPriorityBucket(b, todayIso);
  if (bucketA !== bucketB) return bucketA - bucketB;

  const anchorA = jobTargetIsoDate(a) || jobAnchorIsoDate(a) || '';
  const anchorB = jobTargetIsoDate(b) || jobAnchorIsoDate(b) || '';
  const daysA = anchorA ? daysFromToday(anchorA, todayIso) : 999;
  const daysB = anchorB ? daysFromToday(anchorB, todayIso) : 999;
  if (daysA !== daysB) return daysA - daysB;

  return String(a.created_at || '').localeCompare(String(b.created_at || ''));
}

function isQueuedThresholdJobEligibleByMode(job) {
  const mode = job?.mode || THRESHOLD_SCAN_JOB_MODE_DATE;
  return mode === THRESHOLD_SCAN_JOB_MODE_DATE
    || mode === THRESHOLD_SCAN_JOB_MODE_WEEK
    || mode === THRESHOLD_SCAN_JOB_MODE_APPLY;
}

function evaluateQueuedJobModeEligibility(job) {
  if (!isQueuedThresholdJobEligibleByMode(job)) {
    return { modeEligible: false, modeSkipReason: 'unsupported_mode' };
  }
  return { modeEligible: true, modeSkipReason: null };
}

function evaluateQueuedJobClaimSkipReason(job, {
  runningCount = 0,
  scrapeLockAvailable = true,
} = {}) {
  const mode = evaluateQueuedJobModeEligibility(job);
  if (!mode.modeEligible) {
    return { eligible: false, skipReason: mode.modeSkipReason, modeEligible: false };
  }
  if ((runningCount || 0) > 0) {
    return { eligible: false, skipReason: 'job_already_running', modeEligible: true };
  }
  if (thresholdScanJobNeedsScrapeLock(job) && !scrapeLockAvailable) {
    return { eligible: false, skipReason: 'scrape_lock_unavailable', modeEligible: true };
  }
  return { eligible: true, skipReason: null, modeEligible: true };
}

function countSkipReasons(entries = []) {
  const skipReasonsByCode = {};
  for (const entry of entries) {
    if (!entry?.skipReason) continue;
    skipReasonsByCode[entry.skipReason] = (skipReasonsByCode[entry.skipReason] || 0) + 1;
  }
  return skipReasonsByCode;
}

function analyzeThresholdWorkerQueue({
  queuedJobs = [],
  runningCount = 0,
  scrapeLockHeld = false,
  scrapeLockAgeMs = null,
  todayIso,
} = {}) {
  const queuedTotal = queuedJobs.length;
  const scrapeLockAvailable = !scrapeLockHeld;
  const perJob = queuedJobs.map((job) => {
    const evaluation = evaluateQueuedJobClaimSkipReason(job, {
      runningCount,
      scrapeLockAvailable,
    });
    return {
      jobId: job.id,
      mode: job.mode || THRESHOLD_SCAN_JOB_MODE_DATE,
      dryRun: job.dry_run !== false,
      writeEnabled: job.write_enabled === true,
      createdAt: job.created_at || null,
      weekStart: job.results_json?.weekStart ?? null,
      modeEligible: evaluation.modeEligible === true,
      eligible: evaluation.eligible,
      skipReason: evaluation.skipReason,
      needsScrapeLock: thresholdScanJobNeedsScrapeLock(job),
    };
  });

  const sorted = [...queuedJobs].sort((a, b) => compareThresholdScanJobPriority(a, b, todayIso));
  const sortedEvaluations = sorted.map((job) => {
    const row = perJob.find((entry) => entry.jobId === job.id);
    return row || {
      jobId: job.id,
      mode: job.mode,
      eligible: false,
      skipReason: 'unknown',
      needsScrapeLock: thresholdScanJobNeedsScrapeLock(job),
    };
  });

  const queuedModeEligibleCount = perJob.filter((row) => row.modeEligible).length;
  const queuedEligibleCount = perJob.filter((row) => row.eligible).length;
  const skippedCount = perJob.filter((row) => !row.eligible).length;
  const skipReasonsByCode = countSkipReasons(perJob);

  let primaryBlocker = null;
  if (queuedTotal === 0) {
    primaryBlocker = 'no_queued_jobs';
  } else if ((runningCount || 0) > 0) {
    primaryBlocker = 'job_already_running';
  } else {
    const firstSorted = sortedEvaluations[0] || null;
    if (firstSorted && !firstSorted.eligible) {
      primaryBlocker = firstSorted.skipReason || 'scrape_lock_unavailable';
    } else if (queuedEligibleCount === 0) {
      primaryBlocker = 'no_eligible_jobs';
    }
  }

  const nextClaimCandidate = sortedEvaluations[0] || null;
  const adminWarnings = [];
  if (queuedModeEligibleCount > 0 && queuedEligibleCount === 0 && primaryBlocker !== 'no_queued_jobs') {
    adminWarnings.push({
      code: 'eligible_queued_jobs_unclaimed',
      queuedModeEligibleCount,
      queuedEligibleCount,
      primaryBlocker,
      message: `${queuedModeEligibleCount} queued threshold job(s) are eligible by mode but remain unclaimed (${primaryBlocker})`,
    });
  } else if (queuedTotal > 0 && queuedModeEligibleCount === 0) {
    adminWarnings.push({
      code: 'queued_jobs_ineligible',
      queuedTotal,
      primaryBlocker: primaryBlocker || 'no_eligible_jobs',
      message: `${queuedTotal} queued threshold job(s) are present but none are eligible by mode`,
    });
  }

  return {
    queuedTotal,
    queuedModeEligibleCount,
    queuedEligibleCount,
    skippedCount,
    skipReasonsByCode,
    runningJobCount: runningCount || 0,
    scrapeLockHeld: scrapeLockHeld === true,
    scrapeLockAvailable,
    scrapeLockAgeMs,
    primaryBlocker,
    nextClaimCandidate,
    queuedJobs: perJob,
    sortedClaimOrder: sortedEvaluations.map((row) => row.jobId),
    adminWarnings,
  };
}

function buildThresholdWorkerClaimResult({
  claimed = null,
  reason = null,
  lockAcquired = false,
  queueAnalysis = null,
  claimedCount = null,
} = {}) {
  const diagnostics = queueAnalysis ? { ...queueAnalysis } : null;
  if (diagnostics) {
    diagnostics.claimedCount = claimedCount != null ? claimedCount : (claimed ? 1 : 0);
    if (diagnostics.queuedModeEligibleCount > 0 && !claimed) {
      if (!diagnostics.adminWarnings.some((w) => w.code === 'eligible_queued_jobs_unclaimed')) {
        diagnostics.adminWarnings.push({
          code: 'eligible_queued_jobs_unclaimed',
          queuedModeEligibleCount: diagnostics.queuedModeEligibleCount,
          queuedEligibleCount: diagnostics.queuedEligibleCount,
          primaryBlocker: reason || diagnostics.primaryBlocker || 'unknown',
          message: `${diagnostics.queuedModeEligibleCount} queued threshold job(s) are eligible by mode but remain unclaimed (${reason || diagnostics.primaryBlocker || 'unknown'})`,
        });
      }
    }
  }
  return {
    claimed,
    reason,
    lockAcquired,
    diagnostics,
  };
}

module.exports = {
  THRESHOLD_SCAN_JOB_MODE_DATE,
  THRESHOLD_SCAN_JOB_MODE_WEEK,
  THRESHOLD_SCAN_JOB_MODE_APPLY,
  parseDateKey,
  daysFromToday,
  jobAnchorIsoDate,
  jobTargetIsoDate,
  getDateScanJobClaimBucket,
  thresholdScanJobNeedsScrapeLock,
  getThresholdScanJobPriorityBucket,
  compareThresholdScanJobPriority,
  isQueuedThresholdJobEligibleByMode,
  evaluateQueuedJobModeEligibility,
  evaluateQueuedJobClaimSkipReason,
  analyzeThresholdWorkerQueue,
  buildThresholdWorkerClaimResult,
};
