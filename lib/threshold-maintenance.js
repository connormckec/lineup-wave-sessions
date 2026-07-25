'use strict';

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
  'available_entries_at_least',
  'slot_status',
  'slot_source',
  'threshold_confidence',
  'threshold_max_visible',
  'threshold_scanned_at',
  'threshold_scan_verified',
  'threshold_scan_at',
  'expectedCapacity',
  'thresholdTrustedSuspendedAt',
];

function thresholdFieldOnSession(s, field) {
  if (!s) return null;
  if (s[field] != null) return s[field];
  if (s.raw && s.raw[field] != null) return s.raw[field];
  return null;
}

function parseThresholdTimestamp(value) {
  if (!value) return null;
  const ts = new Date(value).getTime();
  return Number.isFinite(ts) ? ts : null;
}

function getThresholdScanTimestamp(s) {
  return parseThresholdTimestamp(
    thresholdFieldOnSession(s, 'threshold_scanned_at')
      ?? thresholdFieldOnSession(s, 'thresholdScanAt')
      ?? thresholdFieldOnSession(s, 'threshold_scan_at'),
  );
}

function getThresholdSuspendedTimestamp(s) {
  return parseThresholdTimestamp(thresholdFieldOnSession(s, 'thresholdTrustedSuspendedAt'));
}

function sessionThresholdScanVerified(s) {
  if (!s) return false;
  return s.thresholdScanVerified === true
    || s.raw?.thresholdScanVerified === true
    || s.threshold_scan_verified === true
    || s.raw?.threshold_scan_verified === true;
}

function thresholdConfidenceOnSession(s) {
  return thresholdFieldOnSession(s, 'thresholdConfidence')
    ?? thresholdFieldOnSession(s, 'threshold_confidence')
    ?? thresholdFieldOnSession(s, 'slot_status');
}

function isThresholdSlotsTrusted(s) {
  if (!s || s.available === false) return false;
  if (!sessionThresholdScanVerified(s)) return false;

  const scanTs = getThresholdScanTimestamp(s);
  const suspendedTs = getThresholdSuspendedTimestamp(s);
  if (suspendedTs != null && (scanTs == null || scanTs <= suspendedTs)) return false;

  const slotSource = thresholdFieldOnSession(s, 'slot_source');
  if (slotSource && slotSource !== 'entries_left_threshold_scan') return false;
  const conf = thresholdConfidenceOnSession(s);
  return conf === 'exact' || conf === 'at_least';
}

function isIncomingThresholdScanNewer(existing, incomingScanAtRaw) {
  const existingTs = getThresholdScanTimestamp(existing);
  const incomingTs = parseThresholdTimestamp(incomingScanAtRaw);
  if (incomingTs == null) return false;
  if (existingTs == null) return true;
  return incomingTs >= existingTs;
}

function hasDisplayableTrustedThreshold(s) {
  if (!isThresholdSlotsTrusted(s)) return false;
  return thresholdFieldOnSession(s, 'available_entries') != null
    || thresholdFieldOnSession(s, 'available_entries_at_least') != null
    || thresholdFieldOnSession(s, 'thresholdInferredSlots') != null;
}

function restoreTrustedThresholdFields(existing, merged, { scrapeKind = 'basic' } = {}) {
  if (!existing || !hasDisplayableTrustedThreshold(existing)) return merged;
  if (hasDisplayableTrustedThreshold(merged)) return merged;
  if (scrapeKind !== 'basic' && scrapeKind !== 'detailed') return merged;

  const incomingScanTs = parseThresholdTimestamp(
    thresholdFieldOnSession(merged, 'threshold_scanned_at')
      ?? thresholdFieldOnSession(merged, 'thresholdScanAt')
      ?? thresholdFieldOnSession(merged, 'threshold_scan_at'),
  );
  const existingScanTs = getThresholdScanTimestamp(existing);
  if (incomingScanTs != null && existingScanTs != null && incomingScanTs !== existingScanTs) {
    return merged;
  }

  for (const field of THRESHOLD_SESSION_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(existing, field)) {
      merged[field] = existing[field];
    }
  }
  return merged;
}

function applyPackedThresholdSuspension(existing, merged, nowIso = new Date().toISOString()) {
  if (merged.available === false && existing?.available !== false) {
    merged.thresholdTrustedSuspendedAt = nowIso;
  }
  if (merged.available === true && existing?.available === false) {
    const scanTs = getThresholdScanTimestamp(merged) ?? getThresholdScanTimestamp(existing);
    const suspendedTs = getThresholdSuspendedTimestamp(merged) ?? getThresholdSuspendedTimestamp(existing);
    if (suspendedTs != null && (scanTs == null || scanTs <= suspendedTs)) {
      merged.thresholdTrustedSuspendedAt = suspendedTs;
    }
  }
  return merged;
}

function shouldApplyPreparedThresholdUpdate(existing, prep) {
  const prepScanAt = prep?.threshold_scanned_at ?? prep?.thresholdScanAt ?? prep?.threshold_scan_at ?? null;
  if (!existing) return true;
  return isIncomingThresholdScanNewer(existing, prepScanAt);
}

function applyPreparedThresholdUpdate(session, prep) {
  const entry = { ...session };
  if (!shouldApplyPreparedThresholdUpdate(session, prep)) {
    return entry;
  }

  const confidence = prep.threshold_confidence ?? prep.thresholdConfidence ?? null;
  const scannedAt = prep.threshold_scanned_at ?? prep.thresholdScanAt ?? prep.threshold_scan_at ?? null;
  entry.thresholdInferredSlots = prep.thresholdInferredSlots
    ?? prep.available_entries
    ?? prep.available_entries_at_least
    ?? null;
  entry.thresholdMaxVisible = prep.threshold_max_visible ?? prep.thresholdMaxVisible ?? null;
  entry.thresholdConfidence = confidence;
  entry.thresholdScanVerified = prep.threshold_scan_verified ?? prep.thresholdScanVerified === true;
  entry.thresholdScanAt = scannedAt;
  entry.thresholdScanMaxTested = prep.thresholdScanMaxTested ?? null;
  entry.thresholdScanMethod = prep.thresholdScanMethod ?? 'entries_left_filter';
  entry.thresholdDiagnostics = prep.thresholdDiagnostics ?? null;
  entry.threshold_confidence = confidence;
  entry.threshold_max_visible = entry.thresholdMaxVisible;
  entry.threshold_scanned_at = scannedAt;
  entry.threshold_scan_at = scannedAt;
  entry.threshold_scan_verified = entry.thresholdScanVerified;
  entry.slot_source = prep.slot_source ?? 'entries_left_threshold_scan';
  entry.available_entries = prep.available_entries ?? null;
  entry.available_entries_at_least = prep.available_entries_at_least ?? null;
  entry.slot_status = prep.slot_status ?? confidence ?? null;

  if (entry.available !== false && entry.thresholdScanVerified) {
    entry.thresholdTrustedSuspendedAt = null;
  }

  return entry;
}

function buildApplyDateOutcome({
  isoDate,
  preparedForDate = [],
  rehydrated = { rows: [], missingSessionKeys: [] },
  writeResult = null,
  dateError = null,
  writeMode = null,
}) {
  const rowsPrepared = preparedForDate.length;
  const rowsMatched = rehydrated.rows?.length ?? 0;
  const rowsWritten = writeResult?.rowsWritten ?? 0;
  const missingSessionKeys = rehydrated.missingSessionKeys || [];
  const rowsUnresolved = missingSessionKeys.length;
  const partialApply = rowsUnresolved > 0 && rowsMatched > 0;
  let error = dateError;
  if (!error && rowsUnresolved > 0 && rowsMatched === 0) {
    error = `missing_sessions:${rowsUnresolved}`;
  }

  return {
    isoDate,
    rowsPrepared,
    rowsMatched,
    rowsWritten,
    rowsUnresolved,
    partialApply,
    missingSessionKeysCount: rowsUnresolved,
    unresolvedSessionKeys: missingSessionKeys,
    warning: partialApply ? `partial_missing_sessions:${rowsUnresolved}` : null,
    unmatchedInferenceSample: missingSessionKeys.slice(0, 12).map((sessionKey) => ({ sessionKey })),
    writeMode,
    writesPerformed: writeResult?.writesPerformed === true,
    error,
  };
}

function summarizePartialApplyJob(dateResults = [], preparedUpdatesCount = 0) {
  const rowsPrepared = dateResults.reduce((sum, row) => sum + (row.rowsPrepared || 0), 0);
  const rowsMatched = dateResults.reduce((sum, row) => sum + (row.rowsMatched || 0), 0);
  const rowsWritten = dateResults.reduce((sum, row) => sum + (row.rowsWritten || 0), 0);
  const rowsUnresolved = dateResults.reduce((sum, row) => sum + (row.rowsUnresolved || 0), 0);
  const partialApply = rowsUnresolved > 0;
  const unresolvedByDate = {};
  for (const row of dateResults) {
    if ((row.rowsUnresolved || 0) > 0) {
      unresolvedByDate[row.isoDate] = row.unresolvedSessionKeys || [];
    }
  }
  const failedDates = dateResults.filter((row) => row.error);
  const hardFailures = dateResults.filter((row) => row.error && !row.partialApply && (row.rowsMatched || 0) === 0);

  return {
    preparedUpdatesCount,
    rowsPrepared,
    rowsMatched,
    rowsWritten,
    rowsUnresolved,
    partialApply,
    unresolvedByDate,
    failedDateCount: failedDates.length,
    hardFailureCount: hardFailures.length,
    writesPerformed: rowsWritten > 0,
  };
}

function resolveApplyJobCompletion({ dateResults = [], workerError = null, preparedUpdatesCount = 0 }) {
  const summary = summarizePartialApplyJob(dateResults, preparedUpdatesCount);
  const hardFailures = dateResults.filter((row) => row.error && (row.rowsMatched || 0) === 0);
  const hasHardFailure = Boolean(workerError) || hardFailures.length > 0 || dateResults.length === 0;

  if (hasHardFailure) {
    return {
      ok: false,
      status: 'failed',
      stage: 'failed',
      error: workerError
        || (dateResults.length === 0 ? 'no_date_results' : null)
        || hardFailures[0]?.error
        || 'apply_failed',
      partialApply: summary.partialApply,
      ...summary,
    };
  }

  return {
    ok: true,
    status: 'completed',
    stage: summary.partialApply ? 'completed_partial' : 'completed',
    error: summary.partialApply ? 'partial_apply_unresolved_rows_remain' : null,
    partialApply: summary.partialApply,
    ...summary,
  };
}

function buildApplyPreparedFinalResults({
  dateResults = [],
  workerError = null,
  sourcePreparedUpdatesCount = 0,
  sourceWeekStart = null,
} = {}) {
  const preparedCount = Number(sourcePreparedUpdatesCount) || 0;
  const completion = resolveApplyJobCompletion({
    dateResults,
    workerError,
    preparedUpdatesCount: preparedCount,
  });
  return {
    ...completion,
    weekStart: sourceWeekStart ?? null,
    preparedUpdatesCount: Number(completion.preparedUpdatesCount) || preparedCount,
    rowsPrepared: Number(completion.rowsPrepared) || 0,
    rowsMatched: Number(completion.rowsMatched) || 0,
    rowsWritten: Number(completion.rowsWritten) || 0,
    rowsUnresolved: Number(completion.rowsUnresolved) || 0,
  };
}

function isDryScanSourceFullyApplied(sourceJobId, preparedScanCompletedAt, applyJobs = []) {
  if (!sourceJobId || !preparedScanCompletedAt) return false;
  const scanTs = parseThresholdTimestamp(preparedScanCompletedAt);
  if (scanTs == null) return false;

  const matching = (applyJobs || []).filter((job) => {
    if (job.results_json?.sourceJobId !== sourceJobId) return false;
    if (job.status !== 'completed') return false;
    if (job.error && job.results_json?.partialApply !== true) return false;
    const applyTs = parseThresholdTimestamp(job.completed_at);
    return applyTs != null && applyTs >= scanTs;
  });

  if (!matching.length) return false;
  const latest = matching.sort((a, b) => (
    parseThresholdTimestamp(b.completed_at) - parseThresholdTimestamp(a.completed_at)
  ))[0];
  const resultsJson = latest.results_json || {};
  if (resultsJson.partialApply === true && (resultsJson.rowsUnresolved || 0) > 0) return false;
  return true;
}

function mergeSessionThresholdFields(incoming, existing, { scrapeKind = 'basic', nowIso = new Date().toISOString() } = {}) {
  const merged = { ...(existing || {}), ...incoming };
  const preserveFields = THRESHOLD_SESSION_FIELDS;

  if (scrapeKind === 'basic' && existing) {
    for (const field of preserveFields) {
      if (existing[field] != null && incoming[field] == null) merged[field] = existing[field];
    }
  }

  applyPackedThresholdSuspension(existing, merged, nowIso);
  if (existing) restoreTrustedThresholdFields(existing, merged, { scrapeKind });
  return merged;
}

module.exports = {
  THRESHOLD_SESSION_FIELDS,
  thresholdFieldOnSession,
  parseThresholdTimestamp,
  getThresholdScanTimestamp,
  getThresholdSuspendedTimestamp,
  sessionThresholdScanVerified,
  thresholdConfidenceOnSession,
  isThresholdSlotsTrusted,
  hasDisplayableTrustedThreshold,
  isIncomingThresholdScanNewer,
  restoreTrustedThresholdFields,
  applyPackedThresholdSuspension,
  shouldApplyPreparedThresholdUpdate,
  applyPreparedThresholdUpdate,
  buildApplyDateOutcome,
  summarizePartialApplyJob,
  resolveApplyJobCompletion,
  buildApplyPreparedFinalResults,
  isDryScanSourceFullyApplied,
  mergeSessionThresholdFields,
};
