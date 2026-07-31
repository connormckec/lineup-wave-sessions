'use strict';

function countTilesForIsoDate(tiles, isoDate) {
  return (tiles || []).filter((tile) => tile?.isoDate === isoDate).length;
}

function filterInferencesForIsoDate(inferences, isoDate) {
  return (inferences || []).filter((row) => {
    const key = row?.identityKey || '';
    const parts = key.split('|');
    return parts[0] === isoDate;
  });
}

function validatePreparedUpdatesForTargetDate(preparedRows, targetIsoDate) {
  const rows = preparedRows || [];
  const wrongDateRows = rows.filter((row) => {
    const iso = row?.isoDate || row?.iso_date || row?.thresholdDiagnostics?.identityKey?.split('|')?.[0];
    return iso && iso !== targetIsoDate;
  });
  if (wrongDateRows.length) {
    return { ok: false, failureReason: 'target_date_identity_mismatch', wrongDateCount: wrongDateRows.length };
  }
  return { ok: true, failureReason: null, wrongDateCount: 0 };
}

/**
 * Positive evidence contract before threshold inference results may advance verification.
 */
function validateTargetDateScanEvidence({
  targetIsoDate,
  visibleHeaders = [],
  tiles = [],
  inferences = [],
  preparedRows = [],
  sessionsOnTargetDate = null,
} = {}) {
  if (!targetIsoDate) {
    return { ok: false, failureReason: 'target_date_not_visible' };
  }
  const headers = visibleHeaders || [];
  if (!headers.includes(targetIsoDate)) {
    return { ok: false, failureReason: 'target_date_not_visible' };
  }

  const targetTiles = countTilesForIsoDate(tiles, targetIsoDate);
  const targetInferences = filterInferencesForIsoDate(inferences, targetIsoDate);
  const foreignInferences = (inferences || []).filter((row) => {
    const iso = row?.identityKey?.split('|')?.[0];
    return iso && iso !== targetIsoDate;
  });

  if (foreignInferences.length && !targetInferences.length) {
    return { ok: false, failureReason: 'target_date_identity_mismatch', targetDateTileCount: targetTiles };
  }

  if (targetTiles === 0 && sessionsOnTargetDate > 0) {
    return { ok: false, failureReason: 'target_date_tiles_not_found', targetDateTileCount: 0 };
  }

  if (preparedRows?.length) {
    const prepCheck = validatePreparedUpdatesForTargetDate(preparedRows, targetIsoDate);
    if (!prepCheck.ok) return { ...prepCheck, targetDateTileCount: targetTiles };
  }

  return {
    ok: true,
    failureReason: null,
    targetDateTileCount: targetTiles,
    parsedTargetDateSessionCount: targetInferences.length,
  };
}

module.exports = {
  countTilesForIsoDate,
  filterInferencesForIsoDate,
  validatePreparedUpdatesForTargetDate,
  validateTargetDateScanEvidence,
};
