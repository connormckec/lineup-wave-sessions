'use strict';

const assert = require('assert');
const tm = require('../lib/threshold-maintenance');

function trustedSession(overrides = {}) {
  return {
    key: 'sess-1',
    available: true,
    thresholdScanVerified: true,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    thresholdConfidence: 'exact',
    slot_status: 'exact',
    available_entries: 7,
    thresholdInferredSlots: 7,
    thresholdScanAt: '2026-07-24T18:25:36.000Z',
    threshold_scanned_at: '2026-07-24T18:25:36.000Z',
    ...overrides,
  };
}

function runRestoreTests() {
  console.log('restoreTrustedThresholdFields regression');

  // A. trusted count exists → basic scrape with null threshold fields
  {
    const existing = trustedSession();
    const incoming = {
      key: 'sess-1',
      available: true,
      lastBasicCheckAt: '2026-07-25T15:00:00.000Z',
      thresholdScanVerified: false,
      threshold_scan_verified: false,
      available_entries: null,
      thresholdInferredSlots: null,
    };
    const merged = tm.mergeSessionThresholdFields(incoming, existing, {
      scrapeKind: 'basic',
      nowIso: '2026-07-25T15:00:00.000Z',
    });
    assert.strictEqual(merged.available_entries, 7);
    assert.strictEqual(merged.thresholdScanVerified, true);
    assert.strictEqual(merged.thresholdScanAt, '2026-07-24T18:25:36.000Z');
  }

  // B. trusted count exists → session becomes packed
  {
    const existing = trustedSession();
    const merged = tm.applyPackedThresholdSuspension(existing, {
      ...existing,
      available: false,
      lastBasicCheckAt: '2026-07-25T15:05:00.000Z',
    }, '2026-07-25T15:05:00.000Z');
    assert.strictEqual(merged.available, false);
    assert.ok(merged.thresholdTrustedSuspendedAt);
    assert.strictEqual(tm.isThresholdSlotsTrusted(merged), false);
  }

  // C. packed session later reopens without a new threshold scan
  {
    const existing = trustedSession({
      available: false,
      thresholdTrustedSuspendedAt: '2026-07-25T15:05:00.000Z',
    });
    const merged = tm.applyPackedThresholdSuspension(existing, {
      ...existing,
      available: true,
      lastBasicCheckAt: '2026-07-25T16:00:00.000Z',
    }, '2026-07-25T16:00:00.000Z');
    assert.strictEqual(merged.available, true);
    assert.strictEqual(tm.isThresholdSlotsTrusted(merged), false);
  }

  // D. newer threshold scan replaces the old count
  {
    const existing = trustedSession({ available_entries: 7, thresholdInferredSlots: 7 });
    const incoming = trustedSession({
      available_entries: 3,
      thresholdInferredSlots: 3,
      thresholdScanAt: '2026-07-25T14:46:47.000Z',
      threshold_scanned_at: '2026-07-25T14:46:47.000Z',
    });
    const prep = {
      available_entries: 3,
      thresholdInferredSlots: 3,
      threshold_confidence: 'exact',
      threshold_scan_verified: true,
      threshold_scanned_at: '2026-07-25T14:46:47.000Z',
      slot_source: 'entries_left_threshold_scan',
      slot_status: 'exact',
    };
    assert.strictEqual(tm.shouldApplyPreparedThresholdUpdate(existing, prep), true);
    const applied = tm.applyPreparedThresholdUpdate(existing, prep);
    assert.strictEqual(applied.available_entries, 3);
    assert.strictEqual(applied.thresholdScanAt, '2026-07-25T14:46:47.000Z');
    assert.strictEqual(applied.thresholdTrustedSuspendedAt, null);
  }

  // E. older threshold payload cannot overwrite a newer count
  {
    const existing = trustedSession({
      available_entries: 3,
      thresholdScanAt: '2026-07-25T14:46:47.000Z',
      threshold_scanned_at: '2026-07-25T14:46:47.000Z',
    });
    const prep = {
      available_entries: 7,
      thresholdInferredSlots: 7,
      threshold_confidence: 'exact',
      threshold_scan_verified: true,
      threshold_scanned_at: '2026-07-24T18:25:36.000Z',
      slot_source: 'entries_left_threshold_scan',
      slot_status: 'exact',
    };
    assert.strictEqual(tm.shouldApplyPreparedThresholdUpdate(existing, prep), false);
    const applied = tm.applyPreparedThresholdUpdate(existing, prep);
    assert.strictEqual(applied.available_entries, 3);
    assert.strictEqual(applied.thresholdScanAt, '2026-07-25T14:46:47.000Z');
  }
}

function runApplyTests() {
  console.log('partial apply regression');

  // A. zero matching base sessions
  {
    const outcome = tm.buildApplyDateOutcome({
      isoDate: '2026-08-12',
      preparedForDate: [{ sessionKey: 'a' }, { sessionKey: 'b' }],
      rehydrated: { rows: [], missingSessionKeys: ['a', 'b'] },
      writeMode: 'write',
    });
    assert.strictEqual(outcome.rowsPrepared, 2);
    assert.strictEqual(outcome.rowsMatched, 0);
    assert.strictEqual(outcome.rowsWritten, 0);
    assert.strictEqual(outcome.rowsUnresolved, 2);
    assert.strictEqual(outcome.partialApply, false);
    assert.match(outcome.error, /missing_sessions:2/);
  }

  // B. some matching and some missing
  {
    const outcome = tm.buildApplyDateOutcome({
      isoDate: '2026-08-10',
      preparedForDate: [{ sessionKey: 'a' }, { sessionKey: 'b' }, { sessionKey: 'c' }],
      rehydrated: { rows: [{ key: 'a' }, { key: 'b' }], missingSessionKeys: ['c'] },
      writeResult: { rowsWritten: 2, writesPerformed: true },
      writeMode: 'write',
    });
    assert.strictEqual(outcome.rowsMatched, 2);
    assert.strictEqual(outcome.rowsUnresolved, 1);
    assert.strictEqual(outcome.partialApply, true);
    assert.strictEqual(outcome.error, null);
  }

  // C. all matching
  {
    const completion = tm.resolveApplyJobCompletion({
      dateResults: [{
        isoDate: '2026-08-10',
        rowsPrepared: 2,
        rowsMatched: 2,
        rowsWritten: 2,
        rowsUnresolved: 0,
        partialApply: false,
      }],
      preparedUpdatesCount: 2,
    });
    assert.strictEqual(completion.ok, true);
    assert.strictEqual(completion.partialApply, false);
    assert.strictEqual(completion.status, 'completed');
  }

  // D. retry after missing base sessions later appear
  {
    const partialCompletion = tm.resolveApplyJobCompletion({
      dateResults: [
        {
          isoDate: '2026-08-10',
          rowsPrepared: 2,
          rowsMatched: 2,
          rowsWritten: 2,
          rowsUnresolved: 0,
          partialApply: false,
        },
        {
          isoDate: '2026-08-12',
          rowsPrepared: 2,
          rowsMatched: 0,
          rowsWritten: 0,
          rowsUnresolved: 2,
          partialApply: false,
          error: 'missing_sessions:2',
        },
      ],
      preparedUpdatesCount: 4,
    });
    assert.strictEqual(partialCompletion.ok, false);
    assert.strictEqual(
      tm.isDryScanSourceFullyApplied('source-1', '2026-07-25T14:46:47.000Z', []),
      false,
    );

    const retryCompletion = tm.resolveApplyJobCompletion({
      dateResults: [
        {
          isoDate: '2026-08-10',
          rowsPrepared: 2,
          rowsMatched: 2,
          rowsWritten: 2,
          rowsUnresolved: 0,
          partialApply: false,
        },
        {
          isoDate: '2026-08-12',
          rowsPrepared: 2,
          rowsMatched: 2,
          rowsWritten: 2,
          rowsUnresolved: 0,
          partialApply: false,
        },
      ],
      preparedUpdatesCount: 4,
    });
    assert.strictEqual(retryCompletion.ok, true);
    assert.strictEqual(retryCompletion.partialApply, false);
    assert.strictEqual(
      tm.isDryScanSourceFullyApplied('source-1', '2026-07-25T14:46:47.000Z', [{
        status: 'completed',
        completed_at: '2026-07-25T15:00:00.000Z',
        results_json: {
          sourceJobId: 'source-1',
          partialApply: false,
          rowsUnresolved: 0,
        },
      }]),
      true,
    );
  }

  // E. duplicate retry after all rows were already written
  {
    const firstApply = {
      status: 'completed',
      completed_at: '2026-07-25T15:00:00.000Z',
      results_json: {
        sourceJobId: 'source-1',
        partialApply: false,
        rowsUnresolved: 0,
      },
    };
    assert.strictEqual(
      tm.isDryScanSourceFullyApplied('source-1', '2026-07-25T14:46:47.000Z', [firstApply]),
      true,
    );

    const partialApply = {
      status: 'completed',
      completed_at: '2026-07-25T15:10:00.000Z',
      error: 'partial_apply_unresolved_rows_remain',
      results_json: {
        sourceJobId: 'source-1',
        partialApply: true,
        rowsUnresolved: 3,
      },
    };
    assert.strictEqual(
      tm.isDryScanSourceFullyApplied('source-1', '2026-07-25T14:46:47.000Z', [partialApply]),
      false,
    );
  }
}

function main() {
  runRestoreTests();
  runApplyTests();
  console.log('All threshold maintenance regression tests passed.');
}

main();
