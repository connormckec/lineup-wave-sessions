'use strict';

const assert = require('assert');
const adaptiveSchedule = require('../lib/adaptive-threshold-schedule');
const supportedHorizon = require('../lib/supported-horizon-config');
const marketObservations = require('../lib/market-observations');
const sessionChangeEvents = require('../lib/session-change-events');

function session(overrides = {}) {
  return {
    key: 'sess-1',
    ts: overrides.ts ?? Math.floor(Date.now() / 1000) + 6 * 3600,
    isoDate: '2026-07-27',
    dateKey: '2026-07-27',
    available: overrides.available ?? true,
    threshold_scanned_at: overrides.threshold_scanned_at ?? null,
    lastDetailedCheckAt: overrides.lastDetailedCheckAt ?? null,
    ...overrides,
  };
}

console.log('adaptive threshold schedule regression');

async function runTests() {
  // 1. Watched session within 72 hours becomes due after 5 minutes.
  {
  const now = new Date('2026-07-26T18:00:00.000Z');
  const s = session({
    ts: Math.floor(new Date('2026-07-27T12:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-26T17:56:00.000Z',
  });
  const fresh = adaptiveSchedule.evaluateInventorySchedule(s, { watched: true, now });
  assert.strictEqual(fresh.due, false);
  assert.strictEqual(fresh.targetFreshnessMinutes, 5);
  const due = adaptiveSchedule.evaluateInventorySchedule(session({
    ...s,
    threshold_scanned_at: '2026-07-26T17:55:00.000Z',
  }), { watched: true, now });
  assert.strictEqual(due.due, true);
  assert.strictEqual(due.cadenceSource, 'watch_priority');
}

// 2. Unwatched same-day session becomes due after 30 minutes.
{
  const now = new Date('2026-07-26T18:00:00.000Z');
  const s = session({
    ts: Math.floor(new Date('2026-07-26T20:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-26T17:31:00.000Z',
  });
  const notDue = adaptiveSchedule.evaluateInventorySchedule(s, { watched: false, now });
  assert.strictEqual(notDue.targetFreshnessMinutes, 30);
  assert.strictEqual(notDue.due, false);
  const due = adaptiveSchedule.evaluateInventorySchedule(session({
    ...s,
    threshold_scanned_at: '2026-07-26T17:29:00.000Z',
  }), { watched: false, now });
  assert.strictEqual(due.due, true);
  assert.strictEqual(due.cadenceSource, 'general_proximity');
}

// 3. Watched packed session remains eligible.
{
  const now = new Date('2026-07-26T18:00:00.000Z');
  const packed = session({
    available: false,
    ts: Math.floor(new Date('2026-07-27T12:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: null,
  });
  assert.strictEqual(adaptiveSchedule.isSessionEligibleForInventorySchedule(packed, { watched: true, now }), true);
  const evalResult = adaptiveSchedule.evaluateInventorySchedule(packed, { watched: true, now });
  assert.strictEqual(evalResult.eligible, true);
  assert.strictEqual(evalResult.due, true);
}

// 4. Far-future unwatched session beyond supported horizon does not trigger date scan.
{
  const now = new Date('2026-07-26T18:00:00.000Z');
  const far = session({
    isoDate: '2026-09-01',
    dateKey: '2026-09-01',
    ts: Math.floor(new Date('2026-09-01T12:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: null,
  });
  const horizon = supportedHorizon.resolveSupportedHorizon({ todayIso: '2026-07-26' });
  const dueScan = adaptiveSchedule.collectDueDateScanCandidates([far], {
    watchKeys: new Set(),
    now,
    todayIso: '2026-07-26',
    daysFromTodayFn: () => 36,
    horizon,
  });
  assert.strictEqual(dueScan.candidates.length, 0);
  assert.strictEqual(
    adaptiveSchedule.isDateWithinNearTermDateScan({
      isoDate: '2026-09-01',
      watched: false,
      todayIso: '2026-07-26',
      daysFromTodayFn: () => 36,
      horizon,
    }),
    false,
  );
}

// 5. Price modal checks do not run at five-minute inventory cadence.
{
  const now = new Date('2026-07-26T18:00:00.000Z');
  const s = session({
    ts: Math.floor(new Date('2026-07-27T12:00:00.000Z').getTime() / 1000),
    lastDetailedCheckAt: '2026-07-26T17:56:00.000Z',
  });
  const price = adaptiveSchedule.evaluatePriceSchedule(s, { watched: true, now });
  assert.strictEqual(price.due, false);
  assert.ok(price.targetFreshnessMinutes >= 30);
  const inventory = adaptiveSchedule.evaluateInventorySchedule(session({
    ...s,
    threshold_scanned_at: '2026-07-26T17:54:00.000Z',
  }), { watched: true, now });
  assert.strictEqual(inventory.targetFreshnessMinutes, 5);
  assert.strictEqual(inventory.due, true);
}

// 6. Actual state change creates an immediate observation.
{
  marketObservations.resetObservationMemoryForTests();
  const prior = session({ key: 'obs-1', threshold_scanned_at: '2026-07-26T17:00:00.000Z', available: true });
  const next = session({
    key: 'obs-1',
    threshold_scanned_at: '2026-07-26T18:00:00.000Z',
    available: true,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    available_entries: 3,
  });
  const built = marketObservations.buildSessionMarketObservationRow({
    session: next,
    previousSession: prior,
    observedAt: new Date('2026-07-26T18:00:00.000Z'),
  });
  assert.ok(built.record);
  assert.ok(built.row.observation_reason.includes('inventory_change'));
}

// 7. Unchanged five-minute inventory checks are deduplicated.
{
  marketObservations.resetObservationMemoryForTests();
  const s = session({
    key: 'obs-2',
    threshold_scanned_at: '2026-07-26T18:00:00.000Z',
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    available_entries: 4,
  });
  const first = marketObservations.buildSessionMarketObservationRow({
    session: s,
    observedAt: new Date('2026-07-26T18:00:00.000Z'),
  });
  assert.ok(first.record);
  marketObservations.rememberObservationMemoryForTests(s, first);
  const retry = marketObservations.buildSessionMarketObservationRow({
    session: s,
    observedAt: new Date('2026-07-26T18:04:00.000Z'),
  });
  assert.strictEqual(retry.record, false);
}

// 8. Near-term unchanged heartbeat occurs at 30 minutes.
{
  marketObservations.resetObservationMemoryForTests();
  const mock = {
    client: {
      from(table) {
        if (table === 'session_market_observations') {
          return {
            insert(row) {
              return {
                select() {
                  return {
                    maybeSingle: async () => ({ data: { id: 'obs-hb-1', ...row }, error: null }),
                  };
                },
              };
            },
          };
        }
        if (table === 'session_product_price_observations') {
          return { insert: async () => ({ error: null }) };
        }
        throw new Error(`unexpected table ${table}`);
      },
    },
  };
  const s = session({
    key: 'obs-3',
    ts: Math.floor(new Date('2026-07-26T20:00:00.000Z').getTime() / 1000),
    threshold_scanned_at: '2026-07-26T18:00:00.000Z',
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    slot_status: 'exact',
    available_entries: 4,
  });
  const first = marketObservations.buildSessionMarketObservationRow({
    session: s,
    observedAt: new Date('2026-07-26T18:00:00.000Z'),
  });
  assert.ok(first.record);
  await marketObservations.insertMarketObservation(mock.client, first, s);
  const early = marketObservations.buildSessionMarketObservationRow({
    session: s,
    observedAt: new Date('2026-07-26T18:20:00.000Z'),
  });
  assert.strictEqual(early.record, false);
  const heartbeat = marketObservations.buildSessionMarketObservationRow({
    session: s,
    observedAt: new Date('2026-07-26T18:31:00.000Z'),
  });
  assert.ok(heartbeat.record);
  assert.ok(heartbeat.policy.reasons.includes('heartbeat'));
  assert.strictEqual(heartbeat.policy.heartbeatTargetMinutes, 30);
}

// 9. Notification delivery begins only after trusted apply succeeds.
{
  function makeTrustedSession(overrides = {}) {
    return {
      key: 'n1',
      available: overrides.available ?? true,
      thresholdScanVerified: true,
      threshold_scan_verified: true,
      slot_source: 'entries_left_threshold_scan',
      slot_status: 'exact',
      available_entries: overrides.available_entries ?? overrides.slots ?? 2,
      thresholdInferredSlots: overrides.thresholdInferredSlots ?? overrides.slots ?? 2,
      threshold_scanned_at: overrides.threshold_scanned_at || '2026-07-26T17:00:00.000Z',
      thresholdScanAt: overrides.thresholdScanAt || overrides.threshold_scanned_at || '2026-07-26T17:00:00.000Z',
      ...overrides,
    };
  }
  const prev = makeTrustedSession({ slots: 2, available_entries: 2, thresholdInferredSlots: 2, threshold_scanned_at: '2026-07-26T17:00:00.000Z' });
  const next = makeTrustedSession({
    slots: 5,
    available_entries: 5,
    thresholdInferredSlots: 5,
    threshold_scanned_at: '2026-07-26T18:00:00.000Z',
  });
  assert.strictEqual(
    sessionChangeEvents.deriveSessionChangeEvent({ previousSession: prev, nextSession: next, writeSucceeded: false }),
    null,
  );
  const event = sessionChangeEvents.deriveSessionChangeEvent({ previousSession: prev, nextSession: next, writeSucceeded: true });
  assert.ok(event);
  assert.strictEqual(event.event_type, sessionChangeEvents.EVENT_SPOTS_OPENED);
}

// 10. Worker priority and safeguards remain documented in server/worker modules.
{
  const fs = require('fs');
  const path = require('path');
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /runNearTermMaintenanceTick/);
  assert.match(serverJs, /\/api\/admin\/maintenance\/near-term-tick/);
  assert.match(serverJs, /\/api\/admin\/adaptive-schedule\/diagnostics/);
  assert.match(serverJs, /THRESHOLD_SCAN_JOB_MODE_DATE/);
  const twc = require('../lib/threshold-worker-claim');
  const applyJob = { mode: twc.THRESHOLD_SCAN_JOB_MODE_APPLY, created_at: '2026-07-26T18:00:00.000Z' };
  const dateJob = { mode: twc.THRESHOLD_SCAN_JOB_MODE_DATE, dates: ['2026-07-26'], created_at: '2026-07-26T17:00:00.000Z' };
  const weekJob = { mode: twc.THRESHOLD_SCAN_JOB_MODE_WEEK, results_json: { weekStart: '2026-07-20' }, dates: ['2026-07-20'], created_at: '2026-07-26T16:00:00.000Z' };
  const sorted = [weekJob, dateJob, applyJob].sort((a, b) => twc.compareThresholdScanJobPriority(a, b, '2026-07-26'));
  assert.strictEqual(sorted[0].mode, twc.THRESHOLD_SCAN_JOB_MODE_APPLY);
  assert.strictEqual(sorted[1].mode, twc.THRESHOLD_SCAN_JOB_MODE_DATE);
}

  console.log('adaptive threshold schedule regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
