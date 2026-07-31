'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const trustedSessionState = require('../lib/trusted-session-state');
const thresholdMaintenance = require('../lib/threshold-maintenance');
const SEM = require('../lib/browse-ui-semantics');

console.log('canonical slot count regression');

function makeTrustedSession(overrides = {}) {
  return {
    key: 's1',
    available: true,
    thresholdScanVerified: true,
    threshold_scan_verified: true,
    slot_status: 'exact',
    slot_source: 'entries_left_threshold_scan',
    thresholdConfidence: 'exact',
    threshold_scanned_at: '2026-07-30T18:00:00.000Z',
    thresholdScanAt: '2026-07-30T18:00:00.000Z',
    slots: null,
    thresholdSlots: null,
    ...overrides,
  };
}

function sanitizeLike(session) {
  const canonical = trustedSessionState.resolveCanonicalSlotCount(session);
  const out = { ...session };
  if (canonical != null && out.slots == null) out.slots = canonical;
  if (canonical != null && out.thresholdSlots == null) out.thresholdSlots = canonical;
  return out;
}

// 1–3. Exact counts 1, 4, 8 from available_entries with null slots.
for (const [count, label] of [[1, '1 spot left'], [4, '4 spots left'], [8, '8 spots left']]) {
  const session = makeTrustedSession({
    available_entries: count,
    thresholdInferredSlots: count,
  });
  assert.strictEqual(trustedSessionState.resolveCanonicalSlotCount(session), count);
  const api = sanitizeLike(session);
  assert.strictEqual(api.slots, count);
  const state = SEM.inventoryState(api);
  assert.strictEqual(state.label, label);
}

// 4. Unknown open count shows Open, not a fabricated number.
{
  const open = { key: 'open', available: true, slots: null };
  assert.strictEqual(trustedSessionState.resolveCanonicalSlotCount(open), null);
  const state = SEM.inventoryState(open);
  assert.strictEqual(state.kind, 'unverified');
  assert.strictEqual(state.label, 'Open');
}

// 5. Full session stays full without positive count.
{
  const full = makeTrustedSession({
    available: false,
    available_entries: 0,
    thresholdInferredSlots: 0,
    slots: null,
  });
  assert.strictEqual(trustedSessionState.resolveCanonicalSlotCount(full), 0);
  const state = SEM.inventoryState(full);
  assert.strictEqual(state.kind, 'full');
  assert.strictEqual(state.label, 'Full');
}

// 6. Detail tile mismatch does not erase threshold count when sanitizing.
{
  const session = makeTrustedSession({
    available_entries: 4,
    thresholdInferredSlots: 4,
    detailStatus: 'failed_tile_mismatch',
    detailVerified: false,
    detailError: 'failed_tile_mismatch',
    lastDetailedCheckAt: '2026-07-30T19:00:00.000Z',
    lastDetailAttemptAt: '2026-07-30T19:00:00.000Z',
    slots: null,
  });
  const api = sanitizeLike(session);
  assert.strictEqual(api.slots, 4);
  assert.strictEqual(trustedSessionState.resolveCanonicalSlotCount(api), 4);
}

// 7. Verification timestamp uses threshold scan fields only.
{
  const session = makeTrustedSession({
    available_entries: 4,
    threshold_scanned_at: '2026-07-30T16:00:00.000Z',
    lastDetailedCheckAt: '2026-07-30T19:30:00.000Z',
    lastDetailAttemptAt: '2026-07-30T19:30:00.000Z',
    lastScraped: '2026-07-30T19:45:00.000Z',
  });
  assert.strictEqual(
    trustedSessionState.getThresholdScannedAt(session),
    '2026-07-30T16:00:00.000Z',
  );
  assert.strictEqual(SEM.verificationAgeMinutes(session, Date.parse('2026-07-30T17:00:00.000Z')), 60);
}

// 8. Field precedence: available_entries beats thresholdInferredSlots.
{
  const session = makeTrustedSession({
    available_entries: 4,
    thresholdInferredSlots: 8,
  });
  assert.strictEqual(trustedSessionState.resolveCanonicalSlotCount(session), 4);
}

// 9. thresholdInferredSlots used when available_entries missing.
{
  const session = makeTrustedSession({
    available_entries: null,
    thresholdInferredSlots: 8,
  });
  assert.strictEqual(trustedSessionState.resolveCanonicalSlotCount(session), 8);
}

// 10. Do not overwrite existing non-null slots.
{
  const session = makeTrustedSession({
    available_entries: 4,
    slots: 2,
  });
  const applied = trustedSessionState.applyCanonicalSlots(session);
  assert.strictEqual(applied.slots, 2);
}

// 11. sessionsWithSlotsCount helper counts canonical sessions.
{
  const sessions = [
    makeTrustedSession({ available_entries: 1 }),
    makeTrustedSession({ available_entries: 4, key: 's2' }),
    makeTrustedSession({ available_entries: 8, key: 's3' }),
    { key: 's4', available: true, slots: null },
  ];
  const count = sessions.filter((s) => trustedSessionState.hasCanonicalSlotCount(s)).length;
  assert.strictEqual(count, 3);
}

// 12. Untrusted session without exact slot_status returns null.
{
  const session = makeTrustedSession({
    available_entries: 4,
    slot_status: 'no_match',
    thresholdConfidence: 'no_match',
  });
  assert.strictEqual(trustedSessionState.resolveCanonicalSlotCount(session), null);
  assert.strictEqual(thresholdMaintenance.isThresholdSlotsTrusted(session), false);
}

// 13. Server sanitize path uses trusted-session-state.
{
  const serverJs = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  assert.match(serverJs, /trustedSessionState\.resolveCanonicalSlotCount/);
  assert.match(serverJs, /trustedSessionState\.hasCanonicalSlotCount/);
}

console.log('canonical slot count regression: all tests passed');
