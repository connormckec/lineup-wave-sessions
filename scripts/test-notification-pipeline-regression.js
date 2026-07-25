'use strict';

const assert = require('assert');
const crypto = require('crypto');
const profileAuth = require('../lib/profile-auth');
const notificationTopic = require('../lib/notification-topic');
const sessionChangeEvents = require('../lib/session-change-events');
const notificationDeliveries = require('../lib/notification-deliveries');
const notificationDiagnostics = require('../lib/notification-diagnostics');
const thresholdMaintenance = require('../lib/threshold-maintenance');

console.log('notification pipeline regression');

const TEST_PROFILE_A = 'test-profile-alpha-001';
const TEST_TOPIC_A = 'testtopic001alpha';

function makeTrustedSession(overrides = {}) {
  return {
    key: overrides.key || '1785120000_1',
    available: overrides.available ?? true,
    isoDate: overrides.isoDate || '2026-08-20',
    level: overrides.level || 'Advanced Turns',
    session_type: overrides.session_type || 'Advanced Turns',
    wave_side: overrides.wave_side || 'Left Wave',
    time: overrides.time || '8:00 am',
    thresholdScanVerified: true,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    threshold_confidence: overrides.threshold_confidence || 'exact',
    slot_status: overrides.slot_status || 'exact',
    available_entries: overrides.available_entries ?? overrides.slots ?? 5,
    thresholdInferredSlots: overrides.thresholdInferredSlots ?? overrides.slots ?? 5,
    threshold_scanned_at: overrides.threshold_scanned_at || '2026-08-20T12:00:00.000Z',
    thresholdScanAt: overrides.thresholdScanAt || overrides.threshold_scanned_at || '2026-08-20T12:00:00.000Z',
    ...overrides,
  };
}

function mockSupabase(initial = {}) {
  const tables = {
    session_change_events: [...(initial.session_change_events || [])],
    notification_deliveries: [...(initial.notification_deliveries || [])],
    notification_profiles: [...(initial.notification_profiles || [])],
    watchlist_items: [...(initial.watchlist_items || [])],
  };

  return {
    from(table) {
      const state = {
        filters: [],
        insertRow: null,
        updatePatch: null,
        limitN: null,
        maybeSingle: false,
        head: false,
        countExact: false,
      };
      const api = {
        select(_cols, opts = {}) {
          state.head = opts.head === true;
          state.countExact = opts.count === 'exact';
          return api;
        },
        eq(field, value) {
          state.filters.push({ field, op: 'eq', value });
          return api;
        },
        order() { return api; },
        limit(n) { state.limitN = n; return api; },
        insert(row) {
          state.insertRow = row;
          return api;
        },
        upsert(row) {
          state.insertRow = row;
          return api;
        },
        update(patch) {
          state.updatePatch = patch;
          return api;
        },
        maybeSingle() {
          state.maybeSingle = true;
          return api;
        },
        single() {
          state.single = true;
          return api;
        },
        then(resolve, reject) {
          try {
            if (state.insertRow) {
              const row = { id: crypto.randomUUID(), ...state.insertRow };
              if (table === 'session_change_events') {
                const dup = tables.session_change_events.find((r) => r.dedupe_key === row.dedupe_key);
                if (dup) {
                  return resolve({ data: null, error: { code: '23505', message: 'duplicate' } });
                }
                tables.session_change_events.push(row);
              } else if (table === 'notification_deliveries') {
                const dup = tables.notification_deliveries.find((r) => r.dedupe_key === row.dedupe_key);
                if (dup) {
                  return resolve({ data: null, error: { code: '23505', message: 'duplicate' } });
                }
                tables.notification_deliveries.push(row);
              } else if (table === 'notification_profiles') {
                const idx = tables.notification_profiles.findIndex((r) => r.user_key === row.user_key);
                if (idx >= 0) tables.notification_profiles[idx] = { ...tables.notification_profiles[idx], ...row };
                else tables.notification_profiles.push(row);
                return resolve({ data: tables.notification_profiles.find((r) => r.user_key === row.user_key), error: null });
              }
              return resolve({ data: row, error: null });
            }

            if (state.updatePatch && state.filters.length) {
              const rows = tables[table] || [];
              const match = rows.find((row) => state.filters.every((f) => row[f.field] === f.value));
              if (match) Object.assign(match, state.updatePatch);
              return resolve({ data: match, error: null });
            }

            let rows = (tables[table] || []).filter((row) => state.filters.every((f) => row[f.field] === f.value));
            if (state.limitN != null) rows = rows.slice(0, state.limitN);
            if (state.head && state.countExact) {
              return resolve({ count: rows.length, error: null });
            }
            if (state.maybeSingle) {
              return resolve({ data: rows[0] || null, error: null });
            }
            return resolve({ data: rows, error: null });
          } catch (err) {
            return reject(err);
          }
        },
      };
      return api;
    },
    rpc() {
      return Promise.resolve({ data: [], error: null });
    },
    _tables: tables,
  };
}

async function runTests() {
  {
    const keyA = profileAuth.deriveUserKeyFromProfileCodeSync(TEST_PROFILE_A);
    const keyB = profileAuth.deriveUserKeyFromProfileCodeSync('test-profile-beta-002');
    assert.notStrictEqual(keyA, keyB);
    assert.match(keyA, /^profile:[a-f0-9]{32}$/);
  }

  {
    const good = notificationTopic.validateNtfyTopic(TEST_TOPIC_A);
    assert.strictEqual(good.ok, true);
    const bad = notificationTopic.validateNtfyTopic('https://evil.example/topic');
    assert.strictEqual(bad.ok, false);
    assert.strictEqual(notificationTopic.maskDestination(TEST_TOPIC_A), 'te••••ha');
  }

  {
    const packed = makeTrustedSession({ available: false, available_entries: 0, thresholdInferredSlots: 0 });
    const open = makeTrustedSession({ available: true, available_entries: 12, thresholdInferredSlots: 12, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    const event = sessionChangeEvents.deriveSessionChangeEvent({
      previousSession: packed,
      nextSession: open,
      writeSucceeded: true,
    });
    assert.ok(event);
    assert.strictEqual(event.event_type, sessionChangeEvents.EVENT_BECAME_AVAILABLE);
    assert.strictEqual(event.new_slots, 12);
  }

  {
    const prev = makeTrustedSession({ available_entries: 2, thresholdInferredSlots: 2, threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    const next = makeTrustedSession({ available_entries: 5, thresholdInferredSlots: 5, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    const event = sessionChangeEvents.deriveSessionChangeEvent({ previousSession: prev, nextSession: next, writeSucceeded: true });
    assert.ok(event);
    assert.strictEqual(event.event_type, sessionChangeEvents.EVENT_SPOTS_OPENED);
  }

  {
    const prev = makeTrustedSession({
      thresholdScanVerified: false,
      threshold_scan_verified: false,
      available_entries: null,
      thresholdInferredSlots: null,
    });
    const next = makeTrustedSession({ available_entries: 5, thresholdInferredSlots: 5 });
    const event = sessionChangeEvents.deriveSessionChangeEvent({ previousSession: prev, nextSession: next, writeSucceeded: true });
    assert.strictEqual(event, null);
  }

  {
    const session = makeTrustedSession({ threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    const first = sessionChangeEvents.deriveSessionChangeEvent({
      previousSession: makeTrustedSession({ available: false, available_entries: 0, thresholdInferredSlots: 0 }),
      nextSession: session,
      writeSucceeded: true,
    });
    const retry = sessionChangeEvents.deriveSessionChangeEvent({
      previousSession: session,
      nextSession: { ...session },
      writeSucceeded: true,
    });
    assert.ok(first);
    assert.strictEqual(retry, null);
  }

  {
    const existing = makeTrustedSession({ threshold_scanned_at: '2026-08-20T14:00:00.000Z' });
    const older = makeTrustedSession({ threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    const event = sessionChangeEvents.deriveSessionChangeEvent({
      previousSession: existing,
      nextSession: older,
      writeSucceeded: true,
    });
    assert.strictEqual(event, null);
  }

  {
    const supabase = mockSupabase();
    const eventRow = sessionChangeEvents.buildEventRecord({
      sessionKey: '1785120000_1',
      isoDate: '2026-08-20',
      eventType: sessionChangeEvents.EVENT_BECAME_AVAILABLE,
      prev: { available: false, slots: 0, trusted: true },
      next: { available: true, slots: 5, trusted: true, thresholdScannedAt: '2026-08-20T12:00:00.000Z' },
    });
    const watch = { id: crypto.randomUUID(), user_key: profileAuth.deriveUserKeyFromProfileCodeSync(TEST_PROFILE_A), session_key: '1785120000_1', active: true };
    const resolveDestination = async () => ({ ok: true, destination: TEST_TOPIC_A });
    const first = await notificationDeliveries.recordChangeEventAndDeliveries({
      supabase,
      eventRow,
      watches: [watch],
      resolveDestination,
    });
    assert.strictEqual(first.deliveries.length, 1);
    const second = await notificationDeliveries.recordChangeEventAndDeliveries({
      supabase,
      eventRow,
      watches: [watch],
      resolveDestination,
    });
    assert.strictEqual(second.duplicateEvent, true);
    assert.strictEqual(second.deliveries.length, 0);
  }

  {
    const masked = notificationDiagnostics.maskDeliveryRow({
      destination: TEST_TOPIC_A,
      user_key: profileAuth.deriveUserKeyFromProfileCodeSync(TEST_PROFILE_A),
    });
    assert.ok(masked.destination.includes('••••'));
  }

  {
    const prev = thresholdMaintenance.restoreTrustedThresholdFields(
      makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T12:00:00.000Z' }),
      makeTrustedSession({ available_entries: null, threshold_scanned_at: '2026-08-20T12:00:00.000Z' }),
      { scrapeKind: 'basic' },
    );
    const event = sessionChangeEvents.deriveSessionChangeEvent({
      previousSession: makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T12:00:00.000Z' }),
      nextSession: prev,
      writeSucceeded: true,
    });
    assert.strictEqual(event, null);
  }

  {
    const resolvedA = await profileAuth.resolveProfileFromRequest({
      headers: { authorization: `Bearer ${TEST_PROFILE_A}` },
    });
    const resolvedB = await profileAuth.resolveProfileFromRequest({
      headers: { authorization: 'Bearer test-profile-beta-002' },
    });
    assert.strictEqual(resolvedA.ok, true);
    assert.strictEqual(resolvedB.ok, true);
    assert.notStrictEqual(resolvedA.userKey, resolvedB.userKey);
    const missing = await profileAuth.resolveProfileFromRequest({ headers: {} });
    assert.strictEqual(missing.ok, false);
    assert.strictEqual(missing.error, 'profile_auth_required');
  }

  console.log('notification pipeline regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
