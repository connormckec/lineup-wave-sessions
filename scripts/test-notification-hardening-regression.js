'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const notificationConfig = require('../lib/notification-config');
const sessionChangeEvents = require('../lib/session-change-events');
const notificationProfileStore = require('../lib/notification-profile-store');
const notificationDeliveries = require('../lib/notification-deliveries');
const notificationPipeline = require('../lib/notification-pipeline');
const profileAuth = require('../lib/profile-auth');

console.log('notification hardening regression');

const TEST_PROFILE_A = 'test-profile-alpha-001';
const TEST_PROFILE_B = 'test-profile-beta-002';
const TEST_TOPIC_A = 'testtopic001alpha';
const TEST_TOPIC_B = 'testtopic002beta';
const TEST_USER_A = profileAuth.deriveUserKeyFromProfileCodeSync(TEST_PROFILE_A);
const TEST_USER_B = profileAuth.deriveUserKeyFromProfileCodeSync(TEST_PROFILE_B);

function makeTrustedSession(overrides = {}) {
  return {
    key: overrides.key || '1785120000_1',
    available: overrides.available ?? true,
    isoDate: overrides.isoDate || '2026-08-20',
    thresholdScanVerified: true,
    threshold_scan_verified: true,
    slot_source: 'entries_left_threshold_scan',
    threshold_confidence: overrides.threshold_confidence || 'exact',
    slot_status: overrides.slot_status || 'exact',
    available_entries: overrides.available_entries ?? overrides.slots ?? 5,
    thresholdInferredSlots: overrides.thresholdInferredSlots ?? overrides.slots ?? 5,
    threshold_scanned_at: overrides.threshold_scanned_at || '2026-08-20T12:00:00.000Z',
    thresholdScanAt: overrides.thresholdScanAt || overrides.threshold_scanned_at || '2026-08-20T12:00:00.000Z',
    thresholdTrustedSuspendedAt: overrides.thresholdTrustedSuspendedAt ?? null,
    ...overrides,
  };
}

function deriveEvent(prev, next) {
  return sessionChangeEvents.deriveSessionChangeEvent({
    previousSession: prev,
    nextSession: next,
    writeSucceeded: true,
  });
}

async function runTests() {
  {
    const prod = notificationConfig.readNotificationConfig({
      LEGACY_INLINE_WATCH_ALERTS_ENABLED: 'false',
      ALLOW_INTERNAL_DEFAULT_NTFY_TOPIC: 'false',
      SUPABASE_URL: 'https://example.supabase.co',
      SUPABASE_SERVICE_ROLE_KEY: 'service-key',
    });
    assert.strictEqual(prod.legacyInlineWatchAlertsEnabled, false);
    assert.strictEqual(prod.durableNotificationPipelineEnabled, true);
    assert.strictEqual(prod.dualRealAlertSystemsEnabled, false);

    const dual = notificationConfig.readNotificationConfig({
      LEGACY_INLINE_WATCH_ALERTS_ENABLED: 'true',
      SUPABASE_URL: 'https://example.supabase.co',
      SUPABASE_SERVICE_ROLE_KEY: 'service-key',
    });
    assert.strictEqual(dual.dualRealAlertSystemsEnabled, true);
  }

  {
    const serverSrc = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    assert.ok(serverSrc.includes('if (!LEGACY_INLINE_WATCH_ALERTS_ENABLED)'));
    assert.ok(!serverSrc.includes("'ap-surf-connor-2026'"));
    assert.ok(!serverSrc.includes('const INTERNAL_DEFAULT_NTFY_TOPIC'));
    assert.ok(!serverSrc.includes('const INTERNAL_DEFAULT_PROFILE_CODE'));
    assert.match(serverSrc, /app\.post\('\/api\/notify\/test'/);
    assert.ok(serverSrc.includes('notificationProvider.sendNotification'));
    assert.ok(serverSrc.includes('function watchItemToClient(w)'));
    assert.ok(!serverSrc.match(/watchItemToClient[\s\S]{0,400}ntfy_topic/));
    const fnStart = serverSrc.indexOf('async function processWatchAlertsAfterScrape');
    const fnBody = serverSrc.slice(fnStart, fnStart + 1200);
    const legacyGate = fnBody.indexOf('if (!LEGACY_INLINE_WATCH_ALERTS_ENABLED)');
    const evaluateCall = fnBody.indexOf('evaluateSessionWatchAlerts');
    assert.ok(legacyGate > 0);
    assert.ok(evaluateCall > legacyGate);
    const maybeSendIdx = serverSrc.indexOf('async function maybeSendWatchAlert');
    const sendNtfyIdx = serverSrc.indexOf('async function sendNtfy');
    assert.ok(maybeSendIdx > 0 && sendNtfyIdx > 0);
  }

  // became_available policy
  {
    const packed = makeTrustedSession({ available: false, available_entries: 0, thresholdInferredSlots: 0 });
    const open = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    assert.strictEqual(deriveEvent(packed, open)?.event_type, sessionChangeEvents.EVENT_BECAME_AVAILABLE);
  }

  {
    const prev = makeTrustedSession({ available_entries: 0, thresholdInferredSlots: 0, threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    const next = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    const event = deriveEvent(prev, next);
    assert.strictEqual(event.event_type, sessionChangeEvents.EVENT_BECAME_AVAILABLE);
    assert.notStrictEqual(event.event_type, sessionChangeEvents.EVENT_SPOTS_OPENED);
  }

  {
    const prev = makeTrustedSession({
      thresholdScanVerified: false,
      threshold_scan_verified: false,
      available_entries: null,
      thresholdInferredSlots: null,
    });
    const next = makeTrustedSession({ available_entries: 5 });
    assert.strictEqual(deriveEvent(prev, next), null);
  }

  {
    const prev = makeTrustedSession({
      threshold_scan_verified: false,
      slot_source: 'tier_scrape',
      available_entries: null,
      thresholdInferredSlots: null,
    });
    const next = makeTrustedSession({ available_entries: 5 });
    assert.strictEqual(deriveEvent(prev, next), null);
  }

  {
    const next = makeTrustedSession({ available_entries: 12, thresholdInferredSlots: 12 });
    assert.strictEqual(deriveEvent(null, next), null);
  }

  {
    const prev = makeTrustedSession({
      available_entries: 3,
      thresholdInferredSlots: 3,
      threshold_scanned_at: '2026-08-20T10:00:00.000Z',
      thresholdTrustedSuspendedAt: '2026-08-20T11:00:00.000Z',
    });
    const next = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    assert.strictEqual(deriveEvent(prev, next), null);
  }

  {
    const prev = makeTrustedSession({ available_entries: 2, threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    const next = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    assert.strictEqual(deriveEvent(prev, next)?.event_type, sessionChangeEvents.EVENT_SPOTS_OPENED);
  }

  {
    const prev = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    const next = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    assert.strictEqual(deriveEvent(prev, next), null);
  }

  {
    const prev = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    const next = makeTrustedSession({ available_entries: 2, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    assert.strictEqual(deriveEvent(prev, next), null);
  }

  {
    const session = makeTrustedSession({ threshold_scanned_at: '2026-08-20T12:00:00.000Z' });
    assert.strictEqual(deriveEvent(session, { ...session }), null);
  }

  // threshold apply + later tier scrape → one event only (same trusted state)
  {
    const packed = makeTrustedSession({ available: false, available_entries: 0, thresholdInferredSlots: 0 });
    const open = makeTrustedSession({ available_entries: 5, threshold_scanned_at: '2026-08-20T13:00:00.000Z' });
    const first = deriveEvent(packed, open);
    assert.ok(first);
    const tierRescrape = deriveEvent(open, { ...open, slots: 5, available: true });
    assert.strictEqual(tierRescrape, null);
  }

  // RLS / RPC migration assertions
  {
    const migrationSql = fs.readFileSync(
      path.join(__dirname, '../supabase/migrations/202607251500_notification_pipeline_hardening.sql'),
      'utf8',
    );
    assert.match(migrationSql, /enable row level security/i);
    assert.match(migrationSql, /revoke all on table notification_profiles from public, anon, authenticated/i);
    assert.match(migrationSql, /revoke all on table notification_deliveries from public, anon, authenticated/i);
    assert.match(migrationSql, /revoke all on function claim_notification_deliveries/i);
    assert.match(migrationSql, /grant execute on function claim_notification_deliveries.*to service_role/i);
    assert.match(migrationSql, /set search_path = public/i);
    assert.match(migrationSql, /on delete set null/i);
  }

  // delivery cancellation
  {
    let updated = null;
    const supabase = {
      from() {
        return {
          update(patch) {
            updated = patch;
            return {
              eq() {
                return {
                  select() {
                    return {
                      single() {
                        return Promise.resolve({ data: { id: 'd1', ...patch }, error: null });
                      },
                    };
                  },
                };
              },
            };
          },
        };
      },
    };
    const result = await notificationDeliveries.cancelDelivery(supabase, { id: 'd1' }, 'watch_inactive_or_missing');
    assert.strictEqual(result.status, 'cancelled');
    assert.strictEqual(updated.last_error, 'watch_inactive_or_missing');
  }

  // worker cancels inactive/missing watches without breaking the batch
  {
    const supabase = {
      rpc: async () => ({
        data: [{ id: 'd2', watch_id: 'w-missing', user_key: TEST_USER_A, change_event_id: 'e1', provider: 'ntfy', destination: TEST_TOPIC_A, status: 'claimed' }],
        error: null,
      }),
      from(table) {
        return {
          update(patch) {
            return {
              eq(_field, _value) {
                return {
                  select() {
                    return {
                      single() {
                        return Promise.resolve({ data: { id: 'd2', status: 'cancelled', ...patch }, error: null });
                      },
                    };
                  },
                };
              },
            };
          },
        };
      },
    };
    const result = await notificationPipeline.processDeliveryBatch({
      supabase,
      workerId: 'test-worker',
      loadWatchById: async () => null,
      loadEventById: async () => ({ event_type: sessionChangeEvents.EVENT_BECAME_AVAILABLE, new_slots: 5 }),
      resolveDestination: async () => ({ ok: true, destination: TEST_TOPIC_A }),
    });
    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.results.length, 1);
    assert.strictEqual(result.results[0].status, 'cancelled');
  }

  {
    const pipelineSrc = fs.readFileSync(path.join(__dirname, '../lib/notification-pipeline.js'), 'utf8');
    assert.ok(pipelineSrc.includes('resolveDestination(delivery.user_key)'));
    assert.ok(pipelineSrc.includes('watch.active === false'));
    assert.ok(pipelineSrc.includes('cancelDelivery'));
  }

  {
    const noDest = await notificationProfileStore.resolveDestinationForUser(null, TEST_USER_A, {
      allowServerFallback: false,
    });
    assert.strictEqual(noDest.ok, false);
    const fallbackOff = await notificationProfileStore.resolveDestinationForUser(null, TEST_USER_A, {
      allowServerFallback: false,
      serverFallbackTopic: TEST_TOPIC_A,
    });
    assert.strictEqual(fallbackOff.ok, false);
  }

  assert.notStrictEqual(TEST_USER_A, TEST_USER_B);
  assert.notStrictEqual(TEST_PROFILE_A, TEST_TOPIC_A);

  console.log('notification hardening regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
