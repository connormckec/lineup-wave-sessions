'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const profileAuth = require('../lib/profile-auth');
const notificationConfig = require('../lib/notification-config');
const sessionChangeEvents = require('../lib/session-change-events');
const notificationDeliveries = require('../lib/notification-deliveries');
const webPushConfig = require('../lib/web-push-config');
const pushStore = require('../lib/push-subscription-store');
const webPushProvider = require('../lib/notification-provider-webpush');

console.log('web push regression');

async function runTests() {

const TEST_PROFILE_A = 'test-profile-alpha-001';
const TEST_PROFILE_B = 'test-profile-beta-002';
const TEST_USER_A = profileAuth.deriveUserKeyFromProfileCodeSync(TEST_PROFILE_A);
const TEST_USER_B = profileAuth.deriveUserKeyFromProfileCodeSync(TEST_PROFILE_B);
const VALID_ENDPOINT = 'https://push.example.test/some-endpoint/device123';

function validSubscription(overrides = {}) {
  return {
    endpoint: overrides.endpoint || VALID_ENDPOINT,
    expirationTime: null,
    keys: {
      p256dh: overrides.p256dh || 'BNcRdmlLqZ3wX3V_WJ8xQ8xQ8xQ8xQ8xQ8xQ8xQ8',
      auth: overrides.auth || 'auth-secret-value',
    },
  };
}

function mockSupabase(initial = {}) {
  const tables = {
    push_subscriptions: [...(initial.push_subscriptions || [])],
    notification_deliveries: [...(initial.notification_deliveries || [])],
    session_change_events: [...(initial.session_change_events || [])],
    watchlist_items: [...(initial.watchlist_items || [])],
  };

  function applyFilters(rows, filters) {
    return rows.filter((row) => filters.every((f) => {
      if (f.op === 'eq') return row[f.field] === f.value;
      if (f.op === 'in') return f.value.includes(row[f.field]);
      return true;
    }));
  }

  return {
    tables,
    from(table) {
      const state = { filters: [], insertRow: null, updatePatch: null, maybeSingle: false, inFilter: null };
      const api = {
        select() { return api; },
        eq(field, value) { state.filters.push({ field, op: 'eq', value }); return api; },
        in(field, value) { state.inFilter = { field, value }; return api; },
        order() { return api; },
        insert(row) { state.insertRow = row; return api; },
        update(patch) { state.updatePatch = patch; return api; },
        maybeSingle() { state.maybeSingle = true; return api; },
        single() { state.maybeSingle = true; return api; },
        then(resolve, reject) {
          try {
            let rows = applyFilters([...(tables[table] || [])], state.filters);
            if (state.inFilter) {
              rows = rows.filter((r) => state.inFilter.value.includes(r[state.inFilter.field]));
            }
            if (state.insertRow) {
              const row = { id: crypto.randomUUID(), created_at: new Date().toISOString(), ...state.insertRow };
              tables[table].push(row);
              return resolve({ data: state.maybeSingle ? row : [row], error: null });
            }
            if (state.updatePatch) {
              if (state.inFilter) {
                rows = applyFilters([...(tables[table] || [])], state.filters);
                rows = rows.filter((r) => state.inFilter.value.includes(r[state.inFilter.field]));
                for (const row of rows) Object.assign(row, state.updatePatch);
                return resolve({ data: rows, error: null });
              }
              const target = rows[0];
              if (!target) return resolve({ data: null, error: null });
              Object.assign(target, state.updatePatch);
              return resolve({ data: state.maybeSingle ? target : rows, error: null });
            }
            if (state.maybeSingle) return resolve({ data: rows[0] || null, error: null });
            return resolve({ data: rows, error: null });
          } catch (err) {
            return reject(err);
          }
        },
      };
      return api;
    },
  };
}

{
  const bad = pushStore.validateSubscriptionShape({ endpoint: 'http://insecure.test', keys: {} });
  assert.strictEqual(bad.ok, false);
  const good = pushStore.validateSubscriptionShape(validSubscription());
  assert.strictEqual(good.ok, true);
}

{
  const h1 = pushStore.hashEndpoint(VALID_ENDPOINT);
  const h2 = pushStore.hashEndpoint(VALID_ENDPOINT);
  assert.strictEqual(h1, h2);
  assert.notStrictEqual(h1, pushStore.hashEndpoint(`${VALID_ENDPOINT}/other`));
}

{
  const db = mockSupabase();
  const deviceA = crypto.randomUUID();
  const first = await pushStore.upsertPushSubscription(db, TEST_USER_A, {
    subscription: validSubscription({ endpoint: `${VALID_ENDPOINT}/v1` }),
    deviceInstallId: deviceA,
    deviceLabel: 'iPhone',
    permissionState: 'granted',
  });
  assert.strictEqual(first.ok, true);
  const second = await pushStore.upsertPushSubscription(db, TEST_USER_A, {
    subscription: validSubscription({ endpoint: `${VALID_ENDPOINT}/v2` }),
    deviceInstallId: deviceA,
    deviceLabel: 'iPhone',
    permissionState: 'granted',
  });
  assert.strictEqual(second.ok, true);
  assert.strictEqual(db.tables.push_subscriptions.filter((r) => r.user_key === TEST_USER_A).length, 1);
  assert.strictEqual(db.tables.push_subscriptions[0].endpoint, `${VALID_ENDPOINT}/v2`);
}

{
  const db = mockSupabase();
  const device = crypto.randomUUID();
  await pushStore.upsertPushSubscription(db, TEST_USER_A, {
    subscription: validSubscription(),
    deviceInstallId: device,
  });
  await pushStore.upsertPushSubscription(db, TEST_USER_B, {
    subscription: validSubscription({ endpoint: `${VALID_ENDPOINT}/other-user` }),
    deviceInstallId: device,
  });
  const aRows = db.tables.push_subscriptions.filter((r) => r.user_key === TEST_USER_A);
  const bRows = db.tables.push_subscriptions.filter((r) => r.user_key === TEST_USER_B);
  assert.strictEqual(aRows.length, 1);
  assert.strictEqual(bRows.length, 1);
}

{
  const safe = pushStore.toSafeSubscriptionResponse({
    id: 'sub-1',
    device_install_id: 'dev-1',
    endpoint: VALID_ENDPOINT,
    endpoint_hash: pushStore.hashEndpoint(VALID_ENDPOINT),
    p256dh: 'secret',
    auth: 'secret',
    active: true,
  });
  assert.ok(!JSON.stringify(safe).includes(VALID_ENDPOINT));
  assert.ok(!JSON.stringify(safe).includes('secret'));
  assert.ok(safe.subscriptionId);
}

{
  const migration = fs.readFileSync(
    path.join(__dirname, '../supabase/migrations/202607252100_web_push.sql'),
    'utf8',
  );
  assert.match(migration, /revoke all on table push_subscriptions from public, anon, authenticated/i);
  assert.match(migration, /grant all on table push_subscriptions to service_role/i);
}

{
  const subs = [
    { id: 's1', endpoint_hash: 'h1', active: true, user_key: TEST_USER_A },
    { id: 's2', endpoint_hash: 'h2', active: true, user_key: TEST_USER_A },
    { id: 's3', endpoint_hash: 'h3', active: true, user_key: TEST_USER_A },
  ];
  const db = mockSupabase({
    watchlist_items: [{ id: 'w1', user_key: TEST_USER_A, session_key: 'k1', active: true }],
    session_change_events: [],
  });
  const event = { id: 'e1' };
  db.from('session_change_events').insert = () => ({
    select: () => ({
      single: () => Promise.resolve({ data: event, error: null }),
    }),
  });

  let listCalls = 0;
  const result = await notificationDeliveries.createDeliveriesForEvent(
    db,
    event,
    [{ id: 'w1', user_key: TEST_USER_A, active: true, session_key: 'k1' }],
    {
      deliveryProvider: 'webpush',
      listPushSubscriptions: async () => {
        listCalls += 1;
        return subs;
      },
    },
  );
  assert.strictEqual(listCalls, 1);
  assert.strictEqual(result.created.length, 3);
  assert.ok(result.created.every((d) => d.provider === 'webpush'));
}

{
  const key = sessionChangeEvents.buildDeliveryDedupeKey({
    changeEventId: 'e1',
    watchId: 'w1',
    provider: 'webpush',
    pushSubscriptionId: 's1',
  });
  assert.strictEqual(key, 'e1:w1:s1:webpush');
}

{
  const ok = webPushProvider.classifyWebPushResult({ ok: true, statusCode: 201 });
  assert.strictEqual(ok.ok, true);
  const expired = webPushProvider.classifyWebPushResult({ ok: false, statusCode: 410 });
  assert.strictEqual(expired.deactivateSubscription, true);
  const rate = webPushProvider.classifyWebPushResult({ ok: false, statusCode: 429 });
  assert.strictEqual(rate.transient, true);
  const timeout = webPushProvider.classifyWebPushResult({ ok: false, error: 'timeout' });
  assert.strictEqual(timeout.transient, true);
  const five = webPushProvider.classifyWebPushResult({ ok: false, statusCode: 503 });
  assert.strictEqual(five.transient, true);
  const vapid = webPushProvider.classifyWebPushResult({ ok: false, statusCode: 401 });
  assert.strictEqual(vapid.vapidConfigError, true);
  assert.strictEqual(vapid.deactivateSubscription, false);
}

{
  const db = mockSupabase({
    push_subscriptions: [{ id: 's-exp', active: true, user_key: TEST_USER_A, device_install_id: 'd1', endpoint: VALID_ENDPOINT, endpoint_hash: 'h', p256dh: 'p', auth: 'a' }],
    notification_deliveries: [
      { id: 'd1', push_subscription_id: 's-exp', status: 'pending' },
      { id: 'd2', push_subscription_id: 's-exp', status: 'retryable' },
    ],
  });
  await pushStore.deactivateSubscriptionById(db, 's-exp', 'expired');
  assert.strictEqual(db.tables.push_subscriptions[0].active, false);
  assert.ok(db.tables.notification_deliveries.every((d) => d.status === 'cancelled'));
}

{
  const cfg = notificationConfig.readNotificationConfig({
    NOTIFICATION_DELIVERY_PROVIDER: 'webpush',
    WEB_PUSH_ENABLED: 'true',
    WEB_PUSH_VAPID_PUBLIC_KEY: 'pub',
    WEB_PUSH_VAPID_PRIVATE_KEY: 'priv',
    WEB_PUSH_VAPID_SUBJECT: 'mailto:alerts@example.com',
    SUPABASE_URL: 'https://example.supabase.co',
    SUPABASE_SERVICE_ROLE_KEY: 'key',
    LEGACY_INLINE_WATCH_ALERTS_ENABLED: 'false',
  });
  assert.strictEqual(cfg.notificationDeliveryProvider, 'webpush');
  assert.strictEqual(cfg.webPushEnabled, true);
  assert.strictEqual(cfg.dualRealAlertSystemsEnabled, false);
}

{
  const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
  const sw = fs.readFileSync(path.join(__dirname, '../public/sw.js'), 'utf8');
  assert.ok(html.includes('SW_CACHE_VERSION = \'14\''));
  assert.ok(sw.includes("const CACHE_VERSION = '14'"));
  assert.ok(!html.includes('requestPermission()') || html.includes('enablePushNotifications'));
  assert.ok(!/Notification\.requestPermission\(\)/.test(html.replace(/enablePushNotifications[\s\S]{0,500}/, '')));
  assert.ok(html.includes('push-notifications-block'));
  assert.ok(!html.includes('Internal demo only. For now, alerts use ntfy'));
}

{
  const pushClient = fs.readFileSync(path.join(__dirname, '../public/push-client.js'), 'utf8');
  assert.ok(!pushClient.includes('console.log(subscription.endpoint'));
  assert.ok(pushClient.includes('getOrCreateDeviceInstallId'));
}

{
  const badPayload = webPushProvider.buildPushPayload({
    title: 'T',
    message: 'Body',
    clickUrl: 'https://evil.example/phish',
    lineupOrigin: 'https://lineup.example',
  });
  assert.strictEqual(badPayload.ok, false);
  const goodPayload = webPushProvider.buildPushPayload({
    title: 'T',
    message: 'Body',
    clickUrl: '/?date=2026-08-01',
    eventType: 'became_available',
    sessionKey: '178_1',
    lineupOrigin: 'https://lineup.example',
  });
  assert.strictEqual(goodPayload.ok, true);
  assert.strictEqual(goodPayload.payload.url, '/?date=2026-08-01');
}

{
  const sw = fs.readFileSync(path.join(__dirname, '../public/sw.js'), 'utf8');
  assert.ok(sw.includes("addEventListener('push'"));
  assert.ok(sw.includes("addEventListener('notificationclick'"));
}

{
  const serverSrc = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const cfgSrc = fs.readFileSync(path.join(__dirname, '../lib/notification-config.js'), 'utf8');
  assert.ok(cfgSrc.includes('NOTIFICATION_DELIVERY_PROVIDER'));
  assert.ok(serverSrc.includes('notificationRuntimeConfig.notificationDeliveryProvider'));
  assert.ok(serverSrc.includes('deviceInstallId'));
}

{
  const cfgNtfy = notificationConfig.readNotificationConfig({
    NOTIFICATION_DELIVERY_PROVIDER: 'ntfy',
    WEB_PUSH_ENABLED: 'false',
    SUPABASE_URL: 'x',
    SUPABASE_SERVICE_ROLE_KEY: 'y',
  });
  const cfgWeb = notificationConfig.readNotificationConfig({
    NOTIFICATION_DELIVERY_PROVIDER: 'webpush',
    WEB_PUSH_ENABLED: 'true',
    WEB_PUSH_VAPID_PUBLIC_KEY: 'a',
    WEB_PUSH_VAPID_PRIVATE_KEY: 'b',
    WEB_PUSH_VAPID_SUBJECT: 'mailto:a@b.com',
    SUPABASE_URL: 'x',
    SUPABASE_SERVICE_ROLE_KEY: 'y',
  });
  assert.notStrictEqual(cfgNtfy.notificationDeliveryProvider, cfgWeb.notificationDeliveryProvider);
}

{
  assert.strictEqual(webPushConfig.isValidVapidSubject('mailto:alerts@lineup.test'), true);
  assert.strictEqual(webPushConfig.isValidVapidSubject('not-an-email'), false);
}

console.log('web push regression: ok');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
