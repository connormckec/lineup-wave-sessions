'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const profileAuthClient = require('../lib/profile-auth-client');
const profileAuth = require('../lib/profile-auth');

console.log('profile auth frontend regression');

const TEST_PROFILE = 'test-profile-alpha-001';
const TEST_PROFILE_B = 'test-profile-beta-002';
const TEST_TOPIC = 'testtopic001alpha';

function extractSendTestNotificationBlock(html) {
  const start = html.indexOf('async function sendTestNotification()');
  assert.ok(start >= 0, 'sendTestNotification not found');
  const end = html.indexOf('\nconst LEVEL_ORDER', start);
  assert.ok(end > start, 'sendTestNotification block end not found');
  return html.slice(start, end);
}

async function runTests() {
  {
    const auth = profileAuthClient.buildProfileAuthHeaders(TEST_PROFILE);
    assert.strictEqual(auth.ok, true);
    assert.strictEqual(auth.headers.Authorization, `Bearer ${TEST_PROFILE}`);
  }

  {
    const auth = profileAuthClient.buildProfileAuthHeaders(TEST_PROFILE, {
      'Content-Type': 'application/json',
    });
    assert.strictEqual(auth.ok, true);
    assert.strictEqual(auth.headers.Authorization, `Bearer ${TEST_PROFILE}`);
    assert.strictEqual(auth.headers['Content-Type'], 'application/json');
  }

  {
    const auth = profileAuthClient.buildProfileAuthHeaders('');
    assert.strictEqual(auth.ok, false);
    assert.strictEqual(auth.error, 'profile_code_missing');
    assert.strictEqual(auth.headers.Authorization, undefined);
  }

  {
    const watchlist = profileAuthClient.mergeProfileAuthFetchOptions(TEST_PROFILE, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{"session_key":"x"}',
    });
    const notify = profileAuthClient.mergeProfileAuthFetchOptions(TEST_PROFILE, {
      method: 'POST',
    });
    assert.strictEqual(watchlist.fetchOptions.headers.Authorization, notify.fetchOptions.headers.Authorization);
    assert.strictEqual(
      watchlist.fetchOptions.headers.Authorization,
      `Bearer ${TEST_PROFILE}`,
    );
  }

  {
    const merged = profileAuthClient.mergeProfileAuthFetchOptions(TEST_PROFILE, {
      method: 'POST',
      headers: {
        Authorization: 'Bearer overwritten',
        'Content-Type': 'application/json',
      },
    });
    assert.strictEqual(merged.fetchOptions.headers.Authorization, `Bearer ${TEST_PROFILE}`);
    assert.strictEqual(merged.fetchOptions.headers['Content-Type'], 'application/json');
  }

  {
    const raw = '  My-Beta-Code  ';
    const normalized = profileAuthClient.normalizeProfileCode(raw);
    const auth = profileAuthClient.buildProfileAuthHeaders(raw);
    assert.strictEqual(auth.headers.Authorization, `Bearer ${normalized}`);
    const derived = profileAuth.deriveUserKeyFromProfileCodeSync(normalized);
    const diag = profileAuthClient.buildProfileAuthDiagnostic(normalized);
    assert.strictEqual(diag.authHeaderPrepared, true);
    assert.strictEqual(diag.derivedUserKeyPrefix, `${derived.slice(0, 16)}…`);
    assert.strictEqual(diag.profileCodeLength, normalized.length);
  }

  {
    const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
    const sendTestBlock = extractSendTestNotificationBlock(html);

    assert.ok(sendTestBlock.includes("fetchProfileApi('/api/notify/test', { method: 'POST' })"));
    assert.ok(!sendTestBlock.includes("JSON.stringify({})"));
    assert.ok(!sendTestBlock.includes('getNtfyTopic()'));
    assert.ok(!sendTestBlock.includes('formatProfileAuthError(body.error)'));
    assert.ok(sendTestBlock.includes('formatNotifyTestError'));
    assert.ok(!/async function sendTestNotification/g.test(html.slice(html.indexOf('const LEVEL_ORDER'))));

    const matches = html.match(/async function sendTestNotification/g) || [];
    assert.strictEqual(matches.length, 1);

    assert.ok(html.includes('mergeProfileAuthFetchOptions'));
    assert.ok(!html.includes('headers: auth.headers'));
    assert.ok(!html.match(/fetch\(\s*['"]\/api\/notify\/test/));
    assert.ok(!html.includes('user_key: getUserKey()'));
    assert.ok(!html.includes('user_key: userKey'));
    assert.ok(!html.includes('ap-surf-connor-2026'));
    assert.ok(html.includes('/profile-auth-client.js?v=3'));
  }

  {
    const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
    assert.ok(html.includes("fetchProfileApi('/api/watchlist',"));
    assert.ok(!html.includes('Subscribe to this exact topic in ntfy: ap-surf-connor'));
    assert.ok(html.includes('maskNtfyTopicForDisplay'));
  }

  {
    assert.strictEqual(
      profileAuthClient.formatNotifyTestError(400, 'notification_topic_not_configured'),
      'Save your ntfy topic in Settings first.',
    );
    assert.strictEqual(
      profileAuthClient.formatNotifyTestError(401, 'profile_auth_required').includes('Settings'),
      true,
    );
    assert.strictEqual(
      profileAuthClient.formatNotifyTestError(429, 'test_rate_limited').includes('cooldown'),
      true,
    );
    assert.strictEqual(
      profileAuthClient.formatNotifyTestError(502, 'notify_failed').includes('provider'),
      true,
    );
    assert.strictEqual(
      profileAuthClient.formatNotifyTestError(0, null, { network: true }).includes('connection'),
      true,
    );
    assert.ok(
      !profileAuthClient.formatNotifyTestError(400, 'notification_topic_not_configured')
        .includes('authenticate'),
    );
  }

  {
    const msg = profileAuthClient.formatProfileAuthError('profile_auth_required');
    assert.ok(!msg.includes('profile_auth_required'));
    assert.ok(msg.includes('Settings'));
  }

  {
    const masked = profileAuthClient.maskNtfyTopicForDisplay(TEST_TOPIC);
    assert.ok(masked.includes('••••'));
    assert.ok(!masked.includes(TEST_TOPIC));
  }

  {
    const first = profileAuthClient.mergeProfileAuthFetchOptions(TEST_PROFILE, { method: 'POST' });
    const second = profileAuthClient.mergeProfileAuthFetchOptions(TEST_PROFILE_B, { method: 'POST' });
    assert.notStrictEqual(
      first.fetchOptions.headers.Authorization,
      second.fetchOptions.headers.Authorization,
    );
  }

  {
    const publicClient = fs.readFileSync(path.join(__dirname, '../public/profile-auth-client.js'), 'utf8');
    assert.ok(publicClient.includes('mergeProfileAuthFetchOptions'));
    assert.ok(publicClient.includes('formatNotifyTestError'));
    assert.ok(!publicClient.includes(TEST_PROFILE));
  }

  console.log('profile auth frontend regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
