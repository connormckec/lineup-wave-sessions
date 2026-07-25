'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const profileAuthClient = require('../lib/profile-auth-client');
const profileAuth = require('../lib/profile-auth');

console.log('profile auth frontend regression');

const TEST_PROFILE = 'test-profile-alpha-001';
const TEST_TOPIC = 'testtopic001alpha';

function simulateSaveAndHeaders(savedCode, nextCode) {
  let inMemoryCode = savedCode;
  const setCode = (value) => {
    inMemoryCode = profileAuthClient.normalizeProfileCode(value);
  };
  const getCode = () => inMemoryCode || '';
  setCode(nextCode || savedCode);
  return profileAuthClient.buildProfileAuthHeaders(getCode(), { 'Content-Type': 'application/json' });
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
    const raw = '  My-Beta-Code  ';
    const normalized = profileAuthClient.normalizeProfileCode(raw);
    const auth = profileAuthClient.buildProfileAuthHeaders(raw);
    assert.strictEqual(auth.headers.Authorization, `Bearer ${normalized}`);
    const derived = profileAuth.deriveUserKeyFromProfileCodeSync(normalized);
    const diag = profileAuthClient.buildProfileAuthDiagnostic(raw);
    assert.strictEqual(diag.authHeaderPrepared, true);
    assert.strictEqual(diag.derivedUserKeyPrefix, `${derived.slice(0, 16)}…`);
    assert.strictEqual(diag.profileCodeLength, normalized.length);
  }

  {
    const first = simulateSaveAndHeaders(null, TEST_PROFILE);
    const second = simulateSaveAndHeaders(TEST_PROFILE, 'test-profile-beta-002');
    assert.strictEqual(first.headers.Authorization, `Bearer ${TEST_PROFILE}`);
    assert.strictEqual(second.headers.Authorization, 'Bearer test-profile-beta-002');
  }

  {
    const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
    const routes = [
      "fetchProfileApi('/api/watchlist'",
      "fetchProfileApi('/api/watchlist/sync'",
      "fetchProfileApi('/api/notification-profile'",
      "fetchProfileApi('/api/notify/test'",
      'fetchProfileApi(`/api/watchlist/${existing.id}`',
      'fetchProfileApi(`/api/watchlist/${id}`',
    ];
    for (const snippet of routes) {
      assert.ok(html.includes(snippet), `missing protected fetch: ${snippet}`);
    }
    assert.ok(html.includes('if (!auth.ok)'));
    assert.ok(html.includes('headers: auth.headers'));
    assert.ok(!html.match(/fetch\([^)]*Authorization:/));
    assert.ok(!html.includes('ap-surf-connor-2026'));
    assert.ok(html.includes('/profile-auth-client.js'));
    assert.ok(!html.includes('user_key: getUserKey()'));
    assert.ok(!html.includes('user_key: userKey'));
    assert.ok(html.includes('APP_SHELL_BUILD'));
  }

  {
    const html = fs.readFileSync(path.join(__dirname, '../public/index.html'), 'utf8');
    assert.ok(!html.includes('Subscribe to this exact topic in ntfy: ap-surf-connor'));
    assert.ok(html.includes('maskNtfyTopicForDisplay'));
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
    const publicClient = fs.readFileSync(path.join(__dirname, '../public/profile-auth-client.js'), 'utf8');
    assert.ok(!publicClient.includes('console.log'));
    assert.ok(!publicClient.includes(TEST_PROFILE));
  }

  console.log('profile auth frontend regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
