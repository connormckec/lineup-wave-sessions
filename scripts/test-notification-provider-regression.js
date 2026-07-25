'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const https = require('https');
const { EventEmitter } = require('events');
const providerConfig = require('../lib/notification-provider-config');
const providerRelay = require('../lib/notification-provider-relay');
const notificationProvider = require('../lib/notification-provider');
const providerErrors = require('../lib/notification-provider-errors');

console.log('notification provider regression');

const TEST_TOPIC = 'testtopic001alpha';
const TEST_SECRET = 'a'.repeat(32);
const TEST_RELAY_URL = 'https://relay.example.test/publish';

function mockHttpsResponse(statusCode, body = '') {
  const res = new EventEmitter();
  res.statusCode = statusCode;
  res.setEncoding = () => {};
  process.nextTick(() => {
    if (body) res.emit('data', body);
    res.emit('end');
  });
  return res;
}

function withMockHttpsRequest(implementation, fn) {
  const original = https.request;
  https.request = implementation;
  return Promise.resolve()
    .then(fn)
    .finally(() => {
      https.request = original;
    });
}

async function runTests() {
  {
    assert.strictEqual(providerConfig.resolveTransportMode({ NTFY_TRANSPORT_MODE: 'direct' }), 'direct');
    assert.strictEqual(providerConfig.resolveTransportMode({ NTFY_TRANSPORT_MODE: 'relay' }), 'relay');
    assert.strictEqual(providerConfig.resolveTransportMode({}), 'direct');
  }

  {
    const bad = providerConfig.validateRelayUrl('http://relay.example.test/publish');
    assert.strictEqual(bad.ok, false);
    const localhost = providerConfig.validateRelayUrl('https://localhost/publish');
    assert.strictEqual(localhost.ok, false);
    const good = providerConfig.validateRelayUrl(TEST_RELAY_URL);
    assert.strictEqual(good.ok, true);
    assert.strictEqual(good.relayHost, 'relay.example.test');
  }

  {
    const short = providerConfig.validateRelaySecret('short');
    assert.strictEqual(short.ok, false);
    const good = providerConfig.validateRelaySecret(TEST_SECRET);
    assert.strictEqual(good.ok, true);
  }

  {
    const settings = providerConfig.resolveProviderSettings({
      NTFY_TRANSPORT_MODE: 'relay',
      NTFY_RELAY_URL: TEST_RELAY_URL,
      NTFY_RELAY_SECRET: TEST_SECRET,
    });
    assert.strictEqual(settings.mode, 'relay');
    assert.strictEqual(settings.relayHost, 'relay.example.test');
  }

  {
    const missing = providerConfig.resolveProviderSettings({
      NTFY_TRANSPORT_MODE: 'relay',
      NTFY_RELAY_URL: TEST_RELAY_URL,
    });
    assert.strictEqual(missing.ok, false);
  }

  {
    assert.strictEqual(providerRelay.classifyRelayHttpResponse(200).ok, true);
    assert.strictEqual(providerRelay.classifyRelayHttpResponse(400).transient, false);
    assert.strictEqual(providerRelay.classifyRelayHttpResponse(401).error, 'relay_auth_failed');
    assert.strictEqual(providerRelay.classifyRelayHttpResponse(429).transient, true);
    assert.strictEqual(providerRelay.classifyRelayHttpResponse(500).transient, true);
    assert.strictEqual(providerRelay.classifyRelayHttpResponse(504).error, 'provider_timeout');
  }

  {
    let relayCalled = false;
    let directCalled = false;
    await withMockHttpsRequest((options, cb) => {
      if (options.hostname === 'relay.example.test') relayCalled = true;
      if (options.hostname === 'ntfy.sh') directCalled = true;
      const req = new EventEmitter();
      req.write = () => true;
      req.end = () => {
        process.nextTick(() => req.emit('response', mockHttpsResponse(200)));
      };
      req.destroy = () => {};
      return req;
    }, async () => {
      const relaySettings = providerConfig.resolveProviderSettings({
        NTFY_TRANSPORT_MODE: 'relay',
        NTFY_RELAY_URL: TEST_RELAY_URL,
        NTFY_RELAY_SECRET: TEST_SECRET,
      });
      const relayResult = await notificationProvider.sendNotification({
        destination: TEST_TOPIC,
        title: 'Alert',
        message: 'Hello',
        _providerSettings: relaySettings,
      });
      assert.strictEqual(relayResult.ok, true);
      assert.strictEqual(relayResult.transport, 'relay');
      assert.strictEqual(relayCalled, true);
      assert.strictEqual(directCalled, false);

      const directResult = await notificationProvider.sendNotification({
        destination: TEST_TOPIC,
        title: 'Alert',
        message: 'Hello',
        _providerSettings: providerConfig.resolveProviderSettings({ NTFY_TRANSPORT_MODE: 'direct' }),
      });
      assert.strictEqual(directResult.transport, 'https.request');
      assert.strictEqual(directCalled, true);
    });
  }

  {
    let authHeader = null;
    let requestBody = null;
    await withMockHttpsRequest((options, cb) => {
      authHeader = options.headers.Authorization;
      const req = new EventEmitter();
      req.write = (chunk) => {
        requestBody = chunk.toString();
      };
      req.end = () => {
        process.nextTick(() => req.emit('response', mockHttpsResponse(200)));
      };
      req.destroy = () => {};
      return req;
    }, async () => {
      await notificationProvider.sendNotification({
        destination: TEST_TOPIC,
        title: 'Alert',
        message: 'Hello',
        _providerSettings: providerConfig.resolveProviderSettings({
          NTFY_TRANSPORT_MODE: 'relay',
          NTFY_RELAY_URL: TEST_RELAY_URL,
          NTFY_RELAY_SECRET: TEST_SECRET,
        }),
      });
      assert.ok(authHeader.startsWith('Bearer '));
      assert.ok(!authHeader.includes(TEST_SECRET.slice(0, 10)) || authHeader === `Bearer ${TEST_SECRET}`);
      const parsed = JSON.parse(requestBody);
      assert.strictEqual(parsed.topic, TEST_TOPIC);
      assert.ok(!JSON.stringify(parsed).includes('user_key'));
    });
  }

  {
    await withMockHttpsRequest((_options, cb) => {
      const req = new EventEmitter();
      req.write = () => true;
      req.end = () => {
        process.nextTick(() => req.emit('response', mockHttpsResponse(401, '{"error":"unauthorized"}')));
      };
      req.destroy = () => {};
      return req;
    }, async () => {
      const result = await notificationProvider.sendNotification({
        destination: TEST_TOPIC,
        title: 'Alert',
        message: 'Hello',
        _providerSettings: providerConfig.resolveProviderSettings({
          NTFY_TRANSPORT_MODE: 'relay',
          NTFY_RELAY_URL: TEST_RELAY_URL,
          NTFY_RELAY_SECRET: TEST_SECRET,
        }),
      });
      assert.strictEqual(result.ok, false);
      assert.strictEqual(result.error, 'relay_auth_failed');
      assert.strictEqual(result.transient, false);
    });
  }

  {
    const invalid = await notificationProvider.sendNotification({
      destination: 'bad/topic',
      title: 'Alert',
      message: 'Hello',
      _providerSettings: providerConfig.resolveProviderSettings({ NTFY_TRANSPORT_MODE: 'direct' }),
    });
    assert.strictEqual(invalid.ok, false);
  }

  {
    const meta = providerErrors.buildSafeRequestMetadata({
      destination: TEST_TOPIC,
      title: 'Title',
      message: 'Hello',
      timeoutMs: 10000,
      transport: 'relay',
      providerHost: 'relay.example.test',
    });
    assert.ok(meta.destinationMasked.includes('••••'));
    assert.strictEqual(meta.providerHost, 'relay.example.test');
  }

  {
    const pipelineSrc = fs.readFileSync(path.join(__dirname, '../lib/notification-pipeline.js'), 'utf8');
    assert.ok(pipelineSrc.includes('notificationProvider.sendNotification'));
    const serverSrc = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    assert.ok(serverSrc.includes('/api/admin/notifications/relay-publish-test'));
    assert.ok(!serverSrc.includes('NODE_TLS_REJECT_UNAUTHORIZED'));
  }

  console.log('notification provider regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
