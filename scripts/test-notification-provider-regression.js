'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const https = require('https');
const { EventEmitter } = require('events');
const providerErrors = require('../lib/notification-provider-errors');
const notificationProvider = require('../lib/notification-provider');
const notificationProviderProbe = require('../lib/notification-provider-probe');
const adminAuth = require('../lib/admin-auth');

console.log('notification provider regression');

const TEST_TOPIC = 'testtopic001alpha';

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
    const err = Object.assign(new TypeError('fetch failed'), {
      cause: Object.assign(new Error('connect failed'), {
        code: 'ECONNREFUSED',
        errno: -61,
        syscall: 'connect',
        address: '2606:4700:3030::6815:5001',
        port: 443,
      }),
    });
    const diagnostic = providerErrors.extractSafeTransportError(err, { timeoutTriggered: false });
    assert.strictEqual(diagnostic.causeCode, 'ECONNREFUSED');
    assert.strictEqual(diagnostic.causeSyscall, 'connect');
    assert.ok(diagnostic.causeAddress.includes('••••'));
    assert.strictEqual(diagnostic.timeoutTriggered, false);
    assert.ok(diagnostic.nodeVersion.startsWith('v'));
  }

  {
    const meta = providerErrors.buildSafeRequestMetadata({
      destination: TEST_TOPIC,
      title: 'Title',
      message: 'Hello',
      clickUrl: 'https://example.test/?date=2026-08-20',
      timeoutMs: 10000,
      transport: 'https.request',
      family: 4,
    });
    assert.ok(meta.destinationMasked.includes('••••'));
    assert.ok(!meta.destinationMasked.includes(TEST_TOPIC));
    assert.strictEqual(meta.clickHost, 'example.test');
    assert.strictEqual(meta.topicLength, TEST_TOPIC.length);
  }

  {
    const timeout = providerErrors.classifyTransportFailure(
      Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' }),
      { timeoutTriggered: true },
    );
    assert.strictEqual(timeout.error, 'provider_timeout');

    const dns = providerErrors.classifyTransportFailure(
      Object.assign(new Error('not found'), { code: 'ENOTFOUND' }),
    );
    assert.strictEqual(dns.error, 'provider_dns_failed');

    const tls = providerErrors.classifyTransportFailure(
      Object.assign(new Error('certificate has expired'), { code: 'CERT_HAS_EXPIRED' }),
    );
    assert.strictEqual(tls.error, 'provider_tls_failed');

    const generic = providerErrors.classifyTransportFailure(
      Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } }),
    );
    assert.strictEqual(generic.error, 'provider_transport_failed');
  }

  {
    const badClick = notificationProvider.validateClickUrl('not-a-url');
    assert.strictEqual(badClick.ok, false);
    const newline = notificationProvider.validateClickUrl('https://example.test/\n');
    assert.strictEqual(newline.ok, false);
    const good = notificationProvider.validateClickUrl('https://example.test/');
    assert.strictEqual(good.ok, true);
  }

  {
    const headers = notificationProvider.buildNtfyHeaders({
      title: 'Alert',
      testEvent: true,
      clickUrl: 'https://example.test/',
    });
    assert.ok(headers.Title.startsWith('[TEST]'));
    assert.strictEqual(headers.Click, 'https://example.test/');
    assert.strictEqual(headers.Priority, 'high');
  }

  {
    await withMockHttpsRequest((options, cb) => {
      assert.strictEqual(options.hostname, 'ntfy.sh');
      assert.strictEqual(options.method, 'POST');
      assert.strictEqual(options.family, undefined);
      const req = new EventEmitter();
      req.write = () => true;
      req.end = () => {
        cb(mockHttpsResponse(200));
      };
      req.destroy = () => {};
      process.nextTick(() => req.emit('response', mockHttpsResponse(200)));
      return req;
    }, async () => {
      const result = await notificationProvider.sendNotification({
        destination: TEST_TOPIC,
        title: 'Alert',
        message: 'Hello',
        clickUrl: 'https://example.test/',
        testEvent: true,
      });
      assert.strictEqual(result.ok, true);
      assert.strictEqual(result.transport, 'https.request');
    });
  }

  {
    await withMockHttpsRequest((_options, cb) => {
      const req = new EventEmitter();
      req.write = () => true;
      req.end = () => {
        cb(mockHttpsResponse(503, 'overloaded'));
      };
      req.destroy = () => {};
      process.nextTick(() => req.emit('response', mockHttpsResponse(503, 'overloaded')));
      return req;
    }, async () => {
      const result = await notificationProvider.sendNotification({
        destination: TEST_TOPIC,
        title: 'Alert',
        message: 'Hello',
      });
      assert.strictEqual(result.ok, false);
      assert.strictEqual(result.error, 'provider_http_error');
      assert.strictEqual(result.providerStatus, 503);
      assert.strictEqual(result.transient, true);
    });
  }

  {
    await withMockHttpsRequest((options, cb) => {
      assert.strictEqual(options.family, 4);
      const req = new EventEmitter();
      req.write = () => true;
      req.end = () => {
        process.nextTick(() => {
          req.emit('error', Object.assign(new Error('connect refused'), { code: 'ECONNREFUSED' }));
        });
      };
      req.destroy = () => {};
      return req;
    }, async () => {
      const prev = process.env.NTFY_HTTP_FAMILY;
      process.env.NTFY_HTTP_FAMILY = '4';
      try {
        const result = await notificationProvider.sendNotification({
          destination: TEST_TOPIC,
          title: 'Alert',
          message: 'Hello',
        });
        assert.strictEqual(result.ok, false);
        assert.strictEqual(result.error, 'provider_transport_failed');
      } finally {
        if (prev == null) delete process.env.NTFY_HTTP_FAMILY;
        else process.env.NTFY_HTTP_FAMILY = prev;
      }
    });
  }

  {
    assert.strictEqual(notificationProvider.resolveHttpFamily(), undefined);
    process.env.NTFY_HTTP_FAMILY = '4';
    assert.strictEqual(notificationProvider.resolveHttpFamily(), 4);
    delete process.env.NTFY_HTTP_FAMILY;
  }

  {
    const masked = notificationProviderProbe.maskLookupResults([
      { address: '104.21.55.129', family: 4 },
      { address: '2606:4700:3030::6815:5001', family: 6 },
    ]);
    assert.ok(masked[0].address.includes('••••'));
    assert.ok(masked[1].address.includes('••••'));
  }

  {
    const invalid = await notificationProvider.sendNotification({
      destination: TEST_TOPIC,
      title: 'Alert',
      message: 'Hello',
      clickUrl: 'https://example.test/\nattack',
    });
    assert.strictEqual(invalid.ok, false);
    assert.strictEqual(invalid.error, 'invalid_click_url');
  }

  {
    const serverSrc = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    assert.ok(serverSrc.includes("app.use('/api/admin', requireAdminToken)"));
    const probeIdx = serverSrc.indexOf('/api/admin/notifications/provider-probe');
    const adminGuardIdx = serverSrc.indexOf("app.use('/api/admin', requireAdminToken)");
    assert.ok(probeIdx > adminGuardIdx);
    assert.ok(!serverSrc.includes("method: 'POST'") || serverSrc.includes('provider-probe'));
  }

  {
    const probeSrc = fs.readFileSync(path.join(__dirname, '../lib/notification-provider-probe.js'), 'utf8');
    assert.ok(probeSrc.includes("method: 'HEAD'"));
    assert.ok(!probeSrc.includes('encodeTopic'));
    assert.ok(!probeSrc.includes('POST'));
  }

  {
    const blocked = adminAuth.validateAdminToken('', 'secret');
    assert.strictEqual(blocked.ok, false);
    const ok = adminAuth.validateAdminToken('secret', 'secret');
    assert.strictEqual(ok.ok, true);
  }

  {
    const pipelineSrc = fs.readFileSync(path.join(__dirname, '../lib/notification-pipeline.js'), 'utf8');
    assert.ok(pipelineSrc.includes("require('./notification-provider')"));
    assert.ok(pipelineSrc.includes('notificationProvider.sendNotification'));
  }

  console.log('notification provider regression: all tests passed');
}

runTests().catch((err) => {
  console.error(err);
  process.exit(1);
});
