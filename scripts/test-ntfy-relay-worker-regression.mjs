import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const worker = await import(pathToFileURL(
  path.join(__dirname, '../cloudflare/ntfy-relay/src/index.js'),
).href);

const { __test } = worker;
const TEST_SECRET = 'b'.repeat(40);

console.log('ntfy relay worker regression');

{
  const masked = __test.maskTopic('testtopic001alpha');
  assert.ok(masked.includes('••••'));
  assert.ok(!masked.includes('testtopic001alpha'));
}

{
  const bad = __test.validateTopic('bad/topic');
  assert.equal(bad.ok, false);
  const good = __test.validateTopic('valid-topic_01');
  assert.equal(good.ok, true);
}

{
  const invalid = __test.validatePublishBody({ topic: 'x', message: 'hi', extra: true });
  assert.equal(invalid.ok, false);
  assert.equal(invalid.error, 'unexpected_field');

  const valid = __test.validatePublishBody({
    topic: 'valid-topic_01',
    title: 'Title',
    message: 'Hello',
    priority: 3,
    tags: ['wave'],
    clickUrl: 'https://example.test/',
  });
  assert.equal(valid.ok, true);
}

{
  assert.equal(await __test.verifySecret(TEST_SECRET, TEST_SECRET), true);
  assert.equal(await __test.verifySecret('wrong-secret-value', TEST_SECRET), false);
}

{
  const health = __test.handleHealth();
  assert.equal(health.status, 200);
  const body = await health.json();
  assert.equal(body.service, 'lineup-ntfy-relay');
  assert.equal(body.ok, true);
}

{
  const handler = worker.default;
  const notFound = await handler.fetch(new Request('https://relay.example.test/other', { method: 'GET' }), {
    NTFY_RELAY_SECRET: TEST_SECRET,
  });
  assert.equal(notFound.status, 404);

  const wrongMethod = await handler.fetch(new Request('https://relay.example.test/publish', { method: 'GET' }), {
    NTFY_RELAY_SECRET: TEST_SECRET,
  });
  assert.equal(wrongMethod.status, 404);

  const noAuth = await handler.fetch(new Request('https://relay.example.test/publish', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ topic: 'valid-topic_01', message: 'hello' }),
  }), { NTFY_RELAY_SECRET: TEST_SECRET });
  assert.equal(noAuth.status, 401);
}

console.log('ntfy relay worker regression: all tests passed');
