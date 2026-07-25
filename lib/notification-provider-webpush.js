'use strict';

const { URL } = require('url');
const webpush = require('web-push');
const { readWebPushConfig } = require('./web-push-config');

const DEFAULT_TIMEOUT_MS = 10_000;
const MAX_TITLE_LENGTH = 120;
const MAX_BODY_LENGTH = 280;
const MAX_TAG_LENGTH = 128;
const MAX_SESSION_KEY_LENGTH = 128;
const MAX_EVENT_TYPE_LENGTH = 64;
const VAPID_CONFIG_ERRORS = new Set([
  'vapid_keys_incomplete',
  'vapid_subject_invalid',
  'webpush_not_configured',
]);

function sanitizeText(value, maxLen) {
  if (value == null) return '';
  return String(value).replace(/[\r\n\u0000]/g, ' ').trim().slice(0, maxLen);
}

function validateSameOriginClickPath(raw, lineupOrigin) {
  if (raw == null || raw === '') return { ok: true, url: '/' };
  const text = sanitizeText(raw, 512);
  if (!text.startsWith('/')) return { ok: false, error: 'invalid_click_url' };
  if (text.includes('://') || text.startsWith('//')) return { ok: false, error: 'invalid_click_url' };
  try {
    const resolved = new URL(text, lineupOrigin || 'https://lineup.invalid');
    if (lineupOrigin) {
      const origin = new URL(lineupOrigin).origin;
      if (resolved.origin !== origin) return { ok: false, error: 'invalid_click_url' };
    }
    return { ok: true, url: `${resolved.pathname}${resolved.search}${resolved.hash}` || '/' };
  } catch {
    return { ok: false, error: 'invalid_click_url' };
  }
}

function buildPushPayload({
  title,
  message,
  clickUrl,
  eventType,
  sessionKey,
  isoDate,
  test = false,
  lineupOrigin,
} = {}) {
  const safeTitle = sanitizeText(title, MAX_TITLE_LENGTH) || 'Lineup';
  const safeBody = sanitizeText(message, MAX_BODY_LENGTH);
  if (!safeBody) return { ok: false, error: 'missing_message' };

  const safeEventType = sanitizeText(eventType || (test ? 'test' : 'alert'), MAX_EVENT_TYPE_LENGTH);
  const safeSessionKey = sanitizeText(sessionKey || 'test', MAX_SESSION_KEY_LENGTH);
  const tag = sanitizeText(`lineup:${safeSessionKey}:${safeEventType}`, MAX_TAG_LENGTH);
  const click = validateSameOriginClickPath(clickUrl, lineupOrigin);
  if (!click.ok) return click;

  const payload = {
    title: test ? `[TEST] ${safeTitle}` : safeTitle,
    body: safeBody,
    tag,
    eventType: safeEventType,
    sessionKey: safeSessionKey,
    isoDate: isoDate ? sanitizeText(isoDate, 32) : null,
    url: click.url,
    test: !!test,
  };

  const json = JSON.stringify(payload);
  if (Buffer.byteLength(json, 'utf8') > 3500) {
    return { ok: false, error: 'payload_too_large' };
  }
  return { ok: true, payload, payloadJson: json };
}

function classifyWebPushResult(result) {
  if (result?.ok) {
    return {
      ok: true,
      transient: false,
      providerStatus: result.statusCode,
      error: null,
      deactivateSubscription: false,
      vapidConfigError: false,
    };
  }

  const status = result?.statusCode ?? null;
  const error = result?.error || 'webpush_failed';

  if (status === 404 || status === 410) {
    return {
      ok: false,
      transient: false,
      providerStatus: status,
      error: 'subscription_expired',
      deactivateSubscription: true,
      vapidConfigError: false,
    };
  }

  if (status === 413) {
    return {
      ok: false,
      transient: false,
      providerStatus: status,
      error: 'payload_too_large',
      deactivateSubscription: false,
      vapidConfigError: false,
    };
  }

  if (status === 401 || status === 403) {
    return {
      ok: false,
      transient: true,
      providerStatus: status,
      error: 'vapid_configuration_error',
      deactivateSubscription: false,
      vapidConfigError: true,
    };
  }

  if (status === 429) {
    return {
      ok: false,
      transient: true,
      providerStatus: status,
      error: 'rate_limited',
      deactivateSubscription: false,
      vapidConfigError: false,
      retryAfterMs: result?.retryAfterMs ?? null,
    };
  }

  if (status >= 500) {
    return {
      ok: false,
      transient: true,
      providerStatus: status,
      error: 'push_service_error',
      deactivateSubscription: false,
      vapidConfigError: false,
    };
  }

  if (error === 'timeout' || error === 'network_error') {
    return {
      ok: false,
      transient: true,
      providerStatus: null,
      error,
      deactivateSubscription: false,
      vapidConfigError: false,
    };
  }

  if (VAPID_CONFIG_ERRORS.has(error)) {
    return {
      ok: false,
      transient: false,
      providerStatus: null,
      error,
      deactivateSubscription: false,
      vapidConfigError: true,
    };
  }

  return {
    ok: false,
    transient: false,
    providerStatus: status,
    error,
    deactivateSubscription: false,
    vapidConfigError: false,
  };
}

let vapidConfigured = false;

function ensureVapidConfigured(config = readWebPushConfig()) {
  if (vapidConfigured && config.configured) return { ok: true, config };
  if (!config.configured) {
    return { ok: false, error: config.configError || 'webpush_not_configured' };
  }
  webpush.setVapidDetails(config.subject, config.publicKey, config.privateKey);
  vapidConfigured = true;
  return { ok: true, config };
}

function resetVapidConfiguredForTests() {
  vapidConfigured = false;
}

async function sendWebPush({
  subscription,
  title,
  message,
  clickUrl,
  eventType,
  sessionKey,
  isoDate,
  deliveryId = null,
  test = false,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  lineupOrigin = null,
  _webPushConfig = readWebPushConfig(),
  _sendFn = webpush.sendNotification.bind(webpush),
} = {}) {
  if (!subscription?.endpoint || !subscription?.keys?.p256dh || !subscription?.keys?.auth) {
    return classifyWebPushResult({ ok: false, error: 'subscription_invalid' });
  }

  const vapid = ensureVapidConfigured(_webPushConfig);
  if (!vapid.ok) {
    return classifyWebPushResult({ ok: false, error: vapid.error });
  }

  const built = buildPushPayload({
    title,
    message,
    clickUrl,
    eventType,
    sessionKey,
    isoDate,
    test,
    lineupOrigin,
  });
  if (!built.ok) {
    return classifyWebPushResult({ ok: false, error: built.error });
  }

  const pushSubscription = {
    endpoint: subscription.endpoint,
    keys: {
      p256dh: subscription.keys.p256dh,
      auth: subscription.keys.auth,
    },
  };

  try {
    const result = await Promise.race([
      _sendFn(pushSubscription, built.payloadJson, {
        TTL: 60 * 60,
        urgency: 'high',
      }),
      new Promise((_, reject) => {
        setTimeout(() => reject(Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' })), timeoutMs);
      }),
    ]);

    return classifyWebPushResult({
      ok: true,
      statusCode: result?.statusCode || 201,
      deliveryId,
    });
  } catch (err) {
    const statusCode = err?.statusCode ?? null;
    if (statusCode) {
      return classifyWebPushResult({
        ok: false,
        statusCode,
        error: 'provider_http_error',
        retryAfterMs: parseRetryAfterMs(err?.headers?.['retry-after']),
        deliveryId,
      });
    }
    if (err?.code === 'ETIMEDOUT' || err?.name === 'TimeoutError') {
      return classifyWebPushResult({ ok: false, error: 'timeout', deliveryId });
    }
    return classifyWebPushResult({ ok: false, error: 'network_error', deliveryId });
  }
}

function parseRetryAfterMs(raw) {
  if (raw == null) return null;
  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const date = Date.parse(String(raw));
  if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  return null;
}

module.exports = {
  DEFAULT_TIMEOUT_MS,
  sanitizeText,
  validateSameOriginClickPath,
  buildPushPayload,
  classifyWebPushResult,
  ensureVapidConfigured,
  resetVapidConfiguredForTests,
  sendWebPush,
};
