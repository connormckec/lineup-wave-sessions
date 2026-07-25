'use strict';

const https = require('https');
const { URL } = require('url');
const { validateNtfyTopic } = require('./notification-topic');
const {
  extractSafeTransportError,
  buildSafeRequestMetadata,
  classifyTransportFailure,
  logProviderFailure,
} = require('./notification-provider-errors');

const NTFY_BASE_URL = 'https://ntfy.sh';
const NTFY_HOST = 'ntfy.sh';
const DEFAULT_TIMEOUT_MS = 10_000;
const TRANSPORT_NAME = 'https.request';

function encodeTopic(topic) {
  return encodeURIComponent(String(topic || '').trim());
}

function resolveHttpFamily() {
  const raw = String(process.env.NTFY_HTTP_FAMILY || '').trim().toLowerCase();
  if (raw === '4' || raw === 'ipv4') return 4;
  if (raw === '6' || raw === 'ipv6') return 6;
  return undefined;
}

function sanitizeHeaderValue(value) {
  if (value == null) return null;
  const text = String(value).replace(/[\r\n\u0000]/g, '').trim();
  return text || null;
}

function validateClickUrl(raw) {
  if (raw == null || raw === '') return { ok: true, clickUrl: null };
  const text = sanitizeHeaderValue(raw);
  if (!text) return { ok: true, clickUrl: null };
  if (/[\r\n\u0000]/.test(String(raw))) {
    return { ok: false, error: 'invalid_click_url' };
  }
  try {
    const parsed = new URL(text);
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
      return { ok: false, error: 'invalid_click_url' };
    }
    return { ok: true, clickUrl: parsed.toString() };
  } catch {
    return { ok: false, error: 'invalid_click_url' };
  }
}

function buildNtfyHeaders({ title, testEvent, clickUrl }) {
  const safeTitle = sanitizeHeaderValue(title) || 'AP Session Alert';
  const headers = {
    Title: sanitizeHeaderValue(testEvent ? `[TEST] ${safeTitle}` : safeTitle),
    Priority: sanitizeHeaderValue('high'),
    Tags: sanitizeHeaderValue(testEvent ? 'test_tube' : 'wave'),
  };
  if (clickUrl) headers.Click = sanitizeHeaderValue(clickUrl);
  for (const key of Object.keys(headers)) {
    if (headers[key] == null) delete headers[key];
  }
  return headers;
}

function postNtfyRequest({
  topic,
  message,
  headers,
  timeoutMs,
  family,
}) {
  return new Promise((resolve, reject) => {
    const body = String(message ?? '');
    const path = `/${encodeTopic(topic)}`;
    let timeoutTriggered = false;
    let settled = false;

    const req = https.request({
      hostname: NTFY_HOST,
      port: 443,
      path,
      method: 'POST',
      family,
      headers: {
        ...headers,
        'Content-Length': Buffer.byteLength(body, 'utf8'),
      },
    }, (res) => {
      let responseText = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        if (responseText.length < 500) responseText += chunk;
      });
      res.on('end', () => {
        if (settled) return;
        settled = true;
        resolve({
          statusCode: res.statusCode,
          responseText: responseText.trim(),
        });
      });
    });

    const timer = setTimeout(() => {
      timeoutTriggered = true;
      const timeoutErr = Object.assign(new Error('timeout'), { code: 'ETIMEDOUT', name: 'TimeoutError' });
      if (typeof req.destroy === 'function') req.destroy(timeoutErr);
      else req.emit('error', timeoutErr);
    }, timeoutMs);

    req.on('error', (err) => {
      clearTimeout(timer);
      if (settled) return;
      settled = true;
      err.timeoutTriggered = timeoutTriggered;
      reject(err);
    });

    req.on('close', () => {
      clearTimeout(timer);
    });

    req.write(body);
    req.end();
  });
}

async function sendNotification({
  provider = 'ntfy',
  destination,
  title,
  message,
  clickUrl,
  deliveryId = null,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  testEvent = false,
  _transportFamily = resolveHttpFamily(),
} = {}) {
  if (provider !== 'ntfy') {
    return { ok: false, providerStatus: null, error: 'unsupported_provider', transient: false };
  }

  const topicValidation = validateNtfyTopic(destination);
  if (!topicValidation.ok) {
    return { ok: false, providerStatus: null, error: topicValidation.error, transient: false };
  }
  const topic = topicValidation.topic;

  const safeTitle = sanitizeHeaderValue(title);
  const safeMessage = sanitizeHeaderValue(message);
  if (!safeMessage) {
    return { ok: false, providerStatus: null, error: 'missing_message', transient: false };
  }

  const click = validateClickUrl(clickUrl);
  if (!click.ok) {
    return { ok: false, providerStatus: null, error: click.error, transient: false };
  }

  const headers = buildNtfyHeaders({
    title: safeTitle,
    testEvent,
    clickUrl: click.clickUrl,
  });

  const requestMeta = buildSafeRequestMetadata({
    destination: topic,
    title: safeTitle,
    message: safeMessage,
    clickUrl: click.clickUrl,
    timeoutMs,
    transport: TRANSPORT_NAME,
    family: _transportFamily,
  });

  if (deliveryId) {
    requestMeta.deliveryId = deliveryId;
  }

  try {
    const response = await postNtfyRequest({
      topic,
      message: safeMessage,
      headers,
      timeoutMs,
      family: _transportFamily,
    });

    if (response.statusCode >= 200 && response.statusCode < 300) {
      return {
        ok: true,
        providerStatus: response.statusCode,
        error: null,
        transient: false,
        transport: TRANSPORT_NAME,
      };
    }

    const transient = response.statusCode === 408
      || response.statusCode === 429
      || response.statusCode >= 500;

    logProviderFailure('provider_http_error', {
      statusCode: response.statusCode,
      responseSnippet: (response.responseText || '').slice(0, 120) || null,
    }, requestMeta);

    return {
      ok: false,
      providerStatus: response.statusCode,
      error: 'provider_http_error',
      transient,
      transport: TRANSPORT_NAME,
    };
  } catch (err) {
    const timeoutTriggered = !!err.timeoutTriggered
      || err.code === 'ETIMEDOUT'
      || err.name === 'TimeoutError'
      || err.name === 'AbortError';
    const diagnostic = extractSafeTransportError(err, {
      timeoutTriggered,
      providerHost: NTFY_HOST,
    });
    const classified = classifyTransportFailure(err, { timeoutTriggered });
    logProviderFailure('transport_failure', diagnostic, requestMeta);
    return {
      ok: false,
      providerStatus: null,
      error: classified.error,
      transient: classified.transient,
      transport: TRANSPORT_NAME,
    };
  }
}

module.exports = {
  NTFY_BASE_URL,
  NTFY_HOST,
  DEFAULT_TIMEOUT_MS,
  TRANSPORT_NAME,
  encodeTopic,
  resolveHttpFamily,
  sanitizeHeaderValue,
  validateClickUrl,
  buildNtfyHeaders,
  postNtfyRequest,
  sendNotification,
};
