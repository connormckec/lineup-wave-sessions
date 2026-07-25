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
const {
  resolveProviderSettings,
  resolveTransportMode,
  buildStartupLogLines,
} = require('./notification-provider-config');
const { classifyRelayHttpResponse } = require('./notification-provider-relay');

const NTFY_BASE_URL = 'https://ntfy.sh';
const NTFY_HOST = 'ntfy.sh';
const DEFAULT_TIMEOUT_MS = 10_000;
const DIRECT_TRANSPORT_NAME = 'https.request';
const RELAY_TRANSPORT_NAME = 'relay';

function encodeTopic(topic) {
  return encodeURIComponent(String(topic || '').trim());
}

function resolveHttpFamily() {
  const raw = String(process.env.NTFY_HTTP_FAMILY || '').trim().toLowerCase();
  if (raw === '4' || raw === 'ipv4') return 4;
  if (raw === '6' || raw === 'ipv6') return 6;
  return undefined;
}

function getTransportName(mode = resolveTransportMode()) {
  return mode === 'relay' ? RELAY_TRANSPORT_NAME : DIRECT_TRANSPORT_NAME;
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

function buildRelayPayload({ topic, title, message, testEvent, clickUrl }) {
  return {
    topic,
    title: sanitizeHeaderValue(title) || 'AP Session Alert',
    message,
    priority: 3,
    tags: [testEvent ? 'test_tube' : 'wave'],
    clickUrl: clickUrl || undefined,
  };
}

function httpsJsonRequest({
  hostname,
  path,
  method,
  headers,
  body,
  timeoutMs,
  family,
}) {
  return new Promise((resolve, reject) => {
    const payload = body == null ? '' : JSON.stringify(body);
    let timeoutTriggered = false;
    let settled = false;

    const req = https.request({
      hostname,
      port: 443,
      path,
      method,
      family,
      headers: {
        ...headers,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload, 'utf8'),
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

    if (payload) req.write(payload);
    req.end();
  });
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

async function postRelayRequest({
  relayUrl,
  relaySecret,
  payload,
  timeoutMs,
}) {
  const parsed = new URL(relayUrl);
  return httpsJsonRequest({
    hostname: parsed.hostname,
    path: parsed.pathname + parsed.search,
    method: 'POST',
    headers: {
      Authorization: `Bearer ${relaySecret}`,
    },
    body: payload,
    timeoutMs,
  });
}

function mapHttpResponse(response, transport, requestMeta) {
  if (response.statusCode >= 200 && response.statusCode < 300) {
    return {
      ok: true,
      providerStatus: response.statusCode,
      error: null,
      transient: false,
      transport,
    };
  }

  const classified = transport === RELAY_TRANSPORT_NAME
    ? classifyRelayHttpResponse(response.statusCode)
    : {
      ok: false,
      error: 'provider_http_error',
      transient: response.statusCode === 408
        || response.statusCode === 429
        || response.statusCode >= 500,
    };

  logProviderFailure('provider_http_error', {
    statusCode: response.statusCode,
    responseSnippet: (response.responseText || '').slice(0, 120) || null,
  }, requestMeta);

  return {
    ok: false,
    providerStatus: response.statusCode,
    error: classified.error,
    transient: classified.transient,
    transport,
  };
}

async function sendViaDirect({
  topic,
  safeTitle,
  safeMessage,
  clickUrl,
  testEvent,
  timeoutMs,
  family,
  deliveryId,
}) {
  const headers = buildNtfyHeaders({
    title: safeTitle,
    testEvent,
    clickUrl,
  });

  const requestMeta = buildSafeRequestMetadata({
    destination: topic,
    title: safeTitle,
    message: safeMessage,
    clickUrl,
    timeoutMs,
    transport: DIRECT_TRANSPORT_NAME,
    family,
    providerHost: NTFY_HOST,
  });
  if (deliveryId) requestMeta.deliveryId = deliveryId;

  const response = await postNtfyRequest({
    topic,
    message: safeMessage,
    headers,
    timeoutMs,
    family,
  });
  return mapHttpResponse(response, DIRECT_TRANSPORT_NAME, requestMeta);
}

async function sendViaRelay({
  topic,
  safeTitle,
  safeMessage,
  clickUrl,
  testEvent,
  timeoutMs,
  relaySettings,
  deliveryId,
}) {
  const payload = buildRelayPayload({
    topic,
    title: safeTitle,
    message: safeMessage,
    testEvent,
    clickUrl,
  });

  const requestMeta = buildSafeRequestMetadata({
    destination: topic,
    title: safeTitle,
    message: safeMessage,
    clickUrl,
    timeoutMs,
    transport: RELAY_TRANSPORT_NAME,
    family: null,
    providerHost: relaySettings.relayHost,
  });
  if (deliveryId) requestMeta.deliveryId = deliveryId;

  const response = await postRelayRequest({
    relayUrl: relaySettings.relayUrl,
    relaySecret: relaySettings.relaySecret,
    payload,
    timeoutMs,
  });
  return mapHttpResponse(response, RELAY_TRANSPORT_NAME, requestMeta);
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
  _providerSettings = resolveProviderSettings(),
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

  const settings = _providerSettings;
  if (!settings.ok) {
    return {
      ok: false,
      providerStatus: null,
      error: settings.error || 'relay_not_configured',
      transient: false,
      transport: getTransportName(settings.mode),
    };
  }

  const transport = getTransportName(settings.mode);

  try {
    if (settings.mode === 'relay') {
      return await sendViaRelay({
        topic,
        safeTitle,
        safeMessage,
        clickUrl: click.clickUrl,
        testEvent,
        timeoutMs,
        relaySettings: settings,
        deliveryId,
      });
    }

    return await sendViaDirect({
      topic,
      safeTitle,
      safeMessage,
      clickUrl: click.clickUrl,
      testEvent,
      timeoutMs,
      family: _transportFamily,
      deliveryId,
    });
  } catch (err) {
    const timeoutTriggered = !!err.timeoutTriggered
      || err.code === 'ETIMEDOUT'
      || err.name === 'TimeoutError'
      || err.name === 'AbortError';
    const host = settings.mode === 'relay' ? settings.relayHost : NTFY_HOST;
    const requestMeta = buildSafeRequestMetadata({
      destination: topic,
      title: safeTitle,
      message: safeMessage,
      clickUrl: click.clickUrl,
      timeoutMs,
      transport,
      family: settings.mode === 'relay' ? null : _transportFamily,
      providerHost: host,
    });
    if (deliveryId) requestMeta.deliveryId = deliveryId;

    const diagnostic = extractSafeTransportError(err, {
      timeoutTriggered,
      providerHost: host,
    });
    const classified = classifyTransportFailure(err, { timeoutTriggered });
    logProviderFailure('transport_failure', diagnostic, requestMeta);
    return {
      ok: false,
      providerStatus: null,
      error: classified.error,
      transient: classified.transient,
      transport,
    };
  }
}

module.exports = {
  NTFY_BASE_URL,
  NTFY_HOST,
  DEFAULT_TIMEOUT_MS,
  DIRECT_TRANSPORT_NAME,
  RELAY_TRANSPORT_NAME,
  TRANSPORT_NAME: DIRECT_TRANSPORT_NAME,
  getTransportName,
  encodeTopic,
  resolveHttpFamily,
  resolveProviderSettings,
  buildStartupLogLines,
  sanitizeHeaderValue,
  validateClickUrl,
  buildNtfyHeaders,
  buildRelayPayload,
  postNtfyRequest,
  postRelayRequest,
  httpsJsonRequest,
  sendNotification,
};
