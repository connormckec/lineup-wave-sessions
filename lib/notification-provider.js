'use strict';

const NTFY_BASE_URL = 'https://ntfy.sh';
const DEFAULT_TIMEOUT_MS = 10_000;

function encodeTopic(topic) {
  return encodeURIComponent(String(topic || '').trim());
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
} = {}) {
  if (provider !== 'ntfy') {
    return { ok: false, providerStatus: null, error: 'unsupported_provider', transient: false };
  }

  const topic = String(destination || '').trim();
  if (!topic) {
    return { ok: false, providerStatus: null, error: 'missing_destination', transient: false };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const headers = {
    Title: testEvent ? `[TEST] ${title}` : title,
    Priority: 'high',
    Tags: testEvent ? 'test_tube' : 'wave',
  };
  if (clickUrl) headers.Click = clickUrl;

  try {
    const response = await fetch(`${NTFY_BASE_URL}/${encodeTopic(topic)}`, {
      method: 'POST',
      headers,
      body: message,
      signal: controller.signal,
    });
    clearTimeout(timer);
    if (response.ok) {
      return { ok: true, providerStatus: response.status, error: null, transient: false };
    }
    const transient = response.status === 408
      || response.status === 429
      || response.status >= 500;
    return {
      ok: false,
      providerStatus: response.status,
      error: `HTTP ${response.status}`,
      transient,
    };
  } catch (err) {
    clearTimeout(timer);
    const aborted = err?.name === 'AbortError';
    return {
      ok: false,
      providerStatus: null,
      error: aborted ? 'timeout' : (err?.message || 'network_error'),
      transient: true,
    };
  }
}

module.exports = {
  NTFY_BASE_URL,
  DEFAULT_TIMEOUT_MS,
  sendNotification,
};
