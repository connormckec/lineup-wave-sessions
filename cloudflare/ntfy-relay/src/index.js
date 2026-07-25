/**
 * Lineup ntfy relay — Cloudflare Worker
 * Fixed upstream: https://ntfy.sh (JSON API)
 */

const NTFY_UPSTREAM = 'https://ntfy.sh';
const MAX_TOPIC_LENGTH = 64;
const MAX_TITLE_LENGTH = 250;
const MAX_MESSAGE_LENGTH = 4096;
const MAX_CLICK_LENGTH = 2048;
const MAX_TAGS = 5;
const MAX_TAG_LENGTH = 32;
const UPSTREAM_TIMEOUT_MS = 10000;
const TOPIC_PATTERN = /^[a-zA-Z0-9_-]+$/;
const RATE_LIMIT_WINDOW_MS = 60_000;
const RATE_LIMIT_MAX = 120;

/** @type {Map<string, { count: number, resetAt: number }>} */
const rateByIp = new Map();

function maskTopic(topic) {
  const text = String(topic || '').trim();
  if (!text) return '—';
  if (text.length <= 4) return '••••';
  return `${text.slice(0, 2)}••••${text.slice(-2)}`;
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function extractBearer(request) {
  const header = request.headers.get('Authorization') || '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

async function digestSecret(value) {
  const bytes = new TextEncoder().encode(String(value || ''));
  return crypto.subtle.digest('SHA-256', bytes);
}

function bytesEqual(a, b) {
  const ua = new Uint8Array(a);
  const ub = new Uint8Array(b);
  if (ua.length !== ub.length) return false;
  if (typeof crypto.timingSafeEqual === 'function') {
    return crypto.timingSafeEqual(ua, ub);
  }
  if (crypto.subtle && typeof crypto.subtle.timingSafeEqual === 'function') {
    return crypto.subtle.timingSafeEqual(ua, ub);
  }
  let diff = 0;
  for (let i = 0; i < ua.length; i += 1) diff |= ua[i] ^ ub[i];
  return diff === 0;
}

async function verifySecret(provided, expected) {
  if (!expected) return false;
  const [providedDigest, expectedDigest] = await Promise.all([
    digestSecret(provided),
    digestSecret(expected),
  ]);
  return bytesEqual(providedDigest, expectedDigest);
}

function validateTopic(raw) {
  const topic = String(raw || '').trim();
  if (!topic) return { ok: false, error: 'topic_required' };
  if (topic.length > MAX_TOPIC_LENGTH) return { ok: false, error: 'topic_too_long' };
  if (/[\s/\\:@]/.test(topic) || !TOPIC_PATTERN.test(topic)) {
    return { ok: false, error: 'topic_invalid' };
  }
  return { ok: true, topic };
}

function sanitizeText(value, maxLen) {
  if (value == null) return null;
  const text = String(value).replace(/[\r\n\u0000]/g, '').trim();
  if (!text) return null;
  return text.slice(0, maxLen);
}

function validatePriority(value) {
  if (value == null) return 3;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1 || n > 5) return null;
  return Math.trunc(n);
}

function validateTags(raw) {
  if (raw == null) return [];
  if (!Array.isArray(raw)) return null;
  if (raw.length > MAX_TAGS) return null;
  const tags = [];
  for (const item of raw) {
    const tag = sanitizeText(item, MAX_TAG_LENGTH);
    if (!tag || !/^[a-zA-Z0-9_-]+$/.test(tag)) return null;
    tags.push(tag);
  }
  return tags;
}

function validateClickUrl(raw) {
  const click = sanitizeText(raw, MAX_CLICK_LENGTH);
  if (!click) return null;
  try {
    const parsed = new URL(click);
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return null;
    return parsed.toString();
  } catch {
    return null;
  }
}

function validatePublishBody(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return { ok: false, error: 'invalid_json_body' };
  }
  const allowed = new Set(['topic', 'title', 'message', 'priority', 'tags', 'clickUrl']);
  for (const key of Object.keys(body)) {
    if (!allowed.has(key)) return { ok: false, error: 'unexpected_field' };
  }

  const topic = validateTopic(body.topic);
  if (!topic.ok) return topic;

  const message = sanitizeText(body.message, MAX_MESSAGE_LENGTH);
  if (!message) return { ok: false, error: 'message_required' };

  const title = sanitizeText(body.title, MAX_TITLE_LENGTH) || 'AP Session Alert';
  const priority = validatePriority(body.priority);
  if (priority == null) return { ok: false, error: 'priority_invalid' };

  const tags = validateTags(body.tags);
  if (tags == null) return { ok: false, error: 'tags_invalid' };

  const click = body.clickUrl == null ? null : validateClickUrl(body.clickUrl);
  if (body.clickUrl != null && !click) return { ok: false, error: 'click_url_invalid' };

  return {
    ok: true,
    payload: {
      topic: topic.topic,
      title,
      message,
      priority,
      tags,
      click,
    },
  };
}

function checkRateLimit(request) {
  const ip = request.headers.get('CF-Connecting-IP')
    || request.headers.get('x-forwarded-for')
    || 'unknown';
  const now = Date.now();
  const entry = rateByIp.get(ip);
  if (!entry || now >= entry.resetAt) {
    rateByIp.set(ip, { count: 1, resetAt: now + RATE_LIMIT_WINDOW_MS });
    return true;
  }
  entry.count += 1;
  return entry.count <= RATE_LIMIT_MAX;
}

async function publishToNtfy(payload) {
  const upstreamBody = {
    topic: payload.topic,
    title: payload.title,
    message: payload.message,
    priority: payload.priority,
    tags: payload.tags,
  };
  if (payload.click) upstreamBody.click = payload.click;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    const response = await fetch(NTFY_UPSTREAM, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(upstreamBody),
      signal: controller.signal,
    });
    clearTimeout(timer);
    const text = await response.text();
    return {
      ok: response.ok,
      status: response.status,
      body: text.slice(0, 500),
    };
  } catch (err) {
    clearTimeout(timer);
    const aborted = err?.name === 'AbortError';
    return {
      ok: false,
      status: aborted ? 504 : 502,
      body: aborted ? 'upstream_timeout' : 'upstream_transport_failed',
      transportError: true,
    };
  }
}

async function handlePublish(request, env) {
  if (!checkRateLimit(request)) {
    return jsonResponse({ ok: false, error: 'rate_limited' }, 429);
  }

  const secret = env.NTFY_RELAY_SECRET;
  if (!secret) {
    return jsonResponse({ ok: false, error: 'relay_not_configured' }, 503);
  }

  const token = extractBearer(request);
  if (!token || !(await verifySecret(token, secret))) {
    return jsonResponse({ ok: false, error: 'unauthorized' }, 401);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return jsonResponse({ ok: false, error: 'invalid_json' }, 400);
  }

  const validated = validatePublishBody(body);
  if (!validated.ok) {
    return jsonResponse({ ok: false, error: validated.error }, 400);
  }

  console.log('[ntfy-relay] publish', {
    topicMasked: maskTopic(validated.payload.topic),
    titleLength: validated.payload.title.length,
    messageLength: validated.payload.message.length,
    tagCount: validated.payload.tags.length,
    hasClick: !!validated.payload.click,
  });

  const upstream = await publishToNtfy(validated.payload);
  if (upstream.ok) {
    return jsonResponse({ ok: true, upstreamStatus: upstream.status }, 200);
  }
  if (upstream.transportError) {
    return jsonResponse({
      ok: false,
      error: upstream.body,
      upstreamStatus: upstream.status,
    }, upstream.status === 504 ? 504 : 502);
  }
  return jsonResponse({
    ok: false,
    error: 'upstream_http_error',
    upstreamStatus: upstream.status,
    upstreamBody: upstream.body.slice(0, 120) || null,
  }, upstream.status >= 400 && upstream.status < 600 ? upstream.status : 502);
}

function handleHealth() {
  return jsonResponse({ ok: true, service: 'lineup-ntfy-relay' }, 200);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health' && request.method === 'GET') {
      return handleHealth();
    }
    if (url.pathname === '/publish' && request.method === 'POST') {
      const contentType = request.headers.get('Content-Type') || '';
      if (!contentType.toLowerCase().includes('application/json')) {
        return jsonResponse({ ok: false, error: 'content_type_must_be_json' }, 400);
      }
      return handlePublish(request, env);
    }
    return jsonResponse({ ok: false, error: 'not_found' }, 404);
  },
};

// Test helpers (Node regression tests)
export const __test = {
  maskTopic,
  validatePublishBody,
  validateTopic,
  verifySecret,
  handleHealth,
};
