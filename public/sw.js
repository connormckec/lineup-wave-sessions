/* Lineup service worker v8 — static cache + Web Push handlers */
const CACHE_VERSION = '12';
const STATIC_CACHE = `lineup-static-v${CACHE_VERSION}`;

const STATIC_ASSETS = [
  '/manifest.json',
  '/icon-192.png',
  '/icon-512.png',
  '/apple-touch-icon.png',
];

function isApiRequest(url) {
  return url.pathname.startsWith('/api/');
}

function isAppShell(url) {
  return url.pathname === '/' || url.pathname === '/index.html';
}

function isPrototypeAsset(url) {
  const path = url.pathname;
  return path === '/browse-prototype.html'
    || path === '/browse-ui-semantics.js'
    || path === '/lib/browse-ui-semantics.js'
    || path === '/lib/browse-ui-fixtures.js';
}

function isStaticAsset(url) {
  if (url.origin !== self.location.origin) return false;
  if (isApiRequest(url) || isAppShell(url) || isPrototypeAsset(url)) return false;
  return /\.(png|jpg|jpeg|webp|svg|ico|woff2?|css|js|json)$/i.test(url.pathname)
    || url.pathname.startsWith('/brand/');
}

function sanitizeText(value, maxLen) {
  if (value == null) return '';
  return String(value).replace(/[\r\n\u0000]/g, ' ').trim().slice(0, maxLen);
}

function safeSameOriginPath(raw) {
  const text = sanitizeText(raw, 512);
  if (!text) return '/';
  if (!text.startsWith('/') || text.startsWith('//') || text.includes('://')) return null;
  try {
    const resolved = new URL(text, self.location.origin);
    if (resolved.origin !== self.location.origin) return null;
    return `${resolved.pathname}${resolved.search}${resolved.hash}` || '/';
  } catch {
    return null;
  }
}

function parsePushPayload(event) {
  if (!event?.data) return null;
  try {
    return event.data.json();
  } catch {
    return null;
  }
}

function validatePushPayload(payload) {
  if (!payload || typeof payload !== 'object') return null;
  const title = sanitizeText(payload.title, 120);
  const body = sanitizeText(payload.body, 280);
  const tag = sanitizeText(payload.tag, 128) || 'lineup-alert';
  const url = safeSameOriginPath(payload.url);
  if (!title || !body || !url) return null;
  return {
    title,
    body,
    tag,
    url,
    eventType: sanitizeText(payload.eventType, 64) || null,
    sessionKey: sanitizeText(payload.sessionKey, 128) || null,
    isoDate: sanitizeText(payload.isoDate, 32) || null,
  };
}

self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open(STATIC_CACHE)
      .then((cache) => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys.filter((k) => k !== STATIC_CACHE).map((k) => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  const { request } = e;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  if (isApiRequest(url) || isAppShell(url)) return;

  if (isStaticAsset(url)) {
    e.respondWith(
      caches.match(request).then((cached) => {
        const network = fetch(request)
          .then((res) => {
            if (res.ok) {
              const copy = res.clone();
              caches.open(STATIC_CACHE).then((cache) => cache.put(request, copy));
            }
            return res;
          })
          .catch(() => cached);
        return cached || network;
      })
    );
  }
});

self.addEventListener('push', (event) => {
  const payload = validatePushPayload(parsePushPayload(event));
  if (!payload) return;

  event.waitUntil(self.registration.showNotification(payload.title, {
    body: payload.body,
    tag: payload.tag,
    icon: '/icon-192.png',
    badge: '/icon-192.png',
    data: {
      url: payload.url,
      eventType: payload.eventType,
      sessionKey: payload.sessionKey,
      isoDate: payload.isoDate,
    },
  }));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetPath = safeSameOriginPath(event.notification?.data?.url) || '/';
  const targetUrl = new URL(targetPath, self.location.origin).href;

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
      for (const client of clients) {
        if (client.url.startsWith(self.location.origin)) {
          client.focus();
          if ('navigate' in client) return client.navigate(targetUrl);
          return client;
        }
      }
      return self.clients.openWindow(targetUrl);
    })
  );
});
