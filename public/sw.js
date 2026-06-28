/* Lineup service worker — static shell only; never cache API responses */
const CACHE_VERSION = '2';
const STATIC_CACHE = `lineup-static-v${CACHE_VERSION}`;

const SHELL = [
  '/',
  '/index.html',
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

function isStaticAsset(url) {
  if (url.origin !== self.location.origin) return false;
  if (isApiRequest(url) || isAppShell(url)) return false;
  return /\.(png|jpg|jpeg|webp|svg|ico|woff2?|css|js|json)$/i.test(url.pathname)
    || url.pathname.startsWith('/brand/');
}

self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open(STATIC_CACHE)
      .then((cache) => cache.addAll(SHELL))
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

  // API: never intercept — browser goes straight to network with no-store headers
  if (isApiRequest(url)) return;

  // App shell: network-first so deploys land quickly
  if (isAppShell(url)) {
    e.respondWith(
      fetch(request, { cache: 'no-store' })
        .then((res) => {
          if (res.ok) {
            const copy = res.clone();
            caches.open(STATIC_CACHE).then((cache) => cache.put(request, copy));
          }
          return res;
        })
        .catch(() => caches.match(request))
    );
    return;
  }

  // Versioned static assets: cache-first, refresh in background
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
