/* Lineup service worker v3 — never cache API or index.html */
const CACHE_VERSION = '3';
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

function isStaticAsset(url) {
  if (url.origin !== self.location.origin) return false;
  if (isApiRequest(url) || isAppShell(url)) return false;
  return /\.(png|jpg|jpeg|webp|svg|ico|woff2?|css|js|json)$/i.test(url.pathname)
    || url.pathname.startsWith('/brand/');
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

  // API + app shell: never intercept
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
