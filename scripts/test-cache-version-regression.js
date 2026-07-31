'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');

console.log('cache version regression');

const RELEASE_VERSION = '16';
const ROOT = path.join(__dirname, '..');
const html = fs.readFileSync(path.join(ROOT, 'public/index.html'), 'utf8');
const sw = fs.readFileSync(path.join(ROOT, 'public/sw.js'), 'utf8');

function extractConst(source, name) {
  const match = source.match(new RegExp(`const ${name} = '([^']+)'`));
  assert.ok(match, `${name} not found`);
  return match[1];
}

{
  const swCache = extractConst(sw, 'CACHE_VERSION');
  const htmlCache = extractConst(html, 'SW_CACHE_VERSION');
  assert.strictEqual(swCache, htmlCache, 'CACHE_VERSION must equal SW_CACHE_VERSION');
  assert.strictEqual(swCache, RELEASE_VERSION, 'release version alignment');
}

{
  const helpers = [
    '/push-client.js?v=10',
    '/profile-auth-client.js?v=10',
    '/lineup-config.js?v=16',
    '/session-capacity-config.js?v=16',
    '/browse-live-schedule.js?v=16',
    '/browse-session-filters.js?v=16',
    '/browse-availability-view.js?v=16',
    '/browse-ui-semantics.js?v=16',
  ];
  for (const src of helpers) {
    assert.ok(html.includes(`src="${src}"`), `missing script ${src}`);
  }
  assert.ok(!html.includes('/push-client.js?v=1"'), 'stale push-client version');
  assert.ok(!html.includes('/profile-auth-client.js?v=4"'), 'stale profile-auth version');
  assert.ok(!html.match(/browse-session-filters\.js"/), 'browse-session-filters must use query version');
}

{
  const registrations = html.match(/serviceWorker\.register/g) || [];
  assert.strictEqual(registrations.length, 1, 'exactly one service worker registration');
  assert.ok(html.includes(`register(\`/sw.js?v=\${SW_CACHE_VERSION}\``));
  assert.ok(html.includes("updateViaCache: 'none'"));
}

{
  assert.ok(sw.includes("addEventListener('push'"));
  assert.ok(sw.includes("addEventListener('notificationclick'"));
  assert.match(sw, /function isApiRequest\([\s\S]*pathname\.startsWith\('\/api\/'\)/);
  assert.match(sw, /function isAppShell[\s\S]*return url\.pathname === '\/' \|\| url\.pathname === '\/index\.html'/);
  assert.match(sw, /if \(isApiRequest\(url\) \|\| isAppShell\(url\)\) return;/);
}

{
  assert.ok(sw.includes('caches.keys()'));
  assert.ok(sw.includes('k !== STATIC_CACHE'));
  assert.ok(sw.includes('caches.delete(k)'));
  assert.strictEqual(
    sw.match(/const STATIC_CACHE = `lineup-static-v\$\{CACHE_VERSION\}`/) != null,
    true,
    'cache bucket uses lineup-static-v prefix',
  );
}

{
  assert.ok(html.includes('async function resetLocalAppCache()'));
  assert.ok(html.includes('caches.keys()'));
  assert.ok(html.includes('caches.delete(k)'));
  assert.ok(html.includes('getRegistrations()'));
  assert.ok(html.includes('r.unregister()'));
}

{
  assert.ok(html.includes("APP_SHELL_BUILD = '2026-07-31-availability-view-v14'"));
  assert.ok(html.includes('id="live-panel"'));
  assert.ok(html.includes('id="day-rail"'));
  assert.ok(html.includes('class="watch-btn'));
  assert.ok(html.includes('renderLivePanel'));
  assert.doesNotMatch(html, /trusted open/i);
}

{
  const required = [
    'public/push-client.js',
    'public/profile-auth-client.js',
    'public/sw.js',
    'public/manifest.json',
    'public/icon-192.png',
    'public/icon-512.png',
    'public/apple-touch-icon.png',
  ];
  for (const rel of required) {
    assert.ok(fs.existsSync(path.join(ROOT, rel)), `missing asset ${rel}`);
  }
}

{
  const serverJs = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');
  assert.ok(serverJs.includes("app.get('/lineup-config.js'"));
  assert.ok(serverJs.includes("app.get('/browse-availability-view.js'"));
  assert.ok(serverJs.includes("app.get('/browse-session-filters.js'"));
  assert.ok(serverJs.includes("app.get('/browse-live-schedule.js'"));
}

console.log('cache version regression: ok');
