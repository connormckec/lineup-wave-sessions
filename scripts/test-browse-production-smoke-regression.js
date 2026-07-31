'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { spawn } = require('child_process');
const { chromium } = require('playwright');

console.log('browse production smoke regression');

const ROOT = path.join(__dirname, '..');
const TEST_PORT = 3098;
const html = fs.readFileSync(path.join(ROOT, 'public/index.html'), 'utf8');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForServer(port, timeoutMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`);
      if (res.ok) return;
    } catch (_) { /* retry */ }
    await sleep(250);
  }
  throw new Error(`Test server did not start on port ${port}`);
}

async function withTestServer(fn) {
  const child = spawn('node', ['server.js'], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(TEST_PORT) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  try {
    await waitForServer(TEST_PORT);
    await fn(`http://127.0.0.1:${TEST_PORT}`);
  } finally {
    child.kill('SIGTERM');
    await sleep(200);
  }
}

function countScriptIncludes(source, src) {
  const re = new RegExp(`<script[^>]+src="${src.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"`, 'g');
  return (source.match(re) || []).length;
}

{
  const scripts = [
    '/lineup-config.js?v=15',
    '/session-capacity-config.js?v=15',
    '/browse-session-filters.js?v=15',
    '/browse-live-schedule.js?v=15',
    '/browse-availability-view.js?v=15',
    '/browse-ui-semantics.js?v=15',
  ];
  for (const src of scripts) {
    assert.strictEqual(countScriptIncludes(html, src), 1, `expected one ${src} script tag`);
  }
  assert.match(html, /const browseState = \{/);
  assert.match(html, /function getBrowseSessionsForDay\(/);
  assert.match(html, /const browseSessionPool = Array\.isArray\(browseState\.selectedDateSessions\)/);
  assert.doesNotMatch(html, /function browseSessionPool\(/);
  assert.doesNotMatch(html, /browseSessionPool\(\)/);
  assert.match(html, /No sessions match these filters\./);
  assert.match(html, /window\.LineupBrowse/, 'index uses LineupBrowse namespace');
  assert.match(html, /bookingTimeZone\(\)/, 'index reads timezone from LineupConfig');
}

{
  const sandbox = {
    window: {},
    console,
    Intl,
    Date,
    Object,
    Set,
    Number,
    String,
    Math,
    Array,
    RegExp,
    module: undefined,
    exports: undefined,
    require: undefined,
  };
  sandbox.window = sandbox;
  vm.createContext(sandbox);

  const loadOrder = [
    'lib/lineup-config.js',
    'lib/session-capacity-config.js',
    'lib/browse-session-filters.js',
    'lib/browse-live-schedule.js',
    'lib/browse-availability-view.js',
    'lib/browse-ui-semantics.js',
  ];

  for (const rel of loadOrder) {
    const code = fs.readFileSync(path.join(ROOT, rel), 'utf8');
    assert.doesNotMatch(code, /^const api = /m, `${rel} must not declare global const api`);
    vm.runInContext(code, sandbox, { filename: rel });
  }

  assert.ok(sandbox.LineupConfig?.BOOKING_TZ, 'LineupConfig exported');
  assert.strictEqual(typeof sandbox.LineupBrowse?.liveSchedule?.isSessionLiveAt, 'function');
  assert.strictEqual(typeof sandbox.LineupBrowse?.sessionFilters?.isLessonSession, 'function');
  assert.strictEqual(typeof sandbox.LineupBrowse?.semantics?.computeLiveNowSummary, 'function');
}

(async () => {
  await withTestServer(async (origin) => {
    const browser = await chromium.launch({ channel: 'chrome' }).catch(() => chromium.launch());
    const pageErrors = [];
    const consoleErrors = [];

    const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
    const page = await context.newPage();
    page.on('pageerror', (err) => pageErrors.push(String(err?.message || err)));
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text());
    });

    await page.goto(`${origin}/`, { waitUntil: 'domcontentloaded' });
    await page.waitForFunction(() => (
      typeof window.LineupBrowse?.semantics?.computeLiveNowSummary === 'function'
      && typeof window.LineupBrowse?.sessionFilters?.isLessonSession === 'function'
      && document.getElementById('live-panel')
      && document.getElementById('sidebar')
      && document.getElementById('session-list')
    ), { timeout: 15000 });

    await page.waitForFunction(() => {
      const list = document.getElementById('session-list');
      if (!list) return false;
      const text = list.textContent || '';
      return list.querySelector('.sc')
        || text.includes('No sessions match these filters.')
        || text.includes('Loading sessions')
        || text.includes('No sessions found')
        || text.includes('Not checked yet');
    }, { timeout: 15000 });

    const state = await page.evaluate(() => ({
      bootReady: document.body?.dataset?.bootState || null,
      livePanelText: document.getElementById('live-panel')?.textContent?.trim() || '',
      dayRailCount: document.getElementById('day-rail')?.children.length || 0,
      levelTabsCount: document.getElementById('level-tabs')?.children.length || 0,
      sidebarText: document.getElementById('sidebar')?.textContent?.trim() || '',
      browseVisible: !document.getElementById('pane-browse')?.hidden,
      lessonsLabel: document.getElementById('lessons-toggle')?.textContent?.trim() || '',
      sessionCards: document.querySelectorAll('#session-list .sc').length,
      sessionListText: document.getElementById('session-list')?.textContent?.trim() || '',
      lineupBrowseReady: !!window.LineupBrowse?.semantics && !!window.LineupBrowse?.sessionFilters,
    }));

    const allErrors = [...pageErrors, ...consoleErrors];
    const syntaxErrors = allErrors.filter((msg) => /SyntaxError|already been declared/i.test(msg));
    const browsePoolErrors = allErrors.filter((msg) => /browseSessionPool is not defined|ReferenceError.*browseSessionPool/i.test(msg));

    assert.strictEqual(syntaxErrors.length, 0, `syntax errors: ${syntaxErrors.join(' | ')}`);
    assert.strictEqual(browsePoolErrors.length, 0, `browseSessionPool errors: ${browsePoolErrors.join(' | ')}`);
    assert.ok(state.lineupBrowseReady, 'LineupBrowse helpers initialized');
    assert.ok(state.livePanelText.length > 0, 'live panel rendered');
    assert.ok(state.sidebarText.length > 0, 'sidebar rendered');
    assert.match(state.livePanelText, /Live now|Live session data unavailable/i);
    assert.ok(state.dayRailCount >= 1 || state.levelTabsCount >= 1, 'browse filters/day rail rendered');
    assert.ok(state.browseVisible, 'browse pane visible');
    assert.match(state.lessonsLabel, /Show lessons|Hide lessons/, 'lessons toggle rendered');
    assert.ok(
      state.sessionCards > 0
      || /No sessions match these filters\.|Loading sessions|No sessions found|Not checked yet/.test(state.sessionListText),
      'session list rendered a card row or explicit empty/loading state',
    );

    await context.close();
    await browser.close();
  });

  console.log('browse production smoke regression: ok');
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
