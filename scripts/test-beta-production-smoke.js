'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { chromium } = require('playwright');
const { buildBetaSmokeFixtures, isoDateInTimeZone } = require('./beta-smoke-fixtures');

console.log('beta production smoke');

const ROOT = path.join(__dirname, '..');
const TEST_PORT = 3099;
const PROFILE_CODE = 'test-profile-alpha-001';
const FIXTURE_DATE = isoDateInTimeZone('America/New_York');
const FIXTURES = buildBetaSmokeFixtures(FIXTURE_DATE);
const ACTIVE_FIXTURE_DATE = FIXTURES.isoDate;

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

function countDots(html, className) {
  if (!html) return 0;
  if (className === 'gray') return (html.match(/class="slot-dot"/g) || []).length;
  return (html.match(new RegExp(`class="slot-dot ${className}"`, 'g')) || []).length;
}

function isIgnorableConsoleError(text) {
  const msg = String(text || '');
  return /\[sw\]|service worker|Failed to register|push|PushManager|notification/i.test(msg);
}

function createMockState() {
  const watchlistItems = [FIXTURES.initialWatchlistItem()];
  return {
    watchlistItems,
    addWatch(payload) {
      const item = {
        id: `watch-${Date.now()}`,
        session_key: payload.session_key || payload.key,
        key: payload.session_key || payload.key,
        iso_date: payload.iso_date || payload.dateKey || ACTIVE_FIXTURE_DATE,
        dateKey: payload.dateKey || payload.iso_date || ACTIVE_FIXTURE_DATE,
        ts: payload.ts || payload.start_ts,
        time: payload.time,
        level: payload.level || payload.session_type,
        wave: payload.wave,
        waveSide: payload.wave_side || payload.waveSide,
        wave_side: payload.wave_side || payload.waveSide,
        active: true,
      };
      watchlistItems.push(item);
      return item;
    },
    removeWatch(id) {
      const idx = watchlistItems.findIndex((w) => w.id === id);
      if (idx >= 0) watchlistItems.splice(idx, 1);
    },
  };
}

async function installMockRoutes(page, mockState) {
  await page.route(/\/api\//, async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    const { pathname } = url;

    if (pathname === '/api/sessions') {
      const date = url.searchParams.get('date');
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(FIXTURES.sessionsResponse(date)),
      });
    }

    if (pathname === '/api/status') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(FIXTURES.statusPayload),
      });
    }

    if (pathname === '/api/session-date-coverage') {
      const startDate = url.searchParams.get('startDate');
      const endDate = url.searchParams.get('endDate');
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(FIXTURES.coverageResponse(startDate, endDate)),
      });
    }

    if (pathname === '/api/watchlist' && req.method() === 'GET') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true, items: mockState.watchlistItems }),
      });
    }

    if (pathname === '/api/watchlist' && req.method() === 'POST') {
      let payload = {};
      try {
        payload = JSON.parse(req.postData() || '{}');
      } catch (_) { /* empty */ }
      const item = mockState.addWatch(payload);
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true, item }),
      });
    }

    if (/^\/api\/watchlist\/.+/.test(pathname) && req.method() === 'DELETE') {
      const id = pathname.split('/').pop();
      mockState.removeWatch(id);
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true }),
      });
    }

    if (pathname === '/api/watchlist/sync') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true, items: mockState.watchlistItems }),
      });
    }

    if (pathname === '/api/notification-profile') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true, hasTopic: false }),
      });
    }

    if (pathname === '/api/push/vapid-public-key') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true, publicKey: 'test-vapid-key' }),
      });
    }

    if (pathname === '/api/push/status') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true, active: false, uiState: 'unsupported' }),
      });
    }

    if (pathname === '/api/session-details/enrich-date') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ ok: true, queued: false }),
      });
    }

    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ ok: true }),
    });
  });
}

async function waitForBrowseReady(page) {
  await page.waitForFunction(() => (
    typeof window.LineupBrowse?.semantics?.computeLiveNowSummary === 'function'
    && document.getElementById('session-list')
    && document.querySelectorAll('#session-list .sc').length >= 2
  ), { timeout: 15000 });
}

async function collectBrowseState(page) {
  return page.evaluate(() => ({
    livePanelText: document.getElementById('live-panel')?.textContent?.trim() || '',
    selectedDateLabel: document.getElementById('selected-date-label')?.textContent?.trim() || '',
    dayRailCount: document.getElementById('day-rail')?.children.length || 0,
    waveFilterCount: document.getElementById('wave-chips')?.children.length || 0,
    levelFilterCount: document.getElementById('level-tabs')?.children.length || 0,
    lessonsLabel: document.getElementById('lessons-toggle')?.textContent?.trim() || '',
    sessionCards: document.querySelectorAll('#session-list .sc').length,
    sessionListText: document.getElementById('session-list')?.textContent?.trim() || '',
    openCards: document.querySelectorAll('#session-list .sc:not(.is-full)').length,
    fullCards: document.querySelectorAll('#session-list .sc.is-full').length,
    firstDotBar: document.querySelector('#session-list .slot-bar')?.innerHTML || '',
    betaFooterText: document.getElementById('beta-footer')?.textContent?.trim() || '',
  }));
}

{
  const html = fs.readFileSync(path.join(ROOT, 'public/index.html'), 'utf8');
  assert.match(html, /id="beta-footer"/, 'beta footer present');
  assert.match(html, /id="report-issue-btn"/, 'report issue action present');
  assert.match(html, /Unofficial beta companion for Atlantic Park/, 'beta disclaimer text present');
  assert.match(html, /function buildReportIssueText\(/, 'report issue builder present');
}

(async () => {
  await withTestServer(async (origin) => {
    const browser = await chromium.launch({ channel: 'chrome' }).catch(() => chromium.launch());
    const mockState = createMockState();

    async function openApp(viewport = { width: 1280, height: 900 }) {
      const pageErrors = [];
      const consoleErrors = [];
      const context = await browser.newContext({ viewport });
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      const page = await context.newPage();
      page.on('pageerror', (err) => pageErrors.push(String(err?.message || err)));
      page.on('console', (msg) => {
        if (msg.type() === 'error') consoleErrors.push(msg.text());
      });

      await installMockRoutes(page, mockState);
      await page.addInitScript(({ isoDate, profileCode }) => {
        localStorage.setItem('ap_active_day_key', isoDate);
        localStorage.setItem('ap_profile_code', profileCode);
        localStorage.setItem('ap_show_lessons', '0');
        localStorage.removeItem('ap_watchlist');
      }, { isoDate: ACTIVE_FIXTURE_DATE, profileCode: PROFILE_CODE });

      await page.goto(`${origin}/`, { waitUntil: 'domcontentloaded' });
      await waitForBrowseReady(page);

      return {
        page,
        context,
        pageErrors,
        consoleErrors,
        assertNoFatalErrors() {
          const allErrors = [...pageErrors, ...consoleErrors.filter((msg) => !isIgnorableConsoleError(msg))];
          const syntaxErrors = allErrors.filter((msg) => /SyntaxError|ReferenceError/i.test(msg));
          assert.strictEqual(syntaxErrors.length, 0, `browser syntax/reference errors: ${syntaxErrors.join(' | ')}`);
        },
      };
    }

    // Desktop Browse smoke with fixtures
    {
      const app = await openApp();
      app.assertNoFatalErrors();

      const state = await collectBrowseState(app.page);
      assert.match(state.livePanelText, /Live now|Live session data unavailable/i, 'live now panel renders');
      assert.ok(state.selectedDateLabel.length > 0, 'selected date renders');
      assert.ok(state.dayRailCount >= 1, 'date rail renders');
      assert.ok(state.waveFilterCount >= 1, 'wave filters render');
      assert.ok(state.levelFilterCount >= 1, 'type filters render');
      assert.match(state.lessonsLabel, /Show lessons|Hide lessons/, 'lessons toggle renders');
      assert.ok(state.sessionCards >= 2, 'session cards render');
      assert.ok(state.openCards >= 1, 'open sessions remain visible');
      assert.ok(state.fullCards >= 1, 'full sessions remain visible');
      assert.strictEqual(countDots(state.firstDotBar, 'gray'), 13, 'true-capacity gray dots for 5/18');
      assert.strictEqual(countDots(state.firstDotBar, 'open') + countDots(state.firstDotBar, 'scarce'), 5, 'true-capacity teal dots for 5/18');
      assert.match(state.betaFooterText, /Unofficial beta companion for Atlantic Park/i, 'beta footer visible');
      assert.ok(state.sessionListText.length > 0, 'browse content is not blank');

      const lessonVisibleBefore = await app.page.locator('#session-list').filter({ hasText: 'Lesson' }).count();
      assert.ok(lessonVisibleBefore === 0, 'lessons hidden by default');

      await app.page.click('#lessons-toggle');
      await app.page.waitForFunction(() => document.querySelectorAll('#session-list .sc').length >= 3);
      const lessonVisibleAfterShow = await app.page.locator('#session-list').filter({ hasText: 'Lesson' }).count();
      assert.ok(lessonVisibleAfterShow > 0, 'show lessons reveals lesson rows');

      await app.page.click('#lessons-toggle');
      await app.page.waitForFunction(() => (
        document.querySelectorAll('#session-list .sc').length <= 2
      ));
      const lessonVisibleAfterHide = await app.page.locator('#session-list').filter({ hasText: 'Lesson' }).count();
      assert.ok(lessonVisibleAfterHide === 0, 'hide lessons hides lesson rows');

      await app.context.close();
    }

    // Watch / unwatch
    {
      mockState.watchlistItems.length = 0;
      const app = await openApp();
      app.assertNoFatalErrors();

      const watchBtn = app.page.locator('#session-list .watch-btn').first();
      await watchBtn.click();
      await app.page.waitForFunction(() => (
        document.querySelector('#session-list .watch-btn.watching')
      ), { timeout: 5000 });

      await app.page.locator('#session-list .watch-btn.watching').click();
      await app.page.waitForFunction(() => (
        !document.querySelector('#session-list .watch-btn.watching')
      ), { timeout: 5000 });

      await app.context.close();
    }

    // Lineup joins current session record
    {
      mockState.watchlistItems = [FIXTURES.initialWatchlistItem()];
      const app = await openApp();
      app.assertNoFatalErrors();

      await app.page.click('#nav-watching');
      await app.page.waitForFunction(() => document.querySelectorAll('#watch-list .wc').length === 1);
      const lineupDots = await app.page.evaluate(() => {
        const bar = document.querySelector('#watch-list .slot-bar');
        return bar ? bar.innerHTML : '';
      });
      assert.strictEqual(countDots(lineupDots, 'gray'), 13, 'lineup uses current session occupancy, not stale watchlist count');
      assert.strictEqual(countDots(lineupDots, 'open') + countDots(lineupDots, 'scarce'), 5, 'lineup teal dots match live session');

      await app.context.close();
    }

    // Settings + report issue
    {
      const app = await openApp();
      app.assertNoFatalErrors();

      await app.page.click('#nav-setup');
      await app.page.waitForSelector('#profile-sync-block');
      const settingsVisible = await app.page.evaluate(() => (
        !!document.getElementById('profile-sync-block')
        && !!document.getElementById('push-notifications-block')
      ));
      assert.ok(settingsVisible, 'settings renders');

      await app.page.click('#report-issue-btn');
      await app.page.waitForFunction(() => {
        const sheet = document.getElementById('report-sheet');
        return sheet && !sheet.hidden && sheet.classList.contains('open');
      });
      await app.page.fill('#report-issue-description', 'Smoke test feedback');
      await app.page.click('#report-issue-submit');

      const copied = await app.page.evaluate(async () => {
        try {
          return await navigator.clipboard.readText();
        } catch {
          return document.getElementById('report-issue-preview')?.textContent || '';
        }
      });
      assert.match(copied, /Smoke test feedback/, 'report issue captures description');
      assert.match(copied, /App version:/, 'report issue captures app version');
      assert.match(copied, /Report generated:/, 'report issue captures generation timestamp');
      assert.match(copied, /App URL:/, 'report issue captures current app URL');
      assert.match(copied, /Selected date:/, 'report issue captures selected date');
      assert.doesNotMatch(copied, /push\/subscribe|Authorization:|Bearer /i, 'report issue excludes secrets');

      const confirmText = await app.page.evaluate(() => (
        document.getElementById('report-issue-msg')?.textContent || ''
      ));
      assert.match(confirmText, /Report copied\. Paste it into an email or message to Connor\./, 'report copy confirmation shown');

      await app.context.close();
    }

    // Mobile layout
    {
      const app = await openApp({ width: 390, height: 844 });
      app.assertNoFatalErrors();
      const mobile = await collectBrowseState(app.page);
      assert.ok(mobile.sessionCards >= 2, 'mobile browse renders session cards');
      assert.ok(mobile.dayRailCount >= 1, 'mobile date rail renders');
      assert.match(mobile.livePanelText, /Live now|Live session data unavailable/i, 'mobile live panel renders');
      await app.context.close();
    }

    // Malformed session does not blank page
    {
      const app = await openApp();
      app.assertNoFatalErrors();
      const cards = await app.page.evaluate(() => document.querySelectorAll('#session-list .sc').length);
      assert.ok(cards >= 2, 'malformed session skipped without blanking browse list');
      const listText = await app.page.evaluate(() => document.getElementById('session-list')?.textContent?.trim() || '');
      assert.ok(listText.length > 0, 'browse list text remains after malformed row');
      app.assertNoFatalErrors();
      await app.context.close();
    }

    await browser.close();
  });

  console.log('beta production smoke: ok');
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
