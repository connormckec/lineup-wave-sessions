'use strict';

function isoDateInTimeZone(timeZone) {
  return new Intl.DateTimeFormat('en-CA', { timeZone }).format(new Date());
}

function addDaysToIso(isoDate, days) {
  const [y, m, d] = String(isoDate).slice(0, 10).split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d + days, 12, 0, 0));
  return dt.toISOString().slice(0, 10);
}

function minutesFromMidnightEt(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour: 'numeric',
    minute: 'numeric',
    hour12: false,
  }).formatToParts(date);
  const hour = Number(parts.find((p) => p.type === 'hour')?.value ?? 0);
  const minute = Number(parts.find((p) => p.type === 'minute')?.value ?? 0);
  return hour * 60 + minute;
}

function clockEtFromMinutes(totalMinutes) {
  const h24 = Math.floor(totalMinutes / 60) % 24;
  const minute = totalMinutes % 60;
  const h12 = h24 % 12 || 12;
  const ampm = h24 < 12 ? 'am' : 'pm';
  return `${h12}:${String(minute).padStart(2, '0')} ${ampm}`;
}

function sessionTs(isoDate, clockEt) {
  return Math.floor(Date.parse(`${isoDate}T${clockEt}-04:00`) / 1000);
}

function sessionTsFromMinutes(isoDate, totalMinutes) {
  const h24 = Math.floor(totalMinutes / 60) % 24;
  const minute = totalMinutes % 60;
  return sessionTs(
    isoDate,
    `${String(h24).padStart(2, '0')}:${String(minute).padStart(2, '0')}:00`,
  );
}

function resolveFixtureSchedule(isoDate = isoDateInTimeZone('America/New_York')) {
  let fixtureDate = isoDate;
  let baseMinutes = minutesFromMidnightEt();
  const latestStart = 22 * 60;
  const spacing = 60;
  const neededSpan = spacing * 4;
  if (baseMinutes + neededSpan > latestStart) {
    fixtureDate = addDaysToIso(isoDate, 1);
    baseMinutes = 10 * 60;
  }
  const openMinutes = baseMinutes + spacing;
  const fullMinutes = baseMinutes + spacing * 2;
  const lessonMinutes = baseMinutes + spacing * 3;
  const badMinutes = baseMinutes + spacing * 4;
  return {
    fixtureDate,
    openMinutes,
    fullMinutes,
    lessonMinutes,
    badMinutes,
  };
}

function trustedFields(overrides = {}) {
  return {
    threshold_scan_verified: true,
    thresholdScanVerified: true,
    slot_status: 'exact',
    slot_source: 'entries_left_threshold_scan',
    thresholdConfidence: 'exact',
    threshold_scanned_at: new Date().toISOString(),
    ...overrides,
  };
}

function buildBetaSmokeFixtures(isoDate = isoDateInTimeZone('America/New_York')) {
  const schedule = resolveFixtureSchedule(isoDate);
  const fixtureDate = schedule.fixtureDate;
  const openTs = sessionTsFromMinutes(fixtureDate, schedule.openMinutes);
  const fullTs = sessionTsFromMinutes(fixtureDate, schedule.fullMinutes);
  const lessonTs = sessionTsFromMinutes(fixtureDate, schedule.lessonMinutes);
  const badTs = sessionTsFromMinutes(fixtureDate, schedule.badMinutes);

  const openKey = `${openTs}_1`;
  const fullKey = `${fullTs}_2`;
  const lessonKey = `${lessonTs}_3`;

  const openSession = {
    key: openKey,
    session_key: openKey,
    iso_date: fixtureDate,
    isoDate: fixtureDate,
    dateKey: fixtureDate,
    ts: openTs,
    time: clockEtFromMinutes(schedule.openMinutes),
    level: 'Progressive',
    wave: 1,
    waveSide: 'Left Wave',
    available: true,
    available_entries: 5,
    capacity: 18,
    ...trustedFields(),
  };

  const fullSession = {
    key: fullKey,
    session_key: fullKey,
    iso_date: fixtureDate,
    isoDate: fixtureDate,
    dateKey: fixtureDate,
    ts: fullTs,
    time: clockEtFromMinutes(schedule.fullMinutes),
    level: 'Advanced Turns',
    wave: 2,
    waveSide: 'Right Wave',
    available: false,
    available_entries: 0,
    thresholdInferredSlots: 0,
    capacity: 12,
    ...trustedFields({ threshold_scanned_at: new Date().toISOString() }),
  };

  const lessonSession = {
    key: lessonKey,
    session_key: lessonKey,
    iso_date: fixtureDate,
    isoDate: fixtureDate,
    dateKey: fixtureDate,
    ts: lessonTs,
    time: clockEtFromMinutes(schedule.lessonMinutes),
    level: 'Progressive Lesson',
    wave: 3,
    waveSide: 'Left Lesson',
    available: true,
    available_entries: 6,
    capacity: 18,
    ...trustedFields(),
  };

  const malformedSession = {
    key: 'malformed-card-row',
    session_key: 'malformed-card-row',
    iso_date: fixtureDate,
    isoDate: fixtureDate,
    dateKey: fixtureDate,
    ts: badTs,
    time: { invalid: 'object breaks replace()' },
    level: 'Progressive',
    wave: 1,
    waveSide: 'Left Wave',
    available: true,
    available_entries: 2,
    capacity: 18,
    ...trustedFields(),
  };

  const sessions = [openSession, fullSession, lessonSession, malformedSession];

  const statusPayload = {
    ok: true,
    appVersion: 'beta-smoke-fixture',
    buildTime: new Date().toISOString(),
    webPushEnabled: true,
    notificationDeliveryProvider: 'webpush',
    internalBetaNotifications: false,
    showNtfyDevUi: false,
    dataSource: 'beta-smoke-fixture',
    source: 'beta-smoke-fixture',
    lastSuccessfulScrape: new Date(Date.now() - 12 * 60 * 1000).toISOString(),
    dataAgeMinutes: 12,
    minutesSinceLastScrape: 12,
    scrapeInProgress: false,
    sessions,
    sessionsCount: sessions.length,
    currentSessionsCount: sessions.length,
    watchList: [],
  };

  function sessionsResponse(date) {
    const dateKey = date || fixtureDate;
    const rows = dateKey === fixtureDate ? sessions : [];
    return {
      ok: true,
      date: dateKey,
      statusReason: rows.length ? 'saved_sessions_found' : 'checked_no_sessions',
      sessionsCount: rows.length,
      dataSource: 'beta-smoke-fixture',
      wasDateChecked: true,
      hasSavedSessions: rows.length > 0,
      isScrapeInProgress: false,
      isFallback: false,
      sessions: rows,
      lastSuccessfulScrape: statusPayload.lastSuccessfulScrape,
      dataAgeMinutes: statusPayload.dataAgeMinutes,
      webPushEnabled: true,
      notificationDeliveryProvider: 'webpush',
    };
  }

  function coverageResponse(startDate, endDate) {
    const byDate = {};
    for (const day of enumerateDates(startDate, endDate)) {
      byDate[day] = {
        statusReason: day === fixtureDate ? 'saved_sessions_found' : 'not_checked',
        sessionsCount: day === fixtureDate ? sessions.length : 0,
        lastCheckedAt: day === fixtureDate ? statusPayload.lastSuccessfulScrape : null,
      };
    }
    return {
      ok: true,
      byDate,
      complete: true,
      rowCountScanned: Object.keys(byDate).length,
      requestedRange: `${startDate}:${endDate}`,
    };
  }

  function initialWatchlistItem() {
    return {
      id: 'watch-smoke-1',
      session_key: openKey,
      key: openKey,
      iso_date: fixtureDate,
      dateKey: fixtureDate,
      ts: openTs,
      time: clockEtFromMinutes(schedule.openMinutes),
      level: 'Progressive',
      wave: 1,
      waveSide: 'Left Wave',
      wave_side: 'Left Wave',
      active: true,
      available_entries: 99,
    };
  }

  return {
    isoDate: fixtureDate,
    openKey,
    fullKey,
    lessonKey,
    openSession,
    fullSession,
    lessonSession,
    malformedSession,
    sessions,
    statusPayload,
    sessionsResponse,
    coverageResponse,
    initialWatchlistItem,
  };
}

function enumerateDates(startDate, endDate) {
  const out = [];
  if (!startDate || !endDate || startDate > endDate) return out;
  const cur = new Date(`${startDate}T12:00:00Z`);
  const end = new Date(`${endDate}T12:00:00Z`);
  while (cur <= end) {
    out.push(cur.toISOString().slice(0, 10));
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

module.exports = {
  isoDateInTimeZone,
  buildBetaSmokeFixtures,
};
