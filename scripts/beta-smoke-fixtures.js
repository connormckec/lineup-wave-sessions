'use strict';

function isoDateInTimeZone(timeZone) {
  return new Intl.DateTimeFormat('en-CA', { timeZone }).format(new Date());
}

function sessionTs(isoDate, clockEt) {
  return Math.floor(Date.parse(`${isoDate}T${clockEt}-04:00`) / 1000);
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
  const openTs = sessionTs(isoDate, '14:00:00');
  const fullTs = sessionTs(isoDate, '15:00:00');
  const lessonTs = sessionTs(isoDate, '16:00:00');
  const badTs = sessionTs(isoDate, '17:00:00');

  const openKey = `${openTs}_1`;
  const fullKey = `${fullTs}_2`;
  const lessonKey = `${lessonTs}_3`;

  const openSession = {
    key: openKey,
    session_key: openKey,
    iso_date: isoDate,
    isoDate,
    dateKey: isoDate,
    ts: openTs,
    time: '2:00 pm',
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
    iso_date: isoDate,
    isoDate,
    dateKey: isoDate,
    ts: fullTs,
    time: '3:00 pm',
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
    iso_date: isoDate,
    isoDate,
    dateKey: isoDate,
    ts: lessonTs,
    time: '4:00 pm',
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
    iso_date: isoDate,
    isoDate,
    dateKey: isoDate,
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
    const dateKey = date || isoDate;
    const rows = dateKey === isoDate ? sessions : [];
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
        statusReason: day === isoDate ? 'saved_sessions_found' : 'not_checked',
        sessionsCount: day === isoDate ? sessions.length : 0,
        lastCheckedAt: day === isoDate ? statusPayload.lastSuccessfulScrape : null,
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
      iso_date: isoDate,
      dateKey: isoDate,
      ts: openTs,
      time: '2:00 pm',
      level: 'Progressive',
      wave: 1,
      waveSide: 'Left Wave',
      wave_side: 'Left Wave',
      active: true,
      available_entries: 99,
    };
  }

  return {
    isoDate,
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
