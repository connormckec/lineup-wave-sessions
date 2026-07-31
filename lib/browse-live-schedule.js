'use strict';

const BOOKING_TZ = 'America/New_York';
const DEFAULT_DURATION_MINUTES = 60;

function parseClockParts(timeStr) {
  if (timeStr == null) return null;
  const raw = String(timeStr).replace(/\s+/g, ' ').trim().toLowerCase();
  if (!raw) return null;
  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i)
    || raw.match(/(\d{1,2})\s*:\s*(\d{2})\s*(am|pm)/i);
  if (!match) return null;
  let hour = Number(match[1]);
  const minute = Number(match[2] || 0);
  const mer = (match[3] || '').toLowerCase();
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (mer === 'pm' && hour < 12) hour += 12;
  if (mer === 'am' && hour === 12) hour = 0;
  if (!mer && hour >= 1 && hour <= 7) hour += 12;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return { hour24: hour, minute };
}

function parseTileTextRange(tileText) {
  if (!tileText) return null;
  const text = String(tileText);
  const match = text.match(
    /from\s*:?\s*(\d{1,2}:\d{2}\s*[ap]\.?m\.?)\s*[-–—]\s*to\s*:?\s*(\d{1,2}:\d{2}\s*[ap]\.?m\.?)/i,
  );
  if (!match) return null;
  return { start: match[1].replace(/\s+/g, ' ').trim(), end: match[2].replace(/\s+/g, ' ').trim() };
}

function getZonedYmdHm(ms, timeZone = BOOKING_TZ) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(new Date(ms));
  const map = Object.fromEntries(parts.filter((p) => p.type !== 'literal').map((p) => [p.type, p.value]));
  let hour = Number(map.hour);
  if (hour === 24) hour = 0;
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour,
    minute: Number(map.minute),
  };
}

function wallClockToUtcMs(isoDate, timeStr, timeZone = BOOKING_TZ) {
  const clock = parseClockParts(timeStr);
  if (!clock || !isoDate || !/^\d{4}-\d{2}-\d{2}$/.test(isoDate)) return null;
  const [y, mo, da] = isoDate.split('-').map(Number);
  let utcMs = Date.UTC(y, mo - 1, da, clock.hour24, clock.minute, 0);
  for (let i = 0; i < 5; i += 1) {
    const z = getZonedYmdHm(utcMs, timeZone);
    const desiredMin = clock.hour24 * 60 + clock.minute;
    const actualMin = z.hour * 60 + z.minute;
    const desiredDayNum = Date.UTC(y, mo - 1, da) / 86400000;
    const actualDayNum = Date.UTC(z.year, z.month - 1, z.day) / 86400000;
    const diffMin = (actualDayNum - desiredDayNum) * 24 * 60 + (actualMin - desiredMin);
    if (diffMin === 0) return utcMs;
    utcMs -= diffMin * 60 * 1000;
  }
  return utcMs;
}

function sessionIsoDate(session) {
  return session?.isoDate || session?.dateKey || session?.iso_date || null;
}

function resolveSessionWindow(session, timeZone = BOOKING_TZ) {
  const isoDate = sessionIsoDate(session);
  const tileText = session?.tileText || session?.raw?.tileText || '';
  const tileRange = parseTileTextRange(tileText);
  const startLabel = session?.time || session?.start_time || tileRange?.start || null;
  const endLabel = session?.end_time || session?.endTime || session?.endTimeLabel || tileRange?.end || null;

  let startMs = (isoDate && startLabel) ? wallClockToUtcMs(isoDate, startLabel, timeZone) : null;
  if (startMs == null) {
    const ts = session?.start_ts ?? session?.ts ?? session?.startTs;
    if (ts != null) {
      const n = Number(ts) * 1000;
      if (Number.isFinite(n)) startMs = n;
    }
  }

  let endMs = (isoDate && endLabel) ? wallClockToUtcMs(isoDate, endLabel, timeZone) : null;
  if (endMs == null && startMs != null) {
    const durationMin = Number(session?.durationMinutes);
    const fallback = Number.isFinite(durationMin) && durationMin > 0
      ? durationMin
      : DEFAULT_DURATION_MINUTES;
    endMs = startMs + fallback * 60 * 1000;
  }

  return {
    isoDate,
    startMs,
    endMs,
    startLabel,
    endLabel,
    timeZone,
  };
}

function sessionStartMs(session, timeZone = BOOKING_TZ) {
  return resolveSessionWindow(session, timeZone).startMs;
}

function sessionEndMs(session, timeZone = BOOKING_TZ) {
  return resolveSessionWindow(session, timeZone).endMs;
}

function sessionDurationMs(session, timeZone = BOOKING_TZ) {
  const { startMs, endMs } = resolveSessionWindow(session, timeZone);
  if (startMs == null || endMs == null) return DEFAULT_DURATION_MINUTES * 60 * 1000;
  return Math.max(0, endMs - startMs);
}

function isSessionLiveAt(session, nowMs = Date.now(), timeZone = BOOKING_TZ) {
  const { startMs, endMs } = resolveSessionWindow(session, timeZone);
  if (startMs == null || endMs == null) return false;
  return nowMs >= startMs && nowMs < endMs;
}

function isSessionPastAt(session, nowMs = Date.now(), timeZone = BOOKING_TZ) {
  const { endMs } = resolveSessionWindow(session, timeZone);
  if (endMs == null) return false;
  return nowMs >= endMs;
}

function formatClockLabel(timeStr) {
  const parts = parseClockParts(timeStr);
  if (!parts) return String(timeStr || '').trim();
  const h = parts.hour24 % 12 || 12;
  const mer = parts.hour24 >= 12 ? 'pm' : 'am';
  return `${h}:${String(parts.minute).padStart(2, '0')} ${mer}`;
}

function formatClockRange(session, timeZone = BOOKING_TZ) {
  const window = resolveSessionWindow(session, timeZone);
  if (window.startLabel && window.endLabel) {
    return `${formatClockLabel(window.startLabel)}–${formatClockLabel(window.endLabel)}`;
  }
  if (window.startMs == null || window.endMs == null) return session?.time || '—';
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone,
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
  const start = fmt.format(new Date(window.startMs)).replace(/\s*(AM|PM)/i, (m) => m.toLowerCase());
  const end = fmt.format(new Date(window.endMs)).replace(/\s*(AM|PM)/i, (m) => m.toLowerCase());
  const startBare = start.replace(/\s*(am|pm)$/i, '').trim();
  const endBare = end.replace(/\s*(am|pm)$/i, '').trim();
  const mer = (start.match(/(am|pm)$/i) || end.match(/(am|pm)$/i) || ['', ''])[0];
  return `${startBare}–${endBare} ${mer}`.trim();
}

function pickLiveSessionForSide(sessions, sideKey, nowMs = Date.now(), timeZone = BOOKING_TZ) {
  const live = (sessions || [])
    .filter((s) => isSessionLiveAt(s, nowMs, timeZone))
    .filter((s) => {
      if (sideKey === 'left') return s?.wave === 1 || /^left/i.test(String(s?.waveSide || s?.wave_side || ''));
      if (sideKey === 'right') return s?.wave === 2 || /^right/i.test(String(s?.waveSide || s?.wave_side || ''));
      return false;
    })
    .sort((a, b) => String(a.key || '').localeCompare(String(b.key || '')));
  return live[0] || null;
}

const api = {
  BOOKING_TZ,
  DEFAULT_DURATION_MINUTES,
  parseClockParts,
  parseTileTextRange,
  wallClockToUtcMs,
  resolveSessionWindow,
  sessionStartMs,
  sessionEndMs,
  sessionDurationMs,
  isSessionLiveAt,
  isSessionPastAt,
  formatClockRange,
  pickLiveSessionForSide,
};

if (typeof module !== 'undefined' && module.exports) {
  module.exports = api;
}

if (typeof window !== 'undefined') {
  window.BrowseLiveSchedule = api;
}
