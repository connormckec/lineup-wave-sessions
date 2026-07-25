'use strict';

/** Agenda column indices for lesson columns (fallback when waveSide is missing). */
const LESSON_WAVE_INDICES = new Set([3, 4]);

function sessionWaveSide(session) {
  if (!session) return '';
  const raw = session.waveSide ?? session.wave_side ?? '';
  return String(raw).replace(/\s+/g, ' ').trim();
}

function sessionWaveIndex(session) {
  if (!session) return null;
  const raw = session.wave ?? session.raw?.wave ?? null;
  if (raw == null || raw === '') return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

/**
 * True when waveSide is exactly Left Lesson or Right Lesson (case-insensitive).
 * Uses a strict Left|Right + Lesson pattern — does not match unrelated product names.
 */
function isLessonWaveSide(waveSide) {
  const text = String(waveSide || '').replace(/\s+/g, ' ').trim();
  if (!text) return false;
  return /\b(Left|Right)\s+Lesson\b/i.test(text);
}

/**
 * Canonical lesson detection for Browse filtering.
 * Primary: normalized waveSide (Left Lesson / Right Lesson).
 * Fallback: agenda column index 3 or 4 when waveSide is absent.
 */
function isLessonSession(session) {
  if (!session) return false;
  if (isLessonWaveSide(sessionWaveSide(session))) return true;
  const wave = sessionWaveIndex(session);
  return wave != null && LESSON_WAVE_INDICES.has(wave);
}

/**
 * Wave chip + show-lessons filter used by Browse.
 * Lesson hiding applies even when a Left/Right wave chip is active.
 */
function matchesWaveFilter(session, { showLessons = false, activeWave = null } = {}) {
  if (!showLessons && isLessonSession(session)) return false;
  if (activeWave != null) return sessionWaveIndex(session) === activeWave;
  return true;
}

function filterByWaveAndLessons(sessions, options) {
  return sessions.filter((session) => matchesWaveFilter(session, options));
}

const api = {
  LESSON_WAVE_INDICES,
  sessionWaveSide,
  sessionWaveIndex,
  isLessonWaveSide,
  isLessonSession,
  matchesWaveFilter,
  filterByWaveAndLessons,
};

if (typeof module !== 'undefined' && module.exports) {
  module.exports = api;
}

if (typeof window !== 'undefined') {
  window.BrowseSessionFilters = api;
}
