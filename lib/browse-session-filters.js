'use strict';

(function initBrowseSessionFilters() {
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

  function isLessonWaveSide(waveSide) {
    const text = String(waveSide || '').replace(/\s+/g, ' ').trim();
    if (!text) return false;
    return /\b(Left|Right)\s+Lesson\b/i.test(text);
  }

  function lessonTextFields(session) {
    if (!session) return [];
    return [
      session.level,
      session.session_type,
      session.sessionType,
      session.tileText,
      session.raw?.tileText,
      session.waveSide,
      session.wave_side,
      session.displayName,
      session.display_name,
      session.category,
    ]
      .filter((value) => value != null && String(value).trim())
      .map((value) => String(value));
  }

  function textContainsLessonWord(text) {
    return /\blesson\b/i.test(String(text || ''));
  }

  /**
   * Canonical lesson detection for Browse filtering.
   * Primary: any relevant display field containing the word "lesson".
   * Also matches Left Lesson / Right Lesson waveSide and lesson agenda columns.
   */
  function isLessonSession(session) {
    if (!session) return false;
    if (isLessonWaveSide(sessionWaveSide(session))) return true;
    if (lessonTextFields(session).some(textContainsLessonWord)) return true;
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

  const sessionFilters = {
    LESSON_WAVE_INDICES,
    sessionWaveSide,
    sessionWaveIndex,
    isLessonWaveSide,
    lessonTextFields,
    isLessonSession,
    matchesWaveFilter,
    filterByWaveAndLessons,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = sessionFilters;
  }

  if (typeof window !== 'undefined') {
    window.LineupBrowse = window.LineupBrowse || {};
    window.LineupBrowse.sessionFilters = sessionFilters;
  }
}());
