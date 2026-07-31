'use strict';

const SESSION_CAPACITY_BY_LEVEL = Object.freeze({
  progressive: 18,
  intermediate: 12,
  'advanced turns': 12,
  'advanced barrels': 12,
  'expert turns': 12,
  'expert barrels': 12,
  'pro turns': 10,
  'pro barrels': 10,
});

function normalizeLevelName(level) {
  if (level == null) return '';
  return String(level).trim().toLowerCase().replace(/\s+/g, ' ');
}

function resolveCapacityForLevel(level) {
  const key = normalizeLevelName(level);
  if (!key) return null;
  return Object.prototype.hasOwnProperty.call(SESSION_CAPACITY_BY_LEVEL, key)
    ? SESSION_CAPACITY_BY_LEVEL[key]
    : null;
}

function resolveConfiguredCapacity(session, { isLessonSession = null } = {}) {
  if (!session) return null;
  if (typeof isLessonSession === 'function' && isLessonSession(session)) return null;
  return resolveCapacityForLevel(session.level || session.session_type || session.sessionType);
}

const capacityConfig = {
  SESSION_CAPACITY_BY_LEVEL,
  normalizeLevelName,
  resolveCapacityForLevel,
  resolveConfiguredCapacity,
};

if (typeof module !== 'undefined' && module.exports) {
  module.exports = capacityConfig;
}

if (typeof window !== 'undefined') {
  window.LineupCapacity = capacityConfig;
}
