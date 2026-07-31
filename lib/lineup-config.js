'use strict';

(function initLineupConfig(root) {
  if (!root || root.LineupConfig) return;
  root.LineupConfig = Object.freeze({
    BOOKING_TZ: 'America/New_York',
  });
})(typeof window !== 'undefined' ? window : null);

if (typeof module !== 'undefined' && module.exports) {
  module.exports = Object.freeze({ BOOKING_TZ: 'America/New_York' });
}
