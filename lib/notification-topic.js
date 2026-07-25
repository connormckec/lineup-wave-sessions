'use strict';

const MAX_TOPIC_LENGTH = 64;
const TOPIC_PATTERN = /^[a-zA-Z0-9_-]+$/;

function validateNtfyTopic(raw) {
  const topic = String(raw || '').trim();
  if (!topic) {
    return { ok: false, error: 'ntfy_topic_required' };
  }
  if (topic.length > MAX_TOPIC_LENGTH) {
    return { ok: false, error: 'ntfy_topic_too_long' };
  }
  if (/[\s/\\:@]/.test(topic) || /^https?:/i.test(topic)) {
    return { ok: false, error: 'ntfy_topic_invalid' };
  }
  if (!TOPIC_PATTERN.test(topic)) {
    return { ok: false, error: 'ntfy_topic_invalid' };
  }
  return { ok: true, topic };
}

function maskDestination(value) {
  const text = String(value || '').trim();
  if (!text) return '—';
  if (text.length <= 4) return '••••';
  return `${text.slice(0, 2)}••••${text.slice(-2)}`;
}

module.exports = {
  MAX_TOPIC_LENGTH,
  TOPIC_PATTERN,
  validateNtfyTopic,
  maskDestination,
};
