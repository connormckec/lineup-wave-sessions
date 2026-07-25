'use strict';

const MAX_VAPID_KEY_LENGTH = 512;
const MAX_VAPID_SUBJECT_LENGTH = 512;

function isValidVapidSubject(subject) {
  const text = String(subject || '').trim();
  if (!text || text.length > MAX_VAPID_SUBJECT_LENGTH) return false;
  if (/^mailto:/i.test(text)) {
    const addr = text.slice(7).trim();
    return addr.includes('@') && !/\s/.test(addr);
  }
  try {
    const parsed = new URL(text);
    return parsed.protocol === 'https:' || parsed.protocol === 'http:';
  } catch {
    return false;
  }
}

function readWebPushConfig(env = process.env) {
  const enabled = env.WEB_PUSH_ENABLED === 'true';
  const publicKey = String(env.WEB_PUSH_VAPID_PUBLIC_KEY || '').trim();
  const privateKey = String(env.WEB_PUSH_VAPID_PRIVATE_KEY || '').trim();
  const subject = String(env.WEB_PUSH_VAPID_SUBJECT || '').trim();

  const hasPublic = !!publicKey;
  const hasPrivate = !!privateKey;
  const subjectValid = isValidVapidSubject(subject);
  const configured = hasPublic && hasPrivate && subjectValid;

  let configError = null;
  if (enabled && !configured) {
    if (!hasPublic || !hasPrivate) {
      configError = 'vapid_keys_incomplete';
    } else if (!subjectValid) {
      configError = 'vapid_subject_invalid';
    }
  }

  return {
    enabled,
    configured,
    publicKey: hasPublic ? publicKey : null,
    privateKey: hasPrivate ? privateKey : null,
    subject: subjectValid ? subject : null,
    configError,
    vapidConfigured: configured,
  };
}

module.exports = {
  MAX_VAPID_KEY_LENGTH,
  isValidVapidSubject,
  readWebPushConfig,
};
