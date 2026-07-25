'use strict';

function readNotificationConfig(env = process.env) {
  const legacyInlineWatchAlertsEnabled = env.LEGACY_INLINE_WATCH_ALERTS_ENABLED === 'true';
  const allowInternalDefaultNtfyTopic = env.ALLOW_INTERNAL_DEFAULT_NTFY_TOPIC === 'true';
  const supabaseConfigured = !!(env.SUPABASE_URL && env.SUPABASE_SERVICE_ROLE_KEY);
  const durableNotificationPipelineEnabled = supabaseConfigured;

  return {
    legacyInlineWatchAlertsEnabled,
    allowInternalDefaultNtfyTopic,
    durableNotificationPipelineEnabled,
    dualRealAlertSystemsEnabled: legacyInlineWatchAlertsEnabled && durableNotificationPipelineEnabled,
  };
}

function logNotificationStartup(config) {
  console.log('── Notification delivery config ──');
  console.log(`  legacy inline watch alerts: ${config.legacyInlineWatchAlertsEnabled ? 'enabled' : 'disabled'}`);
  console.log(`  durable notification pipeline: ${config.durableNotificationPipelineEnabled ? 'enabled' : 'disabled'}`);
  if (config.allowInternalDefaultNtfyTopic) {
    console.warn('  WARNING: ALLOW_INTERNAL_DEFAULT_NTFY_TOPIC=true — insecure shared-topic fallback is enabled');
  }
  if (config.dualRealAlertSystemsEnabled) {
    console.warn('  WARNING: both legacy inline watch alerts and durable notification pipeline are enabled — users may receive duplicate notifications');
  }
}

module.exports = {
  readNotificationConfig,
  logNotificationStartup,
};
