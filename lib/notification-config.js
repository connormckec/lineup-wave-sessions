'use strict';

const { readWebPushConfig } = require('./web-push-config');

const VALID_DELIVERY_PROVIDERS = new Set(['webpush', 'ntfy', 'disabled']);

function normalizeDeliveryProvider(raw) {
  const value = String(raw || 'disabled').trim().toLowerCase();
  if (!VALID_DELIVERY_PROVIDERS.has(value)) return 'disabled';
  return value;
}

function readNotificationConfig(env = process.env) {
  const legacyInlineWatchAlertsEnabled = env.LEGACY_INLINE_WATCH_ALERTS_ENABLED === 'true';
  const allowInternalDefaultNtfyTopic = env.ALLOW_INTERNAL_DEFAULT_NTFY_TOPIC === 'true';
  const supabaseConfigured = !!(env.SUPABASE_URL && env.SUPABASE_SERVICE_ROLE_KEY);
  const durableNotificationPipelineEnabled = supabaseConfigured;
  const webPush = readWebPushConfig(env);
  const notificationDeliveryProvider = normalizeDeliveryProvider(env.NOTIFICATION_DELIVERY_PROVIDER);
  const showNtfyDevUi = env.SHOW_NTFY_DEV_UI === 'true';

  const dualRealAlertSystemsEnabled = legacyInlineWatchAlertsEnabled && durableNotificationPipelineEnabled;
  const providerMismatch = notificationDeliveryProvider === 'webpush' && !webPush.configured;
  const webPushEnabledButProviderDisabled = webPush.enabled && notificationDeliveryProvider === 'disabled';

  return {
    legacyInlineWatchAlertsEnabled,
    allowInternalDefaultNtfyTopic,
    durableNotificationPipelineEnabled,
    dualRealAlertSystemsEnabled,
    webPushEnabled: webPush.enabled,
    webPushConfigured: webPush.configured,
    webPushConfigError: webPush.configError,
    webPushPublicKey: webPush.publicKey,
    notificationDeliveryProvider,
    showNtfyDevUi,
    providerMismatch,
    webPushEnabledButProviderDisabled,
  };
}

function logNotificationStartup(config) {
  console.log('── Notification delivery config ──');
  console.log(`  notification delivery provider: ${config.notificationDeliveryProvider}`);
  console.log(`  web push enabled: ${config.webPushEnabled ? 'true' : 'false'}`);
  console.log(`  VAPID configured: ${config.webPushConfigured ? 'yes' : 'no'}`);
  console.log(`  legacy inline watch alerts: ${config.legacyInlineWatchAlertsEnabled ? 'enabled' : 'disabled'}`);
  console.log(`  durable notification pipeline: ${config.durableNotificationPipelineEnabled ? 'enabled' : 'disabled'}`);
  if (config.allowInternalDefaultNtfyTopic) {
    console.warn('  WARNING: ALLOW_INTERNAL_DEFAULT_NTFY_TOPIC=true — insecure shared-topic fallback is enabled');
  }
  if (config.dualRealAlertSystemsEnabled) {
    console.warn('  WARNING: both legacy inline watch alerts and durable notification pipeline are enabled — users may receive duplicate notifications');
  }
  if (config.notificationDeliveryProvider === 'webpush' && !config.webPushConfigured) {
    console.warn('  WARNING: NOTIFICATION_DELIVERY_PROVIDER=webpush but VAPID is incomplete — Web Push deliveries will fail until configured');
  }
  if (config.webPushEnabledButProviderDisabled) {
    console.warn('  WARNING: WEB_PUSH_ENABLED=true but NOTIFICATION_DELIVERY_PROVIDER is disabled — subscription UI may work without durable deliveries');
  }
  if (config.notificationDeliveryProvider === 'ntfy') {
    console.warn('  WARNING: ntfy rollback provider active — production should use webpush');
  }
}

module.exports = {
  VALID_DELIVERY_PROVIDERS,
  normalizeDeliveryProvider,
  readNotificationConfig,
  logNotificationStartup,
};
