'use strict';

const { validateNtfyTopic } = require('./notification-topic');

async function getNotificationProfile(supabase, userKey) {
  if (!supabase || !userKey) return null;
  const { data, error } = await supabase
    .from('notification_profiles')
    .select('*')
    .eq('user_key', userKey)
    .maybeSingle();
  if (error) throw error;
  return data || null;
}

async function upsertNotificationProfile(supabase, userKey, ntfyTopic) {
  const validation = validateNtfyTopic(ntfyTopic);
  if (!validation.ok) {
    return { ok: false, error: validation.error };
  }
  const now = new Date().toISOString();
  const row = {
    user_key: userKey,
    ntfy_topic: validation.topic,
    topic_valid: true,
    topic_updated_at: now,
    updated_at: now,
  };
  const { data, error } = await supabase
    .from('notification_profiles')
    .upsert(row, { onConflict: 'user_key' })
    .select('*')
    .single();
  if (error) throw error;
  return { ok: true, profile: data };
}

async function resolveDestinationForUser(supabase, userKey, {
  serverFallbackTopic = null,
  allowServerFallback = false,
} = {}) {
  const profile = await getNotificationProfile(supabase, userKey);
  const topic = (profile?.ntfy_topic || '').trim();
  if (topic && profile?.topic_valid !== false) {
    return { ok: true, destination: topic, source: 'notification_profiles' };
  }
  if (allowServerFallback && serverFallbackTopic) {
    return { ok: true, destination: serverFallbackTopic.trim(), source: 'server_fallback' };
  }
  return { ok: false, error: 'no_destination' };
}

function profileResponseForClient(profile) {
  const topic = (profile?.ntfy_topic || '').trim();
  return {
    hasTopic: !!topic,
    topicValid: profile?.topic_valid === true,
    topicUpdatedAt: profile?.topic_updated_at || null,
  };
}

module.exports = {
  getNotificationProfile,
  upsertNotificationProfile,
  resolveDestinationForUser,
  profileResponseForClient,
};
