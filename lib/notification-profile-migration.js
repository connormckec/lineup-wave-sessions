'use strict';

const profileAuth = require('./profile-auth');

async function migrateProfileData(supabase, fromUserKey, toUserKey) {
  if (!supabase || !fromUserKey || !toUserKey) {
    return { ok: false, error: 'missing_arguments' };
  }
  if (fromUserKey === toUserKey) {
    return { ok: false, error: 'same_user_key' };
  }

  const { data: watches, error: watchErr } = await supabase
    .from('watchlist_items')
    .select('*')
    .eq('user_key', fromUserKey)
    .eq('active', true);
  if (watchErr) throw watchErr;

  let watchesMigrated = 0;
  for (const watch of watches || []) {
    const { data: existing } = await supabase
      .from('watchlist_items')
      .select('id')
      .eq('user_key', toUserKey)
      .eq('session_key', watch.session_key)
      .maybeSingle();
    if (existing?.id) {
      await supabase.from('watchlist_items').update({ active: false }).eq('id', watch.id);
      continue;
    }
    const { error } = await supabase
      .from('watchlist_items')
      .update({ user_key: toUserKey })
      .eq('id', watch.id);
    if (error) throw error;
    watchesMigrated += 1;
  }

  const { data: fromProfile } = await supabase
    .from('notification_profiles')
    .select('*')
    .eq('user_key', fromUserKey)
    .maybeSingle();
  const { data: toProfile } = await supabase
    .from('notification_profiles')
    .select('*')
    .eq('user_key', toUserKey)
    .maybeSingle();

  let profileMigrated = false;
  if (fromProfile?.ntfy_topic && !toProfile?.ntfy_topic) {
    const now = new Date().toISOString();
    const { error } = await supabase.from('notification_profiles').upsert({
      user_key: toUserKey,
      ntfy_topic: fromProfile.ntfy_topic,
      topic_valid: fromProfile.topic_valid,
      topic_updated_at: fromProfile.topic_updated_at || now,
      updated_at: now,
    }, { onConflict: 'user_key' });
    if (error) throw error;
    profileMigrated = true;
  }

  return {
    ok: true,
    fromUserKey,
    toUserKey,
    watchesMigrated,
    profileMigrated,
  };
}

async function migrateProfileByCodes(supabase, fromCode, toCode) {
  const fromUserKey = profileAuth.deriveUserKeyFromProfileCodeSync(fromCode);
  const toUserKey = profileAuth.deriveUserKeyFromProfileCodeSync(toCode);
  if (!fromUserKey || !toUserKey) {
    return { ok: false, error: 'invalid_profile_code' };
  }
  return migrateProfileData(supabase, fromUserKey, toUserKey);
}

module.exports = {
  migrateProfileData,
  migrateProfileByCodes,
};
