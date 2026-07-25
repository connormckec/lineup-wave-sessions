/* Lineup Web Push client — pairs with lib/push-subscription-store.js server routes */
(function (global) {
  'use strict';

  const DEVICE_INSTALL_STORAGE = 'ap_device_install_id';

  function supportsServiceWorker() {
    return 'serviceWorker' in navigator;
  }

  function supportsPushManager() {
    return supportsServiceWorker() && 'PushManager' in window;
  }

  function supportsNotifications() {
    return typeof Notification !== 'undefined';
  }

  function isStandaloneMode() {
    return window.matchMedia('(display-mode: standalone)').matches
      || window.navigator.standalone === true;
  }

  function getOrCreateDeviceInstallId() {
    let id = localStorage.getItem(DEVICE_INSTALL_STORAGE);
    if (!id) {
      id = (global.crypto && crypto.randomUUID)
        ? crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      localStorage.setItem(DEVICE_INSTALL_STORAGE, id);
    }
    return id;
  }

  function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
    const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
    const raw = atob(base64);
    const arr = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i += 1) arr[i] = raw.charCodeAt(i);
    return arr;
  }

  function defaultDeviceLabel() {
    const ua = navigator.userAgent || '';
    if (/iPhone/i.test(ua)) return 'iPhone';
    if (/iPad/i.test(ua)) return 'iPad';
    if (/Android/i.test(ua)) return 'Android';
    if (/Mac/i.test(ua)) return 'Mac';
    if (/Windows/i.test(ua)) return 'Windows';
    return 'This device';
  }

  function deriveUiState({
    webPushEnabled = false,
    webPushConfigured = false,
    permission = 'default',
    standalone = false,
    thisDeviceActive = false,
    repairNeeded = false,
  } = {}) {
    if (!supportsNotifications() || !supportsPushManager()) {
      return 'unsupported';
    }
    if (!webPushEnabled || !webPushConfigured) {
      return 'unavailable';
    }
    if (!standalone && /iPhone|iPad|iPod/i.test(navigator.userAgent || '')) {
      return 'needs_homescreen';
    }
    if (permission === 'denied') return 'denied';
    if (repairNeeded) return 'repair';
    if (thisDeviceActive && permission === 'granted') return 'enabled';
    if (permission === 'granted') return 'repair';
    return 'ready';
  }

  function stateMessage(state) {
    switch (state) {
      case 'unsupported':
        return 'Notifications are not supported in this browser.';
      case 'unavailable':
        return 'Web Push is temporarily unavailable.';
      case 'needs_homescreen':
        return 'Add Lineup to your Home Screen to enable notifications on iPhone.';
      case 'ready':
        return 'Ready to enable notifications on this device.';
      case 'enabled':
        return 'Enabled on this device.';
      case 'denied':
        return 'Notification permission denied. Enable notifications for Lineup in Settings.';
      case 'repair':
        return 'Subscription needs repair — tap Enable notifications again.';
      default:
        return '';
    }
  }

  async function getServiceWorkerRegistration() {
    if (!supportsServiceWorker()) return null;
    return navigator.serviceWorker.ready;
  }

  async function getBrowserSubscription() {
    const reg = await getServiceWorkerRegistration();
    if (!reg?.pushManager) return null;
    return reg.pushManager.getSubscription();
  }

  async function fetchVapidPublicKey(fetchFn) {
    const res = await fetchFn('/api/push/vapid-public-key');
    const body = await res.json().catch(() => ({}));
    if (!res.ok || !body.publicKey) {
      throw Object.assign(new Error(body.error || 'vapid_unavailable'), { code: body.error || 'vapid_unavailable' });
    }
    return body.publicKey;
  }

  async function syncSubscriptionToServer(fetchFn, subscription, {
    permissionState = Notification.permission,
    deviceLabel = defaultDeviceLabel(),
  } = {}) {
    const deviceInstallId = getOrCreateDeviceInstallId();
    const res = await fetchFn('/api/push/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        subscription: subscription.toJSON(),
        deviceInstallId,
        deviceLabel,
        permissionState,
      }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw Object.assign(new Error(body.error || 'subscribe_failed'), { code: body.error, status: res.status });
    }
    return body;
  }

  async function enableNotifications(fetchFn) {
    if (!supportsPushManager()) {
      throw Object.assign(new Error('unsupported'), { code: 'unsupported' });
    }
    const permission = await Notification.requestPermission();
    if (permission !== 'granted') {
      return { ok: false, permission, state: permission === 'denied' ? 'denied' : 'ready' };
    }
    const publicKey = await fetchVapidPublicKey(fetchFn);
    const reg = await getServiceWorkerRegistration();
    if (!reg) throw Object.assign(new Error('service_worker_unavailable'), { code: 'service_worker_unavailable' });
    let subscription = await reg.pushManager.getSubscription();
    if (!subscription) {
      subscription = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(publicKey),
      });
    }
    await syncSubscriptionToServer(fetchFn, subscription, { permissionState: permission });
    return { ok: true, permission, state: 'enabled' };
  }

  async function resyncIfGranted(fetchFn) {
    if (!supportsPushManager() || Notification.permission !== 'granted') {
      return { ok: false, skipped: true, reason: 'permission_not_granted' };
    }
    const subscription = await getBrowserSubscription();
    if (!subscription) {
      return { ok: false, skipped: true, reason: 'no_browser_subscription' };
    }
    await syncSubscriptionToServer(fetchFn, subscription);
    return { ok: true, resynced: true };
  }

  async function disableOnThisDevice(fetchFn) {
    const subscription = await getBrowserSubscription();
    if (subscription) {
      try { await subscription.unsubscribe(); } catch (e) { console.warn('[push] unsubscribe', e); }
    }
    const res = await fetchFn('/api/push/subscribe', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ deviceInstallId: getOrCreateDeviceInstallId() }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw Object.assign(new Error(body.error || 'unsubscribe_failed'), { code: body.error, status: res.status });
    }
    return { ok: true, ...body };
  }

  async function fetchPushStatus(fetchFn) {
    const deviceInstallId = getOrCreateDeviceInstallId();
    const res = await fetchFn(`/api/push/status?deviceInstallId=${encodeURIComponent(deviceInstallId)}`, {
      headers: { 'X-Lineup-Device-Install-Id': deviceInstallId },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw Object.assign(new Error(body.error || 'status_failed'), { code: body.error, status: res.status });
    }
    const state = deriveUiState({
      webPushEnabled: body.enabled,
      webPushConfigured: body.configured,
      permission: supportsNotifications() ? Notification.permission : 'denied',
      standalone: isStandaloneMode(),
      thisDeviceActive: body.thisDeviceActive,
      repairNeeded: body.repairNeeded,
    });
    return { ...body, uiState: state, stateMessage: stateMessage(state) };
  }

  async function sendTestNotification(fetchFn) {
    const deviceInstallId = getOrCreateDeviceInstallId();
    const res = await fetchFn('/api/notify/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ deviceInstallId }),
    });
    const body = await res.json().catch(() => ({}));
    return { ok: res.ok, status: res.status, body };
  }

  function formatPushError(status, errorCode, opts) {
    const network = opts && opts.network;
    if (network) return 'Could not reach the server — check your connection and try again.';
    switch (errorCode) {
      case 'profile_code_missing':
      case 'profile_auth_required':
      case 'invalid_profile_secret':
        return 'Save a profile sync code in Settings first.';
      case 'web_push_disabled':
      case 'webpush_not_configured':
      case 'vapid_unavailable':
        return 'Web Push is temporarily unavailable.';
      case 'push_subscription_not_found':
        return 'Enable notifications on this device first.';
      case 'subscription_expired':
        return 'Subscription needs repair — tap Enable notifications again.';
      case 'timeout':
      case 'provider_transport_failed':
      case 'network_error':
      case 'push_service_error':
        return 'Could not deliver the test notification — try again shortly.';
      case 'test_rate_limited':
        return 'Test notification cooldown — wait a minute and try again.';
      default:
        if (status === 401) return 'Save a profile sync code in Settings first.';
        if (status === 429) return 'Test notification cooldown — wait a minute and try again.';
        if (status === 502) return 'Could not deliver the test notification — try again shortly.';
        if (status === 503) return 'Web Push is temporarily unavailable.';
        return 'Test notification failed — try again.';
    }
  }

  global.LineupPushClient = {
    DEVICE_INSTALL_STORAGE,
    supportsServiceWorker,
    supportsPushManager,
    supportsNotifications,
    isStandaloneMode,
    getOrCreateDeviceInstallId,
    deriveUiState,
    stateMessage,
    getBrowserSubscription,
    enableNotifications,
    resyncIfGranted,
    disableOnThisDevice,
    fetchPushStatus,
    sendTestNotification,
    formatPushError,
  };
})(typeof window !== 'undefined' ? window : globalThis);
