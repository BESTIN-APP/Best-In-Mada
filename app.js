/**
 * APP.JS - BEST'IN MADAGASCAR .2026.
 * VERSION DURCIE : OFFLINE-FIRST PRIORITAIRE + SUPABASE COFFRE-FORT
 */

// --------------------------------------------------
// 0. VERROUILLAGE DOMAINE (ANTI-COPIE)
// --------------------------------------------------
(function enforceAllowedHostnames() {
    try {
        const allowed = new Set(['localhost', '127.0.0.1', 'bestinmada.com', 'www.bestinmada.com']);
        const host = (window.location.hostname || '').toLowerCase();
        if (!allowed.has(host)) {
            document.addEventListener('DOMContentLoaded', () => {
                if (document.body) document.body.innerHTML = '';
                alert('Sécurité BEST\'IN: domaine non autorisé.');
            });
        }
    } catch (_) {}
})();

// --------------------------------------------------
// 1. CONFIGURATION SUPABASE
// --------------------------------------------------
const _supabaseUrl = 'https://qtxxiffczpcombgbgunf.supabase.co';
const _supabaseKey = 'sb_publishable_U_geLLiGCSxlUYVHy9zTvw_9I1cUGGv';
const APP_VERSION = '16.0.0';

function makeSupabaseUnavailableError(message = 'Supabase indisponible') {
    return { name: 'SupabaseUnavailableError', message, code: 'SUPABASE_UNAVAILABLE' };
}

function createUnavailableQueryBuilder() {
    return {
        select: async () => ({ data: null, error: makeSupabaseUnavailableError(), count: 0 }),
        insert: async () => ({ data: null, error: makeSupabaseUnavailableError() }),
        update: async () => ({ data: null, error: makeSupabaseUnavailableError() }),
        upsert: async () => ({ data: null, error: makeSupabaseUnavailableError() }),
        delete: async () => ({ data: null, error: makeSupabaseUnavailableError() }),
        eq: () => createUnavailableQueryBuilder(),
        in: () => createUnavailableQueryBuilder(),
        gt: () => createUnavailableQueryBuilder(),
        order: () => createUnavailableQueryBuilder(),
        maybeSingle: async () => ({ data: null, error: makeSupabaseUnavailableError() }),
        single: async () => ({ data: null, error: makeSupabaseUnavailableError() })
    };
}

function createUnavailableSupabaseClient() {
    return {
        auth: {
            getSession: async () => ({ data: { session: null }, error: makeSupabaseUnavailableError() }),
            getUser: async () => ({ data: { user: null }, error: makeSupabaseUnavailableError() }),
            signInWithPassword: async () => ({ data: null, error: makeSupabaseUnavailableError() }),
            signUp: async () => ({ data: null, error: makeSupabaseUnavailableError() }),
            signOut: async () => ({ error: null }),
            resetPasswordForEmail: async () => ({ data: null, error: makeSupabaseUnavailableError() })
        },
        from: () => createUnavailableQueryBuilder(),
        rpc: async () => ({ data: null, error: makeSupabaseUnavailableError() })
    };
}

function createSupabaseClientSafely() {
    try {
        if (typeof window.supabase === 'undefined' || typeof window.supabase.createClient !== 'function') {
            return createUnavailableSupabaseClient();
        }
        return window.supabase.createClient(_supabaseUrl, _supabaseKey, {
            auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true }
        });
    } catch (_) {
        return createUnavailableSupabaseClient();
    }
}

const _sb = createSupabaseClientSafely();

function isSupabaseAvailable() {
    return typeof window.supabase !== 'undefined' && typeof window.supabase.createClient === 'function';
}

// --------------------------------------------------
// 2. CONFIGURATION INDEXEDDB
// --------------------------------------------------
const DB_NAME = 'BestInDB';
const DB_VERSION = 5;

let db = null;
let syncInProgress = false;
let syncScheduled = null;
let refreshInProgress = false;
let lastRefreshRuntimeAt = 0;
const REFRESH_MIN_INTERVAL_MS = 3 * 60 * 1000;

const BESTIN_TEXTS = {
    dashboard_settings: 'Paramètres',
    dashboard_delivery: 'Suivi<br>livraison',
    status_connected: 'Connecté',
    status_online: 'En ligne',
    status_offline: 'Mode Offline',
    profile_title: 'Mon Profil',
    profile_services_legal: 'Services & Légal',
    profile_settings_center: 'Centre Paramètres',
    logout_btn: 'DÉCONNEXION',
    settings_title: 'Paramètres',
    settings_preferences: "Préférences de l'application",
    settings_theme: 'Thème visuel',
    settings_hint: 'Le changement de thème est appliqué immédiatement et sauvegardé hors-ligne.',
    settings_saved: 'Préférences enregistrées.',
    settings_legal_help: 'Aide & Légal',
    settings_guides: 'Guides Utilisateur',
    settings_support: 'Support',
    settings_terms: 'Termes & Conditions',
    settings_privacy: 'Confidentialité',
    theme_dark: 'Sombre',
    theme_light: 'Clair',
    login_welcome: 'Bienvenue',
    login_email: 'ADRESSE EMAIL',
    login_password: 'MOT DE PASSE',
    login_submit: 'SE CONNECTER',
    login_remember: 'Rester connecté'
};

function t(key) { return BESTIN_TEXTS[key] || key; }

// --------------------------------------------------
// 3. HELPERS GÉNÉRAUX
// --------------------------------------------------
function nowIso() { return new Date().toISOString(); }

function getProfile() {
    return JSON.parse(localStorage.getItem('bestin_profile') || '{}');
}

function setProfile(profile) {
    const safeProfile = profile || {};
    localStorage.setItem('bestin_profile', JSON.stringify(safeProfile));
    if (safeProfile?.id) {
        localStorage.setItem(`bestin_profile_snapshot_${safeProfile.id}`, JSON.stringify(safeProfile));
    }
}

function getCachedAbo() {
    return JSON.parse(localStorage.getItem('cache_abo') || 'null');
}

function setCachedAbo(abo) {
    if (abo) localStorage.setItem('cache_abo', JSON.stringify(abo));
    else localStorage.removeItem('cache_abo');
}

function setAppMeta(key, value) {
    try { localStorage.setItem(key, JSON.stringify(value)); } catch (_) {}
}

function getAppMeta(key, fallback = null) {
    try {
        const raw = localStorage.getItem(key);
        return raw ? JSON.parse(raw) : fallback;
    } catch (_) {
        return fallback;
    }
}

function withLastUpdated(data = {}) {
    const ts = nowIso();
    return { ...data, updated_at: data.updated_at || ts, last_updated_at: data.last_updated_at || ts };
}

function sanitizeVentePayloadForCloud(payload = {}) {
    const next = { ...(payload || {}) };
    if (!next.statut) next.statut = 'VENDRE';
    if (next.quantite === undefined || next.quantite === null) next.quantite = 0;
    
    const fields =[
        'id', 'user_id', 'vendeur_id', 'article_id', 'article_nom', 
        'client_nom', 'client_contact', 'client_adresse', 'fulfillment_method',
        'livraison_statut', 'livraison_preuve_image', 
        'reference_facture', 'prix_unitaire', 'total_paye', 'created_at', 'updated_at',
        'statut', 'quantite',
        'livreur_id', 'livraison_updated_at', 'last_updated_at'
    ];
    
    const sanitized = {};
    fields.forEach(f => {
        if (next[f] !== undefined) sanitized[f] = next[f];
    });

    return sanitized;
}

function logSyncFailureContext(item, error) {
    try {
        console.error('[BESTIN][SYNC][ERROR]', {
            table: item?.table,
            action: item?.action,
            queue_id: item?.queue_id,
            message: error?.message || String(error || ''),
            code: error?.code || null,
            details: error?.details || null,
            hint: error?.hint || null,
            data: item?.data || null
        });
    } catch (_) {}
}

function updateGlobalSyncState(patch = {}) {
    const state = {
        pending_sync_count: getAppMeta('bestin_pending_sync_count', 0),
        last_sync_at: getAppMeta('bestin_last_sync_at', null),
        backend_unavailable: getAppMeta('bestin_backend_unavailable', false),
        sync_error_message: getAppMeta('bestin_sync_error_message', '')
    };
    const next = { ...state, ...patch };
    setAppMeta('bestin_pending_sync_count', next.pending_sync_count);
    setAppMeta('bestin_last_sync_at', next.last_sync_at);
    setAppMeta('bestin_backend_unavailable', next.backend_unavailable);
    setAppMeta('bestin_sync_error_message', next.sync_error_message);
    window.dispatchEvent(new CustomEvent('bestin-sync-state', { detail: next }));
}

async function ensureSession() {
    if (!isSupabaseAvailable()) return null;
    try {
        const { data: { session }, error } = await _sb.auth.getSession();
        if (error) return null;
        return session || null;
    } catch (_) {
        return null;
    }
}

function addSignature() {
    if (document.getElementById('global-signature')) return;
    const sig = document.createElement('div');
    sig.id = 'global-signature';
    sig.innerHTML = "BEST'IN Madagascar .2026.";
    sig.style.cssText = 'position:fixed; bottom:10px; width:100%; text-align:center; font-size:10px; color:#D4AF37; font-weight:bold; opacity:0.6; z-index:9999; pointer-events:none;';
    document.body.appendChild(sig);
}

async function logActivity(type, description) {
    if (!db) return;
    const profile = getProfile();
    const logEntry = {
        id: crypto.randomUUID(),
        type,
        description,
        user_id: profile.id || null,
        vendeur_id: profile.vendeur_id || null,
        created_at: nowIso(),
        last_updated_at: nowIso()
    };
    await localDB.save('activity_logs', logEntry);
    await localDB.addToSyncQueue('activity_logs', 'INSERT', logEntry);
}

async function finalizePendingProfileAfterRegister() {
    try {
        const pending = JSON.parse(localStorage.getItem('bestin_pending_profile_payload') || 'null');
        if (!pending || !pending.id) return;
        const session = await ensureSession();
        if (!session || session.user.id !== pending.id) return;

        const payload = withLastUpdated(pending);
        const result = await _sb.from('profiles').upsert([payload], { onConflict: 'id' });

        if (!result?.error) {
            await cacheProfileLocally(pending);
            localStorage.removeItem('bestin_pending_profile_payload');
        }
    } catch (_) {}
}

// --------------------------------------------------
// 4. THÈME
// --------------------------------------------------
function getBestinTheme() { return localStorage.getItem('bestin_theme') || 'dark'; }
function applyTheme(theme = getBestinTheme()) { document.documentElement.setAttribute('data-theme', theme); }
function setBestinTheme(theme) { localStorage.setItem('bestin_theme', theme); applyTheme(theme); }

function applyPageTranslations() {
    document.querySelectorAll('[data-i18n]').forEach((el) => {
        const key = el.getAttribute('data-i18n');
        if (key) el.innerHTML = t(key);
    });
}

window.BestinPrefs = { getTheme: getBestinTheme, setTheme: setBestinTheme, applyTheme, applyPageTranslations, t };
window.BestinRuntime = { isSupabaseAvailable };

// --------------------------------------------------
// 5. INDEXEDDB
// --------------------------------------------------
function wrapIDBRequest(request) {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}

function txDone(tx) {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error || new Error('Transaction IndexedDB échouée'));
        tx.onabort = () => reject(tx.error || new Error('Transaction IndexedDB annulée'));
    });
}

const initDB = () => {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onupgradeneeded = (e) => {
            const dbInstance = e.target.result;
            if (!dbInstance.objectStoreNames.contains('articles')) dbInstance.createObjectStore('articles', { keyPath: 'id' });
            if (!dbInstance.objectStoreNames.contains('ventes')) dbInstance.createObjectStore('ventes', { keyPath: 'id' });
            if (!dbInstance.objectStoreNames.contains('livraisons')) dbInstance.createObjectStore('livraisons', { keyPath: 'id' });
            if (!dbInstance.objectStoreNames.contains('sync_queue')) dbInstance.createObjectStore('sync_queue', { keyPath: 'queue_id', autoIncrement: true });
            if (!dbInstance.objectStoreNames.contains('activity_logs')) dbInstance.createObjectStore('activity_logs', { keyPath: 'id' });
            if (!dbInstance.objectStoreNames.contains('profiles_cache')) dbInstance.createObjectStore('profiles_cache', { keyPath: 'id' });
            if (!dbInstance.objectStoreNames.contains('abonnements_cache')) dbInstance.createObjectStore('abonnements_cache', { keyPath: 'user_id' });
            if (!dbInstance.objectStoreNames.contains('app_meta')) dbInstance.createObjectStore('app_meta', { keyPath: 'key' });
        };

        request.onsuccess = (e) => { db = e.target.result; resolve(db); };
        request.onerror = (e) => reject(new Error('Erreur IndexedDB : ' + e.target.errorCode));
    });
};

// CORRECTION BUG 2 : Refonte du filtre pour le Livreur
function matchesCurrentProfile(record) {
    const profile = getProfile();
    if (!record || !profile?.id) return false;
    
    if (profile.account_role === 'CHILD_LIVREUR') {
        // Un livreur voit les courses assignées à son ID, ou les courses générées par son parent (user_id/owner_id)
        return record.livreur_id === profile.id || 
               record.user_id === profile.parent_owner_id || 
               record.owner_id === profile.parent_owner_id;
    }
    
    return record.user_id === profile.id || (record.vendeur_id && record.vendeur_id === profile.vendeur_id);
}

const localDB = {
    save: async (storeName, data) => {
        if (!db) await initDB();

        const profile = getProfile();
        let payload = { ...data };

        if ((storeName === 'articles' || storeName === 'ventes' || storeName === 'activity_logs' || storeName === 'livraisons') && profile.id) {
            if (profile.account_role !== 'CHILD_LIVREUR') {
                payload.user_id = payload.user_id || profile.id;
                payload.vendeur_id = payload.vendeur_id || profile.vendeur_id || null;
            }
        }

        if (storeName === 'profiles_cache' && profile.id) payload.id = payload.id || profile.id;
        if (storeName === 'articles' || storeName === 'ventes' || storeName === 'activity_logs' || storeName === 'livraisons') payload = withLastUpdated(payload);

        const tx = db.transaction(storeName, 'readwrite');
        tx.objectStore(storeName).put(payload);
        await txDone(tx);
        return payload;
    },

    get: async (storeName, key) => {
        if (!db) await initDB();
        const tx = db.transaction(storeName, 'readonly');
        const item = await wrapIDBRequest(tx.objectStore(storeName).get(key));
        if (!item) return null;
        if (storeName === 'sync_queue' || storeName === 'app_meta' || storeName === 'profiles_cache' || storeName === 'abonnements_cache') return item;
        return matchesCurrentProfile(item) ? item : null;
    },

    getAll: async (storeName) => {
        if (!db) await initDB();
        const tx = db.transaction(storeName, 'readonly');
        const result = await wrapIDBRequest(tx.objectStore(storeName).getAll());
        if (storeName === 'sync_queue' || storeName === 'app_meta' || storeName === 'profiles_cache' || storeName === 'abonnements_cache') return result || [];
        return (result ||[]).filter(matchesCurrentProfile);
    },

    remove: async (storeName, key) => {
        if (!db) await initDB();
        if (key === undefined || key === null) return;
        const tx = db.transaction(storeName, 'readwrite');
        tx.objectStore(storeName).delete(key);
        await txDone(tx);
    },

    clear: async (storeName) => {
        if (!db) await initDB();
        const tx = db.transaction(storeName, 'readwrite');
        tx.objectStore(storeName).clear();
        await txDone(tx);
    },

    addToSyncQueue: async (table, action, data) => {
        if (!db) await initDB();

        const profile = getProfile();
        const payload = {
            table,
            action,
            data: withLastUpdated({ ...data }),
            user_id: data?.user_id || profile.id || null,
            vendeur_id: data?.vendeur_id || profile.vendeur_id || null,
            timestamp: Date.now(),
            attempts: 0,
            status: 'pending',
            last_error: '',
            last_attempt_at: null
        };

        const tx = db.transaction('sync_queue', 'readwrite');
        tx.objectStore('sync_queue').add(payload);
        await txDone(tx);

        const pending = await localDB.getAll('sync_queue');
        updateGlobalSyncState({ pending_sync_count: pending.length });
        scheduleSync(120);
    }
};

async function handleUserChange() {
    const profile = getProfile();
    if (!profile.id) return;
    localStorage.setItem('bestin_last_user_id', profile.id);
}

// --------------------------------------------------
// 6. CACHE LOCAL GLOBAL
// --------------------------------------------------
async function cacheProfileLocally(profileData) {
    if (!profileData || !profileData.id) return;
    await localDB.save('profiles_cache', profileData);
    setProfile(profileData);
    localStorage.setItem('shop_name', profileData.shop_name || '');
    localStorage.setItem('shop_address', profileData.adresse_boutique || '');
    localStorage.setItem('shop_tel', profileData.phone || '');
}

async function cacheAbonnementLocally(abo) {
    if (!abo || !abo.user_id) return;
    await localDB.save('abonnements_cache', abo);
    setCachedAbo(abo);
}

async function getLocalProfile() {
    const cached = getProfile();
    if (cached && cached.id) return cached;

    const session = await ensureSession();
    if (!session) return {};

    const local = await localDB.get('profiles_cache', session.user.id);
    if (local) {
        setProfile(local);
        return local;
    }

    const snapshotRaw = localStorage.getItem(`bestin_profile_snapshot_${session.user.id}`);
    if (snapshotRaw) {
        try {
            const snapshot = JSON.parse(snapshotRaw);
            if (snapshot?.id === session.user.id) {
                setProfile(snapshot);
                return snapshot;
            }
        } catch (_) {}
    }
    return {};
}

// --------------------------------------------------
// 7. MOTEUR DE SYNC
// --------------------------------------------------
function scheduleSync(delay = 300) {
    clearTimeout(syncScheduled);
    syncScheduled = setTimeout(() => syncData(), delay);
}

function isTransientSupabaseError(error) {
    const msg = String(error?.message || '').toLowerCase();
    const code = String(error?.code || '').toLowerCase();
    return (
        !navigator.onLine ||
        msg.includes('failed to fetch') ||
        msg.includes('network') ||
        msg.includes('timeout') ||
        msg.includes('fetch') ||
        msg.includes('temporar') ||
        msg.includes('unavailable') ||
        msg.includes('gateway') ||
        msg.includes('rate limit') ||
        msg.includes('supabase indisponible') ||
        ['500', '502', '503', '504'].includes(code)
    );
}

async function writeVenteWithFallback(item, rawData) {
    const payload = sanitizeVentePayloadForCloud(rawData || {});

    let result = await _sb.from('ventes').upsert([payload], { onConflict: 'id' });
    if (!result?.error) return result;

    logSyncFailureContext(item, result.error);

    result = await _sb.from('ventes').insert([payload]);
    if (result?.error) logSyncFailureContext(item, result.error);

    return result;
}

async function applyQueueItem(item, profile) {
    const data = item?.table === 'ventes' ? sanitizeVentePayloadForCloud(item?.data || {}) : (item?.data || {});
    let error = null;

    if (item.table === 'profiles') {
        const payload = withLastUpdated({ ...data, id: data.id || profile.id });
        const result = await _sb.from('profiles').upsert([payload], { onConflict: 'id' });
        error = result?.error || null;
    } else if (item.table === 'abonnements') {
        const payload = withLastUpdated(data);
        const result = await _sb.from('abonnements').upsert([payload], { onConflict: 'user_id' });
        error = result?.error || null;
    } else if (item.action === 'INSERT') {
        if (item.table === 'ventes') {
            const result = await writeVenteWithFallback(item, data);
            error = result?.error || null;
        } else {
            const payload = withLastUpdated(data);
            const result = await _sb.from(item.table).upsert([payload], { onConflict: 'id' });
            error = result?.error || null;
        }
    } else if (item.action === 'UPDATE') {
        const payload = withLastUpdated({ ...data });
        let result;
        
        // CORRECTION BUG 2 (PUSH) : Payload sécurisé pour le livreur afin de passer la barrière RLS
        if (item.table === 'livraisons' && profile.account_role === 'CHILD_LIVREUR') {
            const safePayload = {
                statut: payload.statut,
                preuve_image: payload.preuve_image,
                updated_at: payload.updated_at,
                last_updated_at: payload.last_updated_at
            };
            result = await _sb.from(item.table).update(safePayload).eq('id', payload.id);
        } else {
            result = await _sb.from(item.table).upsert([payload], { onConflict: 'id' });
        }
        
        error = result?.error || null;
    } else if (item.action === 'UPDATE_BY_VENTE' && item.table === 'livraisons') {
        const payload = { statut: data.statut, updated_at: data.updated_at };
        if (data.livreur_id !== undefined) payload.livreur_id = data.livreur_id;
        const result = await _sb.from('livraisons').update(payload).eq('vente_id', data.vente_id);
        error = result?.error || null;
    } else if (item.action === 'DELETE') {
        const { error: err } = await _sb.from(item.table).delete().eq('id', data.id);
        error = err || null;
    }

    if (error) logSyncFailureContext(item, error);
    return error;
}

async function updateQueueItem(queueId, patch) {
    if (!db) await initDB();
    if (queueId === undefined || queueId === null) return;
    const tx = db.transaction('sync_queue', 'readwrite');
    const store = tx.objectStore('sync_queue');
    const existing = await wrapIDBRequest(store.get(queueId));
    if (!existing) { await txDone(tx); return; }
    store.put({ ...existing, ...patch });
    await txDone(tx);
}

async function deleteQueueItem(queueId) {
    if (queueId === undefined || queueId === null) return;
    await localDB.remove('sync_queue', queueId);
}

async function deleteQueueItemByFingerprint(item) {
    if (!db || !item) return;
    const tx = db.transaction('sync_queue', 'readwrite');
    const store = tx.objectStore('sync_queue');
    const all = await wrapIDBRequest(store.getAll());

    const target = (all ||[]).find((row) =>
        row &&
        row.table === item.table &&
        row.action === item.action &&
        (row.timestamp || 0) === (item.timestamp || 0) &&
        (row.data?.id || null) === (item.data?.id || null) &&
        (row.user_id || null) === (item.user_id || null)
    );

    if (!target) { await txDone(tx); return; }

    if (target.queue_id !== undefined && target.queue_id !== null) {
        store.delete(target.queue_id);
        await txDone(tx);
        return;
    }

    const remaining = (all ||[]).filter((row) => row !== target);
    store.clear();
    await txDone(tx);

    for (const row of remaining) {
        const clean = { ...row };
        delete clean.queue_id;
        await localDB.addToSyncQueue(clean.table, clean.action, clean.data);
    }
}

async function syncData() {
    if (!navigator.onLine || syncInProgress || !db) return;
    syncInProgress = true;

    try {
        const profile = await getLocalProfile();
        if (!profile.id) return;

        const queue = await localDB.getAll('sync_queue');
        updateGlobalSyncState({ pending_sync_count: queue.length });

        if (queue.length === 0) {
            updateGlobalSyncState({ backend_unavailable: false, sync_error_message: '', last_sync_at: nowIso() });
            return;
        }

        if (!isSupabaseAvailable()) {
            updateGlobalSyncState({
                backend_unavailable: true,
                sync_error_message: 'Supabase indisponible, les données restent locales.',
                last_sync_at: nowIso()
            });
            return;
        }

        const session = await ensureSession();
        if (!session) {
            updateGlobalSyncState({
                backend_unavailable: true,
                sync_error_message: 'Session cloud indisponible, les données restent locales.',
                last_sync_at: nowIso()
            });
            return;
        }

        let transientFailureDetected = false;
        let lastErrorMessage = '';
        const sortedQueue = [...queue].sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

        for (const item of sortedQueue) {
            if (!item) continue;
            if (item.user_id && profile.account_role !== 'CHILD_LIVREUR' && item.user_id !== profile.id) continue;

            try {
                if (item.queue_id !== undefined && item.queue_id !== null) {
                    await updateQueueItem(item.queue_id, {
                        status: 'processing',
                        attempts: (item.attempts || 0) + 1,
                        last_attempt_at: nowIso()
                    });
                }

                const error = await applyQueueItem(item, profile);

                if (!error) {
                    if (item.queue_id === undefined || item.queue_id === null) await deleteQueueItemByFingerprint(item);
                    else await deleteQueueItem(item.queue_id);
                } else {
                    const transient = isTransientSupabaseError(error);
                    transientFailureDetected = transientFailureDetected || transient;
                    lastErrorMessage = error.message || 'Erreur de synchronisation';

                    if (transient) {
                        if (item.queue_id !== undefined && item.queue_id !== null) {
                            await updateQueueItem(item.queue_id, {
                                status: 'pending',
                                last_error: lastErrorMessage
                            });
                        }
                        break; 
                    } else {
                        logSyncFailureContext(item, error);
                        if (item.queue_id !== undefined && item.queue_id !== null) await deleteQueueItem(item.queue_id);
                        else await deleteQueueItemByFingerprint(item);
                    }
                }
            } catch (e) {
                transientFailureDetected = true;
                lastErrorMessage = e?.message || 'Erreur réseau ou backend indisponible';
                logSyncFailureContext(item, e);

                if (item.queue_id !== undefined && item.queue_id !== null) {
                    await updateQueueItem(item.queue_id, {
                        status: 'pending',
                        last_error: lastErrorMessage,
                        last_attempt_at: nowIso()
                    });
                }
                break;
            }
        }

        const remaining = await localDB.getAll('sync_queue');
        updateGlobalSyncState({
            pending_sync_count: remaining.length,
            backend_unavailable: transientFailureDetected,
            sync_error_message: lastErrorMessage,
            last_sync_at: nowIso()
        });

        if (!transientFailureDetected && remaining.length === 0) {
            await refreshLocalCache({ force: false, source: 'sync' });
        }
    } finally {
        syncInProgress = false;
    }
}

async function pullTableIncremental(tableName, userColumn, userId, storeName, lastPullAt) {
    const profile = getProfile();
    let query = _sb.from(tableName).select('*');

    // CORRECTION BUG 2 (PULL) : On retire le filtre 'owner_id' qui plantait. On laisse RLS filtrer les lignes.
    if (tableName === 'livraisons' && profile.account_role === 'CHILD_LIVREUR') {
        // La politique Supabase RLS renvoie automatiquement uniquement les données autorisées pour le livreur
    } else {
        query = query.eq('vendeur_id', profile.vendeur_id);
    }

    if (lastPullAt) query = query.gt('updated_at', lastPullAt);

    const { data, error } = await query;
    if (error || !Array.isArray(data) || data.length === 0) return;

    const tx = db.transaction(storeName, 'readwrite');
    const store = tx.objectStore(storeName);
    const allLocal = await wrapIDBRequest(store.getAll());
    
    for (const row of data) {
        const localRow = allLocal.find(x => x.id === row.id);
        if (localRow) {
            const localTime = new Date(localRow.updated_at || 0).getTime();
            const cloudTime = new Date(row.updated_at || 0).getTime();
            
            // Sécurité anti-boomerang
            if (localTime > cloudTime) {
                continue; 
            }
            if (tableName === 'ventes') {
                row.livraison_statut = row.livraison_statut || localRow.livraison_statut;
                row.livraison_updated_at = row.livraison_updated_at || localRow.livraison_updated_at;
            }
        }
        store.put(row);
    }
    await txDone(tx);
}

async function syncLivraisonsToVentes() {
    const profile = getProfile();
    if (profile.account_role === 'CHILD_LIVREUR') return;

    const { data, error } = await _sb.from('livraisons').select('vente_id, livreur_id, statut, preuve_image, updated_at').eq('vendeur_id', profile.vendeur_id);
    
    if (error || !Array.isArray(data) || data.length === 0) return;

    const tx = db.transaction('ventes', 'readwrite');
    const store = tx.objectStore('ventes');
    const allVentes = await wrapIDBRequest(store.getAll());

    let updated = false;
    for (const liv of data) {
        const v = allVentes.find(x => x.id === liv.vente_id);
        if (v) {
            if (v.statut === 'ANNULEE' || v.livraison_statut === 'ANNULEE') continue;

            let changed = false;
            const localTime = new Date(v.livraison_updated_at || 0).getTime();
            const cloudTime = new Date(liv.updated_at || 0).getTime();
            
            if (cloudTime > localTime || (!v.livraison_updated_at && cloudTime > 0)) {
                if (liv.livreur_id !== undefined && v.livreur_id !== liv.livreur_id) { v.livreur_id = liv.livreur_id; changed = true; }
                if (liv.statut && v.livraison_statut !== liv.statut) { v.livraison_statut = liv.statut; changed = true; }
                if (liv.preuve_image && v.livraison_preuve_image !== liv.preuve_image) { v.livraison_preuve_image = liv.preuve_image; changed = true; }
                
                if (changed) {
                    v.livraison_updated_at = liv.updated_at;
                    store.put(v);
                    updated = true;
                }
            }
        }
    }
    if (updated) await txDone(tx);
}

async function refreshLocalCache(options = {}) {
    const { force = false } = options;
    if (!navigator.onLine || !db || !isSupabaseAvailable()) return;
    if (refreshInProgress) return;

    const elapsed = Date.now() - lastRefreshRuntimeAt;
    if (!force && elapsed < REFRESH_MIN_INTERVAL_MS) return;

    refreshInProgress = true;
    try {
        const session = await ensureSession();
        if (!session?.user?.id) return;
        const userId = session.user.id;
        
        const lastPullAt = getAppMeta('bestin_last_cloud_pull_at', null);
        let forcePull = false;

        const [profRes, aboRes] = await Promise.all([
            _sb.from('profiles').select('*').eq('id', userId).single(),
            _sb.from('abonnements').select('*').eq('user_id', userId).maybeSingle()
        ]);

        if (profRes?.data) await cacheProfileLocally(profRes.data);
        if (aboRes?.data) await cacheAbonnementLocally(aboRes.data);

        const role = (profRes?.data?.account_role || getProfile().account_role || 'OWNER').toUpperCase();
        
        if (role === 'CHILD_LIVREUR') {
            const localLivraisons = await localDB.getAll('livraisons');
            if (!localLivraisons || localLivraisons.length === 0) forcePull = true;
        } else {
            const localArticles = await localDB.getAll('articles');
            if (!localArticles || localArticles.length === 0) forcePull = true;
        }

        const effectivePullAt = forcePull ? null : lastPullAt;
        
        if (role === 'CHILD_LIVREUR') {
            const parentId = profRes?.data?.parent_owner_id || getProfile().parent_owner_id;
            await pullTableIncremental('livraisons', 'owner_id', parentId, 'livraisons', effectivePullAt);
        } else {
            await Promise.all([
                pullTableIncremental('articles', 'user_id', userId, 'articles', effectivePullAt),
                pullTableIncremental('ventes', 'user_id', userId, 'ventes', effectivePullAt),
                pullTableIncremental('activity_logs', 'user_id', userId, 'activity_logs', effectivePullAt)
            ]);
            await syncLivraisonsToVentes();
        }

        setAppMeta('bestin_last_cloud_pull_at', nowIso());
        lastRefreshRuntimeAt = Date.now();
    } catch (_) {
    } finally {
        refreshInProgress = false;
    }
}

// --------------------------------------------------
// 8. PWA INSTALL PROMPT FIXE
// --------------------------------------------------
function initInstallPromptUI() {
    const page = (location.pathname.split('/').pop() || '').toLowerCase();
    if (!['login.html', 'dashboard.html', 'register.html', ''].includes(page)) return;

    const isStandalone = window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone;
    if (isStandalone || localStorage.getItem('bestin_install_dismissed')) return;

    let deferredPrompt = null;

    const overlay = document.createElement('div');
    overlay.id = 'bestin-install-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;z-index:10000;background:rgba(5,10,24,.92);display:none;align-items:center;justify-content:center;padding:20px;';

    const box = document.createElement('div');
    box.style.cssText = 'position:relative;background:#16213e;color:#fff;text-align:center;padding:25px;border-radius:20px;border:1px solid rgba(255,255,255,.1);max-width:350px;width:100%;box-sizing:border-box;';

    box.innerHTML = `
        <img src="BESTIN.png" alt="Logo" style="width:60px;height:60px;border-radius:12px;margin-bottom:15px;border:1px solid #D4AF37;">
        <h3 style="margin:0 0 10px;font-size:1.2em;">Installer BEST'IN</h3>
        <p style="font-size:.9em;opacity:.8;margin-bottom:20px;">Accédez à l'application depuis votre écran d'accueil, même hors ligne.</p>
        <button id="bestin-install-btn" style="width:100%;background:#2d62ff;color:#fff;border:none;border-radius:12px;padding:15px;font-weight:800;font-size:1em;cursor:pointer;">Installer l'Application</button>
        <button id="bestin-dismiss-btn" style="position:absolute;top:10px;right:10px;background:none;border:none;color:#fff;font-size:1.5em;cursor:pointer;line-height:1;">&times;</button>
    `;

    overlay.appendChild(box);
    document.body.appendChild(overlay);

    const showOverlay = () => { overlay.style.display = 'flex'; };
    const hideOverlay = () => { overlay.style.display = 'none'; };

    document.getElementById('bestin-install-btn').onclick = async () => {
        if (!deferredPrompt) return;
        hideOverlay();
        deferredPrompt.prompt();
        await deferredPrompt.userChoice;
        deferredPrompt = null;
        localStorage.setItem('bestin_install_dismissed', '1');
    };

    document.getElementById('bestin-dismiss-btn').onclick = () => {
        hideOverlay();
        localStorage.setItem('bestin_install_dismissed', '1');
    };

    window.addEventListener('beforeinstallprompt', (e) => {
        e.preventDefault();
        deferredPrompt = e;
        showOverlay();
    });
}

// --------------------------------------------------
// 9. UX MISE À JOUR SW
// --------------------------------------------------
function initSwUpdateUX() {
    if (!('serviceWorker' in navigator)) return;
    navigator.serviceWorker.addEventListener('controllerchange', () => {
        if (!window.__bestinRefreshing) {
            window.__bestinRefreshing = true;
            location.reload();
        }
    });
}

function compareSemver(a, b) {
    const pa = String(a || '').split('.').map((x) => parseInt(x, 10) || 0);
    const pb = String(b || '').split('.').map((x) => parseInt(x, 10) || 0);
    const max = Math.max(pa.length, pb.length);
    for (let i = 0; i < max; i++) {
        const da = pa[i] || 0;
        const dbv = pb[i] || 0;
        if (da > dbv) return 1;
        if (da < dbv) return -1;
    }
    return 0;
}

async function checkRemoteAppVersion() {
    if (!navigator.onLine || !isSupabaseAvailable()) return;
    try {
        const { data, error } = await _sb.from('settings').select('value').eq('key', 'app_version').maybeSingle();
        if (error || !data?.value) return;
        const remoteVersion = String(data.value).trim();
        if (compareSemver(remoteVersion, APP_VERSION) > 0) {
            const already = localStorage.getItem('bestin_update_notified_version');
            if (already !== remoteVersion) {
                localStorage.setItem('bestin_update_notified_version', remoteVersion);
                alert(`Une nouvelle version (${remoteVersion}) est disponible. Connectez-vous en ligne pour mettre à jour l'application.`);
            }
        }
    } catch (_) {}
}

async function forceAppUpdate(buttonEl, statusEl) {
    if (!navigator.onLine) {
        if (statusEl) statusEl.innerText = "Erreur : Connexion internet requise pour vérifier les mises à jour.";
        return;
    }
    
    if (!('serviceWorker' in navigator)) {
        if (statusEl) statusEl.innerText = "Non supporté sur ce navigateur.";
        return;
    }

    if (buttonEl) buttonEl.disabled = true;
    if (statusEl) statusEl.innerText = "Recherche de mise à jour en cours...";

    try {
        const reg = await navigator.serviceWorker.getRegistration();
        if (!reg) {
            if (statusEl) statusEl.innerText = "Aucun moteur hors-ligne actif trouvé.";
            if (buttonEl) buttonEl.disabled = false;
            return;
        }

        await reg.update();

        if (reg.waiting) {
            reg.waiting.postMessage({ type: 'SKIP_WAITING' });
            return;
        } 
        
        if (reg.installing) {
            if (statusEl) statusEl.innerText = "Installation de la nouvelle version...";
            reg.installing.addEventListener('statechange', (e) => {
                if (e.target.state === 'installed') {
                    e.target.postMessage({ type: 'SKIP_WAITING' });
                }
            });
            return;
        }

        reg.addEventListener('updatefound', () => {
            const newWorker = reg.installing;
            if (newWorker) {
                if (statusEl) statusEl.innerText = "Téléchargement de la mise à jour...";
                newWorker.addEventListener('statechange', () => {
                    if (newWorker.state === 'installed') {
                        newWorker.postMessage({ type: 'SKIP_WAITING' });
                    }
                });
            }
        });

        setTimeout(() => {
            if (!reg.installing && !reg.waiting) {
                if (statusEl) statusEl.innerText = "✅ L'application est déjà à jour.";
                if (buttonEl) buttonEl.disabled = false;
            }
        }, 2000);

    } catch (err) {
        if (statusEl) statusEl.innerText = "Échec de la vérification : " + err.message;
        if (buttonEl) buttonEl.disabled = false;
    }
}

window.forceAppUpdate = forceAppUpdate;

// --------------------------------------------------
// 10. RÔLES
// --------------------------------------------------
function enforceRoleAccessForPage() {
    const profile = getProfile();
    const role = (profile?.account_role || 'OWNER').toUpperCase();
    if (role !== 'CHILD_LIVREUR') return;

    const page = (location.pathname.split('/').pop() || 'index.html').toLowerCase();
    
    const allowedPages = new Set(['dashboard.html', 'espace_livreur.html', 'parametres.html', 'profil.html', 'login.html', 'reset-password.html', 'support.html', 'termes.html', 'confidentialite.html']);
    
    if (!allowedPages.has(page)) location.assign('dashboard.html');
}

// --------------------------------------------------
// 11. SERVICE WORKER
// --------------------------------------------------
if ('serviceWorker' in navigator) {
    window.addEventListener('load', async () => {
        try {
            await navigator.serviceWorker.register('./sw.js');
            initSwUpdateUX();
        } catch (_) {}
    });
}

// --------------------------------------------------
// 12. INIT APP
// --------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
    applyTheme();
    applyPageTranslations();
    initInstallPromptUI();
    addSignature();
    enforceRoleAccessForPage();

    try {
        await initDB();
        await finalizePendingProfileAfterRegister();

        const localProfile = await getLocalProfile();
        if (localProfile?.id) await handleUserChange();

        const dot = document.getElementById('dot');
        const setStatus = () => { if (dot) dot.style.background = navigator.onLine ? '#2ecc71' : '#f39c12'; };

        window.addEventListener('online', () => {
            setStatus();
            scheduleSync(300);
            refreshLocalCache({ force: true, source: 'online' });
            checkRemoteAppVersion();
        });

        window.addEventListener('offline', setStatus);
        setStatus();

        if (navigator.onLine) {
            await syncData();
            await refreshLocalCache({ force: false, source: 'boot' });
            await checkRemoteAppVersion();
        }

        if (typeof initPage === 'function') initPage();
        else if (typeof init === 'function') init();
        else if (typeof render === 'function') render();
    } catch (err) {
        console.error(err);
    }
});