const STATIC_CACHE = 'bestin-static-v34';
const RUNTIME_CACHE = 'bestin-runtime-v34';
const CDN_CACHE = 'bestin-cdn-v34';

const ASSETS_TO_CACHE = [
  './',
  './index.html',
  './login.html',
  './register.html',
  './dashboard.html',
  './articles.html',
  './add_article.html',
  './view_category.html',
  './stocks.html',
  './ventes.html',
  './livraison.html',
  './factures.html',
  './recettes.html',
  './historique.html',
  './notifications.html',
  './profil.html',
  './equipe_livreurs.html',
  './recompenses.html',
  './parametres.html',
  './guides.html',
  './support.html',
  './abonnement.html',
  './admin.html',
  './confidentialite.html',
  './termes.html',
  './style.css',
  './reset-password.html',
  './espace_livreur.html',
  './register_livreur.html',
  './app.js',
  './manifest.json',
  './BESTIN.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil((async () => {
    const cache = await caches.open(STATIC_CACHE);
    await Promise.allSettled(ASSETS_TO_CACHE.map((asset) => cache.add(asset)));
    await self.skipWaiting();
  })());
});

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(
      keys
        .filter((k) => ![STATIC_CACHE, RUNTIME_CACHE, CDN_CACHE].includes(k))
        .map((k) => caches.delete(k))
    );
    await self.clients.claim();
  })());
});

self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') self.skipWaiting();
});

function isSupabaseRequest(url) {
  return url.hostname.includes('supabase.co');
}

function isJsDelivr(url) {
  return url.hostname.includes('cdn.jsdelivr.net');
}

async function staleWhileRevalidate(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request, { ignoreSearch: false });

  const networkFetch = fetch(request)
    .then((response) => {
      if (response && response.ok) cache.put(request, response.clone());
      return response;
    })
    .catch(() => null);

  if (cached) return cached;
  return (await networkFetch) || null;
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  const url = new URL(req.url);

  if (req.method !== 'GET') return;

  // Ne jamais mettre Supabase en cache (évite conflits + egress inutile)
  if (isSupabaseRequest(url)) return;

  // CDN jsDelivr -> stale-while-revalidate
  if (isJsDelivr(url)) {
    event.respondWith((async () => {
      const response = await staleWhileRevalidate(req, CDN_CACHE);
      return response || new Response('', { status: 503, statusText: 'Offline CDN unavailable' });
    })());
    return;
  }

  // Navigation pages HTML
  if (req.mode === 'navigate') {
    event.respondWith((async () => {
      const cachedPage =
        (await caches.match(req, { ignoreSearch: true })) ||
        (await caches.match(url.pathname, { ignoreSearch: true }));

      if (cachedPage) {
        event.waitUntil((async () => {
          try {
            const fresh = await fetch(req);
            if (fresh && fresh.ok) {
              (await caches.open(RUNTIME_CACHE)).put(req, fresh.clone());
            }
          } catch (_) {}
        })());
        return cachedPage;
      }

      try {
        const network = await fetch(req);
        if (network && network.ok) {
          (await caches.open(RUNTIME_CACHE)).put(req, network.clone());
        }
        return network;
      } catch (_) {
        return (
          (await caches.match('./dashboard.html')) ||
          (await caches.match('./login.html')) ||
          new Response('Offline', { status: 503, statusText: 'Offline' })
        );
      }
    })());
    return;
  }

  // Assets runtime
  event.respondWith((async () => {
    const cached =
      (await caches.match(req)) ||
      (await caches.match(url.pathname, { ignoreSearch: true }));

    if (cached) {
      event.waitUntil((async () => {
        try {
          const fresh = await fetch(req);
          if (fresh && fresh.ok) {
            (await caches.open(RUNTIME_CACHE)).put(req, fresh.clone());
          }
        } catch (_) {}
      })());
      return cached;
    }

    try {
      const network = await fetch(req);
      if (network && network.ok) {
        (await caches.open(RUNTIME_CACHE)).put(req, network.clone());
      }
      return network;
    } catch (_) {
      if (req.destination === 'image') {
        return (await caches.match('./BESTIN.png')) || new Response('Offline', { status: 503 });
      }
      return new Response('Offline', { status: 503, statusText: 'Offline' });
    }
  })());
});