/// <reference lib="webworker" />

import { clientsClaim } from "workbox-core";
import { cleanupOutdatedCaches, precacheAndRoute } from "workbox-precaching";
import { NavigationRoute, registerRoute } from "workbox-routing";
import { NetworkFirst } from "workbox-strategies";

declare let self: ServiceWorkerGlobalScope;

// Activate immediately on install
self.skipWaiting();
clientsClaim();

// Precache all build assets (injected by vite-plugin-pwa at build time)
precacheAndRoute(self.__WB_MANIFEST);

// Remove stale precache entries from previous versions
cleanupOutdatedCaches();

// Network-first for navigation: try the actual server first, fall back to
// cached index.html only when the network is unavailable. This ensures that
// if a different server is running at this origin, it gets served instead of
// the stale cached browser client.
registerRoute(
  new NavigationRoute(
    new NetworkFirst({
      cacheName: "navigation-cache",
      networkTimeoutSeconds: 3,
    }),
  ),
);
