/// <reference lib="webworker" />

import { clientsClaim } from "workbox-core";
import { cleanupOutdatedCaches, createHandlerBoundToURL, precacheAndRoute } from "workbox-precaching";
import { NavigationRoute, registerRoute } from "workbox-routing";

declare let self: ServiceWorkerGlobalScope;

// Activate immediately on install
self.skipWaiting();
clientsClaim();

// Precache all build assets (injected by vite-plugin-pwa at build time)
precacheAndRoute(self.__WB_MANIFEST);

// Remove stale precache entries from previous versions
cleanupOutdatedCaches();

// SPA fallback: serve cached index.html for all navigation requests
registerRoute(new NavigationRoute(createHandlerBoundToURL("index.html")));
