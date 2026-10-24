import type { Context, Hono } from "hono";
import { serveStatic } from "hono/deno";

import { createApp } from "@/handlerHono.ts";

/**
 * Static files from disk, for the Deno server: the llms.txt, the docs site and
 * the browser app. Registered after every API route: the app catch-alls must
 * come last.
 */
export const addDenoStaticRoutes = (app: Hono): void => {
  // Serve llms.txt file from public folder
  app.get("/llms.txt", serveStatic({ path: "app/browser/public/llms.txt" }));

  // VitePress docs site (source: docs/, build: `just docs build`).
  // The site is built with base=/docs/ into docs/dist/docs, so the extra `docs`
  // segment in the request path maps straight onto the directory below the root
  // and no path rewriting is needed. Deployment copies docs/dist — see the
  // `deploy` recipe in app/api/justfile.
  app.get("/docs", (c: Context) => c.redirect("/docs/", 301));
  app.get("/docs/*", serveStatic({ root: "docs/dist" }));
  // `cleanUrls` means links point at /docs/quickstart, but the build still emits
  // quickstart.html, so extensionless requests need the suffix added back.
  app.get("/docs/*", serveStatic({ root: "docs/dist", rewriteRequestPath: (path) => `${path}.html` }));
  // Everything else under /docs is a missing doc, not a browser-app route: fall
  // through to VitePress' own 404 page rather than the client's index.html.
  app.get("/docs/*", async (c: Context) => {
    const notFoundPage = await Deno.readTextFile("docs/dist/docs/404.html").catch(() => undefined);
    return notFoundPage ? c.html(notFoundPage, 404) : c.notFound();
  });

  /**
   * index.html for a page served one directory down (`/j/<jobId>`). The built
   * asset URLs are relative — vite's `base` is empty so the app can also be served
   * from a subpath — which means `./assets/…` would resolve to `/j/assets/…` and
   * 404. Giving the existing `<base>` tag an href repoints them at the origin root
   * without touching the build. Read per request so a rebuilt dist is picked up.
   */
  const serveSpaOneLevelDown = async (indexPath: string, c: Context): Promise<Response> => {
    const html = await Deno.readTextFile(indexPath).catch(() => undefined);
    if (!html) {
      return c.notFound();
    }
    return c.html(
      html.includes("<base ")
        ? html.replace("<base ", '<base href="/" ')
        : html.replace("<head>", '<head><base href="/">'),
    );
  };

  // The bare /j/<jobId> is the browser page for a job (the SPA loads the
  // definition by id). It needs its own route: the static handler below answers a
  // missing file with 404 rather than falling through, so the index.html catch-all
  // at the end never runs. Registered after /j/<jobId>.json so that stays JSON.
  app.get("/j/:jobId", (c: Context) => serveSpaOneLevelDown("app/browser/dist/index.html", c));

  // Serve static assets, and the index.html as the fallback
  app.get("/*", serveStatic({ root: "app/browser/dist" }));
  app.get("/", serveStatic({ path: "app/browser/dist/index.html" }));
  app.get("*", serveStatic({ path: "app/browser/dist/index.html" }));
};

/**
 * The Deno server's request handler: the API plus static files from disk.
 * Kept out of handlerHono.ts because hono's Deno serveStatic reads Deno
 * globals at import, and the Cloudflare Worker imports handlerHono.ts.
 */
export const handlerHttp = createApp({ serveStatic: addDenoStaticRoutes }).fetch as (
  request: Request,
) => Promise<Response | undefined>;
