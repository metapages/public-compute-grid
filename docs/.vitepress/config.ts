import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

export default withMermaid(
  defineConfig({
    title: "compute queues — private Docker job queues anyone can add compute to",
    description:
      "Open-source compute queues for the public internet. A queue is a URL, a job is a Docker container, and adding compute is one docker run. Submit from a browser, a Node/Deno backend, or the CLI.",
    base: "/docs/",

    // The api rewrites extensionless /docs/* requests onto the built .html
    // files, so links can drop the extension. See app/api/src/handlerHono.ts.
    cleanUrls: true,

    // The api server serves this site with `serveStatic({ root: "docs/dist" })`,
    // so a request for /docs/x must resolve to docs/dist/docs/x — hence the
    // extra `docs` segment in outDir. See app/api/src/handlerHono.ts.
    outDir: "./dist/docs",

    ignoreDeadLinks: [/^http:\/\/localhost/, /^https:\/\/worker-metaframe\.localhost/],

    // public/** holds the distributable Agent Skill: markdown that is copied
    // verbatim for download, not compiled as VitePress pages.
    srcExclude: ["public/**"],

    themeConfig: {
      siteTitle: "compute queues",

      nav: [
        { text: "Docs", link: "/quickstart" },
        { text: "Building containers", link: "/guide/building-containers" },
        { text: "Backend integration", link: "/guide/backend-integration" },
        { text: "AI skill", link: "/guide/agent-skill" },
        { text: "Client", link: "https://container.mtfm.io" },
      ],

      sidebar: [
        { text: "Quickstart", link: "/quickstart" },
        {
          text: "Guide",
          items: [
            { text: "How it works", link: "/guide/overview" },
            { text: "Queues, jobs & ids", link: "/guide/queues-and-jobs" },
            { text: "Job definition", link: "/guide/job-definition" },
            { text: "Building containers", link: "/guide/building-containers" },
            { text: "Running workers", link: "/guide/workers" },
            { text: "Backend integration", link: "/guide/backend-integration" },
            { text: "Files in & out", link: "/guide/files" },
          ],
        },
        {
          text: "Reference",
          items: [
            { text: "REST API", link: "/guide/rest-api" },
            { text: "WebSocket API", link: "/guide/websocket-api" },
          ],
        },
        {
          text: "AI agents",
          items: [
            { text: "Agent Skill", link: "/guide/agent-skill" },
            { text: "llms.txt", link: "https://container.mtfm.io/llms.txt" },
          ],
        },
      ],

      search: {
        provider: "local",
      },

      socialLinks: [
        {
          icon: "github",
          link: "https://github.com/metapages/compute-queues",
        },
      ],
    },
  }),
);
