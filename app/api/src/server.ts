import { createHandler } from "metapages/worker/routing/handlerDeno";

import { handlerHttp } from "@/handlerHono.ts";
import { handleWebsocketConnection } from "@/handlerWs.ts";

const port = parseInt(Deno.env.get("PORT") || "8000");
const httpsPort = parseInt(Deno.env.get("HTTPS_PORT") || "8443");

const requestHandler = createHandler(handlerHttp, handleWebsocketConnection);

const config = {
  onError: (e: unknown) => {
    console.error(e);
    return Response.error();
  },
  onListen: ({ hostname, port }: { hostname: string; port: number }) => {
    console.log(`🚀🌙 HTTP Server listening on hostname=${hostname} port=${port}`);
  },
};

const httpsConfig = {
  ...config,
  onListen: ({ hostname, port }: { hostname: string; port: number }) => {
    console.log(`🔒 HTTPS Server listening on hostname=${hostname} port=${port}`);
  },
};

// Always start HTTP server
Deno.serve({
  port,
  ...config,
}, requestHandler);

// Try to start HTTPS server if certificates are available
try {
  const certPath = "../.traefik/certs/local-cert.pem";
  const keyPath = "../.traefik/certs/local-key.pem";

  // Check if certificate files exist
  try {
    Deno.readTextFileSync(certPath);
    Deno.readTextFileSync(keyPath);

    // Certificates exist, start HTTPS server
    Deno.serve({
      port: httpsPort,
      cert: Deno.readTextFileSync(certPath),
      key: Deno.readTextFileSync(keyPath),
      ...httpsConfig,
    }, requestHandler);

    console.log(`✅ HTTPS server started on port ${httpsPort}`);
  } catch {
    console.log(`ℹ️  HTTPS certificates not found, only HTTP server running on port ${port}`);
  }
} catch (error) {
  console.log(`ℹ️  HTTPS server not started: ${error instanceof Error ? error.message : String(error)}`);
}
