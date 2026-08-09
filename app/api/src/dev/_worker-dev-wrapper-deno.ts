import { createHandler } from "metapages/worker/routing/handlerDeno";

import { handlerHttp } from "@/handlerHono.ts";
import { handleWebsocketConnection } from "@/handlerWs.ts";

const APP_FQDN = Deno.env.get("APP_FQDN") || "https://connect.superslides.io";

const requestHandler = createHandler(handlerHttp, handleWebsocketConnection);

const config = {
  onError: (e: unknown) => {
    console.error(e);
    return Response.error();
  },
  onListen: ({ hostname, port }: { hostname: string; port: number }) => {
    console.log(
      `🚀🌙 Listening on APP_FQDN=${APP_FQDN} hostname=${hostname} port=${port}`,
    );
  },
};

const httpsConfig = {
  ...config,
  onListen: ({ hostname, port }: { hostname: string; port: number }) => {
    console.log(
      `🔒 HTTPS Server listening on APP_FQDN=${APP_FQDN} hostname=${hostname} port=${port}`,
    );
  },
};

const httpConfig = {
  ...config,
  onListen: ({ hostname, port }: { hostname: string; port: number }) => {
    console.log(
      `🌐 HTTP Server listening on APP_FQDN=${APP_FQDN} hostname=${hostname} port=${port}`,
    );
  },
};

Deno.serve({
  port: 3001,
  cert: Deno.readTextFileSync("../.traefik/certs/local-cert.pem"),
  key: Deno.readTextFileSync("../.traefik/certs/local-key.pem"),
  ...httpsConfig,
}, requestHandler);

Deno.serve({
  port: 3002,
  ...httpConfig,
}, requestHandler);
