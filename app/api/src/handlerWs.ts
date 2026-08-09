import { wsHandlerClient, wsHandlerWorker } from "@/routes/websocket.ts";
import { attachMCPWebSocket } from "@/routes/mcp/websocket.ts";

export const handleWebsocketConnection = (
  socket: WebSocket,
  request: Request,
) => {
  const urlBlob = new URL(request.url);
  const pathTokens = urlBlob.pathname.split("/").filter((x) => x !== "");

  // Every websocket upgrade is dispatched here, before Hono sees it — so a
  // `app.get("/mcp/ws", …)` route can never fire and MCP clients were silently
  // closed with "Unknown type". Route them here instead.
  if (pathTokens[0] === "mcp" && pathTokens[1] === "ws") {
    attachMCPWebSocket(socket);
    return;
  }

  // previous deprecated routes, now queues always start with /q/<:queueId>
  let queueKey = pathTokens[0];
  let isClient = pathTokens[1] === "browser" || pathTokens[1] === "client";
  let isWorker = pathTokens[1] === "worker";

  if (pathTokens[0] === "q") {
    queueKey = pathTokens[1];
    isClient = pathTokens[2] === "browser" || pathTokens[2] === "client";
    isWorker = pathTokens[2] === "worker";
  }

  if (!queueKey) {
    console.log("No queue key, closing socket");
    socket.close();
    return;
  }

  if (isClient) {
    wsHandlerClient(queueKey, socket, request);
  } else if (isWorker) {
    wsHandlerWorker(queueKey, socket, request);
  } else {
    console.log("Unknown type, closing socket");
    socket.close();
    return;
  }
};
