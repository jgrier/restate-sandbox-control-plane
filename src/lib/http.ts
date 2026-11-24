// Small HTTP helpers shared by the dummy services, the agent and the proxy, so none of them
// needs a web framework. Nothing in here is specific to Restate.

import http from "node:http";
import { timeouts } from "../config.js";

// ------------------------------------------------------------------ client

export class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly body: unknown,
    what: string,
  ) {
    super(`${what} -> ${status} ${typeof body === "string" ? body : JSON.stringify(body)}`);
  }
}

/** The other side isn't there: connection refused, reset, or no answer in time. */
export class Unreachable extends Error {}

export async function request<T = any>(
  method: string,
  url: string,
  body?: unknown,
  timeoutMs = timeouts.http,
): Promise<T> {
  let res: Response;
  try {
    res = await fetch(url, {
      method,
      headers: body === undefined ? {} : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (e: any) {
    throw new Unreachable(`${method} ${url}: ${e?.cause?.code ?? e?.name ?? e}`);
  }
  const text = await res.text();
  const parsed = text ? safeJson(text) : null;
  if (!res.ok) throw new HttpError(res.status, parsed, `${method} ${url}`);
  return parsed as T;
}

function safeJson(text: string) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Deliver a webhook, retrying with backoff until accepted or `retryForMs` has passed. */
export async function deliverWebhook(url: string, body: unknown, retryForMs = timeouts.webhookRetry) {
  const deadline = Date.now() + retryForMs;
  for (let delay = 100; ; delay = Math.min(delay * 2, 2_000)) {
    try {
      return await request("POST", url, body);
    } catch (e) {
      if (Date.now() + delay > deadline) throw e;
      await sleep(delay);
    }
  }
}

// ------------------------------------------------------------------ server

export type Req = {
  params: Record<string, string>;
  query: URLSearchParams;
  body: any;
  raw: http.IncomingMessage;
  res: http.ServerResponse;
};
type Handler = (req: Req) => unknown | Promise<unknown>;

/** Return this from a handler to set a status code other than 200. */
export const withStatus = (status: number, body: unknown) => ({ [STATUS]: status, body });
const STATUS = Symbol("status");

export class Router {
  private routes: { method: string; pattern: RegExp; keys: string[]; handler: Handler }[] = [];

  on(method: string, path: string, handler: Handler) {
    const keys: string[] = [];
    const pattern = new RegExp(
      "^" +
        path
          .replace(/\*$/, "(?<rest>.*)")
          .replace(/:(\w+)/g, (_, k) => (keys.push(k), "([^/]+)")) +
        "$",
    );
    this.routes.push({ method, pattern, keys, handler });
    return this;
  }

  listen(port: number) {
    const server = http.createServer(async (raw, res) => {
      const url = new URL(raw.url ?? "/", "http://x");
      const route = this.routes.find((r) => r.method === raw.method && r.pattern.test(url.pathname));
      if (!route) return send(res, 404, { error: "not found" });
      const m = route.pattern.exec(url.pathname)!;
      const params: Record<string, string> = {};
      route.keys.forEach((k, i) => (params[k] = decodeURIComponent(m[i + 1])));
      if (m.groups?.rest !== undefined) params.rest = decodeURIComponent(m.groups.rest);
      try {
        let text = "";
        for await (const chunk of raw) text += chunk;
        const out: any = await route.handler({ params, query: url.searchParams, body: text ? JSON.parse(text) : {}, raw, res });
        if (res.headersSent) return; // the handler streamed its own response
        if (out && typeof out === "object" && STATUS in out) return send(res, out[STATUS], out.body);
        send(res, 200, out ?? { ok: true });
      } catch (e: any) {
        if (res.headersSent) return res.end();
        if (e instanceof HttpError) return send(res, e.status, e.body);
        console.error(`${raw.method} ${url.pathname}:`, e?.message ?? e);
        send(res, 500, { error: String(e?.message ?? e) });
      }
    });
    return new Promise<http.Server>((resolve) => server.listen(port, () => resolve(server)));
  }
}

function send(res: http.ServerResponse, status: number, body: unknown) {
  if (typeof body === "string" && body.startsWith("<!doctype")) {
    res.writeHead(status, { "content-type": "text/html; charset=utf-8" });
    return res.end(body);
  }
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body ?? null));
}

// ------------------------------------------------------------------ server-sent events

export function openSse(res: http.ServerResponse) {
  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });
  res.write(": connected\n\n");
  const keepAlive = setInterval(() => res.write(": ping\n\n"), 15_000);
  res.on("close", () => clearInterval(keepAlive));
  return {
    send: (event: string, data: unknown) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`),
    closed: () => res.destroyed || res.writableEnded,
  };
}
