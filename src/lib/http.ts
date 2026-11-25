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

type Route = { method: string; pattern: RegExp; keys: string[]; handler: Handler };

export class Router {
  private routes: Route[] = [];

  /** Add a route. `:name` captures one path segment; a trailing `*` captures the rest as `rest`. */
  on(method: string, path: string, handler: Handler) {
    const keys: string[] = [];
    const pattern = new RegExp(
      "^" +
        path
          .replace(/\*$/, "(?<rest>.*)")
          .replace(/:(\w+)/g, (_, key) => (keys.push(key), "([^/]+)")) +
        "$",
    );
    this.routes.push({ method, pattern, keys, handler });
    return this;
  }

  listen(port: number) {
    const server = http.createServer((raw, res) => this.handle(raw, res));
    return new Promise<http.Server>((resolve) => server.listen(port, () => resolve(server)));
  }

  private async handle(raw: http.IncomingMessage, res: http.ServerResponse) {
    const url = new URL(raw.url ?? "/", "http://x");
    const route = this.routes.find((r) => r.method === raw.method && r.pattern.test(url.pathname));
    if (!route) return send(res, 404, { error: "not found" });

    try {
      const body = await readJsonBody(raw);
      const result = await route.handler({ params: pathParams(route, url.pathname), query: url.searchParams, body, raw, res });
      respond(res, result);
    } catch (e) {
      respondWithError(res, e, `${raw.method} ${url.pathname}`);
    }
  }
}

function pathParams(route: Route, pathname: string): Record<string, string> {
  const match = route.pattern.exec(pathname)!;
  const params: Record<string, string> = {};
  route.keys.forEach((key, i) => (params[key] = decodeURIComponent(match[i + 1])));
  if (match.groups?.rest !== undefined) params.rest = decodeURIComponent(match.groups.rest);
  return params;
}

async function readJsonBody(raw: http.IncomingMessage) {
  let text = "";
  for await (const chunk of raw) text += chunk;
  return text ? JSON.parse(text) : {};
}

/** A handler's return value becomes the response: 200 by default, or `withStatus(...)`. */
function respond(res: http.ServerResponse, result: any) {
  if (res.headersSent) return; // the handler streamed its own response
  if (result && typeof result === "object" && STATUS in result) return send(res, result[STATUS], result.body);
  send(res, 200, result ?? { ok: true });
}

/** A thrown HttpError keeps its status and body; anything else is a 500. */
function respondWithError(res: http.ServerResponse, e: any, what: string) {
  if (res.headersSent) return res.end();
  if (e instanceof HttpError) return send(res, e.status, e.body);
  console.error(`${what}:`, e?.message ?? e);
  send(res, 500, { error: String(e?.message ?? e) });
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
