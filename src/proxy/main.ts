// The session proxy: the product API in front of everything. It's the only process that
// talks to Restate's ingress. Browsers, agents and the dummy services all talk to it.
//
//  - browser API: create sessions, send prompts (one-way sends to Conversation), stop, delete
//  - SSE: streams the current agent's events to the browser, following it across sandboxes
//  - lifecycle events from agents and the sandbox service → Session.onEvent
//  - the demo page, its world view, and the control-plane log

import fs from "node:fs";
import * as clients from "@restatedev/restate-sdk-clients";
import { ports, timeouts, urls } from "../config.js";
import { Router, openSse, request, sleep, withStatus } from "../lib/http.js";
import { logger, type LogLine } from "../lib/log.js";
import { Conversation } from "../restate/conversation.js";
import { Disk } from "../restate/disk.js";
import { Sandbox } from "../restate/sandbox.js";
import { Session, type LifecycleEvent } from "../restate/session.js";

const restate = clients.connect({ url: urls.restateIngress });
// Every call to Restate has a timeout.
const opts = (extra: { idempotencyKey?: string } = {}): any => clients.rpc.opts({ timeout: timeouts.proxy, ...extra });
const orNull = <T>(p: PromiseLike<T>) => Promise.resolve(p).catch(() => null);
const log = logger("proxy");
const page = new URL("./public/index.html", import.meta.url).pathname;
const docsDir = new URL("../../docs/", import.meta.url).pathname;

// ------------------------------------------------------------------ control-plane log

const recentLog: LogLine[] = [];
const logStreams = new Set<ReturnType<typeof openSse>>();
function addLog(line: LogLine) {
  recentLog.push(line);
  if (recentLog.length > 500) recentLog.shift();
  for (const s of logStreams) s.closed() ? logStreams.delete(s) : s.send("log", line);
}

// ------------------------------------------------------------------ prompts

async function sendPrompt(sessionId: string, prompt: { messageId: string; text: string }) {
  // Read the generation (a shared handler, never blocked) so a later stop rejects this prompt.
  const status = await restate.objectClient(Session, sessionId).getStatus(opts());
  if (status.status !== "active") return withStatus(409, { error: `session is ${status.status}` });

  const handle = await restate
    .objectSendClient(Conversation, sessionId)
    .deliver({ ...prompt, generation: status.generation }, clients.rpc.sendOpts({ idempotencyKey: `prompt:${sessionId}:${prompt.messageId}` }));
  log.info(`prompt ${prompt.messageId} queued`, sessionId);
  return withStatus(202, { sessionId, messageId: prompt.messageId, invocationId: handle.invocationId });
}

// ------------------------------------------------------------------ SSE: follow the session's agent

async function streamSession(sessionId: string, res: any, raw: any) {
  const stream = openSse(res);
  const abort = new AbortController();
  raw.on("close", () => abort.abort());
  let shownDormant = false;

  while (!stream.closed()) {
    const status = await orNull(restate.objectClient(Session, sessionId).getStatus(opts()));
    const vm = status?.sandboxId ? await orNull(restate.objectClient(Sandbox, status.sandboxId).getStatus(opts())) : null;

    if (vm?.status === "running" && vm.agentUrl) {
      shownDormant = false;
      stream.send("sandbox", { sandboxId: status!.sandboxId });
      await pipeAgentEvents(vm.agentUrl, res, abort.signal); // returns when the agent goes away
      continue;
    }
    if (!shownDormant && status?.diskId) {
      // No live VM: show the conversation from the disk without waking anything up.
      const file = await request("GET", `${urls.disks}/disks/${status.diskId}/files/transcript.jsonl`).catch(() => null);
      const transcript = (file?.content ?? "").split("\n").filter(Boolean).map((l: string) => JSON.parse(l));
      stream.send("snapshot", { sandboxId: null, transcript });
      stream.send("dormant", { status: status.status, sandboxId: status.sandboxId, vm: vm?.status ?? null });
      shownDormant = true;
    }
    await sleep(1_000);
  }
}

async function pipeAgentEvents(agentUrl: string, res: any, signal: AbortSignal) {
  // Time out the connect, but not the long-lived stream that follows.
  const connect = new AbortController();
  const connectTimer = setTimeout(() => connect.abort(), timeouts.http);
  try {
    const upstream = await fetch(`${agentUrl}/api/events`, { signal: AbortSignal.any([signal, connect.signal]) });
    clearTimeout(connectTimer);
    if (!upstream.body) return;
    const reader = upstream.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done || res.destroyed) return;
      res.write(value);
    }
  } catch {
    // The agent went away (or the browser did). The caller decides what's next.
  } finally {
    clearTimeout(connectTimer);
  }
}

// ------------------------------------------------------------------ world view (demo)

async function world() {
  const [diskList, vmList] = await Promise.all([
    request("GET", `${urls.disks}/disks`),
    request("GET", `${urls.sandboxes}/sandboxes`),
  ]);
  const twin = orNull;

  const disks = await Promise.all(
    diskList.map(async (d: any) => ({ ...d, twin: await twin(restate.objectClient(Disk, d.id).getStatus(opts())) })),
  );
  const sandboxes = await Promise.all(
    vmList.map(async (v: any) => ({ ...v, twin: await twin(restate.objectClient(Sandbox, v.sandboxID).getStatus(opts())) })),
  );
  const sessionIds = [...new Set(diskList.map((d: any) => d.labels.sessionId).filter(Boolean))] as string[];
  const sessions = await Promise.all(
    sessionIds.map(async (id) => ({
      id,
      ...(await twin(restate.objectClient(Session, id).getStatus(opts()))),
    })),
  );
  return { sessions, sandboxes, disks, idleTimeoutMs: timeouts.idle, now: Date.now() };
}

// ------------------------------------------------------------------ routes

const newId = (prefix: string) => `${prefix}-${Math.random().toString(36).slice(2, 8)}`;

new Router()
  .on("GET", "/", () => fs.readFileSync(page, "utf8"))
  .on("GET", "/docs/:file", ({ params, res }) => {
    if (!/^[\w-]+\.png$/.test(params.file)) return withStatus(404, { error: "not found" });
    res.writeHead(200, { "content-type": "image/png" });
    res.end(fs.readFileSync(docsDir + params.file));
  })

  // ---- browser API
  .on("POST", "/api/sessions", async ({ body }) => {
    const sessionId: string = body.sessionId ?? newId("s");
    await restate.objectClient(Session, sessionId).create({ ownerId: body.ownerId ?? "demo-user" }, opts({ idempotencyKey: `create:${sessionId}` }));
    log.info("session created", sessionId);
    if (!body.firstPrompt) return withStatus(201, { sessionId });
    return sendPrompt(sessionId, body.firstPrompt);
  })
  .on("POST", "/api/sessions/:sid/prompts", ({ params, body }) =>
    sendPrompt(params.sid, { messageId: body.messageId ?? newId("m"), text: String(body.text ?? "") }),
  )
  .on("GET", "/api/prompts/:invocationId", async ({ params }) => {
    // The prompt's final outcome, once Conversation.deliver has finished.
    const r = await fetch(`${urls.restateIngress}/restate/invocation/${params.invocationId}/output`, {
      signal: AbortSignal.timeout(timeouts.proxy),
    });
    if (r.ok) return { done: true, outcome: await r.json() };
    return { done: false };
  })
  .on("GET", "/api/sessions/:sid", ({ params }) => restate.objectClient(Session, params.sid).getStatus(opts()))
  .on("GET", "/api/sessions/:sid/events", ({ params, res, raw }) => streamSession(params.sid, res, raw))
  .on("POST", "/api/sessions/:sid/stop", async ({ params }) => {
    await restate.objectClient(Session, params.sid).stop(opts());
    return { ok: true };
  })
  .on("DELETE", "/api/sessions/:sid", async ({ params }) => {
    await restate.objectClient(Session, params.sid).delete(opts());
    return { ok: true };
  })

  // ---- demo
  .on("GET", "/api/world", () => world())
  .on("POST", "/api/sandboxes/:id/kill", ({ params }) => request("POST", `${urls.sandboxes}/sandboxes/${params.id}/kill`))
  .on("POST", "/api/sandboxes/:id/expire-idle", async ({ params }) => {
    await restate.objectSendClient(Sandbox, params.id).expireIdleNow();
    return { ok: true };
  })
  .on("GET", "/api/disks/:id/transcript", ({ params }) => request("GET", `${urls.disks}/disks/${params.id}/files/transcript.jsonl`))
  .on("GET", "/api/log", ({ res }) => {
    const stream = openSse(res);
    for (const line of recentLog) stream.send("log", line);
    logStreams.add(stream);
  })

  // ---- internal: lifecycle events and log lines
  .on("POST", "/internal/events", async ({ body }) => {
    const event = body as LifecycleEvent & { sessionId?: string };
    if (!event.sessionId) return withStatus(400, { error: "event without sessionId" });
    await restate
      .objectSendClient(Session, event.sessionId)
      .onEvent(event, clients.rpc.sendOpts({ idempotencyKey: `event:${event.eventId}` }));
    return withStatus(202, { accepted: true });
  })
  .on("POST", "/internal/log", ({ body }) => {
    addLog(body as LogLine);
    return { ok: true };
  })
  .listen(ports.proxy)
  .then(() => console.log(`proxy on ${urls.proxy}`));
