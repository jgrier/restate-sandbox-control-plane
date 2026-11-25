// The session proxy: the product API in front of everything. It's the only process that
// talks to Restate's ingress. Browsers, agents and the dummy services all talk to it.
//
//  - browser API: create sessions, send prompts (one-way sends to Conversation), stop, delete
//  - SSE: streams the current agent's events to the browser, following it across sandboxes
//  - lifecycle events from agents and the sandbox service → Session.onEvent
//  - the demo page, its world view, and the control-plane log

import fs from "node:fs";
import type http from "node:http";
import * as clients from "@restatedev/restate-sdk-clients";
import { ports, timeouts, urls } from "../config.js";
import { Router, openSse, request, sleep, withStatus } from "../lib/http.js";
import { logger, type LogLine } from "../lib/log.js";
import { Conversation } from "../restate/conversation.js";
import { Disk } from "../restate/disk.js";
import { Sandbox } from "../restate/sandbox.js";
import { Session, type LifecycleEvent } from "../restate/session.js";

// Restate: the ingress client. The proxy calls handlers through restate-server's ingress (:8080),
// never through the admin API.
const restate = clients.connect({ url: urls.restateIngress });
const log = logger("proxy");
const page = new URL("./public/index.html", import.meta.url).pathname;
const docsDir = new URL("../../docs/", import.meta.url).pathname;

type PromptText = { messageId: string; text: string };
type Agent = { sandboxId: string; agentUrl: string };
type SseStream = ReturnType<typeof openSse>;

const recentLog: LogLine[] = []; // the control-plane log, for pages that connect later
const logStreams = new Set<SseStream>();

// ------------------------------------------------------------------ the API

new Router()
  .on("GET", "/", () => fs.readFileSync(page, "utf8"))
  .on("GET", "/docs/:file", ({ params, res }) => serveDiagram(params.file, res))

  // browser API
  .on("POST", "/api/sessions", ({ body }) => createSession(body.sessionId ?? newId("s"), body.ownerId ?? "demo-user", body.firstPrompt && promptFrom(body.firstPrompt)))
  .on("POST", "/api/sessions/:sid/prompts", ({ params, body }) => sendPrompt(params.sid, promptFrom(body)))
  .on("GET", "/api/prompts/:invocationId", ({ params }) => promptOutcome(params.invocationId))
  .on("GET", "/api/sessions/:sid", ({ params }) => sessionStatus(params.sid))
  .on("GET", "/api/sessions/:sid/events", ({ params, res, raw }) => streamSession(params.sid, res, raw))
  .on("POST", "/api/sessions/:sid/stop", ({ params }) => stopSession(params.sid))
  .on("DELETE", "/api/sessions/:sid", ({ params }) => deleteSession(params.sid))

  // demo
  .on("GET", "/api/world", () => world())
  .on("POST", "/api/sandboxes/:id/kill", ({ params }) => killSandbox(params.id))
  .on("POST", "/api/sandboxes/:id/expire-idle", ({ params }) => expireIdle(params.id))
  .on("GET", "/api/disks/:id/transcript", ({ params }) => readTranscriptFile(params.id))
  .on("GET", "/api/log", ({ res }) => streamControlPlaneLog(res))

  // internal: lifecycle events and log lines
  .on("POST", "/internal/events", ({ body }) => forwardLifecycleEvent(body))
  .on("POST", "/internal/log", ({ body }) => addLogLine(body as LogLine))
  .listen(ports.proxy)
  .then(() => console.log(`proxy on ${urls.proxy}`));

// ------------------------------------------------------------------ sessions and prompts

async function createSession(sessionId: string, ownerId: string, firstPrompt?: PromptText) {
  // Restate: request-response call through ingress, deduplicated by its idempotency key.
  await session(sessionId).create({ ownerId }, opts({ idempotencyKey: `create:${sessionId}` }));
  log.info("session created", sessionId);
  return firstPrompt ? sendPrompt(sessionId, firstPrompt) : withStatus(201, { sessionId });
}

/** Queue the prompt in Restate. Returns right away (202); the outcome is fetched separately. */
async function sendPrompt(sessionId: string, prompt: PromptText) {
  const status = await sessionStatus(sessionId);
  if (status.status !== "active") return withStatus(409, { error: `session is ${status.status}` });

  const invocationId = await queuePrompt(sessionId, prompt, status.generation);
  log.info(`prompt ${prompt.messageId} queued`, sessionId);
  return withStatus(202, { sessionId, messageId: prompt.messageId, invocationId });
}

/**
 * The prompt carries the session's current generation, so a later stop rejects it.
 * Restate: one-way send through ingress. It returns once Restate has the invocation durably;
 * the idempotency key (from the client's message id) makes browser retries harmless.
 */
async function queuePrompt(sessionId: string, prompt: PromptText, generation: number) {
  const handle = await restate
    .objectSendClient(Conversation, sessionId)
    .deliver({ ...prompt, generation }, clients.rpc.sendOpts({ idempotencyKey: `prompt:${sessionId}:${prompt.messageId}` }));
  return handle.invocationId;
}

/** The prompt's final outcome, once Conversation.deliver has finished. */
async function promptOutcome(invocationId: string) {
  // Restate: read an invocation's result by its id, through ingress.
  const res = await fetch(`${urls.restateIngress}/restate/invocation/${invocationId}/output`, {
    signal: AbortSignal.timeout(timeouts.proxy),
  });
  return res.ok ? { done: true, outcome: await res.json() } : { done: false };
}

function sessionStatus(sessionId: string) {
  return session(sessionId).getStatus(opts());
}

async function stopSession(sessionId: string) {
  await session(sessionId).stop(opts());
  return { ok: true };
}

async function deleteSession(sessionId: string) {
  await session(sessionId).delete(opts());
  return { ok: true };
}

function promptFrom(body: any): PromptText {
  return { messageId: body.messageId ?? newId("m"), text: String(body.text ?? "") };
}

// ------------------------------------------------------------------ SSE: follow the session's agent

/**
 * Stream the session to the browser. While a VM is running, relay its agent's events; when
 * there's none, show the transcript from the disk (without waking anything up) and wait.
 */
async function streamSession(sessionId: string, res: http.ServerResponse, req: http.IncomingMessage) {
  const stream = openSse(res);
  const browserGone = new AbortController();
  req.on("close", () => browserGone.abort());
  let showingDormant = false;

  while (!stream.closed()) {
    const agent = await currentAgent(sessionId);
    if (agent) {
      showingDormant = false;
      stream.send("sandbox", { sandboxId: agent.sandboxId });
      await relayAgentEvents(agent.agentUrl, res, browserGone.signal); // returns when the agent goes away
      continue;
    }
    if (!showingDormant) showingDormant = await showTranscriptFromDisk(sessionId, stream);
    await sleep(1_000);
  }
}

/** The session's running agent, if it has one. */
async function currentAgent(sessionId: string): Promise<Agent | null> {
  const status = await orNull(sessionStatus(sessionId));
  if (!status?.sandboxId) return null;
  const vm = await orNull(restate.objectClient(Sandbox, status.sandboxId).getStatus(opts()));
  return vm?.status === "running" && vm.agentUrl ? { sandboxId: status.sandboxId, agentUrl: vm.agentUrl } : null;
}

/** No live VM: send the conversation as it is on the disk. Returns whether it could. */
async function showTranscriptFromDisk(sessionId: string, stream: SseStream): Promise<boolean> {
  const status = await orNull(sessionStatus(sessionId));
  if (!status?.diskId) return false;
  stream.send("snapshot", { sandboxId: null, transcript: await readTranscript(status.diskId) });
  stream.send("dormant", { status: status.status, sandboxId: status.sandboxId });
  return true;
}

/** Pass the agent's SSE through, byte for byte, until the agent or the browser goes away. */
async function relayAgentEvents(agentUrl: string, res: http.ServerResponse, browserGone: AbortSignal) {
  // Time out the connect, but not the long-lived stream that follows.
  const connect = new AbortController();
  const connectTimer = setTimeout(() => connect.abort(), timeouts.http);
  try {
    const upstream = await fetch(`${agentUrl}/api/events`, { signal: AbortSignal.any([browserGone, connect.signal]) });
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

/** Every disk, sandbox and session, each with its Restate twin's status. */
async function world() {
  const [diskList, vmList] = await Promise.all([listDisks(), listSandboxes()]);
  const [disks, sandboxes, sessions] = await Promise.all([
    disksWithTwins(diskList),
    sandboxesWithTwins(vmList),
    sessionsFromDisks(diskList),
  ]);
  return { sessions, sandboxes, disks, idleTimeoutMs: timeouts.idle, now: Date.now() };
}

function disksWithTwins(diskList: any[]) {
  return Promise.all(diskList.map(async (d) => ({ ...d, twin: await orNull(restate.objectClient(Disk, d.id).getStatus(opts())) })));
}

function sandboxesWithTwins(vmList: any[]) {
  return Promise.all(
    vmList.map(async (v) => ({ ...v, twin: await orNull(restate.objectClient(Sandbox, v.sandboxID).getStatus(opts())) })),
  );
}

/** Sessions are found through their disks: every disk is labelled with its session. */
function sessionsFromDisks(diskList: any[]) {
  const sessionIds = [...new Set(diskList.map((d) => d.labels.sessionId).filter(Boolean))] as string[];
  return Promise.all(sessionIds.map(async (id) => ({ id, ...(await orNull(sessionStatus(id))) })));
}

// ------------------------------------------------------------------ demo actions

function killSandbox(sandboxId: string) {
  return request("POST", `${urls.sandboxes}/sandboxes/${sandboxId}/kill`);
}

async function expireIdle(sandboxId: string) {
  await restate.objectSendClient(Sandbox, sandboxId).expireIdleNow();
  return { ok: true };
}

function readTranscriptFile(diskId: string) {
  return request("GET", `${urls.disks}/disks/${diskId}/files/transcript.jsonl`);
}

function serveDiagram(file: string, res: http.ServerResponse) {
  if (!/^[\w-]+\.png$/.test(file)) return withStatus(404, { error: "not found" });
  res.writeHead(200, { "content-type": "image/png" });
  res.end(fs.readFileSync(docsDir + file));
}

// ------------------------------------------------------------------ lifecycle events

/**
 * Agents (turn.ended) and the sandbox service (sandbox.stopped) report here.
 * Restate: one-way send keyed by the event id, so a webhook delivered twice runs once.
 */
async function forwardLifecycleEvent(event: LifecycleEvent & { sessionId?: string }) {
  if (!event.sessionId) return withStatus(400, { error: "event without sessionId" });
  await restate
    .objectSendClient(Session, event.sessionId)
    .onEvent(event, clients.rpc.sendOpts({ idempotencyKey: `event:${event.eventId}` }));
  return withStatus(202, { accepted: true });
}

// ------------------------------------------------------------------ control-plane log

function addLogLine(line: LogLine) {
  recentLog.push(line);
  if (recentLog.length > 500) recentLog.shift();
  for (const s of logStreams) s.closed() ? logStreams.delete(s) : s.send("log", line);
  return { ok: true };
}

/** The recent lines first, then new ones as they arrive. */
function streamControlPlaneLog(res: http.ServerResponse) {
  const stream = openSse(res);
  for (const line of recentLog) stream.send("log", line);
  logStreams.add(stream);
}

// ------------------------------------------------------------------ helpers

function session(sessionId: string) {
  return restate.objectClient(Session, sessionId);
}

/** Every call to Restate has a timeout. */
function opts(extra: { idempotencyKey?: string } = {}): any {
  return clients.rpc.opts({ timeout: timeouts.proxy, ...extra });
}

function orNull<T>(promise: PromiseLike<T>): Promise<T | null> {
  return Promise.resolve(promise).catch(() => null);
}

async function readTranscript(diskId: string) {
  const file = await readTranscriptFile(diskId).catch(() => null);
  return (file?.content ?? "").split("\n").filter(Boolean).map((line: string) => JSON.parse(line));
}

function listDisks(): Promise<any[]> {
  return request("GET", `${urls.disks}/disks`);
}

function listSandboxes(): Promise<any[]> {
  return request("GET", `${urls.sandboxes}/sandboxes`);
}

function newId(prefix: string) {
  return `${prefix}-${Math.random().toString(36).slice(2, 8)}`;
}
