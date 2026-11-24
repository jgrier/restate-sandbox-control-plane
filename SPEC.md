# Agent sessions on Restate: SPEC

Status: draft for review. Nothing gets built until this is agreed.

## 1. Goal

Model the control plane of an agent harness in Restate as clearly as possible. Each agent
session runs in a disposable sandbox VM, with its state on a persistent network disk. The model
uses digital twins of the real VMs and disks, a session object that coordinates them, and a
prompt path where Restate's own per-key invocation queue is the prompt queue. It's demonstrated
end to end with dummy sandbox and disk services (shaped like CubeSandbox and Archil), a fake
agent, and a simple web page.

This will be a public repository. The aim is to teach the Restate patterns, so the Restate code
has to read as the clean control plane (§2).

**In scope**
- Sessions: create, resume (get or create a live sandbox), stop, delete.
- Every prompt goes through Restate: delivered once, in order, one turn at a time per session.
- Sandboxes are destroyed after 10 minutes of inactivity. The next prompt starts a new sandbox
  on the same disk, and the agent picks up the transcript.
- Recovery from a VM dying mid-turn, and from a stuck turn.
- A dummy sandbox service and a dummy disk service with their own persistent state, plus a fake
  agent-server.
- A web page that lists sessions, sandboxes and disks, lets you chat, and can break things on purpose.

**Out of scope for now:** prewarmed sandboxes, steers, agent-to-agent prompts, authentication
(sessions record an `ownerId` but nothing checks it), real CubeSandbox and Archil, a real LLM
(pi plus a local model is a later step), and suspended (as opposed to destroyed) sandboxes. The
scripted failure test suite comes after the demo works (§12).

## 2. Design principles

**The Restate code is the point.** The handlers are what a reader comes to see, so they should
read like the flow they implement. Anything that isn't illustrative of a Restate primitive moves
out of them.

- **Handlers orchestrate; they don't implement.** Each external effect is one line:
  `await ctx.run("create VM", () => sandboxes.create(...))`. HTTP, payload shapes, error
  classification and polling live in small typed client libraries (`clients/sandboxes.ts`,
  `clients/disks.ts`, `clients/agent.ts`).
- **Push complexity into the dummy services and the agent.** They handle idempotency on client
  tokens, blocking until a VM is ready, detecting process exit and sending webhooks, mount
  bookkeeping, and repairing the transcript to recover a turn. Real services would own these too.
- **Show the primitives plainly.** One feature per primitive, named where it's used:
  - Virtual objects as digital twins, serialized per key.
  - The per-key invocation queue as the prompt queue.
  - One-way sends with idempotency keys.
  - An awakeable to wait for "turn ended".
  - A durable timer for idle shutdown: a delayed self-send, cancelled and re-sent on activity.
  - `ctx.run` for every side effect.
  - Shared handlers for status reads that are never blocked.
- **Every wait has a timeout.** That covers awakeables, calls between objects, `ctx.run` steps,
  HTTP calls, the VM boot, the shutdown hook, and webhook delivery. Nothing can hang forever.
  The values are in §12.
- **Only the proxy talks to Restate.** The browser, the agents and the dummy services never call
  Restate's ingress. They talk to the proxy, which plays the product API's role.
- **No test hooks in handler code.** Faults are injected from outside, by killing processes and
  through switches in the dummy services.
- **Small, typed state.** Each object keeps a handful of fields in one state type.
- **No product or customer names** anywhere in the code, comments or docs.

## 3. Processes

```
 Browser ── page, prompts, SSE ──▶ proxy :3000 ─────▶ Restate :8080 ──▶ services :9080
                                     │  ▲  ▲                            Conversation / Session
                                     │  │  │ lifecycle events           Sandbox / Disk
                                     │  │  └───────── sandboxes :3001 ◀──┐ (VM lifecycle)
                                     │  └─── agent :4xxx (child of :3001) │
                                     │ SSE              │ files           │
                                     └──────▶ agent     ▼                 │
                                                  data/disks/<id>/        │
                                              disks :3002 ◀───────────────┘ (disk lifecycle)
```

| Process | Port | Role |
|---|---|---|
| `restate-server` | 8080 / 9070 | Restate |
| `services` | 9080 | The four virtual objects (§4) |
| `proxy` | 3000 | The session proxy, playing the product API's role. The **only** client of Restate's ingress. Browser API, SSE routing, intake for lifecycle events, and serving the web page |
| `sandboxes` | 3001 | Dummy sandbox service (shaped like CubeSandbox): VM records in SQLite, one fake agent process per sandbox |
| `disks` | 3002 | Dummy disk service (shaped like Archil): disk records in SQLite, each disk a directory `data/disks/<diskId>/`, mount bookkeeping |
| `agent` | 4000+ | Fake agent-server, one per sandbox, started by `sandboxes` |

`npm run dev` starts everything and registers the services with Restate. Everything is
TypeScript on Node. Persistent state uses `node:sqlite`.

## 4. Virtual objects

Dependencies run one way only:

```
Conversation ──▶ Session ──▶ Sandbox
                    └──────▶ Disk
```

- **`Sandbox`** and **`Disk`** are opaque resources: twins of one VM and one disk. They know
  nothing about prompts, turns or sessions, and call no other object.
- **`Session`** is the coordinator. It talks to the agent, tracks the turn in flight, and
  receives every lifecycle event.
- **`Conversation`** is the prompt queue. It runs one turn at a time and waits for each to finish.

Only `Conversation` ever waits on a turn, so `Session`, `Sandbox` and `Disk` handlers are all
short. A session is therefore never locked during a turn, and events, stop and delete always
get through.

### 4.1 `Conversation[sessionId]`: the prompt queue, using Restate's own

No state. One exclusive handler, so **Restate's per-key invocation queue is the prompt
queue**: prompts run one at a time, in the order Restate accepted them.

| Handler | Kind | Behavior |
|---|---|---|
| `deliver({messageId, text, attachmentRefs?, generation})` | exclusive | Runs one turn to completion (§6.2): create an awakeable, `Session.deliverPrompt(...)`, wait on the awakeable with `MAX_TURN_MS`. If the VM died, loop and recover (§7). Returns `completed`, `aborted`, `rejected` or `failed`. |

The proxy reaches it only with a one-way **send**, idempotency key
`prompt:<sessionId>:<messageId>`. The send returns as soon as Restate has accepted the prompt
durably.

### 4.2 `Session[sessionId]`: the coordinator

State: `status` (`active` | `deleted`), `ownerId`, `diskId`, `sandboxId` (current or last, may
be null), `generation` (incremented on every stop and delete), `currentTurn` (`{messageId,
turnToken, sandboxId}` or null). No lists or history: only what is true now.

| Handler | Kind | Behavior |
|---|---|---|
| `create({ownerId})` | exclusive | Idempotent: if the session exists, return it. Otherwise `Disk[disk-<sessionId>].create` and set `active`. The proxy then sends the first prompt like any other, so `Session` never calls `Conversation` and dependencies stay one-way. |
| `resume({generation?})` | exclusive | Get or create a live sandbox. Rejects if `deleted`, or if `generation` is out of date (the session was stopped after the prompt was sent). If `Sandbox[sandboxId].getStatus` is `running`, return `{sandboxId, agentUrl}`. Otherwise: new sandbox id → `Disk.assign` (mount token, `forceMount`) → `Sandbox[new].start(...)` → store and return the address. The proxy calls this to find the agent for SSE. |
| `deliverPrompt({messageId, text, attachmentRefs, turnToken, generation})` | exclusive | The `resume` logic, then POST the prompt to the agent. Agent replies: **admitted** → `currentTurn = {messageId, turnToken, sandboxId}`, `Sandbox.touch()`, return `admitted`. **already-completed** → return it. **Unreachable** (dead or hung) → `Sandbox.stop("unreachable")`, so the VM is really gone, then return `gone`. **Rejected** (stopped or deleted) → return `rejected`. Never waits for the turn. |
| `onEvent({eventId, type, sandboxId, messageId?, reason?})` | exclusive | Every lifecycle event (§8). **`turn.ended`** matching `currentTurn` (same message *and* sandbox): complete `turnToken` with `completed`, clear `currentTurn`, `Sandbox.touch()`. **`sandbox.stopped`**: `Sandbox.markDestroyed(reason)`. If `currentTurn` was on that sandbox, complete its token with `aborted` and the reason. Events about an older sandbox only update that sandbox's twin. |
| `stop()` | exclusive | `generation++`. If a turn is in flight, complete its token with `aborted "stopped"`. `Sandbox.stop()`, then `Disk.release({sandboxId})`. |
| `discardSandbox({reason})` | exclusive | Like `stop` but without bumping the generation: throw the current sandbox away (a stuck agent) and let queued prompts carry on with a fresh one. |
| `delete()` | exclusive | `stop()` as above, then `Disk.delete()`, then `deleted`. A deleted session rejects every call, and its id is never reused. |
| `getStatus()` | shared | `{status, ownerId, diskId, sandboxId, generation, currentTurn}`. Never blocked. |

### 4.3 `Sandbox[sandboxId]`: an opaque VM

State: `status` (`new` | `starting` | `running` | `destroyed`), `diskId`, `agentUrl`,
`destroyedReason`, `idleTimer` (invocation id of the pending delayed `idleTimeout` call),
`lastActivityAt` (for the page's countdown).

| Handler | Kind | Behavior |
|---|---|---|
| `start({diskId, mountToken, forceMount, labels})` | exclusive | `starting` → `ctx.run`: `POST sandboxes /sandboxes` (client token = sandbox id; the service replies once the agent is healthy, or fails after its boot timeout) → `running`, record `agentUrl`, arm the idle timer. |
| `touch()` | exclusive | Activity: cancel the pending idle timer, arm a new one. Called by `Session` when a prompt is admitted and when a turn ends. In practice the 10 minutes count from the end of the last turn. |
| `idleTimeout()` | exclusive | The delayed self-send fired: `stop()` with reason `idle`. |
| `stop({reason})` | exclusive | If `running`: `ctx.run` the agent's shutdown hook (flush, unmount; 10 s limit, best effort), then `ctx.run` `DELETE sandboxes /sandboxes/:id`, then mark destroyed. |
| `markDestroyed({reason})` | exclusive | `status = destroyed`, record the reason, cancel the idle timer. Idempotent. Destroyed is terminal: a new VM is always a new `Sandbox` key. |
| `expireIdleNow()` | exclusive | Demo only: run `idleTimeout` now. |
| `getStatus()` | shared | `{status, diskId, agentUrl, destroyedReason, lastActivityAt}`. |

**Arming the idle timer** is one durable primitive. Cancel the pending call
(`ctx.invocation(idleTimer).cancel()`), then make a delayed self-send
(`ctx.objectSendClient(Sandbox, key).idleTimeout(sendOpts({delay: IDLE_TIMEOUT}))`) and keep
its invocation id. There's no clock arithmetic and no generation counter.

An idle stop doesn't need to tell anyone. The sandbox service sends `sandbox.stopped` for
every destroy, so `Session` hears about it through its normal event path.

### 4.4 `Disk[diskId]`: an opaque disk

State: `status` (`new` | `ready` | `deleted`), `mountToken`, `assignedSandboxId` (the sandbox
Restate last authorized to mount it, or null).

| Handler | Kind | Behavior |
|---|---|---|
| `create({labels})` | exclusive | Idempotent. `ctx.run`: `POST disks /disks` (client token = disk id), store `mountToken`, `ready`. |
| `assign({sandboxId})` | exclusive | Returns `{mountToken, forceMount}`. `forceMount` is true if a previous assignee was never released (it died, or was stopped for being idle). `Session` only calls this once the previous sandbox is destroyed, so forcing is safe. Sets `assignedSandboxId`. |
| `release({sandboxId})` | exclusive | Clears `assignedSandboxId` if it matches. |
| `delete()` | exclusive | `ctx.run`: `DELETE disks /disks/:id?force`. `deleted`. |
| `getStatus()` | shared | `{status, assignedSandboxId}`. |

The agent itself takes and releases the real mount, the way the Archil client works inside a
real VM. `Disk` records what Restate authorized, and the disk service records what is actually
mounted. The page shows both.

## 5. The dummy world

### 5.1 Dummy sandbox service (`sandboxes`, `:3001`, shaped like CubeSandbox)

State lives in SQLite: `sandboxes(id, client_token, labels, disk_id, pid, port, status,
created_at, destroyed_at, exit_code)`.

| Endpoint | Behavior |
|---|---|
| `POST /sandboxes {clientToken, labels, disk:{diskId, mountToken, force}}` | Idempotent on `clientToken`. Spawns a fake agent on a free port, with its configuration in environment variables. Replies `{sandboxID, url, status: "running"}` once the agent's `/health` answers, or 504 after `BOOT_TIMEOUT`. |
| `GET /sandboxes`, `GET /sandboxes/:id` | Every record, including destroyed ones. |
| `DELETE /sandboxes/:id` | SIGTERM, then SIGKILL after 5 s. `destroyed`. Idempotent. |
| `POST /sandboxes/:id/kill` | Demo only: SIGKILL right away, simulating a crash. |
| *(webhook)* | Whenever an agent process exits, for any reason: `POST proxy /internal/events {eventId, type: "sandbox.stopped", sandboxId, labels, reason, exitCode}`. `reason` is `deleted` when it exited because of `DELETE`, and `crashed` otherwise. Retried with backoff for up to `WEBHOOK_RETRY_FOR`. |

`labels` carry `{sessionId}`, so events can be routed. Agent processes are started detached.
On restart, the service checks its records against live process ids.

### 5.2 Dummy disk service (`disks`, `:3002`, shaped like Archil)

A disk is a directory, `data/disks/<diskId>/`, and the agent reads and writes it directly with
ordinary file calls. The service manages disk records and mount bookkeeping in SQLite:
`disks(id, client_token, labels, mount_token, status, mounted_by, mounted_at, created_at)`
and `events(disk_id, at, kind, detail)` (mounted, unmounted, taken over).

| Endpoint | Behavior |
|---|---|
| `POST /disks {clientToken, labels}` | Idempotent. Creates the directory. Returns `{id, mountToken, path}`. |
| `GET /disks`, `GET /disks/:id` | Includes `mountedBy`, `mountedAt`, size, and recent events. |
| `DELETE /disks/:id[?force]` | 409 if mounted, unless `force`. Removes the directory. |
| `POST /disks/:id/mount {token, clientId, force}` | Exclusive: 409 if another client holds it. With `force`, takes over and logs it. Returns `{path}`. |
| `POST /disks/:id/unmount {clientId}` | Releases the mount if that client holds it. |
| `GET /disks/:id/files/*` | Reads a file, for the page's transcript viewer. |

Mount ownership is bookkeeping only. It isn't enforced on writes. That's acceptable because we
only force a takeover once the previous VM is confirmed destroyed.

### 5.3 Fake agent-server (one process per sandbox)

Configuration comes from environment variables: `SANDBOX_ID`, `SESSION_ID`, `DISK_ID`,
`MOUNT_TOKEN`, `FORCE_MOUNT`, `DISKS_URL`, `EVENTS_URL`, `PORT`, `TURN_DELAY_MS`.

**On boot:** mount the disk through the disk service (forcing if told to), which gives the
directory path. Read `transcript.jsonl`. Then answer `/health`.

| Endpoint | Behavior |
|---|---|
| `GET /health` | 200 once mounted and ready. |
| `POST /api/prompt {messageId, text, attachmentRefs}` | If `messageId`'s turn is already **complete** in the transcript: `{status: "already-completed"}`. If it's there but **incomplete**: recover it (§7), then `{status: "admitted", recovered: true}`. Otherwise append the user message, start the turn, and return `{status: "admitted"}`. If a different turn is running: 409, which Conversation's serialization should make impossible. |
| `GET /api/events` | SSE. Starts with a snapshot of the transcript, then live events: `turn.started`, `assistant.delta`, `tool.started`, `tool.finished`, `assistant.message`, `turn.ended`. |
| `POST /api/shutdown` | Stops accepting prompts, flushes, unmounts, replies. The sandbox service then kills the process. |

**A turn (echo agent):** stream a reply token by token, with one simulated tool call partway
through (`tool_call`, a pause, `tool_result`), so there's a mid-tool window to crash in. The
final reply mentions the turn number, the sandbox id, and how many earlier messages it found on
disk, so you can see the conversation carry across VMs. Each step is appended to the transcript
before it's streamed. At the end: append `turn_end`, then `POST proxy /internal/events
{eventId, type: "turn.ended", sandboxId, sessionId, messageId}`, retried with backoff for up to
`WEBHOOK_RETRY_FOR`.

**Transcript** (`transcript.jsonl` on the disk, append-only):
```
{type:"user", messageId, text, at}
{type:"tool_call", messageId, callId, name, args, at}
{type:"tool_result", messageId, callId, result | error, at}
{type:"assistant", messageId, text, at}
{type:"system", messageId?, note, at}
{type:"turn_end", messageId, status: "completed" | "failed" | "interrupted", at}
```

## 6. Flows

### 6.1 Create
```
browser POST /api/sessions {sessionId, firstPrompt}
  proxy → Session[sid].create                (idempotency key create:<sid>)
    → Disk[disk-sid].create                  → disks POST /disks
  proxy → send Conversation[sid].deliver(firstPrompt, generation 0)   (same as §6.2)
  ← 202 {sessionId, invocationId}            (the browser opens SSE, §6.6)
```

### 6.2 Prompt: one turn
```
browser POST /api/sessions/:sid/prompts {messageId, text}
  proxy: g = Session.getStatus().generation           (shared, never blocked)
  proxy: send Conversation[sid].deliver({..., generation: g}), key prompt:<sid>:<messageId>
  ← 202 {invocationId}                                (progress arrives on SSE)

Conversation.deliver          (runs once every earlier prompt for this session has finished)
  1. token = ctx.awakeable()
  2. r = Session.deliverPrompt({messageId, text, turnToken: token.id, generation: g})
       rejected          → return "rejected"
       gone              → back to 1 (resume will start a new sandbox)
       already-completed → return "completed"
  3. outcome = await token.promise, timeout MAX_TURN_MS
       completed            → return "completed"
       aborted "stopped"    → return "aborted"       (user stop: no recovery)
       aborted "vm-died"    → back to 1              (recovery, §7)
       timeout              → Session.discardSandbox(), return "failed"   (stuck turn)

Session.onEvent(turn.ended)  → completes the token → step 3 wakes up
```
While the turn runs, `Conversation[sid]` is busy, so later prompts wait in Restate's queue.
`Session` is free the whole time. The browser's `POST` never waits for any of this. It can
fetch the final outcome later through the proxy (§10).

### 6.3 Idle stop
`Session` touches the sandbox when a prompt is admitted and when its turn ends. `IDLE_TIMEOUT`
after the last touch (10 min; about 30 s for demos), `Sandbox.idleTimeout` runs `stop("idle")`:
shutdown hook (flush, unmount), then destroy. The sandbox service sends `sandbox.stopped`,
`Session.onEvent` records it, and nothing else happens. The next prompt's `resume` finds the
sandbox destroyed, assigns the disk to a new one (`forceMount` is harmless, since the old one
unmounted), and starts it. The agent reads the transcript and takes the prompt.

### 6.4 Stop
`Session.stop`: `generation++`, complete any in-flight turn as `aborted "stopped"`, then
`Sandbox.stop()`, then `Disk.release`. Conversation does not recover a stopped turn. Queued
prompts sent before the stop carry the old generation, so they finish as `rejected`. A prompt
sent after the stop wakes the session again.

### 6.5 Delete
`Session.delete`: stop as above, then `Disk.delete` (the directory is removed), then `deleted`.
Every later call and every queued prompt is rejected.

### 6.6 SSE
The browser holds one stream per session: `GET /api/sessions/:sid/events` on the proxy. The
proxy reads `Session.getStatus` and `Sandbox.getStatus` (shared handlers, never blocked). If a
sandbox is running, it passes that agent's SSE through. Each new connection starts with the
agent's transcript snapshot. When the agent goes away, the proxy keeps the browser's stream
open and follows the session to its next sandbox. While no VM is running it sends the
transcript read straight from the disk, so **watching a session never wakes it up**, and an
idle stop sticks even with the chat open. Responses never pass through Restate.

### 6.7 A VM dies while idle
`kill -9` makes the sandbox service send `sandbox.stopped`. `Session.onEvent` marks the
`Sandbox` destroyed. The next prompt starts a new sandbox. If a prompt arrives before the
event, the agent is unreachable: `deliverPrompt` marks the sandbox destroyed and returns `gone`,
and Conversation loops. Either way the prompt is delivered once.

## 7. Recovering a turn when the VM dies

This follows common practice for agent harnesses whose agent loop lives in memory but whose
transcript is durable:

1. **Write everything to the transcript as it happens.** The user message, every tool call,
   every tool result and the final reply are each appended to the disk before they're
   streamed. Whatever was written before the crash survives it.
2. **Detect the interrupted turn.** The new agent receives the same `messageId` again. It
   finds that message in the transcript without a `turn_end` after it.
3. **Repair the transcript, don't rewrite it.** For each `tool_call` without a matching
   `tool_result`, append a synthetic one: `error: "interrupted: the sandbox stopped before this
   tool finished; its outcome is unknown"`. Then append a system note: "Your previous sandbox
   stopped unexpectedly during this turn. Continue from the transcript. Check the state of any
   side effects before repeating them."
4. **Continue the same turn.** Run the agent loop again from the repaired transcript. The model
   sees what had already finished and decides what to do next. The echo agent's reply
   acknowledges the interruption.
5. **Limit the attempts.** `Conversation` re-drives a turn at most `MAX_RECOVERIES` (2) times,
   then returns `failed`. A later prompt makes the agent close any turn still left open
   (`turn_end: interrupted`, plus a system note) before it starts.

**What Restate adds.** Steps 2–5 are driven by `Conversation.deliver`, which is itself durable.
It notices the abort, provisions a replacement VM, drives the recovery, and keeps every later
prompt waiting so turns never interleave, even if the proxy or the services process crashes in
between. Recovery isn't a best-effort retry loop living in some process's memory.

**A later step:** run the agent loop itself on Restate, journaling each model call and tool
call. Recovery would then resume exactly after the last completed step, instead of relying on
the model to work out what happened. That's the strongest argument for writing the harness on
Restate.

## 8. Lifecycle events

The sandbox service (`sandbox.stopped`) and the agents (`turn.ended`) send `POST proxy
/internal/events`. The proxy sends each event to `Session[sessionId].onEvent` with idempotency
key `event:<eventId>`, so Restate drops duplicates. Past that, a replay is still harmless: an
event only acts if it matches current state (the turn in flight, a sandbox not yet destroyed).
`Session` then updates the `Sandbox` twin and the turn in flight. `Sandbox` itself never
receives events.

## 9. Idempotency rules

| Effect | Why repeating it is safe |
|---|---|
| Create a VM | sandbox service client token = the `Sandbox` key |
| Create a disk | disk service client token = the `Disk` key |
| Deliver a prompt | Restate idempotency key `prompt:<sid>:<messageId>`; the agent also deduplicates on `messageId` using the transcript |
| Lifecycle events | Restate idempotency key `event:<eventId>`; `onEvent` only acts on events matching current state |
| Destroy, delete, unmount, release, markDestroyed, cancel a timer | naturally idempotent |
| Create a session | Restate idempotency key `create:<sid>`; `Session.create` returns an existing session |

## 10. Proxy API

The proxy is the only thing that calls Restate's ingress.

**Browser**
| Endpoint | Calls |
|---|---|
| `POST /api/sessions {sessionId, firstPrompt:{messageId, text}}` | `Session.create` |
| `POST /api/sessions/:sid/prompts {messageId, text}` | `Session.getStatus`, then send `Conversation.deliver`. Returns 202 with the invocation id |
| `GET /api/prompts/:invocationId` | The prompt's outcome, read from Restate (attach to the invocation, with a timeout) |
| `GET /api/sessions/:sid/events` | SSE passthrough (§6.6) |
| `GET /api/sessions/:sid` | `Session.getStatus` |
| `POST /api/sessions/:sid/stop` | `Session.stop` |
| `DELETE /api/sessions/:sid` | `Session.delete` |

**Demo**
| Endpoint | Does |
|---|---|
| `GET /api/world` | One snapshot, plus each session's queued prompt count (Conversation invocations waiting in Restate's inbox, read from the admin SQL API: a demo-only peek). **Disks** from the disk service joined with `Disk.getStatus`. **Sandboxes** from the sandbox service joined with `Sandbox.getStatus`. **Sessions** from each disk's `sessionId` label joined with `Session.getStatus`. Each row shows the twin and the real-world record side by side, with disagreements flagged |
| `POST /api/sandboxes/:id/kill` | sandbox service kill |
| `POST /api/sandboxes/:id/expire-idle` | `Sandbox.expireIdleNow` |
| `GET /api/disks/:id/transcript` | disk service file read |

**Internal:** `POST /internal/events` (§8).

## 11. Web page

A single static page served by the proxy, plain HTML and JavaScript, refreshing about once a
second.

- **Sessions:** id, status, current sandbox, generation, the turn in flight. Actions: new
  session (with a first prompt), open chat, stop, delete.
- **Chat panel:** send prompts, see the streamed responses and each prompt's outcome, a marker
  when the sandbox changes, and recovery notes.
- **Sandboxes:** twin status next to the sandbox service's status, session, idle countdown,
  reason destroyed. Actions: kill -9, expire idle now.
- **Disks:** twin assignment next to the disk service's actual mount holder, recent events
  (mounts, takeovers). Action: view transcript.

## 12. Configuration and milestones

**Timeouts.** Every wait has one.

| Wait | Setting | Default | Demo |
|---|---|---|---|
| A turn, in `Conversation` (awakeable) | `MAX_TURN_MS` | 10 min | 2 min |
| Idle shutdown after the last touch | `IDLE_TIMEOUT` | 10 min | 30 s |
| VM boot, until the agent is healthy (sandbox service) | `BOOT_TIMEOUT` | 30 s | 30 s |
| Calls between objects (`.orTimeout`) | `CALL_TIMEOUT` | 30 s, or boot plus margin for calls that start a VM | same |
| Each HTTP call inside `ctx.run` | `HTTP_TIMEOUT` | 10 s | 10 s |
| Retries of a `ctx.run` step before it fails terminally | `STEP_RETRY_FOR` | 60 s | 60 s |
| Agent shutdown hook | `SHUTDOWN_TIMEOUT` | 10 s | 10 s |
| Webhook delivery retries (services, agents) | `WEBHOOK_RETRY_FOR` | 10 min | 10 min |
| Proxy calls to Restate, and attaching to an outcome | `PROXY_TIMEOUT` | 30 s | 30 s |

Other settings: `MAX_RECOVERIES` = 2, and `TURN_DELAY_MS` = 50 ms per token (echo agent pacing).

**Milestones**
1. The dummy disk service, the dummy sandbox service and the fake agent, each usable on its
   own with curl.
2. The virtual objects and the proxy happy path: create, prompt, SSE, idle stop, resume, stop,
   delete.
3. The web page.
4. Turn recovery after a VM dies (§7), and the stuck-turn timeout.
5. Scripted failure tests: crash at each step, races, event replays, duplicate and queued prompts.
6. Later: the pi agent loop with a local model; real Archil (free tier); real CubeSandbox (x86
   Linux VM).

## 13. Open questions

None at the moment.
