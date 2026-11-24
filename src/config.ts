// Ports, URLs and timeouts. Every wait in the system has a timeout, and they all live here.
// `npm run dev` shortens the idle and turn timeouts so the demo moves quickly.

const env = (name: string, fallback: number) => Number(process.env[name] ?? fallback);
const SECOND = 1_000;
const MINUTE = 60 * SECOND;

export const ports = {
  proxy: 3000,
  sandboxes: 3001,
  disks: 3002,
  services: 9080,
};

export const urls = {
  proxy: `http://localhost:${ports.proxy}`,
  sandboxes: `http://localhost:${ports.sandboxes}`,
  disks: `http://localhost:${ports.disks}`,
  restateIngress: "http://localhost:8080",
  restateAdmin: "http://localhost:9070",
};

export const timeouts = {
  /** How long a single turn may run before we consider the agent stuck. */
  maxTurn: env("MAX_TURN_MS", 10 * MINUTE),
  /** A sandbox is destroyed this long after its last activity. */
  idle: env("IDLE_TIMEOUT_MS", 10 * MINUTE),
  /** The sandbox service gives up on a VM whose agent isn't healthy by then. */
  boot: env("BOOT_TIMEOUT_MS", 30 * SECOND),
  /** Calls between virtual objects. */
  call: env("CALL_TIMEOUT_MS", 30 * SECOND),
  /** Each HTTP request to a dummy service or an agent. */
  http: env("HTTP_TIMEOUT_MS", 10 * SECOND),
  /** How long a ctx.run step keeps retrying before it fails for good. */
  stepRetry: env("STEP_RETRY_FOR_MS", 60 * SECOND),
  /** The agent's graceful shutdown hook. */
  shutdown: env("SHUTDOWN_TIMEOUT_MS", 10 * SECOND),
  /** How long services and agents keep retrying a lifecycle event. */
  webhookRetry: env("WEBHOOK_RETRY_FOR_MS", 10 * MINUTE),
  /** The proxy's calls to Restate. */
  proxy: env("PROXY_TIMEOUT_MS", 30 * SECOND),
};

/** A call that may boot a VM waits for the boot (with retries) plus a margin. */
export const startCallTimeout = timeouts.stepRetry + timeouts.boot + 10 * SECOND;

/** How many times a turn is re-driven on a fresh VM after its VM died. */
export const maxRecoveries = env("MAX_RECOVERIES", 2);

/** Echo-agent pacing, per streamed word. */
export const turnDelayMs = env("TURN_DELAY_MS", 60);

export const dataDir = new URL("../data/", import.meta.url).pathname;
