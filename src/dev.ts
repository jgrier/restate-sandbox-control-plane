// `npm run dev`: starts restate-server, the two dummy services, the Restate services and the
// proxy; registers the services with Restate; and stops everything on Ctrl-C.
// Process output goes to data/logs/<name>.log.

import fs from "node:fs";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { dataDir, ports, urls } from "./config.js";
import { request, sleep } from "./lib/http.js";

// Demo-friendly timers, unless set explicitly.
process.env.IDLE_TIMEOUT_MS ??= "30000";
process.env.MAX_TURN_MS ??= "120000";

const logDir = path.join(dataDir, "logs");
const children: ChildProcess[] = [];
let stopping = false;

stopEverythingOnCtrlC();
startAllProcesses();
await waitUntilEverythingIsUp();
await registerServicesWithRestate();
printWelcome();

// ------------------------------------------------------------------ steps

function startAllProcesses() {
  fs.mkdirSync(logDir, { recursive: true });
  start("restate", "restate-server", ["--base-dir", path.join(dataDir, "restate"), "--no-logo"]);
  start("disks", ...typescript("src/services/disks/main.ts"));
  start("sandboxes", ...typescript("src/services/sandboxes/main.ts"));
  start("services", ...typescript("src/restate/index.ts"));
  start("proxy", ...typescript("src/proxy/main.ts"));
}

async function waitUntilEverythingIsUp() {
  await waitFor(`${urls.restateAdmin}/health`, "restate-server");
  await waitFor(`${urls.disks}/disks`, "disk service");
  await waitFor(`${urls.sandboxes}/sandboxes`, "sandbox service");
  await waitFor(`${urls.proxy}/`, "proxy");
}

/** Tell Restate where the services endpoint is, retrying until that endpoint is listening. */
async function registerServicesWithRestate() {
  for (let attempt = 1; ; attempt++) {
    try {
      await request("POST", `${urls.restateAdmin}/deployments`, { uri: `http://localhost:${ports.services}`, force: true });
      return;
    } catch (e) {
      if (attempt === 50) throw e;
      await sleep(200);
    }
  }
}

function printWelcome() {
  console.log(`
  Demo page        ${urls.proxy}
  Restate UI       ${urls.restateAdmin}
  Idle timeout     ${Number(process.env.IDLE_TIMEOUT_MS) / 1000}s
  Logs             data/logs/

  Ctrl-C to stop. \`npm run reset\` wipes all state.
`);
}

function stopEverythingOnCtrlC() {
  const stopAll = () => {
    stopping = true;
    for (const child of children) child.kill("SIGTERM");
    setTimeout(() => process.exit(0), 500);
  };
  process.on("SIGINT", stopAll);
  process.on("SIGTERM", stopAll);
}

// ------------------------------------------------------------------ helpers

/** Start a process with its output going to data/logs/<name>.log. */
function start(name: string, command: string, args: string[]) {
  const out = fs.openSync(path.join(logDir, `${name}.log`), "a");
  const child = spawn(command, args, { stdio: ["ignore", out, out], env: process.env });
  child.on("exit", (code) => {
    if (!stopping) console.error(`✗ ${name} exited (${code}); see data/logs/${name}.log`);
  });
  children.push(child);
}

/** Run a TypeScript file with node + tsx. */
function typescript(file: string): [string, string[]] {
  return [process.execPath, ["--import", "tsx", file]];
}

/** Poll a URL until something answers (any HTTP status), or give up after ~20 s. */
async function waitFor(url: string, name: string) {
  for (let i = 0; i < 200; i++) {
    try {
      await fetch(url, { signal: AbortSignal.timeout(500) });
      return;
    } catch {
      await sleep(100);
    }
  }
  throw new Error(`${name} didn't come up; see data/logs/`);
}
