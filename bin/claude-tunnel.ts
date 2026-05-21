#!/usr/bin/env bun
import { existsSync, readFileSync, statSync, mkdirSync, openSync } from "node:fs";
import { createConnection, type Socket } from "node:net";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import {
  TUNNEL_DIR, SOCK_PATH, PID_PATH, LOG_PATH,
  encode, decode, isEvent, type ServerResponse,
} from "../src/protocol.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const BROKER_SCRIPT = join(HERE, "..", "src", "broker.ts");

function ping(): Promise<boolean> {
  return new Promise((resolve) => {
    if (!existsSync(SOCK_PATH)) return resolve(false);
    const s = createConnection(SOCK_PATH);
    let done = false;
    const finish = (v: boolean) => { if (done) return; done = true; try { s.destroy(); } catch {} resolve(v); };
    s.once("connect", () => {
      s.write(JSON.stringify({ id: "p", op: "ping" }) + "\n");
      s.once("data", () => finish(true));
    });
    s.once("error", () => finish(false));
    setTimeout(() => finish(false), 500);
  });
}

function dial(): Promise<Socket | null> {
  return new Promise((resolve) => {
    if (!existsSync(SOCK_PATH)) return resolve(null);
    const s = createConnection(SOCK_PATH);
    let done = false;
    const finish = (v: Socket | null) => { if (done) return; done = true; resolve(v); };
    s.once("connect", () => finish(s));
    s.once("error", () => { try { s.destroy(); } catch {} finish(null); });
    setTimeout(() => { if (!done) { try { s.destroy(); } catch {} finish(null); } }, 500);
  });
}

function spawnBroker() {
  try {
    const out = openSync(LOG_PATH, "a");
    const child = spawn("bun", ["run", BROKER_SCRIPT], { detached: true, stdio: ["ignore", out, out], env: process.env });
    child.unref();
  } catch {}
}

async function connectSpawning(): Promise<Socket> {
  mkdirSync(TUNNEL_DIR, { recursive: true });
  for (let i = 0; i < 30; i++) {
    const s = await dial();
    if (s) return s;
    if (i === 0) spawnBroker();
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("could not connect to claude-tunnel broker");
}

// `watch <agent_id>`: tap the agent's delivery stream and print one JSON line
// per incoming message to stdout. Designed to be run under Claude Code's Monitor
// tool so messages surface as chat notifications. Reconnects on broker restart.
async function watch(agentId: string): Promise<never> {
  let backoff = 200;
  for (;;) {
    let sock: Socket;
    try { sock = await connectSpawning(); }
    catch { await new Promise((r) => setTimeout(r, backoff)); backoff = Math.min(backoff * 2, 5000); continue; }
    backoff = 200;

    const buf = { current: "" };
    sock.setEncoding("utf8");
    sock.write(encode({ id: "tap", op: "tap", agent_id: agentId }));

    await new Promise<void>((resolve) => {
      sock.on("data", (chunk: string) => {
        for (const obj of decode(buf, chunk)) {
          if (isEvent(obj)) {
            // one compact line per message -> one Monitor notification
            process.stdout.write(JSON.stringify(obj.message) + "\n");
          } else {
            const r = obj as ServerResponse;
            if (r && (r as any).id === "tap" && r.ok) {
              process.stderr.write(`[watch] tapping "${agentId}" via ${SOCK_PATH}\n`);
            }
          }
        }
      });
      sock.on("close", () => resolve());
      sock.on("error", () => resolve());
    });
    process.stderr.write(`[watch] broker connection lost; reconnecting...\n`);
    await new Promise((r) => setTimeout(r, 300));
  }
}

const cmd = process.argv[2];

switch (cmd) {
  case "status": {
    const alive = await ping();
    const pid = existsSync(PID_PATH) ? readFileSync(PID_PATH, "utf8").trim() : "(none)";
    console.log(`broker: ${alive ? "running" : "stopped"}`);
    console.log(`pid:    ${pid}`);
    console.log(`sock:   ${SOCK_PATH}`);
    console.log(`dir:    ${TUNNEL_DIR}`);
    if (existsSync(LOG_PATH)) {
      const sz = statSync(LOG_PATH).size;
      console.log(`log:    ${LOG_PATH} (${sz} bytes)`);
    }
    break;
  }
  case "stop": {
    if (!existsSync(PID_PATH)) { console.log("no broker pid file"); break; }
    const pid = Number(readFileSync(PID_PATH, "utf8").trim());
    if (!pid) { console.log("invalid pid"); break; }
    try {
      process.kill(pid, "SIGTERM");
      console.log(`sent SIGTERM to pid ${pid}`);
    } catch (e) {
      console.log(`could not kill pid ${pid}: ${(e as Error).message}`);
    }
    break;
  }
  case "logs": {
    if (!existsSync(LOG_PATH)) { console.log("no log file yet"); break; }
    process.stdout.write(readFileSync(LOG_PATH, "utf8"));
    break;
  }
  case "watch": {
    const agentId = process.argv[3];
    if (!agentId) { console.error("usage: claude-tunnel watch <agent_id>"); process.exit(2); }
    await watch(agentId); // never returns
    break;
  }
  default:
    console.log(`claude-tunnel <command>

  status          show broker state
  stop            terminate the broker (SIGTERM)
  logs            dump broker log
  watch <agent>   stream messages for <agent> to stdout (one JSON line each);
                  run under Claude Code's Monitor for push-style delivery

paths:
  ${SOCK_PATH}
  ${PID_PATH}
  ${LOG_PATH}
`);
}
