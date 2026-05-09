#!/usr/bin/env bun
import { existsSync, readFileSync, statSync } from "node:fs";
import { createConnection } from "node:net";
import { TUNNEL_DIR, SOCK_PATH, PID_PATH, LOG_PATH } from "../src/protocol.ts";

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
  default:
    console.log(`claude-tunnel <command>

  status   show broker state
  stop     terminate the broker (SIGTERM)
  logs     dump broker log

paths:
  ${SOCK_PATH}
  ${PID_PATH}
  ${LOG_PATH}
`);
}
