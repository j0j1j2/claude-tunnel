#!/usr/bin/env bun
// Claude Code Stop hook for claude-tunnel.
//
// Behavior:
//   - If this Claude Code session is registered as a tunnel agent AND has
//     pending messages / requests, block stop and tell the model to keep
//     listening. Otherwise allow stop.
//   - Self-disables silently when the broker is not running, when this session
//     hasn't registered, or on any error — so it is safe to install globally.
//
// Hook input on stdin (JSON): { session_id, transcript_path, ... }
// Hook output on stdout (JSON): { decision: "block", reason: "..." } to block,
// or no JSON / empty body to allow.

import { existsSync, readFileSync } from "node:fs";
import { createConnection } from "node:net";
import { join } from "node:path";
import { homedir } from "node:os";

const TUNNEL_DIR = join(homedir(), ".claude-tunnel");
const SOCK_PATH = join(TUNNEL_DIR, "sock");
const SESSIONS_DIR = join(TUNNEL_DIR, "sessions");

// PPID of this script == the Claude Code session that ran us.
const sessionPid = process.ppid;
const sessionFile = join(SESSIONS_DIR, `${sessionPid}.agent`);

function allow() { process.exit(0); }
function block(reason: string) {
  process.stdout.write(JSON.stringify({ decision: "block", reason }));
  process.exit(0);
}

if (!existsSync(sessionFile) || !existsSync(SOCK_PATH)) allow();

let agentId = "";
try { agentId = readFileSync(sessionFile, "utf8").trim(); } catch { allow(); }
if (!agentId) allow();

const sock = createConnection(SOCK_PATH);
let buf = "";
let answered = false;

const giveUp = setTimeout(() => { if (!answered) { try { sock.destroy(); } catch {} allow(); } }, 1500);

sock.setEncoding("utf8");
sock.once("connect", () => {
  sock.write(JSON.stringify({ id: "s", op: "agent_status", agent_id: agentId }) + "\n");
});
sock.on("data", (chunk: string) => {
  buf += chunk;
  const nl = buf.indexOf("\n");
  if (nl < 0) return;
  const line = buf.slice(0, nl);
  answered = true;
  clearTimeout(giveUp);
  try { sock.destroy(); } catch {}
  try {
    const r = JSON.parse(line);
    if (!r.ok) return allow();
    const s = r.result as { messages_pending: number; requests_pending: number; subscriptions: number; registered: boolean };
    const total = s.messages_pending + s.requests_pending;
    if (total > 0) {
      return block(
        `claude-tunnel: agent "${agentId}" still has ${s.messages_pending} pending message(s) and ${s.requests_pending} pending request(s). ` +
        `Call tunnel_inbox(wait_seconds=25) to receive them, reply via tunnel_reply when needed, ` +
        `then call tunnel_leave to release this session.`
      );
    }
    if (s.subscriptions > 0 || s.registered) {
      return block(
        `claude-tunnel: agent "${agentId}" is still registered with ${s.subscriptions} active subscription(s). ` +
        `Call tunnel_inbox(wait_seconds=25) to wait for the next message, ` +
        `or call tunnel_leave if you are done participating.`
      );
    }
    allow();
  } catch {
    allow();
  }
});
sock.on("error", () => { if (!answered) { clearTimeout(giveUp); allow(); } });
