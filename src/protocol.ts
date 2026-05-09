import { homedir } from "node:os";
import { join } from "node:path";

export const TUNNEL_DIR = join(homedir(), ".claude-tunnel");
export const SOCK_PATH = join(TUNNEL_DIR, "sock");
export const PID_PATH = join(TUNNEL_DIR, "broker.pid");
export const LOG_PATH = join(TUNNEL_DIR, "broker.log");
export const SESSIONS_DIR = join(TUNNEL_DIR, "sessions");

export const IDLE_SHUTDOWN_MS = 60_000;

export type AgentStatus = {
  registered: boolean;
  messages_pending: number;
  requests_pending: number;
  subscriptions: number;
};

export type InboxMessage =
  | { type: "publish"; channel: string; from?: string; payload: unknown; ts: number }
  | { type: "request"; request_id: string; from?: string; payload: unknown; ts: number };

export type ClientRequest =
  | { id: string; op: "register"; agent_id: string }
  | { id: string; op: "unregister" }
  | { id: string; op: "agent_status"; agent_id: string }
  | { id: string; op: "who" }
  | { id: string; op: "publish"; channel: string; payload: unknown }
  | { id: string; op: "subscribe"; channel: string }
  | { id: string; op: "unsubscribe"; channel: string }
  | { id: string; op: "inbox"; wait_ms: number; max: number }
  | { id: string; op: "request"; to: string; payload: unknown; wait_ms: number }
  | { id: string; op: "reply"; request_id: string; payload: unknown }
  | { id: string; op: "enqueue"; queue: string; payload: unknown }
  | { id: string; op: "dequeue"; queue: string; wait_ms: number }
  | { id: string; op: "ping" };

export type ServerResponse =
  | { id: string; ok: true; result: unknown }
  | { id: string; ok: false; error: string };

export function encode(obj: unknown): string {
  return JSON.stringify(obj) + "\n";
}

export function* decode(buf: { current: string }, chunk: string): Generator<unknown> {
  buf.current += chunk;
  let nl: number;
  while ((nl = buf.current.indexOf("\n")) !== -1) {
    const line = buf.current.slice(0, nl);
    buf.current = buf.current.slice(nl + 1);
    if (line.length === 0) continue;
    try {
      yield JSON.parse(line);
    } catch {
      // skip malformed line
    }
  }
}
