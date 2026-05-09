import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createConnection, type Socket } from "node:net";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, openSync, writeFileSync, unlinkSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import {
  TUNNEL_DIR, SOCK_PATH, LOG_PATH, SESSIONS_DIR,
  encode, decode,
  type ClientRequest, type ServerResponse,
} from "./protocol.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const BROKER_SCRIPT = join(HERE, "broker.ts");

type Pending = { resolve: (r: unknown) => void; reject: (e: Error) => void };

class BrokerClient {
  private socket: Socket | null = null;
  private buf = { current: "" };
  private pending = new Map<string, Pending>();
  private connecting: Promise<void> | null = null;

  async connect(): Promise<void> {
    if (this.socket && !this.socket.destroyed) return;
    if (this.connecting) return this.connecting;
    this.connecting = this.doConnect().finally(() => { this.connecting = null; });
    return this.connecting;
  }

  private async doConnect(): Promise<void> {
    mkdirSync(TUNNEL_DIR, { recursive: true });
    for (let attempt = 0; attempt < 30; attempt++) {
      const sock = await this.tryDial();
      if (sock) {
        this.attach(sock);
        return;
      }
      if (attempt === 0) this.spawnBroker();
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error("could not connect to claude-tunnel broker");
  }

  private tryDial(): Promise<Socket | null> {
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

  private spawnBroker() {
    try {
      const out = openSync(LOG_PATH, "a");
      const child = spawn("bun", ["run", BROKER_SCRIPT], {
        detached: true,
        stdio: ["ignore", out, out],
        env: process.env,
      });
      child.unref();
    } catch (e) {
      // fall through; next dial will fail
    }
  }

  private attach(socket: Socket) {
    this.socket = socket;
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      for (const obj of decode(this.buf, chunk)) {
        const resp = obj as ServerResponse;
        const p = this.pending.get(resp.id);
        if (!p) continue;
        this.pending.delete(resp.id);
        if (resp.ok) p.resolve(resp.result);
        else p.reject(new Error(resp.error));
      }
    });
    const onEnd = () => {
      for (const [, p] of this.pending) p.reject(new Error("broker disconnected"));
      this.pending.clear();
      this.socket = null;
    };
    socket.on("close", onEnd);
    socket.on("error", onEnd);
  }

  async call(op: ClientRequest): Promise<unknown> {
    await this.connect();
    return new Promise((resolve, reject) => {
      this.pending.set(op.id, { resolve, reject });
      try { this.socket!.write(encode(op)); }
      catch (e) {
        this.pending.delete(op.id);
        reject(e as Error);
      }
    });
  }
}

const broker = new BrokerClient();

function newId(): string { return randomUUID(); }

function asText(v: unknown): { content: { type: "text"; text: string }[] } {
  return { content: [{ type: "text", text: typeof v === "string" ? v : JSON.stringify(v, null, 2) }] };
}

const server = new McpServer({ name: "claude-tunnel", version: "0.1.0" });

// The parent of this MCP server process is (typically) the Claude Code session.
// We stamp a session marker file so the Stop hook can correlate session -> agent_id.
const SESSION_PPID = process.ppid;
const SESSION_FILE = join(SESSIONS_DIR, `${SESSION_PPID}.agent`);

function writeSessionMarker(agentId: string) {
  try {
    mkdirSync(SESSIONS_DIR, { recursive: true });
    writeFileSync(SESSION_FILE, agentId);
  } catch {}
}

function clearSessionMarker() {
  try { unlinkSync(SESSION_FILE); } catch {}
}

process.on("exit", clearSessionMarker);
process.on("SIGINT", () => { clearSessionMarker(); process.exit(0); });
process.on("SIGTERM", () => { clearSessionMarker(); process.exit(0); });

server.registerTool("tunnel_register", {
  description: "Register this Claude Code session as `agent_id`. Required before any other operation. The agent_id is how others address you. While registered, this session will be kept alive by the Stop hook (if installed) until tunnel_leave is called.",
  inputSchema: { agent_id: z.string().min(1).describe("Unique identifier for this agent (e.g. 'planner', 'coder-1')") },
}, async ({ agent_id }) => {
  const r = await broker.call({ id: newId(), op: "register", agent_id });
  writeSessionMarker(agent_id);
  return asText(r);
});

server.registerTool("tunnel_leave", {
  description: "Deregister this session from the tunnel: clears subscriptions, drops pending inbox, and releases the Stop hook so the session can end normally. Call when this agent is done participating.",
  inputSchema: {},
}, async () => {
  const r = await broker.call({ id: newId(), op: "unregister" });
  clearSessionMarker();
  return asText(r);
});

server.registerTool("tunnel_status", {
  description: "Report this agent's current status: pending messages, pending requests, subscriptions. Useful to check whether there is unfinished work before leaving.",
  inputSchema: { agent_id: z.string().min(1) },
}, async ({ agent_id }) => {
  const r = await broker.call({ id: newId(), op: "agent_status", agent_id });
  return asText(r);
});

server.registerTool("tunnel_who", {
  description: "List currently registered agent_ids.",
  inputSchema: {},
}, async () => {
  const r = await broker.call({ id: newId(), op: "who" });
  return asText(r);
});

server.registerTool("tunnel_publish", {
  description: "Broadcast a message to all subscribers of `channel`. Self does not receive own publishes.",
  inputSchema: {
    channel: z.string().min(1),
    payload: z.unknown().describe("Any JSON-serializable value"),
  },
}, async ({ channel, payload }) => {
  const r = await broker.call({ id: newId(), op: "publish", channel, payload });
  return asText(r);
});

server.registerTool("tunnel_subscribe", {
  description: "Subscribe this agent to a channel. Future messages on the channel land in your inbox.",
  inputSchema: { channel: z.string().min(1) },
}, async ({ channel }) => {
  const r = await broker.call({ id: newId(), op: "subscribe", channel });
  return asText(r);
});

server.registerTool("tunnel_unsubscribe", {
  description: "Unsubscribe this agent from a channel.",
  inputSchema: { channel: z.string().min(1) },
}, async ({ channel }) => {
  const r = await broker.call({ id: newId(), op: "unsubscribe", channel });
  return asText(r);
});

server.registerTool("tunnel_inbox", {
  description: "Long-poll for messages addressed to this agent (subscribed publishes + incoming requests). Returns immediately if messages are queued; otherwise waits up to wait_seconds. Reply to incoming requests with tunnel_reply.",
  inputSchema: {
    wait_seconds: z.number().int().min(0).max(120).default(20),
    max: z.number().int().min(1).max(100).default(20),
  },
}, async ({ wait_seconds, max }) => {
  const r = await broker.call({
    id: newId(), op: "inbox",
    wait_ms: (wait_seconds ?? 20) * 1000,
    max: max ?? 20,
  });
  return asText(r);
});

server.registerTool("tunnel_request", {
  description: "Send a request to a specific agent and block until they reply or timeout. The target sees a message of type 'request' in their inbox with a `request_id`; they call tunnel_reply with that id.",
  inputSchema: {
    to: z.string().min(1).describe("Target agent_id"),
    payload: z.unknown(),
    wait_seconds: z.number().int().min(1).max(300).default(60),
  },
}, async ({ to, payload, wait_seconds }) => {
  const r = await broker.call({
    id: newId(), op: "request", to, payload,
    wait_ms: (wait_seconds ?? 60) * 1000,
  });
  return asText(r);
});

server.registerTool("tunnel_reply", {
  description: "Reply to an incoming request_id received via tunnel_inbox.",
  inputSchema: {
    request_id: z.string().min(1),
    payload: z.unknown(),
  },
}, async ({ request_id, payload }) => {
  const r = await broker.call({ id: newId(), op: "reply", request_id, payload });
  return asText(r);
});

server.registerTool("tunnel_enqueue", {
  description: "Push a job onto a named queue (FIFO). Any waiting consumer is woken; otherwise the job is buffered until a consumer arrives.",
  inputSchema: {
    queue: z.string().min(1),
    payload: z.unknown(),
  },
}, async ({ queue, payload }) => {
  const r = await broker.call({ id: newId(), op: "enqueue", queue, payload });
  return asText(r);
});

server.registerTool("tunnel_dequeue", {
  description: "Pop a job from a named queue. If empty, blocks up to wait_seconds. Returns { payload: <job> } or { payload: null } on timeout.",
  inputSchema: {
    queue: z.string().min(1),
    wait_seconds: z.number().int().min(0).max(300).default(30),
  },
}, async ({ queue, wait_seconds }) => {
  const r = await broker.call({
    id: newId(), op: "dequeue", queue,
    wait_ms: (wait_seconds ?? 30) * 1000,
  });
  return asText(r);
});

const transport = new StdioServerTransport();
await server.connect(transport);
