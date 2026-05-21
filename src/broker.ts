import { createServer, createConnection, type Socket } from "node:net";
import { mkdirSync, unlinkSync, writeFileSync, existsSync, openSync, closeSync, appendFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import {
  TUNNEL_DIR, SOCK_PATH, PID_PATH, LOG_PATH, SESSIONS_DIR,
  IDLE_SHUTDOWN_MS, REAPER_INTERVAL_MS,
  encode, decode,
  type ClientRequest, type ServerResponse, type InboxMessage, type AgentStatus, type AgentMeta, type Scope,
} from "./protocol.ts";

type Conn = {
  id: string;
  socket: Socket;
  buf: { current: string };
  agentId: string | null;
  subs: Set<string>;
  taps: Set<string>; // agent_ids this connection is tapping (read stream)
  // long-poll waiters tied to this connection
  inboxWaiter: { reqId: string; max: number; timer: NodeJS.Timeout } | null;
  dequeueWaiters: Map<string /*reqId*/, { queue: string; timer: NodeJS.Timeout }>;
};

type PendingReply = { connId: string; reqId: string; timer: NodeJS.Timeout };

const conns = new Map<string, Conn>();
const agentIndex = new Map<string, string>(); // agent_id -> conn.id
const agentMeta = new Map<string, AgentMeta>(); // agent_id -> metadata (cwd, git_root, pid, ppid)
const inboxes = new Map<string, InboxMessage[]>(); // agent_id -> queued messages
const channelSubs = new Map<string, Set<string>>(); // channel -> agent_ids
const taps = new Map<string, Set<string>>(); // agent_id -> conn.ids tapping its stream
const pendingReplies = new Map<string, PendingReply>(); // request_id -> waiting requester
const queues = new Map<string, unknown[]>(); // queue name -> jobs
const queueWaiters = new Map<string, Array<{ connId: string; reqId: string; timer: NodeJS.Timeout }>>(); // FIFO

let idleTimer: NodeJS.Timeout | null = null;
let reaperTimer: NodeJS.Timeout | null = null;

function pidAlive(pid: number | null | undefined): boolean {
  if (!pid || pid <= 0) return true; // unknown -> assume alive
  try { process.kill(pid, 0); return true; }
  catch (e: any) {
    // ESRCH = no such process; EPERM = process exists but we can't signal -> still alive
    return e?.code === "EPERM";
  }
}

function dropAgent(agentId: string, reason: string) {
  const connId = agentIndex.get(agentId);
  agentIndex.delete(agentId);
  agentMeta.delete(agentId);
  inboxes.delete(agentId);
  for (const [ch, set] of channelSubs) {
    if (set.delete(agentId) && set.size === 0) channelSubs.delete(ch);
  }
  if (connId) {
    const conn = conns.get(connId);
    if (conn && conn.agentId === agentId) {
      conn.agentId = null;
      conn.subs.clear();
      // close the socket so the orphan client gives up; cleanup handler will
      // also wipe queue/inbox waiters tied to this conn.
      try { conn.socket.destroy(); } catch {}
    }
  }
  log(`reaped agent="${agentId}" reason=${reason}`);
}

function reap() {
  // 1. drop agents whose pid (or ppid) is dead
  for (const [agentId, meta] of [...agentMeta]) {
    if (meta.pid && !pidAlive(meta.pid)) { dropAgent(agentId, `pid ${meta.pid} dead`); continue; }
    if (meta.ppid && !pidAlive(meta.ppid)) { dropAgent(agentId, `ppid ${meta.ppid} dead`); continue; }
  }
  // 2. clean stale session marker files (~/.claude-tunnel/sessions/<ppid>.agent)
  try {
    const entries = readdirSync(SESSIONS_DIR);
    for (const name of entries) {
      const m = /^(\d+)\.agent$/.exec(name);
      if (!m) continue;
      const pid = Number(m[1]);
      if (!pidAlive(pid)) {
        try { unlinkSync(join(SESSIONS_DIR, name)); log(`reaped session file ${name}`); } catch {}
      }
    }
  } catch { /* dir may not exist */ }
}

function startReaper() {
  if (reaperTimer) return;
  reaperTimer = setInterval(reap, REAPER_INTERVAL_MS);
  reaperTimer.unref?.();
}

function log(line: string) {
  try { appendFileSync(LOG_PATH, `[${new Date().toISOString()}] ${line}\n`); } catch {}
}

function send(conn: Conn, msg: ServerResponse) {
  try { conn.socket.write(encode(msg)); } catch {}
}

function ok(conn: Conn, reqId: string, result: unknown) {
  send(conn, { id: reqId, ok: true, result });
}

function err(conn: Conn, reqId: string, error: string) {
  send(conn, { id: reqId, ok: false, error });
}

function getInbox(agentId: string): InboxMessage[] {
  let q = inboxes.get(agentId);
  if (!q) { q = []; inboxes.set(agentId, q); }
  return q;
}

function deliverToAgent(agentId: string, msg: InboxMessage) {
  // If the agent has an active tap (e.g. a `claude-tunnel watch` feeding a
  // Monitor), stream the message straight to the tap(s) and skip inbox
  // buffering — the tap is the live consumer, so nothing accumulates.
  const tapSet = taps.get(agentId);
  if (tapSet && tapSet.size > 0) {
    let streamed = false;
    for (const cid of tapSet) {
      const c = conns.get(cid);
      if (c) { try { c.socket.write(encode({ event: "message", agent_id: agentId, message: msg })); streamed = true; } catch {} }
    }
    if (streamed) return;
    // all tap conns vanished; fall through to inbox buffering
  }
  const q = getInbox(agentId);
  q.push(msg);
  // wake any inbox waiter for this agent
  const connId = agentIndex.get(agentId);
  if (!connId) return;
  const conn = conns.get(connId);
  if (!conn || !conn.inboxWaiter) return;
  const w = conn.inboxWaiter;
  conn.inboxWaiter = null;
  clearTimeout(w.timer);
  const drained = q.splice(0, w.max);
  ok(conn, w.reqId, drained);
}

function startIdleTimer() {
  if (idleTimer) clearTimeout(idleTimer);
  idleTimer = setTimeout(() => {
    if (conns.size === 0) {
      log("idle shutdown");
      try { unlinkSync(SOCK_PATH); } catch {}
      try { unlinkSync(PID_PATH); } catch {}
      process.exit(0);
    }
  }, IDLE_SHUTDOWN_MS);
}

function cancelIdleTimer() {
  if (idleTimer) { clearTimeout(idleTimer); idleTimer = null; }
}

function handle(conn: Conn, req: ClientRequest) {
  switch (req.op) {
    case "ping": return ok(conn, req.id, "pong");

    case "tap": {
      conn.taps.add(req.agent_id);
      let set = taps.get(req.agent_id);
      if (!set) { set = new Set(); taps.set(req.agent_id, set); }
      set.add(conn.id);
      return ok(conn, req.id, { tapping: req.agent_id });
    }

    case "register": {
      // unbind any prior agent on this conn
      if (conn.agentId && agentIndex.get(conn.agentId) === conn.id) {
        agentIndex.delete(conn.agentId);
        agentMeta.delete(conn.agentId);
      }
      const prev = agentIndex.get(req.agent_id);
      if (prev && prev !== conn.id) {
        // displace prior holder; their inbox-poll, if any, will just timeout
        const prevConn = conns.get(prev);
        if (prevConn && prevConn.agentId === req.agent_id) prevConn.agentId = null;
      }
      conn.agentId = req.agent_id;
      agentIndex.set(req.agent_id, conn.id);
      const meta: AgentMeta = {
        agent_id: req.agent_id,
        cwd: req.cwd ?? null,
        git_root: req.git_root ?? null,
        pid: req.pid ?? null,
        ppid: req.ppid ?? null,
        registered_at: Date.now(),
      };
      agentMeta.set(req.agent_id, meta);
      return ok(conn, req.id, meta);
    }

    case "unregister": {
      if (!conn.agentId) return ok(conn, req.id, { unregistered: false });
      const agentId = conn.agentId;
      // remove subscriptions
      for (const ch of conn.subs) {
        const s = channelSubs.get(ch);
        if (s) {
          s.delete(agentId);
          if (s.size === 0) channelSubs.delete(ch);
        }
      }
      conn.subs.clear();
      // drop pending inbox + meta
      inboxes.delete(agentId);
      agentMeta.delete(agentId);
      // remove from agent index
      if (agentIndex.get(agentId) === conn.id) agentIndex.delete(agentId);
      conn.agentId = null;
      return ok(conn, req.id, { unregistered: true, agent_id: agentId });
    }

    case "agent_status": {
      const inbox = inboxes.get(req.agent_id) ?? [];
      let messages = 0, requests = 0;
      for (const m of inbox) {
        if (m.type === "publish") messages++;
        else if (m.type === "request") requests++;
      }
      let subs = 0;
      for (const s of channelSubs.values()) if (s.has(req.agent_id)) subs++;
      const status: AgentStatus = {
        registered: agentIndex.has(req.agent_id),
        messages_pending: messages,
        requests_pending: requests,
        subscriptions: subs,
      };
      return ok(conn, req.id, status);
    }

    case "who": {
      const scope: Scope = req.scope ?? "machine";
      const all = [...agentIndex.keys()].map(id => agentMeta.get(id)).filter((m): m is AgentMeta => !!m);
      if (scope === "machine") return ok(conn, req.id, all);
      // scoped: needs caller's own meta
      const callerMeta = conn.agentId ? agentMeta.get(conn.agentId) : null;
      if (!callerMeta) return err(conn, req.id, `must register before scoped who (scope=${scope})`);
      const filtered = all.filter(m => {
        if (scope === "directory") return m.cwd && callerMeta.cwd && m.cwd === callerMeta.cwd;
        if (scope === "repo") return m.git_root && callerMeta.git_root && m.git_root === callerMeta.git_root;
        return true;
      });
      return ok(conn, req.id, filtered);
    }

    case "publish": {
      if (!conn.agentId) return err(conn, req.id, "must register before publishing");
      const subs = channelSubs.get(req.channel);
      let delivered = 0;
      if (subs) {
        for (const agentId of subs) {
          if (agentId === conn.agentId) continue; // don't echo to self
          deliverToAgent(agentId, {
            type: "publish",
            channel: req.channel,
            from: conn.agentId,
            payload: req.payload,
            ts: Date.now(),
          });
          delivered++;
        }
      }
      return ok(conn, req.id, { delivered });
    }

    case "subscribe": {
      if (!conn.agentId) return err(conn, req.id, "must register before subscribing");
      conn.subs.add(req.channel);
      let s = channelSubs.get(req.channel);
      if (!s) { s = new Set(); channelSubs.set(req.channel, s); }
      s.add(conn.agentId);
      return ok(conn, req.id, { channel: req.channel, subscribers: s.size });
    }

    case "unsubscribe": {
      if (!conn.agentId) return err(conn, req.id, "must register before unsubscribing");
      conn.subs.delete(req.channel);
      const s = channelSubs.get(req.channel);
      if (s) {
        s.delete(conn.agentId);
        if (s.size === 0) channelSubs.delete(req.channel);
      }
      return ok(conn, req.id, { channel: req.channel });
    }

    case "inbox": {
      if (!conn.agentId) return err(conn, req.id, "must register before inbox");
      const q = getInbox(conn.agentId);
      if (q.length > 0) {
        const drained = q.splice(0, req.max);
        return ok(conn, req.id, drained);
      }
      if (conn.inboxWaiter) {
        // overwrite previous waiter (latest poll wins)
        clearTimeout(conn.inboxWaiter.timer);
        ok(conn, conn.inboxWaiter.reqId, []);
      }
      const timer = setTimeout(() => {
        if (conn.inboxWaiter && conn.inboxWaiter.reqId === req.id) {
          conn.inboxWaiter = null;
          ok(conn, req.id, []);
        }
      }, Math.max(0, req.wait_ms));
      conn.inboxWaiter = { reqId: req.id, max: req.max, timer };
      return;
    }

    case "request": {
      if (!conn.agentId) return err(conn, req.id, "must register before request");
      if (!agentIndex.has(req.to) && !inboxes.has(req.to)) {
        // allow even if not currently registered — they may show up; but warn if no inbox at all
        // we still buffer
      }
      const requestId = randomUUID();
      const timer = setTimeout(() => {
        if (pendingReplies.has(requestId)) {
          pendingReplies.delete(requestId);
          err(conn, req.id, "request timed out");
        }
      }, Math.max(1, req.wait_ms));
      pendingReplies.set(requestId, { connId: conn.id, reqId: req.id, timer });
      deliverToAgent(req.to, {
        type: "request",
        request_id: requestId,
        from: conn.agentId,
        payload: req.payload,
        ts: Date.now(),
      });
      return;
    }

    case "reply": {
      const pr = pendingReplies.get(req.request_id);
      if (!pr) return err(conn, req.id, "no such request_id (already replied or timed out)");
      pendingReplies.delete(req.request_id);
      clearTimeout(pr.timer);
      const requester = conns.get(pr.connId);
      if (requester) {
        ok(requester, pr.reqId, { request_id: req.request_id, from: conn.agentId, payload: req.payload });
      }
      return ok(conn, req.id, { delivered: !!requester });
    }

    case "enqueue": {
      const waiters = queueWaiters.get(req.queue);
      if (waiters && waiters.length > 0) {
        const w = waiters.shift()!;
        if (waiters.length === 0) queueWaiters.delete(req.queue);
        clearTimeout(w.timer);
        const target = conns.get(w.connId);
        if (target) ok(target, w.reqId, { payload: req.payload });
        return ok(conn, req.id, { delivered: !!target });
      }
      let q = queues.get(req.queue);
      if (!q) { q = []; queues.set(req.queue, q); }
      q.push(req.payload);
      return ok(conn, req.id, { queued: true, depth: q.length });
    }

    case "dequeue": {
      const q = queues.get(req.queue);
      if (q && q.length > 0) {
        const payload = q.shift()!;
        if (q.length === 0) queues.delete(req.queue);
        return ok(conn, req.id, { payload });
      }
      let waiters = queueWaiters.get(req.queue);
      if (!waiters) { waiters = []; queueWaiters.set(req.queue, waiters); }
      const timer = setTimeout(() => {
        const arr = queueWaiters.get(req.queue);
        if (!arr) return;
        const i = arr.findIndex(w => w.reqId === req.id && w.connId === conn.id);
        if (i >= 0) arr.splice(i, 1);
        if (arr.length === 0) queueWaiters.delete(req.queue);
        ok(conn, req.id, { payload: null });
      }, Math.max(0, req.wait_ms));
      waiters.push({ connId: conn.id, reqId: req.id, timer });
      conn.dequeueWaiters.set(req.id, { queue: req.queue, timer });
      return;
    }
  }
}

function onConnection(socket: Socket) {
  cancelIdleTimer();
  const conn: Conn = {
    id: randomUUID(),
    socket,
    buf: { current: "" },
    agentId: null,
    subs: new Set(),
    taps: new Set(),
    inboxWaiter: null,
    dequeueWaiters: new Map(),
  };
  conns.set(conn.id, conn);

  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    for (const obj of decode(conn.buf, chunk)) {
      const req = obj as ClientRequest;
      if (!req || typeof (req as any).id !== "string" || typeof (req as any).op !== "string") continue;
      try { handle(conn, req); }
      catch (e) { err(conn, (req as any).id, String((e as Error)?.message ?? e)); }
    }
  });

  const cleanup = () => {
    if (!conns.has(conn.id)) return;
    conns.delete(conn.id);
    if (conn.agentId && agentIndex.get(conn.agentId) === conn.id) {
      agentIndex.delete(conn.agentId);
      agentMeta.delete(conn.agentId);
    }
    for (const ch of conn.subs) {
      const s = channelSubs.get(ch);
      if (s && conn.agentId) {
        s.delete(conn.agentId);
        if (s.size === 0) channelSubs.delete(ch);
      }
    }
    for (const agentId of conn.taps) {
      const set = taps.get(agentId);
      if (set) { set.delete(conn.id); if (set.size === 0) taps.delete(agentId); }
    }
    if (conn.inboxWaiter) clearTimeout(conn.inboxWaiter.timer);
    for (const [reqId, dw] of conn.dequeueWaiters) {
      clearTimeout(dw.timer);
      const arr = queueWaiters.get(dw.queue);
      if (arr) {
        const i = arr.findIndex(w => w.connId === conn.id && w.reqId === reqId);
        if (i >= 0) arr.splice(i, 1);
        if (arr.length === 0) queueWaiters.delete(dw.queue);
      }
    }
    if (conns.size === 0) startIdleTimer();
  };
  socket.on("close", cleanup);
  socket.on("error", cleanup);
}

function tryConnect(path: string): Promise<boolean> {
  return new Promise((resolve) => {
    const s = createConnection(path);
    let done = false;
    const finish = (v: boolean) => { if (done) return; done = true; s.destroy(); resolve(v); };
    s.once("connect", () => finish(true));
    s.once("error", () => finish(false));
    setTimeout(() => finish(false), 500);
  });
}

async function main() {
  mkdirSync(TUNNEL_DIR, { recursive: true });
  // ensure log file exists
  try { closeSync(openSync(LOG_PATH, "a")); } catch {}

  if (existsSync(SOCK_PATH)) {
    const alive = await tryConnect(SOCK_PATH);
    if (alive) {
      log("another broker is alive; exiting");
      process.exit(0);
    }
    try { unlinkSync(SOCK_PATH); } catch {}
  }

  const server = createServer(onConnection);
  server.on("error", (e) => { log(`server error: ${e}`); process.exit(1); });
  server.listen(SOCK_PATH, () => {
    writeFileSync(PID_PATH, String(process.pid));
    log(`listening pid=${process.pid} sock=${SOCK_PATH}`);
    startIdleTimer(); // starts with no clients
    startReaper();   // periodic dead-pid + stale-session-file cleanup
    reap();          // immediate sweep on startup
  });

  const shutdown = (sig: string) => {
    log(`signal ${sig}; shutting down`);
    try { unlinkSync(SOCK_PATH); } catch {}
    try { unlinkSync(PID_PATH); } catch {}
    process.exit(0);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((e) => { log(`fatal: ${e}`); process.exit(1); });
