// Verify the tap stream: a tapping connection receives delivered messages as
// server events (no inbox buffering), and a separately-registered MCP-style
// connection can still send AS the same agent (bidirectional).
import { createConnection, type Socket } from "node:net";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  SOCK_PATH, encode, decode, isEvent,
  type ClientRequest, type ServerResponse, type ServerEvent,
} from "../src/protocol.ts";

class Client {
  s!: Socket;
  buf = { current: "" };
  pending = new Map<string, { resolve: (r: unknown) => void; reject: (e: Error) => void }>();
  events: ServerEvent[] = [];
  onEvent: ((e: ServerEvent) => void) | null = null;
  async connect() {
    this.s = createConnection(SOCK_PATH);
    await new Promise<void>((res, rej) => { this.s.once("connect", () => res()); this.s.once("error", rej); });
    this.s.setEncoding("utf8");
    this.s.on("data", (chunk: string) => {
      for (const obj of decode(this.buf, chunk)) {
        if (isEvent(obj)) { this.events.push(obj); this.onEvent?.(obj); continue; }
        const r = obj as ServerResponse;
        const p = this.pending.get(r.id);
        if (!p) continue;
        this.pending.delete(r.id);
        if (r.ok) p.resolve(r.result); else p.reject(new Error(r.error));
      }
    });
  }
  call(op: Omit<ClientRequest, "id">): Promise<unknown> {
    const id = randomUUID();
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.s.write(encode({ ...op, id } as ClientRequest));
    });
  }
  nextEvent(timeoutMs = 1500): Promise<ServerEvent | null> {
    if (this.events.length) return Promise.resolve(this.events.shift()!);
    return new Promise((resolve) => {
      const t = setTimeout(() => { this.onEvent = null; resolve(null); }, timeoutMs);
      this.onEvent = (e) => { clearTimeout(t); this.onEvent = null; this.events.shift(); resolve(e); };
    });
  }
  close() { this.s.destroy(); }
}

function ok(label: string, cond: boolean, extra: unknown = "") {
  console.log(`${cond ? "✓" : "✗"} ${label}`, extra ?? "");
  if (!cond) process.exitCode = 1;
}

// A = sender, BMcp = B's "MCP side" (registered, sends as B), BWatch = B's tap stream
const A = new Client(); await A.connect();
const BMcp = new Client(); await BMcp.connect();
const BWatch = new Client(); await BWatch.connect();

await A.call({ op: "register", agent_id: "A" });
await BMcp.call({ op: "register", agent_id: "B" }); // exclusive registration for sending as B
await BWatch.call({ op: "tap", agent_id: "B" });     // read stream for receiving

// 1. request A -> B arrives on the tap stream
const reqP = A.call({ op: "request", to: "B", payload: { q: "status?" }, wait_ms: 4000 });
const ev1 = await BWatch.nextEvent();
ok("tap receives request event", !!ev1 && ev1.message.type === "request" && (ev1.message as any).payload?.q === "status?", ev1?.message);
const requestId = (ev1!.message as any).request_id as string;

// 2. B replies via its MCP-side connection (reply needs no registration, but B is registered anyway)
await BMcp.call({ op: "reply", request_id: requestId, payload: { a: "all good" } });
const reply = (await reqP) as any;
ok("A gets B's reply (sent from MCP side)", reply.payload?.a === "all good" && reply.from === "B", reply);

// 3. pub/sub: B subscribes (via MCP side), A publishes, tap receives
await BMcp.call({ op: "subscribe", channel: "news" });
await A.call({ op: "publish", channel: "news", payload: { headline: "hi" } });
const ev2 = await BWatch.nextEvent();
ok("tap receives published event", !!ev2 && ev2.message.type === "publish" && (ev2.message as any).payload?.headline === "hi", ev2?.message);

// 4. bidirectional: B sends a request to A (proving the registered MCP side can initiate as B)
const AWatch = new Client(); await AWatch.connect();
await AWatch.call({ op: "tap", agent_id: "A" });
const bReqP = BMcp.call({ op: "request", to: "A", payload: { from: "B" }, wait_ms: 4000 });
const evA = await AWatch.nextEvent();
ok("B can initiate to A; A's tap receives it", !!evA && (evA.message as any).payload?.from === "B" && (evA.message as any).from === "B", evA?.message);
await A.call({ op: "reply", request_id: (evA!.message as any).request_id, payload: { ok: 1 } });
const bReply = (await bReqP) as any;
ok("B receives A's reply", bReply.payload?.ok === 1, bReply);

// 5. while tap active, inbox poll on a *different* (non-tapped) registration is unaffected;
//    also confirm tapped agent's inbox stays empty (no buffering).
const stat = (await BMcp.call({ op: "agent_status", agent_id: "B" })) as any;
ok("tapped agent inbox not buffered (0 pending)", stat.messages_pending === 0 && stat.requests_pending === 0, stat);

A.close(); BMcp.close(); BWatch.close(); AWatch.close();
console.log("done");
process.exit(process.exitCode ?? 0);
