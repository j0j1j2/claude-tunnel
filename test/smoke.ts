// Two simulated clients exercising pub/sub, request/reply, and queue.
import { createConnection, type Socket } from "node:net";
import { randomUUID } from "node:crypto";
import { SOCK_PATH, encode, decode, type ClientRequest, type ServerResponse } from "../src/protocol.ts";

class Client {
  s!: Socket;
  buf = { current: "" };
  pending = new Map<string, { resolve: (r: unknown) => void; reject: (e: Error) => void }>();

  constructor(public name: string) {}

  async connect() {
    this.s = createConnection(SOCK_PATH);
    await new Promise<void>((res, rej) => { this.s.once("connect", () => res()); this.s.once("error", rej); });
    this.s.setEncoding("utf8");
    this.s.on("data", (chunk: string) => {
      for (const obj of decode(this.buf, chunk)) {
        const r = obj as ServerResponse;
        const p = this.pending.get(r.id);
        if (!p) continue;
        this.pending.delete(r.id);
        if (r.ok) p.resolve(r.result); else p.reject(new Error(r.error));
      }
    });
  }

  call(op: Omit<ClientRequest, "id"> & Partial<Pick<ClientRequest, "id">>): Promise<unknown> {
    const id = randomUUID();
    const msg = { ...op, id } as ClientRequest;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.s.write(encode(msg));
    });
  }

  close() { this.s.destroy(); }
}

function ok(label: string, cond: boolean, extra: unknown = "") {
  console.log(`${cond ? "✓" : "✗"} ${label}`, extra ?? "");
  if (!cond) process.exitCode = 1;
}

const A = new Client("A");
const B = new Client("B");
await A.connect(); await B.connect();

await A.call({ op: "register", agent_id: "alpha" });
await B.call({ op: "register", agent_id: "beta" });

const who = await A.call({ op: "who" }) as string[];
ok("who lists both", who.includes("alpha") && who.includes("beta"), who);

// pub/sub
await B.call({ op: "subscribe", channel: "news" });
const pubResp = await A.call({ op: "publish", channel: "news", payload: { hello: "world" } }) as { delivered: number };
ok("publish delivered to 1 sub", pubResp.delivered === 1, pubResp);

const inbox1 = await B.call({ op: "inbox", wait_ms: 1000, max: 10 }) as any[];
ok("B got 1 publish msg", inbox1.length === 1 && inbox1[0].type === "publish" && inbox1[0].channel === "news", inbox1);

// request/reply
// Start B's inbox poll first to receive the request, then reply
const bInboxP = B.call({ op: "inbox", wait_ms: 5000, max: 10 });
// A asks B
const reqP = A.call({ op: "request", to: "beta", payload: { q: "ping?" }, wait_ms: 5000 });
const inbox2 = (await bInboxP) as any[];
ok("B saw request", inbox2.length === 1 && inbox2[0].type === "request" && typeof inbox2[0].request_id === "string", inbox2);
const requestId = inbox2[0].request_id as string;
await B.call({ op: "reply", request_id: requestId, payload: { a: "pong!" } });
const reply = (await reqP) as any;
ok("A got reply", reply.payload?.a === "pong!" && reply.from === "beta", reply);

// queue: enqueue first, then dequeue immediately
await A.call({ op: "enqueue", queue: "jobs", payload: { task: 1 } });
const job1 = (await B.call({ op: "dequeue", queue: "jobs", wait_ms: 1000 })) as any;
ok("dequeue gets buffered job", job1?.payload?.task === 1, job1);

// queue: dequeue first (waits), then enqueue
const dqP = B.call({ op: "dequeue", queue: "jobs", wait_ms: 5000 });
await new Promise(r => setTimeout(r, 50));
await A.call({ op: "enqueue", queue: "jobs", payload: { task: 2 } });
const job2 = (await dqP) as any;
ok("blocked dequeue wakes on enqueue", job2?.payload?.task === 2, job2);

// timeout
const start = Date.now();
const empty = (await B.call({ op: "dequeue", queue: "no-such-q", wait_ms: 200 })) as any;
const elapsed = Date.now() - start;
ok("dequeue timeout returns null payload", empty?.payload === null && elapsed >= 150 && elapsed < 1000, { empty, elapsed });

A.close(); B.close();
console.log("done");
process.exit(process.exitCode ?? 0);
