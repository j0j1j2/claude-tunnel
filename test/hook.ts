// Verify new broker ops (unregister, agent_status) and the Stop hook
// decision logic against a live broker.
import { createConnection, type Socket } from "node:net";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { writeFileSync, mkdirSync, unlinkSync, existsSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import {
  SOCK_PATH, SESSIONS_DIR,
  encode, decode, type ClientRequest, type ServerResponse, type AgentStatus,
} from "../src/protocol.ts";

class Client {
  s!: Socket;
  buf = { current: "" };
  pending = new Map<string, { resolve: (r: unknown) => void; reject: (e: Error) => void }>();
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
  call(op: Omit<ClientRequest, "id">): Promise<unknown> {
    const id = randomUUID();
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.s.write(encode({ ...op, id } as ClientRequest));
    });
  }
  close() { this.s.destroy(); }
}

function ok(label: string, cond: boolean, extra: unknown = "") {
  console.log(`${cond ? "✓" : "✗"} ${label}`, extra ?? "");
  if (!cond) process.exitCode = 1;
}

// Run the hook script as a subprocess with controlled PPID.
// We can't override PPID directly, but we can make the parent be `bun run hook`
// which will be the script's PPID. Then we plant the session file under that pid.
async function runHookForPid(pid: number, sessionFile: string, agentId: string): Promise<{ stdout: string; code: number }> {
  // write the session file (as if MCP server wrote it for that ppid)
  mkdirSync(SESSIONS_DIR, { recursive: true });
  writeFileSync(sessionFile, agentId);

  // We need a child whose PPID equals `pid`. The test process's pid is `pid`.
  // So spawning the hook directly works.
  return new Promise((resolve) => {
    const child = spawn("bun", ["run", "hooks/stop-block.ts"], { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    child.stdout.on("data", (d) => { out += d.toString(); });
    child.on("close", (code) => resolve({ stdout: out, code: code ?? 0 }));
  });
}

const A = new Client("A");
const B = new Client("B");
await A.connect(); await B.connect();

// scenario 1: agent with no pending work — hook should ALLOW
const myPid = process.pid;
const myAgent = `test-pid-${myPid}`;
const sessionFile = join(SESSIONS_DIR, `${myPid}.agent`);

await A.call({ op: "register", agent_id: myAgent });
await A.call({ op: "subscribe", channel: "irrelevant" });
const status1 = (await A.call({ op: "agent_status", agent_id: myAgent })) as AgentStatus;
ok("agent_status reports registered + 1 sub, 0 pending",
   status1.registered && status1.subscriptions === 1 && status1.messages_pending === 0 && status1.requests_pending === 0,
   status1);

// hook should BLOCK because still registered + has subscriptions (even with empty inbox)
const hookA = await runHookForPid(myPid, sessionFile, myAgent);
ok("hook blocks: registered with active subscription",
   hookA.stdout.includes('"decision":"block"') && hookA.stdout.includes(myAgent),
   hookA.stdout.slice(0, 200));

// scenario 2: pending message → block with stronger reason
await B.call({ op: "register", agent_id: "sender" });
await B.call({ op: "publish", channel: "irrelevant", payload: { ping: 1 } });
const hookB = await runHookForPid(myPid, sessionFile, myAgent);
ok("hook blocks: pending messages",
   hookB.stdout.includes('"decision":"block"') && hookB.stdout.includes("waiting for you"),
   hookB.stdout.slice(0, 200));
ok("pending-message block says handle, not leave",
   /tunnel_inbox/.test(hookB.stdout)
   && /tunnel_reply/.test(hookB.stdout)
   && !/tunnel_leave/i.test(hookB.stdout),
   hookB.stdout.slice(0, 280));

// scenario 3: drain inbox + unregister → hook should ALLOW
await A.call({ op: "inbox", wait_ms: 500, max: 100 });
await A.call({ op: "unregister" });
try { unlinkSync(sessionFile); } catch {}
const hookC = await runHookForPid(myPid, sessionFile, myAgent);
ok("hook allows: session file gone",
   hookC.stdout.length === 0,
   { code: hookC.code, out: hookC.stdout });

// scenario 4: with marker but agent fully unregistered → allow
writeFileSync(sessionFile, myAgent);
const status2 = (await A.call({ op: "agent_status", agent_id: myAgent })) as AgentStatus;
ok("after unregister status is clean",
   !status2.registered && status2.subscriptions === 0 && status2.messages_pending === 0,
   status2);
const hookD = await runHookForPid(myPid, sessionFile, myAgent);
ok("hook allows: agent unregistered, no pending",
   hookD.stdout.length === 0,
   { code: hookD.code, out: hookD.stdout });
try { unlinkSync(sessionFile); } catch {}

A.close(); B.close();
console.log("done");
process.exit(process.exitCode ?? 0);
