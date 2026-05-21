// Verify CLAUDE_TUNNEL_LOCK=1 makes tunnel_leave refuse to disconnect.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const SERVER = join(HERE, "..", "src", "mcp-server.ts");

function ok(label: string, cond: boolean, extra: unknown = "") {
  console.log(`${cond ? "✓" : "✗"} ${label}`, extra ?? "");
  if (!cond) process.exitCode = 1;
}

async function runSession(env: Record<string, string>) {
  const child = spawn("bun", ["run", SERVER], {
    stdio: ["pipe", "pipe", "inherit"],
    env: { ...process.env, ...env },
  });
  let buf = "";
  const pending = new Map<number, (m: any) => void>();
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    buf += chunk;
    let nl: number;
    while ((nl = buf.indexOf("\n")) !== -1) {
      const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line);
        if (typeof msg.id === "number" && pending.has(msg.id)) {
          const r = pending.get(msg.id)!; pending.delete(msg.id); r(msg);
        }
      } catch {}
    }
  });
  let nextId = 1;
  const send = (method: string, params: any): Promise<any> => {
    const id = nextId++;
    return new Promise((resolve) => {
      pending.set(id, resolve);
      child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
    });
  };
  const notify = (method: string, params: any) =>
    child.stdin.write(JSON.stringify({ jsonrpc: "2.0", method, params }) + "\n");

  await send("initialize", { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "t", version: "0" } });
  notify("notifications/initialized", {});
  return { send, kill: () => child.kill() };
}

// Locked session
{
  const s = await runSession({ CLAUDE_TUNNEL_LOCK: "1" });
  await s.send("tools/call", { name: "tunnel_register", arguments: { agent_id: "locked-agent" } });
  const leave = await s.send("tools/call", { name: "tunnel_leave", arguments: {} });
  const text = JSON.stringify(leave);
  ok("locked: tunnel_leave refuses", text.includes('"locked": true') || text.includes("locked"), leave.result?.content);
  ok("locked: still says stay connected", /stay connected|must stay/i.test(text), text.slice(0, 160));
  s.kill();
}

// Unlocked session
{
  const s = await runSession({ CLAUDE_TUNNEL_LOCK: "" });
  await s.send("tools/call", { name: "tunnel_register", arguments: { agent_id: "free-agent" } });
  const leave = await s.send("tools/call", { name: "tunnel_leave", arguments: {} });
  const text = JSON.stringify(leave);
  ok("unlocked: tunnel_leave succeeds", text.includes("unregistered") && !text.includes('"locked": true'), leave.result?.content);
  s.kill();
}

setTimeout(() => process.exit(process.exitCode ?? 0), 150);
