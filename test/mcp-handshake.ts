// Drive the MCP stdio server with raw JSON-RPC and verify it works end-to-end,
// including auto-spawning the broker if it isn't already running.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const SERVER = join(HERE, "..", "src", "mcp-server.ts");

const child = spawn("bun", ["run", SERVER], { stdio: ["pipe", "pipe", "inherit"] });

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
        const r = pending.get(msg.id)!;
        pending.delete(msg.id);
        r(msg);
      }
    } catch {}
  }
});

let nextId = 1;
function send(method: string, params: any): Promise<any> {
  const id = nextId++;
  const wire = { jsonrpc: "2.0", id, method, params };
  return new Promise((resolve) => {
    pending.set(id, resolve);
    child.stdin.write(JSON.stringify(wire) + "\n");
  });
}

function notify(method: string, params: any) {
  child.stdin.write(JSON.stringify({ jsonrpc: "2.0", method, params }) + "\n");
}

function ok(label: string, cond: boolean, extra: unknown = "") {
  console.log(`${cond ? "✓" : "✗"} ${label}`, extra ?? "");
  if (!cond) process.exitCode = 1;
}

const init = await send("initialize", {
  protocolVersion: "2024-11-05",
  capabilities: {},
  clientInfo: { name: "smoke", version: "0.0" },
});
ok("initialize ok", init?.result?.serverInfo?.name === "claude-tunnel", init?.result?.serverInfo);
notify("notifications/initialized", {});

const tools = await send("tools/list", {});
const names = (tools.result.tools as any[]).map(t => t.name).sort();
ok("tools list contains all 12 (incl. tunnel_status, tunnel_leave)", names.length === 12, names);

const reg = await send("tools/call", { name: "tunnel_register", arguments: { agent_id: "smoke-mcp" } });
ok("register via MCP (auto-spawn broker)", JSON.stringify(reg).includes("smoke-mcp"), reg.result?.content);

const who = await send("tools/call", { name: "tunnel_who", arguments: {} });
ok("who via MCP", JSON.stringify(who).includes("smoke-mcp"), who.result?.content);

child.kill();
setTimeout(() => process.exit(process.exitCode ?? 0), 100);
