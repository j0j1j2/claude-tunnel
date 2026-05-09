#!/usr/bin/env bun
// Non-interactive installer. Designed to be safely run by agents.
//
//   bun run scripts/install.ts                  # MCP + Stop hook (default)
//   bun run scripts/install.ts --mcp-only       # just MCP, skip hook
//   bun run scripts/install.ts --scope=project  # narrow MCP scope (default: user)
//
// Idempotent: re-running replaces the MCP entry and skips the hook if it's
// already there. Always exits non-zero on hard failure so agents can detect it.

import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";

const HERE = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(HERE, "..");
const MCP_PATH = join(PROJECT_ROOT, "src", "mcp-server.ts");
const HOOK_PATH = join(PROJECT_ROOT, "hooks", "stop-block.ts");
const CLAUDE_DIR = join(homedir(), ".claude");
const SETTINGS_PATH = join(CLAUDE_DIR, "settings.json");

const argv = process.argv.slice(2);
const mcpOnly = argv.includes("--mcp-only");
const scopeArg = argv.find((a) => a.startsWith("--scope="));
const scope = scopeArg ? scopeArg.slice("--scope=".length) : "user";
if (!["user", "project", "local"].includes(scope)) {
  console.error(`✗ unknown scope: ${scope} (expected user|project|local)`);
  process.exit(1);
}

function step(msg: string) { console.log(`→ ${msg}`); }
function done(msg: string) { console.log(`✓ ${msg}`); }
function fail(msg: string): never { console.error(`✗ ${msg}`); process.exit(1); }

function which(cmd: string): boolean {
  return spawnSync("which", [cmd], { stdio: "ignore" }).status === 0;
}

function run(cmd: string, args: string[], opts: Parameters<typeof spawnSync>[2] = {}) {
  return spawnSync(cmd, args, { stdio: "inherit", ...opts });
}

// 1. Preflight
if (!which("bun")) fail("bun not installed (https://bun.sh)");
if (!which("claude")) fail("claude CLI not installed (https://docs.claude.com/en/docs/claude-code)");
done("bun + claude CLI detected");

// 2. Install dependencies
step("installing dependencies");
{
  const r = run("bun", ["install"], { cwd: PROJECT_ROOT });
  if (r.status !== 0) fail("bun install failed");
}
done("dependencies installed");

// 3. Register MCP server (idempotent: remove then add)
step(`registering MCP server 'tunnel' (scope: ${scope})`);
spawnSync("claude", ["mcp", "remove", "tunnel", "-s", scope], { stdio: "ignore" });
{
  const r = run("claude", ["mcp", "add", "tunnel", "-s", scope, "--", "bun", "run", MCP_PATH]);
  if (r.status !== 0) fail("claude mcp add failed");
}
done(`MCP server registered (${MCP_PATH})`);

// 4. Stop hook
if (mcpOnly) {
  console.log("• skipped Stop hook (--mcp-only)");
} else {
  step(`installing Stop hook into ${SETTINGS_PATH}`);
  mkdirSync(CLAUDE_DIR, { recursive: true });
  let settings: any = {};
  if (existsSync(SETTINGS_PATH)) {
    const raw = readFileSync(SETTINGS_PATH, "utf8");
    try { settings = raw.trim() ? JSON.parse(raw) : {}; }
    catch { fail(`${SETTINGS_PATH} is not valid JSON; please fix or delete it and re-run`); }
  }
  settings.hooks ??= {};
  settings.hooks.Stop ??= [];

  const command = `bun run ${HOOK_PATH}`;
  let already = false;
  for (const entry of settings.hooks.Stop) {
    for (const h of entry.hooks ?? []) {
      if (h?.command === command) { already = true; break; }
    }
    if (already) break;
  }

  if (already) {
    done("Stop hook already present");
  } else {
    settings.hooks.Stop.push({
      matcher: "",
      hooks: [{ type: "command", command }],
    });
    writeFileSync(SETTINGS_PATH, JSON.stringify(settings, null, 2) + "\n");
    done("Stop hook added");
  }
}

// 5. Final summary
console.log(`
Setup complete.

  • MCP:        \`tunnel\` registered at ${scope} scope
  • Hook:       ${mcpOnly ? "(not installed; pass without --mcp-only to enable)" : "Stop hook → keeps registered tunnel sessions alive"}
  • Verify:     /mcp inside Claude Code, or  bun run bin/claude-tunnel.ts status
  • Uninstall:  bun run scripts/uninstall.ts

Restart any active Claude Code sessions for the MCP changes to take effect.
`);
