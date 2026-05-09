#!/usr/bin/env bun
// Reverses scripts/install.ts. Removes the MCP entry from all scopes,
// strips the Stop hook from ~/.claude/settings.json, and stops the broker.
// Leaves ~/.claude-tunnel/{log,sessions} alone.

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";

const HERE = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(HERE, "..");
const HOOK_PATH = join(PROJECT_ROOT, "hooks", "stop-block.ts");
const CLI_PATH = join(PROJECT_ROOT, "bin", "claude-tunnel.ts");
const SETTINGS_PATH = join(homedir(), ".claude", "settings.json");

function step(msg: string) { console.log(`→ ${msg}`); }
function done(msg: string) { console.log(`✓ ${msg}`); }

// 1. claude mcp remove (try every scope; ignore failures)
step("removing 'tunnel' MCP entry from all scopes");
for (const scope of ["user", "project", "local"]) {
  spawnSync("claude", ["mcp", "remove", "tunnel", "-s", scope], { stdio: "ignore" });
}
done("MCP entries removed (where present)");

// 2. Strip Stop hook
if (existsSync(SETTINGS_PATH)) {
  step(`stripping Stop hook from ${SETTINGS_PATH}`);
  try {
    const settings = JSON.parse(readFileSync(SETTINGS_PATH, "utf8"));
    const stops: any[] = settings.hooks?.Stop ?? [];
    const command = `bun run ${HOOK_PATH}`;
    const filtered = stops
      .map((entry) => ({
        ...entry,
        hooks: (entry.hooks ?? []).filter(
          (h: any) => h?.command !== command && !h?.command?.endsWith("hooks/stop-block.ts"),
        ),
      }))
      .filter((entry) => (entry.hooks?.length ?? 0) > 0);
    if (settings.hooks) {
      if (filtered.length === 0) delete settings.hooks.Stop;
      else settings.hooks.Stop = filtered;
      if (Object.keys(settings.hooks).length === 0) delete settings.hooks;
    }
    writeFileSync(SETTINGS_PATH, JSON.stringify(settings, null, 2) + "\n");
    done("Stop hook removed");
  } catch (e) {
    console.error(`✗ could not edit settings.json: ${(e as Error).message}`);
  }
} else {
  console.log(`• no ${SETTINGS_PATH} (skipping)`);
}

// 3. Stop broker
step("stopping broker (if running)");
spawnSync("bun", ["run", CLI_PATH, "stop"], { stdio: "inherit" });
done("done");

console.log(`
Uninstall complete.

  ~/.claude-tunnel/{log,sessions} was left intact. Remove it manually if you don't need it:
  rm -rf ~/.claude-tunnel
`);
