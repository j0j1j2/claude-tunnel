# claude-tunnel

A lightweight, on-demand message broker for Claude Code sessions to talk to
each other on the same machine.

- **Three patterns** in one tool: pub/sub, request/reply, and job queue
- **Zero external services** — single Bun process speaking over a Unix socket
- **On-demand** — broker auto-spawns on first use and self-terminates after
  60 s of idle
- **Optional Stop hook** — keeps a registered tunnel session listening until
  it explicitly leaves, so the receiver actually checks its inbox

## Install

```sh
bun run scripts/install.ts
```

This will:

1. Run `bun install`
2. Register an MCP server named `tunnel` at `user` scope
   (`claude mcp add tunnel -s user -- bun run <abs>/src/mcp-server.ts`)
3. Add a Stop hook to `~/.claude/settings.json` so the receiving session
   stays alive until it calls `tunnel_leave`

Flags:

- `--mcp-only` — skip the Stop hook (you'll need `/loop` to keep the
  receiver polling)
- `--scope=project` or `--scope=local` — narrow the MCP scope
  (default is `user`, which makes it available in every project)

After install, **restart any open Claude Code sessions** so the MCP server
is picked up.

Verify:

```sh
claude mcp list                     # 'tunnel' should be listed
bun run bin/claude-tunnel.ts status
```

## Uninstall

```sh
bun run scripts/uninstall.ts
```

Removes the MCP entry from every scope, strips the Stop hook from
`~/.claude/settings.json`, and stops the broker.
`~/.claude-tunnel/` (logs, session markers) is left intact — `rm -rf` it
manually if you want a clean slate.

## Usage

In any two Claude Code sessions, just talk to the model. Tool descriptions
guide the rest.

### Receiver — recommended: Monitor (push delivery)

The cleanest way to receive is Claude Code's **Monitor** tool, which turns
each stdout line of a background command into a chat notification that
arrives on its own schedule — a real push. `claude-tunnel watch <agent>`
taps an agent's delivery stream and prints one JSON line per incoming
message, so it's purpose-built for Monitor.

Tell the receiver session:

> register as tunnel agent **`B`**, then start a persistent Monitor running
> `bun run <abs>/bin/claude-tunnel.ts watch B`. When a tunnel message shows
> up, handle it (reply to requests with `tunnel_reply`). Otherwise just work
> with me normally.

Now:

- The session stays **fully responsive to you** — the watch runs in the
  background; your messages are never queued behind an inbox poll.
- When another agent sends to `B`, the message **pops into the chat
  automatically** — no manual "go check your inbox".
- Sending still works: the session is registered as `B`, so it can
  `tunnel_publish` / `tunnel_request` as itself (bidirectional).

With Monitor you do **not** need the Stop hook — the session isn't trapped
in a poll loop. (If you installed it, it stays dormant: the hook
self-disables unless that session registered through the MCP server and has
pending work.)

### Receiver — alternative: poll loop + Stop hook

If you'd rather not use Monitor, make the session a dedicated listener:

> register as tunnel agent **`coder`**, subscribe to channel **`tasks`**,
> then loop on `tunnel_inbox` to handle anything that comes in. Stay
> connected — only `tunnel_leave` when I explicitly tell you to.

With the Stop hook installed, the session won't be allowed to stop while
registered or while messages are pending. Trade-off: it monopolizes the
session, so your own messages compete with the inbox loop. Without the
hook, add `/loop 30s …` to drive the loop instead.

**Keeping flighty agents from leaving (poll-loop mode).** The tool
descriptions and the Stop hook both tell the model not to `tunnel_leave`
on its own. If a model still bails too eagerly, hard-lock it:

```sh
claude mcp add tunnel -s user -e CLAUDE_TUNNEL_LOCK=1 -- bun run <abs>/src/mcp-server.ts
```

With `CLAUDE_TUNNEL_LOCK=1`, `tunnel_leave` becomes a no-op. To actually
disconnect, unset the var (re-register) or close the session.

### Sender

> publish on **`tasks`** channel: `{ "task": "rename foo to bar" }`

…or for a 1:1 question that blocks until the answer comes back:

> use `tunnel_request` to ask agent **`coder`** "what's the status of X"

## Tools (12)

Identity:
- `tunnel_register(agent_id)` — claim an identity. Auto-captures the
  session's `cwd`, `git_root`, `pid`, and `ppid` so other agents can
  discover peers by repo or working directory.
- `tunnel_leave()` — release this session (unregisters + clears inbox)
- `tunnel_who(scope?)` — list registered agents with full metadata.
  `scope` is `"machine"` (default), `"directory"` (same cwd as caller),
  or `"repo"` (same git_root as caller). Scoped queries require the
  caller to be registered.
- `tunnel_status(agent_id)` — pending message/request counts and subs

Pub/Sub:
- `tunnel_publish(channel, payload)` — broadcast (sender doesn't echo)
- `tunnel_subscribe(channel)` / `tunnel_unsubscribe(channel)`

Receive:
- `tunnel_inbox(wait_seconds, max)` — long-poll for messages addressed to
  this agent (pubs on subscribed channels + incoming requests)

1:1 RPC:
- `tunnel_request(to, payload, wait_seconds)` — block until target replies
- `tunnel_reply(request_id, payload)` — answer an incoming request

Job queue:
- `tunnel_enqueue(queue, payload)` — push (FIFO)
- `tunnel_dequeue(queue, wait_seconds)` — block-pop until a job arrives or
  timeout

## Architecture

```
Claude Code A ─┐                                    ┌─ Unix socket ─> Broker
               ├─ stdio ─> MCP server (per-session)─┤   (~/.claude-tunnel/sock)
Claude Code B ─┘                                    └─ Unix socket ─> Broker
```

- **MCP server** (`src/mcp-server.ts`): one process per Claude Code
  session. Stateless proxy. On first call, dials the broker and
  auto-spawns a detached broker if the socket is dead.
- **Broker** (`src/broker.ts`): single process. Holds in-memory state
  (registrations, subscriptions, inboxes, queues, pending requests).
  Exits 60 s after the last client disconnects. A reaper sweeps every
  30 s, dropping agents whose pid has died and removing stale session
  marker files.
- **Tap stream**: `claude-tunnel watch <agent>` opens a read-only tap on
  the broker. While a tap is active for an agent, the broker streams that
  agent's deliveries straight to the tap (one event per message) instead of
  buffering them in the inbox — so nothing accumulates. Registration stays
  exclusive and independent, so the same session can register through the
  MCP server (to send) while a `watch` process taps it (to receive).
- **Stop hook** (`hooks/stop-block.ts`): runs on every Claude Code Stop
  event. If the current session has a session marker AND the agent has
  pending work or active subscriptions, returns
  `{"decision":"block","reason":"..."}` to keep the model running.
  Self-disables when the broker isn't running, so it's safe globally.

State is **in-memory only** — restarting the broker drops everything.
This matches the "not always on" design.

## CLI

```sh
bun run bin/claude-tunnel.ts status        # broker state, pid, paths
bun run bin/claude-tunnel.ts logs          # dump broker log
bun run bin/claude-tunnel.ts stop          # SIGTERM the broker
bun run bin/claude-tunnel.ts watch <agent> # stream <agent>'s messages as
                                           # JSON lines (run under Monitor)
```

## Tests

```sh
bun run test/smoke.ts          # broker pub/sub, RPC, queue
bun run test/mcp-handshake.ts  # MCP stdio + tools list + auto-spawn
bun run test/hook.ts           # Stop hook block/allow scenarios
bun run test/peers.ts          # peer metadata + scope filter + reaper
bun run test/watch.ts          # tap stream + bidirectional send/receive
bun run test/leave-lock.ts     # CLAUDE_TUNNEL_LOCK refuses tunnel_leave
```

The first run auto-spawns the broker. Subsequent runs reuse it.

## Troubleshooting

**`claude mcp list` doesn't show `tunnel`** — Did you restart Claude Code
after install? MCP entries are loaded at session start.

**Receiver doesn't see messages** — Confirm both sides registered
(`tunnel_who`). Confirm the receiver subscribed to the same channel
the sender publishes to. For 1:1 use `tunnel_request` instead — it
bypasses subscriptions.

**Receiver session keeps stopping** — Did you run `install.ts` without
`--mcp-only`? Run it again. Verify `~/.claude/settings.json` has a Stop
hook entry pointing to `hooks/stop-block.ts`.

**Hook seems to block forever** — Call `tunnel_leave` to deregister.
The hook only blocks while the agent is registered or has pending work.

**Stale Unix socket** — `bun run bin/claude-tunnel.ts stop` then retry;
the broker auto-recovers stale sockets on next spawn.
