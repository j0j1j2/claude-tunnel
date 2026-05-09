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

### Receiver (listener)

Tell the model to identify itself and start listening:

> register as tunnel agent **`coder`**, subscribe to channel **`tasks`**,
> then loop on `tunnel_inbox` to handle anything that comes in. Call
> `tunnel_leave` when I tell you we're done.

With the Stop hook installed, the receiver session will not be allowed
to stop until you say done. Without the hook, add `/loop 30s …` instead.

### Sender

> publish on **`tasks`** channel: `{ "task": "rename foo to bar" }`

…or for a 1:1 question that blocks until the answer comes back:

> use `tunnel_request` to ask agent **`coder`** "what's the status of X"

## Tools (12)

Identity:
- `tunnel_register(agent_id)` — claim an identity for this session
- `tunnel_leave()` — release this session (unregisters + clears inbox)
- `tunnel_who()` — list currently registered agent_ids
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
  Exits 60 s after the last client disconnects.
- **Stop hook** (`hooks/stop-block.ts`): runs on every Claude Code Stop
  event. If the current session has a session marker AND the agent has
  pending work or active subscriptions, returns
  `{"decision":"block","reason":"..."}` to keep the model running.
  Self-disables when the broker isn't running, so it's safe globally.

State is **in-memory only** — restarting the broker drops everything.
This matches the "not always on" design.

## CLI

```sh
bun run bin/claude-tunnel.ts status   # broker state, pid, paths
bun run bin/claude-tunnel.ts logs     # dump broker log
bun run bin/claude-tunnel.ts stop     # SIGTERM the broker
```

## Tests

```sh
bun run test/smoke.ts          # broker pub/sub, RPC, queue
bun run test/mcp-handshake.ts  # MCP stdio + tools list + auto-spawn
bun run test/hook.ts           # Stop hook block/allow scenarios
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
