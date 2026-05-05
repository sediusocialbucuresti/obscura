# B2B Fallback Orchestrator

This document describes the local fallback monitor requested for the B2B pilot.

## Scope

The fallback monitor runs inside a dedicated zellij session named `b2b-fallback-orchestrator`.
It keeps the generated static website available, reruns the B2B export every 5 hours, restarts the local static server after each scheduled export, and writes a handoff file for an operator or Codex fallback.

It cannot monitor this chat's token budget. zellij is a terminal multiplexer and has no access to the parent ChatGPT/Codex conversation token counter.

## Fallback Model

Use GPT-5.3-Codex-Spark for a manual fallback session if the main chat stops:

```sh
codex --model gpt-5.3-codex-spark --cd /root/obscura --sandbox danger-full-access --ask-for-approval never "Continue B2B orchestration from /root/obscura/data/b2b/FALLBACK_HANDOFF.md. First read docs/b2b-gleif-10k-pilot.md and check git status."
```

The model name is also written into the watchdog state and handoff files.

## Start Monitor

Create the zellij session in the background:

```sh
zellij attach -b b2b-fallback-orchestrator
```

Start the watchdog pane:

```sh
ZELLIJ_SESSION_NAME=b2b-fallback-orchestrator zellij run \
  --name b2b-watchdog \
  --cwd /root/obscura \
  -- bash -lc './tools/b2b_watchdog.sh'
```

Attach to inspect it:

```sh
zellij attach b2b-fallback-orchestrator
```

## Files

Runtime files are written under `data/b2b/`:

```text
data/b2b/FALLBACK_HANDOFF.md
data/b2b/fallback-orchestrator-state.json
data/b2b/logs/fallback-orchestrator.log
data/b2b/logs/static-site-server.log
data/b2b/site-server-8080.pid
```

These files are generated runtime state and are ignored by Git.

## Defaults

```text
PORT=8080
INTERVAL_SECONDS=18000
HEALTH_INTERVAL_SECONDS=60
FALLBACK_MODEL=gpt-5.3-codex-spark
SITE_BASE_URL=http://127.0.0.1:8080
RUN_EXPORT_ON_START=0
TAKE_OWNERSHIP=1
```

Override any value when launching the watchdog:

```sh
ZELLIJ_SESSION_NAME=b2b-fallback-orchestrator zellij run \
  --name b2b-watchdog \
  --cwd /root/obscura \
  -- bash -lc 'SITE_BASE_URL=https://sediusocialbucuresti.github.io/obscura INTERVAL_SECONDS=18000 HEALTH_INTERVAL_SECONDS=30 ./tools/b2b_watchdog.sh'
```

## Health Checks

Check the website:

```sh
curl -I http://127.0.0.1:8080/
```

Watch the monitor log:

```sh
tail -f /root/obscura/data/b2b/logs/fallback-orchestrator.log
```

Read current state:

```sh
cat /root/obscura/data/b2b/fallback-orchestrator-state.json
```
