# Worker Guardian

Fleet manager daemon for heterogeneous worker processes with Postgres heartbeat monitoring.

## Quick Reference

- **Entry point**: `src/worker_guardian/cli.py` — Click CLI
- **Core loop**: `src/worker_guardian/daemon.py`
- **Design doc**: `docs/DESIGN.md` — full architecture with 6-round review history (2 reviewers)
- **Fleet config example**: `fleet-examples/poiverifier.yaml`
- **Python**: >=3.11, developed on 3.12
- **Build**: hatchling (`pip install -e .`)

## Architecture

One daemon per machine that:
1. Reads a YAML fleet config defining worker pools (types, counts, spawn commands)
2. Spawns workers as invisible background processes (`CREATE_NO_WINDOW`)
3. Monitors health via Postgres `llm_workers` heartbeat table every 12s (host-filtered)
4. Respawns dead workers automatically (wall-clock stale threshold: 90s)
5. Drains workers gracefully on shutdown via Postgres status column
6. Supports multi-machine: each guardian manages its own machine's workers only

Key design decisions:
- **Per-machine targets** — no fleet-wide auto-distribution, no cross-machine coordination
- **Pool locality** — `local` (orchestrator only: fetch, cluster) or `remote` (any machine: LLM)
- **Hold-steady circuit breaker** — fast-open, slow-close; suspends kills during DB partition
- **Post-partition grace** — extends stale threshold after reconnect to prevent mass kill
- **Guardian instances table** — shared Postgres table for global fleet visibility

See `docs/DESIGN.md` for full design (1800+ lines, 6 rounds adversarial review, 95% confidence).

## Key Files

| File | Purpose |
|------|---------|
| `src/worker_guardian/daemon.py` | Core loop: poll → health → spawn/kill → persist |
| `src/worker_guardian/config.py` | YAML loader, Pydantic validation, ${VAR} interpolation, enabled_when DSL |
| `src/worker_guardian/health.py` | Postgres heartbeat poller |
| `src/worker_guardian/spawner.py` | subprocess.Popen with CREATE_NO_WINDOW + log redirect |
| `src/worker_guardian/tracker.py` | PID tracking, state persistence, pidfile management |
| `src/worker_guardian/models.py` | PoolSpec, WorkerState, FleetState dataclasses |
| `src/worker_guardian/cli.py` | Click CLI: start, stop, status, reload |

## Conventions

- **No eval()** — `enabled_when` uses a restricted DSL parser, not Python eval
- **Atomic state writes** — write to `.tmp`, then `os.replace()`
- **Handle lifecycle** — close guardian's file handle copy immediately after Popen
- **Rate limiting** — MAX_SPAWN_PER_CYCLE=5 per pool, exponential backoff on rapid death
- **Degraded mode** — Postgres down → hold-steady circuit breaker (fast-open, slow-close)
- **Locality guard** — `local` pools only spawn on orchestrator machine
- **Host-filtered queries** — each guardian counts only its own machine's workers

## Don'ts

- Don't use `stdout=PIPE` for workers — causes deadlock under load with 28+ workers
- Don't use `CREATE_NEW_CONSOLE` — causes cascade crash when Windows Terminal dies
- Don't use `CTRL_BREAK_EVENT` for drain — doesn't work with `CREATE_NO_WINDOW`
- Don't use `eval()` for `enabled_when` — security risk from config files
- Don't use `psutil.Process.environ()` — unreliable across Windows user contexts
- Don't spawn without checking Postgres alive count first — prevents double-spawning
- Don't use `locality: any` — force explicit `local` or `remote` per pool
- Don't use fleet-wide target counts — each machine specifies its own targets
- Don't kill workers during hold-steady — when you can't observe, don't act

## Dependencies

- `psycopg[binary]>=3.2` — Postgres queries
- `pyyaml>=6.0` — fleet config parsing
- `click>=8.1` — CLI
- `psutil>=6.0` — PID liveness, create_time validation
- `pydantic>=2.7` — config validation

## Related Projects

- `C:\PROJECTS\llm-queue-manager` — queue primitives, worker loop, heartbeat (upstream dependency)
- `C:\PROJECTS\poiVerifier\V2\poiVerifier` — primary consumer, ships fleet config YAML
