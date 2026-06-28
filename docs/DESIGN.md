# Worker Guardian — Design Document

Version: 2.0 | Reviewed: 6 rounds adversarial (2 independent reviewers × 3 rounds) | Confidence: 95%

## 1. Problem Statement

The poiVerifier project operates a heterogeneous fleet of worker processes — LLM
inference (DeepSeek, DeepInfra, OpenAI, OpenRouter), browser fetch (Playwright),
Scrapfly fetch, and cluster compute (spaCy + GNAF). Three problems:

1. **No respawn on death.** Workers are fire-and-forget via `subprocess.Popen`. If
   a worker crashes (OOM, Chromium segfault, unhandled exception), nothing replaces
   it. The fleet silently degrades until the operator notices stale heartbeats.

2. **Console cascade crash.** Every worker is spawned with `CREATE_NEW_CONSOLE`,
   giving each its own visible console window. When Windows Terminal crashes or is
   closed, all attached console windows die together — a single point of failure
   that kills the entire fleet.

3. **Manual fleet management.** Target worker counts, environment variables, stagger
   intervals, and conditional pool logic are hardcoded across `startup.py`, `config.py`,
   and seven `.bat` files. Changing the fleet composition requires code edits and a
   restart of `program.py`.

## 2. Solution Overview

A standalone pip-installable project that acts as a background daemon process manager.
It reads a declarative YAML fleet configuration, spawns workers as invisible background
processes (`CREATE_NO_WINDOW`), monitors their health via the existing `llm_workers`
Postgres heartbeat table, and automatically respawns dead workers.

Designed for **multi-machine deployment**: one guardian per machine, each managing its
own local workers with per-machine targets. Health queries filter by `worker_host` to
prevent cross-machine interference. LLM workers go remote first (API-only, zero local
dependencies); fetch/cluster workers stay local until blob store migration (Phase 2).

```
Machine A (orchestrator)              Machine B (remote node)
┌──────────────────────┐              ┌──────────────────────┐
│ guardian-A            │              │ guardian-B            │
│ fleet-a.yaml         │              │ fleet-b.yaml         │
│ (all pool types)     │              │ (remote pools only)  │
│ spawns local workers  │              │ spawns local workers  │
│ monitors by host="A"  │              │ monitors by host="B"  │
└──────────┬───────────┘              └──────────┬───────────┘
           │                                      │
           └──────────┬───────────────────────────┘
                      │
               ┌──────┴──────┐
               │  Postgres   │
               │ (central)   │
               │ llm_workers │
               │ guardian_instances │
               └─────────────┘
```

### 2.1. Dependency Direction

```
llm-queue-manager   (library: queue primitives, worker loop, heartbeat)
       ↑
worker-guardian     (operator tool: fleet management, spawn/monitor/respawn)
       ↑
poiVerifier         (application: ships fleet config YAML defining desired workers)
```

The guardian imports nothing from poiVerifier. It reads only the YAML config and
queries Postgres. poiVerifier ships a `fleet.yaml` that the guardian reads by path.

### 2.2. Shared State

The guardian's external dependencies are minimal:

| Resource | Access | Purpose |
|----------|--------|---------|
| Postgres `llm_workers` table | Read + Write (status) | Health monitoring, drain signal |
| Postgres `guardian_instances` table | Read + Write | Guardian identity, heartbeat, fleet summary |
| Fleet YAML file | Read | Desired fleet specification |
| State directory | Read/Write | PID tracking, pidfiles, guardian lock |
| Log directory | Write | Worker stdout/stderr logs |

The guardian does NOT touch: SQLite databases, blob stores, browser state, or any
application-level data. It does NOT manage Postgres/PgBouncer Docker containers.

---

## 3. Project Structure

```
C:\PROJECTS\worker-guardian\
    pyproject.toml              # hatchling build, console_script entry point
    CLAUDE.md                   # Project conventions for AI assistants
    docs\
        DESIGN.md               # This document
    src\worker_guardian\
        __init__.py             # Package version
        cli.py                  # Click CLI: start, stop, status, reload
        config.py               # YAML loader + Pydantic validation + interpolation
        daemon.py               # Core loop: poll → health → spawn/kill → persist
        health.py               # Postgres heartbeat poller
        spawner.py              # subprocess.Popen wrapper with CREATE_NO_WINDOW
        tracker.py              # PID tracking, state persistence, pidfile management
        models.py               # Dataclasses: PoolSpec, WorkerState, FleetState
    tests\
        test_config.py          # YAML loading, interpolation, enabled_when DSL
        test_health.py          # Postgres mock, stale detection
        test_spawner.py         # Spawn + handle lifecycle
        test_tracker.py         # State persistence, pidfile, PID recycling
        test_daemon.py          # Core loop integration
    fleet-examples\
        poiverifier.yaml        # Reference fleet config for the poiVerifier project
```

---

## 4. Fleet Config Schema

The fleet config is a YAML file that the consumer project (poiVerifier) ships. The
guardian reads it by path. This decouples the guardian from any specific project.

### 4.1. Full Example

```yaml
version: 1

guardian:
  host: auto                           # resolved via socket.gethostname(); set explicitly for multi-machine
  orchestrator_host: "WORKSTATION-A"   # which machine runs poiVerifier (locality: local pools run here only)
  database_url: "postgresql://llm_queue:password@192.168.1.100:6432/llm_queue?sslmode=require"

defaults:
  log_dir: "C:\\tmp\\worker-guardian\\logs"
  stale_threshold_seconds: 90
  poll_interval_seconds: 12
  state_dir: "%LOCALAPPDATA%\\worker-guardian"
  max_log_dir_bytes: 2147483648    # 2 GB total log cap
  log_retention_days: 7
  drain_grace_seconds: 90

pools:
  deepseek:
    locality: remote                  # API-only, can run on any machine
    enabled_when: "INFERENCE_MODE == 'openrouter'"
    target_count: 28                  # per THIS machine, not fleet-wide
    stagger_seconds: 1.5
    heartbeat_model: "deepseek-v4-flash"
    spawn:
      command: ["start_worker_deepseek.bat", "${WORKER_ID}"]
      cwd: "C:\\PROJECTS\\poiVerifier\\V2\\poiVerifier"
      env:
        OPENAI_API_KEY: "${DEEPSEEK_KEY}"
        LLM_QUEUE_OPENAI_BASE_URL: "https://api.deepseek.com"
        LLM_QUEUE_WORKER_MODEL: "deepseek-v4-flash"
        LLM_QUEUE_WORKER_SERVED_MODELS: "deepseek-v4-flash"

  deepinfra:
    locality: remote
    enabled_when: "INFERENCE_MODE == 'deepinfra'"
    target_count: 28
    stagger_seconds: 1.5
    heartbeat_model: "${DEEPINFRA_MODEL}"
    spawn:
      command: ["start_worker_deepinfra.bat", "${WORKER_ID}"]
      cwd: "C:\\PROJECTS\\poiVerifier\\V2\\poiVerifier"
      env:
        OPENAI_API_KEY: "${DEEPINFRA_KEY}"
        LLM_QUEUE_OPENAI_BASE_URL: "https://api.deepinfra.com/v1/openai"
        LLM_QUEUE_WORKER_MODEL: "${DEEPINFRA_MODEL}"
        LLM_QUEUE_WORKER_SERVED_MODELS: "${DEEPINFRA_MODEL},deepseek-ai/DeepSeek-V4-Flash"

  openai:
    locality: remote
    enabled_when: "INFERENCE_MODE == 'openai'"
    target_count: 32
    stagger_seconds: 1.5
    heartbeat_model: "gpt-4o-mini"
    spawn:
      command: ["start_worker_openai.bat", "${WORKER_ID}"]
      cwd: "C:\\PROJECTS\\poiVerifier\\V2\\poiVerifier"
      env:
        OPENAI_API_KEY: "${OPENAI_KEY}"

  openrouter:
    locality: remote
    enabled_when: "INFERENCE_MODE == 'openrouter'"
    target_count: 10
    stagger_seconds: 1.5
    heartbeat_model: "meta-llama/llama-3.1-8b-instruct"
    spawn:
      command: ["start_worker_openrouter.bat", "${WORKER_ID}"]
      cwd: "C:\\PROJECTS\\poiVerifier\\V2\\poiVerifier"
      env:
        OPENAI_API_KEY: "${OPENROUTER_KEY}"
        LLM_QUEUE_OPENAI_BASE_URL: "https://openrouter.ai/api/v1"
        LLM_QUEUE_WORKER_MODEL: "meta-llama/llama-3.1-8b-instruct"
        LLM_QUEUE_WORKER_SERVED_MODELS: "meta-llama/llama-3.1-8b-instruct"

  fetch:
    locality: local               # needs browser + blob_dir + display
    target_count: 3
    stagger_seconds: 2.5
    heartbeat_model: "task:browser_fetch"
    spawn:
      command: ["${PYTHON_EXE}", "-m", "services.queue.fetch_worker",
                "--worker-id", "${WORKER_ID}", "--max-concurrency", "4"]
      cwd: "C:\\PROJECTS\\poiVerifier\\V2\\poiVerifier"
      env:
        FETCH_USE_QUEUE: "0"
        SCRAPFLY_USE_QUEUE: "0"
        PYTHONIOENCODING: "utf-8"

  scrapfly:
    locality: local               # needs blob_dir for results
    target_count: 4
    stagger_seconds: 0.5
    heartbeat_model: "task:scrapfly_fetch"
    spawn:
      command: ["${PYTHON_EXE}", "-m", "services.queue.scrapfly_worker",
                "--worker-id", "${WORKER_ID}", "--max-concurrency", "4"]
      cwd: "C:\\PROJECTS\\poiVerifier\\V2\\poiVerifier"
      env:
        FETCH_USE_QUEUE: "0"
        SCRAPFLY_USE_QUEUE: "0"
        PYTHONIOENCODING: "utf-8"

  cluster:
    locality: local               # needs GNAF SQLite databases
    target_count: 8
    stagger_seconds: 1.0
    heartbeat_model: "task:cluster_cpu"
    spawn:
      command: ["${PYTHON_EXE}", "-m", "services.queue.cluster_worker",
                "--worker-id", "${WORKER_ID}"]
      cwd: "C:\\PROJECTS\\poiVerifier\\V2\\poiVerifier"
      env:
        PYTHONIOENCODING: "utf-8"
```

### 4.2. Variable Interpolation

The `${VAR_NAME}` syntax in command arrays and env values is resolved at spawn time:

| Variable | Resolution |
|----------|-----------|
| `${PYTHON_EXE}` | `sys.executable` — the Python that launched the guardian |
| `${WORKER_ID}` | Auto-generated: `{pool_name}-{index:02d}-{hostname}-{guardian_pid}` |
| `${VENV_SCRIPTS}` | `os.path.dirname(sys.executable)` |
| `%LOCALAPPDATA%` | Windows environment variable expansion (in `state_dir` only) |
| Any other `${NAME}` | `os.environ[NAME]` at spawn time |

Missing variables: the pool is skipped with a warning log, not a daemon crash.
Variable values are resolved fresh on every spawn (not cached), so changing an env
var and respawning picks up the new value.

### 4.3. Conditional Pools (`enabled_when`)

Evaluated as a restricted DSL — NOT `eval()`. The parser converts expressions into
a small AST of comparison nodes:

```
enabled_when: "INFERENCE_MODE == 'openrouter'"
enabled_when: "INFERENCE_MODE != 'local'"
enabled_when: "INFERENCE_MODE in ('deepinfra', 'openai')"
enabled_when: "CLUSTER_USE_QUEUE != '0' and INFERENCE_MODE == 'deepinfra'"
```

Implementation (`config.py`):
- Tokenize on whitespace, `==`, `!=`, `in`, `and`, `or`, `(`, `)`, quoted strings.
- Build AST: `EnvEquals(var, val)`, `EnvNotEquals(var, val)`, `EnvIn(var, [vals])`,
  `And(left, right)`, `Or(left, right)`.
- Evaluate: each node resolves `os.environ.get(var, "")` and compares.
- No arbitrary code execution. Unknown operators → parse error at config load time.

Evaluated once per health check cycle (every 12s). Changing an env var activates/
deactivates pools dynamically.

### 4.4. Guardian Section

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | `auto` | Guardian identity. `auto` = `socket.gethostname()`. Set explicitly for multi-machine (REQUIRED to avoid hostname collisions) |
| `orchestrator_host` | string | null | Hostname of the machine running poiVerifier. Pools with `locality: local` only spawn when `host == orchestrator_host` |
| `database_url` | string | required | Postgres DSN. Use `sslmode=require` for remote connections |

### 4.5. Defaults Section

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `log_dir` | string | required | Root directory for worker log files |
| `stale_threshold_seconds` | int | 90 | Seconds without heartbeat before worker is declared stale. Invariant: `heartbeat_interval * 2 < stale_threshold` |
| `poll_interval_seconds` | int | 12 | Health check cycle interval |
| `state_dir` | string | `%LOCALAPPDATA%\worker-guardian` | Directory for state file, pidfiles, guardian lock |
| `max_log_dir_bytes` | int | 2147483648 (2GB) | Total log directory size cap |
| `log_retention_days` | int | 7 | Delete log files older than this |
| `drain_grace_seconds` | int | 90 | Wait time after drain signal before TerminateProcess |

### 4.6. Pool Spec Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | true | Static toggle |
| `enabled_when` | string | null | Dynamic condition (DSL expression) |
| `locality` | string | `local` | `local` (orchestrator machine only) or `remote` (any machine). No `any` — force explicit choice |
| `target_count` | int | 1 | Desired number of workers **for this machine** (per-machine, not fleet-wide) |
| `stagger_seconds` | float | 1.5 | Delay between consecutive worker spawns |
| `heartbeat_model` | string | required | `model` value in `llm_workers` for health check |
| `spawn.command` | list[str] | required | argv for `subprocess.Popen` |
| `spawn.cwd` | string | null | Working directory (null = inherit guardian's) |
| `spawn.env` | dict[str,str] | {} | Additional env vars merged onto `os.environ` |

---

## 5. Module Specifications

### 5.1. `models.py` — Data Types

```python
from dataclasses import dataclass, field
from typing import Any

@dataclass
class SpawnSpec:
    """How to launch a worker process."""
    command: list[str]                  # argv; supports ${VAR} interpolation
    cwd: str | None = None              # working directory; None = inherit
    env: dict[str, str] = field(default_factory=dict)  # merged onto os.environ at spawn

@dataclass
class PoolSpec:
    """Desired state for one worker pool."""
    name: str                           # pool identifier (key in YAML)
    heartbeat_model: str                # model value in llm_workers for health check
    spawn: SpawnSpec
    enabled: bool = True                # static toggle
    enabled_when: str | None = None     # dynamic condition (DSL expression)
    locality: str = "local"             # "local" (orchestrator only) or "remote" (any machine)
    target_count: int = 1               # per THIS machine, not fleet-wide
    stagger_seconds: float = 1.5
    log_prefix: str = ""                # defaults to pool name

@dataclass
class WorkerState:
    """Runtime state of one managed worker."""
    pool_name: str
    worker_id: str                      # e.g. "fetch-02-12345"
    pid: int | None = None
    proc: Any = None                    # subprocess.Popen object (not serialized)
    started_at: float = 0.0             # epoch seconds
    status: str = "starting"            # starting | running | stale | dead

@dataclass
class PoolState:
    """Runtime state of one pool."""
    name: str
    workers: dict[str, WorkerState] = field(default_factory=dict)
    rapid_death_count: int = 0          # consecutive deaths within 10s of spawn
    backoff_until: float = 0.0          # epoch; no spawning before this time
    spawn_failure_count: int = 0        # consecutive Popen failures
    config_hash: str = ""               # hash of spawn spec for backoff reset

@dataclass
class FleetState:
    """Full runtime state of the guardian."""
    guardian_pid: int = 0
    pools: dict[str, PoolState] = field(default_factory=dict)

@dataclass
class GuardianConfig:
    """Identity and coordination settings for this guardian instance."""
    host: str                           # resolved hostname (from "auto" or explicit)
    orchestrator_host: str | None = None # hostname of the orchestrator machine
    database_url: str = ""              # Postgres DSN

@dataclass
class FleetConfig:
    """Parsed + validated fleet configuration."""
    guardian: GuardianConfig            # multi-machine identity
    log_dir: str
    state_dir: str
    stale_threshold_seconds: int = 90
    poll_interval_seconds: int = 12
    max_log_dir_bytes: int = 2_147_483_648
    log_retention_days: int = 7
    drain_grace_seconds: int = 90
    pools: list[PoolSpec] = field(default_factory=list)
```

### 5.2. `config.py` — Configuration Loader

**Responsibilities:**
1. Load YAML file into Pydantic model for validation
2. Resolve `%ENVVAR%` in `state_dir` (Windows-style expansion)
3. Parse `enabled_when` expressions into AST nodes
4. Compute config hash per pool (for backoff reset detection)

**Key functions:**

```python
def load_config(path: str) -> FleetConfig:
    """Load + validate fleet YAML. Raises on invalid schema or unparseable DSL."""

def evaluate_enabled_when(expression: str) -> bool:
    """Evaluate an enabled_when DSL expression against os.environ.
    Returns True if expression is None (always enabled)."""

def interpolate_vars(template: str, worker_id: str) -> str:
    """Resolve ${VAR} references in a string. Special vars:
    PYTHON_EXE, WORKER_ID, VENV_SCRIPTS. Others from os.environ.
    Raises KeyError for missing required vars."""

def pool_config_hash(pool: PoolSpec) -> str:
    """SHA-256 of (command, cwd, env keys+values). Used to detect config
    changes that should reset respawn backoff."""
```

**enabled_when DSL implementation:**

```python
import re
from dataclasses import dataclass

@dataclass
class EnvEquals:
    var: str
    val: str
    def evaluate(self) -> bool:
        return os.environ.get(self.var, "") == self.val

@dataclass
class EnvNotEquals:
    var: str
    val: str
    def evaluate(self) -> bool:
        return os.environ.get(self.var, "") != self.val

@dataclass
class EnvIn:
    var: str
    vals: list[str]
    def evaluate(self) -> bool:
        return os.environ.get(self.var, "") in self.vals

@dataclass
class And:
    left: "Node"
    right: "Node"
    def evaluate(self) -> bool:
        return self.left.evaluate() and self.right.evaluate()

@dataclass
class Or:
    left: "Node"
    right: "Node"
    def evaluate(self) -> bool:
        return self.left.evaluate() or self.right.evaluate()

def parse_enabled_when(expr: str) -> "Node":
    """Tokenize and parse into AST. Raises ValueError on invalid syntax."""
    # Tokenizer: split on whitespace preserving quoted strings
    # Parser: recursive descent for and/or with ==, !=, in as atoms
```

### 5.3. `health.py` — Postgres Health Poller

**Responsibilities:**
1. Connect to Postgres (PgBouncer on 6432 for queries)
2. Query `llm_workers` for heartbeat data per pool
3. Detect stale workers (wall-clock threshold)
4. Set drain status on shutdown
5. Handle Postgres unavailability gracefully

**Key functions:**

```python
class HealthPoller:
    def __init__(self, database_url: str):
        self._dsn = database_url
        self._conn: psycopg.Connection | None = None
        self._last_connect_attempt: float = 0.0
        self._consecutive_failures: int = 0

    def connect(self) -> bool:
        """Establish or re-establish Postgres connection.
        Returns False if connection fails (caller should enter degraded mode)."""

    def count_alive(self, heartbeat_model: str, worker_host: str, stale_seconds: int = 90) -> int | None:
        """Count workers with fresh heartbeats for the given model ON THIS HOST.
        Filters by worker_host to prevent cross-machine interference.
        Returns None if Postgres is unreachable (degraded mode signal)."""
        # SQL:
        # SELECT COUNT(*) FROM llm_workers
        #  WHERE model = %s
        #    AND worker_host = %s
        #    AND last_heartbeat_at > NOW() - INTERVAL '%s seconds'

    def get_worker_heartbeats(self, heartbeat_model: str, worker_host: str) -> list[dict] | None:
        """Fetch individual worker heartbeat data for this host.
        Returns list of {worker_id, last_heartbeat_at, status} or None."""
        # SQL:
        # SELECT worker_id, last_heartbeat_at, status
        #   FROM llm_workers
        #  WHERE model = %s AND worker_host = %s

    def set_draining(self, worker_ids: list[str]) -> int:
        """Set status='draining' for the given worker IDs. Returns rows affected.
        Used during graceful shutdown to signal workers to self-drain."""
        # SQL:
        # UPDATE llm_workers
        #    SET status = 'draining'
        #  WHERE worker_id = ANY(%s)
        #    AND status NOT IN ('draining', 'offline')

    def is_connected(self) -> bool:
        """Check if we have a healthy Postgres connection."""
```

**Postgres unavailability behavior:**
- If `connect()` or any query fails, set `_consecutive_failures += 1`.
- Log a warning every 60s (rate-limited, not every 12s poll).
- Return `None` from `count_alive()` / `get_worker_heartbeats()`.
- Caller (daemon.py) interprets `None` as "skip health checks AND spawning".
- PID liveness checks via `proc.poll()` continue (local, no Postgres needed).
- On next successful query, reset `_consecutive_failures = 0`.
- Connection retry uses exponential backoff: `min(60, 5 * 2^failures)` seconds.

### 5.4. `spawner.py` — Process Spawner

**Responsibilities:**
1. Resolve `${VAR}` references in command and env
2. Build merged environment (guardian env + pool env + venv PATH)
3. Open log file, spawn process with `CREATE_NO_WINDOW`
4. Close guardian's file handle copy immediately after spawn
5. Return `(Popen, worker_id)` tuple

**Key functions:**

```python
CREATE_NO_WINDOW = 0x08000000

def spawn_worker(
    pool: PoolSpec,
    worker_index: int,
    guardian_pid: int,
    guardian_host: str,
    log_dir: str,
) -> tuple[subprocess.Popen, str]:
    """Spawn a single worker process.

    Returns (proc, worker_id).
    Raises SpawnError on failure (file not found, permission denied, etc.).
    """
    worker_id = f"{pool.name}-{worker_index:02d}-{guardian_host}-{guardian_pid}"

    # 1. Resolve command template
    resolved_cmd = [interpolate_vars(arg, worker_id) for arg in pool.spawn.command]

    # 2. Build environment
    merged_env = os.environ.copy()

    # Ensure venv Scripts dir is on PATH
    venv_scripts = os.path.dirname(sys.executable)
    current_path = merged_env.get("PATH", "")
    if venv_scripts not in current_path:
        merged_env["PATH"] = venv_scripts + os.pathsep + current_path
    merged_env["VIRTUAL_ENV"] = os.path.dirname(venv_scripts)

    # Overlay pool-specific env vars (resolved)
    for key, val_template in pool.spawn.env.items():
        merged_env[key] = interpolate_vars(val_template, worker_id)

    # 3. Open log file (date-suffixed)
    log_subdir = os.path.join(log_dir, pool.name)
    os.makedirs(log_subdir, exist_ok=True)
    date_str = time.strftime("%Y-%m-%d")
    log_path = os.path.join(log_subdir, f"{pool.name}_{worker_index:02d}_{date_str}.log")
    log_fh = open(log_path, "a", encoding="utf-8")

    # 4. Spawn process
    try:
        proc = subprocess.Popen(
            resolved_cmd,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            cwd=pool.spawn.cwd,
            env=merged_env,
            creationflags=CREATE_NO_WINDOW,
            close_fds=False,  # explicit: ensure stdout handle inheritance on Windows
            shell=False,
        )
    except Exception:
        log_fh.close()
        raise

    # 5. Close guardian's copy of the file handle.
    #    The child process keeps its own copy via OS handle inheritance
    #    (handle was duplicated into child's handle table at CreateProcess time).
    #    Closing the guardian's copy prevents:
    #    - File handle leaks in the guardian process
    #    - Sharing violations on respawn (opening the same file again)
    log_fh.close()

    return proc, worker_id
```

**Why `close_fds=False`:**
On Python 3.7+/Windows, `close_fds` defaults to `True` when redirecting standard
handles. With `True`, standard handle redirection still works (Python sets up
`STARTUPINFO.hStdOutput`), but `close_fds=False` is explicit defense — it ensures
no ambiguity about handle inheritance behavior across Python minor versions.

### 5.5. `tracker.py` — Process Tracker

**Responsibilities:**
1. Track `(worker_id, Popen, pid, started_at)` in memory
2. Persist state to JSON file (atomic write)
3. Manage worker pidfiles
4. Rediscover orphaned workers on guardian restart

**Key functions:**

```python
class ProcessTracker:
    def __init__(self, state_dir: str):
        self._state_dir = state_dir
        self._state_path = os.path.join(state_dir, "state.json")
        self._pids_dir = os.path.join(state_dir, "pids")
        self._fleet_state = FleetState()
        os.makedirs(self._pids_dir, exist_ok=True)

    def add_worker(self, pool_name: str, worker_id: str, proc: subprocess.Popen):
        """Register a newly spawned worker."""
        # Add to in-memory state
        # Write pidfile: {pids_dir}/{worker_id}.pid containing "pid\ncreate_time"

    def remove_worker(self, worker_id: str):
        """Remove a dead worker from tracking. Delete its pidfile."""

    def is_alive(self, worker_id: str) -> bool:
        """Check if the worker's OS process is still running.
        Uses proc.poll() for in-memory tracked workers.
        Returns False if proc is None (adopted orphan with no Popen)."""

    def kill_worker(self, worker_id: str):
        """Terminate the worker process. Uses proc.terminate() (TerminateProcess
        on Windows). Waits up to 5s for exit, then proc.kill() as last resort."""

    def persist_state(self):
        """Write state to JSON file atomically (write to .tmp, os.replace)."""
        # Content: {"guardian_pid": N, "workers": {"id": {"pid": N, "pool": "...", "started_at": T}}}

    def load_state(self) -> bool:
        """Load state from JSON file. Returns True if loaded, False if
        missing/corrupt (caller should fall back to Postgres-only rediscovery)."""

    def rediscover_orphans(self, active_pools: list[PoolSpec]) -> dict[str, int]:
        """On guardian restart: rediscover workers from prior instance.

        Strategy:
        1. Read pidfiles from {pids_dir}/*.pid
        2. For each: parse pid + create_time
        3. Validate: psutil.Process(pid).create_time() matches within 2s
        4. If match: adopt (add to state with status='running', no Popen object)
        5. If no match: delete stale pidfile

        Returns: {pool_name: adopted_count}

        Note: adopted workers have no Popen object, so is_alive() falls back
        to psutil.pid_exists(). kill_worker() uses psutil.Process(pid).terminate().
        """

    def get_pool_workers(self, pool_name: str) -> list[WorkerState]:
        """List all tracked workers for a pool."""

    def get_next_worker_index(self, pool_name: str) -> int:
        """Find the next available index for a new worker in the pool.
        Fills gaps from dead workers (e.g., if 00 and 02 are alive, returns 01)."""
```

**Pidfile format** (`{state_dir}/pids/{worker_id}.pid`):
```
23456
1719561600.0
```
Line 1: PID. Line 2: `time.time()` at spawn. Workers write their own pidfile on
startup (3 lines added to worker init). The guardian also writes a pidfile on spawn
as a fallback (in case the worker crashes before it writes its own).

**PID recycling protection:**
- In-memory: the `Popen` object holds the Windows process handle, which prevents
  PID recycling for the guardian's lifetime. `proc.poll()` is always accurate.
- On-disk (guardian restart): compare `psutil.Process(pid).create_time()` with
  the stored `create_time`. Match within 2s → same process. No match → PID was
  recycled, delete stale pidfile.

**State file** (`{state_dir}/state.json`):
```json
{
  "guardian_pid": 12345,
  "started_at": 1719561600.0,
  "workers": {
    "deepseek-00-12345": {"pid": 23456, "pool": "deepseek", "started_at": 1719561600.0},
    "fetch-01-12345": {"pid": 23789, "pool": "fetch", "started_at": 1719561605.0}
  }
}
```
Atomic write: write to `state.json.tmp`, then `os.replace("state.json.tmp", "state.json")`.
Corrupt or missing on startup → Postgres + pidfiles provide full rediscovery.

### 5.6. `daemon.py` — Core Loop

**Responsibilities:**
1. Initialize config, state, health poller, single-instance lock
2. Run the main health-check/spawn/kill loop
3. Handle graceful shutdown
4. Handle config hot-reload
5. Manage log pruning

**Core loop pseudocode:**

```python
def run(config_path: str, foreground: bool = True):
    # ── Initialization ──
    config = load_config(config_path)
    ensure_state_dir(config.state_dir)
    acquire_single_instance_lock(config.state_dir)  # exits if lock held

    tracker = ProcessTracker(config.state_dir)
    health = HealthPoller(config.database_url)
    health.connect()

    # Rediscover orphans from prior guardian instance
    tracker.rediscover_orphans(config.pools)

    shutdown_requested = False
    config_mtime = os.path.getmtime(config_path)
    last_log_prune = 0.0

    # ── Signal handlers ──
    def on_shutdown(sig, frame):
        nonlocal shutdown_requested
        shutdown_requested = True

    signal.signal(signal.SIGINT, on_shutdown)
    signal.signal(signal.SIGTERM, on_shutdown)
    # On Windows, also handle SIGBREAK
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, on_shutdown)

    # ── Main loop ──
    while not shutdown_requested:
        cycle_start = time.time()

        # 1. Check config reload (file mtime change or reload sentinel)
        try:
            current_mtime = os.path.getmtime(config_path)
            reload_sentinel = os.path.join(config.state_dir, "reload_requested")
            reload_requested = (
                current_mtime != config_mtime or
                os.path.exists(reload_sentinel)
            )
            if reload_requested:
                try:
                    new_config = load_config(config_path)
                    reconcile_config(tracker, config, new_config)
                    config = new_config
                    config_mtime = current_mtime
                    if os.path.exists(reload_sentinel):
                        os.remove(reload_sentinel)
                    log.info("Config reloaded successfully")
                except Exception as e:
                    log.error("Config reload failed, keeping old config: %s", e)
        except Exception:
            pass  # config file temporarily unavailable

        # 2. Evaluate which pools are active
        active_pools = [
            p for p in config.pools
            if p.enabled and (
                p.enabled_when is None or
                evaluate_enabled_when(p.enabled_when)
            )
        ]
        active_pool_names = {p.name for p in active_pools}

        # 3. Per-pool health check + reconciliation
        for pool in active_pools:
            # 3a. Query Postgres
            alive_count = health.count_alive(
                pool.heartbeat_model, config.stale_threshold_seconds
            )
            if alive_count is None:
                # Postgres is down — skip this pool (degraded mode)
                continue

            # 3b. Check PID liveness for tracked workers
            for worker in tracker.get_pool_workers(pool.name):
                if worker.status in ("running", "starting"):
                    if not tracker.is_alive(worker.worker_id):
                        worker.status = "dead"
                        lived = time.time() - worker.started_at
                        log.warning(
                            "Worker %s (PID %s) died after %.1fs",
                            worker.worker_id, worker.pid, lived,
                        )
                        # Rapid death detection for backoff
                        if lived < 10.0:
                            pool_state = tracker.get_pool_state(pool.name)
                            pool_state.rapid_death_count += 1
                        else:
                            pool_state = tracker.get_pool_state(pool.name)
                            pool_state.rapid_death_count = 0
                        tracker.remove_worker(worker.worker_id)

            # 3c. Check for stale heartbeats (tracked workers with live PIDs)
            heartbeats = health.get_worker_heartbeats(pool.heartbeat_model)
            if heartbeats is not None:
                now = time.time()
                for worker in tracker.get_pool_workers(pool.name):
                    if worker.status == "running":
                        hb = next(
                            (h for h in heartbeats if h["worker_id"] == worker.worker_id),
                            None,
                        )
                        if hb is None or _is_stale(hb["last_heartbeat_at"], config.stale_threshold_seconds):
                            # Worker has live PID but stale heartbeat → kill it
                            log.warning(
                                "Worker %s has stale heartbeat, killing PID %s",
                                worker.worker_id, worker.pid,
                            )
                            tracker.kill_worker(worker.worker_id)
                            tracker.remove_worker(worker.worker_id)

            # 3d. Spawn replacements if under target
            pool_state = tracker.get_pool_state(pool.name)

            # Check backoff
            if time.time() < pool_state.backoff_until:
                continue

            # Check spawn failure circuit breaker
            if pool_state.spawn_failure_count >= 5:
                continue

            deficit = pool.target_count - alive_count
            if deficit > 0:
                MAX_SPAWN_PER_CYCLE = 5
                to_spawn = min(deficit, MAX_SPAWN_PER_CYCLE)

                for _ in range(to_spawn):
                    idx = tracker.get_next_worker_index(pool.name)
                    try:
                        proc, worker_id = spawn_worker(
                            pool, idx, os.getpid(), config.log_dir,
                        )
                        tracker.add_worker(pool.name, worker_id, proc)
                        pool_state.spawn_failure_count = 0
                        log.info("Spawned %s (PID %d)", worker_id, proc.pid)
                    except Exception as e:
                        pool_state.spawn_failure_count += 1
                        log.error("Spawn failed for pool %s: %s", pool.name, e)
                        break  # stop spawning this pool this cycle

                    time.sleep(pool.stagger_seconds)

                # Apply backoff if rapid deaths
                if pool_state.rapid_death_count >= 3:
                    backoff = min(300, 30 * (2 ** (pool_state.rapid_death_count - 3)))
                    pool_state.backoff_until = time.time() + backoff
                    log.warning(
                        "Pool %s respawn backoff: %ds (%d rapid deaths)",
                        pool.name, backoff, pool_state.rapid_death_count,
                    )

        # 4. Drain workers in pools that became inactive
        for pool in config.pools:
            if pool.name not in active_pool_names:
                workers = tracker.get_pool_workers(pool.name)
                if workers:
                    worker_ids = [w.worker_id for w in workers]
                    health.set_draining(worker_ids)
                    # Workers will self-drain on next heartbeat cycle.
                    # Guardian will detect their exit in step 3b next cycle.

        # 5. Persist state
        tracker.persist_state()

        # 6. Guardian self-heartbeat
        _write_guardian_heartbeat(config.state_dir)

        # 7. Log pruning (every 60s)
        if time.time() - last_log_prune > 60:
            prune_logs(config.log_dir, config.max_log_dir_bytes, config.log_retention_days)
            last_log_prune = time.time()

        # 8. Sleep until next cycle
        elapsed = time.time() - cycle_start
        sleep_time = max(0, config.poll_interval_seconds - elapsed)
        # Use interruptible sleep (check shutdown_requested every 1s)
        _interruptible_sleep(sleep_time, lambda: shutdown_requested)

    # ── Graceful shutdown ──
    graceful_shutdown(tracker, health, config.drain_grace_seconds)
    release_single_instance_lock()


def graceful_shutdown(tracker, health, grace_seconds):
    """Drain all workers, wait, then force-kill survivors."""
    all_worker_ids = [w.worker_id for w in tracker.get_all_workers()]
    if not all_worker_ids:
        return

    # 1. Signal drain via Postgres
    try:
        health.set_draining(all_worker_ids)
        log.info("Drain signal sent to %d workers", len(all_worker_ids))
    except Exception as e:
        log.warning("Failed to send drain signal via Postgres: %s", e)
        # Fall through to TerminateProcess

    # 2. Wait for workers to exit
    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        still_alive = [w for w in tracker.get_all_workers() if tracker.is_alive(w.worker_id)]
        if not still_alive:
            log.info("All workers exited cleanly")
            return
        time.sleep(1)

    # 3. Force-kill survivors
    survivors = [w for w in tracker.get_all_workers() if tracker.is_alive(w.worker_id)]
    for worker in survivors:
        log.warning("Force-killing %s (PID %s)", worker.worker_id, worker.pid)
        tracker.kill_worker(worker.worker_id)


def reconcile_config(tracker, old_config, new_config):
    """Handle config changes between old and new fleet configs."""
    old_pools = {p.name: p for p in old_config.pools}
    new_pools = {p.name: p for p in new_config.pools}

    for name, new_pool in new_pools.items():
        if name in old_pools:
            old_hash = pool_config_hash(old_pools[name])
            new_hash = pool_config_hash(new_pool)
            if old_hash != new_hash:
                # Spawn spec changed → reset backoff
                pool_state = tracker.get_pool_state(name)
                pool_state.rapid_death_count = 0
                pool_state.backoff_until = 0.0
                pool_state.spawn_failure_count = 0
                pool_state.config_hash = new_hash
                log.info("Pool %s config changed, backoff reset", name)

    # Pools removed from config → drain their workers
    for name in old_pools:
        if name not in new_pools:
            # Will be handled by step 4 of main loop (inactive pool drain)
            pass
```

**Single-instance lock implementation:**

```python
import msvcrt

_lock_fd = None

def acquire_single_instance_lock(state_dir: str):
    global _lock_fd
    pid_path = os.path.join(state_dir, "guardian.pid")
    _lock_fd = open(pid_path, "w")
    try:
        msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
    except (IOError, OSError):
        _lock_fd.close()
        raise SystemExit("Another worker-guardian instance is already running")
    _lock_fd.write(str(os.getpid()))
    _lock_fd.flush()

def release_single_instance_lock():
    global _lock_fd
    if _lock_fd:
        try:
            msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
        except Exception:
            pass
        _lock_fd.close()
        _lock_fd = None
```

**Interruptible sleep:**

```python
def _interruptible_sleep(total: float, should_stop):
    """Sleep in 1s increments, checking should_stop() each iteration."""
    end = time.time() + total
    while time.time() < end:
        if should_stop():
            return
        time.sleep(min(1.0, end - time.time()))
```

**Log pruning:**

```python
def prune_logs(log_dir: str, max_bytes: int, retention_days: int):
    """Delete old log files + enforce total directory size cap."""
    if not os.path.isdir(log_dir):
        return

    now = time.time()
    cutoff = now - (retention_days * 86400)

    # Collect all log files with stats
    files = []
    for root, _, names in os.walk(log_dir):
        for name in names:
            if name.endswith(".log"):
                path = os.path.join(root, name)
                try:
                    st = os.stat(path)
                    files.append((path, st.st_mtime, st.st_size))
                except OSError:
                    pass

    # 1. Delete files older than retention
    for path, mtime, _ in files:
        if mtime < cutoff:
            try:
                os.remove(path)
            except OSError:
                pass

    # 2. Enforce total size cap (delete oldest first)
    files = [(p, m, s) for p, m, s in files if os.path.exists(p)]
    files.sort(key=lambda x: x[1])  # oldest first
    total = sum(s for _, _, s in files)
    while total > max_bytes and files:
        path, _, size = files.pop(0)
        try:
            os.remove(path)
            total -= size
        except OSError:
            pass
```

### 5.7. `cli.py` — Command-Line Interface

```python
import click

@click.group()
def main():
    """Worker Guardian — fleet manager daemon."""

@main.command()
@click.option("--config", "-c", default="fleet.yaml", help="Path to fleet YAML config")
@click.option("--foreground", is_flag=True, help="Run in foreground (default; use pythonw for background)")
@click.option("--log-level", default="INFO", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]))
def start(config, foreground, log_level):
    """Start the guardian daemon."""
    # Set up logging (to file + optionally console)
    setup_logging(log_level, foreground)
    # Run the daemon loop
    from .daemon import run
    run(config_path=config, foreground=foreground)

@main.command()
@click.option("--state-dir", default=None, help="State directory (default: from config)")
@click.option("--timeout", default=90, help="Drain grace period in seconds")
def stop(state_dir, timeout):
    """Stop the running guardian and drain all workers."""
    # Read guardian PID from guardian.pid
    # Send SIGTERM (or TerminateProcess on Windows)
    # Wait up to --timeout for exit

@main.command()
@click.option("--state-dir", default=None)
@click.option("--config", "-c", default="fleet.yaml")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output")
def status(state_dir, config, as_json):
    """Show current fleet status."""
    # Read state file + query Postgres for live heartbeats
    # Display: per-pool target vs actual, worker PIDs, heartbeat ages
    # Format: table (default) or JSON

@main.command()
@click.option("--state-dir", default=None)
def reload(state_dir):
    """Signal the guardian to re-read fleet config."""
    # Touch {state_dir}/reload_requested sentinel file
    # Guardian picks it up on next cycle (within poll_interval_seconds)
```

**`status` output example:**

```
Worker Guardian — Fleet Status
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Pool          Target  Alive  Status
────────────  ──────  ─────  ──────────
deepseek          28     28  ✔ healthy
fetch              3      3  ✔ healthy
scrapfly           4      4  ✔ healthy
cluster            8      7  ⚠ 1 respawning
openrouter        10      -  ○ disabled (INFERENCE_MODE != 'openrouter')

Guardian PID: 12345 | Uptime: 2h 14m | Last check: 3s ago
Postgres: connected | Log dir: 847 MB / 2048 MB
```

---

## 6. Required Changes to Other Projects

### 6.1. llm-queue-manager — Drain detection in heartbeat (5 lines)

**File**: `src/llm_queue/worker_registry.py`

**Change 1** — Modify `_HEARTBEAT_SQL` to return status:

```sql
-- Current (line ~103):
UPDATE llm_workers
   SET last_heartbeat_at = now(),
       status            = COALESCE(%(status)s, status)
 WHERE worker_id = %(worker_id)s;

-- New:
UPDATE llm_workers
   SET last_heartbeat_at = now(),
       status            = COALESCE(%(status)s, status)
 WHERE worker_id = %(worker_id)s
 RETURNING status;
```

**Change 2** — In `WorkerHeartbeatTask._run()` (~line 416), after heartbeat write:

```python
# Current:
n = worker_heartbeat(conn, worker_id=self._worker_id, status=status)

# New:
n = worker_heartbeat(conn, worker_id=self._worker_id, status=status)
# Check if guardian set us to 'draining' externally
if n > 0:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM llm_workers WHERE worker_id = %s", (self._worker_id,))
        row = cur.fetchone()
        if row and row[0] == 'draining' and self._drain_signal and not self._drain_signal.is_set():
            self._drain_signal.set()
```

Note: Since `_HEARTBEAT_SQL` uses `COALESCE(%(status)s, status)`, a normal heartbeat
(status=None) preserves the 'draining' status set by the guardian. The worker reads
it back and sets its drain signal.

**Alternative** (simpler, uses RETURNING): Modify `worker_heartbeat()` function to
return the status from the `RETURNING` clause, then check in `WorkerHeartbeatTask._run()`.

### 6.2. poiVerifier — Migration cutover (optional, 2 lines)

**File**: `startup.py`, at the top of `ensure_worker_fleet()` (~line 1807):

```python
def ensure_worker_fleet(*, verbose: bool = True) -> dict:
    if os.environ.get("WORKER_GUARDIAN_ACTIVE") == "1":
        return {}
    # ... existing code ...
```

### 6.3. poiVerifier — Pidfile write in worker init (optional, 3 lines per worker type)

**Files**: `services/queue/fetch_worker.py`, `cluster_worker.py`, `scrapfly_worker.py`

Add near the top of `main()`, after `worker_id` is determined:

```python
# Write pidfile for worker-guardian restart recovery
_pidfile = os.path.join(os.environ.get("WG_STATE_DIR", ""), "pids", worker_id + ".pid")
if os.environ.get("WG_STATE_DIR"):
    os.makedirs(os.path.dirname(_pidfile), exist_ok=True)
    with open(_pidfile, "w") as _f:
        _f.write(f"{os.getpid()}\n{time.time()}\n")
```

The guardian passes `WG_STATE_DIR` as an env var when spawning workers.

---

## 7. Edge Cases and Error Handling

### 7.1. Guardian Crashes and Restarts

Workers continue running headless (invisible, no console dependency). On restart:
1. Read `state.json` for prior worker tracking
2. Read `pids/*.pid` for PID + create_time
3. Validate via `psutil.Process(pid).create_time()` (2s tolerance)
4. Adopt surviving workers
5. Query Postgres for current alive counts
6. Spawn only for true deficit

No double-spawning: Postgres alive count is the authoritative source for deficit.

### 7.2. PID Recycling

**In-memory**: Popen handle prevents recycling. `proc.poll()` always accurate.
**On-disk**: PID + create_time (2s tolerance). If mismatch, stale pidfile deleted.

### 7.3. Postgres Down (Hold-Steady Circuit Breaker)

Circuit breaker activates after 3× poll_interval (36s) with no successful query.
**Fast-open, slow-close**: enter degraded instantly, require 3 consecutive healthy
polls to exit. During degraded: kill/drain/spawn SUSPENDED; local PID checks CONTINUE.
Post-partition grace period extends stale threshold by partition duration (capped at
3× stale_threshold = 270s) for one cycle on exit.

See Section 11.2 for full state machine and implementation.

### 7.4. Postgres Down During Shutdown

Workers never see the drain signal. Guardian waits `drain_grace_seconds` (90s)
then `TerminateProcess`. Same outcome as current system (no graceful drain was
possible before either).

### 7.5. Spawn Failure

`Popen` raises (file not found, permission denied): log error, increment
`spawn_failure_count`. After 5 consecutive failures, mark pool errored and stop
attempts. Reset on config reload (which resets all backoff state).

### 7.6. Rapid Death Loop

Worker crashes within 10s of spawn → `rapid_death_count++`. After 3:
`backoff = min(300, 30 * 2^(n-3))`. Prevents CPU/memory thrash from a broken
worker binary. Reset when first worker survives >60s or config hash changes.

### 7.7. Mixed Mode (Guardian + Manual Workers)

Guardian counts ALL fresh heartbeats for a model ON THIS HOST, including manually
launched workers. Manual workers above target → guardian spawns nothing. Manual
workers die → guardian respawns up to its target. Guardian manages the floor, not
the ceiling.

### 7.8. Two Guardians on Same Machine

Prevented by `msvcrt.locking` exclusive lock on `guardian.pid`. Second instance
exits immediately with clear error message. Lock auto-releases on crash.

### 7.11. Network Partition During Drain

If `drain-pool` sets workers to `draining` in Postgres, then a network partition
occurs, workers can't read the drain signal. They continue running indefinitely.
When connectivity restores, they see the drain flag on their next heartbeat and
exit. No operator action needed.

### 7.12. Hostname Collision

Two machines with the same hostname (common on unmanaged Windows: `DESKTOP-ABC`)
would collide in `worker_host` filters and `guardian_instances` PK. The guardian
validates uniqueness on startup: if `guardian_instances` has a row for this host
with a different PID that is NOT stale, refuse to start with a CRITICAL error.
For multi-machine: require explicit `guardian.host` in config.

### 7.9. Config Reload with Syntax Error

New YAML is validated with Pydantic BEFORE swapping. Invalid config → log error,
keep old config. Daemon continues with previous fleet specification.

### 7.10. Stale Workers from Previous Guardian

If old guardian managed 28 workers, new guardian starts with target 10: deficit is
negative, no spawning. Excess workers eventually die (no one respawns them). For
immediate convergence: `worker-guardian stop` the old fleet first.

---

## 8. Migration Strategy

### Step 1 — Install guardian alongside existing system

```powershell
cd C:\PROJECTS\worker-guardian
pip install -e .
```

### Step 2 — Create fleet config

Copy `fleet-examples/poiverifier.yaml` to the poiVerifier project, adjust paths
and env var references.

### Step 3 — Run guardian alongside startup.py

```powershell
worker-guardian start --config C:\PROJECTS\poiVerifier\V2\poiVerifier\fleet.yaml --foreground
```

Both systems check Postgres alive counts before spawning. Whoever spawns first
fills targets, the other is a no-op. No conflict.

### Step 4 — Explicit cutover

Set `WORKER_GUARDIAN_ACTIVE=1` in the environment. `startup.py`'s
`ensure_worker_fleet()` skips entirely.

### Step 5 — Cleanup

Remove `_ensure_*_workers()` functions from `startup.py`. Guardian is the sole
fleet manager.

---

## 9. Phased Implementation Plan

### Phase 1 — Core Daemon (MVP)

**Files**: `models.py`, `config.py`, `spawner.py`, `tracker.py`, `health.py`, `daemon.py`, `cli.py`

**Scope**:
- Load fleet YAML with Pydantic validation
- ${VAR} interpolation in commands and env
- `enabled_when` DSL parser and evaluator
- `spawn_worker()` with CREATE_NO_WINDOW + log file redirect
- ProcessTracker with in-memory tracking + state file persistence
- HealthPoller with Postgres connection + count_alive query
- Core loop: poll → health → spawn → persist → sleep
- CLI: `start --foreground`, `status`
- Single-instance lock via `msvcrt.locking`

**Tests**:
- Config loading + validation + interpolation
- enabled_when DSL parsing (==, !=, in, and, or)
- Spawn + handle lifecycle (close after Popen)
- State file atomic write + load
- Health poller mock
- Deficit calculation

**Acceptance criteria**:
- `worker-guardian start --config fleet.yaml --foreground` spawns configured pools
- Workers are invisible (no console windows)
- Workers appear in `llm_workers` with fresh heartbeats
- Manually killing a worker → guardian respawns it within 1-2 cycles
- `worker-guardian status` shows per-pool target vs actual

### Phase 2 — Lifecycle Management

**Additional scope**:
- CLI: `stop`, `reload`
- `graceful_shutdown()` with drain-via-Postgres
- Config hot-reload with mtime detection + reload sentinel
- Config reconciliation (new/removed pools, target changes, backoff reset)
- Log pruning (age + size cap)
- `set_draining()` in health poller
- llm-queue-manager heartbeat change (RETURNING status + drain detection)

**Tests**:
- Graceful shutdown: drain signal sent, workers exit, no force-kill needed
- Reload: new config applied, backoff reset on spec change
- Reconciliation: pool added/removed, target changed
- Log pruning: old files deleted, size cap enforced

**Acceptance criteria**:
- `worker-guardian stop` drains workers within 90s
- `worker-guardian reload` picks up config changes without restart
- Log directory stays under 2GB

### Phase 3 — Robustness

**Additional scope**:
- Pidfile-based guardian restart recovery (`rediscover_orphans()`)
- Postgres-down degraded mode (skip health + spawn, PID checks continue)
- Spawn failure circuit breaker (5 consecutive failures → stop pool)
- Respawn backoff with config-change reset
- Connection retry with exponential backoff

**Tests**:
- Guardian crash → restart → orphan rediscovery → no double-spawn
- Postgres disconnected → degraded mode → reconnect → recovery
- Broken spawn command → circuit breaker → config reload → reset
- Rapid death → backoff → config change → reset

**Acceptance criteria**:
- Kill guardian, verify workers still running, restart guardian, verify no new spawns
- Disconnect Postgres, verify no kills/spawns, reconnect, verify recovery
- Break a bat file, verify backoff, fix bat, reload, verify immediate retry

### Phase 4 — Polish

**Additional scope**:
- Guardian self-heartbeat file
- `--json` output for status command
- `startup.py` migration cutover flag
- Worker pidfile writes (3-line change per worker type)
- Comprehensive integration tests

**Acceptance criteria**:
- `worker-guardian status --json` returns machine-readable output
- Guardian heartbeat file updated every cycle
- Setting `WORKER_GUARDIAN_ACTIVE=1` disables startup.py spawning

### Phase 5 — Multi-Machine

**Additional scope**:
- `guardian` config section with `host`, `orchestrator_host`, `database_url`
- `locality` field on pools: `local` or `remote`
- Hostname in worker ID: `{pool}-{index:02d}-{hostname}-{guardian_pid}`
- Host-filtered health queries (`WHERE worker_host = %s`)
- Hold-steady circuit breaker (fast-open, slow-close, post-partition grace)
- `guardian_instances` Postgres table + UPSERT every cycle
- `status --global` CLI command
- `drain-pool` CLI command
- Cold-start degraded mode (enter immediately if first query fails)
- Heartbeat jitter (1-line llm-queue-manager change)
- Runtime prerequisites check for `locality: local` pools

**Tests**:
- Circuit breaker state machine: NORMAL → DEGRADED → NORMAL transitions
- Post-partition grace period: extended threshold, cap at 3×
- Cold-start into degraded: first query fails → immediate degraded
- Locality guard: local pools skipped on non-orchestrator
- Host-filtered alive count: only this machine's workers counted
- Worker ID uniqueness across hostnames
- guardian_instances UPSERT and stale detection
- drain-pool signal-and-return

**Acceptance criteria**:
- Deploy to 2 machines. Machine B has only `locality: remote` pools.
- Kill Machine B's network cable. Guardian enters degraded. Workers continue.
- Restore network. Guardian exits degraded in ~36s. No mass kill. No double-spawn.
- `status --global` shows both guardians and their fleet summaries.
- `drain-pool` on Machine B → workers finish + exit → guardian respawns replacements.

---

## 10. Review History

### 10.1. Single-Machine Design (Rounds 1-3, 1 reviewer)

| Round | Issue | Severity | Resolution |
|-------|-------|----------|------------|
| 1 | CTRL_BREAK_EVENT fails with CREATE_NO_WINDOW | CRITICAL | Drain via Postgres status column |
| 1 | Pipe deadlock with 28 workers (stdout=PIPE) | CRITICAL | Direct file redirect, no pipes |
| 1 | PID recycling (1s Windows resolution) | HIGH | Popen handle in-memory + pidfiles on-disk |
| 1 | Bat workers: no PID-to-worker-id correlation | HIGH | Pass guardian ID as bat 2nd argument |
| 1 | 3-strike stale rule coupled to poll rate | MEDIUM | Wall-clock threshold (90s) |
| 1 | State file corruption on unclean exit | MEDIUM | Atomic write (os.replace) + Postgres fallback |
| 1 | RotatingFileHandler threading with 28 workers | LOW | Moot — no pipes, no relay threads |
| 1 | No backoff on rapid respawn loops | LOW | Exponential backoff (30s → 300s) |
| 2 | Log file handle leak on worker respawn | CRITICAL | Close guardian's copy immediately after Popen |
| 2 | psutil.Process.environ() unreliable on Windows | HIGH | Pidfiles as primary, no environ() |
| 2 | Sentinel file drain has 0-30s detection latency | HIGH | Postgres drain (instant on next heartbeat) |
| 2 | Dual guardian instances → double-spawning | HIGH | PID file + msvcrt exclusive lock |
| 2 | No migration interaction with startup.py | HIGH | Natural idempotency (both check alive counts) |
| 2 | Date-suffixed logs unbounded in size | MEDIUM | 2GB directory cap + oldest-first pruning |
| 2 | Orphan sentinel files on guardian crash | MEDIUM | Moot — no sentinel files (Postgres drain) |
| 2 | Respawn backoff not reset on config change | MEDIUM | Reset on config hash change |
| 3 | WorkerHeartbeatTask is write-only (no drain detect) | HIGH | 5-line llm-queue-manager change documented |
| 3 | close_fds default True on Python 3.12/Windows | MEDIUM | Explicit close_fds=False in Popen call |
| 3 | Postgres-down during shutdown = no drain | LOW | Documented as known degradation (90s → kill) |

### 10.2. Multi-Machine Review (Rounds 4-6, 2 independent reviewers)

| Round | Issue | Severity | Resolution |
|-------|-------|----------|------------|
| 4 | Health query counts ALL machines globally | CRITICAL | Filter by `worker_host` — each guardian counts only its own host |
| 4 | Worker ID collision across machines | CRITICAL | Include hostname: `{pool}-{index:02d}-{hostname}-{guardian_pid}` |
| 4 | No guardian identity or per-machine config | HIGH | `guardian` section in YAML with `host`, `orchestrator_host`, `database_url` |
| 5 | Double-spawn: two guardians both target 28 = 56 | CRITICAL | `target_count` is per-machine, not fleet-wide. No cross-machine coordination |
| 5 | Network partition → guardian kills healthy workers | CRITICAL | Hold-steady circuit breaker: fast-open, slow-close (3 consecutive healthy polls) |
| 5 | Post-partition: all heartbeats look stale → mass kill | CRITICAL | Grace period: `stale_threshold + partition_duration`, capped at 3× stale_threshold |
| 5 | Blob store inaccessible cross-machine | CRITICAL | Phase 1: `locality: local` guard. Phase 2: S3 migration via existing BlobStore Protocol |
| 5 | No location-bound vs location-agnostic distinction | HIGH | `locality` field: `local` or `remote` only (no `any`) |
| 5 | No global fleet view across machines | HIGH | `guardian_instances` table + `status --global` CLI |
| 5 | PgBouncer exhaustion with 150+ workers | HIGH | Document recommended sizing + heartbeat jitter (30-40s) |
| 5 | Version drift across machines | HIGH | Version in `guardian_instances`, compat window, prominent in status |
| 5 | Postgres security over network | HIGH | `sslmode=require`, pg_hba whitelist, `${ENV_VAR}` refs |
| 5 | Hostname normalization (FQDN vs short) | MEDIUM | Always `socket.gethostname()`, documented, consistent with workers |
| 5 | Hostname collision on unmanaged Windows | MEDIUM | Require explicit `guardian.host` for multi-machine |
| 5 | `locality: any` is ambiguous | MEDIUM | Dropped — force explicit `local` or `remote` |
| 6 | Cold-start into degraded: first query fails | MEDIUM | Enter degraded immediately on startup if first query fails |
| 6 | Rolling upgrades: no drain mechanism | HIGH | `drain-pool` CLI command (signal and return) |
| 6 | Hold-steady kills local PID-dead workers | MEDIUM | Local PID checks continue during hold-steady; respawning waits for exit |
| 6 | Drain + partition interaction | LOW | Documented: workers can't see drain during partition, normal on reconnect |
| 6 | No "add a machine" runbook | MEDIUM | Documented in Section 12 |

---

## 11. Multi-Machine Architecture

### 11.1. Design Principles

1. **One guardian per machine.** Each guardian manages only its own local processes
   via `subprocess.Popen`. No cross-machine spawning or killing.

2. **Per-machine targets.** Each fleet YAML specifies targets for THAT machine only.
   For 28 total deepseek across 2 machines: Machine A = 14, Machine B = 14. The
   operator manages distribution manually. Auto-distribution is a Phase 3 concern
   (at 10+ machines, use Kubernetes/Nomad instead).

3. **Host-filtered health queries.** Every `count_alive()` and `get_worker_heartbeats()`
   call filters by `worker_host`. Each guardian sees only its own workers.

4. **Postgres is the coordination plane.** All shared state lives in Postgres:
   worker heartbeats, guardian instances, drain signals. No direct guardian-to-guardian
   communication.

5. **When you can't observe, don't act.** (Hold-steady principle.) If the guardian
   can't reach Postgres, it suspends all kill/drain/spawn decisions. Workers continue
   running undisturbed.

### 11.2. Hold-Steady Circuit Breaker

The circuit breaker protects against two failure modes: (a) guardian kills healthy
workers during a network partition, and (b) mass kill on reconnect because all
heartbeats look stale.

```
State Machine:
  NORMAL → DEGRADED:  Instant (first query failure after 3× poll_interval = 36s)
  DEGRADED → NORMAL:  Slow-close (3 consecutive successful polls = 36s of stability)

During DEGRADED:
  SUSPENDED: kill-on-stale, drain signals, new worker spawning
  ACTIVE: local PID liveness checks (proc.poll()), dead worker tracking

Cold Start:
  If first DB query fails → enter DEGRADED immediately (don't wait for threshold)

Post-Partition Grace:
  On exiting DEGRADED, the first healthy cycle uses:
    effective_stale_threshold = min(stale_threshold + partition_duration, 3 × stale_threshold)
  This gives workers time to heartbeat after connectivity restores.
  Second cycle reverts to normal stale_threshold (90s).
  Cap at 3× (270s) prevents hours-long grace periods after extended outages.
```

**Implementation:**

```python
class CircuitBreaker:
    HEALTHY_POLLS_TO_RECOVER = 3

    def __init__(self, poll_interval: float, stale_threshold: float):
        self._poll_interval = poll_interval
        self._stale_threshold = stale_threshold
        self._degraded = False
        self._degraded_entered_at: float = 0.0
        self._consecutive_healthy: int = 0
        self._last_successful_query: float = 0.0
        self._post_partition_cycle: bool = False

    def on_query_success(self, now: float):
        self._last_successful_query = now
        self._consecutive_healthy += 1
        if self._degraded and self._consecutive_healthy >= self.HEALTHY_POLLS_TO_RECOVER:
            partition_duration = now - self._degraded_entered_at
            self._degraded = False
            self._post_partition_cycle = True
            self._post_partition_grace = min(
                partition_duration, 2 * self._stale_threshold
            )
            log.info("Exiting degraded mode after %.0fs partition, grace=%.0fs",
                     partition_duration, self._post_partition_grace)

    def on_query_failure(self, now: float):
        self._consecutive_healthy = 0
        threshold = 3 * self._poll_interval
        if not self._degraded:
            # Cold start: immediate degraded if we've never succeeded
            if self._last_successful_query == 0.0 or (now - self._last_successful_query) > threshold:
                self._degraded = True
                self._degraded_entered_at = now
                log.warning("Entering degraded mode: DB unreachable")

    @property
    def kills_suspended(self) -> bool:
        return self._degraded

    @property
    def effective_stale_threshold(self) -> float:
        if self._post_partition_cycle:
            self._post_partition_cycle = False
            return self._stale_threshold + self._post_partition_grace
        return self._stale_threshold
```

### 11.3. Guardian Instances Table

```sql
CREATE TABLE IF NOT EXISTS guardian_instances (
    host              TEXT PRIMARY KEY,
    guardian_pid      INT NOT NULL,
    version           TEXT,
    status            TEXT DEFAULT 'running',   -- running | degraded | shutting_down
    config_hash       TEXT,
    last_heartbeat_at TIMESTAMPTZ DEFAULT NOW(),
    fleet_summary     JSONB,
    started_at        TIMESTAMPTZ DEFAULT NOW()
);
```

Each guardian UPSERTs every cycle. The `INSERT ... ON CONFLICT (host) DO UPDATE`
checks the old PID is dead before adopting (prevents split-brain if old guardian
hasn't fully exited).

**`status --global` output:**

```
Worker Guardian — Global Fleet Status
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Host             Version  Status   Last Check  Workers
───────────────  ───────  ───────  ──────────  ────────────────────
workstation-a    0.1.0    running  3s ago      DS:14 FE:3 SF:4 CL:8
worker-node-1    0.1.0    running  8s ago      DS:14
worker-node-2    0.1.0    degraded 45s ago     DS:14 ⚠

⚠ worker-node-2: DEGRADED (DB unreachable for 45s)
Total: 42 deepseek, 3 fetch, 4 scrapfly, 8 cluster = 57 workers
```

### 11.4. Pool Locality

Every pool must declare `locality: local` or `locality: remote`. No `any` — ambiguity
leads to silent failures.

| Locality | Meaning | Examples |
|----------|---------|---------|
| `local` | Only runs on the orchestrator machine. Needs local resources (browser, GNAF, blob store) | fetch, scrapfly, cluster |
| `remote` | Can run on any machine. API-only, no local resource dependencies | deepseek, deepinfra, openai, openrouter |

**Startup guard:**
```python
if pool.locality == "local" and config.guardian.host != config.guardian.orchestrator_host:
    log.info("Skipping pool %s: locality=local but this is not the orchestrator", pool.name)
    continue
```

**Runtime prerequisites** (checked at startup for `locality: local` pools):
- `spawn.cwd` directory exists
- For fetch pools: blob directory is writable

### 11.5. Drain-Pool CLI Command (Rolling Upgrades)

```python
@main.command("drain-pool")
@click.option("--pool", required=True, help="Pool name to drain")
@click.option("--config", "-c", default="fleet.yaml")
def drain_pool(pool, config):
    """Signal all workers in a pool to drain. Returns immediately.
    Workers finish their current job, then exit. Guardian respawns
    replacements (with new code) as workers exit."""
    cfg = load_config(config)
    health = HealthPoller(cfg.guardian.database_url)
    health.connect()
    workers = health.get_worker_heartbeats(
        pool_heartbeat_model, cfg.guardian.host
    )
    if workers:
        ids = [w["worker_id"] for w in workers]
        n = health.set_draining(ids)
        click.echo(f"Drain signal sent to {n} workers in pool '{pool}'.")
        click.echo(f"Monitor with: worker-guardian status")
    else:
        click.echo(f"No active workers found for pool '{pool}'.")
```

**Rolling upgrade procedure:**
1. Update worker code on the machine (git pull, pip install)
2. `worker-guardian drain-pool --pool deepseek` — signals existing workers
3. Workers finish current job, exit (30-90s)
4. Guardian detects deficit, spawns replacements with new code
5. Natural rolling upgrade — old drain out, new fill in

### 11.6. Heartbeat Jitter

Workers spawned simultaneously by the guardian will heartbeat in lockstep
(all at exactly 30s intervals). With 150+ workers, this creates periodic
load spikes.

**Fix (1-line change in llm-queue-manager `WorkerHeartbeatTask.__init__`):**
```python
self._interval = interval_seconds + random.uniform(0, interval_seconds / 3)
```

Workers heartbeat every 30-40s instead of exactly 30s. Spreads the load.

### 11.7. Security Checklist (Before Multi-Machine Deployment)

- [ ] `database_url` uses `sslmode=require`
- [ ] Postgres `pg_hba.conf` whitelists guardian/worker IPs
- [ ] Fleet YAML uses `${ENV_VAR}` references (no raw API keys)
- [ ] PgBouncer `max_client_conn` sized for total workers: `workers × 1.5`
- [ ] PgBouncer in `transaction` mode (not `session`)
- [ ] Each machine has explicit `guardian.host` (no `auto` in production)
- [ ] Firewall allows Postgres port only from known IPs

### 11.8. PgBouncer Recommended Settings

```ini
[pgbouncer]
pool_mode = transaction
max_client_conn = 500
default_pool_size = 50
reserve_pool_size = 10
reserve_pool_timeout = 3
server_idle_timeout = 600
```

Verified: workers use short transactions for heartbeats (connect → UPDATE → disconnect)
and job claiming (SELECT FOR UPDATE SKIP LOCKED → UPDATE → COMMIT). Workers have
built-in reconnect with exponential backoff (1s → 2s → 4s → 8s → 30s) and self-healing
re-registration. PgBouncer restart = workers retry automatically, guardian enters
degraded mode for ~36s then recovers.

### 11.9. Blob Store Migration Path (Phase 2)

When fetch workers need to go remote, the blob store must be network-accessible.

**Current**: `LocalBlobStore` writes to `C:\tmp\queue_blobs\{run_id}\`. Workers and
orchestrator must be on the same machine.

**Target**: `S3BlobStore` implementing the existing `BlobStore(Protocol)` interface
from `llm-queue-manager`. The Protocol is already defined (`blob_store.py:39`);
S3 adapter is explicitly designed-for but deferred.

Options evaluated:
- **(a) S3/MinIO**: Correct. Standard pattern. MinIO self-hosted or AWS S3. `boto3`
  already in `llm-queue-manager` optional dependencies.
- **(b) Postgres bytea**: Too heavy. HTML blobs are 1-5MB × hundreds. Bloats WAL.
- **(c) Shared SMB drive**: Fragile on Windows. Permission nightmares.
- **(d) HTTP pull from workers**: Wrong — makes workers into file servers.

### 11.10. Known Limitations

1. **Orchestrator is a SPOF for local pools.** If the orchestrator machine dies,
   `locality: local` pools (fetch, cluster) have zero capacity. This is acceptable —
   the orchestrator IS a real SPOF today (it runs poiVerifier). Multi-machine fixes
   LLM worker availability, not orchestrator availability.

2. **No fleet-wide auto-distribution.** Operator manually distributes targets across
   machines via per-machine fleet YAMLs. At 10+ machines, switch to a real orchestrator
   (Kubernetes, Nomad). The guardian is for 2-5 machines where the operator knows
   every box by name.

3. **No cross-guardian signaling.** Guardians read the shared `guardian_instances`
   table but can't signal each other. Coordinated actions (e.g., fleet-wide scale-down)
   require operator intervention on each machine. LISTEN/NOTIFY or a `guardian_commands`
   table is a Phase 3 concern.

---

## 12. Operational Runbooks

### 12.1. Add a New Machine to the Fleet

1. **Install**: `pip install git+https://github.com/org/worker-guardian.git`
2. **Copy fleet template**: Copy `fleet-examples/poiverifier.yaml`, edit:
   - Set `guardian.host` to unique hostname
   - Set `guardian.orchestrator_host` to the orchestrator's hostname
   - Set `guardian.database_url` to the central Postgres (with `sslmode=require`)
   - Set pool `target_count` values for this machine's contribution
   - Remove `locality: local` pools (they won't spawn anyway, but cleaner)
   - Set env vars: API keys, `INFERENCE_MODE`
3. **Firewall**: Allow outbound to Postgres IP:port, LLM API endpoints
4. **PgBouncer**: Add machine IP to `pg_hba.conf`, reload
5. **Start**: `worker-guardian start --config fleet.yaml --foreground`
6. **Verify**: `worker-guardian status` — check workers are alive and heartbeating
7. **Verify global**: `worker-guardian status --global --config fleet.yaml` — see all guardians

### 12.2. Rolling Upgrade (Update Worker Code)

1. `worker-guardian drain-pool --pool deepseek --config fleet.yaml`
2. Wait for `worker-guardian status` to show 0 alive (workers finishing current jobs)
3. Update code: `git pull`, `pip install -e .`, etc.
4. Workers respawn automatically with new code. Monitor via `status`.

### 12.3. Guardian Crash Recovery

Workers survive (CREATE_NO_WINDOW). No operator action needed if guardian auto-restarts.
If not:
1. `worker-guardian start --config fleet.yaml --foreground`
2. Guardian reads pidfiles, adopts surviving workers, resumes normal operation.
3. Verify: `worker-guardian status` — workers show as adopted.

### 12.4. Network Partition Recovery

1. Guardian enters degraded mode automatically (logged every 60s)
2. Workers continue running — their heartbeats are stale to Postgres but they're alive
3. On network restore: guardian exits degraded after 3 healthy polls (~36s)
4. Post-partition grace period gives workers time to heartbeat (up to 270s)
5. Monitor: `worker-guardian status --global` — check for `degraded` status

### 12.5. PgBouncer Restart

1. All guardians enter degraded mode (~36s)
2. All workers retry heartbeats with exponential backoff (1-30s)
3. On PgBouncer recovery: guardians exit degraded, workers resume heartbeating
4. No operator action needed. Monitor via logs.

---

**Final confidence: 95%.** The 5% gap is implementation risk, not design gaps:
- Circuit breaker state machine edge-case combinations need a focused integration test
- Windows handle inheritance needs validation under load
- Operational validation: deploy to two machines, partition one, observe recovery

Reaches 98% after: (1) integration test walking through all circuit breaker state
transitions with mocked time/DB, (2) 2-machine smoke test with simulated partition.
