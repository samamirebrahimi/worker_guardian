"""Core daemon loop — poll, health-check, spawn/kill, persist."""
from __future__ import annotations

import logging
import os
import signal
import threading
import time

from . import __version__
from .config import evaluate_enabled_when, load_config, pool_config_hash
from .display import add_event, render, stop as stop_display
from .health import HealthPoller
from .models import FleetConfig
from .spawner import SpawnError, spawn_worker
from .tracker import ProcessTracker

log = logging.getLogger(__name__)

MAX_SPAWN_PER_CYCLE = 15

# ---------------------------------------------------------------------------
# Single-instance lock (Windows msvcrt)
# ---------------------------------------------------------------------------

_lock_fd = None


def _acquire_lock(state_dir: str) -> None:
    global _lock_fd
    import msvcrt

    pid_path = os.path.join(state_dir, "guardian.pid")
    os.makedirs(state_dir, exist_ok=True)
    _lock_fd = open(pid_path, "w")
    try:
        msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
    except (IOError, OSError):
        _lock_fd.close()
        _lock_fd = None
        raise SystemExit("Another worker-guardian instance is already running")
    _lock_fd.write(str(os.getpid()))
    _lock_fd.flush()


def _release_lock() -> None:
    global _lock_fd
    if _lock_fd is not None:
        try:
            import msvcrt
            msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
        except Exception:
            pass
        try:
            _lock_fd.close()
        except Exception:
            pass
        _lock_fd = None


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

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
        self._post_partition_grace: float = 0.0

    def on_query_success(self, now: float) -> None:
        self._last_successful_query = now
        self._consecutive_healthy += 1
        if self._degraded and self._consecutive_healthy >= self.HEALTHY_POLLS_TO_RECOVER:
            partition_duration = now - self._degraded_entered_at
            self._degraded = False
            self._post_partition_cycle = True
            self._post_partition_grace = min(
                partition_duration, 2 * self._stale_threshold,
            )
            log.info(
                "Exiting degraded mode after %.0fs partition, grace=%.0fs",
                partition_duration, self._post_partition_grace,
            )

    def on_query_failure(self, now: float) -> None:
        self._consecutive_healthy = 0
        if not self._degraded:
            if (
                self._last_successful_query == 0.0
                or (now - self._last_successful_query) > 3 * self._poll_interval
            ):
                self._degraded = True
                self._degraded_entered_at = now
                log.warning("Entering degraded mode: DB unreachable")

    @property
    def is_degraded(self) -> bool:
        return self._degraded

    @property
    def kills_suspended(self) -> bool:
        return self._degraded

    @property
    def effective_stale_threshold(self) -> float:
        if self._post_partition_cycle:
            self._post_partition_cycle = False
            return self._stale_threshold + self._post_partition_grace
        return self._stale_threshold


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _interruptible_sleep(total: float, should_stop) -> None:
    end = time.time() + total
    while time.time() < end:
        if should_stop():
            return
        time.sleep(min(1.0, max(0, end - time.time())))


def _is_stale(last_heartbeat, stale_seconds: float) -> bool:
    """Check if a heartbeat timestamp is stale."""
    from datetime import datetime, timezone
    if last_heartbeat is None:
        return True
    if hasattr(last_heartbeat, "timestamp"):
        hb_epoch = last_heartbeat.timestamp()
    else:
        hb_epoch = float(last_heartbeat)
    return (time.time() - hb_epoch) > stale_seconds


def _write_guardian_heartbeat(state_dir: str) -> None:
    hb_path = os.path.join(state_dir, "guardian_heartbeat")
    try:
        with open(hb_path, "w") as f:
            f.write(f"{time.time()}\n")
    except OSError:
        pass


def _build_fleet_summary(tracker: ProcessTracker, config: FleetConfig) -> dict:
    summary = {}
    for pool in config.pools:
        workers = tracker.get_pool_workers(pool.name)
        alive = sum(1 for w in workers if tracker.is_alive(w.worker_id))
        summary[pool.name] = {"target": pool.target_count, "alive": alive}
    return summary


# ---------------------------------------------------------------------------
# Log pruning
# ---------------------------------------------------------------------------

def prune_logs(log_dir: str, max_bytes: int, retention_days: int) -> None:
    if not os.path.isdir(log_dir):
        return

    cutoff = time.time() - (retention_days * 86400)
    files: list[tuple[str, float, int]] = []
    for root, _, names in os.walk(log_dir):
        for name in names:
            if name.endswith(".log"):
                path = os.path.join(root, name)
                try:
                    st = os.stat(path)
                    files.append((path, st.st_mtime, st.st_size))
                except OSError:
                    pass

    for path, mtime, _ in files:
        if mtime < cutoff:
            try:
                os.remove(path)
            except OSError:
                pass

    files = [(p, m, s) for p, m, s in files if os.path.exists(p)]
    files.sort(key=lambda x: x[1])
    total = sum(s for _, _, s in files)
    while total > max_bytes and files:
        path, _, size = files.pop(0)
        try:
            os.remove(path)
            total -= size
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Config reconciliation
# ---------------------------------------------------------------------------

def _reconcile_config(
    tracker: ProcessTracker,
    old_config: FleetConfig,
    new_config: FleetConfig,
) -> None:
    old_pools = {p.name: p for p in old_config.pools}
    new_pools = {p.name: p for p in new_config.pools}

    for name, new_pool in new_pools.items():
        if name in old_pools:
            old_hash = pool_config_hash(old_pools[name])
            new_hash = pool_config_hash(new_pool)
            if old_hash != new_hash:
                ps = tracker.get_pool_state(name)
                ps.rapid_death_count = 0
                ps.backoff_until = 0.0
                ps.spawn_failure_count = 0
                ps.config_hash = new_hash
                log.info("Pool %s config changed, backoff reset", name)


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

def _graceful_shutdown(
    tracker: ProcessTracker,
    health: HealthPoller,
    grace_seconds: int,
) -> None:
    all_workers = tracker.get_all_workers()
    if not all_workers:
        return

    worker_ids = [w.worker_id for w in all_workers]
    try:
        n = health.set_draining(worker_ids)
        log.info("Drain signal sent to %d workers", n)
    except Exception as e:
        log.warning("Failed to send drain signal via Postgres: %s", e)

    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        still_alive = [w for w in tracker.get_all_workers() if tracker.is_alive(w.worker_id)]
        if not still_alive:
            log.info("All workers exited cleanly")
            return
        time.sleep(1)

    survivors = [w for w in tracker.get_all_workers() if tracker.is_alive(w.worker_id)]
    for w in survivors:
        log.warning("Force-killing %s (PID %s)", w.worker_id, w.pid)
        tracker.kill_worker(w.worker_id)


# ---------------------------------------------------------------------------
# Main daemon loop
# ---------------------------------------------------------------------------

def run(config_path: str, foreground: bool = True) -> None:
    config = load_config(config_path)
    os.makedirs(config.state_dir, exist_ok=True)
    os.makedirs(config.log_dir, exist_ok=True)

    _acquire_lock(config.state_dir)
    log.info("Worker Guardian %s starting (host=%s, PID=%d)",
             __version__, config.guardian.host, os.getpid())

    tracker = ProcessTracker(config.state_dir)
    tracker.set_guardian_pid(os.getpid())
    tracker.load_state()

    health = HealthPoller(config.guardian.database_url)
    health.ensure_guardian_table()

    breaker = CircuitBreaker(
        poll_interval=config.poll_interval_seconds,
        stale_threshold=config.stale_threshold_seconds,
    )

    adopted = tracker.rediscover_orphans(config.pools)
    if adopted:
        log.info("Adopted orphans: %s", adopted)
        for pname, cnt in adopted.items():
            add_event(f"Adopted {cnt} orphan(s) in {pname}")

    shutdown_requested = False
    config_mtime = _safe_mtime(config_path)
    last_log_prune = 0.0
    started_at = time.time()
    alive_counts: dict[str, int | None] = {}

    def _on_shutdown(sig, frame):
        nonlocal shutdown_requested
        shutdown_requested = True

    signal.signal(signal.SIGINT, _on_shutdown)
    signal.signal(signal.SIGTERM, _on_shutdown)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _on_shutdown)

    # ── Main loop ──
    while not shutdown_requested:
      try:
        cycle_start = time.time()

        # 1. Config hot-reload
        try:
            current_mtime = _safe_mtime(config_path)
            reload_sentinel = os.path.join(config.state_dir, "reload_requested")
            should_reload = (
                current_mtime != config_mtime
                or os.path.exists(reload_sentinel)
            )
            if should_reload:
                try:
                    new_config = load_config(config_path)
                    _reconcile_config(tracker, config, new_config)
                    config = new_config
                    config_mtime = current_mtime
                    if os.path.exists(reload_sentinel):
                        os.remove(reload_sentinel)
                    breaker = CircuitBreaker(
                        poll_interval=config.poll_interval_seconds,
                        stale_threshold=config.stale_threshold_seconds,
                    )
                    log.info("Config reloaded successfully")
                except Exception as e:
                    log.error("Config reload failed, keeping old config: %s", e)
        except Exception:
            pass

        # 2. Determine active pools
        is_orchestrator = (
            config.guardian.orchestrator_host is None
            or config.guardian.host == config.guardian.orchestrator_host
        )
        active_pools = []
        for p in config.pools:
            if not p.enabled:
                continue
            if p.enabled_when is not None and not evaluate_enabled_when(p.enabled_when):
                continue
            if p.locality == "local" and not is_orchestrator:
                continue
            active_pools.append(p)

        active_pool_names = {p.name for p in active_pools}

        # 3. Per-pool health check + reconciliation
        any_query_succeeded = False
        any_query_failed = False
        spawn_plan: list[tuple] = []

        for pool in active_pools:
            stale_secs = int(breaker.effective_stale_threshold)

            # 3a. Query Postgres for alive count
            alive_count = health.count_alive(
                pool.heartbeat_model, config.guardian.host, stale_secs,
            )
            if alive_count is None:
                any_query_failed = True
                alive_counts[pool.name] = None
                continue
            any_query_succeeded = True
            alive_counts[pool.name] = alive_count

            # 3b. PID liveness check for tracked workers
            deaths_this_cycle = 0
            for worker in tracker.get_pool_workers(pool.name):
                if worker.status in ("running", "starting"):
                    if not tracker.is_alive(worker.worker_id):
                        lived = time.time() - worker.started_at
                        log.warning(
                            "Worker %s (PID %s) died after %.1fs",
                            worker.worker_id, worker.pid, lived,
                        )
                        add_event(f"DIED: {worker.worker_id} (PID {worker.pid}) after {lived:.0f}s", "warn")
                        ps = tracker.get_pool_state(pool.name)
                        if lived < 10.0:
                            ps.rapid_death_count += 1
                        else:
                            ps.rapid_death_count = 0
                        tracker.remove_worker(worker.worker_id)
                        deaths_this_cycle += 1

            if deaths_this_cycle > 0 and alive_count is not None:
                alive_counts[pool.name] = max(0, alive_count - deaths_this_cycle)

            # 3c. Kill workers with stale heartbeats (only if not degraded)
            if not breaker.kills_suspended:
                heartbeats = health.get_worker_heartbeats(
                    pool.heartbeat_model, config.guardian.host,
                )
                if heartbeats is not None:
                    now = time.time()
                    for worker in tracker.get_pool_workers(pool.name):
                        if worker.status == "running":
                            age = now - worker.started_at
                            if age < stale_secs:
                                continue
                            hb = next(
                                (h for h in heartbeats if h["worker_id"] == worker.worker_id),
                                None,
                            )
                            if hb is None or _is_stale(hb["last_heartbeat_at"], stale_secs):
                                log.warning(
                                    "Worker %s has stale heartbeat, killing PID %s",
                                    worker.worker_id, worker.pid,
                                )
                                add_event(f"KILLED (stale): {worker.worker_id} (PID {worker.pid})", "warn")
                                tracker.kill_worker(worker.worker_id)
                                tracker.remove_worker(worker.worker_id)

            # 3d. Promote starting → running (survived >10s)
            for worker in tracker.get_pool_workers(pool.name):
                if worker.status == "starting" and (time.time() - worker.started_at) > 10.0:
                    worker.status = "running"

            # 3e. Spawn replacements if under target (skip if degraded)
            if breaker.kills_suspended:
                continue

            ps = tracker.get_pool_state(pool.name)
            if time.time() < ps.backoff_until:
                continue
            if ps.spawn_failure_count >= 5:
                continue

            effective_alive = alive_counts.get(pool.name) or 0
            deficit = pool.target_count - effective_alive
            if deficit > 0:
                to_spawn = min(deficit, MAX_SPAWN_PER_CYCLE)
                spawn_plan.append((pool, ps, to_spawn))
            elif effective_alive > pool.target_count:
                excess = effective_alive - pool.target_count
                workers = tracker.get_pool_workers(pool.name)
                workers_by_age = sorted(workers, key=lambda w: w.started_at, reverse=True)
                for w in workers_by_age[:excess]:
                    log.info("Culling excess worker %s (PID %s)", w.worker_id, w.pid)
                    add_event(f"CULLED: {w.worker_id} (PID {w.pid})", "warn")
                    tracker.kill_worker(w.worker_id)
                    tracker.remove_worker(w.worker_id)
                    alive_counts[pool.name] = max(0, (alive_counts.get(pool.name) or 0) - 1)

        # Spawn across all pools in parallel (per-pool stagger preserved)
        if spawn_plan:
            _spawn_lock = threading.Lock()

            def _spawn_pool(pool, ps, count):
                for _ in range(count):
                    if shutdown_requested:
                        break
                    with _spawn_lock:
                        if (alive_counts.get(pool.name) or 0) >= pool.target_count:
                            break
                        idx = tracker.get_next_worker_index(pool.name)
                    try:
                        proc, worker_id = spawn_worker(
                            pool, idx, os.getpid(), config.guardian.host, config.log_dir,
                        )
                        with _spawn_lock:
                            tracker.add_worker(pool.name, worker_id, proc)
                            ps.spawn_failure_count = 0
                            alive_counts[pool.name] = (alive_counts.get(pool.name) or 0) + 1
                            log.info("Spawned %s (PID %d)", worker_id, proc.pid)
                            add_event(f"Spawned {worker_id} (PID {proc.pid})")
                            render(
                                config=config, tracker=tracker, health=health,
                                breaker=breaker, started_at=started_at,
                                foreground=foreground, alive_counts=alive_counts,
                            )
                    except SpawnError as e:
                        with _spawn_lock:
                            ps.spawn_failure_count += 1
                            log.error("Spawn failed for pool %s: %s", pool.name, e)
                            add_event(f"SPAWN FAILED: {pool.name} - {e}", "error")
                        break
                    time.sleep(pool.stagger_seconds)

                with _spawn_lock:
                    if ps.rapid_death_count >= 3:
                        backoff = min(300, 30 * (2 ** (ps.rapid_death_count - 3)))
                        ps.backoff_until = time.time() + backoff
                        log.warning(
                            "Pool %s respawn backoff: %ds (%d rapid deaths)",
                            pool.name, backoff, ps.rapid_death_count,
                        )

            threads = [threading.Thread(target=_spawn_pool, args=(p, ps, c), daemon=True)
                       for p, ps, c in spawn_plan]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # Update circuit breaker
        now = time.time()
        if any_query_succeeded:
            breaker.on_query_success(now)
        if any_query_failed and not any_query_succeeded:
            breaker.on_query_failure(now)

        # 4. Drain workers in pools that became inactive
        for pool in config.pools:
            if pool.name not in active_pool_names:
                workers = tracker.get_pool_workers(pool.name)
                if workers:
                    worker_ids = [w.worker_id for w in workers]
                    health.set_draining(worker_ids)

        # 5. Persist state
        tracker.persist_state()

        # 6. Guardian self-heartbeat (Postgres + local file)
        _write_guardian_heartbeat(config.state_dir)
        summary = _build_fleet_summary(tracker, config)
        status = "degraded" if breaker.is_degraded else "running"
        cfg_hash = "|".join(pool_config_hash(p) for p in config.pools)
        health.upsert_guardian(
            host=config.guardian.host,
            pid=os.getpid(),
            version=__version__,
            status=status,
            config_hash=cfg_hash,
            fleet_summary=summary,
        )

        # 7. Live display
        render(
            config=config,
            tracker=tracker,
            health=health,
            breaker=breaker,
            started_at=started_at,
            foreground=foreground,
            alive_counts=alive_counts,
        )

        # 8. Log pruning (every 60s)
        if time.time() - last_log_prune > 60:
            prune_logs(config.log_dir, config.max_log_dir_bytes, config.log_retention_days)
            last_log_prune = time.time()

        # 9. Sleep
        elapsed = time.time() - cycle_start
        sleep_time = max(0, config.poll_interval_seconds - elapsed)
        _interruptible_sleep(sleep_time, lambda: shutdown_requested)
      except Exception:
        import traceback
        crash_msg = traceback.format_exc()
        log.exception("CRASH in main loop cycle")
        crash_path = os.path.join(config.state_dir, "crash.log")
        with open(crash_path, "a", encoding="utf-8") as f:
            f.write(f"\n--- {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n{crash_msg}\n")

    # ── Graceful shutdown ──
    stop_display()
    log.info("Shutdown requested, draining workers...")
    health.upsert_guardian(
        host=config.guardian.host,
        pid=os.getpid(),
        version=__version__,
        status="shutting_down",
        config_hash="",
        fleet_summary=_build_fleet_summary(tracker, config),
    )
    _graceful_shutdown(tracker, health, config.drain_grace_seconds)
    tracker.persist_state()
    _release_lock()
    log.info("Worker Guardian stopped")


def _safe_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0
