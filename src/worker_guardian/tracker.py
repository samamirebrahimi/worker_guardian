"""Process tracker — in-memory state, pidfiles, state persistence."""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import psutil

from .models import FleetState, PoolSpec, PoolState, WorkerState

log = logging.getLogger(__name__)


class ProcessTracker:
    def __init__(self, state_dir: str):
        self._state_dir = state_dir
        self._state_path = os.path.join(state_dir, "state.json")
        self._pids_dir = os.path.join(state_dir, "pids")
        self._fleet = FleetState()
        os.makedirs(self._pids_dir, exist_ok=True)

    # -- pool state access ---------------------------------------------------

    def get_pool_state(self, pool_name: str) -> PoolState:
        if pool_name not in self._fleet.pools:
            self._fleet.pools[pool_name] = PoolState(name=pool_name)
        return self._fleet.pools[pool_name]

    # -- worker management ---------------------------------------------------

    def add_worker(self, pool_name: str, worker_id: str, proc) -> None:
        ps = self.get_pool_state(pool_name)
        ws = WorkerState(
            pool_name=pool_name,
            worker_id=worker_id,
            pid=proc.pid,
            proc=proc,
            started_at=time.time(),
            status="starting",
        )
        ps.workers[worker_id] = ws
        self._write_pidfile(worker_id, proc.pid)

    def remove_worker(self, worker_id: str) -> None:
        for ps in self._fleet.pools.values():
            if worker_id in ps.workers:
                del ps.workers[worker_id]
                break
        self._delete_pidfile(worker_id)

    def is_alive(self, worker_id: str) -> bool:
        ws = self._find_worker(worker_id)
        if ws is None:
            return False
        if ws.proc is not None:
            return ws.proc.poll() is None
        if ws.pid is not None:
            return psutil.pid_exists(ws.pid)
        return False

    def kill_worker(self, worker_id: str) -> None:
        ws = self._find_worker(worker_id)
        if ws is None:
            return
        if ws.proc is not None:
            try:
                ws.proc.terminate()
                try:
                    ws.proc.wait(timeout=5)
                except Exception:
                    ws.proc.kill()
            except OSError:
                pass
        elif ws.pid is not None:
            try:
                p = psutil.Process(ws.pid)
                p.terminate()
                try:
                    p.wait(timeout=5)
                except psutil.TimeoutExpired:
                    p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def get_pool_workers(self, pool_name: str) -> list[WorkerState]:
        ps = self._fleet.pools.get(pool_name)
        if ps is None:
            return []
        return list(ps.workers.values())

    def get_all_workers(self) -> list[WorkerState]:
        result: list[WorkerState] = []
        for ps in self._fleet.pools.values():
            result.extend(ps.workers.values())
        return result

    def get_next_worker_index(self, pool_name: str) -> int:
        ps = self._fleet.pools.get(pool_name)
        if ps is None:
            return 0
        used = set()
        for wid in ps.workers:
            parts = wid.split("-")
            if len(parts) >= 2:
                try:
                    used.add(int(parts[1]))
                except ValueError:
                    pass
        idx = 0
        while idx in used:
            idx += 1
        return idx

    # -- state persistence ---------------------------------------------------

    def persist_state(self) -> None:
        data = {
            "guardian_pid": self._fleet.guardian_pid,
            "started_at": time.time(),
            "workers": {},
        }
        for ws in self.get_all_workers():
            data["workers"][ws.worker_id] = {
                "pid": ws.pid,
                "pool": ws.pool_name,
                "started_at": ws.started_at,
            }
        tmp_path = self._state_path + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, self._state_path)
        except OSError as e:
            log.warning("Failed to persist state: %s", e)

    def load_state(self) -> bool:
        if not os.path.exists(self._state_path):
            return False
        try:
            with open(self._state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._fleet.guardian_pid = data.get("guardian_pid", 0)
            return True
        except (json.JSONDecodeError, OSError, KeyError) as e:
            log.warning("Failed to load state file: %s", e)
            return False

    def set_guardian_pid(self, pid: int) -> None:
        self._fleet.guardian_pid = pid

    # -- orphan rediscovery --------------------------------------------------

    def rediscover_orphans(self, active_pools: list[PoolSpec]) -> dict[str, int]:
        adopted: dict[str, int] = {}
        pool_names = {p.name for p in active_pools}

        pidfiles = list(Path(self._pids_dir).glob("*.pid"))
        for pidfile in pidfiles:
            worker_id = pidfile.stem
            pool_name = worker_id.split("-")[0] if "-" in worker_id else ""
            if pool_name not in pool_names:
                pidfile.unlink(missing_ok=True)
                continue

            try:
                lines = pidfile.read_text().strip().split("\n")
                if len(lines) < 2:
                    pidfile.unlink(missing_ok=True)
                    continue
                pid = int(lines[0])
                create_time_stored = float(lines[1])
            except (ValueError, OSError):
                pidfile.unlink(missing_ok=True)
                continue

            try:
                p = psutil.Process(pid)
                actual_create_time = p.create_time()
                if abs(actual_create_time - create_time_stored) > 2.0:
                    log.debug("PID %d recycled (create_time mismatch), removing pidfile", pid)
                    pidfile.unlink(missing_ok=True)
                    continue
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pidfile.unlink(missing_ok=True)
                continue

            ps = self.get_pool_state(pool_name)
            ws = WorkerState(
                pool_name=pool_name,
                worker_id=worker_id,
                pid=pid,
                proc=None,
                started_at=create_time_stored,
                status="running",
            )
            ps.workers[worker_id] = ws
            adopted[pool_name] = adopted.get(pool_name, 0) + 1
            log.info("Adopted orphan %s (PID %d)", worker_id, pid)

        return adopted

    # -- foreign orphan reaping ----------------------------------------------

    def reap_foreign_orphans(
        self,
        pools: list[PoolSpec],
        dry_run: bool = False,
    ) -> list[dict]:
        """Kill worker processes orphaned from a DEAD parent that this
        guardian does not track.

        A process is a reap-able foreign orphan when ALL hold:
          * its command matches a configured worker signature — an exe
            basename or a ``-m <module>`` taken from some pool's spawn
            command, and
          * this guardian does not track its PID, and
          * its spawning parent is dead (parent PID gone, or recycled to a
            process younger than the worker).

        This is the blind spot the heartbeat reconciler misses: a worker whose
        spawner died but which keeps heartbeating is counted alive by
        ``count_alive`` — it masks the deficit (so no replacement spawns) yet
        cannot be culled (not tracked). Killing it lets its heartbeat go stale
        so the normal deficit → spawn path rebuilds it fresh.

        Workers this guardian spawned (live parent) or adopted across a restart
        (tracked via pidfile) are never touched — the dead-parent + not-tracked
        gate spares them. When ``dry_run`` is set, nothing is killed; the return
        value describes what WOULD be reaped. Returns a list of dicts:
        ``{"pid", "worker_id", "pool", "ppid"}``.
        """
        exe_names, modules = self._worker_signatures(pools)
        if not exe_names and not modules:
            return []

        tracked_pids = {w.pid for w in self.get_all_workers() if w.pid}
        protected = self._own_process_tree_pids()

        reaped: list[dict] = []
        for proc in psutil.process_iter(["pid", "name", "ppid", "cmdline", "create_time"]):
            try:
                pid = proc.info["pid"]
                if pid in tracked_pids or pid in protected:
                    continue
                name = proc.info.get("name") or ""
                cmdline = proc.info.get("cmdline") or []
                if not self._matches_worker_signature(name, cmdline, exe_names, modules):
                    continue
                if not self._parent_is_dead(proc):
                    continue

                worker_id = self._extract_worker_id(proc, cmdline)
                pool_name = worker_id.split("-")[0] if worker_id and "-" in worker_id else "?"
                entry = {
                    "pid": pid,
                    "worker_id": worker_id,
                    "pool": pool_name,
                    "ppid": proc.info.get("ppid"),
                }
                if not dry_run:
                    self._kill_process_tree(proc)
                    if worker_id:
                        # Drop any stale tracking + pidfile for this id.
                        self.remove_worker(worker_id)
                reaped.append(entry)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            except Exception:
                log.debug("orphan scan: unexpected error on a process", exc_info=True)
                continue
        return reaped

    @staticmethod
    def _worker_signatures(pools: list[PoolSpec]) -> tuple[set[str], set[str]]:
        """Derive worker process signatures from pool spawn commands so the
        reaper adapts to config instead of hardcoding module names."""
        exe_names: set[str] = set()
        modules: set[str] = set()
        for pool in pools:
            spawn = getattr(pool, "spawn", None)
            cmd = list(getattr(spawn, "command", None) or [])
            for i, tok in enumerate(cmd):
                low = str(tok).lower()
                if low.endswith(".exe") and "worker" in low:
                    exe_names.add(os.path.basename(low))
                if tok == "-m" and i + 1 < len(cmd):
                    modules.add(cmd[i + 1])
        return exe_names, modules

    @staticmethod
    def _matches_worker_signature(
        name: str, cmdline: list[str], exe_names: set[str], modules: set[str]
    ) -> bool:
        if name and name.lower() in exe_names:
            return True
        if cmdline and "-m" in cmdline:
            for m in modules:
                if m in cmdline:
                    return True
        return False

    @staticmethod
    def _parent_is_dead(proc) -> bool:
        """True when the process's spawning parent no longer exists (or its PID
        was recycled to a process younger than the worker → real parent gone).
        Conservative: returns False when liveness cannot be determined, so a
        worker with a possibly-live parent is never killed."""
        try:
            ppid = proc.ppid()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False
        if ppid is None or ppid <= 0:
            return True
        if not psutil.pid_exists(ppid):
            return True
        try:
            parent = psutil.Process(ppid)
            if parent.create_time() > proc.create_time():
                return True  # ppid recycled — the true parent is dead
        except psutil.NoSuchProcess:
            return True
        except psutil.AccessDenied:
            return False
        return False

    @staticmethod
    def _extract_worker_id(proc, cmdline: list[str]) -> str | None:
        if cmdline and "--worker-id" in cmdline:
            i = cmdline.index("--worker-id")
            if i + 1 < len(cmdline):
                return cmdline[i + 1]
        # llm-queue-worker.exe carries its id in the environment, not argv.
        try:
            wid = proc.environ().get("LLM_QUEUE_WORKER_ID")
            if wid:
                return wid
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass
        return None

    @staticmethod
    def _own_process_tree_pids() -> set[int]:
        """The guardian's own PID + ancestors — never reap these even if a
        signature somehow matched."""
        pids: set[int] = {os.getpid()}
        try:
            cur = psutil.Process(os.getpid())
            while cur is not None:
                pids.add(cur.pid)
                cur = cur.parent()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        return pids

    @staticmethod
    def _kill_process_tree(proc) -> None:
        try:
            targets = proc.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            targets = []
        targets.append(proc)
        for p in targets:
            try:
                p.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        _, alive = psutil.wait_procs(targets, timeout=5)
        for p in alive:
            try:
                p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    # -- pidfile management --------------------------------------------------

    def _write_pidfile(self, worker_id: str, pid: int) -> None:
        pidfile = os.path.join(self._pids_dir, f"{worker_id}.pid")
        try:
            with open(pidfile, "w") as f:
                f.write(f"{pid}\n{time.time()}\n")
        except OSError as e:
            log.warning("Failed to write pidfile for %s: %s", worker_id, e)

    def _delete_pidfile(self, worker_id: str) -> None:
        pidfile = os.path.join(self._pids_dir, f"{worker_id}.pid")
        try:
            os.remove(pidfile)
        except OSError:
            pass

    def _find_worker(self, worker_id: str) -> WorkerState | None:
        for ps in self._fleet.pools.values():
            if worker_id in ps.workers:
                return ps.workers[worker_id]
        return None
