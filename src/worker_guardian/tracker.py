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
