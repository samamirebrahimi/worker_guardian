"""Process spawner — launches workers as invisible background processes."""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time

from .config import interpolate_vars
from .models import PoolSpec

log = logging.getLogger(__name__)

CREATE_NO_WINDOW = 0x08000000


class SpawnError(Exception):
    pass


def spawn_worker(
    pool: PoolSpec,
    worker_index: int,
    guardian_pid: int,
    guardian_host: str,
    log_dir: str,
) -> tuple[subprocess.Popen, str]:
    """Spawn a single worker process.

    Returns (proc, worker_id).
    Raises SpawnError on failure.
    """
    worker_id = f"{pool.name}-{worker_index:02d}-{guardian_host}-{guardian_pid}"

    resolved_cmd = [interpolate_vars(arg, worker_id) for arg in pool.spawn.command]

    merged_env = os.environ.copy()
    venv_scripts = os.path.dirname(sys.executable)
    current_path = merged_env.get("PATH", "")
    if venv_scripts not in current_path:
        merged_env["PATH"] = venv_scripts + os.pathsep + current_path
    merged_env["VIRTUAL_ENV"] = os.path.dirname(venv_scripts)

    for key, val_template in pool.spawn.env.items():
        merged_env[key] = interpolate_vars(val_template, worker_id)

    log_subdir = os.path.join(log_dir, pool.log_prefix or pool.name)
    os.makedirs(log_subdir, exist_ok=True)
    date_str = time.strftime("%Y-%m-%d")
    log_path = os.path.join(
        log_subdir, f"{pool.log_prefix or pool.name}_{worker_index:02d}_{date_str}.log"
    )
    log_fh = open(log_path, "a", encoding="utf-8")

    try:
        proc = subprocess.Popen(
            resolved_cmd,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            cwd=pool.spawn.cwd,
            env=merged_env,
            creationflags=CREATE_NO_WINDOW,
            close_fds=False,
            shell=False,
        )
    except Exception as exc:
        log_fh.close()
        raise SpawnError(f"Failed to spawn {worker_id}: {exc}") from exc

    log_fh.close()
    return proc, worker_id
