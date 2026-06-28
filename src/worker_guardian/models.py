"""Data types for worker-guardian fleet management."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SpawnSpec:
    command: list[str]
    cwd: str | None = None
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class PoolSpec:
    name: str
    heartbeat_model: str
    spawn: SpawnSpec
    enabled: bool = True
    enabled_when: str | None = None
    locality: str = "local"
    target_count: int = 1
    stagger_seconds: float = 1.5
    log_prefix: str = ""


@dataclass
class WorkerState:
    pool_name: str
    worker_id: str
    pid: int | None = None
    proc: Any = None
    started_at: float = 0.0
    status: str = "starting"


@dataclass
class PoolState:
    name: str
    workers: dict[str, WorkerState] = field(default_factory=dict)
    rapid_death_count: int = 0
    backoff_until: float = 0.0
    spawn_failure_count: int = 0
    config_hash: str = ""


@dataclass
class FleetState:
    guardian_pid: int = 0
    pools: dict[str, PoolState] = field(default_factory=dict)


@dataclass
class GuardianConfig:
    host: str
    orchestrator_host: str | None = None
    database_url: str = ""


@dataclass
class FleetConfig:
    guardian: GuardianConfig
    log_dir: str
    state_dir: str
    stale_threshold_seconds: int = 90
    poll_interval_seconds: int = 12
    max_log_dir_bytes: int = 2_147_483_648
    log_retention_days: int = 7
    drain_grace_seconds: int = 90
    pools: list[PoolSpec] = field(default_factory=list)
