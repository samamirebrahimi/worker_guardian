"""Fleet config loader with YAML parsing, validation, ${VAR} interpolation,
and enabled_when DSL evaluation."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import sys
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, field_validator

from .models import FleetConfig, GuardianConfig, PoolSpec, SpawnSpec

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pydantic schema for validation
# ---------------------------------------------------------------------------

class _SpawnSchema(BaseModel):
    command: list[str]
    cwd: str | None = None
    env: dict[str, str] = {}


class _PoolSchema(BaseModel):
    enabled: bool = True
    enabled_when: str | None = None
    locality: str = "local"
    target_count: int = 1
    stagger_seconds: float = 1.5
    heartbeat_model: str
    log_prefix: str = ""
    spawn: _SpawnSchema

    @field_validator("locality")
    @classmethod
    def _check_locality(cls, v: str) -> str:
        if v not in ("local", "remote"):
            raise ValueError(f"locality must be 'local' or 'remote', got '{v}'")
        return v


class _GuardianSchema(BaseModel):
    host: str = "auto"
    orchestrator_host: str | None = None
    database_url: str


class _DefaultsSchema(BaseModel):
    log_dir: str
    stale_threshold_seconds: int = 90
    poll_interval_seconds: int = 12
    state_dir: str = "%LOCALAPPDATA%\\worker-guardian"
    max_log_dir_bytes: int = 2_147_483_648
    log_retention_days: int = 7
    drain_grace_seconds: int = 90


class _FleetSchema(BaseModel):
    version: int = 1
    guardian: _GuardianSchema
    defaults: _DefaultsSchema
    pools: dict[str, _PoolSchema] = {}


# ---------------------------------------------------------------------------
# ${VAR} interpolation
# ---------------------------------------------------------------------------

_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def interpolate_vars(template: str, worker_id: str = "") -> str:
    """Resolve ${VAR} references.  Special vars: PYTHON_EXE, WORKER_ID,
    VENV_SCRIPTS.  Others resolved from os.environ."""
    specials = {
        "PYTHON_EXE": sys.executable,
        "WORKER_ID": worker_id,
        "VENV_SCRIPTS": str(Path(sys.executable).parent),
    }

    def _replace(m: re.Match) -> str:
        name = m.group(1)
        if name in specials:
            return specials[name]
        val = os.environ.get(name)
        if val is None:
            raise KeyError(f"Environment variable ${{{name}}} is not set")
        return val

    return _VAR_RE.sub(_replace, template)


def _expand_windows_vars(path: str) -> str:
    """Expand %ENVVAR% references in paths (Windows convention)."""
    return re.sub(
        r"%([^%]+)%",
        lambda m: os.environ.get(m.group(1), m.group(0)),
        path,
    )


# ---------------------------------------------------------------------------
# enabled_when DSL
# ---------------------------------------------------------------------------

class _EnvEquals:
    __slots__ = ("var", "val")
    def __init__(self, var: str, val: str):
        self.var = var
        self.val = val
    def evaluate(self) -> bool:
        return os.environ.get(self.var, "") == self.val


class _EnvNotEquals:
    __slots__ = ("var", "val")
    def __init__(self, var: str, val: str):
        self.var = var
        self.val = val
    def evaluate(self) -> bool:
        return os.environ.get(self.var, "") != self.val


class _EnvIn:
    __slots__ = ("var", "vals")
    def __init__(self, var: str, vals: list[str]):
        self.var = var
        self.vals = vals
    def evaluate(self) -> bool:
        return os.environ.get(self.var, "") in self.vals


class _And:
    __slots__ = ("left", "right")
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right
    def evaluate(self) -> bool:
        return self.left.evaluate() and self.right.evaluate()


class _Or:
    __slots__ = ("left", "right")
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right
    def evaluate(self) -> bool:
        return self.left.evaluate() or self.right.evaluate()


_TOKEN_RE = re.compile(
    r"""'([^']*)'|"([^"]*)"|(\w+|==|!=|[(),])""",
    re.ASCII,
)


def _tokenize(expr: str) -> list[str]:
    tokens: list[str] = []
    for m in _TOKEN_RE.finditer(expr):
        if m.group(1) is not None:
            tokens.append(m.group(1))
        elif m.group(2) is not None:
            tokens.append(m.group(2))
        else:
            tokens.append(m.group(3))
    return tokens


def parse_enabled_when(expr: str) -> Any:
    """Parse an enabled_when DSL expression into an evaluatable AST node."""
    tokens = _tokenize(expr)
    pos = [0]

    def _peek() -> str | None:
        return tokens[pos[0]] if pos[0] < len(tokens) else None

    def _consume(expected: str | None = None) -> str:
        tok = tokens[pos[0]]
        if expected is not None and tok != expected:
            raise ValueError(f"Expected '{expected}', got '{tok}' in: {expr}")
        pos[0] += 1
        return tok

    def _parse_atom() -> Any:
        var = _consume()
        op = _peek()
        if op == "==":
            _consume("==")
            val = _consume()
            return _EnvEquals(var, val)
        elif op == "!=":
            _consume("!=")
            val = _consume()
            return _EnvNotEquals(var, val)
        elif op == "in":
            _consume("in")
            _consume("(")
            vals: list[str] = []
            while _peek() != ")":
                v = _consume()
                if v == ",":
                    continue
                vals.append(v)
            _consume(")")
            return _EnvIn(var, vals)
        else:
            raise ValueError(f"Unknown operator '{op}' after '{var}' in: {expr}")

    def _parse_or() -> Any:
        left = _parse_and()
        while _peek() == "or":
            _consume("or")
            right = _parse_and()
            left = _Or(left, right)
        return left

    def _parse_and() -> Any:
        left = _parse_atom()
        while _peek() == "and":
            _consume("and")
            right = _parse_atom()
            left = _And(left, right)
        return left

    node = _parse_or()
    if pos[0] != len(tokens):
        raise ValueError(f"Unexpected tokens after position {pos[0]} in: {expr}")
    return node


def evaluate_enabled_when(expression: str | None) -> bool:
    if expression is None:
        return True
    return parse_enabled_when(expression).evaluate()


# ---------------------------------------------------------------------------
# Config hash
# ---------------------------------------------------------------------------

def pool_config_hash(pool: PoolSpec) -> str:
    blob = json.dumps(
        {"cmd": pool.spawn.command, "cwd": pool.spawn.cwd, "env": pool.spawn.env},
        sort_keys=True,
    )
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(path: str) -> FleetConfig:
    """Load and validate a fleet YAML config file."""
    raw = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Fleet config must be a YAML mapping, got {type(data).__name__}")

    schema = _FleetSchema(**data)

    host = schema.guardian.host
    if host == "auto":
        host = socket.gethostname()

    guardian = GuardianConfig(
        host=host,
        orchestrator_host=schema.guardian.orchestrator_host,
        database_url=schema.guardian.database_url,
    )

    state_dir = _expand_windows_vars(schema.defaults.state_dir)

    pools: list[PoolSpec] = []
    for name, pool_schema in schema.pools.items():
        # Pre-validate enabled_when syntax at load time
        if pool_schema.enabled_when:
            parse_enabled_when(pool_schema.enabled_when)

        pools.append(PoolSpec(
            name=name,
            heartbeat_model=pool_schema.heartbeat_model,
            spawn=SpawnSpec(
                command=pool_schema.spawn.command,
                cwd=pool_schema.spawn.cwd,
                env=dict(pool_schema.spawn.env),
            ),
            enabled=pool_schema.enabled,
            enabled_when=pool_schema.enabled_when,
            locality=pool_schema.locality,
            target_count=pool_schema.target_count,
            stagger_seconds=pool_schema.stagger_seconds,
            log_prefix=pool_schema.log_prefix or name,
        ))

    return FleetConfig(
        guardian=guardian,
        log_dir=schema.defaults.log_dir,
        state_dir=state_dir,
        stale_threshold_seconds=schema.defaults.stale_threshold_seconds,
        poll_interval_seconds=schema.defaults.poll_interval_seconds,
        max_log_dir_bytes=schema.defaults.max_log_dir_bytes,
        log_retention_days=schema.defaults.log_retention_days,
        drain_grace_seconds=schema.defaults.drain_grace_seconds,
        pools=pools,
    )
