"""Postgres heartbeat poller — monitors llm_workers and guardian_instances."""
from __future__ import annotations

import json
import logging
import time

import psycopg

log = logging.getLogger(__name__)

_RECONNECT_BACKOFF = (1.0, 2.0, 5.0, 10.0, 30.0, 60.0)


class HealthPoller:
    def __init__(self, database_url: str):
        self._dsn = database_url
        self._conn: psycopg.Connection | None = None
        self._consecutive_failures = 0
        self._last_warning_at = 0.0

    # -- connection management -----------------------------------------------

    def connect(self) -> bool:
        if self._conn is not None:
            try:
                self._conn.execute("SELECT 1")
                return True
            except Exception:
                self._close()

        try:
            self._conn = psycopg.connect(self._dsn, autocommit=True)
            self._consecutive_failures = 0
            return True
        except Exception:
            self._consecutive_failures += 1
            delay = _RECONNECT_BACKOFF[
                min(self._consecutive_failures - 1, len(_RECONNECT_BACKOFF) - 1)
            ]
            now = time.time()
            if now - self._last_warning_at > 60:
                log.warning(
                    "Postgres connect failed (attempt %d, next retry in %.0fs)",
                    self._consecutive_failures, delay, exc_info=True,
                )
                self._last_warning_at = now
            return False

    def _close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def is_connected(self) -> bool:
        return self._conn is not None

    # -- queries -------------------------------------------------------------

    def count_alive(
        self,
        heartbeat_model: str,
        worker_host: str,
        stale_seconds: int = 90,
    ) -> int | None:
        if not self.connect():
            return None
        try:
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                cur.execute(
                    """
                    SELECT COUNT(*) FROM llm_workers
                     WHERE (model = %(model)s OR %(model)s = ANY(served_models))
                       AND worker_host = %(host)s
                       AND last_heartbeat_at > NOW() - make_interval(secs => %(stale)s)
                    """,
                    {"model": heartbeat_model, "host": worker_host, "stale": stale_seconds},
                )
                row = cur.fetchone()
                return row[0] if row else 0
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._close()
            self._consecutive_failures += 1
            return None

    def get_worker_heartbeats(
        self,
        heartbeat_model: str,
        worker_host: str,
    ) -> list[dict] | None:
        if not self.connect():
            return None
        try:
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                cur.execute(
                    """
                    SELECT worker_id, last_heartbeat_at, status
                      FROM llm_workers
                     WHERE (model = %(model)s OR %(model)s = ANY(served_models))
                       AND worker_host = %(host)s
                    """,
                    {"model": heartbeat_model, "host": worker_host},
                )
                cols = [d[0] for d in cur.description] if cur.description else []
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._close()
            self._consecutive_failures += 1
            return None

    def set_draining(self, worker_ids: list[str]) -> int:
        if not worker_ids:
            return 0
        if not self.connect():
            return 0
        try:
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                cur.execute(
                    """
                    UPDATE llm_workers
                       SET status = 'draining'
                     WHERE worker_id = ANY(%(ids)s)
                       AND status NOT IN ('draining', 'offline')
                    """,
                    {"ids": worker_ids},
                )
                return cur.rowcount
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._close()
            return 0

    def count_alive_global(self, heartbeat_model: str, stale_seconds: int = 90) -> int | None:
        """Count alive workers for a model across ALL hosts (for status --global)."""
        if not self.connect():
            return None
        try:
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                cur.execute(
                    """
                    SELECT COUNT(*) FROM llm_workers
                     WHERE (model = %(model)s OR %(model)s = ANY(served_models))
                       AND last_heartbeat_at > NOW() - make_interval(secs => %(stale)s)
                    """,
                    {"model": heartbeat_model, "stale": stale_seconds},
                )
                row = cur.fetchone()
                return row[0] if row else 0
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._close()
            self._consecutive_failures += 1
            return None

    # -- guardian instances ---------------------------------------------------

    def upsert_guardian(
        self,
        host: str,
        pid: int,
        version: str,
        status: str,
        config_hash: str,
        fleet_summary: dict,
    ) -> bool:
        if not self.connect():
            return False
        try:
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                cur.execute(
                    """
                    INSERT INTO guardian_instances
                        (host, guardian_pid, version, status, config_hash,
                         last_heartbeat_at, fleet_summary, started_at)
                    VALUES
                        (%(host)s, %(pid)s, %(ver)s, %(status)s, %(hash)s,
                         NOW(), %(summary)s::jsonb, NOW())
                    ON CONFLICT (host) DO UPDATE SET
                        guardian_pid      = EXCLUDED.guardian_pid,
                        version           = EXCLUDED.version,
                        status            = EXCLUDED.status,
                        config_hash       = EXCLUDED.config_hash,
                        last_heartbeat_at = NOW(),
                        fleet_summary     = EXCLUDED.fleet_summary
                    """,
                    {
                        "host": host,
                        "pid": pid,
                        "ver": version,
                        "status": status,
                        "hash": config_hash,
                        "summary": json.dumps(fleet_summary),
                    },
                )
                return True
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._close()
            return False
        except psycopg.errors.UndefinedTable:
            log.debug("guardian_instances table does not exist yet, skipping upsert")
            return False

    def get_all_guardians(self) -> list[dict] | None:
        if not self.connect():
            return None
        try:
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                cur.execute(
                    """
                    SELECT host, guardian_pid, version, status, config_hash,
                           last_heartbeat_at, fleet_summary, started_at
                      FROM guardian_instances
                     ORDER BY host
                    """
                )
                cols = [d[0] for d in cur.description] if cur.description else []
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._close()
            return None
        except psycopg.errors.UndefinedTable:
            return []

    def ensure_guardian_table(self) -> bool:
        if not self.connect():
            return False
        try:
            with self._conn.cursor() as cur:  # type: ignore[union-attr]
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS guardian_instances (
                        host              TEXT PRIMARY KEY,
                        guardian_pid      INT NOT NULL,
                        version           TEXT,
                        status            TEXT DEFAULT 'running',
                        config_hash       TEXT,
                        last_heartbeat_at TIMESTAMPTZ DEFAULT NOW(),
                        fleet_summary     JSONB,
                        started_at        TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            return True
        except Exception:
            log.warning("Failed to create guardian_instances table", exc_info=True)
            return False
