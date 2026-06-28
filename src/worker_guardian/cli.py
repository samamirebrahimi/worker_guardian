"""CLI interface for worker-guardian."""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import click

from . import __version__


def _setup_logging(log_level: str, foreground: bool) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = []
    if foreground:
        log_dir = os.path.join(os.environ.get("LOCALAPPDATA", "."), "worker-guardian")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "guardian.log")
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers or None)
    logging.getLogger("worker_guardian").setLevel(level)


@click.group()
@click.version_option(version=__version__)
def main():
    """Worker Guardian -- fleet manager daemon."""


@main.command()
@click.option("--config", "-c", default="fleet.yaml", help="Path to fleet YAML config")
@click.option("--foreground", is_flag=True, default=True,
              help="Run in foreground (default; use pythonw for background)")
@click.option("--log-level", default="INFO",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False))
def start(config, foreground, log_level):
    """Start the guardian daemon."""
    _setup_logging(log_level, foreground)
    from .daemon import run
    run(config_path=config, foreground=foreground)


@main.command()
@click.option("--config", "-c", default="fleet.yaml", help="Path to fleet YAML config")
@click.option("--timeout", default=90, help="Drain grace period in seconds")
def stop(config, timeout):
    """Stop the running guardian and drain all workers."""
    from .config import load_config

    try:
        cfg = load_config(config)
    except Exception as e:
        click.echo(f"Failed to load config: {e}", err=True)
        raise SystemExit(1)

    pid_path = os.path.join(cfg.state_dir, "guardian.pid")
    if not os.path.exists(pid_path):
        click.echo("No guardian PID file found. Is the daemon running?", err=True)
        raise SystemExit(1)

    try:
        pid = int(Path(pid_path).read_text().strip())
    except (ValueError, OSError):
        click.echo("Could not read guardian PID file.", err=True)
        raise SystemExit(1)

    import psutil
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        click.echo(f"Sent SIGTERM to guardian (PID {pid}), waiting up to {timeout}s...")
        try:
            proc.wait(timeout=timeout)
            click.echo("Guardian stopped.")
        except psutil.TimeoutExpired:
            click.echo("Guardian did not exit within timeout, force-killing.", err=True)
            proc.kill()
    except psutil.NoSuchProcess:
        click.echo(f"Guardian process (PID {pid}) is not running.", err=True)
    except psutil.AccessDenied:
        click.echo(f"Access denied terminating PID {pid}.", err=True)
        raise SystemExit(1)


@main.command()
@click.option("--config", "-c", default="fleet.yaml", help="Path to fleet YAML config")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output")
@click.option("--global", "show_global", is_flag=True, help="Show all guardians across machines")
def status(config, as_json, show_global):
    """Show current fleet status."""
    from .config import evaluate_enabled_when, load_config
    from .health import HealthPoller

    try:
        cfg = load_config(config)
    except Exception as e:
        click.echo(f"Failed to load config: {e}", err=True)
        raise SystemExit(1)

    health = HealthPoller(cfg.guardian.database_url)

    if show_global:
        guardians = health.get_all_guardians()
        if guardians is None:
            click.echo("Could not connect to Postgres.", err=True)
            raise SystemExit(1)
        if as_json:
            click.echo(json.dumps(guardians, default=str, indent=2))
            return
        click.echo("Worker Guardian -- Global Fleet Status")
        click.echo("=" * 60)
        click.echo()
        click.echo(f"{'Host':<20} {'Version':<10} {'Status':<12} {'Last Check':<12} {'Workers'}")
        click.echo("-" * 20 + "  " + "-" * 8 + "  " + "-" * 10 + "  " + "-" * 10 + "  " + "-" * 20)
        for g in guardians:
            ago = ""
            if g.get("last_heartbeat_at"):
                hb = g["last_heartbeat_at"]
                if hasattr(hb, "timestamp"):
                    secs = time.time() - hb.timestamp()
                else:
                    secs = time.time() - float(hb)
                ago = f"{int(secs)}s ago"

            summary_parts = []
            fleet = g.get("fleet_summary") or {}
            if isinstance(fleet, str):
                try:
                    fleet = json.loads(fleet)
                except (json.JSONDecodeError, TypeError):
                    fleet = {}
            for pool_name, info in fleet.items():
                prefix = pool_name[:2].upper()
                alive = info.get("alive", "?")
                summary_parts.append(f"{prefix}:{alive}")
            workers_str = " ".join(summary_parts) if summary_parts else "-"

            status_str = g.get("status", "?")
            if status_str == "degraded":
                status_str += " !"

            click.echo(f"{g.get('host', '?'):<20} {g.get('version', '?'):<10} "
                        f"{status_str:<12} {ago:<12} {workers_str}")
        return

    # Local status
    state_path = os.path.join(cfg.state_dir, "state.json")
    state_data = {}
    if os.path.exists(state_path):
        try:
            with open(state_path, "r") as f:
                state_data = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass

    hb_path = os.path.join(cfg.state_dir, "guardian_heartbeat")
    last_check_ago = "?"
    if os.path.exists(hb_path):
        try:
            hb_time = float(Path(hb_path).read_text().strip())
            last_check_ago = f"{int(time.time() - hb_time)}s ago"
        except (ValueError, OSError):
            pass

    pool_rows: list[dict] = []
    for pool in cfg.pools:
        enabled = pool.enabled
        if pool.enabled_when is not None:
            enabled = evaluate_enabled_when(pool.enabled_when)

        alive = health.count_alive(
            pool.heartbeat_model, cfg.guardian.host, cfg.stale_threshold_seconds,
        )

        if not enabled:
            reason = ""
            if pool.enabled_when:
                reason = f" ({pool.enabled_when})"
            pool_rows.append({
                "pool": pool.name, "target": pool.target_count,
                "alive": "-", "status": f"disabled{reason}",
            })
        elif alive is None:
            pool_rows.append({
                "pool": pool.name, "target": pool.target_count,
                "alive": "?", "status": "db unreachable",
            })
        elif alive >= pool.target_count:
            pool_rows.append({
                "pool": pool.name, "target": pool.target_count,
                "alive": alive, "status": "healthy",
            })
        else:
            deficit = pool.target_count - alive
            pool_rows.append({
                "pool": pool.name, "target": pool.target_count,
                "alive": alive, "status": f"{deficit} respawning",
            })

    if as_json:
        click.echo(json.dumps({
            "host": cfg.guardian.host,
            "version": __version__,
            "guardian_pid": state_data.get("guardian_pid"),
            "last_check": last_check_ago,
            "pools": pool_rows,
        }, indent=2))
        return

    guardian_pid = state_data.get("guardian_pid", "?")
    click.echo("Worker Guardian -- Fleet Status")
    click.echo("=" * 55)
    click.echo()
    click.echo(f"{'Pool':<16} {'Target':>6}  {'Alive':>5}  Status")
    click.echo("-" * 16 + "  " + "-" * 6 + "  " + "-" * 5 + "  " + "-" * 20)
    for row in pool_rows:
        click.echo(f"{row['pool']:<16} {row['target']:>6}  {str(row['alive']):>5}  {row['status']}")
    click.echo()

    db_status = "connected" if health.is_connected() else "disconnected"
    click.echo(f"Guardian PID: {guardian_pid} | Host: {cfg.guardian.host} | "
               f"Last check: {last_check_ago}")
    click.echo(f"Postgres: {db_status}")


@main.command()
@click.option("--config", "-c", default="fleet.yaml", help="Path to fleet YAML config")
def reload(config):
    """Signal the guardian to re-read fleet config."""
    from .config import load_config

    try:
        cfg = load_config(config)
    except Exception as e:
        click.echo(f"Failed to load config: {e}", err=True)
        raise SystemExit(1)

    sentinel = os.path.join(cfg.state_dir, "reload_requested")
    try:
        Path(sentinel).touch()
        click.echo(f"Reload requested. Guardian will pick it up within "
                    f"{cfg.poll_interval_seconds}s.")
    except OSError as e:
        click.echo(f"Failed to create reload sentinel: {e}", err=True)
        raise SystemExit(1)


@main.command(name="drain-pool")
@click.argument("pool_name")
@click.option("--config", "-c", default="fleet.yaml", help="Path to fleet YAML config")
def drain_pool(pool_name, config):
    """Signal workers in a pool to drain (for rolling upgrades)."""
    from .config import load_config
    from .health import HealthPoller

    try:
        cfg = load_config(config)
    except Exception as e:
        click.echo(f"Failed to load config: {e}", err=True)
        raise SystemExit(1)

    pool = next((p for p in cfg.pools if p.name == pool_name), None)
    if pool is None:
        click.echo(f"Pool '{pool_name}' not found in config.", err=True)
        raise SystemExit(1)

    health = HealthPoller(cfg.guardian.database_url)
    heartbeats = health.get_worker_heartbeats(pool.heartbeat_model, cfg.guardian.host)
    if heartbeats is None:
        click.echo("Could not connect to Postgres.", err=True)
        raise SystemExit(1)

    worker_ids = [h["worker_id"] for h in heartbeats if h.get("status") != "draining"]
    if not worker_ids:
        click.echo(f"No active workers found for pool '{pool_name}'.")
        return

    n = health.set_draining(worker_ids)
    click.echo(f"Drain signal sent to {n} workers in pool '{pool_name}'.")
    click.echo("Workers will finish current work and exit. "
               "Guardian will respawn replacements after they exit.")
