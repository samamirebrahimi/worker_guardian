"""Live console dashboard for the guardian daemon using Rich."""
from __future__ import annotations

import os
import time
from collections import deque
from datetime import datetime

import psutil
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from . import __version__
from .config import evaluate_enabled_when
from .models import FleetConfig

_events: deque[tuple[float, str, str]] = deque(maxlen=12)
_live: Live | None = None
_console = Console()

WORKER_OK = "●"
WORKER_BAD = "○"

_CATEGORY_MAP = {
    "task:browser_fetch":  ("FETCH", "Playwright", "\U0001f310"),
    "task:scrapfly_fetch": ("FETCH", "Scrapfly",   "\U0001f310"),
    "task:cluster_cpu":    ("CPU",   "cluster",    "⚙️"),
}
_DEFAULT_CATEGORY = ("LLM", None, "\U0001f9e0")


def _collect_pool_stats(tracker) -> dict[str, tuple[int, float]]:
    """Return {pool_name: (total_rss_bytes, total_cpu_percent)} for tracked workers."""
    stats: dict[str, tuple[int, float]] = {}
    for pool_name, pool_state in tracker._fleet.pools.items():
        total_rss = 0
        total_cpu = 0.0
        for ws in pool_state.workers.values():
            try:
                p = psutil.Process(ws.pid)
                total_rss += p.memory_info().rss
                total_cpu += p.cpu_percent(interval=0)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        stats[pool_name] = (total_rss, total_cpu)
    return stats


def _format_bytes(n: int) -> str:
    if n < 1024 * 1024:
        return f"{n / 1024:.0f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.0f} MB"
    return f"{n / (1024 * 1024 * 1024):.1f} GB"


def _pool_display(pool) -> tuple[str, str, str]:
    for prefix, (cat, label, icon) in _CATEGORY_MAP.items():
        if pool.heartbeat_model.startswith(prefix):
            return icon, cat, label or pool.name
    return _DEFAULT_CATEGORY[2], _DEFAULT_CATEGORY[0], pool.name


def add_event(msg: str, level: str = "info") -> None:
    _events.append((time.time(), msg, level))


def _format_uptime(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m}m"


def _format_ago(seconds: float) -> str:
    if seconds < 1:
        return "now"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    return f"{int(seconds // 3600)}h ago"


def _pool_enabled(pool, config) -> bool:
    enabled = pool.enabled
    if pool.enabled_when is not None:
        enabled = evaluate_enabled_when(pool.enabled_when)
    is_orchestrator = (
        config.guardian.orchestrator_host is None
        or config.guardian.host == config.guardian.orchestrator_host
    )
    if pool.locality == "local" and not is_orchestrator:
        enabled = False
    return enabled


def _build_worker_figures(alive: int, target: int, status: str) -> Text:
    figures = Text()
    if status == "disabled":
        return figures
    alive_str = f" {WORKER_OK}" * alive
    deficit = target - alive
    deficit_str = f" {WORKER_BAD}" * deficit
    if status == "healthy":
        figures.append(alive_str, style="green")
    elif status == "spawning":
        figures.append(alive_str, style="green")
        figures.append(deficit_str, style="yellow")
    elif status == "backoff":
        figures.append(alive_str, style="green")
        figures.append(deficit_str, style="red")
    elif status == "error":
        figures.append(alive_str, style="green")
        figures.append(deficit_str, style="red")
    elif status == "unreachable":
        figures.append(f" {WORKER_BAD}" * target, style="dark_orange")
    return figures


def _build_dashboard(
    config: FleetConfig,
    tracker,
    health,
    breaker,
    started_at: float,
    alive_counts: dict[str, int | None],
    pool_stats: dict[str, tuple[int, float]] | None = None,
) -> Group:
    now = time.time()
    uptime = _format_uptime(now - started_at)
    ts = datetime.now().strftime("%H:%M:%S")

    # ── Header ──
    title = Text()
    title.append("\U0001f6e1️  Worker Guardian ", style="bold")
    title.append(f"v{__version__}", style="dim")

    info = Text()
    info.append(config.guardian.host, style="bold white")
    info.append("  ·  ", style="dim")
    info.append(f"PID {os.getpid()}", style="dim")
    info.append("  ·  ", style="dim")
    info.append("⏱ ", style="dim")
    info.append(f"up {uptime}", style="bold")
    info.append("  ·  ", style="dim")
    info.append(ts, style="dim")

    db_line = Text()
    if health.is_connected():
        db_line.append("\U0001f7e2 DB connected", style="green")
    else:
        db_line.append("\U0001f534 DB DISCONNECTED", style="red bold")
    db_line.append("  ·  ", style="dim")
    if breaker.is_degraded:
        db_line.append("DEGRADED", style="red bold")
    else:
        db_line.append("running", style="green")

    # ── Pool table ──
    table = Table(
        show_header=True,
        header_style="dim",
        box=box.SIMPLE_HEAD,
        padding=(0, 1),
        pad_edge=True,
    )
    table.add_column("TYPE", width=10, no_wrap=True)
    table.add_column("NAME", width=12, no_wrap=True)
    table.add_column("STATUS", width=10, no_wrap=True)
    table.add_column("COUNT", width=7, no_wrap=True, justify="left")
    table.add_column("RAM", width=8, no_wrap=True, justify="left")
    table.add_column("CPU", width=6, no_wrap=True, justify="left")
    table.add_column("WORKERS", min_width=30, no_wrap=True)

    active_count = 0
    total_alive = 0
    total_target = 0

    for pool in config.pools:
        icon, category, label = _pool_display(pool)
        enabled = _pool_enabled(pool, config)
        alive = alive_counts.get(pool.name)

        type_cell = Text()
        type_cell.append(f"{icon} ")
        type_cell.append(category)

        name_cell = Text(label)

        if not enabled:
            type_cell.stylize("dim")
            name_cell.stylize("dim")
            table.add_row(
                type_cell,
                name_cell,
                Text("disabled", style="dim italic"),
                Text("─", style="dim"),
                Text(""),
                Text(""),
                Text(""),
            )
            continue

        active_count += 1
        total_target += pool.target_count

        if alive is None:
            status_text = "unreachable"
            status_style = "dark_orange"
            count_text = "?"
            total_alive += 0
        elif alive >= pool.target_count:
            status_text = "healthy"
            status_style = "green"
            count_text = f"{alive}/{pool.target_count}"
            total_alive += alive
        else:
            ps = tracker.get_pool_state(pool.name)
            total_alive += alive
            if now < ps.backoff_until:
                status_text = "backoff"
                status_style = "red"
                count_text = f"{alive}/{pool.target_count}"
            elif ps.spawn_failure_count >= 5:
                status_text = "ERROR"
                status_style = "red bold"
                count_text = f"{alive}/{pool.target_count}"
            else:
                status_text = "spawning"
                status_style = "yellow"
                count_text = f"{alive}/{pool.target_count}"

        figures = _build_worker_figures(
            alive if alive is not None else 0,
            pool.target_count,
            status_text,
        )

        rss, cpu = (pool_stats or {}).get(pool.name, (0, 0.0))
        ram_text = Text(_format_bytes(rss), style="cyan") if rss > 0 else Text("─", style="dim")
        cpu_text = Text(f"{cpu:.1f}%", style="cyan") if rss > 0 else Text("─", style="dim")

        table.add_row(
            type_cell,
            name_cell,
            Text(status_text, style=status_style),
            Text(count_text),
            ram_text,
            cpu_text,
            figures,
        )

    # ── Summary ──
    summary = Text()
    summary.append(f"  {active_count} pools", style="dim")
    summary.append("  ·  ", style="dim")
    if total_alive >= total_target:
        summary.append(f"{total_alive}/{total_target}", style="green bold")
    elif total_alive == 0 and total_target > 0:
        summary.append(f"{total_alive}/{total_target}", style="red bold")
    else:
        summary.append(f"{total_alive}/{total_target}", style="yellow bold")
    summary.append(" workers", style="dim")

    # ── Events ──
    events_content = Text()
    if _events:
        event_icons = {
            "adopted": "\U0001f4e6",
            "spawned": "\U0001f423",
            "died":    "\U0001f480",
            "killed":  "\U0001fa94",
            "failed":  "❌",
        }
        for i, (ts_ev, msg, level) in enumerate(reversed(list(_events))):
            if i > 0:
                events_content.append("\n")
            ago = _format_ago(now - ts_ev)
            events_content.append(f"  {ago:>8}  ", style="dim")

            ev_icon = ""
            msg_lower = msg.lower()
            if "adopted" in msg_lower:
                ev_icon = event_icons["adopted"]
            elif "spawned" in msg_lower:
                ev_icon = event_icons["spawned"]
            elif "died" in msg_lower:
                ev_icon = event_icons["died"]
            elif "killed" in msg_lower:
                ev_icon = event_icons["killed"]
            elif "failed" in msg_lower:
                ev_icon = event_icons["failed"]

            if level == "error":
                events_content.append(f"{ev_icon} ", style="red")
                events_content.append(msg, style="red")
            elif level == "warn":
                events_content.append(f"{ev_icon} ", style="dim yellow")
                events_content.append(msg, style="dim yellow")
            else:
                events_content.append(f"{ev_icon} ", style="dim")
                events_content.append(msg, style="dim")
    else:
        events_content.append("  No events yet", style="dim italic")

    return Group(
        Text(""),
        title,
        info,
        db_line,
        Text(""),
        Rule(title="\U0001f6e1️ The Fleet", style="dim", align="left", characters="═"),
        Text(""),
        table,
        Text(""),
        summary,
        Text(""),
        Rule(title="\U0001f4cb Events", style="dim", align="left", characters="═"),
        Text(""),
        events_content,
        Text(""),
    )


def render(
    config: FleetConfig,
    tracker,
    health,
    breaker,
    started_at: float,
    foreground: bool,
    alive_counts: dict[str, int | None],
) -> None:
    if not foreground:
        return

    global _live

    pool_stats = _collect_pool_stats(tracker)
    dashboard = _build_dashboard(
        config, tracker, health, breaker, started_at, alive_counts, pool_stats,
    )

    if _live is None:
        _live = Live(
            dashboard,
            console=_console,
            refresh_per_second=1,
            screen=True,
        )
        _live.start()
        return
    _live.update(dashboard)


def stop() -> None:
    global _live
    if _live is not None:
        _live.stop()
        _live = None
