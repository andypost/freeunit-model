"""FreeUnit Schedule Subsystem Simulation (PR #452 & plan-cron.md).

Models:
- Main router engine schedule timer firing and in_flight concurrency tracking.
- Synthetic HTTP request generation using DEVNULL protocol table.
- Dedicated capacity isolation (jobs_app with isolated timeout and process quota).
- Separate run-context deadline timer (preventing r->timer collision).
- Concurrency skips, timeout aborts, and non-ready engine checks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

from unit_model.c_types import HttpHeaderField, UnitRequest, UnitResponse
from unit_model.engine.event_loop import SimEventLoop
from unit_model.engine.listener import Connection, ConnState
from unit_model.engine.router import RouteAction, Router


@dataclass
class ScheduleConfig:
    name: str
    target_path: str
    target_app: str = "jobs_app"
    interval_ms: int = 60_000
    timeout_ms: int = 300_000
    max_in_flight: int = 1


@dataclass
class ScheduleRunContext:
    run_id: int
    schedule_name: str
    stream_id: int
    start_time_ns: int
    completed: bool = False
    timed_out: bool = False


class ScheduleRunner:
    """Simulated FreeUnit PR 452 schedule execution engine."""

    def __init__(self, loop: SimEventLoop, router: Router):
        self.loop = loop
        self.router = router
        self.schedules: Dict[str, ScheduleConfig] = {}
        self.in_flight: Dict[str, int] = {}
        self.runs: Dict[int, ScheduleRunContext] = {}
        self.next_run_id: int = 1
        self.total_invocations: int = 0
        self.total_skips: int = 0
        self.total_timeouts: int = 0

    def add_schedule(self, config: ScheduleConfig) -> None:
        self.schedules[config.name] = config
        self.in_flight[config.name] = 0
        self.loop.record_trace("scheduler", "schedule_added", name=config.name, path=config.target_path)

    def trigger_schedule(self, schedule_name: str) -> bool:
        """Trigger a scheduled run (due occurrence)."""
        cfg = self.schedules.get(schedule_name)
        if not cfg:
            return False

        # Invariant: Concurrency cap check
        if self.in_flight[schedule_name] >= cfg.max_in_flight:
            self.total_skips += 1
            self.loop.record_trace(
                "scheduler",
                "schedule_concurrency_skip",
                name=schedule_name,
                in_flight=self.in_flight[schedule_name],
            )
            return False

        # Check router engine 0 readiness
        if not self.router.engines or not self.router.workers.get(cfg.target_app):
            self.loop.record_trace(
                "scheduler",
                "schedule_engine_not_ready",
                name=schedule_name,
                target_app=cfg.target_app,
            )
            return False

        run_id = self.next_run_id
        self.next_run_id += 1
        self.in_flight[schedule_name] += 1
        self.total_invocations += 1

        run_ctx = ScheduleRunContext(
            run_id=run_id,
            schedule_name=schedule_name,
            stream_id=0,
            start_time_ns=self.loop.now(),
        )
        self.runs[run_id] = run_ctx

        # Synthesize DEVNULL HTTP request
        syn_req = UnitRequest(
            method="GET",
            version="HTTP/1.1",
            remote="127.0.0.1",
            local_addr="127.0.0.1",
            local_port="0",
            tls=False,
            websocket_handshake=False,
            app_target=0,
            server_name="localhost",
            target=cfg.target_path,
            path=cfg.target_path.split("?")[0],
            query=cfg.target_path.split("?")[1] if "?" in cfg.target_path else "",
            headers=[
                HttpHeaderField(name="Host", value="localhost"),
                HttpHeaderField(name="X-FreeUnit-Schedule", value=schedule_name),
            ],
        )

        # Synthetic connection on engine 0
        syn_conn = Connection(
            conn_id=-run_id,
            engine_id=0,
            remote_addr="127.0.0.1",
            state=ConnState.ACTIVE,
        )

        self.loop.record_trace(
            "scheduler",
            "schedule_dispatched",
            run_id=run_id,
            name=schedule_name,
            target=cfg.target_path,
        )

        # Schedule deadline timer in run context (separate from r->timer)
        timeout_ev = self.loop.schedule(
            cfg.timeout_ms * 1_000_000,
            "scheduler",
            "schedule_timeout",
            lambda: self._on_schedule_timeout(run_id),
        )

        # Dispatch via router
        self.router.on_listener_request(
            engine_id=0,
            conn=syn_conn,
            req=syn_req,
            action=RouteAction.PASS,
            app_id=cfg.target_app,
        )

        return True

    def _on_schedule_timeout(self, run_id: int) -> None:
        run_ctx = self.runs.get(run_id)
        if not run_ctx or run_ctx.completed:
            return

        run_ctx.timed_out = True
        run_ctx.completed = True
        self.in_flight[run_ctx.schedule_name] -= 1
        self.total_timeouts += 1

        self.loop.record_trace(
            "scheduler",
            "schedule_timed_out_aborted",
            run_id=run_id,
            name=run_ctx.schedule_name,
        )

    def on_schedule_completed(self, run_id: int) -> None:
        run_ctx = self.runs.get(run_id)
        if not run_ctx or run_ctx.completed:
            return

        run_ctx.completed = True
        self.in_flight[run_ctx.schedule_name] -= 1

        self.loop.record_trace(
            "scheduler",
            "schedule_run_succeeded",
            run_id=run_id,
            name=run_ctx.schedule_name,
            duration_ns=self.loop.now() - run_ctx.start_time_ns,
        )
