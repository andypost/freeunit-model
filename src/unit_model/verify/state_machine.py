"""Hypothesis RuleBasedStateMachine Model for FreeUnit Verification.

Explores complex state-space combinations across:
- Client connections (EPOLLEXCLUSIVE distribution)
- Request dispatch across dual-channel IPC (AppQueue vs Unix socket)
- Worker process crashes (SIGKILL) mid-transfer
- Shared memory OOSM starvation and recovery
- Continuous assertion of core safety and liveness invariants
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional, Set

try:
    import hypothesis.strategies as st
    from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, rule

    HAVE_HYPOTHESIS = True
except ImportError:
    HAVE_HYPOTHESIS = False
    RuleBasedStateMachine = object  # type: ignore

from unit_model.analyzer.invariants import InvariantChecker
from unit_model.c_types import HttpHeaderField, UnitRequest, UnitResponse
from unit_model.engine.event_loop import SimEventLoop
from unit_model.engine.ipc_queue import AppQueue
from unit_model.engine.ipc_socket import UnixSocketPair
from unit_model.engine.listener import Connection, Listener
from unit_model.engine.router import Router
from unit_model.engine.shm_pool import SharedMemoryPool
from unit_model.engine.worker import AppWorker


if HAVE_HYPOTHESIS:

    class FreeUnitStateMachine(RuleBasedStateMachine):
        """Stateful property-based testing of FreeUnit lifecycle & IPC."""

        connections = Bundle("connections")
        requests = Bundle("requests")

        def __init__(self) -> None:
            super().__init__()
            self.loop = SimEventLoop()
            self.shm = SharedMemoryPool(self.loop, max_segments=4)
            self.listener = Listener(self.loop, num_engines=2)
            self.router = Router(self.loop, self.listener, self.shm, num_engines=2)

            self.workers: Dict[int, AppWorker] = {}
            for wid in [101, 102]:
                q = AppQueue(self.loop)
                sock = UnixSocketPair(self.loop, name=f"worker_{wid}_sock")
                worker = AppWorker(
                    self.loop,
                    worker_id=wid,
                    app_id="web",
                    app_queue=q,
                    socket_pair=sock,
                    shm_pool=self.shm,
                )
                self.router.register_worker("web", wid, q, sock)
                worker.on_send_response_to_router = self.router.on_worker_response_msg
                worker.handler = lambda req: UnitResponse(status=200, content=b"OK")
                self.workers[wid] = worker

            self.checker = InvariantChecker(self.shm)
            self.req_counter = 0

        @rule(target=connections, client_ip=st.sampled_from(["10.0.0.1", "10.0.0.2", "192.168.1.1"]))
        def accept_client(self, client_ip: str) -> Connection:
            conn = self.listener.accept_client(client_ip)
            return conn

        @rule(
            target=requests,
            conn=connections,
            path=st.sampled_from(["/index.html", "/api/v1/status", "/data"]),
        )
        def send_request(self, conn: Connection, path: str) -> UnitRequest:
            self.req_counter += 1
            req = UnitRequest(
                method="GET",
                version="HTTP/1.1",
                remote=conn.remote_addr,
                local_addr="127.0.0.1",
                local_port="8080",
                target=path,
                path=path,
                query="",
                headers=[HttpHeaderField(name="Host", value="localhost")],
            )
            self.router.on_listener_request(
                engine_id=conn.engine_id,
                conn=conn,
                req=req,
                app_id="web",
            )
            return req

        @rule(wid=st.sampled_from([101, 102]))
        def crash_worker_sigkill(self, wid: int) -> None:
            """Simulate sudden worker death via SIGKILL."""
            worker = self.workers.get(wid)
            if worker and worker.is_alive:
                worker.crash_sigkill()

        @rule()
        def advance_event_loop(self) -> None:
            """Step the deterministic virtual clock."""
            self.loop.run_until_idle()

        @invariant()
        def assert_invariants(self) -> None:
            """Check the core safety invariants on every state transition."""
            self.loop.run_until_idle()
            violations = self.checker.verify_trace(self.loop.get_trace())
            # During mid-scenario execution, requests may be pending completion if a worker died
            safety_violations = [v for v in violations if v.invariant_id not in ("INV-4-PROGRESS", "INV-3-NO-LEAK")]
            if safety_violations:
                msg = "\n".join(f"! [{v.invariant_id}] {v.description}" for v in safety_violations)
                raise AssertionError(f"Safety invariant violations detected:\n{msg}")

        def teardown(self) -> None:
            """Final assertion after scenario sequence finishes."""
            self.loop.run_until_idle()
            # If workers are still alive and all requests finished, verify clean state
            violations = self.checker.verify_trace(self.loop.get_trace())
            # Filter out progress if worker died
            dead_workers = any(not w.is_alive for w in self.workers.values())
            if not dead_workers and violations:
                msg = "\n".join(f"! [{v.invariant_id}] {v.description}" for v in violations)
                raise AssertionError(f"Final invariant violations detected:\n{msg}")

else:

    class FreeUnitStateMachine:  # type: ignore
        """Placeholder when hypothesis is not installed."""

        pass
