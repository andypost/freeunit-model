"""Bounded State Space & Invariant Explorer (Model B).

Orchestrates combinatorial state space exploration across:
- Dual-channel message interleavings.
- Asynchronous fault injections (crashes, OOSM, RSTs).
- Multiple framework adapters under continuous load.
- Reproduces historical edge cases deterministically.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from unit_model.c_types import HttpHeaderField, UnitRequest
from unit_model.engine import (
    AppQueue,
    AppWorker,
    Listener,
    Router,
    SharedMemoryPool,
    SimEventLoop,
    UnixSocketPair,
)
from unit_model.analyzer.fault_injector import ChaosFaultInjector
from unit_model.analyzer.interleaver import AdversarialInterleaver
from unit_model.analyzer.invariants import InvariantChecker, InvariantViolation


@dataclass
class ExplorationResult:
    seed: int
    steps_executed: int
    sim_time_ns: int
    violations: List[InvariantViolation]
    trace_length: int


class StateSpaceExplorer:
    """Explores the behavioral state space under chaos and interleaving."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def run_scenario(
        self,
        num_engines: int = 2,
        num_workers: int = 2,
        num_requests: int = 10,
        inject_jitter: bool = True,
        inject_crash: bool = False,
        inject_oosm: bool = False,
    ) -> ExplorationResult:
        loop = SimEventLoop()
        shm = SharedMemoryPool(loop, max_segments=4)
        listener = Listener(loop, num_engines=num_engines)
        router = Router(loop, listener, shm, num_engines=num_engines)
        interleaver = AdversarialInterleaver(loop, seed=self.seed)
        fault_injector = ChaosFaultInjector(loop, seed=self.seed)

        workers: List[AppWorker] = []
        for w_idx in range(num_workers):
            wid = 200 + w_idx
            q = AppQueue(loop)
            sock = UnixSocketPair(loop, name=f"worker_{wid}_sock")
            w = AppWorker(
                loop=loop,
                worker_id=wid,
                app_id="test_app",
                app_queue=q,
                socket_pair=sock,
                shm_pool=shm,
            )
            router.register_worker("test_app", wid, q, sock)
            w.on_send_response_to_router = router.on_worker_response_msg
            workers.append(w)

            if inject_jitter:
                interleaver.inject_socket_jitter(sock, min_delay_ns=50, max_delay_ns=2_000)

        # Inject faults if requested
        if inject_oosm:
            fault_injector.exhaust_shared_memory(shm)
            # Schedule memory recovery after 5us
            loop.schedule(5_000, "chaos", "recover_shm", lambda: fault_injector.release_exhausted_shm(shm))

        if inject_crash and workers:
            target_w = self.rng.choice(workers)
            fault_injector.inject_worker_sigkill(target_w, delay_ns=1_500)

        # Dispatch requests
        for r_idx in range(num_requests):
            conn = listener.accept_client(f"192.168.1.{r_idx % 20}")
            req = UnitRequest(
                method="GET",
                version="HTTP/1.1",
                remote=conn.remote_addr,
                local_addr="127.0.0.1",
                local_port="8080",
                tls=False,
                websocket_handshake=False,
                app_target=0,
                server_name="test.local",
                target=f"/item/{r_idx}",
                path=f"/item/{r_idx}",
                query="",
                headers=[HttpHeaderField(name="Host", value="test.local")],
            )
            # Stagger dispatch times
            stagger = r_idx * 500
            loop.schedule(
                stagger,
                "client",
                "send_req",
                lambda e=r_idx % num_engines, c=conn, rq=req: router.on_listener_request(e, c, rq, app_id="test_app"),
            )

        steps = loop.run_until_idle(max_steps=50_000)

        checker = InvariantChecker(shm)
        violations = checker.verify_trace(loop.get_trace())

        return ExplorationResult(
            seed=self.seed,
            steps_executed=steps,
            sim_time_ns=loop.now(),
            violations=violations,
            trace_length=len(loop.get_trace()),
        )
