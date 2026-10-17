"""Adversarial Chaos Fault Injector (Model B).

Injects real-world failure modes:
1. Asynchronous worker crashes (SIGKILL / SIGCHLD) mid-transfer (issue #195).
2. Shared memory starvation (OOSM) under peak load.
3. Abrupt client disconnections (TCP RST) during active response writing.
4. Process churn and rapid restarts.
"""

from __future__ import annotations

import random
from typing import List, Optional

from unit_model.engine.event_loop import EventPriority, SimEventLoop
from unit_model.engine.listener import Connection, Listener
from unit_model.engine.shm_pool import SharedMemoryPool, ShmChunkAllocation
from unit_model.engine.worker import AppWorker


class ChaosFaultInjector:
    """Injects asynchronous failures into FreeUnit simulation components."""

    def __init__(self, loop: SimEventLoop, seed: int = 42):
        self.loop = loop
        self.rng = random.Random(seed)
        self.parked_shm_allocs: List[ShmChunkAllocation] = []

    def inject_worker_sigkill(
        self,
        worker: AppWorker,
        delay_ns: int = 500,
    ) -> None:
        """Kills worker abruptly while processing a request."""
        self.loop.schedule(
            delay_ns,
            "chaos_injector",
            "inject_sigkill",
            lambda: worker.crash_sigkill(),
            priority=EventPriority.HIGH,
        )

    def exhaust_shared_memory(
        self,
        shm_pool: SharedMemoryPool,
        reserve_bytes: int = 10 * 1024 * 1024,
    ) -> None:
        """Fills shared memory chunks to trigger OOSM."""
        while not shm_pool.in_oosm:
            alloc = shm_pool.allocate(64 * 1024)  # 64 KiB chunks
            if alloc is None:
                break
            self.parked_shm_allocs.append(alloc)
        self.loop.record_trace("chaos_injector", "shm_exhaustion_injected", count=len(self.parked_shm_allocs))

    def release_exhausted_shm(self, shm_pool: SharedMemoryPool) -> None:
        """Releases parked chunks to simulate SHM_ACK memory recovery."""
        for alloc in self.parked_shm_allocs:
            shm_pool.free(alloc.mmap_id, alloc.start_chunk_id, alloc.chunk_count)
        self.parked_shm_allocs.clear()
        self.loop.record_trace("chaos_injector", "shm_exhaustion_recovered")

    def inject_client_rst(
        self,
        listener: Listener,
        conn: Connection,
        delay_ns: int = 200,
    ) -> None:
        """Simulates client TCP RST."""
        self.loop.schedule(
            delay_ns,
            "chaos_injector",
            "client_rst",
            lambda: listener.close_connection(conn),
            priority=EventPriority.HIGH,
        )
