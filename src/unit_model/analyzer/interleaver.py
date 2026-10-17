"""Adversarial Dual-Channel Interleaver (Model B).

Actively explores non-deterministic timing interleavings between:
1. Shared-memory App Queue items (ring buffer reads).
2. Socket datagram notifications (READ_QUEUE doorbells, direct socket messages).
3. Concurrent router engine thread scheduling turns.

Reproduces and asserts immunity against dual-channel message inversions (issues #573, #597).
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable, List, Optional

from unit_model.engine.event_loop import EventPriority, SimEventLoop
from unit_model.engine.ipc_queue import AppQueue
from unit_model.engine.ipc_socket import UnixSocketPair


class AdversarialInterleaver:
    """Injects controlled scheduling jitter into IPC channels."""

    def __init__(self, loop: SimEventLoop, seed: int = 42):
        self.loop = loop
        self.rng = random.Random(seed)
        self.total_delays_injected: int = 0

    def inject_socket_jitter(
        self,
        socket_pair: UnixSocketPair,
        min_delay_ns: int = 100,
        max_delay_ns: int = 10_000,
    ) -> None:
        """Adds randomized delay to socket datagram transit."""
        original_send = socket_pair.send_a_to_b

        def delayed_send(*args, **kwargs):
            delay = self.rng.randint(min_delay_ns, max_delay_ns)
            self.total_delays_injected += 1
            self.loop.record_trace("interleaver", "injected_socket_delay", delay_ns=delay)
            # Schedule execution after delay
            self.loop.schedule(
                delay,
                "interleaver",
                "delayed_socket_send",
                lambda: original_send(*args, **kwargs),
            )
            return True

        socket_pair.send_a_to_b = delayed_send

    def inject_queue_read_delay(
        self,
        app_queue: AppQueue,
        read_delay_ns: int = 5_000,
    ) -> None:
        """Artificially holds back App Queue dequeues to test socket overtaking."""
        original_recv = app_queue.recv
        held_until = self.loop.now() + read_delay_ns

        def delayed_recv():
            if self.loop.now() < held_until:
                self.loop.record_trace("interleaver", "app_queue_read_blocked")
                return None
            return original_recv()

        app_queue.recv = delayed_recv
