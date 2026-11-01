"""Symfony Worker Lifecycle Adapter (runtime/unit-symfony).

Models:
- Persistent worker loop (FrankenPhpWorkerRunner / UnitWorkerRunner).
- ResetInterface services (services requiring clean state per request).
- Doctrine Entity Manager reset ($em->clear(), reconnect if connection died).
- Monolog buffer flushing.
- Request stack clearing.

Response profile (ASSUMPTIONS, not measured):
- A JSON API response of 1-8 KiB (median 3 KiB).
- Response::send() echoes the content once and then calls flush(); the body
  is small enough that it is a single write, and the flush finds the header
  already sent.  Output buffering is assumed off in a worker (the runner
  handles output itself).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse
from unit_model.engine.frameworks.profile import ResponseProfile


SYMFONY_JSON = ResponseProfile(
    name="symfony_json",
    median_bytes=3 * 1024,
    sigma=0.6,
    min_bytes=1024,
    max_bytes=8 * 1024,
    write_chunk=None,
    content_type="application/json",
    source="assumption: Symfony JSON API; HttpFoundation Response::send() echoes once",
)


@dataclass
class SymfonyState:
    kernel_booted: bool = True
    reset_services_count: int = 42
    doctrine_entities_managed: int = 0
    monolog_buffer_size: int = 0
    total_requests: int = 0


class SymfonyAdapter:
    """Symfony runtime state manager."""

    profile = SYMFONY_JSON

    def __init__(self, is_worker_mode: bool = True, seed: int = 1):
        self.is_worker_mode = is_worker_mode
        self.state = SymfonyState()
        self.rng = random.Random(seed)

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        self.state.total_requests += 1

        self.state.doctrine_entities_managed += 15
        self.state.monolog_buffer_size += 5

        resp = self.profile.make_response(self.rng)

        if self.is_worker_mode:
            self._reset_services()

        return resp

    def _reset_services(self) -> None:
        """Invokes ResetInterface on container services."""
        self.state.doctrine_entities_managed = 0
        self.state.monolog_buffer_size = 0
