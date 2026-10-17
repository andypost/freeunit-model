"""Symfony Worker Lifecycle Adapter (runtime/unit-symfony).

Models:
- Persistent worker loop (FrankenPhpWorkerRunner / UnitWorkerRunner).
- ResetInterface services (services requiring clean state per request).
- Doctrine Entity Manager reset ($em->clear(), reconnect if connection died).
- Monolog buffer flushing.
- Request stack clearing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse


@dataclass
class SymfonyState:
    kernel_booted: bool = True
    reset_services_count: int = 42
    doctrine_entities_managed: int = 0
    monolog_buffer_size: int = 0
    total_requests: int = 0


class SymfonyAdapter:
    """Symfony runtime state manager."""

    def __init__(self, is_worker_mode: bool = True):
        self.is_worker_mode = is_worker_mode
        self.state = SymfonyState()

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        self.state.total_requests += 1

        # Simulate service mutations during request execution
        self.state.doctrine_entities_managed += 15
        self.state.monolog_buffer_size += 5

        resp = UnitResponse(
            status=200,
            content=b'{"status":"ok","framework":"symfony"}',
        )

        if self.is_worker_mode:
            self._reset_services()

        return resp

    def _reset_services(self) -> None:
        """Invokes ResetInterface on container services."""
        # 1. Doctrine $em->clear()
        self.state.doctrine_entities_managed = 0
        # 2. Flush Monolog buffer
        self.state.monolog_buffer_size = 0
        # 3. Request stack clear
