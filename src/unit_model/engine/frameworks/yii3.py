"""Yii3 Worker Lifecycle Adapter (yiisoft/yii-runner-unit).

Models:
- Persistent worker loop.
- Yiisoft\\Di\\StateResetter component.
- Middleware state reset.
- Active DB transaction rollback.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse


@dataclass
class Yii3State:
    di_container_ready: bool = True
    middleware_state_dirty: bool = False
    active_transaction: bool = False
    total_requests: int = 0


class Yii3Adapter:
    """Yii3 runtime state manager."""

    def __init__(self):
        self.state = Yii3State()

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        self.state.total_requests += 1

        # Simulate middleware mutation
        self.state.middleware_state_dirty = True
        self.state.active_transaction = True

        resp = UnitResponse(status=200, content=b"Yii3 Response")

        # Invoke StateResetter
        self._reset_state()
        return resp

    def _reset_state(self) -> None:
        """Invokes StateResetter."""
        self.state.middleware_state_dirty = False
        self.state.active_transaction = False
