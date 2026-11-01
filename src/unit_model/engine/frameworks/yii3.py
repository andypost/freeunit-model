"""Yii3 Worker Lifecycle Adapter (yiisoft/yii-runner-unit).

Models:
- Persistent worker loop.
- Yiisoft\\Di\\StateResetter component.
- Middleware state reset.
- Active DB transaction rollback.

Response profile (ASSUMPTIONS, not measured):
- An HTML page of 8-40 KiB (median 16 KiB).
- yii-runner-http's SapiEmitter sends the status line and headers, calls
  flush(), then writes the body from the stream in 8 MiB reads; so the
  header goes to the router on its own (nxt_php_flush) and the body follows
  as one write for anything under 8 MiB.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse
from unit_model.engine.frameworks.profile import ResponseProfile


YII3_PAGE = ResponseProfile(
    name="yii3_page",
    median_bytes=16 * 1024,
    sigma=0.5,
    min_bytes=8 * 1024,
    max_bytes=40 * 1024,
    write_chunk=None,
    flush_header_first=True,
    source="assumption: yii-runner-http SapiEmitter flushes headers, then emits the body",
)


@dataclass
class Yii3State:
    di_container_ready: bool = True
    middleware_state_dirty: bool = False
    active_transaction: bool = False
    total_requests: int = 0


class Yii3Adapter:
    """Yii3 runtime state manager."""

    profile = YII3_PAGE

    def __init__(self, seed: int = 1):
        self.state = Yii3State()
        self.rng = random.Random(seed)

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        self.state.total_requests += 1

        self.state.middleware_state_dirty = True
        self.state.active_transaction = True

        resp = self.profile.make_response(self.rng)

        self._reset_state()
        return resp

    def _reset_state(self) -> None:
        """Invokes StateResetter."""
        self.state.middleware_state_dirty = False
        self.state.active_transaction = False
