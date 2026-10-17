"""Laravel Octane Worker Lifecycle Adapter.

Models:
- Octane persistent worker loop (bin/unit-worker.php).
- Sandbox container cloning ($app->clone()).
- Database reconnect and active transaction rollback.
- Auth guard purge (session / JWT token reset).
- Queued cookies flush.
- max_requests jitter to prevent coordinated worker churn.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse


@dataclass
class LaravelState:
    base_app_bound: bool = True
    active_sandbox: bool = False
    auth_user_id: Optional[int] = None
    db_transactions: int = 0
    queued_cookies: int = 0
    max_requests_limit: int = 1000
    total_requests: int = 0


class LaravelAdapter:
    """Laravel Octane runtime state manager."""

    def __init__(self, base_max_requests: int = 1000, jitter: int = 50):
        self.state = LaravelState(
            max_requests_limit=base_max_requests + random.randint(-jitter, jitter)
        )

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        self.state.total_requests += 1

        # 1. Clone sandbox container
        self.state.active_sandbox = True

        # 2. Simulate business logic mutating state
        self.state.auth_user_id = 42
        self.state.queued_cookies += 2

        resp = UnitResponse(
            status=200,
            content=b'{"message":"Laravel Octane Response"}',
        )

        # 3. Teardown sandbox & purge state
        self._purge_sandbox()
        return resp

    def _purge_sandbox(self) -> None:
        """Purges mutated state back to clean sandbox."""
        self.state.auth_user_id = None
        self.state.db_transactions = 0
        self.state.queued_cookies = 0
        self.state.active_sandbox = False
