"""Laravel Octane Worker Lifecycle Adapter.

Models:
- Octane persistent worker loop (bin/unit-worker.php).
- Sandbox container cloning ($app->clone()).
- Database reconnect and active transaction rollback.
- Auth guard purge (session / JWT token reset).
- Queued cookies flush.
- max_requests jitter to prevent coordinated worker churn.

Response profile (ASSUMPTIONS, not measured):
- A JSON API response of 512 bytes to 4 KiB (median 1.5 KiB).
- The response is written once (Response::send()).  A 1.5 KiB body does not
  fit the room of the 1008-byte header buffer, so the header goes alone and
  the body is a second message; a body under about 900 bytes rides with the
  header.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse
from unit_model.engine.frameworks.profile import ResponseProfile


LARAVEL_JSON = ResponseProfile(
    name="laravel_json",
    median_bytes=1536,
    sigma=0.6,
    min_bytes=512,
    max_bytes=4 * 1024,
    write_chunk=None,
    content_type="application/json",
    source="assumption: Laravel Octane JSON API; one write per response",
)


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

    profile = LARAVEL_JSON

    def __init__(self, base_max_requests: int = 1000, jitter: int = 50, seed: int = 1):
        self.rng = random.Random(seed)
        self.state = LaravelState(
            max_requests_limit=base_max_requests + self.rng.randint(-jitter, jitter)
        )

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        self.state.total_requests += 1

        self.state.active_sandbox = True
        self.state.auth_user_id = 42
        self.state.queued_cookies += 2

        resp = self.profile.make_response(self.rng)

        self._purge_sandbox()
        return resp

    def _purge_sandbox(self) -> None:
        """Purges mutated state back to clean sandbox."""
        self.state.auth_user_id = None
        self.state.db_transactions = 0
        self.state.queued_cookies = 0
        self.state.active_sandbox = False
