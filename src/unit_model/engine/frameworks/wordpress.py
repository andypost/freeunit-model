"""WordPress Lifecycle & State Reset Adapter.

Models:
- Classic mode (request-per-process startup and shutdown).
- Superglobals ($_SERVER, $_GET, $_POST, $_COOKIE) initialization and teardown.
- Database connection ($wpdb) and user auth cache ($current_user).
- wp-cron.php (DISABLE_WP_CRON) scheduled background invocation via FreeUnit schedules.

Response profile (ASSUMPTIONS, not measured):
- A rendered page is 30-80 KiB of HTML before compression (median 50 KiB,
  from the usual size of a default-theme front page with a few plugins).
- WordPress echoes template output as it renders; with output_buffering=4096
  the SAPI gets writes of about 4 KiB, so a page is a dozen or so writes.
- wp-cron.php answers with an empty 200 (DISABLE_WP_CRON + scheduled GET).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse
from unit_model.engine.frameworks.profile import PHP_OUTPUT_BUFFERING, ResponseProfile


WORDPRESS_PAGE = ResponseProfile(
    name="wordpress_page",
    median_bytes=50 * 1024,
    sigma=0.4,
    min_bytes=30 * 1024,
    max_bytes=80 * 1024,
    write_chunk=PHP_OUTPUT_BUFFERING,
    source="assumption: default theme front page; php.ini-production output_buffering=4096",
)


@dataclass
class WordPressState:
    installed: bool = True
    active_plugins: List[str] = field(default_factory=lambda: ["cache", "seo"])
    superglobals: Dict[str, Any] = field(default_factory=dict)
    wpdb_connected: bool = True
    cron_tasks_pending: int = 0
    total_requests: int = 0


class WordPressAdapter:
    """WordPress runtime state manager."""

    profile = WORDPRESS_PAGE

    def __init__(self, is_worker_mode: bool = False, seed: int = 1):
        self.is_worker_mode = is_worker_mode
        self.state = WordPressState()
        self.rng = random.Random(seed)

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        """Executes a WordPress request lifecycle."""
        self.state.total_requests += 1

        path = req.path if req else "/"
        if "wp-cron.php" in path:
            processed = self.state.cron_tasks_pending
            self.state.cron_tasks_pending = 0
            return UnitResponse(
                status=200,
                content=f"WP-Cron executed {processed} tasks".encode("utf-8"),
            )

        if not self.is_worker_mode:
            self._classic_startup()
            resp = self.profile.make_response(self.rng)
            self._classic_shutdown()
            return resp

        resp = self.profile.make_response(self.rng)
        self._worker_reset()
        return resp

    def _classic_startup(self) -> None:
        self.state.superglobals = {"_GET": {}, "_POST": {}, "_COOKIE": {}, "_SERVER": {}}

    def _classic_shutdown(self) -> None:
        self.state.superglobals.clear()

    def _worker_reset(self) -> None:
        self.state.superglobals.clear()
