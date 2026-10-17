"""WordPress Lifecycle & State Reset Adapter.

Models:
- Classic mode (request-per-process startup and shutdown).
- Superglobals ($_SERVER, $_GET, $_POST, $_COOKIE) initialization and teardown.
- Database connection ($wpdb) and user auth cache ($current_user).
- wp-cron.php (DISABLE_WP_CRON) scheduled background invocation via FreeUnit schedules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse


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

    def __init__(self, is_worker_mode: bool = False):
        self.is_worker_mode = is_worker_mode
        self.state = WordPressState()

    def handle_request(self, req: Optional[UnitRequest]) -> UnitResponse:
        """Executes a WordPress request lifecycle."""
        self.state.total_requests += 1

        # Check if this is the background cron endpoint
        path = req.path if req else "/"
        if "wp-cron.php" in path:
            # Execute background cron tasks
            processed = self.state.cron_tasks_pending
            self.state.cron_tasks_pending = 0
            return UnitResponse(
                status=200,
                content=f"WP-Cron executed {processed} tasks".encode("utf-8"),
            )

        # Standard page request
        # In Classic mode, each request resets superglobals completely
        if not self.is_worker_mode:
            self._classic_startup()
            resp = UnitResponse(status=200, content=b"<!DOCTYPE html><html>WP Page</html>")
            self._classic_shutdown()
            return resp
        else:
            # Worker mode: manual cleanup required
            resp = UnitResponse(status=200, content=b"<!DOCTYPE html><html>WP Worker Page</html>")
            self._worker_reset()
            return resp

    def _classic_startup(self) -> None:
        self.state.superglobals = {"_GET": {}, "_POST": {}, "_COOKIE": {}, "_SERVER": {}}

    def _classic_shutdown(self) -> None:
        self.state.superglobals.clear()

    def _worker_reset(self) -> None:
        # Reset globals that leak across requests in worker mode
        self.state.superglobals.clear()
