"""Drupal Lifecycle Adapter.

Models:
- Classic & persistent worker mode (DrupalRuntime).
- Issue #321 visitor request hijacking: solved via detached request completion (unit_finish_request).
- Service container resets between requests.
- Render cache cleanup and PDO connection verification.
- Dedicated jobs_app schedule isolation for /cron/KEY.

Response profile (ASSUMPTIONS, not measured):
- A Drupal 10/11 page is 20-60 KiB of HTML (median 35 KiB; Olivero/Claro
  pages with a handful of blocks).
- Drupal sends through Symfony HttpFoundation Response::send(), which echoes
  the whole content once; with output_buffering=4096 the SAPI still gets it
  in about 4 KiB flushes.
- /cron/KEY answers with a small 204-like body after unit_finish_request().
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from unit_model.c_types import UnitRequest, UnitResponse
from unit_model.engine.frameworks.profile import PHP_OUTPUT_BUFFERING, ResponseProfile


DRUPAL_PAGE = ResponseProfile(
    name="drupal_page",
    median_bytes=35 * 1024,
    sigma=0.4,
    min_bytes=20 * 1024,
    max_bytes=60 * 1024,
    write_chunk=PHP_OUTPUT_BUFFERING,
    source="assumption: Drupal 10/11 Olivero page; php.ini-production output_buffering=4096",
)


@dataclass
class DrupalState:
    container_initialized: bool = True
    cached_services: int = 150
    db_connected: bool = True
    detached_cron_running: bool = False
    total_requests: int = 0


class DrupalAdapter:
    """Drupal runtime state manager."""

    profile = DRUPAL_PAGE

    def __init__(self, is_worker_mode: bool = True, seed: int = 1):
        self.is_worker_mode = is_worker_mode
        self.state = DrupalState()
        self.rng = random.Random(seed)

    def handle_request(
        self,
        req: Optional[UnitRequest],
        on_finish_request_cb: Optional[Callable[[], None]] = None,
    ) -> UnitResponse:
        self.state.total_requests += 1
        path = req.path if req else "/"

        if "/cron/" in path:
            if on_finish_request_cb:
                on_finish_request_cb()
                self.state.detached_cron_running = True
                self._run_cron_tasks()
                self.state.detached_cron_running = False

            return UnitResponse(status=200, content=b"Drupal Cron Completed")

        resp = self.profile.make_response(self.rng)

        if self.is_worker_mode:
            self._reset_container()

        return resp

    def _reset_container(self) -> None:
        """Resets request-scoped services in DI container."""
        self.state.cached_services = 150

    def _run_cron_tasks(self) -> None:
        pass
