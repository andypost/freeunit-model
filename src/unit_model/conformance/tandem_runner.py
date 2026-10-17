"""Tandem Execution Harness for Conformance Testing.

Runs requests through both the discrete-event model and live FreeUnit instances,
comparing outputs via WireComparator.
"""

from __future__ import annotations

import http.client
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from unit_model.c_types import HttpHeaderField, UnitRequest, UnitResponse
from unit_model.conformance.comparator import ComparisonResult, WireComparator
from unit_model.engine import (
    AppQueue,
    AppWorker,
    Listener,
    Router,
    SharedMemoryPool,
    SimEventLoop,
    UnixSocketPair,
)


class TandemSession:
    """Manages tandem execution between Model A simulation and a target server."""

    def __init__(
        self,
        target_url: Optional[str] = None,
        comparator: Optional[WireComparator] = None,
    ) -> None:
        self.target_url = target_url
        self.comparator = comparator or WireComparator()

        # Initialize Model A discrete-event engine
        self.loop = SimEventLoop()
        self.shm = SharedMemoryPool(self.loop, max_segments=4)
        self.listener = Listener(self.loop, num_engines=2)
        self.router = Router(self.loop, self.listener, self.shm, num_engines=2)

        # Worker setup
        self.q = AppQueue(self.loop)
        self.sock = UnixSocketPair(self.loop, name="tandem_worker_sock")
        self.worker = AppWorker(
            self.loop,
            worker_id=200,
            app_id="tandem_app",
            app_queue=self.q,
            socket_pair=self.sock,
            shm_pool=self.shm,
        )
        self.router.register_worker("tandem_app", 200, self.q, self.sock)
        self.worker.on_send_response_to_router = self.router.on_worker_response_msg
        self.worker.handler = self._default_handler

        self.last_simulated_response: Optional[UnitResponse] = None

    def _default_handler(self, req: UnitRequest) -> UnitResponse:
        """Default application handler mirroring standard status responses."""
        if req.path == "/health":
            return UnitResponse(
                status=200,
                headers=[HttpHeaderField(name="Content-Type", value="text/plain")],
                content=b"OK",
            )
        elif req.path == "/echo" and req.method == "POST":
            return UnitResponse(
                status=200,
                headers=[
                    HttpHeaderField(name="Content-Type", value="application/octet-stream")
                ],
                content=req.preread_content,
            )
        elif req.path == "/not-found":
            return UnitResponse(status=404, content=b"Not Found")
        return UnitResponse(
            status=200,
            headers=[HttpHeaderField(name="Content-Type", value="text/html")],
            content=b"<h1>FreeUnit</h1>",
        )

    def execute_in_model(self, req: UnitRequest) -> UnitResponse:
        """Run request through Model A discrete-event simulator."""
        conn = self.listener.accept_client(req.remote)
        captured_response: List[UnitResponse] = []

        original_send = self.listener.send_http_response

        def on_response(c: Any, resp: UnitResponse, keep_alive: bool = True) -> None:
            captured_response.append(resp)
            original_send(c, resp, keep_alive)

        self.listener.send_http_response = on_response
        try:
            self.router.on_listener_request(
                engine_id=conn.engine_id,
                conn=conn,
                req=req,
                app_id="tandem_app",
            )
            self.loop.run_until_idle()
        finally:
            self.listener.send_http_response = original_send

        if not captured_response:
            raise RuntimeError("Simulation did not produce a response")

        self.last_simulated_response = captured_response[0]
        return captured_response[0]

    def execute_live(self, req: UnitRequest) -> UnitResponse:
        """Send HTTP request to live FreeUnit target over TCP/HTTP."""
        if not self.target_url:
            raise ValueError("target_url was not configured for live execution")

        parsed = urlparse(self.target_url)
        conn = http.client.HTTPConnection(parsed.hostname or "127.0.0.1", parsed.port or 8080, timeout=5)
        headers = {h.name: h.value for h in req.headers}
        conn.request(req.method, req.path, body=req.preread_content or None, headers=headers)
        res = conn.getresponse()

        body = res.read()
        resp_headers = [HttpHeaderField(name=k, value=v) for k, v in res.getheaders()]
        conn.close()

        return UnitResponse(status=res.status, headers=resp_headers, content=body)

    def verify_equivalence(
        self,
        req: UnitRequest,
        reference_response: Optional[UnitResponse] = None,
    ) -> ComparisonResult:
        """Run request through model and verify against live target or reference response."""
        oracle_resp = self.execute_in_model(req)
        if reference_response is not None:
            target_resp = reference_response
        elif self.target_url is not None:
            target_resp = self.execute_live(req)
        else:
            raise ValueError("Must provide either reference_response or target_url")

        return self.comparator.compare_responses(oracle_resp, target_resp)
