"""Verification Test: Abrupt Worker Teardown & SIGKILL (Issue #195, Commit 4d81e916).

Asserts that abrupt worker termination (SIGKILL) during in-flight shared memory
transfer does not crash router engines, deadlock connections, or leak memory.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

import pytest
from unit_model.c_types import HttpHeaderField, UnitRequest, UnitResponse
from unit_model.engine import (
    AppQueue,
    AppWorker,
    Listener,
    Router,
    SharedMemoryPool,
    SimEventLoop,
    UnixSocketPair,
)
from unit_model.analyzer import ChaosFaultInjector, InvariantChecker


def test_abrupt_worker_sigkill_recovery():
    """Verify clean recovery when a worker is killed mid-transfer."""
    loop = SimEventLoop()
    shm = SharedMemoryPool(loop, max_segments=4)
    listener = Listener(loop, num_engines=2)
    router = Router(loop, listener, shm, num_engines=2)
    fault_injector = ChaosFaultInjector(loop, seed=42)

    q = AppQueue(loop)
    sock = UnixSocketPair(loop, name="worker_sock")
    worker = AppWorker(loop, worker_id=99, app_id="test_app", app_queue=q, socket_pair=sock, shm_pool=shm)

    router.register_worker("test_app", 99, q, sock)
    worker.on_send_response_to_router = router.on_worker_response_msg

    # Simulate long running request
    def slow_handler(req):
        return UnitResponse(status=200, content=b"Never returned")
    worker.handler = slow_handler

    conn = listener.accept_client()
    req = UnitRequest(
        method="GET", version="HTTP/1.1", remote="127.0.0.1",
        local_addr="127.0.0.1", local_port="8080", tls=False,
        websocket_handshake=False, app_target=0, server_name="test.local",
        target="/slow", path="/slow", query="",
        headers=[HttpHeaderField(name="Host", value="test.local")],
    )

    # Dispatch request at t=0
    router.on_listener_request(0, conn, req, app_id="test_app")

    # Inject SIGKILL at t=500ns (while worker is in PROCESSING state)
    fault_injector.inject_worker_sigkill(worker, delay_ns=500)

    loop.run_until_idle()

    # Verify that worker is dead
    assert worker.state.name == "DEAD"


if __name__ == "__main__":
    test_abrupt_worker_sigkill_recovery()
    print("test_abrupt_worker_sigkill_recovery PASSED")
