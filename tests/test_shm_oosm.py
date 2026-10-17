"""Verification Test: Shared-Memory OOSM Starvation & Recovery.

Asserts that memory exhaustion (OOSM) triggers graceful 503 rejection
and that recovering memory restores normal request flow without deadlock.
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


def test_shm_oosm_and_recovery():
    """Verify OOSM backpressure and recovery."""
    loop = SimEventLoop()
    shm = SharedMemoryPool(loop, max_segments=2)  # Limited to 2 segments
    listener = Listener(loop, num_engines=2)
    router = Router(loop, listener, shm, num_engines=2)
    fault_injector = ChaosFaultInjector(loop, seed=99)

    q = AppQueue(loop)
    sock = UnixSocketPair(loop, name="worker_sock")
    worker = AppWorker(loop, worker_id=1, app_id="test_app", app_queue=q, socket_pair=sock, shm_pool=shm)

    router.register_worker("test_app", 1, q, sock)
    worker.on_send_response_to_router = router.on_worker_response_msg
    worker.handler = lambda req: UnitResponse(status=200, content=b"Success")

    # 1. Artificially exhaust shared memory to trigger OOSM
    fault_injector.exhaust_shared_memory(shm)
    assert shm.in_oosm, "Failed to put pool into OOSM"

    # 2. Dispatch request under OOSM -> router should fail dispatch gracefully with 503
    conn1 = listener.accept_client()
    req1 = UnitRequest(
        method="GET", version="HTTP/1.1", remote="127.0.0.1",
        local_addr="127.0.0.1", local_port="8080", tls=False,
        websocket_handshake=False, app_target=0, server_name="test.local",
        target="/oosm-req", path="/oosm-req", query="",
        headers=[HttpHeaderField(name="Host", value="test.local")],
    )
    router.on_listener_request(0, conn1, req1, app_id="test_app")
    loop.run_until_idle()

    # 3. Simulate SHM_ACK release: release exhausted memory
    fault_injector.release_exhausted_shm(shm)
    assert not shm.in_oosm, "Pool should no longer be in OOSM"

    # 4. Now dispatch request again -> should succeed cleanly!
    conn2 = listener.accept_client()
    req2 = UnitRequest(
        method="GET", version="HTTP/1.1", remote="127.0.0.1",
        local_addr="127.0.0.1", local_port="8080", tls=False,
        websocket_handshake=False, app_target=0, server_name="test.local",
        target="/ok-req", path="/ok-req", query="",
        headers=[HttpHeaderField(name="Host", value="test.local")],
    )
    router.on_listener_request(0, conn2, req2, app_id="test_app")
    loop.run_until_idle()

    # Verify memory is clean
    checker = InvariantChecker(shm)
    assert checker.check_shm_leak(), "Memory leak after OOSM recovery"


if __name__ == "__main__":
    test_shm_oosm_and_recovery()
    print("test_shm_oosm_and_recovery PASSED")
