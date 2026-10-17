"""Verification Test: Dual-Channel Message Ordering (Issues #573, #597).

Asserts that FreeUnit's dual-channel IPC preserves FIFO delivery order
between the shared-memory App Queue and Unix socket datagram channel.
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
from unit_model.analyzer import AdversarialInterleaver, InvariantChecker


def test_dual_channel_fifo_order_under_jitter():
    """Verify that socket jitter cannot invert message delivery order."""
    loop = SimEventLoop()
    shm = SharedMemoryPool(loop, max_segments=4)
    listener = Listener(loop, num_engines=2)
    router = Router(loop, listener, shm, num_engines=2)
    interleaver = AdversarialInterleaver(loop, seed=123)

    q = AppQueue(loop)
    sock = UnixSocketPair(loop, name="worker_sock")
    worker = AppWorker(loop, worker_id=1, app_id="test_app", app_queue=q, socket_pair=sock, shm_pool=shm)

    router.register_worker("test_app", 1, q, sock)
    worker.on_send_response_to_router = router.on_worker_response_msg

    # Inject extreme jitter on socket channel (100ns to 10,000ns)
    interleaver.inject_socket_jitter(sock, min_delay_ns=100, max_delay_ns=10_000)

    delivered_streams = []
    def recording_handler(req):
        delivered_streams.append(worker.current_stream)
        return UnitResponse(status=200, content=b"OK")
    worker.handler = recording_handler

    # Send 5 sequential requests
    for i in range(5):
        conn = listener.accept_client()
        req = UnitRequest(
            method="GET", version="HTTP/1.1", remote="127.0.0.1",
            local_addr="127.0.0.1", local_port="8080", tls=False,
            websocket_handshake=False, app_target=0, server_name="test.local",
            target=f"/req/{i}", path=f"/req/{i}", query="",
            headers=[HttpHeaderField(name="Host", value="test.local")],
        )
        router.on_listener_request(0, conn, req, app_id="test_app")

    loop.run_until_idle()

    # Verify FIFO delivery
    expected = [1000, 1001, 1002, 1003, 1004]
    assert delivered_streams == expected, f"Delivery order inverted: {delivered_streams} != {expected}"

    # Verify zero shm leak
    checker = InvariantChecker(shm)
    assert checker.check_shm_leak(), "Shared memory leak after dual-channel processing"


if __name__ == "__main__":
    test_dual_channel_fifo_order_under_jitter()
    print("test_dual_channel_fifo_order_under_jitter PASSED")
