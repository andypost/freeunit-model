"""Tests for K2 Single-Buffer Response Framing and PR 751 static_script Target Lifecycle.

Verifies:
1. K2 Response Framing (Commit a7c5d3c69 / 7cfec443e):
   - Responses with body length <= NXT_PHP_RESPONSE_ROOM (8192 bytes) fit directly into the
     response header buffer as piggyback content.
   - Emits a single DATA response message with last=True, avoiding extra roundtrips.
   - Responses larger than NXT_PHP_RESPONSE_ROOM are flagged and chunked across multiple buffers.
2. Fixed vs Dynamic Script Target Lifecycle (PR 751 / Commit c7aebd899):
   - Fixed targets (static_script=True) skip per-request stat().
   - Missing/invalid scripts on fixed targets trigger 404 AFTER php_request_startup(),
     delivering PHP startup headers (e.g. X-Powered-By).
   - Dynamic targets (static_script=False) stat() before PHP startup; missing files return
     404 with no PHP headers.
"""

from unit_model.c_types import (
    HttpHeaderField,
    NXT_PHP_RESPONSE_ROOM,
    NXT_UNIT_MAX_PLAIN_SIZE,
    PortMsgType,
    UnitRequest,
    UnitResponse,
)
from unit_model.engine.event_loop import SimEventLoop
from unit_model.engine.ipc_queue import AppQueue
from unit_model.engine.ipc_socket import UnixSocketPair
from unit_model.engine.shm_pool import SharedMemoryPool
from unit_model.engine.worker import AppWorker, WorkerState


def test_small_response_piggybacked_single_message() -> None:
    """Verify that a small response (<= 8 KiB) uses piggyback content and a single port message."""
    loop = SimEventLoop()
    app_queue = AppQueue(loop=loop)
    socket_pair = UnixSocketPair(loop=loop)
    shm_pool = SharedMemoryPool(loop=loop)

    worker = AppWorker(
        loop=loop,
        worker_id=1,
        app_id="test_php",
        app_queue=app_queue,
        socket_pair=socket_pair,
        shm_pool=shm_pool,
    )

    sent_messages = []

    def mock_router_post(reply_port, pmsg, payload, worker_id):
        sent_messages.append((pmsg, payload))

    worker.on_send_response_to_router = mock_router_post

    # Small response: 512 bytes body
    small_body = b"Hello FreeUnit K2 Single-Buffer Response Framing!" * 10
    assert len(small_body) <= NXT_PHP_RESPONSE_ROOM

    resp = UnitResponse(
        status=200,
        headers=[
            HttpHeaderField(name="Content-Type", value="text/plain"),
            HttpHeaderField(name="Server", value="FreeUnit"),
        ],
        content=small_body,
    )

    worker.current_stream = 42
    worker.current_reply_port = 8000
    worker._send_response(resp)

    # Invariants
    assert worker.total_single_buffer_responses == 1
    assert worker.total_multi_buffer_responses == 0
    assert len(sent_messages) == 1

    pmsg, mmap_payload = sent_messages[0]
    assert pmsg.stream == 42
    assert pmsg.type == PortMsgType.DATA
    assert pmsg.last is True
    assert pmsg.mmap is True

    # Deserialize response from shm using the payload mmsg to confirm round-trip integrity
    from unit_model.c_types import MmapMsg
    mmsg = MmapMsg.from_bytes(mmap_payload)
    raw_data = shm_pool.get_data(mmsg.mmap_id, mmsg.chunk_id, mmsg.size)
    assert raw_data is not None

    roundtrip_resp = UnitResponse.deserialize(raw_data)
    assert roundtrip_resp.status == 200
    assert len(roundtrip_resp.headers) == 2
    assert roundtrip_resp.content == small_body


def test_large_response_multi_buffer_chunked() -> None:
    """Verify that large responses (> 8 KiB) are recognized as needing multi-buffer chunking."""
    loop = SimEventLoop()
    app_queue = AppQueue(loop=loop)
    socket_pair = UnixSocketPair(loop=loop)
    shm_pool = SharedMemoryPool(loop=loop)

    worker = AppWorker(
        loop=loop,
        worker_id=2,
        app_id="test_large",
        app_queue=app_queue,
        socket_pair=socket_pair,
        shm_pool=shm_pool,
    )
    worker.on_send_response_to_router = lambda *args: None

    # Large response: 64 KiB
    large_body = b"X" * (64 * 1024)
    assert len(large_body) > NXT_PHP_RESPONSE_ROOM

    resp = UnitResponse(
        status=200,
        headers=[HttpHeaderField(name="Content-Type", value="application/octet-stream")],
        content=large_body,
    )

    worker.current_stream = 43
    worker.current_reply_port = 8000
    worker._send_response(resp)

    assert worker.total_single_buffer_responses == 0
    assert worker.total_multi_buffer_responses == 1


def test_php_static_script_target_lifecycle() -> None:
    """Verify PR 751 lifecycle difference between fixed script targets and dynamic targets."""
    from unit_model.c_types import MmapMsg
    loop = SimEventLoop()
    app_queue = AppQueue(loop=loop)
    socket_pair = UnixSocketPair(loop=loop)
    shm_pool = SharedMemoryPool(loop=loop)

    sent_responses = []

    def mock_post(reply_port, pmsg, payload, worker_id):
        if pmsg.type == PortMsgType.DATA and pmsg.mmap:
            mmsg = MmapMsg.from_bytes(payload)
            data = shm_pool.get_data(mmsg.mmap_id, mmsg.chunk_id, mmsg.size)
            sent_responses.append(UnitResponse.deserialize(data) if data else None)

    # 1. Fixed script target with missing file (static_script=True, script_exists=False)
    worker_fixed = AppWorker(
        loop=loop,
        worker_id=10,
        app_id="php_fixed",
        app_queue=app_queue,
        socket_pair=socket_pair,
        shm_pool=shm_pool,
        static_script=True,
        script_exists=False,
    )
    worker_fixed.on_send_response_to_router = mock_post

    # Prepare request
    req = UnitRequest(target="/info.php", path="/info.php")
    req_bytes = req.serialize()
    alloc = shm_pool.allocate(len(req_bytes), req_bytes)
    assert alloc is not None
    mmsg = shm_pool.to_mmap_msg(alloc)

    worker_fixed._process_request_mmap(stream=100, reply_port=1, mmsg=mmsg)
    loop.run_until_idle()

    assert len(sent_responses) == 1
    resp_fixed = sent_responses[0]
    assert resp_fixed is not None
    assert resp_fixed.status == 404
    # Fixed target emits PHP startup headers on 404
    header_names = [h.name for h in resp_fixed.headers]
    assert "X-Powered-By" in header_names

    # 2. Dynamic target with missing file (static_script=False, script_exists=False)
    worker_dynamic = AppWorker(
        loop=loop,
        worker_id=11,
        app_id="php_dynamic",
        app_queue=app_queue,
        socket_pair=socket_pair,
        shm_pool=shm_pool,
        static_script=False,
        script_exists=False,
    )
    worker_dynamic.on_send_response_to_router = mock_post

    worker_dynamic._process_request_mmap(stream=101, reply_port=1, mmsg=mmsg)
    loop.run_until_idle()

    assert len(sent_responses) == 2
    resp_dynamic = sent_responses[1]
    assert resp_dynamic is not None
    assert resp_dynamic.status == 404
    # Dynamic target fails at stat() before php_request_startup, NO PHP headers
    assert len(resp_dynamic.headers) == 0
