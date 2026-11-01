"""Response framing as libunit does it in FreeUnit master.

Checked against master 42251f7c9e366df31a2e70a5da3679a2fa855b3d:
- nxt_unit_response_write_nb(), src/nxt_unit.c:3849-3936: the first write
  rides in the header buffer as piggyback content when it fits the room
  left; with a plain-sized header buffer (<= 1024 bytes) a write that does
  not fit sends the header alone.  Each later write is a message of its own.
- nxt_unit_request_done(), src/nxt_unit.c:4980-5033: a header that was
  never sent goes with the last flag; otherwise an empty DATA message with
  the last flag ends the stream.
- nxt_unit_get_outgoing_buf(), src/nxt_unit.c:5805-5912: up to 15 bytes, or
  any plain-sized buffer on the socket, is plain; up to 1024 bytes probes
  shared memory and falls back to plain; more than 1024 bytes waits for a
  shared memory ack.

Also keeps the PR 751 fixed/dynamic script target lifecycle test.
"""

from unit_model.c_types import (
    HttpHeaderField,
    MmapMsg,
    NXT_UNIT_MAX_PLAIN_SIZE,
    NXT_UNIT_QUEUE_PLAIN_SIZE,
    NXT_UNIT_RESPONSE_PLAIN_ALLOC,
    PortMsgType,
    UnitRequest,
    UnitResponse,
)
from unit_model.engine.event_loop import SimEventLoop
from unit_model.engine.ipc_queue import AppQueue
from unit_model.engine.ipc_socket import UnixSocketPair
from unit_model.engine.shm_pool import SharedMemoryPool
from unit_model.engine.worker import AppWorker


def make_worker(shm_pool=None, worker_id=1, **kw):
    loop = SimEventLoop()
    shm_pool = shm_pool or SharedMemoryPool(loop=loop)
    worker = AppWorker(
        loop=loop,
        worker_id=worker_id,
        app_id="php",
        app_queue=AppQueue(loop=loop),
        socket_pair=UnixSocketPair(loop=loop),
        shm_pool=shm_pool,
        **kw,
    )
    sent = []
    worker.on_send_response_to_router = lambda rp, pmsg, payload, wid: sent.append((pmsg, payload))
    worker.current_stream = 42
    worker.current_reply_port = 1
    return loop, shm_pool, worker, sent


def data_msgs(sent):
    return [(p, pl) for p, pl in sent if p.type == PortMsgType.DATA]


def test_constants_match_master():
    assert NXT_UNIT_MAX_PLAIN_SIZE == 1024            # src/nxt_unit.c:30
    assert NXT_UNIT_RESPONSE_PLAIN_ALLOC == 1008      # src/nxt_unit.c:33-34
    assert NXT_UNIT_QUEUE_PLAIN_SIZE == 15            # src/nxt_unit.c:37-38


def test_hello_world_is_header_with_piggyback_then_empty_last():
    """echo "hello": header + piggyback (no last), then the empty last message."""
    loop, shm, worker, sent = make_worker()
    resp = UnitResponse(
        status=200,
        headers=[HttpHeaderField(name="Content-Type", value="text/plain")],
        content=b"Hello FreeUnit!",
    )
    worker._send_response(resp)
    loop.run_until_idle()

    msgs = data_msgs(sent)
    assert len(msgs) == 2
    first, payload = msgs[0]
    assert first.last is False and first.mmap is True
    mmsg = MmapMsg.from_bytes(payload)
    raw = shm.get_data(mmsg.mmap_id, mmsg.chunk_id, mmsg.size)
    parsed = UnitResponse.deserialize(raw)
    assert parsed.status == 200 and parsed.content == b"Hello FreeUnit!"
    last, last_payload = msgs[1]
    assert last.last is True and last.mmap is False and last_payload == b""
    assert worker.total_piggyback_responses == 1


def test_first_write_over_room_sends_header_alone():
    """A 2000-byte first write does not fit a 1008-byte plain-sized header
    buffer, so the header goes alone, the write follows in shared memory."""
    loop, shm, worker, sent = make_worker()
    body = b"X" * 2000
    worker._send_response(UnitResponse(status=200, content=body))
    loop.run_until_idle()

    msgs = data_msgs(sent)
    assert len(msgs) == 3
    hdr, hdr_payload = msgs[0]
    mm = MmapMsg.from_bytes(hdr_payload)
    assert UnitResponse.deserialize(shm.get_data(mm.mmap_id, mm.chunk_id, mm.size)).content == b""
    body_msg, body_payload = msgs[1]
    assert body_msg.mmap is True and body_msg.last is False
    mm = MmapMsg.from_bytes(body_payload)
    assert mm.size == 2000
    assert msgs[2][0].last is True
    assert worker.total_header_alone_responses == 1


def test_each_write_is_one_message():
    """Three writes: header+first write, two more buffers, the empty last."""
    loop, shm, worker, sent = make_worker()
    body = b"a" * 100 + b"b" * 10 + b"c" * 500
    resp = UnitResponse(status=200, content=body, write_sizes=[100, 10, 500])
    worker._send_response(resp)
    loop.run_until_idle()

    msgs = data_msgs(sent)
    assert len(msgs) == 4
    # 10 bytes <= NXT_UNIT_QUEUE_PLAIN_SIZE: plain data behind the header.
    assert msgs[1][0].mmap is False and msgs[1][1] == b"b" * 10
    # 500 bytes: shared memory when a chunk is free.
    assert msgs[2][0].mmap is True
    assert [m.last for m, _ in msgs] == [False, False, False, True]


def test_flush_before_output_sends_header_alone():
    """flush() before any echo: nxt_php_flush sends the header on its own."""
    loop, shm, worker, sent = make_worker()
    resp = UnitResponse(status=200, content=b"body", flush_header_first=True)
    worker._send_response(resp)
    loop.run_until_idle()
    msgs = data_msgs(sent)
    assert len(msgs) == 3
    mm = MmapMsg.from_bytes(msgs[0][1])
    assert UnitResponse.deserialize(shm.get_data(mm.mmap_id, mm.chunk_id, mm.size)).content == b""
    assert msgs[1][1] == b"body"


def test_no_output_sends_header_with_last_flag():
    """A response without body: nxt_unit_buf_send_done sends the header last=1."""
    loop, shm, worker, sent = make_worker()
    worker._send_response(UnitResponse(status=204, content=b""))
    loop.run_until_idle()
    msgs = data_msgs(sent)
    assert len(msgs) == 1
    assert msgs[0][0].last is True and msgs[0][0].mmap is True


def test_large_write_takes_whole_chunks_and_releases_the_tail():
    """64 KiB in one write: one mmap message of four 16 KiB chunks."""
    loop, shm, worker, sent = make_worker()
    body = b"X" * (64 * 1024)
    worker._send_response(UnitResponse(status=200, content=body))
    loop.run_until_idle()
    msgs = data_msgs(sent)
    assert len(msgs) == 3
    mm = MmapMsg.from_bytes(msgs[1][1])
    assert mm.size == 64 * 1024
    # Header chunk + 4 body chunks are held until the router releases them.
    busy = sum(seg.total_chunks - seg.free_chunks for seg in shm.segments.values())
    assert busy == 5


def test_small_buffer_falls_back_to_plain_under_oosm():
    """<= 1024 bytes probes shared memory and never waits: plain fallback."""
    loop, shm, worker, sent = make_worker(shm_pool=None)
    shm.max_segments = 1
    parked = []
    while True:
        a = shm.allocate(64 * 1024, probe=True)
        if a is None:
            break
        parked.append(a)
    worker._send_response(UnitResponse(status=200, content=b"Y" * 500))
    loop.run_until_idle()
    msgs = data_msgs(sent)
    assert len(msgs) == 2
    assert msgs[0][0].mmap is False            # header buffer went plain
    assert worker.total_oosm_waits == 0
    assert worker.total_shm_probe_fallbacks >= 1
    actions = [r.action for r in loop.get_trace()]
    assert "oosm_wait_shm_ack" not in actions


def test_large_buffer_waits_for_shm_ack_then_resumes():
    """> 1024 bytes cannot go plain: OOSM, wait for SHM_ACK, then send."""
    loop, shm, worker, sent = make_worker()
    shm.max_segments = 1
    parked = []
    while True:
        a = shm.allocate(64 * 1024, probe=True)
        if a is None:
            break
        parked.append(a)

    worker._send_response(UnitResponse(status=200, content=b"Z" * 5000))
    loop.run_until_idle()

    # Header went plain (probe miss); the 5000-byte body blocks.
    oosm = [p for p, _ in sent if p.type == PortMsgType.OOSM]
    assert len(oosm) == 1
    assert worker.total_oosm_waits == 1
    assert len(data_msgs(sent)) == 1           # header only so far
    assert shm.in_oosm

    # The router frees a chunk: SHM_ACK, the write resumes.
    a = parked.pop()
    shm.free(a.mmap_id, a.start_chunk_id, a.chunk_count)
    loop.run_until_idle()

    msgs = data_msgs(sent)
    assert len(msgs) == 3
    assert msgs[1][0].mmap is True and MmapMsg.from_bytes(msgs[1][1]).size == 5000
    assert msgs[2][0].last is True
    actions = [r.action for r in loop.get_trace()]
    assert "oosm_wait_shm_ack" in actions and "shm_ack_received" in actions


def test_php_static_script_target_lifecycle():
    """PR 751: fixed script targets vs dynamic targets on a missing script."""
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

    worker_fixed = AppWorker(
        loop=loop, worker_id=10, app_id="php_fixed", app_queue=app_queue,
        socket_pair=socket_pair, shm_pool=shm_pool, static_script=True, script_exists=False,
    )
    worker_fixed.on_send_response_to_router = mock_post

    req = UnitRequest(target="/info.php", path="/info.php")
    req_bytes = req.serialize()
    alloc = shm_pool.allocate(len(req_bytes), req_bytes)
    assert alloc is not None
    mmsg = shm_pool.to_mmap_msg(alloc)

    worker_fixed._process_request_mmap(stream=100, reply_port=1, mmsg=mmsg)
    loop.run_until_idle()

    assert len(sent_responses) == 1
    resp_fixed = sent_responses[0]
    assert resp_fixed.status == 404
    assert "X-Powered-By" in [h.name for h in resp_fixed.headers]

    # The worker released the request chunk when it released the request.
    alloc = shm_pool.allocate(len(req_bytes), req_bytes)
    mmsg = shm_pool.to_mmap_msg(alloc)
    worker_dynamic = AppWorker(
        loop=loop, worker_id=11, app_id="php_dynamic", app_queue=app_queue,
        socket_pair=socket_pair, shm_pool=shm_pool, static_script=False, script_exists=False,
    )
    worker_dynamic.on_send_response_to_router = mock_post
    worker_dynamic._process_request_mmap(stream=101, reply_port=1, mmsg=mmsg)
    loop.run_until_idle()

    assert len(sent_responses) == 2
    resp_dynamic = sent_responses[1]
    assert resp_dynamic.status == 404
    assert len(resp_dynamic.headers) == 0
