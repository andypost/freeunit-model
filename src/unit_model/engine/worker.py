"""Application Worker Process Runtime Simulation (nxt_unit.c, libunit).

Checked against FreeUnit master 42251f7c9e366df31a2e70a5da3679a2fa855b3d.

Models:
- The worker loop: the shared queue is drained before the socket is read.
- Request unpacking from shared memory and REQ_HEADERS_ACK
  (src/nxt_unit.c:2017, 2269).
- The response path, message by message, as libunit sends it:
  * nxt_unit_response_init() allocates the header buffer, at least
    NXT_UNIT_RESPONSE_PLAIN_ALLOC = 1008 bytes (src/nxt_unit.c:2949-2951).
  * nxt_unit_response_write_nb() (src/nxt_unit.c:3849-3936): while the header
    is unsent, the first write goes behind the fields as piggyback content
    when it fits the room left in the header buffer; if the buffer is a plain
    one (<= 1024 bytes) and the write does not fit, the header goes alone.
    nxt_unit_response_send() then sends the header buffer, without the last
    flag (src/nxt_unit.c:3249-3287).  Every later write is one more DATA
    message in a buffer of its own.
  * nxt_unit_get_outgoing_buf() (src/nxt_unit.c:5805-5912) picks the memory:
    up to NXT_UNIT_QUEUE_PLAIN_SIZE = 15 bytes, or any plain-sized buffer on
    the socket, is plain memory; a buffer up to NXT_UNIT_MAX_PLAIN_SIZE =
    1024 probes shared memory and falls back to plain when no chunk is free;
    a larger buffer sends OOSM and waits for SHM_ACK
    (nxt_unit_mmap_get, src/nxt_unit.c:5296-5346).
  * nxt_unit_request_done() (src/nxt_unit.c:4980-5033): a header never sent
    goes now with the last flag (nxt_unit_buf_send_done); otherwise an empty
    DATA message with the last flag ends the stream.
  * nxt_unit_req_send() (src/nxt_unit.c:8662-8716): a message that fits a
    queue item (<= 31 bytes: a 16-byte header plus 12-byte mmap record, or up
    to 15 bytes of plain data) goes through the port queue when the request
    is queued, or the last shared memory allocation worked; otherwise the
    socket.  A request on the socket stays there until its last message.
- Chunk release points: the worker releases the request chunks when it
  releases the request (src/nxt_unit.c:2621-2627) and the unused tail of a
  response buffer right after the send (src/nxt_unit.c:3661-3680); the
  router releases the sent response chunks (nxt_port_mmap_buf_completion).
- Detached response completion (nxt_port_detached_t).
- Request limits (max_requests), abrupt worker crashes and shutdowns.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from unit_model.c_types import (
    DetachedMode,
    HttpHeaderField,
    MmapMsg,
    NXT_PORT_MSG_SIZE,
    NXT_PORT_QUEUE_MSG_SIZE,
    NXT_UNIT_MAX_PLAIN_SIZE,
    NXT_UNIT_QUEUE_PLAIN_SIZE,
    NXT_UNIT_RESPONSE_PLAIN_ALLOC,
    NXT_UNIT_SHM_FAILED,
    NXT_UNIT_SHM_FULL,
    NXT_UNIT_SHM_OK,
    NXT_UNIT_SHM_UNKNOWN,
    PORT_MMAP_CHUNK_SIZE,
    PORT_MMAP_DATA_SIZE,
    CNxtUnitField,
    CNxtUnitResponse,
    PortMsg,
    PortMsgType,
    QuitMode,
    UnitRequest,
    UnitResponse,
)
from unit_model.engine.event_loop import SimEventLoop
from unit_model.engine.ipc_queue import AppQueue
from unit_model.engine.ipc_socket import SocketDatagram, UnixSocketPair
from unit_model.engine.shm_pool import SharedMemoryPool


class WorkerState(Enum):
    IDLE = auto()
    PROCESSING = auto()
    DETACHED = auto()
    SHUTTING_DOWN = auto()
    DEAD = auto()


class ReqQueueState(Enum):
    """nxt_unit_request_info_impl_t.queue_state (src/nxt_unit.c:49-53)."""

    UNQUEUED = auto()
    QUEUED = auto()
    SOCKET = auto()
    FAILED = auto()


@dataclass
class OutgoingBuf:
    """nxt_unit_mmap_buf_t: a response buffer in shared or plain memory."""

    capacity: int
    shm: bool
    mmap_id: int = -1
    chunk_id: int = -1
    nchunks: int = 0


@dataclass
class ResponseContext:
    """What libunit keeps for a response in flight."""

    resp: UnitResponse
    header_buf: Optional[OutgoingBuf] = None
    header_sent: bool = False
    pending_writes: Deque[bytes] = field(default_factory=deque)
    queue_state: ReqQueueState = ReqQueueState.UNQUEUED
    messages: int = 0
    waiting_shm_ack: bool = False
    done_sent: bool = False


class AppWorker:
    """Simulated libunit worker runtime process."""

    def __init__(
        self,
        loop: SimEventLoop,
        worker_id: int,
        app_id: str,
        app_queue: AppQueue,
        socket_pair: UnixSocketPair,
        shm_pool: SharedMemoryPool,
        max_requests: int = 1000,
        request_timeout_ms: int = 30_000,
        static_script: bool = False,
        script_exists: bool = True,
        has_port_queue: bool = True,
    ):
        self.loop = loop
        self.worker_id = worker_id
        self.app_id = app_id
        self.app_queue = app_queue
        self.socket_pair = socket_pair
        self.shm_pool = shm_pool
        self.max_requests = max_requests
        self.request_timeout_ms = request_timeout_ms
        self.static_script = static_script
        self.script_exists = script_exists
        # port_impl->queue: the router port has a queue for small messages.
        self.has_port_queue = has_port_queue

        self.state: WorkerState = WorkerState.IDLE
        self.requests_handled: int = 0
        self.current_stream: Optional[int] = None
        self.current_reply_port: Optional[int] = None
        self.current_request_chunks: Optional[Tuple[int, int, int]] = None
        self._resp: Optional[ResponseContext] = None

        # lib->shm: what the outgoing shared memory last showed.
        self.shm_state: int = NXT_UNIT_SHM_UNKNOWN

        # Counters over all responses of this worker.
        self.total_response_messages: int = 0
        self.total_shm_messages: int = 0
        self.total_plain_messages: int = 0
        self.total_queue_messages: int = 0
        self.total_socket_messages: int = 0
        self.total_piggyback_responses: int = 0
        self.total_header_alone_responses: int = 0
        self.total_shm_probe_fallbacks: int = 0
        self.total_oosm_waits: int = 0
        self.total_responses: int = 0

        # Framework handler: callable(UnitRequest) -> UnitResponse
        self.handler: Optional[Callable[[UnitRequest], UnitResponse]] = None

        # Link socket readability to worker event loop
        self.socket_pair.on_b_readable = self.on_socket_readable

        # Post a port message to the router: (reply_port, msg, payload, pid)
        self.on_send_response_to_router: Optional[
            Callable[[int, PortMsg, bytes, int], None]
        ] = None
        # The process is gone: main reaps it and the router removes the peer.
        self.on_exit: Optional[Callable[[int], None]] = None

    @property
    def is_alive(self) -> bool:
        return self.state != WorkerState.DEAD

    # ------------------------------------------------------------------ input

    def on_socket_readable(self) -> None:
        """Called when datagram arrives on worker socket."""
        if self.state in (WorkerState.DEAD, WorkerState.SHUTTING_DOWN):
            return

        self._check_next_work()

    def _check_next_work(self) -> None:
        """Check for and dispatch next work unit when worker is IDLE."""
        if self.state != WorkerState.IDLE:
            return

        # The shared queue is drained before the socket is read.
        if not self.app_queue.is_empty():
            item = self.app_queue.recv()
            if item:
                self.loop.record_trace(
                    f"worker_{self.worker_id}",
                    "app_queue_item_received",
                    tracking=item.tracking,
                    size=item.size,
                )
                self.app_queue.notification_received()
                if len(item.data) >= 16:
                    reply_port = int.from_bytes(item.data[:4], "little")
                    mmsg = MmapMsg.from_bytes(item.data[4:16])
                    self._process_request_mmap(stream=item.tracking, reply_port=reply_port, mmsg=mmsg)
                    return

        while True:
            dgram = self.socket_pair.recv_at_b()
            if not dgram:
                break

            pmsg = dgram.port_msg
            self.loop.record_trace(
                f"worker_{self.worker_id}",
                "socket_msg_received",
                msg_type=pmsg.type,
                stream=pmsg.stream,
            )

            if pmsg.type == PortMsgType.READ_QUEUE:
                self._check_next_work()
                return

            if pmsg.type == PortMsgType.REQ_HEADERS:
                if pmsg.mmap and len(dgram.payload) >= 12:
                    mmsg = MmapMsg.from_bytes(dgram.payload[:12])
                    self._process_request_mmap(
                        stream=pmsg.stream,
                        reply_port=pmsg.reply_port,
                        mmsg=mmsg,
                    )
                    return

            if pmsg.type == PortMsgType.QUIT:
                mode = QuitMode.GRACEFUL if (dgram.payload and dgram.payload[0] == 1) else QuitMode.NORMAL
                self.shutdown(mode=mode)
                return
            return

    def _send_port_msg(self, pmsg: PortMsg, payload: bytes) -> None:
        if self.on_send_response_to_router:
            self.on_send_response_to_router(
                self.current_reply_port or 0, pmsg, payload, self.worker_id
            )

    def _process_request_mmap(self, stream: int, reply_port: int, mmsg: MmapMsg) -> None:
        """Unpack request from shared memory and execute handler."""
        self.state = WorkerState.PROCESSING
        self.current_stream = stream
        self.current_reply_port = reply_port
        self.requests_handled += 1
        nchunks = max(1, math.ceil(mmsg.size / PORT_MMAP_CHUNK_SIZE))
        self.current_request_chunks = (mmsg.mmap_id, mmsg.chunk_id, nchunks)

        # REQ_HEADERS_ACK (src/nxt_unit.c:2017): the router learns which
        # process took the request.  It never goes through the queue
        # (src/nxt_unit.c:8698).
        ack_msg = PortMsg(
            stream=stream,
            pid=self.worker_id,
            reply_port=0,
            type=PortMsgType.REQ_HEADERS_ACK,
            last=False,
        )
        self._send_port_msg(ack_msg, b"")

        raw_req = self.shm_pool.get_data(mmsg.mmap_id, mmsg.chunk_id, mmsg.size)
        parsed_req = UnitRequest.deserialize(raw_req) if raw_req else None

        def execute_handler() -> None:
            if self.state == WorkerState.DEAD:
                return

            if self.static_script:
                # Fixed target (PR 751): per-request stat() is skipped.  A
                # missing script gives a 404 after php_request_startup(),
                # with the PHP startup headers.
                if not self.script_exists:
                    resp = UnitResponse(
                        status=404,
                        headers=[HttpHeaderField(name="X-Powered-By", value="PHP/8.5.0-dev")],
                        content=b"",
                    )
                    self._send_response(resp)
                    return
            else:
                # Dynamic target: stat() runs before php_request_startup(),
                # so a missing script gives a 404 with no PHP headers.
                if not self.script_exists:
                    resp = UnitResponse(status=404, headers=[], content=b"")
                    self._send_response(resp)
                    return

            resp = UnitResponse(status=200, content=b"Simulated Response")
            if self.handler:
                resp = self.handler(parsed_req)

            self._send_response(resp)

        self.loop.schedule(
            1_000,  # 1us simulated application execution
            f"worker_{self.worker_id}",
            "app_handler_execution",
            execute_handler,
        )

    # --------------------------------------------------------------- response

    @staticmethod
    def header_buf_size(resp: UnitResponse) -> int:
        """nxt_unit_response_init(): nxt_unit_response_buf_size()
        (src/nxt_unit.c:302-327) raised to NXT_UNIT_RESPONSE_PLAIN_ALLOC
        (src/nxt_unit.c:2949-2951)."""
        import ctypes

        fields_size = sum(
            len(h.name.encode("utf-8")) + len(h.value.encode("utf-8")) for h in resp.headers
        )
        total = ctypes.sizeof(CNxtUnitResponse) + len(resp.headers) * (
            ctypes.sizeof(CNxtUnitField) + 2
        )
        return max(total + fields_size, NXT_UNIT_RESPONSE_PLAIN_ALLOC)

    @staticmethod
    def header_bytes(resp: UnitResponse, piggyback: bytes) -> bytes:
        """The header buffer as sent: nxt_unit_response_t, fields, strings,
        then the piggyback content (nxt_unit_response_add_content)."""
        hdr = UnitResponse(
            status=resp.status,
            headers=resp.headers,
            content=piggyback,
            content_length=(
                resp.content_length if resp.content_length is not None else len(resp.content)
            ),
        )
        return hdr.serialize()

    def _send_response(self, resp: UnitResponse) -> None:
        """The application answers: response_init, the writes, request_done."""
        if self.state == WorkerState.DEAD:
            return

        ctx = ResponseContext(resp=resp)
        ctx.pending_writes = deque(resp.writes())
        self._resp = ctx
        self.total_responses += 1

        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "response_init",
            stream=self.current_stream,
            status=resp.status,
            fields=len(resp.headers),
            body_bytes=len(resp.content),
            writes=len(ctx.pending_writes),
        )
        self._continue_response()

    def _continue_response(self) -> None:
        """Run the response state machine until it blocks or completes."""
        ctx = self._resp
        if ctx is None or self.state == WorkerState.DEAD:
            return

        # nxt_unit_response_init: the header buffer.
        if ctx.header_buf is None:
            size = self.header_buf_size(ctx.resp)
            buf = self._get_outgoing_buf(ctx, size, size)
            if buf is None:
                return  # waiting for SHM_ACK
            ctx.header_buf = buf

        # An application flush() before any output sends the header alone
        # (nxt_php_flush, src/nxt_php_sapi.c:1724-1748).
        if ctx.resp.flush_header_first and not ctx.header_sent:
            self._response_send(ctx, b"")

        while ctx.pending_writes:
            w = ctx.pending_writes[0]

            if not ctx.header_sent:
                # nxt_unit_response_write_nb, "response is not sent yet"
                # branch (src/nxt_unit.c:3873-3900).
                header_only = len(self.header_bytes(ctx.resp, b""))
                room = ctx.header_buf.capacity - header_only
                if ctx.header_buf.capacity <= NXT_UNIT_MAX_PLAIN_SIZE and len(w) > room:
                    part = 0
                else:
                    part = min(len(w), room)
                self._response_send(ctx, w[:part])
                w = w[part:]
                if w:
                    ctx.pending_writes[0] = w
                else:
                    ctx.pending_writes.popleft()
                continue

            # One more buffer per write (src/nxt_unit.c:3904-3933).
            part_size = min(len(w), PORT_MMAP_DATA_SIZE)
            min_part = min(part_size, PORT_MMAP_CHUNK_SIZE)
            buf = self._get_outgoing_buf(ctx, part_size, min_part)
            if buf is None:
                return  # waiting for SHM_ACK, resumed by _on_shm_ack
            part_size = min(part_size, buf.capacity)
            self._mmap_buf_send(ctx, buf, w[:part_size], last=False)
            w = w[part_size:]
            if w:
                ctx.pending_writes[0] = w
            else:
                ctx.pending_writes.popleft()

        self._request_done(ctx)

    def _response_send(self, ctx: ResponseContext, piggyback: bytes) -> None:
        """nxt_unit_response_send: header fields plus piggyback content, no
        last flag (src/nxt_unit.c:3249-3287)."""
        assert ctx.header_buf is not None
        data = self.header_bytes(ctx.resp, piggyback)
        if piggyback:
            self.total_piggyback_responses += 1
        else:
            self.total_header_alone_responses += 1
        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "response_header_send",
            stream=self.current_stream,
            piggyback_bytes=len(piggyback),
        )
        self._mmap_buf_send(ctx, ctx.header_buf, data, last=False)
        ctx.header_sent = True

    def _request_done(self, ctx: ResponseContext) -> None:
        """nxt_unit_request_done (src/nxt_unit.c:4980-5033)."""
        if ctx.done_sent:
            return
        ctx.done_sent = True
        if not ctx.header_sent:
            # nxt_unit_buf_send_done: the header buffer goes with last=1.
            assert ctx.header_buf is not None
            self.loop.record_trace(
                f"worker_{self.worker_id}",
                "response_header_send",
                stream=self.current_stream,
                piggyback_bytes=0,
                last=True,
            )
            self.total_header_alone_responses += 1
            self._mmap_buf_send(ctx, ctx.header_buf, self.header_bytes(ctx.resp, b""), last=True)
            ctx.header_sent = True
        else:
            # An empty DATA message with the last flag; it always fits the
            # queue (src/nxt_unit.c:5027-5030, 8653).
            msg = PortMsg(
                stream=self.current_stream or 0,
                pid=self.worker_id,
                reply_port=0,
                type=PortMsgType.DATA,
                last=True,
                mmap=False,
            )
            self._req_send(ctx, msg, b"")

        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "response_done",
            stream=self.current_stream,
            messages=ctx.messages,
        )
        self._request_info_release()

    def _request_info_release(self) -> None:
        """nxt_unit_request_info_release: the request's incoming (request)
        buffers are freed, which sets their chunks free in the router's
        segment (src/nxt_unit.c:2621-2627, 3661-3680)."""
        if self.current_request_chunks is not None:
            mmap_id, chunk_id, n = self.current_request_chunks
            self.shm_pool.free(mmap_id, chunk_id, n)
            self.current_request_chunks = None
        self._resp = None
        self.state = WorkerState.IDLE
        self.current_stream = None

        if self.requests_handled >= self.max_requests:
            self.loop.record_trace(
                f"worker_{self.worker_id}",
                "max_requests_reached",
                handled=self.requests_handled,
            )
            self.shutdown(QuitMode.GRACEFUL)
            return

        self.loop.schedule(0, f"worker_{self.worker_id}", "check_next_work", self._check_next_work)

    # ---------------------------------------------------------- buffers & send

    def _get_outgoing_buf(
        self, ctx: ResponseContext, size: int, min_size: int
    ) -> Optional[OutgoingBuf]:
        """nxt_unit_get_outgoing_buf (src/nxt_unit.c:5805-5912).

        Returns None when the writer blocks for SHM_ACK; _on_shm_ack resumes.
        """
        plain_ok = size <= NXT_UNIT_MAX_PLAIN_SIZE

        if size <= NXT_UNIT_QUEUE_PLAIN_SIZE or (
            plain_ok and (not self.has_port_queue or ctx.queue_state == ReqQueueState.SOCKET)
        ):
            return self._plain_buf(ctx, size, reason="small_or_socket")

        if plain_ok and self.shm_state == NXT_UNIT_SHM_FAILED:
            # Master retries a failed segment every NXT_UNIT_SHM_RETRY seconds;
            # the simulated clock never gets there, so this is always plain.
            return self._plain_buf(ctx, size, reason="shm_failed_backoff")

        nchunks = math.ceil(size / PORT_MMAP_CHUNK_SIZE)
        alloc = self.shm_pool.allocate(nchunks * PORT_MMAP_CHUNK_SIZE, probe=plain_ok)
        if alloc is None:
            if plain_ok:
                self.total_shm_probe_fallbacks += 1
                return self._plain_buf(ctx, size, reason="shm_probe_miss")

            # Out of shared memory: OOSM to the router, then block in recv
            # until SHM_ACK (src/nxt_unit.c:5296-5346, 5402-5470).
            self.shm_state = NXT_UNIT_SHM_FULL
            self.total_oosm_waits += 1
            ctx.waiting_shm_ack = True
            oosm = PortMsg(stream=0, pid=self.worker_id, reply_port=0, type=PortMsgType.OOSM)
            self._send_port_msg(oosm, b"")
            self.loop.record_trace(
                f"worker_{self.worker_id}",
                "oosm_wait_shm_ack",
                stream=self.current_stream,
                requested_bytes=size,
            )
            self.shm_pool.wait_shm_ack(self._on_shm_ack)
            return None

        self.shm_state = NXT_UNIT_SHM_OK
        # A buffer that could have been plain gets no more room than asked
        # for (src/nxt_unit.c:5876-5884).
        capacity = size if plain_ok else nchunks * PORT_MMAP_CHUNK_SIZE
        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "outgoing_mmap_allocation",
            stream=self.current_stream,
            mmap_id=alloc.mmap_id,
            chunk_id=alloc.start_chunk_id,
            chunks=nchunks,
        )
        return OutgoingBuf(
            capacity=capacity,
            shm=True,
            mmap_id=alloc.mmap_id,
            chunk_id=alloc.start_chunk_id,
            nchunks=nchunks,
        )

    def _plain_buf(self, ctx: ResponseContext, size: int, reason: str) -> OutgoingBuf:
        # A plain message over a queue item moves an unqueued request to the
        # socket for good (src/nxt_unit.c:5887-5892).
        if size > NXT_UNIT_QUEUE_PLAIN_SIZE and ctx.queue_state == ReqQueueState.UNQUEUED:
            ctx.queue_state = ReqQueueState.SOCKET
        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "outgoing_plain_allocation",
            stream=self.current_stream,
            bytes=size,
            reason=reason,
        )
        return OutgoingBuf(capacity=size, shm=False)

    def _on_shm_ack(self) -> None:
        """nxt_unit_wait_shm_ack returned: retry the allocation."""
        ctx = self._resp
        if ctx is None or self.state == WorkerState.DEAD:
            return
        ctx.waiting_shm_ack = False
        self.loop.record_trace(
            f"worker_{self.worker_id}", "shm_ack_received", stream=self.current_stream
        )
        self._continue_response()

    def _mmap_buf_send(
        self, ctx: ResponseContext, buf: OutgoingBuf, data: bytes, last: bool
    ) -> None:
        """nxt_unit_mmap_buf_send (src/nxt_unit.c:3529-3640): one DATA message
        per buffer, as a 12-byte mmap record or as plain data behind the
        header."""
        use_mmap = buf.shm and len(data) > 0
        msg = PortMsg(
            stream=self.current_stream or 0,
            pid=self.worker_id,
            reply_port=0,
            type=PortMsgType.DATA,
            last=last,
            mmap=use_mmap,
        )
        if use_mmap:
            self.shm_pool.write(buf.mmap_id, buf.chunk_id, data)
            payload = MmapMsg(mmap_id=buf.mmap_id, chunk_id=buf.chunk_id, size=len(data)).to_bytes()
            self.total_shm_messages += 1
        else:
            payload = data
            self.total_plain_messages += 1

        self._req_send(ctx, msg, payload)

        if buf.shm:
            # The router owns the chunks that carry data; the worker gives
            # back the rest of the allocation (src/nxt_unit.c:3579-3596,
            # 3661-3680).
            sent_chunks = math.ceil(len(data) / PORT_MMAP_CHUNK_SIZE) if data else 0
            if sent_chunks < buf.nchunks:
                self.shm_pool.free(buf.mmap_id, buf.chunk_id + sent_chunks, buf.nchunks - sent_chunks)
            buf.nchunks = 0

    def _req_send(self, ctx: ResponseContext, msg: PortMsg, payload: bytes) -> None:
        """nxt_unit_req_send (src/nxt_unit.c:8662-8716): queue or socket."""
        if ctx.queue_state == ReqQueueState.FAILED:
            return
        msg_size = NXT_PORT_MSG_SIZE + len(payload)
        queue = (
            self.has_port_queue
            and msg_size <= NXT_PORT_QUEUE_MSG_SIZE
            and msg.type != PortMsgType.REQ_HEADERS_ACK
            and (
                msg.last
                or ctx.queue_state == ReqQueueState.QUEUED
                or (ctx.queue_state == ReqQueueState.UNQUEUED and self.shm_state == NXT_UNIT_SHM_OK)
            )
        )
        if not queue and ctx.queue_state == ReqQueueState.QUEUED:
            # nxt_unit_req_queue_drain: wait until the router took the queued
            # messages, then the socket for good.  The simulated router takes
            # queue items as they come, so the drain always succeeds here.
            self.loop.record_trace(
                f"worker_{self.worker_id}", "req_queue_drain", stream=self.current_stream
            )
            ctx.queue_state = ReqQueueState.SOCKET

        channel = "queue" if queue else "socket"
        if queue:
            self.total_queue_messages += 1
            if ctx.queue_state == ReqQueueState.UNQUEUED:
                ctx.queue_state = ReqQueueState.QUEUED
        else:
            self.total_socket_messages += 1

        ctx.messages += 1
        self.total_response_messages += 1
        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "response_msg_sent",
            stream=msg.stream,
            last=msg.last,
            mmap=msg.mmap,
            bytes=msg_size,
            channel=channel,
        )
        self._send_port_msg(msg, payload)

    # ---------------------------------------------------------------- detached

    def trigger_detached_start(self) -> None:
        """fastcgi_finish_request / unit_finish_request detached execution start."""
        self.state = WorkerState.DETACHED
        detached_msg = PortMsg(
            stream=self.current_stream or 0,
            pid=self.worker_id,
            reply_port=0,
            type=PortMsgType.DETACHED,
            last=True,
        )
        self._send_port_msg(detached_msg, bytes([DetachedMode.START]))
        self.loop.record_trace(f"worker_{self.worker_id}", "detached_started")

    def trigger_detached_finish(self) -> None:
        """Detached execution completed."""
        detached_msg = PortMsg(
            stream=0,
            pid=self.worker_id,
            reply_port=0,
            type=PortMsgType.DETACHED,
            last=True,
        )
        self._send_port_msg(detached_msg, bytes([DetachedMode.FINISH]))
        self.state = WorkerState.IDLE
        self.loop.record_trace(f"worker_{self.worker_id}", "detached_finished")

    # --------------------------------------------------------------- lifecycle

    def shutdown(self, mode: QuitMode = QuitMode.NORMAL) -> None:
        """Shutdown worker."""
        self.state = WorkerState.SHUTTING_DOWN
        self.socket_pair.close()
        self.state = WorkerState.DEAD
        self.loop.record_trace(f"worker_{self.worker_id}", "worker_shutdown", mode=mode.name)
        self._resp = None
        if self.on_exit is not None:
            self.loop.schedule(100, f"worker_{self.worker_id}", "sigchld_remove_pid",
                               lambda: self.on_exit(self.worker_id))

    def crash_sigkill(self) -> None:
        """Simulate abrupt worker termination via SIGKILL.

        The segments of the dead process pair are unmapped by the router
        (nxt_port_mmaps_destroy, src/nxt_runtime.c:1687, src/nxt_router.c:8419);
        in the single pool of the model that is a release of the chunks the
        worker still held.
        """
        self.state = WorkerState.DEAD
        self.socket_pair.close()
        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "worker_abrupt_sigkill",
            in_flight_stream=self.current_stream,
        )
        if self.current_request_chunks is not None:
            mmap_id, chunk_id, n = self.current_request_chunks
            self.shm_pool.free(mmap_id, chunk_id, n)
            self.current_request_chunks = None
        ctx = self._resp
        if ctx is not None and ctx.header_buf is not None and ctx.header_buf.shm and ctx.header_buf.nchunks:
            self.shm_pool.free(ctx.header_buf.mmap_id, ctx.header_buf.chunk_id, ctx.header_buf.nchunks)
        self._resp = None
        if self.on_exit is not None:
            self.loop.schedule(100, f"worker_{self.worker_id}", "sigchld_remove_pid",
                               lambda: self.on_exit(self.worker_id))
