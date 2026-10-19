"""Application Worker Process Runtime Simulation (nxt_unit.c & libunit SAPI).

Faithfully models:
- Worker event loop with poll() on private socket fd and shared App Queue fd.
- Orderly draining of App Queue before socket fallback (fix for issues #573, #597).
- Request unpacking from shared memory chunks.
- Pluggable framework handler invocation.
- Detached response completion (nxt_port_detached_t / unit_finish_request).
- Request limits (max_requests) and execution timeouts.
- Abrupt worker crashes (SIGKILL / SIGCHLD) and graceful shutdowns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

from unit_model.c_types import (
    DetachedMode,
    HttpHeaderField,
    MmapMsg,
    NXT_PHP_RESPONSE_ROOM,
    NXT_UNIT_MAX_PLAIN_SIZE,
    PortMsg,
    PortMsgFlag,
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

        self.state: WorkerState = WorkerState.IDLE
        self.requests_handled: int = 0
        self.total_single_buffer_responses: int = 0
        self.total_multi_buffer_responses: int = 0
        self.current_stream: Optional[int] = None
        self.current_reply_port: Optional[int] = None

        # Framework handler: callable(UnitRequest) -> UnitResponse
        self.handler: Optional[Callable[[UnitRequest], UnitResponse]] = None

        # Link socket readability to worker event loop
        self.socket_pair.on_b_readable = self.on_socket_readable

        # Post response callback to router
        self.on_send_response_to_router: Optional[
            Callable[[int, PortMsg, bytes, int], None]
        ] = None

    @property
    def is_alive(self) -> bool:
        return self.state != WorkerState.DEAD

    def on_socket_readable(self) -> None:
        """Called when datagram arrives on worker socket."""
        if self.state in (WorkerState.DEAD, WorkerState.SHUTTING_DOWN):
            return

        self._check_next_work()

    def _check_next_work(self) -> None:
        """Check for and dispatch next work unit when worker is IDLE."""
        if self.state != WorkerState.IDLE:
            return

        # Critical FreeUnit invariant: ALWAYS drain shared queue FIRST before socket!
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

        # If queue is empty, read socket datagrams
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
                # Doorbell wake-up for shared queue
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

    def _process_request_mmap(self, stream: int, reply_port: int, mmsg: MmapMsg) -> None:
        """Unpack request from shared memory and execute handler."""
        self.state = WorkerState.PROCESSING
        self.current_stream = stream
        self.current_reply_port = reply_port
        self.requests_handled += 1

        # Send REQ_HEADERS_ACK back to router
        ack_msg = PortMsg(
            stream=stream,
            pid=self.worker_id,
            reply_port=0,
            type=PortMsgType.REQ_HEADERS_ACK,
            last=False,
        )
        if self.on_send_response_to_router:
            self.on_send_response_to_router(reply_port, ack_msg, b"", self.worker_id)

        # Retrieve request payload from shared memory
        raw_req = self.shm_pool.get_data(mmsg.mmap_id, mmsg.chunk_id, mmsg.size)
        parsed_req = UnitRequest.deserialize(raw_req) if raw_req else None

        # Simulate execution latency & handler dispatch
        def execute_handler():
            if self.state == WorkerState.DEAD:
                return

            if self.static_script:
                # Fixed target (PR 751): per-request stat() is skipped.
                # If script does not exist, 404 is emitted after php_request_startup(),
                # retaining PHP startup headers (e.g. X-Powered-By: PHP/8.5.0-dev).
                if not self.script_exists:
                    resp = UnitResponse(
                        status=404,
                        headers=[HttpHeaderField(name="X-Powered-By", value="PHP/8.5.0-dev")],
                        content=b"",
                    )
                    self._send_response(resp)
                    return
            else:
                # Dynamic target: stat() runs before php_request_startup().
                # If script does not exist, 404 is emitted with no PHP headers.
                if not self.script_exists:
                    resp = UnitResponse(status=404, headers=[], content=b"")
                    self._send_response(resp)
                    return

            resp = UnitResponse(status=200, content=b"Simulated Response")
            if self.handler:
                # Call framework handler
                resp = self.handler(parsed_req)

            self._send_response(resp)

        self.loop.schedule(
            1_000,  # 1us simulated application execution
            f"worker_{self.worker_id}",
            "app_handler_execution",
            execute_handler,
        )

    def _send_response(self, resp: UnitResponse) -> None:
        """Write response back to router via shared memory and socket.
        
        Faithfully models single-buffer response framing (K2 / PR 776):
        Responses where body length <= NXT_PHP_RESPONSE_ROOM (8192 bytes)
        fit entirely into the response header buffer as piggyback content,
        reducing port messages to a single response message. Larger responses
        require chunked transmission.
        """
        if self.state == WorkerState.DEAD:
            return

        is_single_buffer = len(resp.content) <= NXT_PHP_RESPONSE_ROOM
        if is_single_buffer:
            self.total_single_buffer_responses += 1
            self.loop.record_trace(
                f"worker_{self.worker_id}",
                "response_single_buffer_piggybacked",
                stream=self.current_stream,
                body_size=len(resp.content),
            )
        else:
            self.total_multi_buffer_responses += 1
            self.loop.record_trace(
                f"worker_{self.worker_id}",
                "response_multi_buffer_chunked",
                stream=self.current_stream,
                body_size=len(resp.content),
            )

        resp_bytes = resp.serialize()

        # Allocate shared memory for response
        alloc = self.shm_pool.allocate(len(resp_bytes), resp_bytes)
        if alloc is None:
            self.loop.record_trace(f"worker_{self.worker_id}", "response_shm_oosm")
            return

        mmsg = self.shm_pool.to_mmap_msg(alloc)
        mmap_bytes = mmsg.to_bytes()

        data_msg = PortMsg(
            stream=self.current_stream or 0,
            pid=self.worker_id,
            reply_port=0,
            type=PortMsgType.DATA,
            last=True,
            mmap=True,
        )

        if self.on_send_response_to_router:
            self.on_send_response_to_router(
                self.current_reply_port or 0,
                data_msg,
                mmap_bytes,
                self.worker_id,
            )

        self.state = WorkerState.IDLE
        self.current_stream = None

        # Check max requests limit
        if self.requests_handled >= self.max_requests:
            self.loop.record_trace(
                f"worker_{self.worker_id}",
                "max_requests_reached",
                handled=self.requests_handled,
            )
            self.shutdown(QuitMode.GRACEFUL)
            return

        # Check next work in queue or socket
        self.loop.schedule(0, f"worker_{self.worker_id}", "check_next_work", self._check_next_work)

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
        if self.on_send_response_to_router:
            self.on_send_response_to_router(
                self.current_reply_port or 0,
                detached_msg,
                bytes([DetachedMode.START]),
                self.worker_id,
            )
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
        if self.on_send_response_to_router:
            self.on_send_response_to_router(
                self.current_reply_port or 0,
                detached_msg,
                bytes([DetachedMode.FINISH]),
                self.worker_id,
            )
        self.state = WorkerState.IDLE
        self.loop.record_trace(f"worker_{self.worker_id}", "detached_finished")

    def shutdown(self, mode: QuitMode = QuitMode.NORMAL) -> None:
        """Shutdown worker."""
        self.state = WorkerState.SHUTTING_DOWN
        self.socket_pair.close()
        self.state = WorkerState.DEAD
        self.loop.record_trace(f"worker_{self.worker_id}", "worker_shutdown", mode=mode.name)

    def crash_sigkill(self) -> None:
        """Simulate abrupt worker termination via SIGKILL."""
        self.state = WorkerState.DEAD
        self.socket_pair.close()
        self.loop.record_trace(
            f"worker_{self.worker_id}",
            "worker_abrupt_sigkill",
            in_flight_stream=self.current_stream,
        )
