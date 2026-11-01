"""Router Data Plane Subsystem (nxt_router.c, nxt_http_route.c, nxt_port_rpc.c).

Models:
- N router event engines with thread-isolated event dispatchers.
- Routing decision chains: 'pass' (to app), 'return' (immediate), 'share' (static).
- RPC lvlhsh stream registry: tracks active streams, reply ports, and in-flight responses.
- Dual-channel message dispatch: App Queue (<=31B) vs Socket fallback + Shared Memory chunks.
- Response handling as nxt_router_response_ready_handler does it
  (src/nxt_router.c:6437-6636 at FreeUnit master 42251f7c9e366df31a2e70a5da3679a2fa855b3d):
  the first DATA message carries the header, which is parsed, then the
  compression decision is made (nxt_http_comp_check_acceptable,
  nxt_http_comp_apply_compression) and the header is sent to the client once;
  the piggyback content of that message and every later message are body
  parts; the message with the last flag appends the end-of-response buffer.
  Chunk release: a header chunk with no piggyback content is released right
  after the parse; a chunk that carries body is released when the client
  write completes (nxt_port_mmap_buf_completion).  Request chunks are released
  by the worker, not by the router.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

from unit_model.c_types import (
    HttpHeaderField,
    MmapMsg,
    PortMsg,
    PortMsgFlag,
    PortMsgType,
    QueueItem,
    UnitRequest,
    UnitResponse,
)
from unit_model.engine.event_loop import SimEventLoop
from unit_model.engine.ipc_queue import AppQueue
from unit_model.engine.ipc_socket import UnixSocketPair
from unit_model.engine.listener import Connection, Listener
from unit_model.engine.shm_pool import SharedMemoryPool


class RouteAction(Enum):
    PASS = auto()
    RETURN = auto()
    SHARE = auto()
    PROXY = auto()


@dataclass
class RpcStream:
    stream_id: int
    engine_id: int
    conn: Connection
    request: UnitRequest
    app_id: str
    target_worker_id: Optional[int] = None
    headers_acked: bool = False
    header_sent: bool = False
    status: int = 0
    coding: Optional[str] = None       # the compression decision, made once
    body_bytes: int = 0
    messages: int = 0


@dataclass
class RouterEngine:
    engine_id: int
    active_streams: Dict[int, RpcStream] = field(default_factory=dict)


class Router:
    """Simulated FreeUnit Router Data Plane."""

    def __init__(
        self,
        loop: SimEventLoop,
        listener: Listener,
        shm_pool: SharedMemoryPool,
        num_engines: int = 2,
    ):
        self.loop = loop
        self.listener = listener
        self.shm_pool = shm_pool
        self.num_engines = num_engines
        self.engines: List[RouterEngine] = [RouterEngine(engine_id=i) for i in range(num_engines)]
        self.next_stream_id: int = 1000

        # Hook listener to router dispatch
        self.listener.on_request_ready = self.on_listener_request

        # Worker registrations: app_id -> List[(worker_id, app_queue, socket_pair)]
        self.workers: Dict[str, List[Tuple[int, AppQueue, UnixSocketPair]]] = {}
        self.worker_rr_index: Dict[str, int] = {}

    def register_worker(
        self,
        app_id: str,
        worker_id: int,
        app_queue: AppQueue,
        socket_pair: UnixSocketPair,
    ) -> None:
        """Register an application worker process with the router."""
        if app_id not in self.workers:
            self.workers[app_id] = []
            self.worker_rr_index[app_id] = 0
        self.workers[app_id].append((worker_id, app_queue, socket_pair))
        self.loop.record_trace("router", "worker_registered", app_id=app_id, worker_id=worker_id)

    def on_worker_death(self, worker_id: int) -> None:
        """A worker process is gone (SIGCHLD in main, REMOVE_PID to the router).

        nxt_port_rpc_remove_peer() fails every stream the dead process had
        taken: a 503 if the header is unsent, else the connection closes
        (nxt_http_request_error).  In master the request queue is shared by
        the processes of an application, so items the dead process had not
        taken are taken by the others; the model has one queue per worker
        and re-dispatches those items to the remaining workers.
        """
        for app_id, worker_list in self.workers.items():
            entry = next((w for w in worker_list if w[0] == worker_id), None)
            if entry is None:
                continue
            _, app_queue, _ = entry
            worker_list.remove(entry)
            if worker_list:
                self.worker_rr_index[app_id] %= len(worker_list)
            else:
                self.worker_rr_index[app_id] = 0
            self.loop.record_trace("router", "worker_removed", app_id=app_id, worker_id=worker_id)

            pending_items = []
            while not app_queue.is_empty():
                item = app_queue.recv()
                if item is not None:
                    pending_items.append(item.tracking)

            for engine in self.engines:
                for rpc in list(engine.active_streams.values()):
                    if rpc.target_worker_id != worker_id:
                        continue
                    if rpc.stream_id in pending_items and not rpc.headers_acked:
                        # Not taken yet: re-dispatch (the shared queue in master).
                        self.loop.record_trace(
                            f"engine_{rpc.engine_id}", "redispatch_after_worker_death",
                            stream=rpc.stream_id,
                        )
                        self._dispatch_to_app(rpc)
                        continue
                    self.loop.record_trace(
                        f"engine_{rpc.engine_id}", "rpc_peer_removed", stream=rpc.stream_id
                    )
                    self._fail_stream(rpc, 503)

    def on_listener_request(
        self,
        engine_id: int,
        conn: Connection,
        req: UnitRequest,
        action: RouteAction = RouteAction.PASS,
        app_id: str = "default_app",
    ) -> None:
        """Called when listener parses an HTTP request."""
        engine = self.engines[engine_id]
        stream_id = self.next_stream_id
        self.next_stream_id += 1

        rpc = RpcStream(
            stream_id=stream_id,
            engine_id=engine_id,
            conn=conn,
            request=req,
            app_id=app_id,
        )
        engine.active_streams[stream_id] = rpc

        self.loop.record_trace(
            f"engine_{engine_id}",
            "route_dispatch",
            stream=stream_id,
            route_action=action.name,
            path=req.path,
        )

        if action == RouteAction.RETURN:
            # Immediate synthetic response
            resp = UnitResponse(status=200, content=b"Direct Return")
            self.listener.send_http_response(conn, resp, stream=stream_id)
            del engine.active_streams[stream_id]
            return

        if action == RouteAction.PASS:
            self._dispatch_to_app(rpc)

    def _dispatch_to_app(self, rpc: RpcStream) -> None:
        """Dispatches request via dual-channel IPC to an available worker."""
        worker_list = self.workers.get(rpc.app_id)
        if not worker_list:
            self.loop.record_trace(
                f"engine_{rpc.engine_id}",
                "app_not_found_503",
                stream=rpc.stream_id,
                app_id=rpc.app_id,
            )
            resp = UnitResponse(status=503, content=b"No workers available")
            self.listener.send_http_response(rpc.conn, resp, stream=rpc.stream_id)
            del self.engines[rpc.engine_id].active_streams[rpc.stream_id]
            return

        # Pick worker round-robin
        rr_idx = self.worker_rr_index[rpc.app_id]
        worker_id, app_queue, socket_pair = worker_list[rr_idx]
        self.worker_rr_index[rpc.app_id] = (rr_idx + 1) % len(worker_list)
        rpc.target_worker_id = worker_id

        # Serialize request
        req_bytes = rpc.request.serialize()

        # Allocate shared memory chunk
        alloc = self.shm_pool.allocate(len(req_bytes), req_bytes)
        if alloc is None:
            # OOSM backpressure: drop or return 503
            self.loop.record_trace(
                f"engine_{rpc.engine_id}",
                "dispatch_failed_oosm",
                stream=rpc.stream_id,
            )
            resp = UnitResponse(status=503, content=b"Out of shared memory")
            self.listener.send_http_response(rpc.conn, resp, stream=rpc.stream_id)
            del self.engines[rpc.engine_id].active_streams[rpc.stream_id]
            return

        # The worker releases these chunks when it releases the request
        # (src/nxt_unit.c:2621-2627); the router does not touch them again.

        # Build nxt_port_mmap_msg_t
        mmap_msg = self.shm_pool.to_mmap_msg(alloc)
        mmap_bytes = mmap_msg.to_bytes()

        # Build nxt_port_msg_t
        pmsg = PortMsg(
            stream=rpc.stream_id,
            pid=0,
            reply_port=rpc.engine_id,
            type=PortMsgType.REQ_HEADERS,
            last=True,
            mmap=True,
        )

        # Pack reply_port (4 bytes) + mmap_bytes (12 bytes) into queue payload (16 bytes <= 31 bytes)
        queue_payload = rpc.engine_id.to_bytes(4, "little") + mmap_bytes
        queue_ok, need_notify, cookie = app_queue.send(queue_payload, tracking=rpc.stream_id)

        if queue_ok:
            self.loop.record_trace(
                f"engine_{rpc.engine_id}",
                "dispatched_via_app_queue",
                stream=rpc.stream_id,
                worker=worker_id,
                need_notify=need_notify,
            )
            if need_notify:
                # Send READ_QUEUE doorbell over socket
                doorbell_msg = PortMsg(
                    stream=0,
                    pid=0,
                    reply_port=rpc.engine_id,
                    type=PortMsgType.READ_QUEUE,
                    last=True,
                )
                socket_pair.send_a_to_b(doorbell_msg)
        else:
            # Fallback directly to socket datagram
            self.loop.record_trace(
                f"engine_{rpc.engine_id}",
                "dispatched_via_socket_fallback",
                stream=rpc.stream_id,
                worker=worker_id,
            )
            socket_pair.send_a_to_b(pmsg, payload=mmap_bytes)

    def on_worker_response_msg(
        self,
        engine_id: int,
        pmsg: PortMsg,
        payload: bytes,
        worker_id: int,
    ) -> None:
        """Handle one port message from an app worker."""
        engine = self.engines[engine_id]

        if pmsg.type == PortMsgType.OOSM:
            # nxt_router_oosm_handler: ack at once if a chunk is already free.
            self.loop.record_trace(f"engine_{engine_id}", "oosm_received", worker=worker_id)
            self.shm_pool.router_oosm_handler()
            return

        rpc = engine.active_streams.get(pmsg.stream)

        if pmsg.type == PortMsgType.REQ_HEADERS_ACK:
            if rpc is not None:
                rpc.headers_acked = True
                self.loop.record_trace(
                    f"engine_{engine_id}",
                    "headers_ack_received",
                    stream=pmsg.stream,
                )
            return

        if pmsg.type not in (PortMsgType.DATA, PortMsgType.RPC_ERROR, PortMsgType.DETACHED):
            return

        mmsg: Optional[MmapMsg] = None
        data = b""
        if pmsg.mmap and len(payload) >= 12:
            mmsg = MmapMsg.from_bytes(payload[:12])
            data = self.shm_pool.get_data(mmsg.mmap_id, mmsg.chunk_id, mmsg.size) or b""
        else:
            data = payload

        def release_chunks() -> None:
            # nxt_port_mmap_buf_completion: the chunks the message named.
            if mmsg is not None:
                n = (mmsg.size + 16383) // 16384
                self.shm_pool.free(mmsg.mmap_id, mmsg.chunk_id, n)

        if rpc is None:
            # A request the router gave up on (src/nxt_router.c:6463-6479):
            # the buffer is completed by the port and nothing is answered.
            self.loop.record_trace(
                f"engine_{engine_id}",
                "response_for_unknown_stream",
                stream=pmsg.stream,
                msg_type=pmsg.type,
                last=pmsg.last,
            )
            release_chunks()
            return

        if pmsg.type == PortMsgType.DETACHED:
            return

        rpc.messages += 1
        conn = rpc.conn
        if rpc.conn.closing:
            # r->error: nothing more goes to the client; the chunk is
            # released and the stream is unlinked (src/nxt_router.c:6481-6484).
            release_chunks()
            del engine.active_streams[pmsg.stream]
            self.loop.record_trace(f"engine_{engine_id}", "stream_dropped_conn_closed", stream=pmsg.stream)
            return

        if pmsg.type == PortMsgType.RPC_ERROR:
            release_chunks()
            self._fail_stream(rpc, 503)
            return

        if not rpc.header_sent:
            # nxt_router_response_header_parse on the first message.
            try:
                parsed = UnitResponse.deserialize(data)
            except Exception:
                release_chunks()
                self._fail_stream(rpc, 503)
                return
            rpc.header_sent = True
            rpc.status = parsed.status

            # Compression is decided here, once, before the header goes out
            # (src/nxt_router.c:6569-6586).  The model has no compressor
            # configured, so the decision is always "identity".
            rpc.coding = "identity"
            self.loop.record_trace(
                f"engine_{engine_id}",
                "compression_negotiated",
                stream=pmsg.stream,
                coding=rpc.coding,
            )

            header = UnitResponse(status=parsed.status, headers=parsed.headers,
                                  content=b"", content_length=parsed.content_length)
            self.listener.send_http_header(conn, header, stream=pmsg.stream)
            body = parsed.content
            if not body:
                # The header buffer is empty after the parse: completed through
                # the fast work queue, before any body part (src/nxt_router.c:6543-6555).
                release_chunks()
                if pmsg.last:
                    self._finish_stream(rpc)
                else:
                    self.loop.record_trace(f"engine_{engine_id}", "response_header_only_msg", stream=pmsg.stream)
                return
            # The piggyback part goes through the compressor like the parts
            # that follow it (src/nxt_router.c:6589-6610).
            rpc.body_bytes += len(body)
            self.listener.send_http_body(
                conn, body, last=pmsg.last, stream=pmsg.stream, on_done=release_chunks
            )
            if pmsg.last:
                self._finish_stream(rpc)
            return

        # Streaming: every later message is a body part (src/nxt_router.c:6512-6540).
        if data:
            rpc.body_bytes += len(data)
            self.listener.send_http_body(
                conn, data, last=pmsg.last, stream=pmsg.stream, on_done=release_chunks
            )
        elif pmsg.last:
            # The empty last message: nxt_http_buf_last(r) ends the body.
            self.listener.send_http_body(conn, b"", last=True, stream=pmsg.stream)
        if pmsg.last:
            self._finish_stream(rpc)

    def _finish_stream(self, rpc: RpcStream) -> None:
        """The last message unlinks the stream (nxt_request_rpc_data_unlink)."""
        engine = self.engines[rpc.engine_id]
        engine.active_streams.pop(rpc.stream_id, None)
        self.loop.record_trace(
            f"engine_{rpc.engine_id}",
            "stream_completed",
            stream=rpc.stream_id,
            messages=rpc.messages,
            body_bytes=rpc.body_bytes,
            coding=rpc.coding,
        )

    def _fail_stream(self, rpc: RpcStream, status: int) -> None:
        """nxt_http_request_error: a 503 if the header is unsent, else the
        connection closes with a truncated response (src/nxt_router.c:6676)."""
        engine = self.engines[rpc.engine_id]
        if not rpc.header_sent:
            resp = UnitResponse(status=status, content=b"")
            self.listener.send_http_response(rpc.conn, resp, stream=rpc.stream_id)
        else:
            self.listener.close_connection(rpc.conn)
        engine.active_streams.pop(rpc.stream_id, None)
        self.loop.record_trace(
            f"engine_{rpc.engine_id}", "stream_failed", stream=rpc.stream_id, status=status
        )
