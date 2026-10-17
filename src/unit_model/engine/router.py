"""Router Data Plane Subsystem (nxt_router.c, nxt_http_route.c, nxt_port_rpc.c).

Models:
- N router event engines with thread-isolated event dispatchers.
- Routing decision chains: 'pass' (to app), 'return' (immediate), 'share' (static).
- RPC lvlhsh stream registry: tracks active streams, reply ports, and in-flight responses.
- Dual-channel message dispatch: App Queue (<=31B) vs Socket fallback + Shared Memory chunks.
- Response handling: ACK tracking, streaming data fragments, and detached lifecycles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

from unit_model.c_types import (
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
    in_flight_shm_allocs: List[Tuple[int, int, int]] = field(default_factory=list) # (mmap_id, chunk_id, count)


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
            self.listener.send_http_response(conn, resp)
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
            self.listener.send_http_response(rpc.conn, resp)
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
            self.listener.send_http_response(rpc.conn, resp)
            del self.engines[rpc.engine_id].active_streams[rpc.stream_id]
            return

        rpc.in_flight_shm_allocs.append((alloc.mmap_id, alloc.start_chunk_id, alloc.chunk_count))

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
        """Handle response message received from an app worker."""
        engine = self.engines[engine_id]
        rpc = engine.active_streams.get(pmsg.stream)
        if not rpc:
            self.loop.record_trace(
                f"engine_{engine_id}",
                "response_for_unknown_stream",
                stream=pmsg.stream,
                msg_type=pmsg.type,
            )
            return

        if pmsg.type == PortMsgType.REQ_HEADERS_ACK:
            rpc.headers_acked = True
            self.loop.record_trace(
                f"engine_{engine_id}",
                "headers_ack_received",
                stream=pmsg.stream,
            )
            return

        if pmsg.type in (PortMsgType.DATA, PortMsgType.DATA | PortMsgFlag.LAST):
            # Parse response payload from shared memory or inline
            resp_content = b"OK"
            resp_status = 200
            if pmsg.mmap and len(payload) >= 12:
                mmsg = MmapMsg.from_bytes(payload[:12])
                shm_bytes = self.shm_pool.get_data(mmsg.mmap_id, mmsg.chunk_id, mmsg.size)
                if shm_bytes:
                    resp_content = shm_bytes
                # Free response chunk in shared memory
                needed_chunks = (mmsg.size + 16383) // 16384
                self.shm_pool.free(mmsg.mmap_id, mmsg.chunk_id, needed_chunks)

            resp = UnitResponse(status=resp_status, content=resp_content)
            self.listener.send_http_response(rpc.conn, resp)

            # Release request shared memory chunks
            for mmap_id, start_chunk, count in rpc.in_flight_shm_allocs:
                self.shm_pool.free(mmap_id, start_chunk, count)
            rpc.in_flight_shm_allocs.clear()

            if pmsg.last:
                del engine.active_streams[pmsg.stream]
                self.loop.record_trace(
                    f"engine_{engine_id}",
                    "stream_completed",
                    stream=pmsg.stream,
                )
