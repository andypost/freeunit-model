"""Listener Subsystem & Connection State Machine (nxt_router.c, nxt_conn_*.c, nxt_h1proto.c).

Faithfully models:
- Kernel socket accept distribution (EPOLLEXCLUSIVE) across N router engines.
- Connection state machine (nxt_conn_t):
    CONNECTED -> HANDSHAKING -> ACTIVE -> WRITING -> KEEP_ALIVE -> CLOSED.
- HTTP/1 request parsing and pipelining (master speaks HTTP/1.x only; HTTP/2 is
  in open FreeUnit pull requests, not in master).
- Response streaming as the router does it: the header once, then body parts
  as they arrive from the application, then the end of the response.
- Keep-alive connection pooling and recycling.
- Deferred release timer (0ms r->timer vs event loop cycle).
- Connection closing flag (c->closing) to protect against I/O on freed connections.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

from unit_model.c_types import UnitRequest, UnitResponse
from unit_model.engine.event_loop import EventPriority, SimEventLoop


class ConnState(Enum):
    CONNECTED = auto()
    HANDSHAKING = auto()
    ACTIVE = auto()
    WRITING = auto()
    IDLE_KEEP_ALIVE = auto()
    CLOSED = auto()


@dataclass
class Connection:
    conn_id: int
    engine_id: int
    remote_addr: str
    state: ConnState = ConnState.CONNECTED
    tls: bool = False
    closing: bool = False
    requests_served: int = 0
    active_stream: int = 0
    in_flight_response: Optional[UnitResponse] = None
    body_bytes_sent: int = 0
    deferred_release_scheduled: bool = False


class Listener:
    """Simulated FreeUnit Listener (*:8080) with EPOLLEXCLUSIVE accept distribution."""

    def __init__(
        self,
        loop: SimEventLoop,
        listen_addr: str = "*:8080",
        num_engines: int = 2,
    ):
        self.loop = loop
        self.listen_addr = listen_addr
        self.num_engines = num_engines
        self.next_conn_id: int = 1
        self.next_engine_idx: int = 0
        self.connections: Dict[int, Connection] = {}
        self.engine_connections: Dict[int, Set[int]] = {i: set() for i in range(num_engines)}
        self.on_request_ready: Optional[Callable[[int, Connection, UnitRequest], None]] = None
        # Fires once per completed response with the status, fields and the
        # body as the client saw them (conformance capture hook).
        self.on_response_complete: Optional[Callable[[Connection, UnitResponse], None]] = None

    def accept_client(self, remote_addr: str = "127.0.0.1", tls: bool = False) -> Connection:
        """Simulate kernel accept() with EPOLLEXCLUSIVE distribution."""
        # EPOLLEXCLUSIVE wakes one thread engine
        engine_id = self.next_engine_idx
        self.next_engine_idx = (self.next_engine_idx + 1) % self.num_engines

        conn_id = self.next_conn_id
        self.next_conn_id += 1

        conn = Connection(
            conn_id=conn_id,
            engine_id=engine_id,
            remote_addr=remote_addr,
            tls=tls,
        )
        self.connections[conn_id] = conn
        self.engine_connections[engine_id].add(conn_id)

        self.loop.record_trace(
            f"engine_{engine_id}",
            "conn_accepted",
            conn_id=conn_id,
            remote=remote_addr,
            tls=tls,
        )

        if tls:
            conn.state = ConnState.HANDSHAKING
            self.loop.schedule(
                500,  # 500ns simulated TLS handshake
                f"engine_{engine_id}",
                "tls_handshake_done",
                lambda: self._on_tls_ready(conn),
            )
        else:
            conn.state = ConnState.IDLE_KEEP_ALIVE

        return conn

    def _on_tls_ready(self, conn: Connection) -> None:
        if conn.closing:
            return
        conn.state = ConnState.IDLE_KEEP_ALIVE
        self.loop.record_trace(f"engine_{conn.engine_id}", "tls_ready", conn_id=conn.conn_id)

    def submit_http_request(self, conn: Connection, req: UnitRequest) -> None:
        """Receive HTTP/1 bytes on connection and parse into UnitRequest."""
        if conn.closing or conn.state == ConnState.CLOSED:
            self.loop.record_trace(
                f"engine_{conn.engine_id}",
                "request_dropped_conn_closed",
                conn_id=conn.conn_id,
            )
            return

        conn.state = ConnState.ACTIVE
        conn.requests_served += 1
        conn.active_stream += 1

        self.loop.record_trace(
            f"engine_{conn.engine_id}",
            "request_parsed",
            conn_id=conn.conn_id,
            stream=conn.active_stream,
            path=req.path,
            method=req.method,
        )

        if self.on_request_ready:
            self.on_request_ready(conn.engine_id, conn, req)

    def send_http_header(
        self,
        conn: Connection,
        resp: UnitResponse,
        stream: int = 0,
    ) -> None:
        """nxt_http_request_header_send: the status line and fields go out
        once, before any body byte (src/nxt_router.c:6618)."""
        if conn.closing:
            self.loop.record_trace(
                f"engine_{conn.engine_id}",
                "response_dropped_conn_closing",
                conn_id=conn.conn_id,
            )
            return

        conn.state = ConnState.WRITING
        conn.in_flight_response = UnitResponse(
            status=resp.status, headers=list(resp.headers), content=b"",
            content_length=resp.content_length,
        )
        conn.body_bytes_sent = 0
        self.loop.record_trace(
            f"engine_{conn.engine_id}",
            "response_header_sent",
            conn_id=conn.conn_id,
            stream=stream,
            status=resp.status,
        )

    def send_http_body(
        self,
        conn: Connection,
        data: bytes,
        last: bool,
        stream: int = 0,
        keep_alive: bool = True,
        on_done: Optional[Callable[[], None]] = None,
    ) -> None:
        """nxt_http_request_send_body: one body part written to the client.

        on_done runs when the write completes: this is the buffer completion
        handler that gives a shared memory chunk back
        (nxt_port_mmap_buf_completion).  It runs on the error path too, so
        the chunk is never held by a connection that is gone.
        """
        if conn.closing:
            self.loop.record_trace(
                f"engine_{conn.engine_id}",
                "response_dropped_conn_closing",
                conn_id=conn.conn_id,
            )
            if on_done is not None:
                on_done()
            return

        if data:
            self.loop.record_trace(
                f"engine_{conn.engine_id}",
                "response_body_part",
                conn_id=conn.conn_id,
                stream=stream,
                bytes=len(data),
                last=last,
            )
            if conn.in_flight_response is not None:
                conn.in_flight_response.content += data

        def on_write_done() -> None:
            if on_done is not None:
                on_done()
            if conn.closing:
                return
            conn.body_bytes_sent += len(data)
            if not last:
                return
            resp = conn.in_flight_response
            self.loop.record_trace(
                f"engine_{conn.engine_id}",
                "response_sent",
                conn_id=conn.conn_id,
                stream=stream,
                status=resp.status if resp else 0,
                body_bytes=conn.body_bytes_sent,
            )
            conn.in_flight_response = None
            if self.on_response_complete is not None and resp is not None:
                self.on_response_complete(conn, resp)

            if keep_alive:
                conn.state = ConnState.IDLE_KEEP_ALIVE
                # Deferred release timer simulation (0ms timer in FreeUnit)
                self._schedule_deferred_release(conn)
            else:
                self.close_connection(conn)

        # 200 ns simulated socket write
        self.loop.schedule(200, f"engine_{conn.engine_id}", "response_flushed", on_write_done)

    def send_http_response(
        self,
        conn: Connection,
        resp: UnitResponse,
        keep_alive: bool = True,
        stream: int = 0,
    ) -> None:
        """A whole response in one go: header, then the body as the last part.

        Used for responses the router builds itself ("return" routes, 503s).
        An application response streams through send_http_header() and
        send_http_body() message by message.
        """
        if conn.closing:
            self.loop.record_trace(
                f"engine_{conn.engine_id}",
                "response_dropped_conn_closing",
                conn_id=conn.conn_id,
            )
            return
        self.send_http_header(conn, resp, stream=stream)
        self.send_http_body(conn, resp.content, last=True, stream=stream, keep_alive=keep_alive)

    def _schedule_deferred_release(self, conn: Connection) -> None:
        """Models FreeUnit 0ms deferred release timer (r->timer)."""
        conn.deferred_release_scheduled = True

        def deferred_work():
            conn.deferred_release_scheduled = False
            self.loop.record_trace(
                f"engine_{conn.engine_id}",
                "deferred_release_timer_fired",
                conn_id=conn.conn_id,
            )

        self.loop.schedule(
            0,
            f"engine_{conn.engine_id}",
            "deferred_release_timer",
            deferred_work,
            priority=EventPriority.DEFERRED,
        )

    def close_connection(self, conn: Connection) -> None:
        """Close connection and mark closing=True."""
        conn.closing = True
        conn.state = ConnState.CLOSED
        if conn.conn_id in self.connections:
            del self.connections[conn.conn_id]
        if conn.conn_id in self.engine_connections[conn.engine_id]:
            self.engine_connections[conn.engine_id].remove(conn.conn_id)

        self.loop.record_trace(
            f"engine_{conn.engine_id}",
            "conn_closed",
            conn_id=conn.conn_id,
        )
