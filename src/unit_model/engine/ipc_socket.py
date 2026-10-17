"""Unix Domain Socket Datagram IPC Channel Simulation (nxt_port_socket.c).

Models:
- Datagram socket pairs (AF_UNIX, SOCK_DGRAM).
- Transmission of nxt_port_msg_t and trailing nxt_port_mmap_msg_t records.
- SCM_RIGHTS file descriptor passing (fd[2]).
- Non-blocking send/receive, OS buffer sizing, and backpressure (EAGAIN).
- Doorbell signaling: NXT_PORT_MSG_READ_QUEUE and NXT_PORT_MSG_READ_SOCKET.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple

from unit_model.c_types import (
    PortMsg,
    PortMsgFlag,
    PortMsgType,
)
from unit_model.engine.event_loop import SimEventLoop


@dataclass
class SocketDatagram:
    port_msg: PortMsg
    payload: bytes = b""
    fds: List[int] = field(default_factory=list)
    wire_size: int = 0


class UnixSocketPair:
    """Simulated bidirectional Unix domain datagram socket pair."""

    def __init__(
        self,
        loop: SimEventLoop,
        name: str = "socket_pair",
        buffer_capacity_bytes: int = 256 * 1024,
    ):
        self.loop = loop
        self.name = name
        self.buffer_capacity_bytes = buffer_capacity_bytes
        self.a_to_b: List[SocketDatagram] = []
        self.b_to_a: List[SocketDatagram] = []
        self.a_to_b_bytes: int = 0
        self.b_to_a_bytes: int = 0
        self.closed: bool = False
        self.on_a_readable: Optional[Callable[[], None]] = None
        self.on_b_readable: Optional[Callable[[], None]] = None

    def send_a_to_b(
        self,
        port_msg: PortMsg,
        payload: bytes = b"",
        fds: Optional[List[int]] = None,
    ) -> bool:
        """Send datagram from endpoint A (e.g. Router) to endpoint B (e.g. App Worker)."""
        return self._send(
            source="router",
            target="app_worker",
            dgram_queue=self.a_to_b,
            byte_counter="_a_to_b_bytes",
            readable_cb=self.on_b_readable,
            port_msg=port_msg,
            payload=payload,
            fds=fds,
        )

    def send_b_to_a(
        self,
        port_msg: PortMsg,
        payload: bytes = b"",
        fds: Optional[List[int]] = None,
    ) -> bool:
        """Send datagram from endpoint B (e.g. App Worker) to endpoint A (e.g. Router)."""
        return self._send(
            source="app_worker",
            target="router",
            dgram_queue=self.b_to_a,
            byte_counter="_b_to_a_bytes",
            readable_cb=self.on_a_readable,
            port_msg=port_msg,
            payload=payload,
            fds=fds,
        )

    def _send(
        self,
        source: str,
        target: str,
        dgram_queue: List[SocketDatagram],
        byte_counter: str,
        readable_cb: Optional[Callable[[], None]],
        port_msg: PortMsg,
        payload: bytes,
        fds: Optional[List[int]],
    ) -> bool:
        if self.closed:
            self.loop.record_trace(self.name, "send_on_closed_socket", source=source)
            return False

        wire_size = 20 + len(payload)  # sizeof(nxt_port_msg_t) + payload
        curr_bytes = getattr(self, byte_counter.lstrip("_"))
        if curr_bytes + wire_size > self.buffer_capacity_bytes:
            self.loop.record_trace(
                self.name,
                "socket_buffer_overflow_eagain",
                source=source,
                curr_bytes=curr_bytes,
                wire_size=wire_size,
            )
            return False

        dgram = SocketDatagram(
            port_msg=port_msg,
            payload=payload,
            fds=list(fds or []),
            wire_size=wire_size,
        )
        dgram_queue.append(dgram)
        setattr(self, byte_counter.lstrip("_"), curr_bytes + wire_size)

        self.loop.record_trace(
            self.name,
            "datagram_sent",
            source=source,
            target=target,
            msg_type=port_msg.type,
            stream=port_msg.stream,
            wire_size=wire_size,
        )

        if readable_cb is not None:
            # Deliver via event loop in next cycle
            self.loop.schedule(
                100,  # 100ns network/kernel transit latency
                target,
                "socket_readable",
                readable_cb,
            )

        return True

    def recv_at_a(self) -> Optional[SocketDatagram]:
        """Read datagram at endpoint A."""
        if not self.b_to_a:
            return None
        dgram = self.b_to_a.pop(0)
        self.b_to_a_bytes -= dgram.wire_size
        self.loop.record_trace(
            self.name,
            "datagram_consumed_at_a",
            msg_type=dgram.port_msg.type,
            stream=dgram.port_msg.stream,
        )
        return dgram

    def recv_at_b(self) -> Optional[SocketDatagram]:
        """Read datagram at endpoint B."""
        if not self.a_to_b:
            return None
        dgram = self.a_to_b.pop(0)
        self.a_to_b_bytes -= dgram.wire_size
        self.loop.record_trace(
            self.name,
            "datagram_consumed_at_b",
            msg_type=dgram.port_msg.type,
            stream=dgram.port_msg.stream,
        )
        return dgram

    def close(self) -> None:
        self.closed = True
        self.loop.record_trace(self.name, "socket_closed")
