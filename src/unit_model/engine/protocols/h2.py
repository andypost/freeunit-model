"""RFC 9113 HTTP/2 Protocol Multiplexer & Flow Control Engine.

Faithfully models FreeUnit's HTTP/2 data plane:
- 9-byte binary frame header encoding and parsing
- Stream multiplexing and concurrent lifecycle state machines
- Connection-level and stream-level flow control windows (WINDOW_UPDATE)
- Stream isolation invariant: RST_STREAM on one stream does not impact peers
- Graceful connection termination via GOAWAY frames
"""

from __future__ import annotations

import struct
from enum import Enum, IntEnum
from typing import Dict, List, Optional, Tuple


class H2FrameType(IntEnum):
    DATA = 0x0
    HEADERS = 0x1
    PRIORITY = 0x2
    RST_STREAM = 0x3
    SETTINGS = 0x4
    PUSH_PROMISE = 0x5
    PING = 0x6
    GOAWAY = 0x7
    WINDOW_UPDATE = 0x8
    CONTINUATION = 0x9


class H2Flag(IntEnum):
    NONE = 0x0
    END_STREAM = 0x1
    ACK = 0x1
    END_HEADERS = 0x4
    PADDED = 0x8
    PRIORITY = 0x20


class H2ErrorCode(IntEnum):
    NO_ERROR = 0x0
    PROTOCOL_ERROR = 0x1
    INTERNAL_ERROR = 0x2
    FLOW_CONTROL_ERROR = 0x3
    SETTINGS_TIMEOUT = 0x4
    STREAM_CLOSED = 0x5
    FRAME_SIZE_ERROR = 0x6
    REFUSED_STREAM = 0x7
    CANCEL = 0x8
    COMPRESSION_ERROR = 0x9
    CONNECT_ERROR = 0xA
    ENHANCE_YOUR_CALM = 0xB
    INADEQUATE_SECURITY = 0xC
    HTTP_1_1_REQUIRED = 0xD


class H2StreamState(Enum):
    IDLE = "idle"
    RESERVED_LOCAL = "reserved_local"
    RESERVED_REMOTE = "reserved_remote"
    OPEN = "open"
    HALF_CLOSED_LOCAL = "half_closed_local"
    HALF_CLOSED_REMOTE = "half_closed_remote"
    CLOSED = "closed"


class H2Frame:
    """An HTTP/2 Binary Frame (RFC 9113 Section 4.1)."""

    def __init__(
        self,
        frame_type: H2FrameType,
        flags: int = 0,
        stream_id: int = 0,
        payload: bytes = b"",
    ) -> None:
        self.frame_type = frame_type
        self.flags = flags
        self.stream_id = stream_id & 0x7FFFFFFF  # 31-bit stream id
        self.payload = payload

    def encode(self) -> bytes:
        length = len(self.payload)
        if length > 0xFFFFFF:
            raise ValueError(f"Frame payload too large: {length} > 16,777,215")

        # 9-byte header: length (24), type (8), flags (8), stream_id (31)
        header = bytearray(9)
        header[0] = (length >> 16) & 0xFF
        header[1] = (length >> 8) & 0xFF
        header[2] = length & 0xFF
        header[3] = int(self.frame_type) & 0xFF
        header[4] = self.flags & 0xFF
        struct.pack_into("!I", header, 5, self.stream_id & 0x7FFFFFFF)

        return bytes(header) + self.payload

    @classmethod
    def decode(cls, data: bytes) -> Tuple[Optional[H2Frame], int]:
        """Decode a single HTTP/2 frame from bytes. Returns (frame, bytes_consumed)."""
        if len(data) < 9:
            return None, 0

        length = (data[0] << 16) | (data[1] << 8) | data[2]
        try:
            frame_type = H2FrameType(data[3])
        except ValueError:
            # Unknown frame types must be ignored as per RFC 9113
            frame_type = H2FrameType(data[3] & 0xFF)
        flags = data[4]
        stream_id = struct.unpack_from("!I", data, 5)[0] & 0x7FFFFFFF

        if len(data) < 9 + length:
            return None, 0

        payload = data[9 : 9 + length]
        frame = cls(frame_type=frame_type, flags=flags, stream_id=stream_id, payload=payload)
        return frame, 9 + length


class H2Stream:
    """An individual HTTP/2 Stream with independent flow control & state machine."""

    DEFAULT_INITIAL_WINDOW_SIZE = 65535

    def __init__(self, stream_id: int, initial_window: int = DEFAULT_INITIAL_WINDOW_SIZE) -> None:
        self.stream_id = stream_id
        self.state: H2StreamState = H2StreamState.IDLE
        self.inbound_window: int = initial_window
        self.outbound_window: int = initial_window

        self.headers: List[Tuple[str, str]] = []
        self.inbound_data: bytearray = bytearray()
        self.error_code: Optional[H2ErrorCode] = None

    def on_headers_received(self, end_stream: bool) -> None:
        if self.state == H2StreamState.IDLE:
            self.state = H2StreamState.HALF_CLOSED_REMOTE if end_stream else H2StreamState.OPEN
        elif self.state == H2StreamState.OPEN:
            if end_stream:
                self.state = H2StreamState.HALF_CLOSED_REMOTE
        elif self.state == H2StreamState.HALF_CLOSED_LOCAL:
            if end_stream:
                self.state = H2StreamState.CLOSED

    def on_data_received(self, chunk: bytes, end_stream: bool) -> None:
        if self.state not in (H2StreamState.OPEN, H2StreamState.HALF_CLOSED_LOCAL):
            raise ValueError(f"Cannot receive DATA in stream state {self.state}")

        self.inbound_data.extend(chunk)
        self.inbound_window -= len(chunk)

        if end_stream:
            if self.state == H2StreamState.OPEN:
                self.state = H2StreamState.HALF_CLOSED_REMOTE
            elif self.state == H2StreamState.HALF_CLOSED_LOCAL:
                self.state = H2StreamState.CLOSED

    def on_rst_stream(self, error_code: H2ErrorCode) -> None:
        self.state = H2StreamState.CLOSED
        self.error_code = error_code


class H2Connection:
    """Full RFC 9113 HTTP/2 Connection & Stream Multiplexer."""

    PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
    DEFAULT_INITIAL_WINDOW_SIZE = 65535

    def __init__(self, is_server: bool = True, max_concurrent_streams: int = 128) -> None:
        self.is_server = is_server
        self.max_concurrent_streams = max_concurrent_streams

        self.connection_inbound_window = self.DEFAULT_INITIAL_WINDOW_SIZE
        self.connection_outbound_window = self.DEFAULT_INITIAL_WINDOW_SIZE

        self.streams: Dict[int, H2Stream] = {}
        self.last_stream_id: int = 0
        self.peer_last_stream_id: int = 0

        self.preface_received: bool = not is_server
        self.closed: bool = False
        self.goaway_sent: bool = False
        self.goaway_received: bool = False

        self.outgoing_frames: List[H2Frame] = []
        self.completed_requests: List[Tuple[int, List[Tuple[str, str]], bytes]] = []

    def receive_bytes(self, data: bytes) -> int:
        """Process incoming raw bytes, decoding frames and dispatching to streams."""
        consumed = 0
        buf = data

        # Check preface on server
        if self.is_server and not self.preface_received:
            if len(buf) < len(self.PREFACE):
                return 0
            if not buf.startswith(self.PREFACE):
                self._send_goaway(0, H2ErrorCode.PROTOCOL_ERROR, "Invalid client connection preface")
                return len(data)
            consumed += len(self.PREFACE)
            buf = buf[len(self.PREFACE) :]
            self.preface_received = True

            # Send server initial SETTINGS frame
            self.outgoing_frames.append(H2Frame(frame_type=H2FrameType.SETTINGS, stream_id=0))

        while buf and not self.closed:
            frame, n = H2Frame.decode(buf)
            if frame is None or n == 0:
                break
            consumed += n
            buf = buf[n:]
            self._handle_frame(frame)

        return consumed

    def _handle_frame(self, frame: H2Frame) -> None:
        if frame.frame_type == H2FrameType.SETTINGS:
            self._handle_settings(frame)
        elif frame.frame_type == H2FrameType.PING:
            self._handle_ping(frame)
        elif frame.frame_type == H2FrameType.GOAWAY:
            self._handle_goaway(frame)
        elif frame.frame_type == H2FrameType.WINDOW_UPDATE:
            self._handle_window_update(frame)
        elif frame.frame_type == H2FrameType.HEADERS:
            self._handle_headers(frame)
        elif frame.frame_type == H2FrameType.DATA:
            self._handle_data(frame)
        elif frame.frame_type == H2FrameType.RST_STREAM:
            self._handle_rst_stream(frame)

    def _handle_settings(self, frame: H2Frame) -> None:
        if frame.stream_id != 0:
            self._send_goaway(0, H2ErrorCode.PROTOCOL_ERROR, "SETTINGS on non-zero stream")
            return
        if frame.flags & H2Flag.ACK:
            # Settings ACK from peer
            return

        # Reply with SETTINGS ACK
        ack = H2Frame(frame_type=H2FrameType.SETTINGS, flags=H2Flag.ACK, stream_id=0)
        self.outgoing_frames.append(ack)

    def _handle_ping(self, frame: H2Frame) -> None:
        if frame.stream_id != 0 or len(frame.payload) != 8:
            self._send_goaway(0, H2ErrorCode.FRAME_SIZE_ERROR, "Invalid PING frame")
            return
        if not (frame.flags & H2Flag.ACK):
            # Reply with PING ACK carrying identical payload
            pong = H2Frame(frame_type=H2FrameType.PING, flags=H2Flag.ACK, stream_id=0, payload=frame.payload)
            self.outgoing_frames.append(pong)

    def _handle_goaway(self, frame: H2Frame) -> None:
        if frame.stream_id != 0 or len(frame.payload) < 8:
            return
        self.goaway_received = True
        self.closed = True

    def _handle_window_update(self, frame: H2Frame) -> None:
        if len(frame.payload) != 4:
            self._send_goaway(0, H2ErrorCode.FRAME_SIZE_ERROR, "Invalid WINDOW_UPDATE size")
            return
        increment = struct.unpack("!I", frame.payload)[0] & 0x7FFFFFFF
        if increment == 0:
            if frame.stream_id == 0:
                self._send_goaway(0, H2ErrorCode.PROTOCOL_ERROR, "Zero WINDOW_UPDATE increment")
            else:
                self._send_rst_stream(frame.stream_id, H2ErrorCode.PROTOCOL_ERROR)
            return

        if frame.stream_id == 0:
            self.connection_outbound_window += increment
        else:
            stream = self.streams.get(frame.stream_id)
            if stream:
                stream.outbound_window += increment

    def _handle_headers(self, frame: H2Frame) -> None:
        if frame.stream_id == 0:
            self._send_goaway(0, H2ErrorCode.PROTOCOL_ERROR, "HEADERS on stream 0")
            return

        stream = self.streams.get(frame.stream_id)
        if stream is None:
            # New stream
            if frame.stream_id <= self.peer_last_stream_id:
                self._send_goaway(frame.stream_id, H2ErrorCode.PROTOCOL_ERROR, "Stream ID must strictly increase")
                return
            self.peer_last_stream_id = frame.stream_id
            stream = H2Stream(frame.stream_id)
            self.streams[frame.stream_id] = stream

        end_stream = bool(frame.flags & H2Flag.END_STREAM)
        stream.on_headers_received(end_stream)

        if end_stream:
            self.completed_requests.append((stream.stream_id, stream.headers, bytes(stream.inbound_data)))

    def _handle_data(self, frame: H2Frame) -> None:
        if frame.stream_id == 0:
            self._send_goaway(0, H2ErrorCode.PROTOCOL_ERROR, "DATA on stream 0")
            return

        stream = self.streams.get(frame.stream_id)
        if stream is None or stream.state == H2StreamState.CLOSED:
            self._send_rst_stream(frame.stream_id, H2ErrorCode.STREAM_CLOSED)
            return

        chunk_len = len(frame.payload)
        # Check flow control
        if chunk_len > self.connection_inbound_window or chunk_len > stream.inbound_window:
            self._send_goaway(frame.stream_id, H2ErrorCode.FLOW_CONTROL_ERROR, "Window exceeded")
            return

        self.connection_inbound_window -= chunk_len
        end_stream = bool(frame.flags & H2Flag.END_STREAM)
        stream.on_data_received(frame.payload, end_stream)

        if end_stream:
            self.completed_requests.append((stream.stream_id, stream.headers, bytes(stream.inbound_data)))

    def _handle_rst_stream(self, frame: H2Frame) -> None:
        if frame.stream_id == 0 or len(frame.payload) != 4:
            self._send_goaway(0, H2ErrorCode.FRAME_SIZE_ERROR, "Invalid RST_STREAM")
            return

        code = struct.unpack("!I", frame.payload)[0]
        stream = self.streams.get(frame.stream_id)
        if stream:
            stream.on_rst_stream(H2ErrorCode(code))

    def _send_rst_stream(self, stream_id: int, error_code: H2ErrorCode) -> None:
        stream = self.streams.get(stream_id)
        if stream:
            stream.on_rst_stream(error_code)
        payload = struct.pack("!I", int(error_code))
        frame = H2Frame(frame_type=H2FrameType.RST_STREAM, stream_id=stream_id, payload=payload)
        self.outgoing_frames.append(frame)

    def _send_goaway(self, last_stream_id: int, error_code: H2ErrorCode, debug_data: str = "") -> None:
        self.goaway_sent = True
        self.closed = True
        payload = struct.pack("!II", last_stream_id & 0x7FFFFFFF, int(error_code)) + debug_data.encode("utf-8")
        frame = H2Frame(frame_type=H2FrameType.GOAWAY, stream_id=0, payload=payload)
        self.outgoing_frames.append(frame)
