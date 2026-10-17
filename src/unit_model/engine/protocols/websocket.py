"""RFC 6455 WebSocket Protocol Implementation.

Faithfully models FreeUnit's WebSocket implementation (nxt_websocket.h,
nxt_websocket_header.h, and nxt_h1proto_websocket.c), including:
- Frame framing, masking, and unmasking
- Fragmentation and continuation frame reassembly
- Exact nxt_h1p_ws_utf8_t continuation buffer modeling across frame boundaries
- RFC 6455 Section 5.5.3 pong coalescing (at most two PONGs kept for connection)
- Control frame interleaving inside fragmented message streams
"""

from __future__ import annotations

import struct
from enum import Enum, IntEnum
from typing import Callable, List, Optional, Tuple


class WebSocketOpcode(IntEnum):
    CONT = 0x00
    TEXT = 0x01
    BINARY = 0x02
    CLOSE = 0x08
    PING = 0x09
    PONG = 0x0A

    @property
    def is_control(self) -> bool:
        return bool(self & 0x08)


class WebSocketCloseCode(IntEnum):
    NORMAL = 1000
    GOING_AWAY = 1001
    PROTOCOL_ERROR = 1002
    UNPROCESSABLE_INPUT = 1003
    NOT_PROVIDED = 1005
    ABNORMAL = 1006
    INVALID_DATA = 1007
    POLICY_VIOLATION = 1008
    MESSAGE_TOO_BIG = 1009
    INTERNAL_SERVER_ERROR = 1011


class WebSocketState(Enum):
    CONNECTING = "connecting"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


class WsUtf8Buffer:
    """Exact behavioral model of nxt_h1p_ws_utf8_t.

    C reference:
    typedef struct {
        uint8_t   len;
        u_char    tail[3];
    } nxt_h1p_ws_utf8_t;

    Preserves up to 3 trailing bytes of an incomplete multibyte UTF-8 sequence
    split across continuation frames.
    """

    def __init__(self) -> None:
        self.len: int = 0
        self.tail: bytearray = bytearray(3)

    def reset(self) -> None:
        self.len = 0

    def feed(self, chunk: bytes, is_final: bool) -> Tuple[str, bool]:
        """Feed bytes into the UTF-8 decoder.

        Returns (decoded_text, is_valid).
        If is_valid is False, an RFC 6455 1007 INVALID_DATA error must be triggered.
        """
        combined = bytearray()
        if self.len > 0:
            combined.extend(self.tail[: self.len])
            self.reset()
        combined.extend(chunk)

        if not combined:
            return "", True

        # Find the valid UTF-8 prefix
        # UTF-8 sequences can be 1, 2, 3, or 4 bytes long.
        raw = bytes(combined)
        valid_end = len(raw)

        # Try incremental decoding from the end if decoding fails
        for cut in range(min(4, len(raw))):
            test_slice = raw[: len(raw) - cut]
            try:
                decoded = test_slice.decode("utf-8")
                # Successful decode of test_slice
                trailing = raw[len(raw) - cut :]
                if len(trailing) > 3:
                    # Incomplete tail cannot exceed 3 bytes
                    return "", False
                if is_final and len(trailing) > 0:
                    # End of message but trailing bytes remain incomplete!
                    return "", False

                # Store incomplete trailing bytes in tail
                self.len = len(trailing)
                for i, b in enumerate(trailing):
                    self.tail[i] = b
                return decoded, True
            except UnicodeDecodeError:
                continue

        # If we couldn't decode even with cut, the stream contains invalid UTF-8
        return "", False


class WebSocketFrame:
    """RFC 6455 WebSocket Frame."""

    def __init__(
        self,
        opcode: WebSocketOpcode,
        payload: bytes = b"",
        fin: bool = True,
        rsv1: bool = False,
        rsv2: bool = False,
        rsv3: bool = False,
        masked: bool = True,
        mask_key: Optional[bytes] = None,
    ) -> None:
        self.opcode = opcode
        self.payload = payload
        self.fin = fin
        self.rsv1 = rsv1
        self.rsv2 = rsv2
        self.rsv3 = rsv3
        self.masked = masked
        self.mask_key = mask_key or (b"\x12\x34\x56\x78" if masked else None)

    @classmethod
    def mask_payload(cls, data: bytes, key: bytes) -> bytes:
        out = bytearray(len(data))
        for i, b in enumerate(data):
            out[i] = b ^ key[i % 4]
        return bytes(out)

    def encode(self) -> bytes:
        b0 = (0x80 if self.fin else 0) | (int(self.opcode) & 0x0F)
        if self.rsv1:
            b0 |= 0x40
        if self.rsv2:
            b0 |= 0x20
        if self.rsv3:
            b0 |= 0x10

        length = len(self.payload)
        mask_bit = 0x80 if self.masked else 0x00

        header = bytearray([b0])
        if length <= 125:
            header.append(mask_bit | length)
        elif length <= 0xFFFF:
            header.append(mask_bit | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(mask_bit | 127)
            header.extend(struct.pack("!Q", length))

        if self.masked and self.mask_key:
            header.extend(self.mask_key)
            header.extend(self.mask_payload(self.payload, self.mask_key))
        else:
            header.extend(self.payload)

        return bytes(header)

    @classmethod
    def decode(cls, data: bytes) -> Tuple[Optional[WebSocketFrame], int]:
        """Decode a single WebSocket frame from byte stream.

        Returns (frame, bytes_consumed). If insufficient bytes, returns (None, 0).
        """
        if len(data) < 2:
            return None, 0

        b0 = data[0]
        b1 = data[1]

        fin = bool(b0 & 0x80)
        rsv1 = bool(b0 & 0x40)
        rsv2 = bool(b0 & 0x20)
        rsv3 = bool(b0 & 0x10)
        try:
            opcode = WebSocketOpcode(b0 & 0x0F)
        except ValueError:
            raise ValueError(f"Invalid WebSocket opcode: {b0 & 0x0F}")

        masked = bool(b1 & 0x80)
        payload_len = b1 & 0x7F
        offset = 2

        if payload_len == 126:
            if len(data) < offset + 2:
                return None, 0
            payload_len = struct.unpack("!H", data[offset : offset + 2])[0]
            offset += 2
        elif payload_len == 127:
            if len(data) < offset + 8:
                return None, 0
            payload_len = struct.unpack("!Q", data[offset : offset + 8])[0]
            offset += 8

        mask_key = None
        if masked:
            if len(data) < offset + 4:
                return None, 0
            mask_key = data[offset : offset + 4]
            offset += 4

        if len(data) < offset + payload_len:
            return None, 0

        raw_payload = data[offset : offset + payload_len]
        payload = cls.mask_payload(raw_payload, mask_key) if masked and mask_key else raw_payload

        frame = cls(
            opcode=opcode,
            payload=payload,
            fin=fin,
            rsv1=rsv1,
            rsv2=rsv2,
            rsv3=rsv3,
            masked=masked,
            mask_key=mask_key,
        )
        return frame, offset + payload_len


class WebSocketConnection:
    """Full RFC 6455 WebSocket Connection & FSM."""

    def __init__(self, max_message_size: int = 16 * 1024 * 1024) -> None:
        self.state: WebSocketState = WebSocketState.OPEN
        self.max_message_size = max_message_size

        # In-flight fragmentation state
        self.fragment_in_progress: bool = False
        self.fragment_opcode: Optional[WebSocketOpcode] = None
        self.fragment_buffer: bytearray = bytearray()
        self.utf8_state = WsUtf8Buffer()

        # Pong coalescing matching nxt_h1proto_websocket.c:953-963
        self.pong_queued: bool = False
        self.pong_next_payload: Optional[bytes] = None

        # Outgoing frames buffer
        self.outgoing_frames: List[WebSocketFrame] = []

        # Completed messages
        self.received_messages: List[Tuple[WebSocketOpcode, bytes]] = []
        self.close_code: Optional[int] = None
        self.close_reason: str = ""

    def receive_bytes(self, data: bytes) -> int:
        """Process incoming raw bytes, extracting frames. Returns total bytes consumed."""
        consumed = 0
        buf = data
        while buf and self.state != WebSocketState.CLOSED:
            frame, n = WebSocketFrame.decode(buf)
            if frame is None or n == 0:
                break
            consumed += n
            buf = buf[n:]
            self._handle_frame(frame)
        return consumed

    def _handle_frame(self, frame: WebSocketFrame) -> None:
        if self.state in (WebSocketState.CLOSING, WebSocketState.CLOSED):
            if frame.opcode == WebSocketOpcode.CLOSE:
                self.state = WebSocketState.CLOSED
            return

        # RSV check (RSV1-3 must be 0 unless extension negotiated)
        if frame.rsv1 or frame.rsv2 or frame.rsv3:
            self._close_with_error(WebSocketCloseCode.PROTOCOL_ERROR, "RSV bits must be 0")
            return

        if frame.opcode.is_control:
            self._handle_control_frame(frame)
        else:
            self._handle_data_frame(frame)

    def _handle_control_frame(self, frame: WebSocketFrame) -> None:
        # Control frames must not be fragmented and payload <= 125 bytes
        if not frame.fin:
            self._close_with_error(WebSocketCloseCode.PROTOCOL_ERROR, "Control frames cannot be fragmented")
            return
        if len(frame.payload) > 125:
            self._close_with_error(WebSocketCloseCode.PROTOCOL_ERROR, "Control frame payload > 125 bytes")
            return

        if frame.opcode == WebSocketOpcode.PING:
            self._on_ping(frame.payload)
        elif frame.opcode == WebSocketOpcode.PONG:
            # Unsolicited or solicited pong
            pass
        elif frame.opcode == WebSocketOpcode.CLOSE:
            code = WebSocketCloseCode.NORMAL
            reason = ""
            if len(frame.payload) >= 2:
                code = struct.unpack("!H", frame.payload[:2])[0]
                reason = frame.payload[2:].decode("utf-8", errors="replace")
            self.close_code = code
            self.close_reason = reason
            self._respond_close(code, reason)

    def _on_ping(self, payload: bytes) -> None:
        """RFC 6455 Section 5.5.3 pong coalescing."""
        if not self.pong_queued:
            self.pong_queued = True
            pong = WebSocketFrame(opcode=WebSocketOpcode.PONG, payload=payload, fin=True, masked=False)
            self.outgoing_frames.append(pong)
        else:
            # A pong is already queued: overwrite with most recent ping payload
            self.pong_next_payload = payload

    def on_pong_sent(self) -> None:
        """Called when a queued PONG has finished sending on the wire."""
        self.pong_queued = False
        if self.pong_next_payload is not None:
            payload = self.pong_next_payload
            self.pong_next_payload = None
            self.pong_queued = True
            pong = WebSocketFrame(opcode=WebSocketOpcode.PONG, payload=payload, fin=True, masked=False)
            self.outgoing_frames.append(pong)

    def _handle_data_frame(self, frame: WebSocketFrame) -> None:
        if frame.opcode == WebSocketOpcode.CONT:
            if not self.fragment_in_progress:
                self._close_with_error(WebSocketCloseCode.PROTOCOL_ERROR, "Unexpected continuation frame")
                return
        else:
            if self.fragment_in_progress:
                self._close_with_error(WebSocketCloseCode.PROTOCOL_ERROR, "Expected continuation frame")
                return
            self.fragment_in_progress = True
            self.fragment_opcode = frame.opcode
            self.fragment_buffer.clear()
            self.utf8_state.reset()

        # Check total buffer bounds
        if len(self.fragment_buffer) + len(frame.payload) > self.max_message_size:
            self._close_with_error(WebSocketCloseCode.MESSAGE_TOO_BIG, "Message too big")
            return

        # If TEXT frame, validate UTF-8 incrementally
        if self.fragment_opcode == WebSocketOpcode.TEXT:
            _, valid = self.utf8_state.feed(frame.payload, is_final=frame.fin)
            if not valid:
                self._close_with_error(WebSocketCloseCode.INVALID_DATA, "Invalid UTF-8 payload")
                return

        self.fragment_buffer.extend(frame.payload)

        if frame.fin:
            # Message complete
            msg = bytes(self.fragment_buffer)
            opcode = self.fragment_opcode
            self.fragment_in_progress = False
            self.fragment_opcode = None
            self.fragment_buffer.clear()
            self.utf8_state.reset()
            self.received_messages.append((opcode, msg))

    def _close_with_error(self, code: WebSocketCloseCode, reason: str) -> None:
        self.close_code = int(code)
        self.close_reason = reason
        self._respond_close(int(code), reason)

    def _respond_close(self, code: int, reason: str) -> None:
        if self.state == WebSocketState.CLOSED:
            return
        payload = struct.pack("!H", code) + reason.encode("utf-8")[:123]
        frame = WebSocketFrame(opcode=WebSocketOpcode.CLOSE, payload=payload, fin=True, masked=False)
        self.outgoing_frames.append(frame)
        self.state = WebSocketState.CLOSED

    def send_text(self, text: str) -> None:
        payload = text.encode("utf-8")
        frame = WebSocketFrame(opcode=WebSocketOpcode.TEXT, payload=payload, fin=True, masked=False)
        self.outgoing_frames.append(frame)

    def send_binary(self, data: bytes) -> None:
        frame = WebSocketFrame(opcode=WebSocketOpcode.BINARY, payload=data, fin=True, masked=False)
        self.outgoing_frames.append(frame)

    def send_ping(self, payload: bytes = b"") -> None:
        frame = WebSocketFrame(opcode=WebSocketOpcode.PING, payload=payload[:125], fin=True, masked=False)
        self.outgoing_frames.append(frame)
