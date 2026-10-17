"""Tests for RFC 6455 WebSocket FSM & Framing in FreeUnit Model."""

import struct
import pytest
from unit_model.engine.protocols.websocket import (
    WebSocketCloseCode,
    WebSocketConnection,
    WebSocketFrame,
    WebSocketOpcode,
    WebSocketState,
    WsUtf8Buffer,
)


def test_frame_encoding_and_decoding():
    payload = b"Hello, FreeUnit WebSocket!"
    frame = WebSocketFrame(
        opcode=WebSocketOpcode.TEXT,
        payload=payload,
        fin=True,
        masked=True,
        mask_key=b"\x11\x22\x33\x44",
    )
    raw = frame.encode()

    decoded_frame, consumed = WebSocketFrame.decode(raw)
    assert decoded_frame is not None
    assert consumed == len(raw)
    assert decoded_frame.opcode == WebSocketOpcode.TEXT
    assert decoded_frame.fin is True
    assert decoded_frame.payload == payload


def test_fragmentation_reassembly():
    conn = WebSocketConnection()

    # Frame 1: Text, fin=False
    f1 = WebSocketFrame(opcode=WebSocketOpcode.TEXT, payload=b"Part 1 - ", fin=False)
    # Frame 2: Cont, fin=False
    f2 = WebSocketFrame(opcode=WebSocketOpcode.CONT, payload=b"Part 2 - ", fin=False)
    # Frame 3: Cont, fin=True
    f3 = WebSocketFrame(opcode=WebSocketOpcode.CONT, payload=b"Part 3 Complete!", fin=True)

    stream = f1.encode() + f2.encode() + f3.encode()
    consumed = conn.receive_bytes(stream)
    assert consumed == len(stream)

    assert len(conn.received_messages) == 1
    opcode, msg = conn.received_messages[0]
    assert opcode == WebSocketOpcode.TEXT
    assert msg == b"Part 1 - Part 2 - Part 3 Complete!"


def test_utf8_continuation_buffer_split_characters():
    """Verify exact nxt_h1p_ws_utf8_t modeling when a 4-byte character is split across frames."""
    conn = WebSocketConnection()

    # The smiley emoji '😀' is 4 bytes: 0xF0 0x9F 0x98 0x80
    emoji_bytes = "FreeUnit 😀 Rocks!".encode("utf-8")
    # Find where the emoji starts
    emoji_idx = emoji_bytes.find(b"\xf0\x9f\x98\x80")
    assert emoji_idx != -1

    # Split right in the middle of the emoji: 2 bytes in frame 1, 2 bytes in frame 2
    part1 = emoji_bytes[: emoji_idx + 2]
    part2 = emoji_bytes[emoji_idx + 2 :]

    f1 = WebSocketFrame(opcode=WebSocketOpcode.TEXT, payload=part1, fin=False)
    f2 = WebSocketFrame(opcode=WebSocketOpcode.CONT, payload=part2, fin=True)

    conn.receive_bytes(f1.encode())
    # UTF-8 buffer must hold the 2 trailing bytes in tail
    assert conn.utf8_state.len == 2

    conn.receive_bytes(f2.encode())
    # Upon completion, UTF-8 state resets and message is intact
    assert conn.utf8_state.len == 0
    assert len(conn.received_messages) == 1
    assert conn.received_messages[0][1].decode("utf-8") == "FreeUnit 😀 Rocks!"


def test_utf8_invalid_tail_on_final_frame():
    """Ending a message with an incomplete multibyte sequence must trigger close 1007."""
    conn = WebSocketConnection()

    # Truncated 3-byte sequence (Euro sign € is 0xE2 0x82 0xAC, send only 2 bytes)
    truncated = b"Price: 50 \xe2\x82"
    f = WebSocketFrame(opcode=WebSocketOpcode.TEXT, payload=truncated, fin=True)

    conn.receive_bytes(f.encode())
    assert conn.close_code == int(WebSocketCloseCode.INVALID_DATA)
    assert conn.state == WebSocketState.CLOSED


def test_pong_coalescing_rfc6455_section_5_5_3():
    """Verify that multiple incoming PINGs coalesce into answering only the most recent."""
    conn = WebSocketConnection()

    # Incoming Ping 1
    p1 = WebSocketFrame(opcode=WebSocketOpcode.PING, payload=b"ping-1", fin=True)
    conn.receive_bytes(p1.encode())

    assert len(conn.outgoing_frames) == 1
    assert conn.outgoing_frames[0].opcode == WebSocketOpcode.PONG
    assert conn.outgoing_frames[0].payload == b"ping-1"
    assert conn.pong_queued is True

    # Incoming Ping 2 and Ping 3 arrive while previous pong is still queued
    p2 = WebSocketFrame(opcode=WebSocketOpcode.PING, payload=b"ping-2", fin=True)
    p3 = WebSocketFrame(opcode=WebSocketOpcode.PING, payload=b"ping-3", fin=True)
    conn.receive_bytes(p2.encode())
    conn.receive_bytes(p3.encode())

    # No additional pong frame emitted yet (coalescing into pong_next_payload)
    assert len(conn.outgoing_frames) == 1
    assert conn.pong_next_payload == b"ping-3"

    # Now simulate completion of first pong transmission
    conn.on_pong_sent()

    # Second pong should now be sent carrying the payload of Ping 3 (Ping 2 was discarded!)
    assert len(conn.outgoing_frames) == 2
    assert conn.outgoing_frames[1].opcode == WebSocketOpcode.PONG
    assert conn.outgoing_frames[1].payload == b"ping-3"


def test_control_frame_interleaving_inside_fragments():
    """Control frames (PING) can arrive in between fragmented data frames."""
    conn = WebSocketConnection()

    f1 = WebSocketFrame(opcode=WebSocketOpcode.TEXT, payload=b"Chunk 1", fin=False)
    ping = WebSocketFrame(opcode=WebSocketOpcode.PING, payload=b"heartbeat", fin=True)
    f2 = WebSocketFrame(opcode=WebSocketOpcode.CONT, payload=b" and Chunk 2", fin=True)

    conn.receive_bytes(f1.encode())
    assert conn.fragment_in_progress is True

    # Interleaved PING
    conn.receive_bytes(ping.encode())
    assert len(conn.outgoing_frames) == 1
    assert conn.outgoing_frames[0].opcode == WebSocketOpcode.PONG
    assert conn.outgoing_frames[0].payload == b"heartbeat"

    # Continuation completes data stream
    conn.receive_bytes(f2.encode())
    assert conn.fragment_in_progress is False
    assert len(conn.received_messages) == 1
    assert conn.received_messages[0][1] == b"Chunk 1 and Chunk 2"
