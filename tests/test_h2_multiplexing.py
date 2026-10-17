"""Tests for RFC 9113 HTTP/2 Multiplexing & Flow Control in FreeUnit Model."""

import struct
import pytest
from unit_model.engine.protocols.h2 import (
    H2Connection,
    H2ErrorCode,
    H2Flag,
    H2Frame,
    H2FrameType,
    H2Stream,
    H2StreamState,
)


def test_h2_preface_and_settings():
    conn = H2Connection(is_server=True)

    # Client preface + SETTINGS frame
    client_stream = H2Connection.PREFACE + H2Frame(
        frame_type=H2FrameType.SETTINGS,
        stream_id=0,
    ).encode()

    consumed = conn.receive_bytes(client_stream)
    assert consumed == len(client_stream)
    assert conn.preface_received is True

    # Server must have sent its initial SETTINGS + SETTINGS ACK
    assert len(conn.outgoing_frames) == 2
    assert conn.outgoing_frames[0].frame_type == H2FrameType.SETTINGS
    assert conn.outgoing_frames[0].flags == 0
    assert conn.outgoing_frames[1].frame_type == H2FrameType.SETTINGS
    assert conn.outgoing_frames[1].flags == H2Flag.ACK


def test_h2_stream_multiplexing():
    """Verify concurrent interleaved streams 1 and 3."""
    conn = H2Connection(is_server=True)
    conn.receive_bytes(H2Connection.PREFACE)

    # Stream 1: HEADERS
    h1 = H2Frame(frame_type=H2FrameType.HEADERS, stream_id=1, flags=0)
    # Stream 3: HEADERS
    h3 = H2Frame(frame_type=H2FrameType.HEADERS, stream_id=3, flags=0)
    # Stream 1: DATA chunk 1
    d1 = H2Frame(frame_type=H2FrameType.DATA, stream_id=1, flags=0, payload=b"Hello Stream 1, ")
    # Stream 3: DATA chunk 1 + END_STREAM
    d3 = H2Frame(frame_type=H2FrameType.DATA, stream_id=3, flags=H2Flag.END_STREAM, payload=b"Hello Stream 3 Full!")
    # Stream 1: DATA chunk 2 + END_STREAM
    d1_end = H2Frame(frame_type=H2FrameType.DATA, stream_id=1, flags=H2Flag.END_STREAM, payload=b"Part 2 Done!")

    wire = h1.encode() + h3.encode() + d1.encode() + d3.encode() + d1_end.encode()
    conn.receive_bytes(wire)

    # Both streams must be tracked independently and completed
    assert 1 in conn.streams
    assert 3 in conn.streams
    assert conn.streams[1].state == H2StreamState.HALF_CLOSED_REMOTE
    assert conn.streams[3].state == H2StreamState.HALF_CLOSED_REMOTE

    assert len(conn.completed_requests) == 2
    # Stream 3 completed first in wire order
    assert conn.completed_requests[0][0] == 3
    assert conn.completed_requests[0][2] == b"Hello Stream 3 Full!"
    # Stream 1 completed second
    assert conn.completed_requests[1][0] == 1
    assert conn.completed_requests[1][2] == b"Hello Stream 1, Part 2 Done!"


def test_h2_stream_isolation_rst():
    """Verify that RST_STREAM on Stream 3 does not affect Stream 1."""
    conn = H2Connection(is_server=True)
    conn.receive_bytes(H2Connection.PREFACE)

    # Start Stream 1 and Stream 3
    h1 = H2Frame(frame_type=H2FrameType.HEADERS, stream_id=1, flags=0)
    h3 = H2Frame(frame_type=H2FrameType.HEADERS, stream_id=3, flags=0)
    conn.receive_bytes(h1.encode() + h3.encode())

    # Send RST_STREAM on Stream 3 (CANCEL error)
    rst3 = H2Frame(
        frame_type=H2FrameType.RST_STREAM,
        stream_id=3,
        payload=struct.pack("!I", int(H2ErrorCode.CANCEL)),
    )
    conn.receive_bytes(rst3.encode())

    # Stream 3 is CLOSED with error CANCEL
    assert conn.streams[3].state == H2StreamState.CLOSED
    assert conn.streams[3].error_code == H2ErrorCode.CANCEL

    # Stream 1 is STILL OPEN and active!
    assert conn.streams[1].state == H2StreamState.OPEN

    # Stream 1 can finish successfully
    d1 = H2Frame(frame_type=H2FrameType.DATA, stream_id=1, flags=H2Flag.END_STREAM, payload=b"Stream 1 Success")
    conn.receive_bytes(d1.encode())

    assert len(conn.completed_requests) == 1
    assert conn.completed_requests[0][0] == 1
    assert conn.completed_requests[0][2] == b"Stream 1 Success"


def test_h2_flow_control_window():
    """Verify window tracking and WINDOW_UPDATE increment."""
    conn = H2Connection(is_server=True)
    conn.receive_bytes(H2Connection.PREFACE)

    h1 = H2Frame(frame_type=H2FrameType.HEADERS, stream_id=1, flags=0)
    conn.receive_bytes(h1.encode())

    initial_window = conn.streams[1].inbound_window
    chunk = b"A" * 1000
    d1 = H2Frame(frame_type=H2FrameType.DATA, stream_id=1, flags=0, payload=chunk)
    conn.receive_bytes(d1.encode())

    # Inbound window decremented
    assert conn.streams[1].inbound_window == initial_window - 1000
    assert conn.connection_inbound_window == H2Connection.DEFAULT_INITIAL_WINDOW_SIZE - 1000

    # Peer sends WINDOW_UPDATE on stream 1
    win_up = H2Frame(
        frame_type=H2FrameType.WINDOW_UPDATE,
        stream_id=1,
        payload=struct.pack("!I", 5000),
    )
    conn.receive_bytes(win_up.encode())
    assert conn.streams[1].outbound_window == H2Stream.DEFAULT_INITIAL_WINDOW_SIZE + 5000


def test_h2_graceful_goaway():
    """Verify GOAWAY handling terminates connection cleanly."""
    conn = H2Connection(is_server=True)
    conn.receive_bytes(H2Connection.PREFACE)

    goaway = H2Frame(
        frame_type=H2FrameType.GOAWAY,
        stream_id=0,
        payload=struct.pack("!II", 1, int(H2ErrorCode.NO_ERROR)),
    )
    conn.receive_bytes(goaway.encode())

    assert conn.goaway_received is True
    assert conn.closed is True
