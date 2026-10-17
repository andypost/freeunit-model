"""FreeUnit Protocol Simulation Modules.

Provides high-fidelity implementations of transport and session protocols:
- websocket: RFC 6455 framing, fragmentation, UTF-8 continuation buffer, ping-pong
- h2: RFC 9113 HTTP/2 stream multiplexer, flow control, frame state machine
"""

from unit_model.engine.protocols.h2 import (
    H2Connection,
    H2ErrorCode,
    H2Frame,
    H2FrameType,
    H2Stream,
    H2StreamState,
)
from unit_model.engine.protocols.websocket import (
    WebSocketCloseCode,
    WebSocketConnection,
    WebSocketFrame,
    WebSocketOpcode,
    WebSocketState,
    WsUtf8Buffer,
)

__all__ = [
    "WebSocketConnection",
    "WebSocketFrame",
    "WebSocketOpcode",
    "WebSocketState",
    "WebSocketCloseCode",
    "WsUtf8Buffer",
    "H2Connection",
    "H2Stream",
    "H2StreamState",
    "H2Frame",
    "H2FrameType",
    "H2ErrorCode",
]
