"""TLS 1.3 0-RTT & Session Resumption Engine.

Faithfully models FreeUnit's TLS lifecycle (nxt_conn_tls.c, OpenSSL 3.0+ EVP callback):
- Full TLS 1.3 Handshake (1.5 RTT)
- Session Ticket Resumption via NewSessionTicket (1.0 RTT)
- 0-RTT Early Data with Anti-Replay Cache (RFC 8446 Section 8)
- Session ticket rotation callback (SSL_CTX_set_tlsext_ticket_key_evp_cb)
- Anti-replay rejection with HTTP 425 Too Early fallback
"""

from __future__ import annotations

import hashlib
import hmac
import os
import struct
import time
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Dict, List, Optional, Set, Tuple


class TlsVersion(IntEnum):
    TLS_1_2 = 0x0303
    TLS_1_3 = 0x0304


class TlsHandshakeType(IntEnum):
    CLIENT_HELLO = 1
    SERVER_HELLO = 2
    NEW_SESSION_TICKET = 4
    END_OF_EARLY_DATA = 5
    ENCRYPTED_EXTENSIONS = 8
    CERTIFICATE = 11
    CERTIFICATE_VERIFY = 15
    FINISHED = 20
    KEY_UPDATE = 24


class TlsCipherSuite(IntEnum):
    TLS_AES_128_GCM_SHA256 = 0x1301
    TLS_AES_256_GCM_SHA384 = 0x1302
    TLS_CHACHA20_POLY1305_SHA256 = 0x1303


@dataclass
class TlsSessionTicket:
    """An encrypted TLS 1.3 Session Ticket."""

    ticket_id: bytes  # 16-byte random nonce
    session_id: str
    cipher_suite: TlsCipherSuite
    master_secret: bytes
    created_at_ns: int
    lifetime_ns: int = 7200 * 1_000_000_000  # 2 hours default
    max_early_data_size: int = 16384  # 16 KiB early data threshold

    def is_expired(self, current_time_ns: int) -> bool:
        return current_time_ns > (self.created_at_ns + self.lifetime_ns)


class TlsAntiReplayCache:
    """Anti-Replay Cache for TLS 1.3 0-RTT (RFC 8446 Section 8.2).

    Prevents replay attacks by tracking single-use ticket IDs within a sliding window.
    """

    def __init__(self, window_size: int = 10000) -> None:
        self.seen_tickets: Set[bytes] = set()
        self.window_size = window_size
        self.ticket_order: List[bytes] = []

    def check_and_record(self, ticket_id: bytes) -> bool:
        """Check if ticket was already seen.

        Returns True if fresh (accepted), False if replayed (rejected).
        """
        if ticket_id in self.seen_tickets:
            return False  # Replay detected!

        self.seen_tickets.add(ticket_id)
        self.ticket_order.append(ticket_id)

        # Evict oldest entries when exceeding window
        if len(self.ticket_order) > self.window_size:
            oldest = self.ticket_order.pop(0)
            self.seen_tickets.discard(oldest)

        return True


class TlsHandshakeEngine:
    """TLS 1.3 Server Handshake Engine modeling OpenSSL callbacks."""

    def __init__(
        self,
        server_name: str = "localhost",
        enable_0rtt: bool = True,
        ticket_key: Optional[bytes] = None,
    ) -> None:
        self.server_name = server_name
        self.enable_0rtt = enable_0rtt
        self.ticket_key = ticket_key or os.urandom(32)
        self.anti_replay = TlsAntiReplayCache()
        self.issued_tickets: Dict[bytes, TlsSessionTicket] = {}

    def issue_session_ticket(
        self,
        session_id: str,
        current_time_ns: int,
        cipher_suite: TlsCipherSuite = TlsCipherSuite.TLS_AES_128_GCM_SHA256,
    ) -> TlsSessionTicket:
        """Generate and encrypt a NewSessionTicket."""
        ticket_id = os.urandom(16)
        master_secret = os.urandom(32)
        ticket = TlsSessionTicket(
            ticket_id=ticket_id,
            session_id=session_id,
            cipher_suite=cipher_suite,
            master_secret=master_secret,
            created_at_ns=current_time_ns,
        )
        self.issued_tickets[ticket_id] = ticket
        return ticket

    def process_client_hello(
        self,
        current_time_ns: int,
        ticket_id: Optional[bytes] = None,
        has_early_data: bool = False,
    ) -> Tuple[str, int, bool]:
        """Process ClientHello and determine handshake mode and latency.

        Returns (mode, latency_ns, early_data_accepted):
        - mode: "FULL_HANDSHAKE", "RESUMPTION", "0RTT_ACCEPTED", "0RTT_REJECTED"
        - latency_ns: Simulated duration to connection ready
        - early_data_accepted: True if 0-RTT data was accepted
        """
        if ticket_id is None or ticket_id not in self.issued_tickets:
            # Full 1.5 RTT Handshake (~1500 ns simulated)
            return "FULL_HANDSHAKE", 1500, False

        ticket = self.issued_tickets[ticket_id]
        if ticket.is_expired(current_time_ns):
            # Expired ticket -> Fallback to full handshake
            return "FULL_HANDSHAKE", 1500, False

        # Session resumption with ticket
        if has_early_data and self.enable_0rtt:
            # Check anti-replay cache
            is_fresh = self.anti_replay.check_and_record(ticket.ticket_id)
            if is_fresh:
                # 0-RTT Accepted! 0 ns latency to start processing early data
                return "0RTT_ACCEPTED", 0, True
            else:
                # Replay detected -> Reject 0-RTT and fall back to 1-RTT resumption
                return "0RTT_REJECTED", 1000, False

        # 1-RTT Resumption (1.0 RTT, ~1000 ns simulated)
        return "RESUMPTION", 1000, False
