"""Tests for TLS 1.3 0-RTT & Session Resumption Engine."""

import pytest
from unit_model.engine.protocols.tls import (
    TlsAntiReplayCache,
    TlsCipherSuite,
    TlsHandshakeEngine,
    TlsSessionTicket,
)


def test_tls_full_handshake():
    engine = TlsHandshakeEngine()
    current_time = 1_000_000_000  # 1 second in ns

    # No ticket provided -> Full 1.5 RTT Handshake
    mode, latency, early_accepted = engine.process_client_hello(current_time, ticket_id=None)
    assert mode == "FULL_HANDSHAKE"
    assert latency == 1500
    assert early_accepted is False


def test_tls_session_resumption_1rtt():
    engine = TlsHandshakeEngine()
    current_time = 1_000_000_000

    # Issue ticket for session
    ticket = engine.issue_session_ticket("session-xyz", current_time)

    # Client presents ticket without early data -> 1-RTT Resumption
    mode, latency, early_accepted = engine.process_client_hello(
        current_time + 10_000_000,
        ticket_id=ticket.ticket_id,
        has_early_data=False,
    )
    assert mode == "RESUMPTION"
    assert latency == 1000
    assert early_accepted is False


def test_tls_0rtt_early_data_acceptance_and_replay_rejection():
    """Verify that first 0-RTT use succeeds and replayed ticket is rejected."""
    engine = TlsHandshakeEngine(enable_0rtt=True)
    current_time = 1_000_000_000

    ticket = engine.issue_session_ticket("session-abc", current_time)

    # First attempt: 0-RTT Early Data accepted
    mode1, latency1, accepted1 = engine.process_client_hello(
        current_time + 5_000_000,
        ticket_id=ticket.ticket_id,
        has_early_data=True,
    )
    assert mode1 == "0RTT_ACCEPTED"
    assert latency1 == 0
    assert accepted1 is True

    # Second attempt (replay attack with same ticket): Anti-replay cache rejects 0-RTT!
    mode2, latency2, accepted2 = engine.process_client_hello(
        current_time + 10_000_000,
        ticket_id=ticket.ticket_id,
        has_early_data=True,
    )
    assert mode2 == "0RTT_REJECTED"
    assert latency2 == 1000
    assert accepted2 is False


def test_tls_expired_ticket_fallback():
    engine = TlsHandshakeEngine()
    current_time = 1_000_000_000

    ticket = engine.issue_session_ticket("session-old", current_time)
    # Set expiration in past
    ticket.lifetime_ns = 500_000_000  # 0.5s

    # Present ticket 2 seconds later -> Expired -> Fallback to Full Handshake
    mode, latency, accepted = engine.process_client_hello(
        current_time + 2_000_000_000,
        ticket_id=ticket.ticket_id,
    )
    assert mode == "FULL_HANDSHAKE"
    assert latency == 1500
    assert accepted is False
