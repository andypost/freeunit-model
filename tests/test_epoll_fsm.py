"""Tests for Epoll FSM & Connection Recycling Races in FreeUnit Model."""

import pytest
from unit_model.engine.epoll_fsm import (
    ConnectionRecyclingFSM,
    EpollFd,
    EpollFlag,
    EpollInstance,
    EpollOp,
)


def test_epoll_level_triggered_vs_edge_triggered():
    ep = EpollInstance(instance_id=1)

    # FD 10: Level-triggered (EPOLLIN)
    ep.epoll_ctl(EpollOp.EPOLL_CTL_ADD, 10, EpollFlag.EPOLLIN)
    # FD 20: Edge-triggered (EPOLLIN | EPOLLET)
    ep.epoll_ctl(EpollOp.EPOLL_CTL_ADD, 20, EpollFlag.EPOLLIN | EpollFlag.EPOLLET)

    # Inbound data arrives on both FDs
    ep.tracked_fds[10].write_inbound(100)
    ep.tracked_fds[20].write_inbound(100)

    # First epoll_wait: both FDs report ready
    ev1 = dict(ep.epoll_wait())
    assert 10 in ev1
    assert 20 in ev1

    # Second epoll_wait without reading data:
    # Level-triggered FD 10 STILL reports ready!
    # Edge-triggered FD 20 does NOT report ready (silenced until next edge or drain)!
    ev2 = dict(ep.epoll_wait())
    assert 10 in ev2
    assert 20 not in ev2

    # Now drain FD 20 partially (50 bytes left): still not re-armed
    ep.tracked_fds[20].read_inbound(50)
    ev3 = dict(ep.epoll_wait())
    assert 20 not in ev3

    # New data arrives on FD 20 -> triggers NEW edge!
    ep.tracked_fds[20].write_inbound(50)
    ev4 = dict(ep.epoll_wait())
    assert 20 in ev4


def test_epoll_oneshot():
    ep = EpollInstance(instance_id=2)
    ep.epoll_ctl(EpollOp.EPOLL_CTL_ADD, 30, EpollFlag.EPOLLIN | EpollFlag.EPOLLONESHOT)

    ep.tracked_fds[30].write_inbound(50)

    # First wait fires
    ev1 = dict(ep.epoll_wait())
    assert 30 in ev1

    # Second wait disabled because of EPOLLONESHOT
    ev2 = dict(ep.epoll_wait())
    assert 30 not in ev2

    # Re-arm via EPOLL_CTL_MOD
    ep.epoll_ctl(EpollOp.EPOLL_CTL_MOD, 30, EpollFlag.EPOLLIN | EpollFlag.EPOLLONESHOT)
    ev3 = dict(ep.epoll_wait())
    assert 30 in ev3


def test_connection_recycling_0ms_timer_race():
    """Verify that pipelined requests arriving before 0ms timer safely re-arm without double-release."""
    conn = ConnectionRecyclingFSM(conn_id=101)

    # Request 1 completes -> 0ms timer scheduled
    conn.finish_request()
    assert conn.requests_completed == 1
    assert conn.deferred_timer_active is True
    assert conn.is_keepalive_idle is True

    # Scenario A: Next pipelined request arrives BEFORE 0ms timer fired
    conn.on_pipelined_request_arrived()
    assert conn.deferred_timer_active is False
    assert conn.is_keepalive_idle is False

    # The stale 0ms timer fires later in the event loop cycle: safely ignored!
    fired = conn.on_deferred_timer_fired()
    assert fired is False
    assert conn.mem_pool_freed is False

    # Request 2 completes -> 0ms timer scheduled again
    conn.finish_request()
    assert conn.requests_completed == 2
    assert conn.deferred_timer_active is True

    # Scenario B: No pipelined request -> 0ms timer fires and frees pool cleanly
    fired2 = conn.on_deferred_timer_fired()
    assert fired2 is True
    assert conn.mem_pool_freed is True
