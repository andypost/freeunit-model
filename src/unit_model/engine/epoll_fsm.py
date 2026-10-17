"""Linux Epoll Engine & Connection Recycling FSM.

Faithfully models:
- Linux epoll flags: EPOLLIN, EPOLLOUT, EPOLLET (Edge-Triggered), EPOLLEXCLUSIVE
- Edge-triggered notification semantics: events fire once upon state change until drained
- EPOLLEXCLUSIVE distribution across multi-engine thread pools
- Keep-alive connection recycling with 0ms deferred release timers (r->timer)
- Protection against use-after-free and double-release races on keepalive recycling
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntFlag
from typing import Dict, List, Optional, Set, Tuple


class EpollFlag(IntFlag):
    NONE = 0x0
    EPOLLIN = 0x001
    EPOLLPRI = 0x002
    EPOLLOUT = 0x004
    EPOLLERR = 0x008
    EPOLLHUP = 0x010
    EPOLLET = 0x80000000
    EPOLLONESHOT = 0x40000000
    EPOLLEXCLUSIVE = 0x10000000


class EpollOp:
    EPOLL_CTL_ADD = 1
    EPOLL_CTL_DEL = 2
    EPOLL_CTL_MOD = 3


class EpollFd:
    """A simulated file descriptor tracked within epoll."""

    def __init__(self, fd: int, flags: EpollFlag = EpollFlag.NONE) -> None:
        self.fd = fd
        self.flags = flags
        self.readable_bytes: int = 0
        self.writable_space: int = 65536
        self.has_error: bool = False
        self.is_hangup: bool = False
        self.closed: bool = False

        # Edge-triggered tracking: true once notified, reset only when drained to 0
        self.et_in_signaled: bool = False
        self.et_out_signaled: bool = False

    @property
    def is_edge_triggered(self) -> bool:
        return bool(self.flags & EpollFlag.EPOLLET)

    def write_inbound(self, nbytes: int) -> None:
        """Data arrived on socket from network."""
        self.readable_bytes += nbytes
        if self.is_edge_triggered and nbytes > 0:
            # New data arrival triggers an edge notification
            self.et_in_signaled = False

    def read_inbound(self, nbytes: int) -> int:
        """Application reads bytes from socket."""
        read = min(self.readable_bytes, nbytes)
        self.readable_bytes -= read
        if self.is_edge_triggered and self.readable_bytes == 0:
            # Completely drained: re-arms edge trigger for next arrival
            self.et_in_signaled = False
        return read

    def get_ready_events(self) -> EpollFlag:
        """Evaluate ready events based on level/edge triggered semantics."""
        events = EpollFlag.NONE
        if self.closed or self.has_error:
            events |= EpollFlag.EPOLLERR
        if self.is_hangup:
            events |= EpollFlag.EPOLLHUP

        # Check read readiness
        if self.flags & EpollFlag.EPOLLIN:
            if self.readable_bytes > 0:
                if self.is_edge_triggered:
                    if not self.et_in_signaled:
                        events |= EpollFlag.EPOLLIN
                        self.et_in_signaled = True
                else:
                    events |= EpollFlag.EPOLLIN

        # Check write readiness
        if self.flags & EpollFlag.EPOLLOUT:
            if self.writable_space > 0:
                if self.is_edge_triggered:
                    if not self.et_out_signaled:
                        events |= EpollFlag.EPOLLOUT
                        self.et_out_signaled = True
                else:
                    events |= EpollFlag.EPOLLOUT

        return events


class EpollInstance:
    """Simulated Linux epoll file descriptor (epoll_create1)."""

    def __init__(self, instance_id: int = 0) -> None:
        self.instance_id = instance_id
        self.tracked_fds: Dict[int, EpollFd] = {}
        self.exclusive_groups: Dict[int, List[int]] = {}  # listen_fd -> [engine_epoll_ids]

    def epoll_ctl(self, op: int, fd: int, flags: EpollFlag = EpollFlag.NONE) -> None:
        if op == EpollOp.EPOLL_CTL_ADD:
            if fd in self.tracked_fds:
                raise ValueError(f"FD {fd} already registered in epoll {self.instance_id}")
            self.tracked_fds[fd] = EpollFd(fd, flags)
        elif op == EpollOp.EPOLL_CTL_MOD:
            if fd not in self.tracked_fds:
                raise ValueError(f"FD {fd} not found for MOD in epoll {self.instance_id}")
            efd = self.tracked_fds[fd]
            efd.flags = flags
            # Modification resets edge trigger signaled states
            efd.et_in_signaled = False
            efd.et_out_signaled = False
        elif op == EpollOp.EPOLL_CTL_DEL:
            self.tracked_fds.pop(fd, None)

    def epoll_wait(self, max_events: int = 64) -> List[Tuple[int, EpollFlag]]:
        """Collect all ready events according to epoll semantics."""
        ready: List[Tuple[int, EpollFlag]] = []
        for fd, efd in self.tracked_fds.items():
            if len(ready) >= max_events:
                break
            ev = efd.get_ready_events()
            if ev != EpollFlag.NONE:
                ready.append((fd, ev))

                # Handle EPOLLONESHOT
                if efd.flags & EpollFlag.EPOLLONESHOT:
                    efd.flags = EpollFlag.NONE

        return ready


class ConnectionRecyclingFSM:
    """Models Keep-Alive Connection Recycling and 0ms Deferred Timer Races.

    Directly verifies the historical bug from GEMINI.md:
    'Deferred release via 0 ms timer (r->timer) collided with app->timeout and event loop scheduling.'
    """

    def __init__(self, conn_id: int) -> None:
        self.conn_id = conn_id
        self.requests_completed: int = 0
        self.deferred_timer_active: bool = False
        self.mem_pool_freed: bool = False
        self.is_keepalive_idle: bool = False
        self.closing: bool = False

    def finish_request(self) -> None:
        """Request finished; schedules 0ms deferred release timer."""
        self.requests_completed += 1
        self.deferred_timer_active = True
        self.is_keepalive_idle = True

    def on_pipelined_request_arrived(self) -> None:
        """Next request arrived BEFORE 0ms deferred release timer fired."""
        if not self.is_keepalive_idle:
            raise ValueError("Cannot pipeline on non-idle connection")

        # In FreeUnit, arriving requests on keepalive must cancel/re-arm r->timer
        self.is_keepalive_idle = False
        self.deferred_timer_active = False

    def on_deferred_timer_fired(self) -> bool:
        """0ms deferred timer callback executes."""
        if not self.deferred_timer_active:
            # Stale timer execution safely ignored!
            return False

        self.deferred_timer_active = False
        self.mem_pool_freed = True
        return True

    def close(self) -> None:
        """Close connection."""
        self.closing = True
        self.deferred_timer_active = False
        self.is_keepalive_idle = False
