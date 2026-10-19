"""Tests for Linux signalfd Non-Blocking Read and Spurious EAGAIN Wakeup FSM.

Verifies FreeUnit commit 14c69bbce (PR 776: core: ignore a spurious signalfd read wakeup):
1. FreeUnit macro parity: nxt_epoll_signalfd_eagain(n, err) returns True iff (n == -1 && err == NXT_EAGAIN).
2. Multi-threaded epoll signalfd race: when a signal arrives, multiple router engine threads can wake up;
   the thread that loses the read() race gets EAGAIN.
3. Spurious wakeups return cleanly without logging false-positive nxt_alert messages.
4. Genuine errors (EINTR, short reads < 128 bytes, descriptor failures) still alert as required.
"""

import signal

from unit_model.c_types import (
    CSignalfdSiginfo,
    NXT_AGAIN,
    NXT_EINTR,
)
from unit_model.engine.epoll_fsm import (
    EpollFlag,
    EpollInstance,
    EpollOp,
    SignalfdFSM,
)


def test_signalfd_eagain_classification() -> None:
    """Parity with FreeUnit src/test/nxt_signalfd_test.c."""
    fsm = SignalfdFSM()

    # Exact cases from nxt_signalfd_cases in FreeUnit C test
    assert fsm.nxt_epoll_signalfd_eagain(-1, NXT_AGAIN) is True
    assert fsm.nxt_epoll_signalfd_eagain(-1, NXT_EINTR) is False
    assert fsm.nxt_epoll_signalfd_eagain(0, 0) is False
    assert fsm.nxt_epoll_signalfd_eagain(1, 0) is False
    assert fsm.nxt_epoll_signalfd_eagain(128, 0) is False
    assert fsm.nxt_epoll_signalfd_eagain(64, 0) is False


def test_signalfd_multi_engine_spurious_wakeup_race() -> None:
    """Verify multi-engine thread race on shared signalfd does not trigger alerts on EAGAIN."""
    signalfd = SignalfdFSM(fd=15)

    # Two router engine epoll instances
    engine_0 = EpollInstance(instance_id=0)
    engine_1 = EpollInstance(instance_id=1)

    engine_0.epoll_ctl(EpollOp.EPOLL_CTL_ADD, signalfd.fd, EpollFlag.EPOLLIN)
    engine_1.epoll_ctl(EpollOp.EPOLL_CTL_ADD, signalfd.fd, EpollFlag.EPOLLIN)

    # 1. Kernel delivers SIGCHLD to router process
    signalfd.raise_signal(signo=signal.SIGCHLD, pid=5432, status=0)
    assert signalfd.total_signals_raised == 1

    # Simulate epoll readiness on both engine threads
    engine_0.tracked_fds[signalfd.fd].readable_bytes = 128
    engine_1.tracked_fds[signalfd.fd].readable_bytes = 128

    events_0 = engine_0.epoll_wait()
    events_1 = engine_1.epoll_wait()

    assert any(fd == signalfd.fd and (ev & EpollFlag.EPOLLIN) for fd, ev in events_0)
    assert any(fd == signalfd.fd and (ev & EpollFlag.EPOLLIN) for fd, ev in events_1)

    # 2. Engine 0 executes handler first and consumes signal
    n0, err0, siginfo0 = signalfd.read_signalfd()
    assert n0 == 128
    assert err0 == 0
    assert siginfo0 is not None
    assert siginfo0.ssi_signo == signal.SIGCHLD
    assert siginfo0.ssi_pid == 5432

    alert0, msg0 = signalfd.handle_signalfd_read(n0, err0)
    assert alert0 is False
    assert "signal consumed successfully" in msg0

    # 3. Engine 1 executes handler second (loses race -> queue is now empty)
    n1, err1, siginfo1 = signalfd.read_signalfd()
    assert n1 == -1
    assert err1 == NXT_AGAIN
    assert siginfo1 is None
    assert signalfd.total_spurious_eagains == 1

    # Engine 1 evaluates read result with nxt_epoll_signalfd_eagain
    alert1, msg1 = signalfd.handle_signalfd_read(n1, err1)
    assert alert1 is False, "Spurious EAGAIN must NOT alert!"
    assert "spurious EAGAIN ignored" in msg1

    # Overall alert count across router engines must be zero
    assert signalfd.total_alerts == 0


def test_signalfd_actual_errors_alert() -> None:
    """Verify that non-EAGAIN errors and short reads still produce alerts."""
    signalfd = SignalfdFSM(fd=15)

    # 1. Interrupted system call (EINTR)
    n, err, _ = signalfd.read_signalfd(force_errno=NXT_EINTR)
    alert, msg = signalfd.handle_signalfd_read(n, err)
    assert alert is True
    assert "read signalfd(15) failed" in msg

    # 2. Short read (< 128 bytes)
    signalfd.raise_signal(signo=signal.SIGHUP, pid=1, status=0)
    n2, err2, _ = signalfd.read_signalfd(buffer_size=64)
    alert2, msg2 = signalfd.handle_signalfd_read(n2, err2)
    assert alert2 is True
    assert "read signalfd(15) failed" in msg2

    assert signalfd.total_alerts == 2
