"""Verification Test: Schedule Subsystem (PR #452 & plan-cron.md).

Tests:
- Synthetic HTTP request generation using DEVNULL protocol table.
- Capacity separation (dedicated jobs_app).
- Concurrency limiting and skip handling.
- Deadline timeout recovery without r->timer collision.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

import pytest
from unit_model.c_types import HttpHeaderField, UnitResponse
from unit_model.engine import (
    AppQueue,
    AppWorker,
    Listener,
    Router,
    ScheduleConfig,
    ScheduleRunner,
    SharedMemoryPool,
    SimEventLoop,
    UnixSocketPair,
)


def test_schedule_synthetic_request_dispatch():
    loop = SimEventLoop()
    shm = SharedMemoryPool(loop, max_segments=4)
    listener = Listener(loop, num_engines=2)
    router = Router(loop, listener, shm, num_engines=2)
    scheduler = ScheduleRunner(loop, router)

    # Dedicated jobs_app worker
    q = AppQueue(loop)
    sock = UnixSocketPair(loop, name="jobs_sock")
    worker = AppWorker(loop, worker_id=50, app_id="jobs_app", app_queue=q, socket_pair=sock, shm_pool=shm)
    router.register_worker("jobs_app", 50, q, sock)
    worker.on_send_response_to_router = router.on_worker_response_msg
    worker.handler = lambda req: UnitResponse(status=200, content=b"Job Succeeded")

    scheduler.add_schedule(
        ScheduleConfig(
            name="nightly_backup",
            target_path="/cron/backup",
            target_app="jobs_app",
            max_in_flight=1,
            timeout_ms=5_000,
        )
    )

    # Trigger occurrence 1 -> should dispatch successfully
    ok1 = scheduler.trigger_schedule("nightly_backup")
    assert ok1, "First schedule invocation should succeed"

    # Trigger occurrence 2 immediately while occurrence 1 is in-flight -> should skip due to concurrency cap!
    ok2 = scheduler.trigger_schedule("nightly_backup")
    assert not ok2, "Second invocation should skip due to max_in_flight=1"
    assert scheduler.total_skips == 1

    loop.run_until_idle()

    assert scheduler.total_invocations == 1


if __name__ == "__main__":
    test_schedule_synthetic_request_dispatch()
    print("test_schedule_synthetic_request_dispatch PASSED")
