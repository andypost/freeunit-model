"""Tests for Application Queue Notification Coalescing and Drain Re-arming.

Verifies FreeUnit commit edd1283ca (router: coalesce application queue notifications for one worker):
1. With depth_bound == 1 (single reader), router rings only on 0->1 transition of q->notified.
2. In-flight enqueues while notified == 1 are coalesced, suppressing redundant doorbells (saving ~4% CPU).
3. nxt_app_queue_recv() automatically clears notified when the queue is drained (nxt_app_queue_depth(q) == 0).
4. Sticky-flag deadlock immunity: if a worker dies or dequeues without reading the doorbell,
   the queue re-arms upon drain, so subsequent requests ring normally.
5. With depth_bound > 1, doorbells are governed by queue position relative to depth_bound.
"""

from unit_model.engine.event_loop import SimEventLoop
from unit_model.engine.ipc_queue import AppQueue


def test_queue_coalescing_single_reader() -> None:
    """Verify single reader (depth_bound=1) doorbell coalescing on notified 0->1."""
    loop = SimEventLoop()
    queue = AppQueue(loop=loop, capacity=128)

    assert queue.notified == 0

    # 1. First item enqueued: 0 -> 1 transition triggers doorbell
    ok, need_notify, cookie = queue.send_bound(b"req_1", tracking=101, depth_bound=1)
    assert ok is True
    assert need_notify is True
    assert queue.notified == 1
    assert queue.total_doorbells_sent == 1
    assert queue.total_doorbells_suppressed == 0

    # 2. Subsequent items enqueued while queue is not drained: coalesced (no doorbell)
    ok2, need_notify2, _ = queue.send_bound(b"req_2", tracking=102, depth_bound=1)
    assert ok2 is True
    assert need_notify2 is False
    assert queue.notified == 1

    ok3, need_notify3, _ = queue.send_bound(b"req_3", tracking=103, depth_bound=1)
    assert ok3 is True
    assert need_notify3 is False
    assert queue.notified == 1
    assert queue.total_doorbells_sent == 1
    assert queue.total_doorbells_suppressed == 2

    # 3. Dequeue items until queue is drained
    item1 = queue.recv()
    assert item1 is not None and item1.tracking == 101
    assert queue.notified == 1  # 2 items still pending

    item2 = queue.recv()
    assert item2 is not None and item2.tracking == 102
    assert queue.notified == 1  # 1 item still pending

    # Last item dequeued: queue is now empty, notified flag must automatically reset to 0!
    item3 = queue.recv()
    assert item3 is not None and item3.tracking == 103
    assert queue.is_empty()
    assert queue.notified == 0  # Doorbell re-armed upon draining

    # 4. Next item enqueued after drain must ring doorbell again
    ok4, need_notify4, _ = queue.send_bound(b"req_4", tracking=104, depth_bound=1)
    assert ok4 is True
    assert need_notify4 is True
    assert queue.notified == 1
    assert queue.total_doorbells_sent == 2


def test_queue_coalescing_multi_reader_depth_bound() -> None:
    """Verify multi-reader (depth_bound > 1) position-based doorbell bounding."""
    loop = SimEventLoop()
    queue = AppQueue(loop=loop, capacity=128)

    depth_bound = 3  # e.g., 3 worker processes/threads

    # Items 0, 1, 2 land at pos < depth_bound -> ring doorbell to wake workers
    for i in range(3):
        ok, need_notify, _ = queue.send_bound(f"req_{i}".encode(), tracking=i, depth_bound=depth_bound)
        assert ok is True
        assert need_notify is True, f"Item {i} at pos {i} should ring"

    assert queue.total_doorbells_sent == 3

    # Items 3 and 4 land at pos >= depth_bound -> suppressed (enough doorbells outstanding)
    for i in range(3, 5):
        ok, need_notify, _ = queue.send_bound(f"req_{i}".encode(), tracking=i, depth_bound=depth_bound)
        assert ok is True
        assert need_notify is False, f"Item {i} at pos {i} should be suppressed"

    assert queue.total_doorbells_sent == 3
    assert queue.total_doorbells_suppressed == 2


def test_queue_drain_deadlock_immunity() -> None:
    """Verify sticky-flag deadlock immunity when a worker drains without reading doorbell.
    
    Before FreeUnit commit edd1283ca, if a worker dequeued items while already awake
    without clearing the notified flag via notification_received(), the flag remained at 1,
    permanently silencing all future requests. FreeUnit fixed this by clearing the flag
    in nxt_app_queue_recv() when depth reaches 0.
    """
    loop = SimEventLoop()
    queue = AppQueue(loop=loop, capacity=128)

    # Enqueue request: rings doorbell and sets notified = 1
    ok, need_notify, _ = queue.send_bound(b"item_a", tracking=1, depth_bound=1)
    assert need_notify is True
    assert queue.notified == 1

    # Worker consumes item directly from queue WITHOUT calling notification_received()
    item = queue.recv()
    assert item is not None
    assert queue.is_empty()

    # Invariant: notified flag MUST be 0 now (automatically cleared upon queue drain)
    assert queue.notified == 0

    # Next request arrives: MUST ring doorbell, not stall in permanent silence
    ok, need_notify2, _ = queue.send_bound(b"item_b", tracking=2, depth_bound=1)
    assert ok is True
    assert need_notify2 is True, "Deadlock! Sticky notified flag silenced subsequent request"
    assert queue.notified == 1
