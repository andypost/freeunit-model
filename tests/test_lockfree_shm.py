"""Tests for LockFreeShmPool and PerEngineSlab."""

import concurrent.futures
import pytest

from unit_model.synthesis.lockfree_shm_synth import (
    DoubleFreeError,
    LockFreeShmPool,
    PerEngineSlab,
    SegmentBitmap,
    ShmChunkDescriptor,
)


def test_segment_bitmap_alloc_and_free():
    seg = SegmentBitmap(segment_id=0)
    assert seg.allocated_count == 0

    c0 = seg.try_alloc_chunk()
    assert c0 == 0
    assert seg.allocated_count == 1
    assert seg.bitmap[0] == 1

    c1 = seg.try_alloc_chunk()
    assert c1 == 1
    assert seg.allocated_count == 2
    assert seg.bitmap[0] == 3

    # Free c0
    seg.atomic_free_chunk(0)
    assert seg.allocated_count == 1
    assert seg.bitmap[0] == 2

    # Double free raises DoubleFreeError
    with pytest.raises(DoubleFreeError):
        seg.atomic_free_chunk(0)

    # Free c1
    seg.atomic_free_chunk(1)
    assert seg.allocated_count == 0
    assert seg.bitmap[0] == 0


def test_per_engine_slab_growth():
    slab = PerEngineSlab(engine_id=0, max_segments=4)
    chunks = []

    # Allocate more than one segment's capacity (640 chunks)
    for _ in range(700):
        c = slab.alloc_chunk()
        chunks.append(c)

    assert len(slab.segments) == 2
    assert slab.active_chunks == 700
    assert slab.total_allocations == 700

    # Free all chunks
    for c in chunks:
        slab.free_chunk(c.segment_id, c.chunk_index)

    assert slab.active_chunks == 0
    assert slab.total_frees == 700


def test_lockfree_shm_pool_cross_engine_frees():
    pool = LockFreeShmPool(num_engines=4)

    # Engine 0 allocates
    c0 = pool.alloc(engine_id=0)
    assert c0.engine_id == 0

    # Engine 1 allocates
    c1 = pool.alloc(engine_id=1)
    assert c1.engine_id == 1

    # Cross-engine free: Worker on Engine 3 frees chunk allocated on Engine 0
    pool.free(c0, calling_engine_or_worker_id=3)
    assert pool.cross_engine_frees == 1

    # Normal free: Engine 1 frees chunk allocated on Engine 1
    pool.free(c1, calling_engine_or_worker_id=1)

    assert pool.verify_invariants() is True
    stats = pool.get_stats()
    assert stats["active_chunks"] == 0
    assert stats["global_mutex_locks"] == 0
    assert stats["double_free_errors"] == 0


def test_concurrent_multithreaded_churn():
    pool = LockFreeShmPool(num_engines=4)
    num_threads = 8
    ops_per_thread = 100

    def worker_task(thread_id: int):
        allocated = []
        for i in range(ops_per_thread):
            eng = thread_id % 4
            chunk = pool.alloc(engine_id=eng)
            allocated.append(chunk)

        # Free in reverse or cross-engine
        for chunk in allocated:
            calling_eng = (thread_id + 1) % 4
            pool.free(chunk, calling_engine_or_worker_id=calling_eng)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker_task, tid) for tid in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    assert pool.verify_invariants() is True
    stats = pool.get_stats()
    assert stats["active_chunks"] == 0
    assert stats["total_allocations"] == num_threads * ops_per_thread
    assert stats["total_frees"] == num_threads * ops_per_thread
    assert stats["global_mutex_locks"] == 0
