"""Shared-Memory Chunk Allocator (nxt_port_memory.c).

Faithfully models:
- 16 KiB chunk allocator (PORT_MMAP_CHUNK_SIZE = 16384).
- 10 MiB segment sizing (PORT_MMAP_DATA_SIZE = 10485760), yielding 640 chunks/segment.
- Bitmap allocator (free_map) with busy/free states.
- Mutual exclusion lock (mmaps->mutex) contention simulation.
- Out-of-Shared-Memory (OOSM) events, backpressure, and SHM_ACK release broadcasts.
- Bounded segment growth up to NXT_PORT_MMAPS_MAX (65536).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from unit_model.c_types import (
    PORT_MMAP_CHUNK_COUNT,
    PORT_MMAP_CHUNK_SIZE,
    PORT_MMAP_DATA_SIZE,
    NXT_PORT_MMAPS_MAX,
    MmapMsg,
)
from unit_model.engine.event_loop import SimEventLoop


@dataclass
class ShmChunkAllocation:
    mmap_id: int
    start_chunk_id: int
    chunk_count: int
    size_bytes: int
    data: bytearray = field(default_factory=bytearray)


@dataclass
class ShmSegment:
    id: int
    total_chunks: int = PORT_MMAP_CHUNK_COUNT
    free_chunks: int = PORT_MMAP_CHUNK_COUNT
    bitmap: List[bool] = field(default_factory=lambda: [True] * PORT_MMAP_CHUNK_COUNT)
    storage: bytearray = field(default_factory=lambda: bytearray(PORT_MMAP_DATA_SIZE))


class SharedMemoryPool:
    """Simulated FreeUnit shared-memory chunk pool across processes."""

    def __init__(
        self,
        loop: SimEventLoop,
        name: str = "shm_pool",
        max_segments: int = 16,  # Configurable test cap, default prod max is 65536
    ):
        self.loop = loop
        self.name = name
        self.max_segments = min(max_segments, NXT_PORT_MMAPS_MAX)
        self.segments: Dict[int, ShmSegment] = {}
        self.next_segment_id: int = 0
        self.mutex_locked: bool = False
        self.in_oosm: bool = False
        self.total_allocations: int = 0
        self.total_deallocations: int = 0
        self.oosm_count: int = 0
        self.lock_contentions: int = 0

        # Create initial segment 0
        self._create_segment()

    def _create_segment(self) -> Optional[ShmSegment]:
        if len(self.segments) >= self.max_segments:
            return None
        seg_id = self.next_segment_id
        self.next_segment_id += 1
        seg = ShmSegment(id=seg_id)
        self.segments[seg_id] = seg
        self.loop.record_trace(self.name, "created_segment", seg_id=seg_id)
        return seg

    def allocate(self, size_bytes: int, payload: bytes = b"") -> Optional[ShmChunkAllocation]:
        """Allocates contiguous chunks under mmaps->mutex."""
        if self.mutex_locked:
            self.lock_contentions += 1
            self.loop.record_trace(self.name, "lock_contention")

        self.mutex_locked = True
        try:
            needed_chunks = max(1, math.ceil(size_bytes / PORT_MMAP_CHUNK_SIZE))

            # Find contiguous free chunks in existing segments
            for seg in self.segments.values():
                alloc = self._try_alloc_in_segment(seg, needed_chunks, size_bytes, payload)
                if alloc is not None:
                    self.total_allocations += 1
                    return alloc

            # Try creating a new segment
            new_seg = self._create_segment()
            if new_seg is not None:
                alloc = self._try_alloc_in_segment(new_seg, needed_chunks, size_bytes, payload)
                if alloc is not None:
                    self.total_allocations += 1
                    return alloc

            # Memory exhausted: enter OOSM
            self.in_oosm = True
            self.oosm_count += 1
            self.loop.record_trace(
                self.name,
                "shm_oosm_exhausted",
                requested_bytes=size_bytes,
                needed_chunks=needed_chunks,
            )
            return None

        finally:
            self.mutex_locked = False

    def _try_alloc_in_segment(
        self,
        seg: ShmSegment,
        needed: int,
        size_bytes: int,
        payload: bytes,
    ) -> Optional[ShmChunkAllocation]:
        if seg.free_chunks < needed:
            return None

        # Linear scan for contiguous free run
        run_start = -1
        run_len = 0
        for i in range(seg.total_chunks):
            if seg.bitmap[i]:  # True = free
                if run_start == -1:
                    run_start = i
                run_len += 1
                if run_len == needed:
                    # Found!
                    for chunk_idx in range(run_start, run_start + needed):
                        seg.bitmap[chunk_idx] = False
                    seg.free_chunks -= needed

                    # Copy payload into simulated buffer
                    alloc_data = bytearray(payload.ljust(size_bytes, b"\x00"))
                    byte_offset = run_start * PORT_MMAP_CHUNK_SIZE
                    seg.storage[byte_offset : byte_offset + len(alloc_data)] = alloc_data

                    self.loop.record_trace(
                        self.name,
                        "allocated",
                        seg_id=seg.id,
                        start_chunk=run_start,
                        chunks=needed,
                        bytes=size_bytes,
                    )
                    return ShmChunkAllocation(
                        mmap_id=seg.id,
                        start_chunk_id=run_start,
                        chunk_count=needed,
                        size_bytes=size_bytes,
                        data=alloc_data,
                    )
            else:
                run_start = -1
                run_len = 0

        return None

    def free(self, mmap_id: int, start_chunk_id: int, chunk_count: int) -> bool:
        """Frees chunks under mmaps->mutex."""
        seg = self.segments.get(mmap_id)
        if not seg:
            return False

        self.mutex_locked = True
        try:
            for i in range(start_chunk_id, start_chunk_id + chunk_count):
                if 0 <= i < seg.total_chunks:
                    seg.bitmap[i] = True
                    seg.free_chunks += 1

            self.total_deallocations += 1
            if self.in_oosm:
                self.in_oosm = False
                self.loop.record_trace(self.name, "shm_oosm_cleared")

            self.loop.record_trace(
                self.name,
                "freed",
                seg_id=mmap_id,
                start_chunk=start_chunk_id,
                chunks=chunk_count,
            )
            return True
        finally:
            self.mutex_locked = False

    def get_data(self, mmap_id: int, start_chunk_id: int, size: int) -> Optional[bytes]:
        """Reads payload from shared memory segment."""
        seg = self.segments.get(mmap_id)
        if not seg:
            return None
        byte_offset = start_chunk_id * PORT_MMAP_CHUNK_SIZE
        return bytes(seg.storage[byte_offset : byte_offset + size])

    def to_mmap_msg(self, alloc: ShmChunkAllocation) -> MmapMsg:
        return MmapMsg(
            mmap_id=alloc.mmap_id,
            chunk_id=alloc.start_chunk_id,
            size=alloc.size_bytes,
        )
