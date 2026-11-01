"""Shared-Memory Chunk Allocator (nxt_port_memory.c, nxt_unit.c).

Checked against FreeUnit master 42251f7c9e366df31a2e70a5da3679a2fa855b3d.

Models:
- 16 KiB chunks in 10 MiB segments, 640 chunks per segment
  (src/nxt_port_memory_int.h:24-32).
- A free_map bitmap with busy/free states, scanned for a contiguous run
  (nxt_unit_mmap_get, src/nxt_unit.c:5225-5290).
- The segment limit of a process pair.  In master this is the "shm" setting
  of the application divided by the segment size (lib->shm_mmap_limit,
  src/nxt_unit.c:5296), at most NXT_PORT_MMAP_MAX_SEGMENTS = 410
  (src/nxt_port_memory_int.h:49); NXT_PORT_MMAPS_MAX = 65536 is the size of
  the segment table (src/nxt_port_memory_int.h:75).
- Out-of-shared-memory (OOSM): a non-probing allocation that finds no free
  chunk raises the hdr->oosm flag (src/nxt_unit.c:5280-5290).  The release
  of a chunk by the peer clears the flag and broadcasts SHM_ACK
  (nxt_port_mmap_buf_completion, src/nxt_port_memory.c:240-252); a waiting
  writer resumes on the ack (nxt_unit_wait_shm_ack, src/nxt_unit.c:5402).
- The router answers an OOSM message by acking at once when it already sees
  a free chunk (nxt_router_oosm_handler, src/nxt_router.c:9679-9755).

Simplification: one pool stands for both directions (router -> worker request
chunks and worker -> router response chunks); master has one segment set per
process pair and direction.  The free_map is shared, so either side can set
chunks free: the worker releases request chunks when it releases the request
(src/nxt_unit.c:2621-2627), the router releases response chunks when it has
sent them to the client (src/nxt_port_memory.c:167-240).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from unit_model.c_types import (
    NXT_PORT_MMAP_MAX_SEGMENTS,
    PORT_MMAP_CHUNK_COUNT,
    PORT_MMAP_CHUNK_SIZE,
    PORT_MMAP_DATA_SIZE,
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
        max_segments: int = 16,  # test cap; master: "shm" limit / 10 MiB, <= 410
    ):
        self.loop = loop
        self.name = name
        self.max_segments = min(max_segments, NXT_PORT_MMAP_MAX_SEGMENTS)
        self.segments: Dict[int, ShmSegment] = {}
        self.next_segment_id: int = 0
        self.mutex_locked: bool = False
        # hdr->oosm: a writer found no free chunk and wants an ack.
        self.oosm_flag: bool = False
        self.total_allocations: int = 0
        self.total_deallocations: int = 0
        self.oosm_count: int = 0
        self.shm_ack_broadcasts: int = 0
        self.lock_contentions: int = 0
        self._ack_waiters: List[Callable[[], None]] = []

        # Create initial segment 0
        self._create_segment()

    @property
    def in_oosm(self) -> bool:
        """True while a writer waits for a free chunk (hdr->oosm set)."""
        return self.oosm_flag

    def _create_segment(self) -> Optional[ShmSegment]:
        if len(self.segments) >= self.max_segments:
            return None
        seg_id = self.next_segment_id
        self.next_segment_id += 1
        seg = ShmSegment(id=seg_id)
        self.segments[seg_id] = seg
        self.loop.record_trace(self.name, "created_segment", seg_id=seg_id)
        return seg

    def free_chunk_count(self) -> int:
        return sum(seg.free_chunks for seg in self.segments.values())

    def allocate(
        self,
        size_bytes: int,
        payload: bytes = b"",
        probe: bool = False,
    ) -> Optional[ShmChunkAllocation]:
        """Allocates contiguous chunks under lib->outgoing.mutex.

        probe=True is nxt_unit_mmap_get(..., probe) for a buffer that may
        fall back to plain memory: a miss does not raise hdr->oosm and the
        caller never waits (src/nxt_unit.c:5281-5283, 5856-5860).
        """
        if self.mutex_locked:
            self.lock_contentions += 1
            self.loop.record_trace(self.name, "lock_contention")

        self.mutex_locked = True
        try:
            needed_chunks = max(1, math.ceil(size_bytes / PORT_MMAP_CHUNK_SIZE))

            for seg in self.segments.values():
                alloc = self._try_alloc_in_segment(seg, needed_chunks, size_bytes, payload)
                if alloc is not None:
                    self.total_allocations += 1
                    return alloc

            new_seg = self._create_segment()
            if new_seg is not None:
                alloc = self._try_alloc_in_segment(new_seg, needed_chunks, size_bytes, payload)
                if alloc is not None:
                    self.total_allocations += 1
                    return alloc

            if probe:
                self.loop.record_trace(
                    self.name,
                    "shm_probe_miss",
                    requested_bytes=size_bytes,
                    needed_chunks=needed_chunks,
                )
                return None

            # No free run and the segment limit is reached: OOSM.
            self.oosm_flag = True
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

        run_start = -1
        run_len = 0
        for i in range(seg.total_chunks):
            if seg.bitmap[i]:  # True = free
                if run_start == -1:
                    run_start = i
                run_len += 1
                if run_len == needed:
                    for chunk_idx in range(run_start, run_start + needed):
                        seg.bitmap[chunk_idx] = False
                    seg.free_chunks -= needed

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

    def write(self, mmap_id: int, chunk_id: int, data: bytes) -> None:
        """Copy data into the chunks of an allocation (the writer's memcpy)."""
        seg = self.segments.get(mmap_id)
        if seg is None:
            return
        off = chunk_id * PORT_MMAP_CHUNK_SIZE
        seg.storage[off : off + len(data)] = data

    def free(self, mmap_id: int, start_chunk_id: int, chunk_count: int) -> bool:
        """Sets chunks free (nxt_port_mmap_set_chunk_free) and acks waiters.

        Mirrors nxt_port_mmap_buf_completion (src/nxt_port_memory.c:224-252):
        once the chunks are free, an atomic 1->0 on hdr->oosm broadcasts
        SHM_ACK to the process that raised the flag.
        """
        seg = self.segments.get(mmap_id)
        if not seg or chunk_count <= 0:
            return False

        self.mutex_locked = True
        try:
            for i in range(start_chunk_id, start_chunk_id + chunk_count):
                if 0 <= i < seg.total_chunks and not seg.bitmap[i]:
                    seg.bitmap[i] = True
                    seg.free_chunks += 1

            self.total_deallocations += 1
            self.loop.record_trace(
                self.name,
                "freed",
                seg_id=mmap_id,
                start_chunk=start_chunk_id,
                chunks=chunk_count,
            )
        finally:
            self.mutex_locked = False

        if self.oosm_flag:
            self.oosm_flag = False
            self.loop.record_trace(self.name, "shm_oosm_cleared")
            self.broadcast_shm_ack()
        return True

    def wait_shm_ack(self, on_ack: Callable[[], None]) -> None:
        """A writer blocks in recv until SHM_ACK (nxt_unit_wait_shm_ack)."""
        self._ack_waiters.append(on_ack)

    def broadcast_shm_ack(self) -> None:
        """nxt_process_broadcast_shm_ack: one SHM_ACK datagram per waiter."""
        waiters = self._ack_waiters
        self._ack_waiters = []
        if not waiters:
            return
        self.shm_ack_broadcasts += 1
        self.loop.record_trace(self.name, "shm_ack_broadcast", waiters=len(waiters))
        for cb in waiters:
            # 100 ns: the datagram transit the socket model uses elsewhere.
            self.loop.schedule(100, self.name, "shm_ack_delivered", cb)

    def router_oosm_handler(self) -> bool:
        """nxt_router_oosm_handler (src/nxt_router.c:9679-9755).

        On an OOSM message the router scans the free maps; a free chunk
        anywhere means a release raced the message, and the ack goes out now.
        """
        if self.free_chunk_count() > 0 and self._ack_waiters:
            self.loop.record_trace(self.name, "oosm_already_free_ack")
            self.oosm_flag = False
            self.broadcast_shm_ack()
            return True
        return False

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
