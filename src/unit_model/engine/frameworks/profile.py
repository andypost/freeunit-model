"""Response size and write-pattern profiles for the framework adapters.

Everything here is an ASSUMPTION used to drive the simulation, not a
measurement of FreeUnit and not a property of FreeUnit master.  Each
framework adapter documents where its numbers come from.  What is checked
against master is only how libunit turns the writes into messages
(src/unit_model/engine/worker.py).

Write patterns that matter to libunit:
- One write of the whole body: the first write rides in the header buffer
  when it fits the room left (about 1008 bytes minus the fields); else the
  header goes alone and the body is one buffer per write.
- PHP with output_buffering=4096 (the php.ini-production default) hands the
  SAPI the output in flushes of about 4 KiB, so a 50 KiB page is about a
  dozen nxt_unit_response_write() calls.  With output_buffering=0 (no
  php.ini) every echo is a write.
- An explicit flush() before any output sends the header on its own
  (nxt_php_flush, src/nxt_php_sapi.c:1724-1748).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import List, Optional

from unit_model.c_types import HttpHeaderField, UnitResponse


PHP_OUTPUT_BUFFERING = 4096  # php.ini-production default (assumption: production php.ini in use)


@dataclass(frozen=True)
class ResponseProfile:
    """A log-normal body size distribution plus the write pattern."""

    name: str
    median_bytes: int
    sigma: float            # log-normal spread; 0.5 ~ x1.6 at one sigma
    min_bytes: int
    max_bytes: int
    write_chunk: Optional[int]   # None: one write; N: writes of N bytes
    flush_header_first: bool = False
    content_type: str = "text/html; charset=UTF-8"
    source: str = ""        # where the assumption comes from

    def sample_size(self, rng: random.Random) -> int:
        size = int(rng.lognormvariate(math.log(self.median_bytes), self.sigma))
        return max(self.min_bytes, min(self.max_bytes, size))

    def write_sizes(self, size: int) -> Optional[List[int]]:
        if self.write_chunk is None or size <= self.write_chunk:
            return None
        n, rest = divmod(size, self.write_chunk)
        sizes = [self.write_chunk] * n
        if rest:
            sizes.append(rest)
        return sizes

    def make_response(
        self,
        rng: random.Random,
        status: int = 200,
        extra_headers: Optional[List[HttpHeaderField]] = None,
        body_byte: bytes = b"x",
    ) -> UnitResponse:
        size = self.sample_size(rng)
        body = body_byte * size
        headers = [HttpHeaderField(name="Content-Type", value=self.content_type)]
        headers.extend(extra_headers or [])
        return UnitResponse(
            status=status,
            headers=headers,
            content=body,
            content_length=size,
            write_sizes=self.write_sizes(size),
            flush_header_first=self.flush_header_first,
        )
