"""Wire and State Differential Comparator for FreeUnit Conformance.

Compares output responses and state transitions between the discrete-event
simulation oracle and live FreeUnit C server executions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from unit_model.c_types import HttpHeaderField, UnitResponse


@dataclass
class ComparisonResult:
    """Outcome of a differential comparison between Model and Live Target."""

    matches: bool
    status_match: bool = True
    headers_match: bool = True
    body_match: bool = True
    framing_match: bool = True
    divergences: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


class WireComparator:
    """Compares HTTP responses for bit-exact and semantic equivalence."""

    DEFAULT_IGNORED_HEADERS: Set[str] = {
        "date",
        "server",
        "x-request-id",
        "last-modified",
        "etag",
    }

    def __init__(self, ignored_headers: Optional[Set[str]] = None) -> None:
        self.ignored_headers = (
            {h.lower() for h in ignored_headers}
            if ignored_headers is not None
            else self.DEFAULT_IGNORED_HEADERS
        )

    def compare_responses(
        self,
        oracle_resp: UnitResponse,
        live_resp: UnitResponse,
        oracle_framing: Optional[str] = None,
        live_framing: Optional[str] = None,
    ) -> ComparisonResult:
        """Compare a simulated response against a live C server response."""
        divergences: List[str] = []
        status_match = oracle_resp.status == live_resp.status
        if not status_match:
            divergences.append(
                f"Status mismatch: Oracle={oracle_resp.status} vs Live={live_resp.status}"
            )

        # Header normalization and comparison
        oracle_hdrs = self._normalize_headers(oracle_resp.headers)
        live_hdrs = self._normalize_headers(live_resp.headers)
        headers_match = True

        all_keys = set(oracle_hdrs.keys()) | set(live_hdrs.keys())
        for key in all_keys:
            if key in self.ignored_headers:
                continue
            val_oracle = oracle_hdrs.get(key)
            val_live = live_hdrs.get(key)
            if val_oracle != val_live:
                headers_match = False
                divergences.append(
                    f"Header '{key}' mismatch: Oracle='{val_oracle}' vs Live='{val_live}'"
                )

        # Body comparison
        body_match = oracle_resp.content == live_resp.content
        if not body_match:
            divergences.append(
                f"Body mismatch: len(Oracle)={len(oracle_resp.content)} vs len(Live)={len(live_resp.content)}"
            )

        # Framing sequence comparison (e.g. K2 single framing 'ABL' vs baseline 'ABBL')
        framing_match = True
        if oracle_framing is not None and live_framing is not None:
            framing_match = oracle_framing == live_framing
            if not framing_match:
                divergences.append(
                    f"Framing sequence mismatch: Oracle='{oracle_framing}' vs Live='{live_framing}'"
                )

        matches = status_match and headers_match and body_match and framing_match
        return ComparisonResult(
            matches=matches,
            status_match=status_match,
            headers_match=headers_match,
            body_match=body_match,
            framing_match=framing_match,
            divergences=divergences,
            details={
                "oracle_status": oracle_resp.status,
                "live_status": live_resp.status,
                "oracle_body_len": len(oracle_resp.content),
                "live_body_len": len(live_resp.content),
            },
        )

    def _normalize_headers(self, headers: List[HttpHeaderField]) -> Dict[str, str]:
        normalized: Dict[str, str] = {}
        for h in headers:
            key = h.name.lower().strip()
            val = h.value.strip()
            if key in normalized:
                normalized[key] += f", {val}"
            else:
                normalized[key] = val
        return normalized
