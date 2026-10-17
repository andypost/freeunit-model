"""Differential & Conformance Testing Subsystem.

Provides tools for comparing behavioral equivalence between the
discrete-event simulation model and live FreeUnit C binaries.
"""

from unit_model.conformance.comparator import ComparisonResult, WireComparator
from unit_model.conformance.tandem_runner import TandemSession

__all__ = [
    "ComparisonResult",
    "WireComparator",
    "TandemSession",
]
