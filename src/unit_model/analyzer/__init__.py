"""FreeUnit Adversarial Invariant Analyzer (Model B).

Exports:
- InvariantChecker, InvariantViolation
- AdversarialInterleaver
- ChaosFaultInjector
- StateSpaceExplorer, ExplorationResult
"""

from unit_model.analyzer.explorer import ExplorationResult, StateSpaceExplorer
from unit_model.analyzer.fault_injector import ChaosFaultInjector
from unit_model.analyzer.interleaver import AdversarialInterleaver
from unit_model.analyzer.invariants import InvariantChecker, InvariantViolation

__all__ = [
    "InvariantChecker",
    "InvariantViolation",
    "AdversarialInterleaver",
    "ChaosFaultInjector",
    "StateSpaceExplorer",
    "ExplorationResult",
]
