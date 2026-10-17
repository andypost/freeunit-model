"""Synthesis & Mathematical Proof of High-Performance C Optimizations.

Provides formal proofs and executable algorithmic models for:
- k1_doorbell_proof: Bounded doorbell notification and baton-pass starvation immunity.
- lockfree_shm_synth: Per-engine lock-free SHM slab allocation eliminating global mmaps->mutex contention.
"""

from unit_model.synthesis.k1_doorbell_proof import K1DoorbellProof, ProofResult
from unit_model.synthesis.lockfree_shm_synth import LockFreeShmPool, PerEngineSlab

__all__ = [
    "K1DoorbellProof",
    "ProofResult",
    "LockFreeShmPool",
    "PerEngineSlab",
]
