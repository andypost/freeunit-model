"""Tests for K1 Doorbell Mathematical Proof and Combinatorial Explorer."""

import pytest

from unit_model.synthesis.k1_doorbell_proof import K1DoorbellProof, ProofResult


def test_k1_notify_decision_logic():
    proof = K1DoorbellProof()

    # When pos < 0: race condition where head moved past producer, must ring!
    assert proof.notify_decision(pos=-1, depth_bound=4) is True
    assert proof.notify_decision(pos=-10, depth_bound=4) is True

    # When 0 <= pos < depth_bound: within concurrency window, ring!
    assert proof.notify_decision(pos=0, depth_bound=4) is True
    assert proof.notify_decision(pos=1, depth_bound=4) is True
    assert proof.notify_decision(pos=3, depth_bound=4) is True

    # When pos >= depth_bound: sufficient doorbells already in flight, do NOT ring!
    assert proof.notify_decision(pos=4, depth_bound=4) is False
    assert proof.notify_decision(pos=10, depth_bound=4) is False


def test_k1_herd_elimination_proof():
    proof = K1DoorbellProof()

    # Burst of 8 on 4 workers
    res = proof.prove_herd_elimination(burst_size=8, depth_bound=4)
    assert res["bound_satisfied"] is True
    assert res["theoretical_doorbells"] == 4
    assert res["simulated_doorbells"] == 4
    assert res["legacy_broadcast_wakeups"] == 32
    assert res["savings_vs_broadcast_pct"] > 80.0

    # Burst of 2 on 4 workers
    res_small = proof.prove_herd_elimination(burst_size=2, depth_bound=4)
    assert res_small["bound_satisfied"] is True
    assert res_small["simulated_doorbells"] == 2


def test_k1_concurrent_burst_simulation():
    proof = K1DoorbellProof()

    # Burst of 16 items with 4 workers and late head samples
    sim = proof.simulate_concurrent_burst(
        burst_size=16,
        num_workers=4,
        worker_service_time=2,
        producer_jitter_max=1,
        interleaved_worker_step=True,
    )
    assert sim["lost_wakeups"] == 0
    assert sim["all_processed"] is True
    assert sim["items_processed"] == 16


def test_k1_worker_crash_recovery():
    proof = K1DoorbellProof()

    # Burst of 12 items with 4 workers, where worker 0 dies mid-flight
    crash_sim = proof.simulate_worker_crash(
        burst_size=12,
        num_workers=4,
        crash_worker_id=0,
    )
    assert crash_sim["queue_cleared"] is True
    assert crash_sim["surviving_workers"] == 3
    assert crash_sim["remaining_items"] == 0


def test_k1_combinatorial_verification_full():
    proof = K1DoorbellProof()
    result = proof.run_combinatorial_verification(
        worker_counts=[1, 2, 4],
        burst_sizes=[1, 4, 16],
        permutations_per_case=5,
    )
    assert result.verified is True
    assert result.lost_wakeups == 0
    assert result.starvation_events == 0
    assert result.total_scenarios > 0
