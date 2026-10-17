"""Hypothesis Stateful Model-Based Testing for FreeUnit Model."""

import pytest
from unit_model.verify.state_machine import HAVE_HYPOTHESIS, FreeUnitStateMachine

if HAVE_HYPOTHESIS:
    from hypothesis import HealthCheck, settings

    TestFreeUnitStateMachine = FreeUnitStateMachine.TestCase
    TestFreeUnitStateMachine.settings = settings(
        max_examples=30,
        stateful_step_count=15,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
else:

    @pytest.mark.skip(reason="hypothesis not installed")
    def test_hypothesis_skipped():
        pass
