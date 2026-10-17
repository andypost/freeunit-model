"""Unit and Conformance Tests for WireComparator and TandemSession."""

import pytest

from unit_model.c_types import HttpHeaderField, UnitRequest, UnitResponse
from unit_model.conformance.comparator import ComparisonResult, WireComparator
from unit_model.conformance.tandem_runner import TandemSession


def test_wire_comparator_exact_match():
    comparator = WireComparator()
    r1 = UnitResponse(
        status=200,
        headers=[
            HttpHeaderField(name="Content-Type", value="text/plain"),
            HttpHeaderField(name="Date", value="Sat, 17 Oct 2026 00:00:00 GMT"),
        ],
        content=b"Hello World",
    )
    r2 = UnitResponse(
        status=200,
        headers=[
            HttpHeaderField(name="Content-Type", value="text/plain"),
            HttpHeaderField(name="Date", value="Sat, 17 Oct 2026 00:01:00 GMT"),  # Ignored
        ],
        content=b"Hello World",
    )

    result = comparator.compare_responses(r1, r2)
    assert result.matches is True
    assert result.status_match is True
    assert result.headers_match is True
    assert result.body_match is True
    assert len(result.divergences) == 0


def test_wire_comparator_divergences():
    comparator = WireComparator()
    r1 = UnitResponse(
        status=200,
        headers=[HttpHeaderField(name="Content-Type", value="text/plain")],
        content=b"Hello World",
    )
    r2 = UnitResponse(
        status=404,
        headers=[HttpHeaderField(name="Content-Type", value="text/html")],
        content=b"Not Found Here",
    )

    result = comparator.compare_responses(
        r1, r2, oracle_framing="ABL", live_framing="ABBL"
    )
    assert result.matches is False
    assert result.status_match is False
    assert result.headers_match is False
    assert result.body_match is False
    assert result.framing_match is False
    assert len(result.divergences) == 4


def test_tandem_session_model_execution():
    session = TandemSession()

    # Health check
    req = UnitRequest(method="GET", path="/health")
    resp = session.execute_in_model(req)
    assert resp.status == 200
    assert resp.content == b"OK"

    # Echo POST
    post_req = UnitRequest(
        method="POST", path="/echo", preread_content=b"payload-12345"
    )
    post_resp = session.execute_in_model(post_req)
    assert post_resp.status == 200
    assert post_resp.content == b"payload-12345"

    # 404 Not Found
    nf_req = UnitRequest(method="GET", path="/not-found")
    nf_resp = session.execute_in_model(nf_req)
    assert nf_resp.status == 404


def test_tandem_session_equivalence():
    session = TandemSession()
    req = UnitRequest(method="GET", path="/health")

    # Reference response matching oracle
    ref_resp = UnitResponse(
        status=200,
        headers=[HttpHeaderField(name="Content-Type", value="text/plain")],
        content=b"OK",
    )

    result = session.verify_equivalence(req, reference_response=ref_resp)
    assert result.matches is True
