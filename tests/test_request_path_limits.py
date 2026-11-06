"""Verification Test: Request Path Limits (freeunit issue #923, 1.37.2).

Two limits the server has or gains in the 1.37.2 bug fixes:

- a header name is at most 255 bytes on both sides of the port boundary
  (uint8_t name_length; request names are capped by the HTTP parser,
  response names are refused by each module in PR #926);
- a pass to a target the application does not have is declined and the
  request gets 404 (PR #925).

Each test also checks that the stream completes and no chunk leaks, so the
new paths keep INV-3 (no SHM leak) and INV-4 (progress).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

import pytest
from unit_model.c_types import (
    NXT_UNIT_FIELD_NAME_MAX,
    FieldNameTooLong,
    HttpHeaderField,
    UnitRequest,
    UnitResponse,
)
from unit_model.engine import (
    AppQueue,
    AppWorker,
    Listener,
    Router,
    SharedMemoryPool,
    SimEventLoop,
    UnixSocketPair,
)
from unit_model.analyzer import InvariantChecker


def make_request(app_target=0, headers=None):
    return UnitRequest(
        method="GET", version="HTTP/1.1", remote="127.0.0.1",
        local_addr="127.0.0.1", local_port="8080", tls=False,
        websocket_handshake=False, app_target=app_target,
        server_name="test.local", target="/", path="/", query="",
        headers=headers or [HttpHeaderField(name="Host", value="test.local")],
    )


def make_system():
    loop = SimEventLoop()
    shm = SharedMemoryPool(loop, max_segments=4)
    listener = Listener(loop, num_engines=1)
    router = Router(loop, listener, shm, num_engines=1)
    q = AppQueue(loop)
    sock = UnixSocketPair(loop, name="worker_sock")
    worker = AppWorker(loop, worker_id=1, app_id="app", app_queue=q,
                       socket_pair=sock, shm_pool=shm)
    router.register_worker("app", 1, q, sock)
    worker.on_send_response_to_router = router.on_worker_response_msg
    return loop, shm, listener, router, worker


def sent_statuses(loop):
    return [r.details["status"] for r in loop.get_trace() if r.action == "response_sent"]


def assert_clean(loop, shm):
    checker = InvariantChecker(shm)
    checker.verify_trace(loop.get_trace())
    assert checker.is_clean(), checker.violations
    assert checker.check_shm_leak()


@pytest.mark.parametrize("length,ok", [(255, True), (256, False), (300, False)])
def test_field_name_length_bound(length, ok):
    """serialize() refuses a name over 255 bytes on both sides instead of
    letting ctypes wrap it to len % 256."""
    name = "X" * length
    objs = (
        make_request(headers=[HttpHeaderField(name=name, value="v")]),
        UnitResponse(status=200, headers=[HttpHeaderField(name=name, value="v")]),
    )
    for obj in objs:
        if ok:
            obj.serialize()
        else:
            with pytest.raises(FieldNameTooLong):
                obj.serialize()
    assert NXT_UNIT_FIELD_NAME_MAX == 255


@pytest.mark.parametrize("length,status", [(255, 200), (256, 503), (300, 503)])
def test_response_header_name_over_255_fails_the_response(length, status):
    """The module-side check of PR #926: 255 passes, 256 and 300 give 503
    instead of a header cut to len % 256."""
    loop, shm, listener, router, worker = make_system()
    worker.handler = lambda _req: UnitResponse(
        status=200, content=b"ok",
        headers=[HttpHeaderField(name="X" * length, value="v")])

    conn = listener.accept_client()
    router.on_listener_request(0, conn, make_request(), app_id="app")
    loop.run_until_idle()

    assert sent_statuses(loop) == [status]
    actions = {r.action for r in loop.get_trace()}
    assert ("response_header_name_too_long" in actions) == (status == 503)
    assert_clean(loop, shm)


def test_missing_target_gets_404_and_completes():
    """PR #925: a pass that names a target the app does not have is declined
    with 404; a known target still runs."""
    loop, shm, listener, router, worker = make_system()
    router.set_app_targets("app", 2)
    worker.handler = lambda _req: UnitResponse(status=200, content=b"ok")

    for tgt in (2, 1, 0):
        conn = listener.accept_client()
        router.on_listener_request(0, conn, make_request(app_target=tgt), app_id="app")
    loop.run_until_idle()

    assert sorted(sent_statuses(loop)) == [200, 200, 404]
    assert "target_not_found_404" in {r.action for r in loop.get_trace()}
    # The worker never saw the declined request.
    assert worker.requests_handled == 2
    assert_clean(loop, shm)


def test_default_single_target():
    """An app without "targets" has one implicit target (index 0)."""
    loop, shm, listener, router, worker = make_system()
    conn = listener.accept_client()
    router.on_listener_request(0, conn, make_request(app_target=1), app_id="app")
    loop.run_until_idle()
    assert sent_statuses(loop) == [404]
    assert worker.requests_handled == 0
    assert_clean(loop, shm)
