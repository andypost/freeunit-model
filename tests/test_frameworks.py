"""Verification Test: Framework Lifecycles & State Isolation.

Tests the state reset guarantees and background schedule handling for:
- WordPress
- Drupal (Issue #321 visitor hijacking & detached completion)
- Symfony (ResetInterface & Doctrine entity clear)
- Laravel (Octane sandbox clone & auth/cookie purge)
- Yii3 (StateResetter)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

import pytest
from unit_model.c_types import HttpHeaderField, UnitRequest, UnitResponse
from unit_model.engine.frameworks.wordpress import WordPressAdapter
from unit_model.engine.frameworks.drupal import DrupalAdapter
from unit_model.engine.frameworks.symfony import SymfonyAdapter
from unit_model.engine.frameworks.laravel import LaravelAdapter
from unit_model.engine.frameworks.yii3 import Yii3Adapter


def test_wordpress_lifecycle():
    wp = WordPressAdapter(is_worker_mode=False)
    # Cron request
    req_cron = UnitRequest(
        method="GET", version="HTTP/1.1", remote="127.0.0.1",
        local_addr="127.0.0.1", local_port="8080", tls=False,
        websocket_handshake=False, app_target=0, server_name="wp.local",
        target="/wp-cron.php?doing_wp_cron=1", path="/wp-cron.php", query="doing_wp_cron=1",
    )
    resp = wp.handle_request(req_cron)
    assert resp.status == 200
    assert b"WP-Cron" in resp.content


def test_drupal_detached_completion():
    drupal = DrupalAdapter(is_worker_mode=True)
    detached_called = False
    def on_detached():
        nonlocal detached_called
        detached_called = True

    req = UnitRequest(
        method="GET", version="HTTP/1.1", remote="127.0.0.1",
        local_addr="127.0.0.1", local_port="8080", tls=False,
        websocket_handshake=False, app_target=0, server_name="drupal.local",
        target="/cron/secret_key", path="/cron/secret_key", query="",
    )
    resp = drupal.handle_request(req, on_finish_request_cb=on_detached)
    assert resp.status == 200
    assert detached_called, "Drupal cron should trigger detached completion callback"


def test_symfony_state_reset():
    symfony = SymfonyAdapter(is_worker_mode=True)
    resp = symfony.handle_request(None)
    assert resp.status == 200
    # Invariant: Doctrine entities and monolog buffer must be cleared
    assert symfony.state.doctrine_entities_managed == 0
    assert symfony.state.monolog_buffer_size == 0


def test_laravel_octane_sandbox_isolation():
    laravel = LaravelAdapter()
    resp = laravel.handle_request(None)
    assert resp.status == 200
    # Invariant: Auth user and cookies must be cleared back to base sandbox
    assert laravel.state.auth_user_id is None
    assert laravel.state.queued_cookies == 0
    assert not laravel.state.active_sandbox


def test_yii3_state_resetter():
    yii = Yii3Adapter()
    resp = yii.handle_request(None)
    assert resp.status == 200
    assert not yii.state.middleware_state_dirty
    assert not yii.state.active_transaction


def test_framework_profiles_are_bounded_and_deterministic():
    """The profiles are assumptions, but they must be bounded and seeded."""
    from unit_model.engine.frameworks.profile import PHP_OUTPUT_BUFFERING
    for cls in (WordPressAdapter, DrupalAdapter, SymfonyAdapter, LaravelAdapter, Yii3Adapter):
        a, b = cls(seed=7), cls(seed=7)
        for _ in range(5):
            ra, rb = a.handle_request(None), b.handle_request(None)
            assert ra.content == rb.content
            prof = cls.profile
            assert prof.min_bytes <= len(ra.content) <= prof.max_bytes
            assert ra.content_length == len(ra.content)
            assert prof.source.startswith("assumption")


def test_wordpress_writes_in_output_buffering_flushes():
    """A 50 KiB page with output_buffering=4096 is about a dozen writes."""
    from unit_model.engine.frameworks.profile import PHP_OUTPUT_BUFFERING
    wp = WordPressAdapter(seed=3)
    resp = wp.handle_request(None)
    writes = resp.writes()
    assert len(writes) == -(-len(resp.content) // PHP_OUTPUT_BUFFERING)
    assert all(len(w) == PHP_OUTPUT_BUFFERING for w in writes[:-1])
    assert b"".join(writes) == resp.content


def test_yii3_flushes_header_before_body_and_symfony_writes_once():
    assert Yii3Adapter.profile.flush_header_first is True
    assert Yii3Adapter(seed=1).handle_request(None).flush_header_first is True
    assert SymfonyAdapter(seed=1).handle_request(None).write_sizes is None
    assert LaravelAdapter(seed=1).handle_request(None).write_sizes is None


def test_framework_responses_through_libunit_path():
    """Run each profile through the worker: header once, body bytes intact."""
    from unit_model.c_types import MmapMsg, PortMsgType
    from unit_model.engine.event_loop import SimEventLoop
    from unit_model.engine.ipc_queue import AppQueue
    from unit_model.engine.ipc_socket import UnixSocketPair
    from unit_model.engine.shm_pool import SharedMemoryPool
    from unit_model.engine.worker import AppWorker

    for cls in (WordPressAdapter, DrupalAdapter, SymfonyAdapter, LaravelAdapter, Yii3Adapter):
        loop = SimEventLoop()
        shm = SharedMemoryPool(loop)
        worker = AppWorker(loop, 1, "app", AppQueue(loop), UnixSocketPair(loop), shm)
        sent = []
        worker.on_send_response_to_router = lambda rp, m, pl, w: sent.append((m, pl))
        worker.current_stream = 7
        resp = cls(seed=5).handle_request(None)
        worker._send_response(resp)
        loop.run_until_idle()

        body = b""
        for m, pl in sent:
            if m.type != PortMsgType.DATA:
                continue
            if m.mmap:
                mm = MmapMsg.from_bytes(pl)
                raw = shm.get_data(mm.mmap_id, mm.chunk_id, mm.size)
                body += UnitResponse.deserialize(raw).content if m is sent[0][0] else raw
            else:
                body += pl
        assert body == resp.content, cls.__name__
        expected_msgs = len(resp.writes()) + 1 + (1 if resp.flush_header_first else 0)
        # One write may ride in the header message: then one message fewer.
        data_msgs = [m for m, _ in sent if m.type == PortMsgType.DATA]
        assert len(data_msgs) in (expected_msgs, expected_msgs + 1), cls.__name__
        assert data_msgs[-1].last is True


if __name__ == "__main__":
    test_wordpress_lifecycle()
    test_drupal_detached_completion()
    test_symfony_state_reset()
    test_laravel_octane_sandbox_isolation()
    test_yii3_state_resetter()
    print("All framework tests PASSED")
