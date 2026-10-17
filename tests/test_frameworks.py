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


if __name__ == "__main__":
    test_wordpress_lifecycle()
    test_drupal_detached_completion()
    test_symfony_state_reset()
    test_laravel_octane_sandbox_isolation()
    test_yii3_state_resetter()
    print("All framework tests PASSED")
