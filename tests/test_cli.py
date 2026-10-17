"""Unit tests for the unit_model CLI commands."""

import argparse
import pytest

from unit_model.cli import (
    cmd_benchmark_optimizations,
    cmd_conformance,
    cmd_lockfree_shm,
    cmd_prove_k1,
    cmd_replay,
    cmd_simulate,
    cmd_verify,
)


def test_cli_simulate():
    args = argparse.Namespace(requests=5, workers=2, engines=2)
    rc = cmd_simulate(args)
    assert rc == 0


def test_cli_verify():
    args = argparse.Namespace(chaos=False, seed=42)
    rc = cmd_verify(args)
    assert rc == 0


def test_cli_benchmark_optimizations():
    args = argparse.Namespace()
    rc = cmd_benchmark_optimizations(args)
    assert rc == 0


def test_cli_prove_k1():
    args = argparse.Namespace(workers=4, permutations=2)
    rc = cmd_prove_k1(args)
    assert rc == 0


def test_cli_lockfree_shm():
    args = argparse.Namespace(engines=2, threads=4, ops=20)
    rc = cmd_lockfree_shm(args)
    assert rc == 0


def test_cli_replay():
    args = argparse.Namespace(trace=None, emit_litmus=True)
    rc = cmd_replay(args)
    assert rc == 0


def test_cli_conformance():
    args = argparse.Namespace(url=None)
    rc = cmd_conformance(args)
    assert rc == 0
