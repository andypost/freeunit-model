"""Production Telemetry Ingestion & Deterministic Trace Replayer.

Enables replaying real-world OpenTelemetry spans, USDT probes, and debug logs
inside the discrete-event simulation engine to reproduce and isolate concurrency
anomalies, and synthesize standalone C litmus repro tests.
"""

from unit_model.replay.analyzer import RootCauseAnalyzer
from unit_model.replay.ingest import TelemetryIngestor, TraceEvent
from unit_model.replay.replayer import DeterministicReplayer, ReplaySummary

__all__ = [
    "TraceEvent",
    "TelemetryIngestor",
    "DeterministicReplayer",
    "ReplaySummary",
    "RootCauseAnalyzer",
]
