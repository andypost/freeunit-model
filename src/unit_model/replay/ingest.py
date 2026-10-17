"""Telemetry Ingestion Pipeline for OpenTelemetry, USDT Probes, and FreeUnit Debug Logs."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any, Dict, List, Optional, Union


@dataclass
class TraceEvent:
    """Normalized trace event representation across all telemetry sources."""

    timestamp_ns: int
    subsystem: str
    event_name: str
    stream_id: Optional[int] = None
    engine_id: Optional[int] = None
    worker_id: Optional[int] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


class TelemetryIngestor:
    """Ingests and normalizes telemetry data from multiple FreeUnit telemetry channels."""

    USDT_PATTERN = re.compile(
        r"\[(?P<ts>\d+)\]\s+freeunit:(?P<probe>\w+)(?P<attrs>.*)"
    )
    ATTR_PATTERN = re.compile(r"(\w+)=([^\s]+)")

    def ingest_otlp_json(
        self, raw_data: Union[str, Dict[str, Any], List[Any]]
    ) -> List[TraceEvent]:
        """Ingests OpenTelemetry OTLP JSON trace exports from src/nxt_otel.c and Rust otel."""
        if isinstance(raw_data, str):
            payload = json.loads(raw_data)
        else:
            payload = raw_data

        events: List[TraceEvent] = []

        # Support both raw list of spans and standard OTLP resourceSpans hierarchy
        spans: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            spans = payload
        elif isinstance(payload, dict):
            if "resourceSpans" in payload:
                for rs in payload["resourceSpans"]:
                    for ss in rs.get("scopeSpans", []):
                        spans.extend(ss.get("spans", []))
            elif "spans" in payload:
                spans = payload["spans"]
            else:
                spans = [payload]

        for s in spans:
            name = s.get("name", "request")
            start_ns = int(s.get("startTimeUnixNano", 0))
            end_ns = int(s.get("endTimeUnixNano", start_ns))

            # Parse attributes
            attrs: Dict[str, Any] = {}
            for a in s.get("attributes", []):
                k = a.get("key")
                v = a.get("value", {})
                val = next(iter(v.values())) if isinstance(v, dict) and v else v
                attrs[k] = val

            stream_id = attrs.get("unit.stream_id") or attrs.get("stream_id")
            if stream_id is not None:
                stream_id = int(stream_id)

            engine_id = attrs.get("unit.engine_id") or attrs.get("engine_id")
            if engine_id is not None:
                engine_id = int(engine_id)

            events.append(
                TraceEvent(
                    timestamp_ns=start_ns,
                    subsystem="router",
                    event_name=f"otel_{name}_start",
                    stream_id=stream_id,
                    engine_id=engine_id,
                    attributes=attrs,
                )
            )

            if end_ns > start_ns:
                events.append(
                    TraceEvent(
                        timestamp_ns=end_ns,
                        subsystem="router",
                        event_name=f"otel_{name}_end",
                        stream_id=stream_id,
                        engine_id=engine_id,
                        attributes=attrs,
                    )
                )

        return self.normalize_timeline(events)

    def ingest_usdt_lines(self, lines: List[str]) -> List[TraceEvent]:
        """Ingests USDT probe traces captured via bpftrace or perf."""
        events: List[TraceEvent] = []

        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            match = self.USDT_PATTERN.search(line)
            if not match:
                continue

            ts = int(match.group("ts"))
            probe = match.group("probe")
            attrs_str = match.group("attrs")

            attrs: Dict[str, Any] = {}
            for k, v in self.ATTR_PATTERN.findall(attrs_str):
                try:
                    attrs[k] = int(v)
                except ValueError:
                    attrs[k] = v

            stream_id = attrs.get("stream") or attrs.get("stream_id")
            engine_id = attrs.get("engine") or attrs.get("engine_id")
            worker_id = attrs.get("worker") or attrs.get("worker_id")

            subsystem = "router"
            if "queue" in probe:
                subsystem = "app_queue"
            elif "mmap" in probe or "shm" in probe:
                subsystem = "shm"
            elif "worker" in probe:
                subsystem = "worker"

            events.append(
                TraceEvent(
                    timestamp_ns=ts,
                    subsystem=subsystem,
                    event_name=f"usdt_{probe}",
                    stream_id=int(stream_id) if stream_id is not None else None,
                    engine_id=int(engine_id) if engine_id is not None else None,
                    worker_id=int(worker_id) if worker_id is not None else None,
                    attributes=attrs,
                )
            )

        return self.normalize_timeline(events)

    def ingest_debug_logs(self, lines: List[str]) -> List[TraceEvent]:
        """Ingests high-resolution FreeUnit debug log lines."""
        events: List[TraceEvent] = []
        cur_ts = 0

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Synthetic incremental timestamps if logs don't include nanoseconds
            cur_ts += 1000

            stream_id = None
            stream_match = re.search(r"stream\s+(\d+)", line)
            if stream_match:
                stream_id = int(stream_match.group(1))

            status_code = None
            status_match = re.search(r"HTTP/\d\.\d\s+(\d{3})", line)
            if status_match:
                status_code = int(status_match.group(1))

            events.append(
                TraceEvent(
                    timestamp_ns=cur_ts,
                    subsystem="log",
                    event_name="debug_log_message",
                    stream_id=stream_id,
                    attributes={"raw_line": line, "status_code": status_code},
                )
            )

        return self.normalize_timeline(events)

    def normalize_timeline(self, events: List[TraceEvent]) -> List[TraceEvent]:
        """Sorts events chronologically and sets origin timestamp t0 = 0."""
        if not events:
            return []

        sorted_events = sorted(events, key=lambda e: e.timestamp_ns)
        t0 = sorted_events[0].timestamp_ns

        normalized = []
        for e in sorted_events:
            normalized.append(
                TraceEvent(
                    timestamp_ns=e.timestamp_ns - t0,
                    subsystem=e.subsystem,
                    event_name=e.event_name,
                    stream_id=e.stream_id,
                    engine_id=e.engine_id,
                    worker_id=e.worker_id,
                    attributes=e.attributes,
                )
            )
        return normalized
