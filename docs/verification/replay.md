# Production Telemetry & Deterministic Trace Replay

The replay subsystem (`src/unit_model/replay/`) transforms real-world production telemetry into deterministic, reproducible discrete-event simulation runs, enabling post-mortem root-cause analysis and automatic synthesis of standalone C regression tests.

---

## 1. Pipeline Architecture

```mermaid
flowchart LR
    subgraph SOURCES["Production Telemetry Inputs"]
        OTEL["OpenTelemetry Spans<br/>(OTLP JSON / Protobuf)"]
        USDT["USDT Probes<br/>(bpftrace / perf)"]
        LOGS["FreeUnit Debug Logs<br/>(--log-level=debug)"]
    end

    subgraph INGEST["Telemetry Ingestion"]
        NORM["Timeline Normalizer<br/>(t0 = 0, Monotonic Clock)"]
    end

    subgraph REPLAY["Deterministic Simulation"]
        LOOP["SimEventLoop Virtual Clock"]
        EXEC["DeterministicReplayer Engine"]
    end

    subgraph DIAGNOSTICS["Root-Cause Diagnostics"]
        WINDOW["Critical Race Window Isolator"]
        LITMUS["C Litmus Repro Generator<br/>(src/test/nxt_*_repro_test.c)"]
    end

    OTEL --> NORM
    USDT --> NORM
    LOGS --> NORM
    NORM --> LOOP
    LOOP --> EXEC
    EXEC --> WINDOW
    WINDOW --> LITMUS
```

---

## 2. Ingestion Formats (`TelemetryIngestor`)

- **OpenTelemetry (OTLP)**: Ingests spans exported by `src/nxt_otel.c` and `src/otel/src/lib.rs`, extracting attributes (`http.status_code`, `http.method`, `unit.stream_id`, `unit.engine_id`) and nanosecond timestamps.
- **USDT DTrace / eBPF Probes**: Parses kernel/user probe trace outputs from `freeunit:queue__enqueue`, `freeunit:mmap__alloc`, `freeunit:worker_crash`.
- **Debug Logs**: Extracts stream IDs, HTTP statuses, and sequential transitions from FreeUnit debug outputs.

---

## 3. Deterministic Replay (`DeterministicReplayer`)

Maps production events to virtual clock ticks, reproducing interleavings that triggered historical bugs (such as #195 SIGKILL double-free, #573 dual-channel inversion, or #708 doorbell starvation).

```python
from unit_model.replay import TelemetryIngestor, DeterministicReplayer

ingestor = TelemetryIngestor()
events = ingestor.ingest_otlp_json(raw_spans_json)

replayer = DeterministicReplayer(num_engines=2, num_workers=4)
replayer.load_events(events)
summary = replayer.replay_all()

print(f"Replayed {summary.replayed_events} events, detected {len(summary.anomalies)} anomalies")
```

---

## 4. Root-Cause Analysis & C Litmus Synthesis (`RootCauseAnalyzer`)

Upon detecting an anomaly, the analyzer isolates the narrow temporal window ($\pm 20\,\mu\text{s}$) surrounding the event and synthesizes an executable C test case:

```python
from unit_model.replay import RootCauseAnalyzer

analyzer = RootCauseAnalyzer(summary, events)
critical_events = analyzer.isolate_critical_race_window()
c_test = analyzer.generate_c_litmus_test("nxt_race_repro_test", "dual_channel_inversion")
```
