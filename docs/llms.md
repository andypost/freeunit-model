# FreeUnit Model - LLM & Machine Reference (`llms.txt`)

> This file follows the [`llms.txt`](https://llmstxt.org/) format to provide AI agents, automated reasoners, and bots with an index and summary of the `freeunit-model` project.

## Project Summary
`freeunit-model` is a formal mathematical, discrete-event simulation, and adversarial verification engine for FreeUnit (NGINX Unit), a high-performance multi-threaded C application server. It models the asynchronous dual-channel IPC (shared-memory ring buffer vs Unix datagram socket), shared-memory 16 KiB chunk allocator under `mmaps->mutex`, Out-Of-Shared-Memory (OOSM) backpressure, web framework worker runtimes, and proves 6 core concurrency invariants.

## Core Documentation Index

- [System Architecture](architecture/overview.md): High-level topology of Supervisor, Controller, Router multi-threaded engines, IPC channels, and Worker process pools.
- [Dual-Channel IPC & Invariants](architecture/dual-channel-ipc.md): Concurrency analysis of `nxt_app_queue_t` vs Unix datagram sockets, message inversion race conditions (issues #573, #597), and queue drain barriers.
- [C Schema & ABI Layouts](architecture/c-schema.md): `tree-sitter-c` AST extractor and `ctypes` memory layout mirrors matching GCC Linux x86_64 ABI bit-for-bit.
- [Model A: Discrete-Event Simulation](models/model-a-simulation.md): Virtual clock (`SimEventLoop`), priority queue event dispatch, and framework adapters for WordPress, Drupal, Symfony, Laravel Octane, and Yii3.
- [Model B: Adversarial Verification](models/model-b-verification.md): Combinatorial timing jitter interleaver, chaos fault injector (worker `SIGKILL`, OOSM starvation, client RST), and the 6 core invariants (INV-1 to INV-6).
- [Micro-Optimizations & Benchmarks](benchmarks/optimizations.md): K1 single-wake doorbell herd elimination (99.9% fewer IPC context switches) and K2 single-buffer response framing (50% fewer syscalls/allocations).
- [Strategic Roadmap](roadmap.md): Multi-phase research and engineering roadmap spanning formal TLA+ specifications, Hypothesis model-based testing, differential testing against live FreeUnit, and telemetry replay.

## Key Source Files
- `src/unit_model/c_types.py`: Bit-accurate ctypes struct mirrors (`nxt_port_msg_t`, `nxt_app_queue_item_t`, `nxt_unit_request_t`, `nxt_unit_response_t`).
- `src/unit_model/ast_extractor.py`: Tree-sitter C AST parser extracting wire constants, enums, and struct fields from FreeUnit C source.
- `src/unit_model/engine/event_loop.py`: Discrete-event virtual clock with nanosecond priority scheduling.
- `src/unit_model/engine/ipc_queue.py`: Lockless circular ring buffer with atomic `notified` cmp-set flag.
- `src/unit_model/engine/shm_pool.py`: 16 KiB chunk allocator with `mmaps->mutex` and OOSM backpressure.
- `src/unit_model/analyzer/invariants.py`: Invariant verification engine evaluating the 6 core invariants.
- `src/unit_model/analyzer/interleaver.py`: Adversarial dual-channel timing jitter injector.
- `src/unit_model/analyzer/fault_injector.py`: Chaos injector simulating worker crashes and memory starvation.
