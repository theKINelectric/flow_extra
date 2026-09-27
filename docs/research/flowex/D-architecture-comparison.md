# D — architectural comparison

Work package D record, 2026-09-27. Reproduce with `MIX_ENV=test mix run scripts/bench_comparison.exs` (3 trials, medians; async-engine calls driven through the same `Task.async_stream` concurrency as the task-stream executor, so both face an equal caller population).

## Results (Elixir 1.20.4 / OTP 29, 12 schedulers, items/s, medians)

| workload | direct | task_stream | sync engine | async engine |
| --- | --- | --- | --- | --- |
| cheap — three tiny pure stages (N=1500) | **5,263,158** | 195,899 | 313,742 | 29,969 |
| cpu — one ~1.6ms CPU stage, single replica (N=240) | 632 | **2,733** | 547 | 605 |
| cpu8 — the same CPU stage, `count: 8` (N=240) | 616 | 2,585 | — | **3,193** |
| uneven — 1ms/20ms/1ms stages, slow stage `count: 10` (N=150) | 48 | 476 | 48 | **473** |
| burst — 2ms jobs from 20 concurrent callers (N=400) | 333 | **6,660** | 332 | 6,366 |

Direct and sync are serial on the uneven/burst rows by architecture — that difference is part of what is measured, not a driver artifact.

## What this establishes (synthetic evidence)

1. **Replicated bottleneck stages earn the architecture.** With the CPU stage at `count: 8`, the async engine beats a tuned task stream (3,193 vs 2,585 items/s); on uneven blocking-IO work it is at parity (473 vs 476); on a 20-caller burst, near-parity (6,366 vs 6,660, ~5% behind). Stage separation plus replica counts delivers the concurrency a task stream gives, with the railway's error track, admission accounting, and restart policy on top.
2. **An unreplicated expensive stage serializes the chain.** The single-replica CPU row (605 items/s vs 2,733 for the task stream) is the cost of forgetting to scale the bottleneck: transport and one-stage-at-a-time demand cap the pipeline at near-serial throughput. Declaring the count is the whole game.
3. **Cheap pure callbacks belong outside the pipeline.** Three tiny stages run at ~30K items/s through the async engine against 5.3M for direct composition — a ~170× transport tax, exactly the case GenStage's own usage guidance warns about. Cheap callbacks should be grouped into fewer stages or composed directly; mirroring every function boundary with a process is not free.
4. **The sync engine behaves as documented** — a debug engine at near-direct speed (a small GenServer tax), serial by construction.

## What this does not establish

These are synthetic workloads on one machine: sleeps proxy blocking IO without its variance, CPU grinding lacks real cache/allocation behavior, and only wall-clock throughput was recorded (no p50/p95/p99 latency, no memory/process counts, no offered-vs-admitted-vs-rejected rates under sustained load). Per the pursuit's bar, a representative consumer workload is required before any production recommendation; the decision enabled here is the narrower one above — which executor class fits which workload shape, pending that representative validation.

## Decision (interim, evidence-backed)

Keep stage separation for pipelines whose bottleneck stages are genuinely expensive (CPU or blocking IO) and replicated to match; do not route cheap pure transforms through the process network. The final architectural qualification — retain, simplify, or replace Flowex for the Factory's real workloads — awaits the representative consumer workload the pursuit names.
