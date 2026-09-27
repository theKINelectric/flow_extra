# C — admission and overload design (FX-001)

Design record, written before implementation per Astra's consult ruling (codex session `01a0e072`, 2026-09-26/27). Work package C.

## The contract (first version)

- **Admission is acknowledged and bounded.** A submission either acquires a permit (admitted: the work WILL be accounted to a terminal outcome) or is refused immediately and observably. There is no waiting queue for capacity and no silent drop.
- **The bound counts admitted work, in flight.** `admission_capacity` (pipeline option, default 100 — deliberately far below GenStage's 10_000-event buffer) bounds queued + executing packets together. A count bound is not a byte bound: callers assume normal-sized payloads; a payload envelope is future credit work.
- **Permits are held for the work's whole lifetime.** A caller's timeout or death does not release a permit — execution continues and the permit is released at the pipeline's terminal observation.
- **Every admitted job reaches exactly one terminal outcome.** The reconciliation law, with v1's observable outcomes:

  ```text
  admitted = active (queued + executing) + succeeded + recovered + expired
             + failed + cancelled + unknown
  ```

  v1 mappings, stated honestly: a packet reaching the consumer counts `succeeded` (no error), `recovered` (error handled by the error pipe — a completed result), or `expired` (deadline died before a stage could begin; the reply, if any, is dropped — whether callbacks had begun is not recorded in v1). `failed` and `cancelled` exist in the ledger and stay zero in v1: nothing produces a definite non-recovered failure or a proven before-any-callback removal yet. A topology failure marks all of that generation's outstanding work `unknown` — no invented outcomes. Waiter timeout and caller death are observations about the caller, not terminal execution states.

## The mechanism (Astra's ruling, concretized)

- **An admission owner outside the replaceable subtree.** The pipeline supervisor becomes a `:rest_for_one` wrapper whose children are `[admission owner, line supervisor]` — the owner first, the producer/stages/consumer line second. Owner death restarts the owner *and tears down the whole line* before a fresh owner can reopen capacity, so a lost ledger can never overlap still-executing old work (the old work is dead by construction). Line-internal restarts (a stage crash contained by the line's own `:rest_for_one`) leave the owner alive.
- **The owner watches the workers.** It monitors every line worker (producer, stage replicas, consumer) by resolving their registry names. Any worker death is a topology failure: the owner quiesces the generation — all outstanding permits become `unknown`, capacity is reclaimed, the generation counter advances, and admission reopens only after every name resolves again (fresh incarnation). Stale releases from dead generations are refused (`{:error, :stale}`), so a late completion cannot corrupt fresh accounting or double-release capacity.
- **A bounded set of active request IDs, not a counter.** Permits are keyed by the packet's reference and tagged with their generation; release pops exactly once.
- **The producer keeps an explicitly bounded queue drained by accumulated demand** — no reliance on GenStage's keep-last event buffer. The queue can never exceed capacity because admission gates everything that reaches it.
- **All async submission paths admit.** `call/3` raises `Flowex.AdmissionError` on refusal (the request was never accepted — a policy outcome, not a pipeline failure). `cast/2` returns `{:error, :overloaded | :unavailable}` instead of a meaningless `:ok` — the protocol change Astra sanctioned, closing the audit's "caller receives :ok for discarded work". A brief bounded retry (250ms) heals the settling window after topology failures so honest restarts do not surface as refusals.
- **The sync engine is deliberately unadmitted.** Its queue is its GenServer mailbox, which cannot be bounded from inside; v1 documents it as the single-process debug engine (parity boundary: concurrency and process identity may differ). Bounding sync ingress is future caller-credit work.

## Capacity ownership across restart — the proof obligations

Astra's decisive tests, all adopted: hold exactly K and refuse K+1; complete one and admit one; a timed-out caller's executing work keeps its permit; a replayed completion cannot double-release; kill the consumer under load (quiesce to `unknown`, reopen, no overlap); kill the owner under load (subtree torn down, fresh ledger, old work provably dead); and the audit's reconciled overload — N submissions become exactly (admitted → all terminal) + (refused → observable), with nothing silently dropped.

## Implementation surface

`Flowex.Admission` (owner GenServer + admit/release/report API), `Flowex.AdmissionError`, producer queue, consumer release-and-reply, `owner_name` on `Flowex.Pipeline`, `:admission_capacity` pipeline option, two new name roles (`:admission_owner`, `:line`). The wrapper reuses `Flowex.Supervisor` (`:rest_for_one`).

## Implementation record (2026-09-27)

All of Astra's decisive tests pass (`test/flowex/admission/overload_test.exs`, 7/7): hold-K-refuse-K+1; complete-one-admit-one; a timed-out caller's executing work keeps its permit (its outcome is `:expired` per the v1 mapping — the reply dropped at a deadline-noticing hop); replayed/stale completions refused; killing the consumer quiesces to `unknown` with stale releases unable to touch the fresh generation; killing the owner tears down the line (the wrapper's `:rest_for_one`) and reopens clean, the old work provably dead; and the audit's reconciled overload — 100 submissions become exactly 20 admitted + 80 refused, with all 20 accounted and nothing silently dropped (against the audit's 10,020 in / 10,001 run / 19 vanished).

Two findings from the build worth recording:

- **`Flowex.Names.await_free!/2` exists because of an OTP-level restart race.** A supervisor-driven restart races the old generation's asynchronous death cascade: a freshly started child registering a still-held name of a dying process adopts that dead pid, and the cascade of adopt-and-DOWN burns restart intensity at every level, killing the owning parent. Reproduced with pure OTP code (no Flowex) — `:rest_for_one` outer supervisor containing a nested supervisor child, `Process.exit(outer, :kill)`, parent dies `:shutdown` non-deterministically. Every Flowex process now waits for its own name to clear before starting: zero cost when there is no stale name, deterministic restarts when there is.
- **The consumer's outcome mapping needed the nil guard**: `%{error: _}` matches `error: nil`, which made every healthy packet count as `:recovered` until an explicit `%{error: nil}` clause ordered before it.

Ledger honesty note: `failed` and `cancelled` exist in the ledger and stay zero in v1 (nothing produces a definite non-recovered failure reaching the consumer, and nothing cancels admitted work before callbacks). `Flowex.Admission.report/1` exposes `active` as one bucket (queued + executing); the queued/executing split is future stage instrumentation.

Commands: `mix test --seed 0/7/77/12345` → 103 passed each; `mix format --check-formatted`, `mix compile --force --warnings-as-errors`, `mix dialyzer` (0), `mix credo --strict` (0) clean. Runtime: Elixir 1.20.4 / OTP 29.
