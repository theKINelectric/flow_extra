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

  v1 mappings, stated honestly: a packet reaching the consumer counts `succeeded` (no error), `recovered` (error handled by the error pipe — a completed result), or `expired` (deadline died before a stage could begin; the reply, if any, is dropped — whether callbacks had begun is not recorded in v1). `failed` and `cancelled` exist in the ledger and stay zero in v1: nothing produces a definite non-recovered failure or a proven before-any-callback removal yet. **Revised 2026-09-27:** a topology failure no longer moves outstanding permits into `unknown` — that mass-classification released capacity while callbacks could still be executing, which the independent validation proved as a violation. Quiesced work that survives completes and counts its real terminal outcome; quiesced work destroyed with the topology stays `active`, outcome unknown — never invented, never quietly balanced away. Waiter timeout and caller death are observations about the caller, not terminal execution states.

## The mechanism (Astra's ruling, concretized)

- **An admission owner outside the replaceable subtree.** The pipeline supervisor becomes a `:rest_for_one` wrapper whose children are `[admission owner, line supervisor]` — the owner first, the producer/stages/consumer line second. Owner death restarts the owner *and tears down the whole line* before a fresh owner can reopen capacity, so a lost ledger can never overlap still-executing old work (the old work is dead by construction). Line-internal restarts (a stage crash contained by the line's own `:rest_for_one`) leave the owner alive.
- **The owner watches the workers.** It monitors every line worker (producer, stage replicas, consumer) by resolving their registry names. Any worker death is a topology failure: the owner quiesces the generation — the generation counter advances, and admission reopens only after every name resolves again (fresh incarnation). **Revised 2026-09-27 after independent validation:** quiesce RETAINS the generation's permits — surviving work keeps executing and keeps its capacity until its own terminal release, so the reopened generation can never double-book a slot under still-executing old work. Releases are ref-keyed exactly-once in any generation; replayed or unknown releases are refused (`{:error, :stale}`).
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

- **`Flowex.Names.await_free!/2` exists because of an OTP-level restart race** — retained as a *scoped observation*. A supervisor-driven restart races the old generation's asynchronous death cascade: a freshly started child registering a still-held name of a dying process adopts that dead pid, and the cascade of adopt-and-DOWN burns restart intensity at every level, killing the owning parent. It was bisected with throwaway pure-OTP probes (no Flowex; `:rest_for_one` outer supervisor containing a nested supervisor child, `Process.exit(outer, :kill)`, parent dying `:shutdown` non-deterministically) that were NOT retained in this repository; the closure review therefore qualifies the claim: the mechanism is observed and the guard is cheap and harmless (zero cost with no stale name), but end-to-end restart determinism is not separately proven from the polling helper alone. An in-repo reproducer remains open work.
- **The consumer's outcome mapping needed the nil guard**: `%{error: _}` matches `error: nil`, which made every healthy packet count as `:recovered` until an explicit `%{error: nil}` clause ordered before it.

Ledger honesty note: `failed` and `cancelled` exist in the ledger and stay zero in v1 (nothing produces a definite non-recovered failure reaching the consumer, and nothing cancels admitted work before callbacks). `Flowex.Admission.report/1` exposes `active` as one bucket (queued + executing); the queued/executing split is future stage instrumentation.

Commands: `mix test --seed 0/7/77/12345` → 103 passed each; `mix format --check-formatted`, `mix compile --force --warnings-as-errors`, `mix dialyzer` (0), `mix credo --strict` (0) clean. Runtime: Elixir 1.20.4 / OTP 29.

## Closure (2026-09-27, after independent validation)

Astra's independent six-case validation ran against `bf4795a` — the same revision as the 106 passing repository tests — and found **0/6 passing**: the passing suite and the failing contract probes described one revision together. The six violations, imported verbatim as repository regressions (`f3ab5c0`, which also replaced the consumer-kill test that had explicitly *approved* overlapping generations) and repaired in `33f2142`:

1. **Capacity was reclaimed at quiesce while the old callback still executed** (the approved-overlap test's mirror). The quiesce now retains permits; the regression kills the consumer under admitted work, proves the stage worker still alive, and refuses the next submission until the survivor's own terminal release frees the slot across the generation boundary.
2. **The reserve→forward gap stranded orphan permits** (caller death between `admit` and the cast). `submit/3` makes reservation and forwarding one transaction inside the owner; a caller dead before processing reserves nothing (liveness checked at processing time).
3. **A 10ms call was still blocked after 80ms** — admission ignored the caller's deadline. The deadline now bounds admission; exhaustion raises the caller's own `PipelineError :timeout`.
4. **The settling grace reset on every retry** (`admit/2` recursion recomputed its limit). The retry is iterative against one fixed budget: the caller's deadline when smaller, the 250ms grace otherwise.
5. **`Client.call` exited and killed the client on overload.** The client rescues `AdmissionError` and re-raises it at the caller boundary; the reusable client survives.
6. **`Client.cast` answered `:ok` to a refused submission.** It returns the pipeline's own acknowledgment.

A build finding of its own: `GenServer.call/3` exits with `{:timeout, {GenServer, :call, _}}` — a tuple, not bare `:timeout` — and the first repair misclassified budget exhaustion as owner-gone (`:noprocess`) until the wrapped reason was matched explicitly.

**The retention trade-off, stated honestly:** permits for packets destroyed with the topology (a stage crash mid-callback, a packet in a dead consumer's mailbox) never receive a terminal release. They stay `active` in the report — admitted, outcome unknown — until the pipeline is restarted. This is deliberate: a timer-based reaper would release capacity while a callback may still be executing (a wider window of violation 1, not a fix), and inventing an outcome to balance the counter is exactly what the contract forbids. Recovery from accumulated unknown permits is a pipeline restart; a lineage-tracking release protocol (stages acknowledging packet handoff) is the only mechanism that could do better, at a per-packet messaging cost the current design does not pay. Byte-weighted admission (payload credits) must NOT be built on this ledger until its ownership lifecycle is proven — it would inherit whatever release defects remain.

Post-repair gates: 111 tests × seeds 1–3, `mix format --check-formatted`, `mix compile --force --warnings-as-errors`, `mix dialyzer` (0), `mix credo --strict` (0) clean; Astra's six-case validation (barrier pattern adapted to the renamed `:submit` message) passes 6/6.

## Follow-up closure (2026-09-27, second independent validation)

Astra's follow-up checks found three more defects at `bf097f6` (1/4 in their reproducer) and one source-verified mismatch. All repaired (`31cb366` RED, `6da2ae5` GREEN; their follow-up validation now 4/4, the six-case validation 6/6):

1. **A reported refusal could execute afterward.** The fixed budget bounded only the client-side wait; the envelope carried no deadline, so the owner happily reserved and forwarded a submission whose caller had already been refused. The budget now travels in the envelope (`{:submit, ip, admission_deadline}`) and is checked at dequeue — the demonstrated delayed-processing case is refused, never executed. The packet's execution deadline stays a separate, later boundary.
2. **Acknowledgment timeout was mistranslated into a definite never-admitted.** An outcome learned by its ack timing out is now marked uncertain: after a 25ms ack slack (an on-the-way reply becomes a definite answer, and a late acceptance becomes the ordinary timeout contract instead of a phantom refusal), `AdmissionError` carries `request_ref` and `report/1` exposes `refs` for reconciliation. The residual race — admitted in the last instant before the reply was lost — is irreducible without a cancellation protocol; it is reported honestly instead of being claimed away. The distinction from an explicit owner refusal is carried in the same public outcome shape (`{:error, :unavailable}`) because their caller handling is identical (retry or reconcile); the reproducer pins that shape.
3. **Reattachment leaked monitors on surviving workers** (4 → 7 → 10 across two consumer restarts): the attach handler replaced the monitor map wholesale. It now diffs — survivors keep their monitors, gone incarnations are demonitored and flushed, only new incarnations are watched — pinned by a restart-cycle count assertion.
4. **Forwarding used the registered name after resolving the incarnation** — the delivery target and the monitored identity could disagree across a restart. The cast now targets the resolved pid (source correction; not race-reproduced by either side).

**The retention stance, narrowed per the review.** The reviewer corrected the record's reading of their own reproducer: the surviving-worker assertion was evidence of the overlap defect, not an architectural requirement to preserve that worker; full-line termination was an equally acceptable repair, and the diagnostic `admit/2` need not survive for their convenience. With that correction, the honest statement of the current design is **retention with manual recovery**:

- "Every admitted job reaches exactly one terminal outcome" is NOT established as written. The true law: every admitted job is either released to exactly one terminal outcome, or remains an unresolved reservation — reported `active`, outcome unknown — until the pipeline is restarted.
- `active` therefore does not mean strictly "queued plus executing"; it also counts unresolved reservations for possibly-destroyed work. The queued/executing/unknown split would need per-packet lineage or a stop-and-sweep.
- "Per-packet lineage tracking is the only mechanism that could do better" was too strong: **confirmed termination of the complete execution generation** is the other conservative reclaim, and it is the documented procedure — `stop` the pipeline (the wrapper terminates owner and line together; nothing survives), `start` again. The ledger dies with the owner, and reclaiming it is safe precisely because the same termination destroyed any surviving work. The procedure is pinned by test: an unresolved reservation holds its slot (visible as `:overloaded`) until the stop/restart, after which admission is honestly reclaimed.
- Outcome uncertainty (what happened to the work) is kept separate from capacity reservation (whether the slot is held): `counts` stay release-driven; `active`/`refs` carry the unresolved set.

Trap-craft lesson from the RED work: a trap that suspends the owner during boot settling exercises the honest explicit settling refusal, not the defect — the barrier must wait for the owner to reach `:open` first (the first cast-mode trap passed vacuously until this was fixed; the boot attach can lag `start/1` by ~5ms).
