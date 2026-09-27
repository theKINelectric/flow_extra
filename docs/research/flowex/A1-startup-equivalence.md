# A1 — startup equivalence (FX-002)

Experiment record, pursuit §3 style. Work package A.

- **Claim:** standalone `start/1` and `supervised_start/2` are one admission path: the DSL's `init/1` runs exactly once, in the caller, and its result is validated before any topology spawns. A restart reuses the prepared options (init does not run again in a restarted child).
- **Counterexample (audited revision `1e06223`):** audit probe `INIT standalone/supervised: {AstraAuditAsync, true, nil}` — same pipeline, `initialized: true` standalone, `nil` supervised, on both engines; an `init/1` returning a non-map was also accepted on the supervised door only.
- **Regression:** `test/flowex/pipeline/supervised_init_test.exs` (+ `test/support/supervised_init_pipelines.ex`). RED at `f58d528` — 0/4: supervised reads `nil` where `true` belongs; both supervised refusals raise nothing.
- **Fix:** generated `supervised_start/2` on both engines runs `init(opts)` and (sync) `validate_opts!/2` caller-side. Sync caller-side validation matters: a raise inside a child's `start_link` under `Supervisor.start_child` surfaces as `{:error, _}`, not as the caller's `ArgumentError`.
- **Commands:** `mix test test/flowex/pipeline/supervised_init_test.exs` → 4/4; `mix test --seed 0` → 73 passed (69 + 4). Runtime: Elixir 1.20.4 / OTP 29.
- **Intentional difference left open:** module-pipe `init/1` timing between engines (async: once per replica at build; sync: per request) is FX-007, not this experiment.
