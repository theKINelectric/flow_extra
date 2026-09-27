# A3 — error-track exclusion on the happy path (FX-004)

Experiment record, pursuit §3 style. Work package A.

- **Claim:** healthy packets never execute either arity of an error handler; failed packets run the intended three-argument handler and skip remaining normal pipes — on both engines.
- **Counterexample (audited revision `1e06223`):** the sync walker dispatched on `ip.error` alone, so a healthy packet entered the error stage's normal two-argument dispatch. The audit's dual-arity module turned a successful sync result into `:wrong_error_path` through `call/2`; with the default three-argument-only `handle_error`, the arity mismatch was rescued into `ip.error` on every successful sync call and concealed by the public API's struct extraction.
- **Regression:** `test/flowex/sync/happy_track_test.exs` (+ `test/support/dual_arity_error_pipes.ex` — `DualArityErrorPipe` reports `:wrong_track`/`:right_track` to an observer so visits are observable, not silent corruption). RED at `7fe5c7d` — 1/2: the healthy-packet test received `:wrong_track` from the sync engine; the failed-packet control passed on both engines.
- **Fix:** `Flowex.Sync.GenServer.process/4` now dispatches on stage type and error state, mirroring `Flowex.Stage`: `:error_pipe` + no error → packet passes through untouched; error → `do_process_error` (which already skips `:pipe` stages); otherwise normal dispatch.
- **Commands:** `mix test test/flowex/sync/happy_track_test.exs` → 2/2; `mix test --seed 0` → 78 passed. Runtime: Elixir 1.20.4 / OTP 29.
- **Left open:** error-module `init/1` timing/frequency parity between engines is FX-007 (package A remainder), deliberately not asserted here.
