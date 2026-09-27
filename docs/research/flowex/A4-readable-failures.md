# A4 — readable failures (FX-008)

Experiment record, pursuit §3 style. Work package A.

- **Claim:** every failure path produces a string-rendering exception while the machine-readable cause survives in its own field.
- **Counterexample (audited revision `1e06223`):** the call failure paths assigned `:timeout`, `:noprocess`, and monitor exit reasons to `Flowex.PipelineError`'s `message` field; the default defexception `message/1` renders a diagnostic — the audit probe's timeout rescue printed `"... (expected a string)"` instead of a failure report.
- **Regression:** `test/flowex/pipeline/error_rendering_test.exs`. RED at `ab9b3de` — 1/4: timeout, vanished pipeline, and crash cascade failed (the explicit-string control passed).
- **Fix:** `Flowex.PipelineError` gains `reason`; `message/1` prefers a given binary `message`, else renders from `reason` ("did not answer before its deadline" / "is not running" / "exited: `<inspect>`"). The three raise sites in `pipeline.ex` now pass `reason:`. `Flowex.PipeError` already carried string messages (`Exception.message(error)` at construction) — unchanged, now documented.
- **Recorded cascade fact:** a stage's `exit(:boom)` reaches the caller as the consumer's `:shutdown` — the rest_for_one teardown, not the origin. The caller learns *that* the line died, not *why*; pinned by assertion in the crash test. Surfacing the originating reason is future contract work (ties into the pursuit's failure-behavior row).
- **Commands:** `mix test test/flowex/pipeline/error_rendering_test.exs` → 4/4; `mix test --seed 0` → 82 passed. Runtime: Elixir 1.20.4 / OTP 29.
