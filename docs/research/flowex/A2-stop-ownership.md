# A2 — stop ownership (FX-003)

Experiment record, pursuit §3 style. Work package A.

- **Claim:** an intentional `stop/1` keeps the instance stopped — the name stays unregistered, the owning parent's child spec is removed, siblings stay usable — while abnormal death still restarts under the parent's policy.
- **Counterexample (audited revision `1e06223`):** `stop_pipeline` called `Supervisor.stop` on the pipeline supervisor, but the supervised child spec says `restart: :permanent`, and the permanent contract restarts even after normal termination. Audit probe: old supervisor pid replaced, new one alive under the same name, both engines.
- **Regression:** `test/flowex/names/stop_ownership_test.exs`. RED at `4a12cb4` — 1/3: both stop tests found the name alive again; the kill-restart control passed (pinning that crash recovery must survive the repair).
- **Fix:** `Flowex.Pipeline` carries `parent` (the owning supervisor pid, set by `supervised_start/2` on both engines). `stop_pipeline` dispatches on ownership: with a parent, `Supervisor.terminate_child(parent, sup_name)` + `Supervisor.delete_child(parent, sup_name)`; standalone keeps the original workers-then-supervisor shutdown.
- **Detour worth recording:** the first RED run's control test failed with an "impossible" `FunctionClauseError` — cause: `FunPipeline`'s pipe heads (`%{a: a}`) require their named opts keys, and the test passed `%{}`. The failure cascaded exactly like an unhandled pipe error: error stage's default handler re-raised, `rest_for_one` tore down the consumer, caller saw `:DOWN :shutdown`. Lesson: pattern-headed pipes double as an opts contract; support pipelines in tests must satisfy it.
- **Commands:** `mix test test/flowex/names/stop_ownership_test.exs` → 3/3; `mix test` seeds 0/42/7 → 76 passed each. Runtime: Elixir 1.20.4 / OTP 29.
