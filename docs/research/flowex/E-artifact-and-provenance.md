# E — artifact integrity and provenance (FX-009, FX-010)

Work package E record, 2026-09-27. Split deliberately: the mechanical work is done; the license declaration is a human decision, prepared with evidence, not made here.

## Mechanical work, done and enforced

- **The package allowlist** (`mix.exs`) now carries `.formatter.exs`, `LICENSE`, and `figures` alongside `lib`, `mix.exs`, `README.md` — closing FX-009's omissions.
- **An artifact-level release gate runs in the default suite** (`test/flowex/package_downstream_test.exs`, ~4.5s): it builds the real package with `mix hex.build`, extracts `contents.tar.gz`, scaffolds a fresh downstream project depending on NOTHING but the extracted artifact, and proves (a) the formatter export arrives — a consumer's `pipe :double, count: 2` keeps its paren-less form under `import_deps: [:flowex]`, (b) the artifact carries the license notice and figures, and (c) a pipeline actually compiles and runs from the artifact (`21 → 42`). A fast allowlist test (`package_test.exs`) guards the config itself.
- **README corrections** that needed no license decision: `mix espec` → the checks the project actually runs (ExUnit, format, compile warnings-as-errors, dialyzer, credo); the stale `%Flowex.Pipeline{sup_pid: ...}` struct example → the current via-tuple shape including `owner_name`; the `{:ok, pipeline.sup_pid}` application example → `GenServer.whereis(pipeline.sup_name)`; the "what happened" list now mentions the admission owner and the wrapper. New sections (already landed with their packages): Initialization and options, Admission and overload.
- **Verification receipts (this machine, Elixir 1.20.4 / OTP 29):** `mix test --seed 0/7` → 105 passed including the artifact gate; hand-run of the same flow outside ExUnit reproduced `FORMAT_EXIT=0` and `DOWNSTREAM_RESULT=42`.

## Runtime matrix — honestly open

The declared floor is `elixir: "~> 1.15"`. This machine can only certify 1.20.4/OTP 29; no other toolchains are installed here. Multi-version certification needs either a machine with the matrix installed or CI — both outside this record. The audit's position stands: a successful 1.20.4 audit does not establish the advertised lower bound.

## Provenance findings (FX-010) — evidence, no verdict

- The checked-in `LICENSE` is **Apache License 2.0**, "Copyright 2017 Anton Mishchuk", introduced by upstream commit `d78a761` ("Add license", 2017-06-24, tagged v0.5.1) — **an ancestor of the fork point `3ccf92e`**. The Apache file is not a fork-era addition; it is upstream's own declaration at v0.5.1.
- The README fork notice (`542322e`, fork-era) states "The author's MIT LICENSE is kept" — contradicting the checked-in file.
- `mix.exs` package metadata declares `licenses: ["MIT"]` — contradicting the checked-in file. `source_url`/links and version `0.5.4` identify the upstream repository, not this fork.

**Decision, 2026-09-27 (maintainer, on the evidence): Apache-2.0.** The LICENSE file is
upstream's own — added by Anton Mishchuk himself (`d78a761`, 2017-06-24, tagged v0.5.1),
present at the fork point `3ccf92e`, and verified still present at upstream `master` on
2026-09-27 (raw `LICENSE` fetched: "Copyright 2017 Anton Mishchuk … Apache License,
Version 2.0"). Upstream's hex.pm page declares "MIT" — the fork inherited that
contradiction through `mix.exs`; the author's own file outranks stale package metadata,
and inherited Apache-2.0 code cannot be relabeled. Applied: `licenses: ["Apache-2.0"]`,
the README fork notice states the facts above with attribution preserved, `source_url`
points at the maintained Codeberg location (upstream kept as a second link), and version
identity moves to `0.6.0` (the fork's own line — the admission protocol and `cast/2`'s
observable refusal are breaking). Pinned by `package_test.exs`: metadata and file must
agree on Apache-2.0.

## Standing limitation

`scripts/traps/*.sh` evidence files now embed the run's PID (`/tmp/fb_run$$._t7_compile.txt` …), so concurrent runs can no longer overwrite one another's receipts — the audit's fixed-filename concern, closed. The runtime-matrix limitation above remains the open item.
