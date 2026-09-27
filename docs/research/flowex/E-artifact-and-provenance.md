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

**Human decisions required before any distribution:** which license declaration is authoritative (the file's Apache-2.0 has the oldest and strongest evidence), whether the fork may relicense at all, the corrected package metadata, the maintained source URL, and the fork's version identity. None of these were changed mechanically. The only metadata-adjacent change is the files allowlist above.

## Standing limitation

Evidence filenames in `scripts/traps` were checked: they already write per-run-distinct `/tmp` names (`t6-dialyzer.sh`, `t7-hygiene.sh` use distinct prefixes); the audit's fixed-filename concern applies to older scripts no longer present.
