# Migrating from Flowex to FlowExtra

FlowExtra is the Factory-maintained continuation of Anton Mishchuk's
[Flowex](https://github.com/antonmi/flowex) (lineage `antonmi/flowex @ 3a9ebae`,
fork point `3ccf92e`, revived 2026). The rename is mechanical in code but
breaking at every binding site; **no compatibility modules are provided** —
the migration is explicit by design.

## Dependency

```elixir
# before (upstream's published package)
{:flowex, "~> 0.5"}

# after (the maintained fork)
{:flowextra, "~> 0.6"}
```

The fork's 0.6.0 line has not been published to hex.pm under either name;
the behavioral differences below are the effective changelog from upstream's
published 0.5.x.

## Namespace

| Flowex | FlowExtra |
| --- | --- |
| `use Flowex.Pipeline` | `use FlowExtra.Pipeline` |
| `use Flowex.Sync.Pipeline` | `use FlowExtra.Sync.Pipeline` |
| `%Flowex.Pipeline{}` struct | `%FlowExtra.Pipeline{}` |
| `Flowex.Client` | `FlowExtra.Client` |
| `Flowex.IP` | `FlowExtra.IP` |
| `Flowex.PipelineError` / `Flowex.PipeError` / `Flowex.AdmissionError` | same suffixes under `FlowExtra.` |
| `Flowex.Admission` (owner + ledger API) | `FlowExtra.Admission` |
| `Flowex.Registry` (internal process registry) | `FlowExtra.Registry` |

## Application configuration

The OTP application atom changes: `config :flowex, ...` becomes
`config :flowextra, ...`. Formatter consumers change
`import_deps: [:flowex]` to `import_deps: [:flowextra]` (the exported DSL
form is unchanged).

## Behavioral differences from upstream Flowex 0.5.x

All of these are pinned by the repository's suite:

- **Admission protocol.** The asynchronous engine bounds admitted work
  (`admission_capacity`, default 100). `cast/2` acknowledges — `:ok` means
  admitted and accounted — or refuses observably; outcomes carry named
  certainty: `:overloaded`/`:unavailable` are definite refusals,
  `{:unacknowledged, ref}` is an unknown outcome with a request identity
  for inspection via `FlowExtra.Admission.report/1`.
- **One deadline per call.** Every `call/3` carries a deadline (default
  5_000 ms — upstream waited forever) shared across client queueing,
  admission, and execution; past the admission budget a 25 ms
  acknowledgment allowance still hears a reply already on its way.
- **Supervised pipelines stop through the owner** — terminate then delete
  the child spec; an intentional stop stays stopped (upstream's `:permanent`
  spec resurrected it).
- **Error routing is honest.** Healthy packets skip error pipes entirely;
  failed packets skip the remaining normal pipes and reach the designated
  error handler. Exceptions render readable messages while keeping the
  machine-readable `reason`.
- **Replies are revocable.** Calls own a process alias as the reply
  destination; a late reply cannot rot in a caller's mailbox.
- **Initialization is startup work.** `init/1` runs on both engines once,
  at start, in the starting caller — never per request; all declarations
  are validated before any initializer runs.
- **The client is a boundary, not a liability.** `FlowExtra.Client.call/3`
  raises expected request failures at the caller boundary and the reusable
  client survives them; `Client.cast/2` is synchronous and returns the
  pipeline's own answer instead of an unconditional `:ok`.
- **Topology failures retain permits.** No slot is reused under surviving
  work; work destroyed with the topology stays admitted with an unknown
  outcome until the pipeline is stopped and started again (see README,
  "Admission and overload"). Upstream had no admission at all.
- **The sync engine stays a single-process debug engine**, deliberately
  without admission.

## What did not change

The DSL (`pipe`/`error_pipe`, options, `count` replication), the GenStage
execution model, railway data semantics, and the Apache-2.0 license with
Anton Mishchuk's original attribution preserved.
