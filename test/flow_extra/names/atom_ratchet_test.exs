defmodule FlowExtra.Names.AtomRatchetTest do
  use ExUnit.Case, async: false

  @moduledoc """
  T5 traps (TS-5, "atom ratchet" — ruled in PASSON_FLOWEX_T5_RULING.md):

  1. The ratchet pin: 50 start/stop cycles must leave the atom table flat
     (delta 0; allowance 1 for the registry's first start — named here).
     Registered atoms are never garbage-collected, so per-instance
     String.to_atom names built around inspect(make_ref()) ratchet the
     table forever.
  2. The concurrent pin: two pipelines of the same module live at once and
     serve interleaved — the key uniqueness must hold across instances.
  """

  # One-time lazy module loading interns atoms (a beam module's atoms are
  # created when it first loads, not per cycle) — warm both tracks up before
  # measuring so the pin targets the per-cycle ratchet and nothing else.
  setup do
    pipeline = MailboxPipeline.start(%{})
    MailboxPipeline.stop(pipeline)
    pipeline = SyncErrorTrackPipeline.start(%{})
    SyncErrorTrackPipeline.stop(pipeline)
    :ok
  end

  test "50 parallel start/stop cycles do not ratchet the atom table" do
    before = :erlang.system_info(:atom_count)

    for _ <- 1..50 do
      pipeline = MailboxPipeline.start(%{})
      MailboxPipeline.stop(pipeline)
    end

    delta = :erlang.system_info(:atom_count) - before
    # Allowance: 1, for the registry's first start. Nothing else may intern.
    assert delta <= 1,
           "atom table grew by #{delta} over 50 parallel start/stop cycles (allowance 1)"
  end

  test "50 sync start/stop cycles do not ratchet the atom table" do
    before = :erlang.system_info(:atom_count)

    for _ <- 1..50 do
      pipeline = SyncErrorTrackPipeline.start(%{})
      SyncErrorTrackPipeline.stop(pipeline)
    end

    delta = :erlang.system_info(:atom_count) - before

    assert delta <= 1,
           "atom table grew by #{delta} over 50 sync start/stop cycles (allowance 1)"
  end

  test "two pipelines of the same module serve concurrently with interleaved stops" do
    p1 = MailboxPipeline.start(%{})
    p2 = MailboxPipeline.start(%{})

    assert MailboxPipeline.call(p1, %MailboxPipeline{number: 1}).number == 1
    assert MailboxPipeline.call(p2, %MailboxPipeline{number: 2}).number == 2

    MailboxPipeline.stop(p1)

    assert MailboxPipeline.call(p2, %MailboxPipeline{number: 3}).number == 3

    MailboxPipeline.stop(p2)
  end
end
