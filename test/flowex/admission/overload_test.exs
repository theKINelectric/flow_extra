defmodule Flowex.Admission.OverloadTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-001 trap (pursuit C, "where did the work go"): the audit's overload
  probe cast 10,020 submissions at a suspended first stage and only 10,001
  callbacks ever ran — nineteen events silently eaten by GenStage's
  keep-last buffer, the caller still holding :ok for every one of them.

  Astra's C design record (docs/research/flowex/C-admission-design.md) is
  the contract: a submission either acquires a permit or is refused
  observably; permits are held for the work's whole lifetime (a caller's
  timeout releases nothing); every admitted job reaches exactly one
  terminal outcome; any topology failure quiesces its generation to
  unknown — never an invented success or failure. The battery below is
  Astra's decisive-test list plus the audit reproducer, reconciled.
  """

  test "holds exactly K admitted jobs and refuses K+1 observably" do
    pipeline = ReplyTrapPipeline.start(%{admission_capacity: 3})
    stage = suspend_work_stage!(pipeline)

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :one})

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :two})

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :three})

    assert {:error, :overloaded} =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :four})

    :sys.resume(stage)
    assert_receive {:worked, :one}, 2_000
    assert_receive {:worked, :two}, 2_000
    assert_receive {:worked, :three}, 2_000
    refute_receive {:worked, :four}, 100

    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).active == 0 end)
    assert Flowex.Admission.report(pipeline.owner_name).counts.succeeded == 3
  end

  test "completing one job frees capacity for exactly one more" do
    pipeline = ReplyTrapPipeline.start(%{admission_capacity: 1})
    stage = suspend_work_stage!(pipeline)

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :first})

    assert {:error, :overloaded} =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :second})

    :sys.resume(stage)
    assert_receive {:worked, :first}, 2_000
    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).active == 0 end)

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :third})

    :sys.resume(stage)
    assert_receive {:worked, :third}, 2_000
  end

  test "a timed-out caller's executing work keeps its permit until it finishes" do
    pipeline = ReplyTrapPipeline.start(%{admission_capacity: 1})

    error =
      assert_raise Flowex.PipelineError, fn ->
        ReplyTrapPipeline.call(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :held}, 100)
      end

    assert error.reason == :timeout

    # The caller left; the work did not. Its permit is still held, so the
    # pipeline refuses rather than double-booking its one slot.
    assert {:error, :overloaded} =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :next})

    assert_receive {:worked, :held}, 2_000
    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).active == 0 end)

    # The abandoned job still reached its terminal outcome: accounted, not
    # lost. It completed after its own deadline, so the outcome is :expired
    # — the reply was dropped and the permit released at completion (v1 does
    # not record whether callbacks had begun; see the C design record).
    assert Flowex.Admission.report(pipeline.owner_name).counts.expired == 1

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :after})
  end

  test "a replayed or stale completion cannot double-release capacity" do
    pipeline = ReplyTrapPipeline.start(%{admission_capacity: 1})
    owner = pipeline.owner_name
    id = make_ref()

    assert {:ok, 1} = Flowex.Admission.admit(owner, id)
    assert :ok = Flowex.Admission.release(owner, id, :succeeded)
    assert {:error, :stale} = Flowex.Admission.release(owner, id, :succeeded)
    assert {:error, :stale} = Flowex.Admission.release(owner, make_ref(), :succeeded)

    report = Flowex.Admission.report(owner)
    assert report.active == 0
    assert report.counts.succeeded == 1

    # Exactly one slot, exactly once: the next submission is admitted.
    assert {:ok, 1} = Flowex.Admission.admit(owner, make_ref())
  end

  test "killing the consumer quiesces the generation to unknown and reopens" do
    pipeline = ReplyTrapPipeline.start(%{admission_capacity: 3})
    stage = suspend_work_stage!(pipeline)

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :old_one})

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :old_two})

    old_consumer = GenServer.whereis(pipeline.out_name)
    Process.exit(old_consumer, :kill)

    # The generation quiesces: outstanding work is unknown (never invented
    # into success or failure), capacity is reclaimed, a fresh generation
    # opens, and the stale releases of the dead generation cannot touch it.
    wait_until(fn ->
      report = Flowex.Admission.report(pipeline.owner_name)
      report.generation == 2 and report.status == :open
    end)

    report = Flowex.Admission.report(pipeline.owner_name)
    assert report.counts.unknown == 2
    assert report.active == 0

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :fresh})

    :sys.resume(stage)

    # The fresh generation's work flows; the parked old packets complete
    # too, but their releases are stale — no double counting anywhere.
    assert_receive {:worked, :fresh}, 2_000
    assert_receive {:worked, :old_one}, 2_000
    assert_receive {:worked, :old_two}, 2_000

    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).active == 0 end)
    report = Flowex.Admission.report(pipeline.owner_name)
    assert report.counts.unknown == 2
    assert report.counts.succeeded == 1
  end

  test "killing the owner tears down the line and reopens fresh, old work dead" do
    pipeline = ReplyTrapPipeline.start(%{admission_capacity: 2})

    doomed = %ReplyTrapPipeline{report_to: self(), ref: :doomed}

    caller =
      spawn(fn ->
        try do
          ReplyTrapPipeline.call(pipeline, doomed, 2_000)
        rescue
          _ -> :done
        catch
          _, _ -> :done
        end
      end)

    Process.sleep(20)
    old_owner = GenServer.whereis(pipeline.owner_name)
    Process.exit(old_owner, :kill)

    # rest_for_one wrapper: the owner's death takes the whole line with it,
    # so the fresh owner's clean capacity can never overlap still-executing
    # old work — the old work is dead by construction. The poll must
    # tolerate the registration gap while the fresh owner starts.
    safe_report = fn ->
      try do
        Flowex.Admission.report(pipeline.owner_name)
      catch
        :exit, _ -> nil
      end
    end

    wait_until(fn ->
      match?(%{status: :open}, safe_report.()) and
        GenServer.whereis(pipeline.owner_name) != old_owner
    end)

    # The in-flight job died with the line: provably never completed.
    refute_receive {:worked, :doomed}, 150

    assert :ok =
             ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :fresh})

    assert_receive {:worked, :fresh}, 2_000
    _ = caller
  end

  test "the audit's overload: every submission is admitted or refused — nothing silently dropped" do
    pipeline = ReplyTrapPipeline.start(%{admission_capacity: 20})
    stage = suspend_work_stage!(pipeline)

    results =
      for i <- 1..100,
          do: ReplyTrapPipeline.cast(pipeline, %ReplyTrapPipeline{report_to: self(), ref: i})

    accepted = Enum.count(results, &(&1 == :ok))
    refused = Enum.count(results, &match?({:error, :overloaded}, &1))

    # The audit's law, reconciled: 10,020 cast, 10,001 ran, 19 vanished.
    # Here: 100 submitted = 20 admitted + 80 refused. Nothing vanishes.
    assert accepted == 20
    assert refused == 80
    assert accepted + refused == 100

    :sys.resume(stage)

    for i <- 1..20, do: assert_receive({:worked, ^i}, 2_000)
    refute_received {:worked, _}

    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).active == 0 end)

    report = Flowex.Admission.report(pipeline.owner_name)
    assert report.counts.succeeded == 20
    # admitted (20) = succeeded (20) + everything else (0): reconciled.
    assert report.active == 0 and report.counts.unknown == 0
  end

  # The audit probe's own technique: find the pipeline's work stage through
  # the supervision tree and suspend it, so admitted work parks in the
  # producer's bounded queue exactly as it did during the overload probe.
  defp suspend_work_stage!(pipeline) do
    [line_sup] =
      Supervisor.which_children(GenServer.whereis(pipeline.sup_name))
      |> Enum.filter(fn {_id, _pid, type, _mods} -> type == :supervisor end)
      |> Enum.map(fn {_id, pid, _type, _mods} -> pid end)

    {_, stage, _, _} =
      Enum.find(Supervisor.which_children(line_sup), fn {_id, pid, _type, _mods} ->
        try do
          match?(%{state: %Flowex.StageOpts{function: :work}}, :sys.get_state(pid))
        rescue
          _ -> false
        end
      end)

    :sys.suspend(stage)
    stage
  end

  defp wait_until(fun, attempts_left \\ 300)

  defp wait_until(_fun, 0), do: flunk("condition was not met before the deadline")

  defp wait_until(fun, attempts_left) do
    unless fun.() do
      Process.sleep(10)
      wait_until(fun, attempts_left - 1)
    end
  end
end
