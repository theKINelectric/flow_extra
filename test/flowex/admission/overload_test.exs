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
  terminal outcome; any topology failure quiesces its generation —
  surviving work RETAINS its permit until its own terminal release, and
  work destroyed with the topology stays admitted, outcome unknown: never
  an invented success or failure, and never a slot reused under surviving
  work. The battery below is Astra's decisive-test list plus the audit
  reproducer, reconciled.
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

  test "old work surviving a consumer restart keeps its permit — no overlap onto it" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})

    assert :ok =
             AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
               observer: self(),
               id: :old
             })

    assert_receive {:entered, :old, worker}
    Process.exit(GenServer.whereis(pipeline.out_name), :kill)

    # The generation quiesces and reopens — but the old work did not die
    # with the consumer: it executes in the stage worker, and its permit
    # stays occupied. Retention, not reuse: the reopened generation cannot
    # double-book the slot under still-executing old work.
    wait_until(fn ->
      report = Flowex.Admission.report(pipeline.owner_name)
      report.generation > 1 and report.status == :open
    end)

    assert Process.alive?(worker)

    assert {:error, :overloaded} =
             AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
               observer: self(),
               id: :new
             })

    # The survivor's terminal release frees the slot — across the
    # generation boundary, exactly once, at the work's own completion.
    send(worker, :release)
    assert_receive {:finished, :old}, 2_000
    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).active == 0 end)
    assert Flowex.Admission.report(pipeline.owner_name).counts.succeeded == 1

    assert :ok =
             AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
               observer: self(),
               id: :after
             })

    assert_receive {:entered, :after, after_worker}, 2_000
    send(after_worker, :release)
    assert_receive {:finished, :after}, 2_000
  end

  test "a caller that dies before its submission is processed reserves nothing" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})
    owner = GenServer.whereis(pipeline.owner_name)
    :sys.suspend(owner)

    observer = self()

    caller =
      spawn(fn ->
        AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
          observer: observer,
          id: :orphan
        })
      end)

    # The submission sits in the suspended owner's mailbox; the caller
    # dies before it is ever processed. Reservation and forwarding are one
    # transaction inside the owner — a dead caller reserves no permit and
    # strands no accounting.
    wait_until(fn ->
      {:messages, messages} = Process.info(owner, :messages)
      Enum.any?(messages, &match?({:"$gen_call", _, {:submit, _, _}}, &1))
    end)

    ref = Process.monitor(caller)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^ref, _, _, :killed}
    :sys.resume(owner)

    assert Flowex.Admission.report(pipeline.owner_name).active == 0
  end

  test "the settling grace is one fixed budget — retries never reset it" do
    {:ok, owner} = GenServer.start_link(Flowex.Admission, {1, [:astra_nonexistent_worker]})

    task = Task.async(fn -> Flowex.Admission.admit(owner, make_ref()) end)
    assert Task.yield(task, 500) == {:ok, {:error, :unavailable}}
    Task.shutdown(task, :brutal_kill)
  end

  test "a cast's :unavailable refusal is not followed by late execution" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})
    owner = GenServer.whereis(pipeline.owner_name)
    # The trap must catch the OPEN pipeline's suspension (the acknowledged
    # admission window), not the boot-time settling that legitimately
    # refuses: wait for the owner to attach before suspending it.
    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).status == :open end)
    :sys.suspend(owner)
    observer = self()

    caller =
      spawn(fn ->
        outcome =
          AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
            observer: observer,
            id: :cast
          })

        send(observer, {:outcome, :cast, outcome})

        receive do
          :stop -> :ok
        after
          2_000 -> :ok
        end
      end)

    assert_receive {:outcome, :cast, {:error, :unavailable}}, 700
    assert Process.alive?(caller)
    :sys.resume(owner)

    # The refusal said the submission was not accepted: the request still
    # queued behind the suspension must be refused at dequeue by its own
    # expired admission deadline — not executed after the fact.
    refute_receive {:entered, :cast, _worker}, 150

    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).status == :open end)
    assert Flowex.Admission.report(pipeline.owner_name).active == 0
    send(caller, :stop)
  end

  test "a call's :unavailable refusal is not followed by late execution" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})
    owner = GenServer.whereis(pipeline.owner_name)
    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).status == :open end)
    :sys.suspend(owner)
    observer = self()

    caller =
      spawn(fn ->
        outcome =
          try do
            AdmissionTrapPipeline.call(
              pipeline,
              %AdmissionTrapPipeline{observer: observer, id: :call},
              1_000
            )

            :returned
          rescue
            e in Flowex.AdmissionError -> {:refused, e.reason, Map.get(e, :request_ref)}
          end

        send(observer, {:outcome, :call, outcome})

        receive do
          :stop -> :ok
        after
          2_000 -> :ok
        end
      end)

    assert_receive {:outcome, :call, {:refused, :unavailable, request_ref}}, 700

    # An outcome learned by its acknowledgment timing out is uncertain,
    # not a proven never-admitted: it carries the request identity so the
    # caller can reconcile against the owner's ledger.
    assert is_reference(request_ref)
    assert Process.alive?(caller)
    :sys.resume(owner)

    refute_receive {:entered, :call, _worker}, 150

    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).status == :open end)
    report = Flowex.Admission.report(pipeline.owner_name)
    assert report.active == 0 and request_ref not in report.refs
    send(caller, :stop)
  end

  test "the documented recovery: stop and restart reclaims an unresolved reservation" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})
    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).status == :open end)

    assert :ok =
             AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
               observer: self(),
               id: :stuck
             })

    assert_receive {:entered, :stuck, worker}

    # Destroy the packet mid-callback: its permit has no terminal release
    # coming — an unresolved reservation that holds the pipeline's one
    # slot (the availability cost of retention, made visible).
    Process.exit(worker, :kill)

    wait_until(fn ->
      report = Flowex.Admission.report(pipeline.owner_name)
      report.status == :open and report.active == 1
    end)

    assert {:error, :overloaded} =
             AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
               observer: self(),
               id: :blocked
             })

    refute_receive {:finished, :stuck}, 150

    # The procedure: confirmed termination of the complete execution
    # generation — stop the pipeline, start it again. Nothing of the old
    # line survives the stop, so reclaiming the ledger with it is safe;
    # the fresh pipeline admits normally.
    AdmissionTrapPipeline.stop(pipeline)

    fresh = AdmissionTrapPipeline.start(%{admission_capacity: 1})

    assert :ok =
             AdmissionTrapPipeline.cast(fresh, %AdmissionTrapPipeline{
               observer: self(),
               id: :recovered
             })

    assert_receive {:entered, :recovered, fresh_worker}, 2_000
    send(fresh_worker, :release)
    assert_receive {:finished, :recovered}, 2_000
  end

  test "reattachment watches each worker exactly once — no monitor accumulation" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})
    owner = GenServer.whereis(pipeline.owner_name)
    # The kill must be SEEN by an attached owner: a kill that beats the
    # boot-time attach is not a topology failure at all.
    wait_until(fn -> Flowex.Admission.report(pipeline.owner_name).status == :open end)

    count = fn ->
      {:monitors, monitors} = Process.info(owner, :monitors)
      length(monitors)
    end

    initial = count.()

    counts =
      for _ <- 1..2 do
        old = GenServer.whereis(pipeline.out_name)
        generation = Flowex.Admission.report(owner).generation
        Process.exit(old, :kill)

        wait_until(fn ->
          report = Flowex.Admission.report(owner)

          report.generation > generation and report.status == :open and
            GenServer.whereis(pipeline.out_name) != old
        end)

        count.()
      end

    assert initial > 0
    assert counts == [initial, initial]
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
