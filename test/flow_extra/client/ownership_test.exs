defmodule FlowExtra.Client.OwnershipTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-006 trap (pursuit B, "who cleans up"): Client.call! stopped its
  temporary client only after a successful GenServer.call — a caught
  timeout leaked the helper, linked to the caller (audit probe: one live
  leaked link after an 80ms callback outlived a 1ms deadline). The client
  also invoked the pipeline with its own default deadline — a longer
  caller timeout could not extend it — and an expected pipeline timeout
  raised inside handle_call, taking the reusable client down with it.

  Astra's B ruling: one absolute budget from the public entry, queueing
  included; expiry at dequeue refuses the request without invoking
  callbacks; the one-shot helper goes unless it has an isolation contract;
  the reusable client survives expected request failures and raises them
  at the caller boundary. Admission refusals are expected request failures
  too (FX-001 closure): `call/3` raises `FlowExtra.AdmissionError` at the
  boundary with the client alive, and `cast/2` reports the pipeline's
  refusal instead of acknowledging a send the pipeline never accepted.
  """

  test "a caught call! timeout leaves no leaked helper behind" do
    pipeline = ClientTrapPipeline.start()
    {:links, before_links} = Process.info(self(), :links)

    released =
      try do
        FlowExtra.Client.call!(pipeline, %ClientTrapPipeline{report_to: self(), ref: :leak}, 40)
        :returned
      rescue
        FlowExtra.PipelineError -> :raised
      catch
        :exit, _ -> :exited
      end

    assert released in [:raised, :exited]

    # The work settles, then the caller's link set must be what it was —
    # no helper process left linked and alive.
    assert_receive {:worked, :leak}, 2_000
    {:links, after_links} = Process.info(self(), :links)
    assert after_links -- before_links == []
    refute_receive %FlowExtra.IP{}, 200
  end

  test "the reusable client survives an expected timeout and keeps serving" do
    pipeline = ClientTrapPipeline.start()
    {:ok, client} = FlowExtra.Client.start(pipeline)

    error =
      assert_raise FlowExtra.PipelineError, fn ->
        FlowExtra.Client.call(client, %ClientTrapPipeline{report_to: self(), ref: :first}, 40)
      end

    assert error.reason == :timeout
    assert Process.alive?(client)

    assert_receive {:worked, :first}, 2_000

    assert %ClientTrapPipeline{} =
             FlowExtra.Client.call(client, %ClientTrapPipeline{ref: :second}, 1_000)
  end

  test "a queued request that expires is refused without invoking callbacks" do
    pipeline = ClientTrapPipeline.start()
    {:ok, client} = FlowExtra.Client.start(pipeline)

    first_ref = make_ref()
    queued_ref = make_ref()

    # The struct is built here so report_to is this test process, not the
    # spawned caller.
    first_struct = %ClientTrapPipeline{report_to: self(), ref: first_ref}

    spawn(fn -> FlowExtra.Client.call(client, first_struct, 2_000) end)

    # Let the first request take the server, then queue one with a budget
    # that dies while it waits.
    Process.sleep(10)

    released =
      try do
        FlowExtra.Client.call(client, %ClientTrapPipeline{ref: queued_ref}, 40)
        :returned
      rescue
        FlowExtra.PipelineError -> :raised
      catch
        :exit, _ -> :exited
      end

    assert released in [:raised, :exited]

    # Only the first request's callbacks ever ran: the queued one expired
    # behind it and must not begin work.
    assert_receive {:worked, ^first_ref}, 2_000
    refute_receive {:worked, ^queued_ref}, 300
  end

  test "one budget covers queueing and execution — beyond the engine's default" do
    pipeline = ClientSlowPipeline.start()
    {:ok, client} = FlowExtra.Client.start(pipeline)

    # 5_500ms of work inside an 8_000ms budget: the engine's 5_000ms
    # default would refuse this, so a completed call proves the caller's
    # budget reached the engine instead of being re-defaulted.
    assert %ClientSlowPipeline{} =
             FlowExtra.Client.call(client, %ClientSlowPipeline{number: 1}, 8_000)
  end

  test "an overload refusal raises AdmissionError at the boundary and the client survives" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})

    assert :ok =
             AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
               observer: self(),
               id: :held
             })

    assert_receive {:entered, :held, worker}
    {:ok, client} = FlowExtra.Client.start(pipeline)
    Process.unlink(client)

    outcome =
      try do
        FlowExtra.Client.call(client, %AdmissionTrapPipeline{observer: self(), id: :refused}, 100)
      rescue
        e -> {:raised, e.__struct__}
      catch
        :exit, _ -> :exited
      end

    # The refusal is the caller's to see — raised, not a silent client
    # exit — and the reusable client lives to serve again.
    assert {outcome, Process.alive?(client)} == {{:raised, FlowExtra.AdmissionError}, true}

    send(worker, :release)
    assert_receive {:finished, :held}, 2_000
    FlowExtra.Client.stop(client)
  end

  test "Client.cast reports the pipeline's admission refusal" do
    pipeline = AdmissionTrapPipeline.start(%{admission_capacity: 1})

    assert :ok =
             AdmissionTrapPipeline.cast(pipeline, %AdmissionTrapPipeline{
               observer: self(),
               id: :held
             })

    assert_receive {:entered, :held, worker}
    {:ok, client} = FlowExtra.Client.start(pipeline)

    assert {:error, :overloaded} =
             FlowExtra.Client.cast(client, %AdmissionTrapPipeline{observer: self(), id: :refused})

    send(worker, :release)
    assert_receive {:finished, :held}, 2_000
    FlowExtra.Client.stop(client)
  end
end
