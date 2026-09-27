defmodule Flowex.Sync.PreparationParityTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-007 trap (pursuit A remainder, "when does init happen"): the async
  engine prepared module options at build time — once per replica — while
  the sync walker called module init/1 during every request, so stateful
  initialization, generated references, or resource allocation differed
  between the production and debug engines (audit probe: one init on the
  async track, two for two calls on the sync track). The sync track also
  skipped the count validator, learning of a bad declaration only by
  crashing on a call. One preparation law (Astra's FX-007 ruling): module
  init/1 runs in the starting caller at explicit startup — once per
  declared occurrence on the sync track, once per replica on the async
  track — and the prepared options are reused for every request and for
  supervisor-driven restarts. Never per packet.
  """

  test "module options are prepared at startup, not per request" do
    async_pipeline = PreparationFrequencyPipeline.start(%{observer: self()})
    sync_pipeline = PreparationFrequencyPipelineSync.start(%{observer: self()})

    for _ <- 1..2 do
      PreparationFrequencyPipeline.call(async_pipeline, %PreparationFrequencyPipeline{})
      PreparationFrequencyPipelineSync.call(sync_pipeline, %PreparationFrequencyPipelineSync{})
    end

    # One preparation per engine: the async track's single replica and the
    # sync track's single occurrence — not one per request.
    assert drain_count(:module_init) == 2
  end

  test "error-stage modules are prepared at startup even when no error occurs" do
    async_pipeline = PreparationErrorStagePipeline.start(%{observer: self()})
    sync_pipeline = PreparationErrorStagePipelineSync.start(%{observer: self()})

    for _ <- 1..2 do
      PreparationErrorStagePipeline.call(async_pipeline, %PreparationErrorStagePipeline{})
      PreparationErrorStagePipelineSync.call(sync_pipeline, %PreparationErrorStagePipelineSync{})
    end

    assert drain_count(:error_module_init) == 2
  end

  test "a restart reuses prepared options instead of re-running init/1" do
    {:ok, parent} = Supervisor.start_link([], strategy: :one_for_one)

    async_pipeline = PreparationFrequencyPipeline.supervised_start(parent, %{observer: self()})
    sync_pipeline = PreparationFrequencyPipelineSync.supervised_start(parent, %{observer: self()})

    for pipeline <- [async_pipeline, sync_pipeline] do
      old_pid = GenServer.whereis(pipeline.sup_name)
      Process.exit(old_pid, :kill)

      wait_until(fn ->
        new_pid = GenServer.whereis(pipeline.sup_name)
        is_pid(new_pid) and new_pid != old_pid
      end)
    end

    for _ <- 1..2 do
      PreparationFrequencyPipeline.call(async_pipeline, %PreparationFrequencyPipeline{})
      PreparationFrequencyPipelineSync.call(sync_pipeline, %PreparationFrequencyPipelineSync{})
    end

    # Two preparations total (one per engine, at startup) — the restarts
    # and the post-restart calls must not add a third.
    assert drain_count(:module_init) == 2
  end

  test "the sync engine validates declared counts like the async engine" do
    assert_raise ArgumentError, ~r/no greater than 100/, fn ->
      SyncCountCapPipeline.start()
    end
  end

  defp drain_count(message) do
    receive do
      ^message -> drain_count(message) + 1
    after
      0 -> 0
    end
  end

  defp wait_until(fun, attempts_left \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition was not met before the deadline")

  defp wait_until(fun, attempts_left) do
    unless fun.() do
      Process.sleep(10)
      wait_until(fun, attempts_left - 1)
    end
  end
end
