defmodule FlowExtra.Sync.PreparationParityTest do
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

  The token instruments make the law observable four ways: preparation
  happens before any request exists, in the starting caller's process,
  under an engine tag, with per-run tokens that must be exactly the ones
  later requests execute on — across restarts too.
  """

  describe "startup preparation, observed before any request" do
    test "the async engine prepares once per replica, in the starting caller" do
      PreparationTokenPipeline.start(%{observer: self(), engine: :async})

      assert length(collect_preparations(:async, 3)) == 3
      # Preparation runs in the starting caller — this test process — not in
      # workers or request walkers.
      assert Enum.all?(collect_processes(), &(&1 == self()))
      refute_received _
    end

    test "the sync engine prepares once per declared occurrence, in the starting caller" do
      PreparationTokenPipelineSync.start(%{observer: self(), engine: :sync})

      # Same declaration (count: 3): the sync engine executes one
      # representative per stage, so one preparation — not three, not
      # per-request.
      assert length(collect_preparations(:sync, 1)) == 1
      assert Enum.all?(collect_processes(), &(&1 == self()))
      refute_received _
    end
  end

  describe "requests run on the prepared options" do
    test "results carry the startup-minted tokens, and no request mints another" do
      async_pipeline = PreparationTokenPipeline.start(%{observer: self(), engine: :async})
      sync_pipeline = PreparationTokenPipelineSync.start(%{observer: self(), engine: :sync})

      async_tokens = collect_preparations(:async, 3)
      [sync_token] = collect_preparations(:sync, 1)

      for _ <- 1..3 do
        async_result = PreparationTokenPipeline.call(async_pipeline, %PreparationTokenPipeline{})
        assert async_result.prepared_token in async_tokens

        sync_result =
          PreparationTokenPipelineSync.call(sync_pipeline, %PreparationTokenPipelineSync{})

        assert sync_result.prepared_token == sync_token
      end

      # No request, on either engine, ran init/1 again.
      refute_received {:prepared, _, _, _}
    end

    test "a restart reuses the prepared options instead of re-running init/1" do
      {:ok, parent} = Supervisor.start_link([], strategy: :one_for_one)

      async_pipeline =
        PreparationTokenPipeline.supervised_start(parent, %{observer: self(), engine: :async})

      sync_pipeline =
        PreparationTokenPipelineSync.supervised_start(parent, %{observer: self(), engine: :sync})

      async_tokens = collect_preparations(:async, 3)
      [sync_token] = collect_preparations(:sync, 1)

      for pipeline <- [async_pipeline, sync_pipeline] do
        old_pid = GenServer.whereis(pipeline.sup_name)

        Process.exit(old_pid, :kill)

        wait_until(fn ->
          new_pid = GenServer.whereis(pipeline.sup_name)
          is_pid(new_pid) and new_pid != old_pid
        end)
      end

      for _ <- 1..2 do
        async_result = PreparationTokenPipeline.call(async_pipeline, %PreparationTokenPipeline{})
        assert async_result.prepared_token in async_tokens

        sync_result =
          PreparationTokenPipelineSync.call(sync_pipeline, %PreparationTokenPipelineSync{})

        assert sync_result.prepared_token == sync_token
      end

      # Restarts and post-restart calls added no preparation.
      refute_received {:prepared, _, _, _}
    end
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

  test "the sync engine validates declared counts like the async engine" do
    assert_raise ArgumentError, ~r/no greater than 100/, fn ->
      SyncCountCapPipeline.start()
    end
  end

  test "all declarations are validated before any module initializer runs" do
    # A valid module pipe declared before an invalid count: the whole
    # declaration must be refused without executing the earlier module's
    # init/1 — otherwise a failed start leaves half-prepared side effects.
    assert_raise ArgumentError, ~r/no greater than 100/, fn ->
      LateBadCountPipeline.start(%{observer: self(), engine: :async})
    end

    assert_raise ArgumentError, ~r/no greater than 100/, fn ->
      LateBadCountPipelineSync.start(%{observer: self(), engine: :sync})
    end

    refute_received {:prepared, _, _, _}
  end

  defp collect_preparations(engine, expected) do
    Stream.repeatedly(fn ->
      receive do
        {:prepared, ^engine, _process, token} -> token
      after
        0 -> :done
      end
    end)
    |> Enum.take_while(&(&1 != :done))
    |> Enum.take(expected + 1)
    |> case do
      tokens when length(tokens) > expected ->
        flunk("expected exactly #{expected} #{engine} preparations, got more")

      tokens ->
        tokens
    end
  end

  defp collect_processes do
    Stream.repeatedly(fn ->
      receive do
        {:prepared, _engine, process, _token} -> process
      after
        0 -> :done
      end
    end)
    |> Enum.take_while(&(&1 != :done))
    |> Enum.to_list()
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
