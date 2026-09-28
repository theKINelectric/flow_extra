defmodule FlowExtra.Pipeline.SupervisedInitTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-002 trap (pursuit A1, "two doors, one law"): `start/1` ran the DSL's
  `init/1` at admission, but `supervised_start/2` passed the raw opts
  straight to the builder — so moving the same pipeline under a supervisor
  silently dropped derived configuration and skipped initialization-time
  validation. Both entry points must run the declared initialization policy
  and refuse the same invalid results; a restart then reuses the prepared
  options (init does not run again in a restarted child).
  """

  setup do
    {:ok, parent} = Supervisor.start_link([], strategy: :one_for_one)
    %{parent: parent}
  end

  describe "async engine" do
    test "standalone and supervised startup run init/1 identically", %{parent: parent} do
      standalone = SupervisedInitFunPipeline.start()
      supervised = SupervisedInitFunPipeline.supervised_start(parent)

      assert %SupervisedInitFunPipeline{initialized: true} =
               SupervisedInitFunPipeline.call(standalone, %SupervisedInitFunPipeline{})

      assert %SupervisedInitFunPipeline{initialized: true} =
               SupervisedInitFunPipeline.call(supervised, %SupervisedInitFunPipeline{})
    end

    test "an invalid init/1 result is refused on both entry points", %{parent: parent} do
      assert_raise ArgumentError, ~r/BadPipelineInitStandalone\.init\/1/, fn ->
        BadPipelineInitStandalone.start()
      end

      assert_raise ArgumentError, ~r/BadPipelineInitStandalone\.init\/1/, fn ->
        BadPipelineInitStandalone.supervised_start(parent)
      end
    end
  end

  describe "sync engine" do
    test "standalone and supervised startup run init/1 identically", %{parent: parent} do
      standalone = SupervisedInitFunPipelineSync.start()
      supervised = SupervisedInitFunPipelineSync.supervised_start(parent)

      assert %SupervisedInitFunPipelineSync{initialized: true} =
               SupervisedInitFunPipelineSync.call(standalone, %SupervisedInitFunPipelineSync{})

      assert %SupervisedInitFunPipelineSync{initialized: true} =
               SupervisedInitFunPipelineSync.call(supervised, %SupervisedInitFunPipelineSync{})
    end

    test "an invalid init/1 result is refused on both entry points", %{parent: parent} do
      assert_raise ArgumentError, ~r/BadPipelineInitStandaloneSync\.init\/1/, fn ->
        BadPipelineInitStandaloneSync.start()
      end

      assert_raise ArgumentError, ~r/BadPipelineInitStandaloneSync\.init\/1/, fn ->
        BadPipelineInitStandaloneSync.supervised_start(parent)
      end
    end
  end
end
