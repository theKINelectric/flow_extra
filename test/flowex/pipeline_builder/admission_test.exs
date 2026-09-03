defmodule Flowex.PipelineBuilder.AdmissionTest do
  use ExUnit.Case, async: true

  @moduledoc """
  T3 trap, remainder (TS-2, "admission"): the builder validates that counts
  are positive but puts no ceiling on them, and nothing anywhere checks what
  `init/1` returns — a module pipe or pipeline whose init returns garbage is
  accepted at build time and explodes at a distance (Protocol.UndefinedError
  on the async track, a wrapped FunctionClauseError on the sync track, or a
  crash on the first call long after start). Admission must refuse invalid
  data loudly, naming the culprit, before any of it becomes running topology.
  """

  test "a count above the cap is refused at build time" do
    assert_raise ArgumentError, ~r/no greater than 100/, fn ->
      CountCapPipeline.start()
    end
  end

  test "a module init that does not return a map is refused on the async track" do
    assert_raise ArgumentError, ~r/BadInitModule\.init\/1/, fn ->
      BadModuleInitPipeline.start()
    end
  end

  test "a module init that does not return a map is refused on the sync track" do
    pipeline = BadModuleInitPipelineSync.start()

    # The sync walker raises inside its GenServer, so the refusal reaches the
    # caller as its exit reason (wrapped by the GenServer call machinery) —
    # assert the death certificate names the culprit, whatever the nesting.
    caller =
      spawn(fn ->
        BadModuleInitPipelineSync.call(pipeline, %BadModuleInitPipelineSync{number: 1})
      end)

    ref = Process.monitor(caller)

    assert_receive {:DOWN, ^ref, _, _, reason}, 1_000
    assert inspect(reason) =~ "BadInitModule.init/1 must return a map"
  end

  test "a pipeline init that does not return a map is refused on the async track" do
    assert_raise ArgumentError, ~r/BadPipelineInitPipeline\.init\/1/, fn ->
      BadPipelineInitPipeline.start()
    end
  end

  test "a pipeline init that does not return a map is refused on the sync track" do
    assert_raise ArgumentError, ~r/BadPipelineInitPipelineSync\.init\/1/, fn ->
      BadPipelineInitPipelineSync.start()
    end
  end
end
