defmodule FlowExtra.PipelineBuilder.AdmissionTest do
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
    # FX-007: the sync engine prepares module options at startup now, so the
    # refusal is immediate and caller-side — the same law as the async track.
    # (The old pinned behavior — start fine, crash the first caller far from
    # the cause — is the defect this trap replaced.)
    assert_raise ArgumentError, ~r/BadInitModule\.init\/1/, fn ->
      BadModuleInitPipelineSync.start()
    end
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
