defmodule InitializationTest do
  use ExUnit.Case, async: true

  describe "function pipeline" do
    test "returns values from different init functions" do
      pipeline = InitOptsFunPipeline.start(%{from_start: 1})

      result = InitOptsFunPipeline.call(pipeline, %InitOptsFunPipeline{})

      assert result == %InitOptsFunPipeline{from_start: 1, from_init: 2, from_opts: 3}
    end
  end

  describe "module pipeline" do
    test "returns values from different init functions" do
      pipeline = InitOptsModulePipeline.start(%{from_start: 1})

      result = InitOptsModulePipeline.call(pipeline, %InitOptsModulePipeline{})

      assert result == %InitOptsModulePipeline{
               from_start: 1,
               from_init: 2,
               from_opts: 3,
               component_init: 4
             }
    end
  end
end
