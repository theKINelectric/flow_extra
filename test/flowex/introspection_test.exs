defmodule IntrospectionTest do
  use ExUnit.Case, async: true

  describe "InitOptsFunPipeline .pipes" do
    test "returns pipe list" do
      assert InitOptsFunPipeline.pipes() == [{:component, 2, %{from_opts: 3}, :pipe}]
    end
  end

  describe "InitOptsFunPipeline .pipe_info" do
    test "returns pipe info" do
      pipe_info = InitOptsFunPipeline.pipe_info(:component)

      assert pipe_info[:name] == :component
      assert pipe_info[:count] == 2
      assert pipe_info[:opts] == %{from_opts: 3}
      assert pipe_info[:type] == :pipe
    end
  end

  describe "InitOptsModulePipeline .pipes" do
    test "returns pipe list" do
      assert InitOptsModulePipeline.pipes() == [{OptComponent, 2, %{from_opts: 3}, :pipe}]
    end
  end

  describe "InitOptsModulePipeline .pipe_info" do
    test "returns pipe info" do
      pipe_info = InitOptsModulePipeline.pipe_info(OptComponent)

      assert pipe_info[:name] == OptComponent
      assert pipe_info[:count] == 2
      assert pipe_info[:opts] == %{from_opts: 3}
      assert pipe_info[:type] == :pipe
    end
  end
end
