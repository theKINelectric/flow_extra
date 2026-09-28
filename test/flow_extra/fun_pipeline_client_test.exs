defmodule FunPipelineClientTest do
  use ExUnit.Case, async: true

  @opts %{a: :a, b: :b, c: :c}

  describe "FlowExtraClient .start/.stop" do
    test "starts and stops a client" do
      pipeline = FunPipeline.start(@opts)
      {:ok, client_pid} = FlowExtra.Client.start(pipeline)

      assert Process.alive?(client_pid) == true

      FlowExtra.Client.stop(client_pid)
      assert Process.alive?(client_pid) == false
    end
  end

  describe "FlowExtraClient .call" do
    test "returns 3 and sets a, b, c" do
      pipeline = FunPipeline.start(@opts)
      {:ok, client_pid} = FlowExtra.Client.start(pipeline)

      result = FlowExtra.Client.call(client_pid, %FunPipeline{number: 2})

      assert result.number == 3
      assert result.a == :a
      assert result.b == :b
      assert result.c == :c
    end

    test "returns the same results when running several times" do
      pipeline = FunPipeline.start(@opts)
      {:ok, client_pid} = FlowExtra.Client.start(pipeline)

      numbers =
        for _ <- 1..3 do
          FlowExtra.Client.call(client_pid, %FunPipeline{number: 2}).number
        end

      assert numbers == [3, 3, 3]
    end
  end

  describe "FlowExtraClient .cast" do
    test "receives result" do
      pipeline = FunPipelineCast.start()
      {:ok, client_pid} = FlowExtra.Client.start(pipeline)

      FlowExtra.Client.cast(client_pid, %FunPipelineCast{number: 2, pid: self()})

      assert_receive(3, 100)
    end
  end

  describe "FlowExtraClient .call!" do
    test "returns 3 and sets a, b, c" do
      pipeline = FunPipeline.start(@opts)

      result = FlowExtra.Client.call!(pipeline, %FunPipeline{number: 2})

      assert result.number == 3
      assert result.a == :a
      assert result.b == :b
      assert result.c == :c
    end

    test "returns the same results when running several times" do
      pipeline = FunPipeline.start(@opts)

      numbers =
        for _ <- 1..3 do
          FlowExtra.Client.call!(pipeline, %FunPipeline{number: 2}).number
        end

      assert numbers == [3, 3, 3]
    end
  end
end
