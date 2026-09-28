defmodule UnhandledErrorTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use FlowExtra.Pipeline

    defstruct data: nil
    pipe(:one, count: 2)
    pipe(:fun, count: 2)
    pipe(:second, count: 2)
    pipe(:at_the_end, count: 2)
    error_pipe(:if_error, count: 2)

    def one(struct, _opts), do: struct

    def fun(struct, _opts) do
      if struct.data == :fail do
        Process.exit(self(), :kill)
      else
        struct
      end
    end

    def second(struct, _opts), do: struct
    def at_the_end(struct, _opts), do: struct

    def if_error(error, _struct, _opts) do
      raise error
    end
  end

  describe "with crash" do
    test "raises a FlowExtra.PipelineError but still works" do
      pipeline = Pipeline.start()

      assert_raise FlowExtra.PipelineError, fn ->
        Pipeline.call(pipeline, %Pipeline{data: :fail})
      end

      Process.sleep(100)
      assert Pipeline.call(pipeline, %Pipeline{data: :ok}) == %Pipeline{data: :ok}

      # one more time
      assert_raise FlowExtra.PipelineError, fn ->
        Pipeline.call(pipeline, %Pipeline{data: :fail})
      end

      Process.sleep(100)
      assert Pipeline.call(pipeline, %Pipeline{data: :ok}) == %Pipeline{data: :ok}
    end
  end

  describe "supervisor crash" do
    setup do
      {:ok, supervisor_pid} = Supervisor.start_link([], strategy: :one_for_one)
      pipeline = Pipeline.supervised_start(supervisor_pid)
      old_pid = GenServer.whereis(pipeline.sup_name)

      pid = GenServer.whereis(pipeline.sup_name)
      Process.exit(pid, :kill)
      Process.sleep(200)

      {:ok, pipeline: pipeline, old_pid: old_pid}
    end

    test "kills supervisor", %{old_pid: old_pid} do
      refute Process.alive?(old_pid)
    end

    test "restarts successfully", %{pipeline: pipeline} do
      assert Pipeline.call(pipeline, %Pipeline{data: :ok}) == %Pipeline{data: :ok}
    end
  end
end
