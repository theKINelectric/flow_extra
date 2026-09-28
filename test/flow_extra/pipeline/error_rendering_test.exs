defmodule FlowExtra.Pipeline.ErrorRenderingTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-008 trap (pursuit A4, "readable failures"): the call failure paths
  assigned atoms and raw exit reasons to PipelineError's `message` field,
  and the default defexception message renders a diagnostic —
  "(expected a string)" — instead of a timeout or crash report. The
  machine-readable cause belongs in its own field; `Exception.message/1`
  must return a useful string for timeout, missing process, and structured
  crash reasons alike.
  """

  defmodule CrashPipeline do
    use FlowExtra.Pipeline

    defstruct data: nil

    pipe(:die)

    def die(_struct, _opts), do: exit(:boom)
  end

  test "a timeout renders a readable message and keeps the machine reason" do
    pipeline = DeadlinePipeline.start()

    error =
      assert_raise FlowExtra.PipelineError, fn ->
        DeadlinePipeline.call(pipeline, %DeadlinePipeline{number: 1}, 1)
      end

    assert error.reason == :timeout
    assert Exception.message(error) =~ "deadline"
    refute Exception.message(error) =~ "expected a string"
  end

  test "a vanished pipeline renders a readable message" do
    pipeline = DeadlinePipeline.start()
    DeadlinePipeline.stop(pipeline)
    wait_until(fn -> GenServer.whereis(pipeline.out_name) == nil end)

    error =
      assert_raise FlowExtra.PipelineError, fn ->
        DeadlinePipeline.call(pipeline, %DeadlinePipeline{number: 1})
      end

    assert error.reason == :noprocess
    assert Exception.message(error) =~ "not running"
    refute Exception.message(error) =~ "expected a string"
  end

  test "a crash cascade renders a readable message and keeps its reason" do
    pipeline = CrashPipeline.start()

    error =
      assert_raise FlowExtra.PipelineError, fn ->
        CrashPipeline.call(pipeline, %CrashPipeline{data: :die})
      end

    # The stage dies with :boom; rest_for_one tears the line down and the
    # caller's monitor sees the consumer's :shutdown — the teardown, not the
    # origin. Whatever the term, the rendering must be a string and the
    # reason must survive intact.
    assert error.reason == :shutdown
    assert is_binary(Exception.message(error))
    refute Exception.message(error) =~ "expected a string"
  end

  test "an explicit string message still wins" do
    error = %FlowExtra.PipelineError{message: "caller-supplied context"}

    assert Exception.message(error) == "caller-supplied context"
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
