defmodule DualArityErrorPipe do
  @moduledoc """
  FX-004 instrument: an error module that defines both `call/2` (a normal
  module pipe's happy-path callback — must never run here) and `call/3`
  (the error handler). Both report to `opts[:observer]`, so a visit to the
  wrong track is observable, not just corrupting.
  """

  defstruct []

  def init(opts), do: opts

  def call(_struct, opts) do
    if observer = opts[:observer], do: send(observer, :wrong_track)
    %{wrong_track: true}
  end

  def call(_error, struct, opts) do
    if observer = opts[:observer], do: send(observer, :right_track)
    struct
  end
end

defmodule HappyTrackPipeline do
  use Flowex.Pipeline

  defstruct value: :original

  pipe(:identity)
  error_pipe(DualArityErrorPipe)

  def identity(data, _opts), do: data
end

defmodule HappyTrackPipelineSync do
  use Flowex.Sync.Pipeline

  defstruct value: :original

  pipe(:identity)
  error_pipe(DualArityErrorPipe)

  def identity(data, _opts), do: data
end

defmodule FailingTrackPipeline do
  use Flowex.Pipeline

  defstruct value: :original

  pipe(:boom)
  error_pipe(DualArityErrorPipe)

  def boom(_data, _opts), do: raise("boom")
end

defmodule FailingTrackPipelineSync do
  use Flowex.Sync.Pipeline

  defstruct value: :original

  pipe(:boom)
  error_pipe(DualArityErrorPipe)

  def boom(_data, _opts), do: raise("boom")
end
