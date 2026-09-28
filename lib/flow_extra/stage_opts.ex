defmodule FlowExtra.StageOpts do
  @moduledoc "Stage state: what to run, how, and where it sits in the line."

  @type t :: %__MODULE__{
          type: :pipe | :error_pipe,
          module: module(),
          function: atom(),
          opts: map(),
          name: term(),
          producer_names: [term()]
        }

  defstruct [:type, :module, :function, :opts, :name, :producer_names]
end
