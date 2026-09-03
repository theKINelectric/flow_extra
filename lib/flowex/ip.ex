defmodule Flowex.IP do
  @moduledoc "Defines internal pipeline struct"

  @type t :: %__MODULE__{
          struct: map() | nil,
          requester: pid() | nil,
          error: term(),
          ref: reference() | nil
        }

  defstruct struct: nil, requester: nil, error: nil, ref: nil
end
