defmodule FlowExtra.IP do
  @moduledoc """
  Internal pipeline packet.

  `reply_to` is the revocable reply destination — a process alias owned
  by the caller, unaliased when the call ends, so a late result is dropped
  by the VM instead of rotting in a mailbox (FX-005). `deadline` is the
  packet's absolute local monotonic deadline in milliseconds (`nil` means
  it never expires); a stage that would begin after it skips the packet
  and marks `expired: true`, which the caller sees as its own timeout.
  Local to one BEAM node — do not treat these fields as a wire protocol.
  """

  @type t :: %__MODULE__{
          struct: map() | nil,
          requester: pid() | nil,
          error: term(),
          ref: reference() | nil,
          reply_to: reference() | pid() | nil,
          deadline: integer() | nil,
          expired: boolean() | nil
        }

  defstruct struct: nil,
            requester: nil,
            error: nil,
            ref: nil,
            reply_to: nil,
            deadline: nil,
            expired: nil
end
