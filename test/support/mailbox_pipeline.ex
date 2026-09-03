defmodule MailboxPipeline do
  @moduledoc """
  T4 trap fixture (TS-4, "mailbox swallow"): a minimal parallel pipeline used
  to observe what a `call/2` does to the calling process's mailbox.
  """

  use Flowex.Pipeline

  defstruct [:number]

  pipe(:do_nothing, count: 1)

  def do_nothing(struct, _opts), do: struct
end
