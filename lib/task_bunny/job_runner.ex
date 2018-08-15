defmodule TaskBunny.JobRunner do
  @moduledoc """
  Callbacks required to be provided by JobRunner modules.
  """
  @callback invoke(atom, any, {any, any}) :: :ok | {:error, any}
end
