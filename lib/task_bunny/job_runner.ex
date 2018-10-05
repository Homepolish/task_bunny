defmodule TaskBunny.JobRunner do
  @moduledoc """
  Callbacks required to be provided by JobRunner modules.
  """

  @callback invoke(map, any) :: :ok | {:error, any}
end
