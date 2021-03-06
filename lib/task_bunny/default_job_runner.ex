defmodule TaskBunny.DefaultJobRunner do
  @moduledoc """
  Handles job invocation concerns.

  DefaultJobRunner wraps up job execution and provides you abilities:

  - invoking jobs concurrently (unblocking job execution)
  - handling a job crashing
  - handling timeout

  ## Signal

  Once the job has finished it sends a message with a tuple consisted with:

  - atom indicating message type: :job_finished
  - result: :ok or {:error, details}
  - meta: meta data for the job

  After sending the messsage, DefaultJobRunner shuts down all processes it started.
  """
  require Logger
  alias TaskBunny.JobError

  @behaviour TaskBunny.JobRunner

  @doc ~S"""
  Invokes the given job with the given payload.

  The job is run in a separate process, which is killed after the job.timeout if the job has not finished yet.
  A :error message is send to the :job_finished of the caller if the job times out.
  """
  @impl TaskBunny.JobRunner
  def invoke(%{"job" => job, "payload" => payload} = decoded, meta) do
    caller = self()
    timeout_error = {:error, JobError.handle_timeout(job, payload)}
    timer = Process.send_after(caller, {:job_finished, timeout_error, decoded, meta}, job.timeout)

    pid =
      spawn(fn ->
        send(caller, {:job_finished, run_job(job, payload), decoded, meta})
        Process.cancel_timer(timer)
      end)

    :timer.kill_after(job.timeout + 10, pid)
  end

  # Performs a job with the given payload.
  # Any raises or throws in the perform are caught and turned into an :error tuple.
  @spec run_job(atom, any) :: :ok | {:ok, any} | {:error, any}
  defp run_job(job, payload) do
    case job.perform(payload) do
      :ok -> :ok
      {:ok, something} -> {:ok, something}
      error -> {:error, JobError.handle_return_value(job, payload, error)}
    end
  rescue
    error ->
      Logger.debug("TaskBunny.DefaultJobRunner - Runner rescued #{inspect(error)}")
      {:error, JobError.handle_exception(job, payload, error, __STACKTRACE__)}
  catch
    _, reason ->
      Logger.debug("TaskBunny.DefaultJobRunner - Runner caught reason: #{inspect(reason)}")
      {:error, JobError.handle_exit(job, payload, reason, __STACKTRACE__)}
  end
end
