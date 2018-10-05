defmodule TaskBunny.Worker do
  @moduledoc """
  A GenServer that listens a queue and consumes messages.

  You don't have to call or start worker explicitly.
  TaskBunny loads config and starts workers automatically for you.

  """

  use GenServer
  require Logger

  alias TaskBunny.Config
  alias TaskBunny.Connection
  alias TaskBunny.Consumer
  alias TaskBunny.FailureBackend
  alias TaskBunny.Message
  alias TaskBunny.Publisher
  alias TaskBunny.Queue
  alias TaskBunny.Worker

  @typedoc """
  Struct that represents a state of the worker GenServer.
  """
  @type t :: %__MODULE__{
          queue: String.t(),
          host: atom,
          concurrency: integer,
          channel: AMQP.Channel.t() | nil,
          consumer_tag: String.t() | nil,
          runners: integer,
          job_stats: %{
            failed: integer,
            succeeded: integer,
            rejected: integer
          }
        }

  defstruct queue: nil,
            host: :default,
            concurrency: 1,
            channel: nil,
            consumer_tag: nil,
            runners: 0,
            job_stats: %{
              failed: 0,
              succeeded: 0,
              rejected: 0
            }

  # Starts a worker for a job with the given config options.
  @doc false
  @spec start_link(list) :: GenServer.on_start()
  def start_link(config) when is_list(config) do
    %Worker{
      host: config[:host] || :default,
      queue: config[:queue],
      concurrency: config[:concurrency]
    }
    |> start_link()
  end

  # Starts a worker given a worker's state
  @doc false
  @spec start_link(t) :: GenServer.on_start()
  def start_link(state = %Worker{}) do
    GenServer.start_link(__MODULE__, state, name: pname(state.queue))
  end

  # Initializes GenServer. Send a request for RabbitMQ connection
  @doc false
  @spec init(t) :: {:ok, t} | {:stop, :connection_not_ready}
  def init(state = %Worker{}) do
    Logger.info(log_msg("initializing", state))

    case Connection.subscribe_connection(state.host, self()) do
      :ok ->
        Process.flag(:trap_exit, true)
        {:ok, state}

      _ ->
        {:stop, :connection_not_ready}
    end
  end

  # Closes the AMQP Channel, when the worker exit is captured.
  @doc false
  @spec terminate(any, TaskBunny.Worker.t()) :: :normal
  def terminate(_reason, state) do
    Logger.info(log_msg("terminating", state))
    if state.channel, do: AMQP.Channel.close(state.channel)
    :normal
  end

  @doc """
  Stops consuming messages from queue.
  Note this doesn't terminate the process and the jobs currently running will continue so.
  """
  @spec stop_consumer(pid) :: :ok
  def stop_consumer(pid) do
    if Process.alive?(pid), do: send(pid, {:stop_consumer})
    :ok
  end

  @doc false
  @spec handle_info(any, t) :: {:noreply, t} | {:stop, reason :: term, t}
  def handle_info(message, state)

  def handle_info({:stop_consumer}, state = %Worker{}) do
    if state.channel && state.consumer_tag do
      Logger.info(log_msg("stop consuming", state))
      Consumer.cancel(state.channel, state.consumer_tag)
      {:noreply, %{state | consumer_tag: nil}}
    else
      Logger.info(log_msg("received :stop_consumer but already stopped", state))
      {:noreply, state}
    end
  end

  # Called when connection to RabbitMQ was established.
  # Start consumer loop
  def handle_info({:connected, connection}, state = %Worker{}) do
    # Declares queue
    Queue.declare_with_subqueues(state.host, state.queue)

    # Consumes the queue
    case Consumer.consume(connection, state.queue, state.concurrency) do
      {:ok, channel, consumer_tag} ->
        Logger.info(log_msg("start consuming", state))
        {:noreply, %{state | channel: channel, consumer_tag: consumer_tag}}

      {:error, error} ->
        {:stop, {:failed_to_consume, error}, state}
    end
  end

  # Called when message was delivered from RabbitMQ.
  # Invokes a job here.
  def handle_info({:basic_deliver, body, meta}, state) do
    case Message.decode(body) do
      {:ok, %{"job" => job} = decoded} ->
        Logger.debug(log_msg("basic_deliver", state, body: decoded))

        start_callback(job, decoded)
        Config.job_runner().invoke(decoded, meta)

        {:noreply, %{state | runners: state.runners + 1}}

      error ->
        Logger.error(log_msg("basic_deliver invalid body", state, body: body, error: error))

        reject_message(state, body, meta)

        # Needs state.runners + 1, because reject_payload does state.runners - 1
        state = %{state | runners: state.runners + 1}
        {:noreply, update_job_stats(state, :rejected)}
    end
  end

  # Called when job was done.
  # Acknowledge to RabbitMQ.
  def handle_info({:job_finished, result, decoded, meta}, state) do
    Logger.debug(log_msg("job_finished", state, body: decoded, meta: meta))

    if succeeded?(result) do
      on_successful_job(state, decoded, meta, result)
    else
      on_failed_job(state, decoded, meta, result)
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Retrieve worker status
  @spec handle_call(atom, {pid, any}, any) :: {:reply, map, t}
  def handle_call(:status, _from, state) do
    channel =
      case state.channel do
        nil -> false
        _channel -> "#{state.queue} (#{state.consumer_tag})"
      end

    status = %TaskBunny.Status.Worker{
      queue: state.queue,
      runners: state.runners,
      channel: channel,
      stats: state.job_stats,
      consuming: !is_nil(state.consumer_tag)
    }

    {:reply, status, state}
  end

  @spec pname(String.t()) :: atom
  defp pname(queue) do
    String.to_atom("TaskBunny.Worker.#{queue}")
  end

  @spec update_job_stats(Worker.t(), :succeeded | :failed | :rejected) :: Worker.t()
  defp update_job_stats(state, success) do
    stats =
      case success do
        :succeeded -> %{state.job_stats | succeeded: state.job_stats.succeeded + 1}
        :failed -> %{state.job_stats | failed: state.job_stats.failed + 1}
        :rejected -> %{state.job_stats | rejected: state.job_stats.rejected + 1}
      end

    %{state | runners: state.runners - 1, job_stats: stats}
  end

  defp succeeded?(:ok), do: true
  defp succeeded?({:ok, _}), do: true
  defp succeeded?(_), do: false

  defp on_successful_job(state, %{"job" => job} = decoded, meta, result) do
    success_callback(job, decoded, result)
    Consumer.ack(state.channel, meta, true)
    {:noreply, update_job_stats(state, :succeeded)}
  end

  defp on_failed_job(state, %{"job" => job} = decoded, meta, {:error, job_error}) do
    failed_count = Message.failed_count(decoded) + 1

    job_error =
      Map.merge(job_error, %{
        raw_body: Jason.encode!(decoded),
        meta: meta,
        failed_count: failed_count,
        queue: state.queue,
        concurrency: state.concurrency,
        pid: self(),
        reject: failed_count > job.max_retry()
      })

    new_message = Message.add_error_log(decoded, job_error)

    FailureBackend.report_job_error(job_error)

    if reject?(job, failed_count, job_error) do
      reject_message(state, new_message, meta)
      reject_callback(job, new_message, Map.get(job_error, :return_value))

      {:noreply, update_job_stats(state, :rejected)}
    else
      retry_message(job, state, new_message, meta, failed_count)
      retry_callback(job, new_message, Map.get(job_error, :return_value))

      {:noreply, update_job_stats(state, :failed)}
    end
  end

  defp reject?(_, _, %{return_value: :reject}), do: true
  defp reject?(_, _, %{return_value: {:reject, _}}), do: true
  defp reject?(job, failed_count, _), do: failed_count > job.max_retry()

  @spec retry_message(atom, Worker.t(), map, any, integer) :: :ok
  defp retry_message(job, state, decoded, meta, failed_count) do
    retry_queue = Queue.retry_queue(state.queue)
    options = [expiration: "#{job.retry_interval(failed_count)}"]
    body = Jason.encode!(decoded)

    Publisher.publish(state.host, retry_queue, body, options)
    Consumer.ack(state.channel, meta, true)
  end

  @spec reject_message(Worker.t(), map | String.t(), any) :: :ok
  defp reject_message(state, maybe_decoded, meta) do
    rejected_queue = Queue.rejected_queue(state.queue)
    body = if is_map(maybe_decoded), do: Jason.encode!(maybe_decoded), else: maybe_decoded

    Publisher.publish(state.host, rejected_queue, body)
    Consumer.ack(state.channel, meta, true)
  end

  @spec start_callback(atom, map) :: :ok
  defp start_callback(job, decoded) do
    if :erlang.function_exported(job, :on_start, 1), do: job.on_start(decoded)
  end

  @spec success_callback(atom, map, term) :: :ok
  defp success_callback(job, decoded, result) do
    if :erlang.function_exported(job, :on_success, 2), do: job.on_success(decoded, result)
  end

  @spec retry_callback(atom, map, term) :: :ok
  defp retry_callback(job, decoded, result) do
    if :erlang.function_exported(job, :on_retry, 2), do: job.on_retry(decoded, result)
  end

  @spec reject_callback(atom, map, term) :: :ok
  defp reject_callback(job, decoded, result) do
    if :erlang.function_exported(job, :on_reject, 2), do: job.on_reject(decoded, result)
  end

  @spec log_msg(String.t(), atom | map, any) :: String.t()
  defp log_msg(message, state, additional \\ nil) do
    message =
      "TaskBunny.Worker: #{message}. Queue: #{state.queue}. Concurrency: #{state.concurrency}. PID: #{
        inspect(self())
      }."

    if additional do
      "#{message} #{inspect(additional)}"
    else
      message
    end
  end
end
