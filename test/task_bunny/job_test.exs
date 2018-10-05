defmodule TaskBunny.JobTest do
  use ExUnit.Case
  import TaskBunny.QueueTestHelper
  alias TaskBunny.{Job, Queue, Message, QueueTestHelper}

  @queue "task_bunny.job_test"

  defmodule TestJob do
    use Job
    def perform(_payload), do: :ok
  end

  setup do
    clean(Queue.queue_with_subqueues(@queue))
    Queue.declare_with_subqueues(:default, @queue)

    :ok
  end

  describe "enqueue" do
    test "enqueues job" do
      payload = %{"foo" => "bar"}
      :ok = TestJob.enqueue(payload, queue: @queue)

      {received, _} = QueueTestHelper.pop(@queue)
      {:ok, %{"payload" => received_payload}} = Message.decode(received)
      assert received_payload == payload
    end

    test "returns an error for wrong option" do
      payload = %{"foo" => "bar"}

      assert {:error, _} =
               TestJob.enqueue(
                 payload,
                 queue: @queue,
                 host: :invalid_host
               )
    end

    test "enqueues optional data" do
      payload = %{"foo" => "bar"}
      job_id = "foo"
      extra = %{"foo" => "bar"}
      :ok = TestJob.enqueue(payload, queue: @queue, id: job_id, extra: extra)

      {received, _} = QueueTestHelper.pop(@queue)

      {:ok, %{"payload" => rec_payload, "extra" => rec_extra, "id" => rec_job_id}} =
        Message.decode(received)

      assert rec_payload == payload
      assert rec_extra == extra
      assert rec_job_id == job_id
    end
  end

  describe "enqueue!" do
    test "raises an exception for a wrong host" do
      payload = %{"foo" => "bar"}

      assert_raise TaskBunny.Publisher.PublishError, fn ->
        TestJob.enqueue!(payload, queue: @queue, host: :invalid_host)
      end
    end
  end
end
