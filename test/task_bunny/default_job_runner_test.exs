defmodule TaskBunny.DefaultJobRunnerTest do
  use ExUnit.Case
  alias TaskBunny.DefaultJobRunner

  defmodule SampleJobs do
    defmodule CrashJob do
      use TaskBunny.Job

      def perform(_payload) do
        raise "Oops"
      end
    end

    defmodule TimeoutJob do
      use TaskBunny.Job

      def timeout, do: 10

      def perform(_payload) do
        :timer.sleep(10_000)
        :ok
      end
    end

    defmodule NormalJob do
      use TaskBunny.Job

      def perform(_payload) do
        :ok
      end
    end

    defmodule ErrorJob do
      use TaskBunny.Job

      def perform(_payload) do
        {:error, "failed!"}
      end
    end

    defmodule PayloadJob do
      use TaskBunny.Job

      def perform(payload) do
        {:ok, payload}
      end
    end
  end

  describe "invoke" do
    defp message(job, payload) do
      job
      |> TaskBunny.Message.encode!(payload)
      |> TaskBunny.Message.decode!()
    end

    test "runs the job and notifies when it has finished" do
      payload = %{hello: "world"}
      meta = %{a: "b"}
      decoded = message(SampleJobs.NormalJob, payload)
      DefaultJobRunner.invoke(decoded, meta)

      assert_receive {:job_finished, :ok, ^decoded, ^meta}
    end

    test "invokes perform method with the given payload" do
      payload = %{hello: "world"}
      decoded = message(SampleJobs.PayloadJob, payload)
      DefaultJobRunner.invoke(decoded, nil)

      assert_receive {:job_finished, {:ok, %{"hello" => "world"}}, ^decoded, nil}
    end

    test "handles job error" do
      decoded = message(SampleJobs.ErrorJob, nil)
      DefaultJobRunner.invoke(decoded, nil)

      assert_receive {
        :job_finished,
        {:error, %{return_value: {:error, "failed!"}}},
        ^decoded,
        nil
      }
    end

    test "handles job crashing" do
      decoded = message(SampleJobs.CrashJob, nil)
      DefaultJobRunner.invoke(decoded, nil)

      assert_receive {:job_finished, {:error, _}, ^decoded, nil}
    end

    test "handles timed-out job" do
      decoded = message(SampleJobs.TimeoutJob, nil)
      DefaultJobRunner.invoke(decoded, nil)

      assert_receive {:job_finished, {:error, _}, ^decoded, nil}, 1000
    end
  end
end
