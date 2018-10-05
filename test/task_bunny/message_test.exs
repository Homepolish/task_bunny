defmodule TaskBunny.MessageTest do
  use ExUnit.Case, async: true
  alias TaskBunny.{Message, JobError}

  defmodule NameJob do
    use TaskBunny.Job

    def perform(payload), do: {:ok, payload["name"]}
  end

  describe "encode/decode message body(payload)" do
    test "encode and decode payload" do
      {:ok, encoded} =
        Message.encode(NameJob, %{"name" => "Joe"}, id: "some-id", extra: %{foo: "bar"})

      {:ok,
       %{"job" => job, "payload" => payload, "id" => "some-id", "extra" => %{"foo" => "bar"}}} =
        Message.decode(encoded)

      assert job.perform(payload) == {:ok, "Joe"}
    end

    test "decode broken json" do
      message = "{aaa:bbb}"
      assert {:error, {:json_decode_error, _}} = Message.decode(message)
    end

    test "decode wrong format" do
      message = "{\"foo\": \"bar\"}"
      assert {:error, {:decode_exception, _}} = Message.decode(message)
    end

    test "decode invalid job" do
      encoded = Message.encode!(InvalidJob, %{"name" => "Joe"})
      assert {:error, :job_not_loaded} == Message.decode(encoded)
    end

    test "decode invalid atom" do
      message =
        "{\"payload\": \"\",\"job\":\"Hello.Message\",\"created_at\":\"2017-02-17T10:14:13.149734Z\"}"

      assert {:error, :job_not_loaded} == Message.decode(message)
    end
  end

  describe "add_error_log" do
    @tag timeout: 1000
    test "adds error information to the message" do
      message =
        NameJob
        |> Message.encode!(%{"name" => "Joe"})
        |> Message.decode!()

      error = %JobError{
        error_type: :return_value,
        return_value: {:error, :test_error},
        failed_count: 0,
        stacktrace: nil,
        raw_body: "abcdefg"
      }

      assert %{"errors" => [added | _]} = Message.add_error_log(message, error)
      assert added["result"][:error_type] == ":return_value"
      assert added["result"][:return_value] == "{:error, :test_error}"
      refute added["result"][:raw_body]
    end
  end
end
