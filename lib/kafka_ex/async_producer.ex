defmodule KafkaEx.AsyncProducer do
  @moduledoc """
  An asynchronous Kafka producer. Messages are buffered in memory until delivery criteria (buffer
  size or timeout) is met and then delivered in batches to the Kafka cluster. Delivery to the Kafka
  cluster is handled in the background so that clients may produce new messages concurrently with
  the network requests to the Kafka cluster.

  ## Options

  An AsyncProducer can be configured with the following options:

    * `:delivery_threshold`: number of messages that can be batched before publishing enqueued
      messages to Kafka cluster. Messages will be delivered every `:delivery_threshold` messages
      when messages are produced at a rate faster than `1000 * delivery_threshold/delivery_interval`
      messages per second.
    * `:delivery_interval`: number of milliseconds that messages may be queued in memory before
      being published to Kafka. Messages will be delivered every `:delivery_interval` milliseconds
      when messages are produced at a rate slower than `1000 * delivery_threshold/delivery_interval`
      messages per second.
    * `:max_request_bytesize`: maximum size (in bytes) of request that can be sent to the Kafka
      cluster. If the undelivered messages for a partition exceeds this limit, then the messages will be
      delivered in multiple requests.
    * `:required_acks`: number of brokers that must ACK a request to consider the request a success.
    * `:timeout`: number of milliseconds to wait for Kafka brokers to ACK a request.
    * `:compression`: compression to apply to produce requests. May be `:snappy`, :gzip`, or `:none`.
    * `:partitioner`: a KafkaEx.Partitioner used to assign messages to a partition.
  """

  alias KafkaEx.AsyncProducer.{Supervisor, Queue}
  alias KafkaEx.Protocol.Produce.Message

  @type topic :: binary

  @type option :: Supervisor.option
  @type options :: [option]

  @type call_option :: {:name, Elixir.Supervisor.supervisor}
                     | {:timeout, timeout}
  @type call_options :: [call_option]

  @default_name __MODULE__

  @doc """
  Starts an AsyncProducer.
  """
  @spec start_link(options, GenServer.options) :: Elixir.Supervisor.on_start
  def start_link(opts \\ [], server_opts \\ []) do
    Supervisor.start_link(opts, Keyword.put_new(server_opts, :name, @default_name))
  end

  @doc """
  Send a message to a topic in a Kafka cluster.
  """
  @spec produce(topic, Message.t, call_options) :: :ok | {:error, :restarting} | {:error, any}
  def produce(topic, %Message{value: value} = message, opts \\ []) when is_binary(topic) and is_binary(value) do
    case pid_of(Queue, opts) do
      :restarting ->
        {:error, :restarting}

      pid ->
        Queue.produce(pid, topic, message, Keyword.get(opts, :timeout, 5000))
    end
  end

  defp pid_of(child, opts) do
    Keyword.get(opts, :name, @default_name)
    |> Supervisor.child_pid(child)
  end
end
