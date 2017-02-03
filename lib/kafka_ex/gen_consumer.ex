defmodule KafkaEx.GenConsumer do
  use GenServer

  alias KafkaEx.Config
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.OffsetCommit.Response, as: OffsetCommitResponse
  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.OffsetFetch.Response, as: OffsetFetchResponse
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
  alias KafkaEx.Protocol.Fetch.Message

  require Logger

  @type state :: any
  @type member :: binary
  @type members :: [member]
  @type topic :: binary
  @type partition_id :: integer
  @type partition :: {topic, partition_id}
  @type partitions :: [partition]
  @type assignments :: %{member => partitions}

  @callback init(topic, partition_id) :: {:ok, state}
  @callback handle_message(Message.t, state) :: {:ack, state} | {:checkpoint, state}
  @callback assign_partitions(members, partitions) :: assignments

  defmacro __using__(_opts) do
    quote do
      @behaviour KafkaEx.GenConsumer
      alias KafkaEx.Protocol.Fetch.Message

      def init(_topic, _partition) do
        {:ok, nil}
      end

      def assign_partitions(members, partitions) do
        Enum.zip(Stream.cycle(members), partitions)
        |> Enum.group_by(&(elem(&1, 0)), &(elem(&1, 1)))
      end

      defoverridable [init: 2, assign_partitions: 2]
    end
  end

  defmodule Supervisor do
    use Elixir.Supervisor

    def start_link(consumer_module, group_name, assignments, opts \\ []) do
      Elixir.Supervisor.start_link(__MODULE__, {consumer_module, group_name, assignments, opts})
    end

    def init({consumer_module, group_name, assignments, opts}) do
      children =
        Enum.map(assignments, fn ({topic, partition}) ->
          worker(KafkaEx.GenConsumer, [consumer_module, group_name, topic, partition, opts], id: {topic, partition})
        end)

      # TODO: how to shutdown the children in parallel?
      supervise(children, strategy: :one_for_one)
    end
  end

  defmodule State do
    @moduledoc false
    defstruct [
      :consumer_module,
      :consumer_state,
      :commit_interval,
      :commit_threshold,
      :worker_name,
      :group,
      :topic,
      :partition,
      :current_offset,
      :committed_offset,
      :acked_offset,
      :last_commit,
    ]
  end

  @commit_interval 5_000
  @commit_threshold 100

  # Client API

  def start_link(consumer_module, group, topic, partition, opts \\ []) do
    {server_opts, consumer_opts} = Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt])

    GenServer.start_link(__MODULE__, {consumer_module, group, topic, partition, consumer_opts}, server_opts)
  end

  # GenServer callbacks

  def init({consumer_module, group, topic, partition, opts}) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker)
    commit_interval = Keyword.get(opts, :commit_interval, Application.get_env(:kafka_ex, :commit_interval, @commit_interval))
    commit_threshold = Keyword.get(opts, :commit_threshold, Application.get_env(:kafka_ex, :commit_threshold, @commit_threshold))

    {:ok, consumer_state} = consumer_module.init(topic, partition)

    state = %State{
      consumer_module: consumer_module,
      consumer_state: consumer_state,
      commit_interval: commit_interval,
      commit_threshold: commit_threshold,
      worker_name: worker_name,
      group: group,
      topic: topic,
      partition: partition,
    }

    Process.flag(:trap_exit, true)

    {:ok, state, 0}
  end

  def handle_info(:timeout, %State{current_offset: nil, last_commit: nil} = state) do
    new_state = %State{load_offsets(state) | last_commit: :erlang.monotonic_time(:milli_seconds)}

    {:noreply, new_state, 0}
  end

  def handle_info(:timeout, %State{} = state) do
    new_state = consume(state)

    {:noreply, new_state, 0}
  end

  def terminate(_reason, %State{} = state) do
    commit(state)
  end

  # Helpers

  defp consume(%State{worker_name: worker_name, topic: topic, partition: partition, current_offset: offset} = state) do
    [%FetchResponse{topic: ^topic, partitions: [response = %{error_code: :no_error, partition: ^partition}]}] =
      KafkaEx.fetch(topic, partition, offset: offset, auto_commit: false, worker_name: worker_name)

    case response do
      %{last_offset: nil, message_set: []} ->
        auto_commit(state)

      %{last_offset: _, message_set: messages} ->
        Enum.reduce(messages, state, &handle_message/2)
    end
  end

  defp handle_message(%Message{offset: offset} = message, %State{consumer_module: consumer_module, consumer_state: consumer_state} = state) do
    case consumer_module.handle_message(message, consumer_state) do
      {:ack, new_state} ->
        %State{state | consumer_state: new_state, acked_offset: offset + 1, current_offset: offset + 1}
        |> auto_commit()

      {:commit, new_state} ->
        %State{state | consumer_state: new_state, acked_offset: offset + 1, current_offset: offset + 1}
        |> commit()
    end
  end

  defp auto_commit(%State{acked_offset: acked, committed_offset: committed, commit_threshold: threshold,
                          last_commit: last_commit, commit_interval: interval} = state) do
    case acked - committed do
      0 ->
        %State{state | last_commit: :erlang.monotonic_time(:milli_seconds)}

      n when n >= threshold ->
        commit(state)

      _ ->
        if :erlang.monotonic_time(:milli_seconds) - last_commit >= interval do
          commit(state)
        else
          state
        end
    end
  end

  defp commit(%State{acked_offset: offset, committed_offset: offset} = state), do: state
  defp commit(%State{worker_name: worker_name, group: group, topic: topic, partition: partition, acked_offset: offset} = state) do
    request = %OffsetCommitRequest{
      consumer_group: group,
      topic: topic,
      partition: partition,
      offset: offset,
    }

    [%OffsetCommitResponse{topic: ^topic, partitions: [^partition]}] =
      KafkaEx.offset_commit(worker_name, request)

    Logger.debug("Committed offset #{topic}/#{partition}@#{offset} for #{group}")

    %State{state | committed_offset: offset, last_commit: :erlang.monotonic_time(:milli_seconds)}
  end

  defp load_offsets(%State{worker_name: worker_name, group: group, topic: topic, partition: partition} = state) do
    request = %OffsetFetchRequest{consumer_group: group, topic: topic, partition: partition}

    [%OffsetFetchResponse{topic: ^topic, partitions: [%{partition: ^partition, error_code: error_code, offset: offset}]}] =
      KafkaEx.offset_fetch(worker_name, request)

    case error_code do
      :no_error ->
        %State{state | current_offset: offset, committed_offset: offset, acked_offset: offset}

      :unknown_topic_or_partition ->
        [%OffsetResponse{topic: ^topic, partition_offsets: [%{partition: ^partition, error_code: :no_error, offset: [offset]}]}] =
          KafkaEx.earliest_offset(topic, partition, worker_name)

        %State{state | current_offset: offset, committed_offset: offset, acked_offset: offset}
    end
  end
end
