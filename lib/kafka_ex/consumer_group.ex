defmodule KafkaEx.ConsumerGroup do
  use GenServer

  alias KafkaEx.Config
  alias KafkaEx.Protocol.Heartbeat.Response, as: HeartbeatResponse
  alias KafkaEx.Protocol.LeaveGroup.Response, as: LeaveGroupResponse
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.Metadata.TopicMetadata
  alias KafkaEx.Protocol.Metadata.PartitionMetadata
  alias KafkaEx.Protocol.SyncGroup.Response, as: SyncGroupResponse

  defmodule State do
    @moduledoc false
    defstruct [
      :worker_name,
      :heartbeat_interval,
      :session_timeout,
      :consumer_module,
      :consumer_opts,
      :group_name,
      :topics,
      :partitions,
      :member_id,
      :generation_id,
      :assignments,
      :consumer_pid,
    ]
  end

  @heartbeat_interval 5_000
  @session_timeout 30_000

  # Client API

  def start_link(consumer_module, group_name, topics, opts \\ []) do
    {server_opts, consumer_opts} = Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt])

    GenServer.start_link(__MODULE__, {consumer_module, group_name, topics, consumer_opts}, server_opts)
  end

  # GenServer callbacks

  def init({consumer_module, group_name, topics, opts}) do
    worker_name = Keyword.get(opts, :worker_name, Config.default_worker)
    heartbeat_interval = Keyword.get(opts, :heartbeat_interval, Application.get_env(:kafka_ex, :heartbeat_interval, @heartbeat_interval))
    session_timeout = Keyword.get(opts, :session_timeout, Application.get_env(:kafka_ex, :session_timeout, @session_timeout))

    consumer_opts = Keyword.drop(opts, [:heartbeat_interval, :session_timeout])

    state = %State{
      worker_name: worker_name,
      heartbeat_interval: heartbeat_interval,
      session_timeout: session_timeout,
      consumer_module: consumer_module,
      consumer_opts: consumer_opts,
      group_name: group_name,
      topics: topics,
      member_id: "",
    }

    Process.flag(:trap_exit, true)

    {:ok, state, 0}
  end

  def handle_info(:timeout, %State{generation_id: nil, member_id: ""} = state) do
    new_state = join(state)

    {:noreply, new_state, new_state.heartbeat_interval}
  end

  def handle_info(:timeout, %State{} = state) do
    new_state = heartbeat(state)

    {:noreply, new_state, new_state.heartbeat_interval}
  end

  def handle_info({:EXIT, _pid, :normal}, %State{} = state) do
    {:noreply, state, state.heartbeat_interval}
  end

  def terminate(_reason, %State{} = state) do
    leave(state)
  end

  # Helpers

  defp join(%State{worker_name: worker_name, session_timeout: session_timeout, group_name: group_name, topics: topics, member_id: member_id} = state) do
    join = KafkaEx.join_group(group_name, topics, member_id: member_id, session_timeout: session_timeout, worker_name: worker_name)
    state = %State{state | member_id: join.member_id, generation_id: join.generation_id}

    if join.member_id == join.leader_id do
      sync_leader(state, join.members)
    else
      sync_follower(state)
    end
  end

  defp sync_leader(%State{worker_name: worker_name, topics: topics, partitions: nil} = state, members) do
    %MetadataResponse{topic_metadatas: topic_metadatas} = KafkaEx.metadata(worker_name: worker_name)

    partitions = Enum.flat_map(topics, fn (topic) ->
      %TopicMetadata{error_code: :no_error, partition_metadatas: partition_metadatas} = Enum.find(topic_metadatas, &(&1.topic == topic))

      Enum.map(partition_metadatas, fn (%PartitionMetadata{error_code: :no_error, partition_id: partition_id}) ->
        {topic, partition_id}
      end)
    end)

    sync_leader(%State{state | partitions: partitions}, members)
  end

  defp sync_leader(%State{worker_name: worker_name, session_timeout: session_timeout,
                          group_name: group_name, generation_id: generation_id, member_id: member_id} = state, members) do
    assignments = assign_partitions(state, members)

    KafkaEx.sync_group(group_name, generation_id, member_id, assignments, timeout: session_timeout + 5000, worker_name: worker_name)
    |> update_assignments(state)
  end

  defp sync_follower(%State{worker_name: worker_name, session_timeout: session_timeout,
                            group_name: group_name, generation_id: generation_id, member_id: member_id} = state) do
    KafkaEx.sync_group(group_name, generation_id, member_id, [], timeout: session_timeout + 5000, worker_name: worker_name)
    |> update_assignments(state)
  end

  defp update_assignments(%SyncGroupResponse{error_code: :rebalance_in_progress}, %State{} = state), do: rebalance(state)
  defp update_assignments(%SyncGroupResponse{error_code: :not_coordinator_for_consumer, assignments: assignments}, %State{} = state) do
    IO.puts "NOT COORDINATOR updated_assignments"
    start_consumer(state, assignments)
  end
  defp update_assignments(%SyncGroupResponse{error_code: :no_error, assignments: assignments}, %State{} = state) do
    start_consumer(state, assignments)
  end

  defp heartbeat(%State{worker_name: worker_name, group_name: group_name, generation_id: generation_id, member_id: member_id} = state) do
    case KafkaEx.heartbeat(group_name, generation_id, member_id, worker_name: worker_name) do
      %HeartbeatResponse{error_code: :no_error} ->
        state

      %HeartbeatResponse{error_code: :rebalance_in_progress} ->
        rebalance(state)
    end
  end

  defp rebalance(%State{} = state) do
    state
    |> stop_consumer()
    |> join()
  end

  defp leave(%State{worker_name: worker_name, group_name: group_name, member_id: member_id} = state) do
    stop_consumer(state)
    case KafkaEx.leave_group(group_name, member_id, worker_name: worker_name) do
      %LeaveGroupResponse{error_code: :no_error} ->
        nil
      %LeaveGroupResponse{error_code: :not_coordinator_for_consumer} ->
        IO.puts "NOT COORDINATOR leave"
        nil
    end
  end

  defp start_consumer(%State{consumer_module: consumer_module, consumer_opts: consumer_opts,
                             group_name: group_name, consumer_pid: nil} = state, assignments) do
    assignments =
      Enum.flat_map(assignments, fn ({topic, partition_ids}) ->
        Enum.map(partition_ids, &({topic, &1}))
      end)

    {:ok, pid} = KafkaEx.GenConsumer.Supervisor.start_link(consumer_module, group_name, assignments, consumer_opts)

    %State{state | assignments: assignments, consumer_pid: pid}
  end

  defp stop_consumer(%State{consumer_pid: nil} = state), do: state
  defp stop_consumer(%State{consumer_pid: pid} = state) when is_pid(pid) do
    :ok = Supervisor.stop(pid)
    %State{state | consumer_pid: nil}
  end

  defp assign_partitions(%State{consumer_module: consumer_module, partitions: partitions}, members) do
    assignments =
      consumer_module.assign_partitions(members, partitions)
      |> Enum.map(fn ({member, topic_partitions}) ->
        assigns =
          topic_partitions
          |> Enum.group_by(&(elem(&1, 0)), &(elem(&1, 1)))
          |> Enum.into([])

        {member, assigns}
      end)
      |> Map.new

    Enum.map(members, fn (member) ->
      {member, Map.get(assignments, member, [])}
    end)
  end
end
