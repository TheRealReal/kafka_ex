defmodule KafkaEx.AsyncProducer.Publisher do
  @moduledoc """
  A GenServer that handles background publication of messages to a Kafka topic. A Publisher works on
  behalf of a Queue to handle all network requests to a Kafka cluster. A Publisher should not be
  called directly by a client. Use the API in the AsyncProducer module instead.
  """

  use GenServer

  alias KafkaEx.Protocol.Produce.Message
  alias KafkaEx.Protocol.Produce.Response, as: ProduceResponse

  require Logger

  # client API

  @type topic :: binary
  @type partition :: non_neg_integer

  @type required_acks :: non_neg_integer | -1
  @type compression :: KafkaEx.Compression.compression_type_t | :none

  @type option :: {:max_request_bytesize, pos_integer}
                | {:required_acks, required_acks}
                | {:timeout, non_neg_integer}
                | {:compression, compression}
                | {:partitioner, module}
  @type options :: [option]

  @doc """
  Start a Publisher process.
  """
  @spec start_link(Supervisor.supervisor, options, GenServer.options) :: GenServer.on_start
  def start_link(supervisor, opts \\ [], server_opts \\ []) do
    GenServer.start_link(__MODULE__, {supervisor, opts}, server_opts)
  end

  @doc """
  Publish a batch of messages to Kafka.
  """
  @spec send(GenServer.server, %{topic => :queue.queue(Message.t)}, timeout) :: :ok
  def send(server, queues, timeout \\ 5000) when is_map(queues) do
    GenServer.call(server, {:send, queues}, timeout)
  end

  @doc """
  Wait for all enqueued messages to be flushed to Kafka.
  """
  @spec wait(GenServer.server, timeout) :: :ok
  def wait(server, timeout \\ 5000) do
    GenServer.call(server, :wait, timeout)
  end

  # process State

  defmodule State do
    alias KafkaEx.AsyncProducer.{Publisher, Supervisor}
    alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
    alias KafkaEx.Protocol.Produce.Message
    alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest

    require Logger

    @allowed_options [:max_request_bytesize, :required_acks, :timeout, :compression, :partitioner]

    @type t :: %__MODULE__{
      # OPTIONS

      max_request_bytesize: pos_integer,
      required_acks: Publisher.required_acks,
      timeout: non_neg_integer,
      compression: Publisher.compression,
      partitioner: module,

      # WORKING STATE

      # the parent supervisor
      supervisor: Elixir.Supervisor.supervisor,

      # the supervisor of the KafkaEx worker
      kafka_supervisor: Elixir.Supervisor.supervisor | nil,
      kafka_monitor: reference | nil,

      # the Task.Supervisor for async network requests
      task_supervisor: Elixir.Supervisor.supervisor | nil,
      task_monitor: reference | nil,

      # metadata needed to calculate partition assignments
      metadata: MetadataResponse.t | nil,
      metadata_timestamp: integer | nil,

      in_flight_request: {reference, {Publisher.topic, Publisher.partition}, :queue.queue(Message.t)} | nil,
      in_flight_timestamp: integer | nil,

      # per-partition message queues and a round-robin queue to control publication order
      buffers: %{{Publisher.topic, Publisher.partition} => :queue.queue(Message.t)},
      buffer_queue: :queue.queue({Publisher.topic, Publisher.partition}),

      # processes waiting for buffers to be flushed
      waiters: [GenServer.from],
    }

    defstruct [
      # opts
      max_request_bytesize: 1_000_000,
      required_acks: -1,
      timeout: 1000,
      compression: :none,
      partitioner: KafkaEx.DefaultPartitioner,

      # working state
      supervisor: nil,
      kafka_supervisor: nil,
      kafka_monitor: nil,
      task_supervisor: nil,
      task_monitor: nil,
      metadata: nil,
      metadata_timestamp: nil,
      in_flight_request: nil,
      in_flight_timestamp: nil,
      buffers: %{},
      buffer_queue: :queue.new(),
      waiters: [],
    ]

    @metadata_timeout 30_000

    # Calculating message size should be handled by `KafkaEx.Protocol.Produce`.
    @request_overhead_bytesize 34 + byte_size("kafka_ex")
    @message_overhead_bytesize 26

    @doc """
    Return a new State struct populated with the parent supervisor and options.
    """
    @spec new(Elixir.Supervisor.supervisor, Publisher.options) :: State.t
    def new(supervisor, opts \\ []) do
      attrs = opts
              |> Keyword.take(@allowed_options)
              |> Enum.into(%{supervisor: supervisor})

      struct(State, attrs)
    end

    @doc """
    Initialize the server's working state so that it's ready to receive requests.
    """
    @spec initialize(State.t) :: State.t
    def initialize(%State{} = state) do
      state
      |> update_kafka_pid()
      |> update_task_pid()
      |> update_metadata()
    end

    @doc """
    Enqueue a batch of messages to be published.
    """
    @spec send(State.t, %{Publisher.topic => :queue.queue(Message.t)}) :: State.t
    def send(%State{} = state, queues) do
      Enum.reduce(queues, state, fn ({topic, messages}, acc_state1) ->
        messages
        |> :queue.to_list()
        |> Enum.reduce(acc_state1, fn (message, acc_state2) ->
          enqueue(acc_state2, topic, message)
        end)
      end)
    end

    @doc """
    Register a caller to be notified when the buffers are fully flushed to the Kafka broker.
    """
    @spec wait(State.t, GenServer.from) :: State.t
    def wait(%State{waiters: waiters} = state, from) do
      %State{state | waiters: [from | waiters]}
    end

    @doc """
    Process the server's state machine:

      * The first priority is to send messages to the Kafka broker, so if there are any enqueued
        messages but no in-flight request, then messages are taken from the buffer to start an
        in-flight request.
      * If the queues are empty and there is no in-flight request, the next priority is to alert any
        callers waiting for a reply from `Publisher.wait/2`.
      * Lastly, process any background tasks.
    """
    @spec process(State.t) :: State.t

    # First priority: start in-flight request for enqueued messages.
    def process(%State{in_flight_request: nil, buffers: buffers} = state) when map_size(buffers) > 0 do
      start_request(state)
    end

    # Second priority: alert waiting callers.
    def process(%State{in_flight_request: nil, buffers: buffers, waiters: waiters} = state) when map_size(buffers) == 0 and waiters != [] do
      Enum.each(waiters, fn (client) ->
        GenServer.reply(client, :ok)
      end)

      %State{state | waiters: []}
    end

    # Third priority: background processing.
    def process(%State{} = state) do
      t_now = now()

      cond do
        metadata_timeout(state, t_now) <= 0 ->
          update_metadata(state)

        in_flight_timeout(state, t_now) <= 0 ->
          {_ref, {topic, partition} , _queue} = state.in_flight_request
          Logger.warn(["Produce request for ", topic, "/", to_string(partition), " timed out"])

          retry_request(state)

        true ->
          Logger.warn("Kafka publisher has nothing to do. This likely indicates a bug.")
          state
      end
    end

    @doc """
    Returns the number of milliseconds until the next background task. `sleep_millis/1` is used to
    calculate the timeout value in GenServer return values, which controls when `process/1` is called.
    """
    @spec sleep_millis(State.t) :: non_neg_integer

    # If there are enqueued messages but no in-flight request, then start a new request immediately.
    def sleep_millis(%State{in_flight_request: nil, buffers: buffers}) when map_size(buffers) > 0 do
      0
    end

    # If there are no more messages to send, but callers waiting for a response, respond to them
    # immediately.
    def sleep_millis(%State{in_flight_request: nil, buffers: buffers, waiters: waiters}) when map_size(buffers) == 0 and waiters != [] do
      0
    end

    # Return the number of milliseconds to the next background task.
    def sleep_millis(%State{} = state) do
      t_now = now()

      [metadata_timeout(state, t_now), in_flight_timeout(state, t_now)]
      |> Enum.reject(&is_nil/1)
      |> Enum.min()
      |> max(0)
    end

    @doc """
    Register a successful response from a Kafka broker. Since the request succeeded, we simply clear
    the in-flight request, which will prompt the next call to `process/1` to start a new in-flight
    request.

    It's possible for the response to be for an old request that has already timed out. In that
    case, the response is ignored.
    """
    @spec handle_success(State.t, reference) :: State.t
    def handle_success(%State{in_flight_request: {ref, _key, _queue}} = state, ref) do
      %State{state | in_flight_request: nil, in_flight_timestamp: nil}
    end
    def handle_success(%State{} = state, _ref), do: state

    @doc """
    Register an error response from a Kafka broker. If the response is for the current in-flight
    request, the request is canceled and re-enqueued so that it can be tried again later.

    It's possible for the response to be for an old request that has already timed out. In that
    case, the response is ignored.
    """
    @spec handle_error(State.t, reference, any) :: State.t
    def handle_error(%State{in_flight_request: {ref, {topic, partition}, _queue}} = state, ref, reason) do
      Logger.warn(["Produce request for ", topic, "/", to_string(partition), " failed: ", inspect(reason)])
      retry_request(state)
    end
    def handle_error(%State{} = state, _ref, reason) do
      Logger.warn(["Produce request failed: ", inspect(reason)])
      state
    end

    @doc """
    Update metadata needed to calculate partition assignments.
    """
    @spec update_metadata(State.t) :: State.t
    def update_metadata(%State{kafka_supervisor: nil} = state) do
      state
      |> update_kafka_pid()
      |> update_metadata()
    end
    def update_metadata(%State{} = state) do
      metadata = KafkaEx.metadata(worker_name: worker_name(state))
      timestamp = now()

      %State{state |
        metadata: metadata,
        metadata_timestamp: timestamp,
      }
    end

    @doc """
    Handle `DOWN` messages from monitored processes. Clears the PID of the monitored process so that
    it can be updated the next time it's needed. The PID is not updated right away because its
    likely that the process is still restarting at the time of handling a `DOWN` message.
    """
    @spec down(State.t, reference) :: State.t
    def down(%State{kafka_monitor: monitor_ref} = state, monitor_ref) do
      %State{state |
        kafka_supervisor: nil,
        kafka_monitor: nil,
      }
    end
    def down(%State{task_monitor: monitor_ref} = state, monitor_ref) do
      %State{state |
        task_supervisor: nil,
        task_monitor: nil,
      }
    end
    def down(%State{in_flight_request: {monitor_ref, _key, _messages}} = state, monitor_ref) do
      retry_request(state)
    end
    def down(%State{} = state, _monitor_ref), do: state

    # helpers

    # Assign a single message to a partition and enqueue it for delivery.
    @spec enqueue(State.t, Publisher.topic, Message.t) :: State.t
    defp enqueue(%State{metadata: metadata, partitioner: partitioner, buffers: buffers, buffer_queue: buffer_queue} = state, topic, message) do
      partition =
        try do
          %ProduceRequest{topic: topic, messages: [message]}
          |> partitioner.assign_partition(metadata)
          |> Map.fetch!(:partition)
        rescue
          exp ->
            Logger.error(["Error assigning partition for topic ", inspect(topic), ". Does the topic exist? (", inspect(exp), ")"])
            0
        end

      key = {topic, partition}

      {new_queue, new_buffer_queue} =
        case Map.fetch(buffers, key) do
          {:ok, queue} ->
            {:queue.in(message, queue), buffer_queue}

          :error ->
            {:queue.from_list([message]), :queue.in(key, buffer_queue)}
        end

      new_buffers = Map.put(buffers, key, new_queue)

      %State{state |
        buffers: new_buffers,
        buffer_queue: new_buffer_queue,
      }
    end

    # Starts an async produce request.
    @spec start_request(State.t) :: State.t
    defp start_request(%State{kafka_supervisor: nil} = state)  do
      state
      |> update_kafka_pid()
      |> start_request()
    end
    defp start_request(%State{task_supervisor: nil} = state)  do
      state
      |> update_task_pid()
      |> start_request()
    end
    defp start_request(%State{task_supervisor: task_supervisor} = state) do
      {state, key, message_queue} = pop_message_set(state)

      # Produce messages async to prevent blocking GenServer.call while waiting on network I/O.
      %Task{ref: ref} = Task.Supervisor.async_nolink(
        task_supervisor,
        __MODULE__,
        :do_request,
        [key, message_queue, worker_name(state), state.required_acks, state.compression, state.timeout]
      )

      %State{state |
        in_flight_request: {ref, key, message_queue},
        in_flight_timestamp: now(),
      }
    end

    # Make request to produce messages to Kafka cluster.
    @doc false
    def do_request(key, message_queue, worker_name, required_acks, compression, timeout) do
      {topic, partition} = key

      request = %ProduceRequest{
        topic: topic,
        partition: partition,
        messages: :queue.to_list(message_queue),
        required_acks: required_acks,
        compression: compression,
        timeout: timeout,
      }

      Logger.debug(fn ->
        ["Sending ", to_string(length(request.messages)), " messages to ", topic, "/", to_string(partition)]
      end)

      KafkaEx.produce(request, worker_name: worker_name)
    end

    # Pops a message queue for delivery. If more messages than can fit in `max_request_bytesize` are
    # present in the queue, a subset will be returned. Returns a new state (with messages removed),
    # a queue of messages, and the topic and partition that the messages should be delivered to.
    @spec pop_message_set(State.t) :: {State.t, {Publisher.topic, Publisher.partition}, :queue.queue(Message.t)}
    defp pop_message_set(%State{buffers: buffers, buffer_queue: buffer_queue} = state) do
      {{:value, {topic, _partition} = key}, tmp_buffer_queue} = :queue.out(buffer_queue)
      {buffer, tmp_buffers} = Map.pop(buffers, key)

      {message_queue, new_buffer} = build_message_set(state, topic, buffer)

      {new_buffer_queue, new_buffers} =
        if :queue.is_empty(new_buffer) do
          {tmp_buffer_queue, tmp_buffers}
        else
          {:queue.in(key, tmp_buffer_queue), Map.put(tmp_buffers, key, new_buffer)}
        end

      new_state = %State{state |
        buffers: new_buffers,
        buffer_queue: new_buffer_queue,
      }

      {new_state, key, message_queue}
    end

    # Pops messages from `queue` while respecting the `max_request_bytesize` setting. Returns a
    # tuple of queues. The first queue returned is the set of messages that can be delivered
    # immediately. The second queue is the remaining messages from the input queue.
    @spec build_message_set(State.t, Publisher.topic, :queue.queue(Message.t)) :: {:queue.queue(Message.t), :queue.queue(Message.t)}
    defp build_message_set(%State{} = state, topic, queue) when is_binary(topic) do
      build_message_set(state, queue, :queue.new(), byte_size(topic) + @request_overhead_bytesize)
    end
    @spec build_message_set(State.t, :queue.queue(Message.t), :queue.queue(Message.t), pos_integer) :: {:queue.queue(Message.t), :queue.queue(Message.t)}
    defp build_message_set(%State{max_request_bytesize: max_bytesize} = state, queue, acc, acc_bytesize) when is_integer(max_bytesize) do
      case :queue.out(queue) do
        {{:value, %Message{key: key, value: value} = message}, new_queue} ->
          new_bytesize = acc_bytesize + byte_size(to_string(key)) + byte_size(value) + @message_overhead_bytesize

          if new_bytesize <= max_bytesize do
            build_message_set(state, new_queue, :queue.in(message, acc), new_bytesize)
          else
            {acc, queue}
          end

        {:empty, _} ->
          {acc, queue}
      end
    end

    # Finds the pid of the supervisor for the Kafka worker.
    @spec update_kafka_pid(State.t) :: State.t
    defp update_kafka_pid(%State{supervisor: supervisor, kafka_supervisor: nil, kafka_monitor: nil} = state) when is_pid(supervisor) do
      case Supervisor.child_pid(supervisor, :kafka) do
        :restarting ->
          Logger.warn("[publisher] cannot monitor Kafka because it's restarting")
          :timer.sleep(1)
          state

        kafka_supervisor ->
          kafka_monitor = Process.monitor(kafka_supervisor)

          %State{state |
            kafka_supervisor: kafka_supervisor,
            kafka_monitor: kafka_monitor,
          }
      end
    end

    # Finds the pid of the Task.Supervisor for async network requests.
    @spec update_task_pid(State.t) :: State.t
    defp update_task_pid(%State{supervisor: supervisor, task_supervisor: nil, task_monitor: nil} = state) when is_pid(supervisor) do
      case Supervisor.child_pid(supervisor, Task.Supervisor) do
        :restarting ->
          Logger.warn("[publisher] cannot monitor Task.Supervisor because it's restarting")
          :timer.sleep(1)
          state

        task_supervisor ->
          task_monitor = Process.monitor(task_supervisor)

          %State{state |
            task_supervisor: task_supervisor,
            task_monitor: task_monitor,
          }
      end
    end

    # Puts the in-flight request back in the delivery queue.
    @spec retry_request(State.t) :: State.t
    defp retry_request(%State{in_flight_request: {_ref, key, in_flight_queue}, buffers: buffers, buffer_queue: buffer_queue} = state) do
      {new_queue, new_buffer_queue} =
        case Map.fetch(buffers, key) do
          {:ok, cur_queue} ->
            # Put in flight messages back at *beginning* of queue to preserve in-order guarantees.
            {:queue.join(in_flight_queue, cur_queue), buffer_queue}

          :error ->
            {in_flight_queue, :queue.in(key, buffer_queue)}
        end

      new_buffers = Map.put(buffers, key, new_queue)

      %State{state |
        buffers: new_buffers,
        buffer_queue: new_buffer_queue,
        in_flight_request: nil,
        in_flight_timestamp: nil,
      }
    end

    # Returns the number of milliseconds until the metadata should be refreshed.
    @spec metadata_timeout(State.t, integer) :: integer
    defp metadata_timeout(%State{metadata_timestamp: nil}, _t_now), do: 0
    defp metadata_timeout(%State{metadata_timestamp: timestamp}, t_now) do
      @metadata_timeout - (t_now - timestamp)
    end

    # Returns the number of milliseconds until the in-flight request should be considered timed out.
    @spec in_flight_timeout(State.t, integer) :: integer | nil
    defp in_flight_timeout(%State{in_flight_timestamp: nil}, _t_now), do: nil
    defp in_flight_timeout(%State{in_flight_timestamp: timestamp, timeout: timeout}, t_now) do
      2 * timeout - (t_now - timestamp)
    end

    # Returns the name of the KafkaEx worker, suitable for use as the `:worker_name` option to many
    # `KafkaEx` functions.
    @spec worker_name(State.t) :: GenServer.server
    defp worker_name(%State{kafka_supervisor: kafka_supervisor} = state) when is_pid(kafka_supervisor) do
      [{:worker, pid, _type, _start}] = Elixir.Supervisor.which_children(kafka_supervisor)

      if pid == :restarting do
        Logger.warn("[publisher] waiting for KafkaEx to restart")
        :timer.sleep(1)
        worker_name(state)
      else
        pid
      end
    end

    # Return a monotonic timestamp suitable for calculating GenServer timeout values.
    @spec now() :: integer
    defp now() do
      System.monotonic_time(:millisecond)
    end
  end

  # GenServer callbacks

  @impl true
  def init({supervisor, opts}) when is_list(opts) do
    Process.flag(:trap_exit, true)

    {:ok, State.new(supervisor, opts), {:continue, :initialize}}
  end

  @impl true
  def handle_continue(:initialize, %State{} = state) do
    state
    |> State.initialize()
    |> noreply()
  end

  @impl true
  def handle_continue(:process, %State{} = state) do
    state
    |> State.process()
    |> noreply()
  end

  @impl true
  def handle_call({:send, queues}, _from, %State{} = state) when is_map(queues) do
    state
    |> State.send(queues)
    |> reply(:ok)
  end

  @impl true
  def handle_call(:wait, _from, %State{in_flight_request: nil, buffers: buffers} = state) when map_size(buffers) == 0 do
    reply(state, :ok)
  end

  @impl true
  def handle_call(:wait, from, %State{} = state) do
    state
    |> State.wait(from)
    |> noreply()
  end

  @impl true
  def handle_info(:timeout, %State{} = state) do
    state
    |> State.process()
    |> noreply()
  end

  @impl true
  def handle_info({ref, response}, %State{} = state) when is_reference(ref) do
    # Handle a response from the Kafka broker.
    case response do
      nil -> State.handle_success(state, ref)
      :ok -> State.handle_success(state, ref)
      {:ok, _offset} -> State.handle_success(state, ref)

      # `:not_leader_for_partition` can happen when the cluster has elected a new leader for a
      # partition, often in response to the previous leader being unavailable. Updating metadata
      # helps find the new leader.
      {:error, [%ProduceResponse{partitions: [%{error_code: :not_leader_for_partition}]}]} ->
        state
        |> State.handle_error(ref, :not_leader_for_partition)
        |> State.update_metadata()

      {:error, reason} ->
        State.handle_error(state, ref, reason)

      # `:leader_not_available` can happen when a broker is down. In a cluster with replicated
      # partitions, another broker may be elected the leader for the topic. Updating the metadata
      # helps find the new leader.
      :leader_not_available ->
        state
        |> State.handle_error(ref, :leader_not_available)
        |> State.update_metadata()
    end
    |> noreply()
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, %State{} = state) do
    state
    |> State.down(monitor_ref)
    |> noreply()
  end

  @impl true
  def handle_info(message, %State{} = state) do
    Logger.warn(["[publisher] received unexpected message: ", inspect(message)])
    noreply(state)
  end

  @impl true
  def terminate(_reason, %State{} = state) do
    :ok = flush(state)
  end

  # helpers

  # Format GenServer return value for reply with correct timeout until next required action.
  defp reply(%State{} = state, response) do
    case State.sleep_millis(state) do
      0 -> {:reply, response, state, {:continue, :process}}
      timeout -> {:reply, response, state, timeout}
    end
  end

  # Format GenServer return value for no reply with correct timeout until next required action.
  defp noreply(%State{} = state) do
    case State.sleep_millis(state) do
      0 -> {:noreply, state, {:continue, :process}}
      timeout -> {:noreply, state, timeout}
    end
  end

  # Drive state machine until all messages are sent and waiters notified. This is used by the
  # `terminate/2` callback to ensure all messages are flushed to the Kafka cluster.
  #
  # In normal operation, the buffers should already be flushed, because the `Queue` process calls
  # `Publisher.wait/2` in its `terminate/2` callback, preventing the supervisor from terminating
  # `Publisher` until its buffers are flushed. This is preferred, because it allows the process's
  # state machine to be driven by the GenServer event loop. Flushing buffers from `terminate/2` is a
  # backup strategy in case of abnormal shutdown.
  defp flush(%State{in_flight_request: nil, buffers: buffers, waiters: []}) when map_size(buffers) == 0, do: :ok
  defp flush(%State{} = state) do
    timeout = State.sleep_millis(state)

    receive do
      message ->
        {:noreply, new_state, _timeout} = handle_info(message, state)
        new_state
    after
      timeout ->
        State.process(state)
    end
    |> flush()
  end
end
