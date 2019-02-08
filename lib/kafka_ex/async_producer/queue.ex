defmodule KafkaEx.AsyncProducer.Queue do
  @moduledoc """
  A GenServer process that queues Kafka messages in memory for batch delivery based on number of
  messages or time. Delivery can be controlled by the `delivery_threshold` and `delivery_interval`
  options. A Queue should not be called directly by a client. Use the API in the AsyncProducer
  module instead.
  """

  use GenServer

  alias KafkaEx.Protocol.Produce.Message

  require Logger

  @type topic :: binary

  @type delivery_threshold :: pos_integer
  @type delivery_interval :: pos_integer

  @type option :: {:delivery_threshold, delivery_threshold}
                | {:delivery_interval, delivery_interval}
  @type options :: [option]

  @doc """
  Start a Queue process.
  """
  @spec start_link(Supervisor.supervisor, options, GenServer.options) :: GenServer.on_start
  def start_link(supervisor, opts \\ [], server_opts \\ []) do
    GenServer.start_link(__MODULE__, {supervisor, opts}, server_opts)
  end

  @doc """
  Produce a message to a Kafka topic.
  """
  @spec produce(GenServer.server, topic, Message.t, timeout) :: :ok
  def produce(server, topic, %Message{value: value} = message, timeout \\ 5000) when is_binary(topic) and is_binary(value) do
    GenServer.call(server, {:produce, topic, message}, timeout)
  end

  # process state

  defmodule State do
    alias KafkaEx.AsyncProducer.{Supervisor, Publisher}
    alias KafkaEx.Protocol.Produce.Message

    require Logger

    @allowed_options [:delivery_threshold, :delivery_interval]

    @type t :: %__MODULE__{
      # OPTIONS

      delivery_threshold: pos_integer,
      delivery_interval: pos_integer,

      # WORKING STATE

      # parent supervisor
      supervisor: Elixir.Supervisor.supervisor,

      # publisher process
      publisher: GenServer.server | nil,
      publisher_monitor: reference | nil,

      # buffer of undelivered messages
      buffer_size: non_neg_integer,
      buffer_timestamp: integer | nil,
      buffers: %{Queue.topic => :queue.queue(Message.t)},
    }

    defstruct [
      # opts
      delivery_threshold: 100,
      delivery_interval: 1000,

      # working state
      supervisor: nil,
      publisher: nil,
      publisher_monitor: nil,
      buffer_size: 0,
      buffer_timestamp: nil,
      buffers: %{},
    ]

    @doc """
    Returns a new State struct populated with the parent supervisor and options.
    """
    @spec new(Elixir.Supervisor.supervisor, Queue.options) :: State.t
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
      update_publisher_pid(state)
    end

    @doc """
    Enqueue a message to a topic.
    """
    @spec enqueue(State.t, Queue.topic, Message.t) :: State.t

    # Mark the timestamp if this is the first message enqueued to an empty buffer.
    def enqueue(%State{buffer_timestamp: nil} = state, topic, %Message{} = message) when is_binary(topic) do
      %State{state | buffer_timestamp: now()}
      |> enqueue(topic, message)
    end

    # Add message to queue and increment buffer size.
    def enqueue(%State{} = state, topic, %Message{} = message) when is_binary(topic) do
      state
      |> Map.update!(:buffers, fn (buffers) ->
        buffers
        |> Map.put_new(topic, :queue.new())
        |> Map.update!(topic, fn (queue) ->
          :queue.in(message, queue)
        end)
      end)
      |> Map.update!(:buffer_size, fn (size) ->
        size + 1
      end)
    end

    @doc """
    Send enqueued messages to the publisher.
    """
    @spec send(State.t) :: State.t
    def send(%State{publisher: nil} = state) do
      state
      |> update_publisher_pid()
      |> send()
    end
    def send(%State{publisher: publisher, buffers: buffers} = state) when is_pid(publisher) and map_size(buffers) > 0 do
      :ok = Publisher.send(publisher, buffers)

      %State{state |
        buffer_size: 0,
        buffer_timestamp: nil,
        buffers: %{},
      }
    end

    @doc """
    Return the number of milliseconds until the buffers should be sent to the publisher. Returns
    `nil` if the buffer is empty (nothing needs to be sent).
    """
    @spec send_timeout(State.t) :: non_neg_integer | nil
    def send_timeout(%State{buffer_size: 0, buffer_timestamp: nil}), do: nil
    def send_timeout(%State{delivery_threshold: threshold, buffer_size: size}) when size >= threshold, do: 0
    def send_timeout(%State{delivery_interval: interval, buffer_timestamp: timestamp}) do
      max(0, interval - (now() - timestamp))
    end

    @doc """
    Wait for publisher to flush its buffers.
    """
    @spec wait(State.t) :: State.t
    def wait(%State{publisher: nil} = state) do
      state
      |> update_publisher_pid()
      |> wait()
    end
    def wait(%State{publisher: publisher, buffers: buffers} = state) when is_pid(publisher) and map_size(buffers) == 0 do
      :ok = Publisher.wait(publisher)

      state
    end

    @doc """
    Handle `DOWN` messages from monitored processes. Clears the PID of the monitored process so that
    it can be updated the next time it's needed. The PID is not updated right away because its
    likely that the process is still restarting at the time of handling a `DOWN` message.
    """
    @spec down(State.t, reference) :: State.t
    def down(%State{publisher_monitor: monitor_ref} = state, monitor_ref) do
      %State{state |
        publisher: nil,
        publisher_monitor: nil,
      }
    end

    # Updates the process state with the PID of the Publisher process.
    @spec update_publisher_pid(State.t) :: State.t
    defp update_publisher_pid(%State{supervisor: supervisor, publisher: nil, publisher_monitor: nil} = state) when is_pid(supervisor) do
      case Supervisor.child_pid(supervisor, Publisher) do
        :restarting ->
          Logger.warn("[queue] waiting for publisher to restart")
          :timer.sleep(1)
          update_publisher_pid(state)

        publisher ->
          publisher_monitor = Process.monitor(publisher)

          %State{state |
            publisher: publisher,
            publisher_monitor: publisher_monitor,
          }
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
  def handle_continue(:send, %State{} = state) do
    state
    |> State.send()
    |> noreply()
  end

  @impl true
  def handle_call({:produce, topic, %Message{} = message}, _from, %State{} = state) when is_binary(topic) do
    state
    |> State.enqueue(topic, message)
    |> reply(:ok)
  end

  @impl true
  def handle_info(:timeout, %State{} = state) do
    state
    |> State.send()
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
    Logger.warn(["[queue] received unexpected message: ", inspect(message)])
    noreply(state)
  end

  @impl true
  def terminate(_reason, %State{buffers: buffers} = state) when map_size(buffers) > 0 do
    state
    |> State.send()
    |> State.wait()
  end
  def terminate(_reason, %State{buffers: buffers} = state) when map_size(buffers) == 0 do
    State.wait(state)
  end

  # helpers

  # Format GenServer return value for reply with timeout value when appropriate.
  defp reply(%State{} = state, response) do
    case State.send_timeout(state) do
      nil -> {:reply, response, state}
      0 -> {:reply, response, state, {:continue, :send}}
      timeout -> {:reply, response, state, timeout}
    end
  end

  # Format GenServer return value for no reply with timeout value when appropriate.
  defp noreply(%State{} = state) do
    case State.send_timeout(state) do
      nil -> {:noreply, state}
      0 -> {:noreply, state, {:continue, :send}}
      timeout -> {:noreply, state, timeout}
    end
  end
end
