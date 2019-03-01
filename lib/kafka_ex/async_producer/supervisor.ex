defmodule KafkaEx.AsyncProducer.Supervisor do
  @moduledoc """
  Provides a supervision tree for an AsyncProducer.
  """

  use Supervisor

  alias KafkaEx.AsyncProducer.{Queue, Publisher}

  @type option :: Queue.option
                | Publisher.option
  @type options :: [option]

  @type child :: Queue
               | Publisher
               | :kafka

  @doc """
  Start an AsyncProducer supervision tree.
  """
  @spec start_link(options, GenServer.options) :: Supervisor.on_start
  def start_link(opts \\ [], server_opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, Keyword.put_new(server_opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    import Supervisor.Spec

    {:ok, worker_opts} = KafkaEx.build_worker_options(consumer_group: :no_consumer_group)

    children = [
      # A separate supervisor for the KafkaEx worker isolates the Queue and Publisher processes from
      # crashing when the KafkaEx worker crashes (such as when the Kafka cluster is unreachable).
      # KafkaEx `:max_restarts` and `:max_seconds` should be configured to survive broker outages.
      supervisor(Supervisor, [
        [worker(
          KafkaEx.Config.server_impl(),
          [worker_opts, :no_name],
          id: :worker)],
        [strategy: :one_for_one,
         max_restarts: Application.get_env(:kafka_ex, :max_restarts),
         max_seconds: Application.get_env(:kafka_ex, :max_seconds)]
      ], id: :kafka),
      supervisor(Task.Supervisor, []),
      worker(Publisher, [self(), opts], shutdown: 30_000),
      worker(Queue, [self(), opts]),
    ]

    supervise(children, strategy: :one_for_one)
  end

  @spec child_pid(Elixir.Supervisor.supervisor, child) :: pid | :restarting
  def child_pid(supervisor, child) do
    supervisor
    |> Supervisor.which_children()
    |> pid_of(child)
  end

  defp pid_of([{child, pid, _type, _start} | _children], child), do: pid
  defp pid_of([_child | children], child), do: pid_of(children, child)
end
