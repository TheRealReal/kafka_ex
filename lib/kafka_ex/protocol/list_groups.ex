defmodule KafkaEx.Protocol.ListGroups do
  @moduledoc """
  Implementation of the Kafka List Groups request and response APIs.
  """

  defmodule Response do
    @moduledoc false
    defstruct error_code: nil, groups: []
    @type t :: %Response{
      error_code: atom,
      groups: [KafkaEx.Protocol.ListGroups.Group.t],
    }
  end

  defmodule Group do
    @moduledoc false
    defstruct group_id: nil, protocol: nil
    @type t :: %Group{
      group_id: binary,
      protocol: binary,
    }
  end

  @spec create_request(integer, binary) :: <<_ :: 64>>
  def create_request(correlation_id, client_id) do
    KafkaEx.Protocol.create_request(:list_groups, correlation_id, client_id)
  end

  @spec parse_response(binary) :: Response.t
  def parse_response(<<_correlation_id :: 32-signed, error_code :: 16-signed, length :: 32-signed, payload :: binary>>) do
    %Response{
      error_code: KafkaEx.Protocol.error(error_code),
      groups: parse_groups(length, payload),
    }
  end

  defp parse_groups(0, <<>>), do: []
  defp parse_groups(length, <<group_len :: 16-signed, group_id :: size(group_len)-binary,
                              proto_len :: 16-signed, protocol :: size(proto_len)-binary, rest :: binary>>) do
    [%Group{group_id: group_id, protocol: protocol} | parse_groups(length - 1, rest)]
  end
end
