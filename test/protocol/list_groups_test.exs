defmodule KafkaEx.Protocol.ListGroups.Test do
  use ExUnit.Case, async: true
  alias KafkaEx.Protocol.ListGroups
  alias KafkaEx.Protocol.ListGroups.{Response, Group}

  test "create_request creates a valid list groups request" do
    good_request = << 16 :: 16, 0 :: 16, 42 :: 32, 9 :: 16, "client_id" :: binary >>
    request = ListGroups.create_request(42, "client_id")
    assert request == good_request
  end

  test "parses empty response correctly" do
    response = <<
      42 :: 32, # correllation ID
       0 :: 16, # error code
       0 :: 32, # length = 0
    >>
    expected_response = %Response{error_code: :no_error, groups: []}
    assert ListGroups.parse_response(response) == expected_response
  end

  test "parses response with multiple groups" do
    response = <<
      42 :: 32, # correllation ID
       0 :: 16, # error code
       2 :: 32, # length = 2
       byte_size("a_group") :: 16, "a_group" :: binary,
       byte_size("a_protocol") :: 16, "a_protocol" :: binary,
       byte_size("another_group") :: 16, "another_group" :: binary,
       byte_size("another_protocol") :: 16, "another_protocol" :: binary,
    >>
    expected_response = %Response{
      error_code: :no_error,
      groups: [
        %Group{group_id: "a_group", protocol: "a_protocol"},
        %Group{group_id: "another_group", protocol: "another_protocol"},
      ]
    }
    assert ListGroups.parse_response(response) == expected_response
  end
end
