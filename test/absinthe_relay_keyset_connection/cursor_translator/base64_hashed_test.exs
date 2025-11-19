defmodule AbsintheRelayKeysetConnection.CursorTranslator.Base64HashedTest do
  use ExUnit.Case, async: true

  alias AbsintheRelayKeysetConnection.CursorTranslator.Base64Hashed
  doctest Base64Hashed, import: true

  describe "encoding and decoding cursors, ensuring valid columns" do
    test "with an id column" do
      raw = %{id: 1}
      encoded = Base64Hashed.from_key(raw, [:id])
      {:ok, decoded} = Base64Hashed.to_key(encoded, [:id])
      assert decoded == raw
    end

    test "with multiple columns combined for uniqueness" do
      expected_columns = [:last_name, :id]
      raw = %{last_name: "Smithers", id: 50}
      encoded = Base64Hashed.from_key(raw, expected_columns)

      # If these are the exact columns we expect, it's valid
      {:ok, decoded} = Base64Hashed.to_key(encoded, expected_columns)
      assert decoded == raw

      # If there are columns we don't expect, it's invalid
      expected_columns = [:id]
      {:error, :invalid_cursor} = Base64Hashed.to_key(encoded, expected_columns)

      # If these are not all the columns we expect, it's invalid
      expected_columns = [:last_name, :first_name, :id]
      {:error, :invalid_cursor} = Base64Hashed.to_key(encoded, expected_columns)
    end

    test "with Date type in null_coalesce config" do
      # When using null_coalesce with Date structs, the decoded cursor should
      # return Date structs, not ISO8601 strings
      columns = [:due_date, :id]

      # Simulate a NULL date being coalesced to a default date
      raw = %{due_date: nil, id: 1}
      config = %{null_coalesce: %{due_date: ~D[0001-01-01]}}

      # When encoding, the nil date gets replaced with the default Date struct
      encoded = Base64Hashed.from_key(raw, columns, config)

      # When decoding, the Date should be properly converted back from JSON string
      {:ok, decoded} = Base64Hashed.to_key(encoded, columns, config)

      # The decoded due_date should be a Date struct, not a string
      assert decoded.due_date == ~D[0001-01-01]
      assert %Date{} = decoded.due_date
      assert decoded.id == 1
    end

    test "with NaiveDateTime type in null_coalesce config" do
      columns = [:updated_at, :id]
      raw = %{updated_at: nil, id: 1}
      config = %{null_coalesce: %{updated_at: ~N[0001-01-01 00:00:00]}}

      encoded = Base64Hashed.from_key(raw, columns, config)
      {:ok, decoded} = Base64Hashed.to_key(encoded, columns, config)

      assert decoded.updated_at == ~N[0001-01-01 00:00:00]
      assert %NaiveDateTime{} = decoded.updated_at
      assert decoded.id == 1
    end
  end
end
