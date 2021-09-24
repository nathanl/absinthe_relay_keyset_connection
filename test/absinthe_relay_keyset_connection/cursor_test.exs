defmodule AbsintheRelayKeysetConnection.CursorTest do
  use ExUnit.Case, async: true

  alias AbsintheRelayKeysetConnection.Cursor
  doctest Cursor, import: true

  describe "encoding and decoding cursors, ensuring valid columns" do
    test "with an id column" do
      raw = %{id: 1}
      encoded = Cursor.from_key(raw, [:id])
      {:ok, decoded} = Cursor.to_key(encoded, [:id])
      assert decoded == raw
    end

    test "with multiple columns combined for uniqueness" do
      expected_columns = [:last_name, :id]
      raw = %{last_name: "Smithers", id: 50}
      encoded = Cursor.from_key(raw, expected_columns)

      # If these are the exact columns we expect, it's valid
      {:ok, decoded} = Cursor.to_key(encoded, expected_columns)
      assert decoded == raw

      # If there are columns we don't expect, it's invalid
      expected_columns = [:id]
      {:error, :invalid_cursor} = Cursor.to_key(encoded, expected_columns)

      # If these are not all the columns we expect, it's invalid
      expected_columns = [:last_name, :first_name, :id]
      {:error, :invalid_cursor} = Cursor.to_key(encoded, expected_columns)
    end
  end
end
