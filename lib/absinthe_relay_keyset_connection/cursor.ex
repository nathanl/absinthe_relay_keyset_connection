defmodule AbsintheRelayKeysetConnection.Cursor do
  @moduledoc false

  @prefix "🔑"
  @pad_length 2
  @pad_bits @pad_length * 8

  @doc """
  Creates the cursor string from a key.
  This encoding is not meant to be tamper-proof, just to hide the cursor data
  as an implementation detail.

  ## Examples

  iex> from_key(%{id: 25}, [:id])
  "Tr7wn5SRWzI1XQ=="

  iex> from_key(%{name: "Mo", id: 26}, [:name, :id])
  "eo7wn5SRWyJNbyIsMjZd"
  """
  def from_key(key_map, cursor_columns) do
    key =
      Enum.map(cursor_columns, fn column ->
        Map.fetch!(key_map, column)
      end)

    {:ok, json} = Jason.encode(key)
    # Makes it easy to visually distinguish between cursors
    padding = padding_from(json)

    Base.encode64(padding <> @prefix <> json)
  end

  @doc """
  Rederives the key from the cursor string.
  The cursor string is supplied by users and may have been tampered with.
  However, we ensure that only the expected column values may appear in the
  cursor, so at worst, they could paginate from a different spot, which is
  fine.

  ## Examples

  iex> to_key("sCLwn5SRWzI1XQ==", [:id])
  {:ok, %{id: 25}}
  """
  def to_key(encoded_cursor, expected_columns) do
    with {:ok, <<_::size(@pad_bits)>> <> @prefix <> json_cursor} <-
           Base.decode64(encoded_cursor),
         {:ok, decoded_list} <- Jason.decode(json_cursor),
         true <- Enum.count(expected_columns) == Enum.count(decoded_list) do
      key =
        expected_columns
        |> Enum.zip(decoded_list)
        |> Enum.into(Map.new())

      {:ok, key}
    else
      _ -> {:error, :invalid_cursor}
    end
  rescue
    ArgumentError ->
      {:error, :invalid_cursor}
  end

  # Builds a varied but deterministic padding string from the input.
  def padding_from(string) do
    :crypto.hash(:sha, string)
    |> Kernel.binary_part(0, @pad_length)
  end
end
