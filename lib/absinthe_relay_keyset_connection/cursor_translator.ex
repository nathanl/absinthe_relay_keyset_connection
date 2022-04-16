defmodule AbsintheRelayKeysetConnection.CursorTranslator do
  @moduledoc """
  A cursor translator handles encoding and decoding pagination cursors.

  This module provides the behaviour for implementing a cursor translator.
  An example use case of this module would be a cursor translator that
  encodes and decodes signed or encrypted pagination cursors.

  ## Example Cursor Translator

  A basic example that encodes and decodes cursors using Jason and base64:

  ```elixir
  defmodule MyApp.Absinthe.Cursor do
    @behaviour AbsintheRelayKeysetConnection.CursorTranslator

    @impl AbsintheRelayKeysetConnection.CursorTranslator
    def from_key(key_map, cursor_columns) do
      cursor_columns
      |> Enum.map(&Map.fetch!(key_map, &1))
      |> Jason.encode!()
      |> Base.encode64(padding: false)
    end

    @impl AbsintheRelayKeysetConnection.CursorTranslator
    def to_key(encoded_cursor, expected_columns) do
      values = encoded_cursor |> Base.decode64!(padding: false) |> Jason.decode!()
      key = expected_columns |> Enum.zip(values) |> Map.new()

      {:ok, key}
    end
  end
  ```
  """

  @typedoc "A key of the column of the node being paginated."
  @type column_key() :: atom() | String.t()

  @typedoc "A map containing the node data."
  @type key_map() :: %{required(column_key()) => term()}

  @typedoc "A string containing the encoded cursor."
  @type encoded_cursor() :: String.t()

  @doc """
  Creates the cursor string from a key.

  Converts a key map into a cursor string using the given cursor columns.
  """
  @callback from_key(key_map(), [column_key()]) :: encoded_cursor()

  @doc """
  Rederives the key from the cursor string.

  Converts a cursor string into a key map using the given cursor columns.
  """
  @callback to_key(encoded_cursor(), expected_columns :: [column_key()]) ::
              {:ok, key_map()} | {:error, term()}
end
