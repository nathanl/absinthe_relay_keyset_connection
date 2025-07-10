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
    def from_key(key_map, cursor_columns, config \\ %{}) do
      null_coalesce = Map.get(config, :null_coalesce, %{})
      
      values = 
        cursor_columns
        |> Enum.map(fn column ->
          value = Map.fetch!(key_map, column)
          case value do
            nil -> Map.get(null_coalesce, column, nil)
            _ -> value
          end
        end)
      
      values
      |> Jason.encode!()
      |> Base.encode64(padding: false)
    end

    @impl AbsintheRelayKeysetConnection.CursorTranslator
    def to_key(encoded_cursor, expected_columns, _config \\ %{}) do
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
  Creates the cursor string from a key with additional configuration.

  Converts a key map into a cursor string using the given cursor columns,
  applying any configured transformations like null coalescing.

  ## Configuration Options

  - `:null_coalesce` - Map of column names to coalesce values for NULL handling
  """
  @callback from_key(key_map(), [column_key()], config :: map()) :: encoded_cursor()

  @doc """
  Rederives the key from the cursor string.

  Converts a cursor string into a key map using the given cursor columns.
  """
  @callback to_key(encoded_cursor(), expected_columns :: [column_key()]) ::
              {:ok, key_map()} | {:error, term()}

  @doc """
  Rederives the key from the cursor string with additional configuration.

  Converts a cursor string into a key map using the given cursor columns,
  applying any configured transformations like null coalescing.

  ## Configuration Options

  - `:null_coalesce` - Map of column names to coalesce values for NULL handling
  """
  @callback to_key(encoded_cursor(), expected_columns :: [column_key()], config :: map()) ::
              {:ok, key_map()} | {:error, term()}
end
