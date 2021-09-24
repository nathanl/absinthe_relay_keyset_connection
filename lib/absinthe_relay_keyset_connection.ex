defmodule AbsintheRelayKeysetConnection do
  @moduledoc """
  Support for paginated result sets using keyset pagination.

  Specifically, `from_query/3` is keyset-paginated replacement for
  `from_query/4` in `Absinthe.Relay.Connection`.

  For information about the connection model, see the [Relay Cursor Connections
  Specification](https://relay.dev/graphql/connections.htm)
  """

  require Ecto.Query

  @cursor_prefix "arrayconnection:"

  @doc """
  Return a single page of results which contain the info specified in the
  [Relay Cursor Connections
  Specification](https://relay.dev/graphql/connections.htm)
  """
  def from_query(_query, _repo_fun, _options, _config \\ %{})

  def from_query(_query, _repo_fun, %{first: _, last: _}, _config) do
    {
      :error,
      ~s[The combination of :first and :last is unsupported. As the GraphQL Cursor Connections Specification (https://relay.dev/graphql/connections.htm) states: 'Including a value for both first and last is strongly discouraged, as it is likely to lead to confusing queries and results.']
    }
  end

  def from_query(_query, _repo_fun, %{before: _, after: _}, _config) do
    {
      :error,
      ~s[The combination of :before and :after is unsupported. Although logically possible, it doesn't make sense for pagination.]
    }
  end

  def from_query(_query, _repo_fun, %{first: _, before: _}, _config) do
    {
      :error,
      ~s[The combination of :first and :before is unsupported. Although logically possible, it doesn't make sense for pagination.]
    }
  end

  def from_query(_query, _repo_fun, %{last: _, after: _}, _config) do
    {
      :error,
      ~s[The combination of :last and :after is unsupported. Although logically possible, it doesn't make sense for pagination.]
    }
  end

  def from_query(_query, _repo_fun, %{first: n}, _config)
      when not is_integer(n) or (is_integer(n) and n < 1) do
    {:error, "The value of :first must be an integer >= 1"}
  end

  def from_query(_query, _repo_fun, %{last: n}, _config)
      when not is_integer(n) or (is_integer(n) and n < 1) do
    {:error, "The value of :last must be an integer >= 1"}
  end

  def from_query(query, repo_fun, opts, config) do
    if Map.has_key?(opts, :first) or Map.has_key?(opts, :last) do
      do_from_query(
        query,
        repo_fun,
        Map.take(opts, [:after, :before, :first, :last, :sorts]),
        config
      )
    else
      {:error,
       "Querying with neither :first nor :last is unsupported. Although logically possible, it doesn't make sense for pagination"}
    end
  end

  defp do_from_query(query, repo_fun, opts, config) do
    with :ok <- validate_sorts(opts),
         {:ok, opts} <- set_default_sorts(opts, config),
         {:ok, cursor_columns} <- get_cursor_columns(opts),
         {:ok, opts} <- decode_cursor(opts, cursor_columns),
         query <- apply_sorts(query, opts),
         {:ok, query} <- apply_where(query, opts),
         query <- limit_plus_one(query, opts) do
      nodes = repo_fun.(query)

      {more_pages?, nodes} = check_for_extra_and_trim(nodes, opts)

      nodes = maybe_reverse(nodes, opts)

      edges = build_edges(nodes, cursor_columns)

      page_info = get_page_info(opts, edges, more_pages?)

      {:ok, %{edges: edges, page_info: page_info}}
    end
  end

  defp decode_cursor(%{after: encoded_cursor} = opts, cursor_columns) do
    case cursor_to_key(encoded_cursor, cursor_columns) do
      {:ok, key} -> {:ok, Map.put(opts, :after, key)}
      {:error, msg} -> {:error, msg}
    end
  end

  defp decode_cursor(%{before: encoded_cursor} = opts, cursor_columns) do
    case cursor_to_key(encoded_cursor, cursor_columns) do
      {:ok, key} -> {:ok, Map.put(opts, :before, key)}
      {:error, msg} -> {:error, msg}
    end
  end

  defp decode_cursor(opts, _cursor_columns) do
    {:ok, opts}
  end

  defp validate_sorts(opts) do
    sorts = Map.get(opts, :sorts, [])

    all_valid? =
      Enum.all?(sorts, fn sort ->
        is_map(sort) and map_size(sort) == 1
      end)

    if all_valid? do
      :ok
    else
      {:error,
       {:invalid_sorts,
        "each sort must specify a single column and direction so that sorts can be applied in the specified order"}}
    end
  end

  defp set_default_sorts(opts, %{unique_column: unique_column})
       when is_atom(unique_column) do
    unique_sort = [{unique_column, :asc}] |> Enum.into(Map.new())

    case opts do
      %{sorts: sorts} ->
        {:ok, sort_columns} = get_cursor_columns(opts)

        if unique_column in sort_columns do
          {:ok, opts}
        else
          {:ok, %{opts | sorts: sorts ++ [unique_sort]}}
        end

      %{} ->
        {:ok, Map.put(opts, :sorts, [unique_sort])}
    end
  end

  defp set_default_sorts(%{sorts: sorts} = opts, _config) when is_list(sorts) do
    {:ok, opts}
  end

  defp set_default_sorts(_pagination_args, _config) do
    {:error, "Must supply at least one column to sort by in :sorts"}
  end

  defp get_cursor_columns(opts) do
    cursor_columns =
      opts
      |> Map.fetch!(:sorts)
      |> Enum.map(fn column_and_dir_map ->
        get_cursor_column(column_and_dir_map)
      end)

    if :invalid_cursor_column in cursor_columns do
      {:error, :invalid_cursor_column}
    else
      {:ok, cursor_columns}
    end
  end

  defp get_cursor_column(column_and_dir_map) do
    case Map.to_list(column_and_dir_map) do
      # each cursor column should look like %{id: :asc} or %{last_name: :desc},
      [{column, _dir}] when is_atom(column) -> column
      [{column, _dir}] when is_binary(column) -> String.to_existing_atom(column)
      _ -> :invalid_cursor_column
    end
  rescue
    BadMapError ->
      :invalid_cursor_column

    ArgumentError ->
      :invalid_cursor_column
  end

  defp build_edges(nodes, cursor_columns) do
    Enum.map(nodes, fn node ->
      cursor = key_to_cursor(node, cursor_columns)

      %{
        node: node,
        cursor: cursor
      }
    end)
  end

  defp apply_sorts(query, opts) do
    # If we want the last 5 records before id 10, ascending, we need to reverse
    # the order of the query to descending and do WHERE id < 10 ORDER BY id
    # DESC LIMIT 5.
    # This gets the correct records, but in the wrong order, so we will have to
    # reverse them later to get them in ascending order.
    flip? = Map.has_key?(opts, :last)

    sorts =
      opts
      |> Map.fetch!(:sorts)
      |> Enum.map(fn m -> Enum.to_list(m) end)

    # Applies each of the sorts (eg [%{year: :desc}, %{name: :asc}]) in the order
    # given. To get the opposite ordering, it reverses each of them.
    # Eg this query: `SELECT * FROM fruits ORDER BY year DESC, name ASC`
    #
    # 2 2021 Apple
    # 4 2021 Banana
    # 3 2020 Apple
    # 1 2020 Banana
    #
    # Reverses to: `SELECT * FROM fruits ORDER BY year ASC, name DESC`
    #
    # 1 2020 Banana
    # 3 2020 Apple
    # 4 2021 Banana
    # 2 2021 Apple
    Enum.reduce(sorts, query, fn [{field, dir}], query ->
      apply_sort(query, field, {dir, flip?})
    end)
  end

  defp apply_sort(query, field, dir_and_flip)
       when dir_and_flip in [{:asc, false}, {:desc, true}] do
    Ecto.Query.order_by(query, [q], asc: field(q, ^field))
  end

  defp apply_sort(query, field, dir_and_flip)
       when dir_and_flip in [{:desc, false}, {:asc, true}] do
    Ecto.Query.order_by(query, [q], desc: field(q, ^field))
  end

  defp limit_plus_one(query, %{first: count}) do
    Ecto.Query.limit(query, ^count + 1)
  end

  defp limit_plus_one(query, %{last: count}) do
    Ecto.Query.limit(query, ^count + 1)
  end

  defp limit_plus_one(query, _) do
    query
  end

  defp check_for_extra_and_trim(nodes, %{first: limit}) do
    do_check_for_extra_and_trim(nodes, limit)
  end

  defp check_for_extra_and_trim(nodes, %{last: limit}) do
    do_check_for_extra_and_trim(nodes, limit)
  end

  defp check_for_extra_and_trim(nodes, _) do
    do_check_for_extra_and_trim(nodes, :infinity)
  end

  defp do_check_for_extra_and_trim(nodes, limit) do
    if Enum.count(nodes) > limit do
      {true, Enum.take(nodes, limit)}
    else
      {false, nodes}
    end
  end

  defp maybe_reverse(nodes, opts) do
    if Map.has_key?(opts, :last) do
      # because we ordered the opposite way for the limit query
      Enum.reverse(nodes)
    else
      nodes
    end
  end

  # With `:after`, the column is sorted as expected
  defp apply_where(query, %{sorts: sorts, after: cursor}) when length(sorts) == 1 do
    [[{col_name, col_dir}]] = Enum.map(sorts, &Map.to_list/1)

    col_val = Map.fetch!(cursor, col_name)

    query =
      case col_dir do
        :asc ->
          where_query(
            query,
            {:gt},
            col_name,
            col_val
          )

        :desc ->
          where_query(
            query,
            {:lt},
            col_name,
            col_val
          )
      end

    {:ok, query}
  end

  # With `:before`, the column is sorted the opposite way
  defp apply_where(query, %{sorts: sorts, before: cursor}) when length(sorts) == 1 do
    [[{col_name, col_dir}]] = Enum.map(sorts, &Map.to_list/1)

    col_val = Map.fetch!(cursor, col_name)

    query =
      case col_dir do
        :asc ->
          where_query(
            query,
            {:lt},
            col_name,
            col_val
          )

        :desc ->
          where_query(
            query,
            {:gt},
            col_name,
            col_val
          )
      end

    {:ok, query}
  end

  # With `:after`, the columns are all sorted as expected
  defp apply_where(query, %{sorts: sorts, after: cursor}) when length(sorts) == 2 do
    [[{col1_name, col1_dir}], [{col2_name, col2_dir}]] = Enum.map(sorts, &Map.to_list/1)

    col1_val = Map.fetch!(cursor, col1_name)
    col2_val = Map.fetch!(cursor, col2_name)

    query =
      case {col1_dir, col2_dir} do
        {:asc, :asc} ->
          where_query(
            query,
            {:gte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )

        {:asc, :desc} ->
          where_query(
            query,
            {:gte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )

        {:desc, :asc} ->
          where_query(
            query,
            {:lte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )

        {:desc, :desc} ->
          where_query(
            query,
            {:lte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )
      end

    {:ok, query}
  end

  # With `:before`, the columns are all sorted the opposite way.
  defp apply_where(query, %{sorts: sorts, before: cursor}) when length(sorts) == 2 do
    [[{col1_name, col1_dir}], [{col2_name, col2_dir}]] = Enum.map(sorts, &Map.to_list/1)

    col1_val = Map.fetch!(cursor, col1_name)
    col2_val = Map.fetch!(cursor, col2_name)

    query =
      case {col1_dir, col2_dir} do
        {:asc, :asc} ->
          where_query(
            query,
            {:lte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )

        {:asc, :desc} ->
          where_query(
            query,
            {:lte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )

        {:desc, :asc} ->
          where_query(
            query,
            {:gte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )

        {:desc, :desc} ->
          where_query(
            query,
            {:gte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val
          )
      end

    {:ok, query}
  end

  # With `:after`, the columns are all sorted as expected
  defp apply_where(query, %{sorts: sorts, after: cursor}) when length(sorts) == 3 do
    [[{col1_name, col1_dir}], [{col2_name, col2_dir}], [{col3_name, col3_dir}]] =
      Enum.map(sorts, &Map.to_list/1)

    col1_val = Map.fetch!(cursor, col1_name)
    col2_val = Map.fetch!(cursor, col2_name)
    col3_val = Map.fetch!(cursor, col3_name)

    query =
      case {col1_dir, col2_dir, col3_dir} do
        {:asc, :asc, :asc} ->
          where_query(
            query,
            {:gte, :gte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:asc, :asc, :desc} ->
          where_query(
            query,
            {:gte, :gte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:asc, :desc, :asc} ->
          where_query(
            query,
            {:gte, :lte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:asc, :desc, :desc} ->
          where_query(
            query,
            {:gte, :lte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :asc, :asc} ->
          where_query(
            query,
            {:lte, :gte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :asc, :desc} ->
          where_query(
            query,
            {:lte, :gte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :desc, :asc} ->
          where_query(
            query,
            {:lte, :lte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :desc, :desc} ->
          where_query(
            query,
            {:lte, :lte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )
      end

    {:ok, query}
  end

  # With `:before`, the columns are all sorted the opposite way.
  defp apply_where(query, %{sorts: sorts, before: cursor}) when length(sorts) == 3 do
    [[{col1_name, col1_dir}], [{col2_name, col2_dir}], [{col3_name, col3_dir}]] =
      Enum.map(sorts, &Map.to_list/1)

    col1_val = Map.fetch!(cursor, col1_name)
    col2_val = Map.fetch!(cursor, col2_name)
    col3_val = Map.fetch!(cursor, col3_name)

    query =
      case {col1_dir, col2_dir, col3_dir} do
        {:asc, :asc, :asc} ->
          where_query(
            query,
            {:lte, :lte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:asc, :asc, :desc} ->
          where_query(
            query,
            {:lte, :lte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:asc, :desc, :asc} ->
          where_query(
            query,
            {:lte, :gte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:asc, :desc, :desc} ->
          where_query(
            query,
            {:lte, :gte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :asc, :asc} ->
          where_query(
            query,
            {:gte, :lte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :asc, :desc} ->
          where_query(
            query,
            {:gte, :lte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :desc, :asc} ->
          where_query(
            query,
            {:gte, :gte, :lt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )

        {:desc, :desc, :desc} ->
          where_query(
            query,
            {:gte, :gte, :gt},
            col1_name,
            col1_val,
            col2_name,
            col2_val,
            col3_name,
            col3_val
          )
      end

    {:ok, query}
  end

  defp apply_where(_query, %{sorts: sorts}) when length(sorts) > 3 do
    {:error,
     "more than 3 sorts are not supported at this time. Requested sorts: #{inspect(sorts)}"}
  end

  defp apply_where(query, _pagination_args) do
    {:ok, query}
  end

  defp get_page_info(%{after: _, first: _}, edges, more_pages?) do
    %{
      # `after` a valid cursor means we definitely skipped at least one
      has_previous_page: true,
      # `first` means we MAY have excluded some records at the end
      has_next_page: more_pages?
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{after: _}, edges, _more_pages?) do
    %{
      # `after` a valid cursor means we definitely skipped at least one
      has_previous_page: true,
      # without `first`, we got all records after `after`
      has_next_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{before: _, last: _}, edges, more_pages?) do
    %{
      # `before` a valid cursor means we stopped short of at least one
      has_next_page: true,
      # `last` means we MAY have excluded some records at the beginning
      has_previous_page: more_pages?
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{before: _}, edges, _more_pages?) do
    %{
      # `before` a valid cursor means we stopped short of at least one
      has_next_page: true,
      # without `last`, we got all records before `before`
      has_previous_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{first: _}, edges, more_pages?) do
    %{
      # With only :first, we may have stopped short of some records
      has_next_page: more_pages?,
      # ...but we got all the ones at the start
      has_previous_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{last: _}, edges, more_pages?) do
    %{
      # With only :last, we may have skipped some records
      has_previous_page: more_pages?,
      # ...but we got all the ones at the end
      has_next_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{}, edges, _more_pages?) do
    %{
      # With no :before, :after, :first, or :last, the current page has all
      # records
      has_next_page: false,
      has_previous_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp add_start_and_end_cursors(page_info, edges) do
    Map.merge(
      page_info,
      %{
        start_cursor: get_cursor(List.first(edges)),
        end_cursor: get_cursor(List.last(edges))
      }
    )
  end

  defp get_cursor(edge) when is_map(edge), do: Map.fetch!(edge, :cursor)
  defp get_cursor(_edge), do: nil

  @doc """
  Creates the cursor string from a key.
  This encoding is not meant to be tamper-proof, just to hide the cursor data
  as an implementation detail.

  ## Examples

  iex> key_to_cursor(%{id: 25}, [:id])
  "Z1pjTmFycmF5Y29ubmVjdGlvbjp7ImlkIjoyNX0="

  iex> key_to_cursor(%{id: 26}, [:id])
  "UGlkRWFycmF5Y29ubmVjdGlvbjp7ImlkIjoyNn0="
  """
  def key_to_cursor(key, cursor_columns) do
    key = Map.take(key, cursor_columns)
    {:ok, json} = Jason.encode(key)
    # Deterministic. Helps with visually distinguishing cursors.
    varied_padding = hash_chunk(json, 4)

    (varied_padding <> @cursor_prefix <> json)
    |> IO.iodata_to_binary()
    |> Base.encode64()
  end

  @doc """
  Rederives the key from the cursor string.
  The cursor string is supplied by users and may have been tampered with.
  However, we ensure that only the expected column values may appear in the
  cursor, so at worst, they could paginate from a different spot, which is
  fine.

  ## Examples

  iex> cursor_to_key("Z1pjTmFycmF5Y29ubmVjdGlvbjp7ImlkIjoyNX0=", [:id])
  {:ok, %{id: 25}}
  """
  def cursor_to_key(encoded_cursor, expected_columns) do
    with {:ok, _varied_padding = <<_::size(32)>> <> @cursor_prefix <> json_cursor} <-
           Base.decode64(encoded_cursor),
         {:ok, decoded_map} <- Jason.decode(json_cursor),
         {:ok, atomized} <- atomize_keys(decoded_map),
         {:ok, valid} <- ensure_valid_cursor(atomized, expected_columns) do
      {:ok, valid}
    else
      _ -> {:error, :invalid_cursor}
    end
  rescue
    ArgumentError ->
      {:error, :invalid_cursor}
  end

  defp atomize_keys(map) when is_map(map) do
    keyword_list =
      map
      |> Map.to_list()
      |> Enum.map(fn {k, v} ->
        cond do
          is_atom(k) -> {k, v}
          is_binary(k) -> {String.to_existing_atom(k), v}
          true -> raise ArgumentError, "must be an atom or binary"
        end
      end)

    {:ok, keyword_list}
  rescue
    ArgumentError ->
      {:error, :invalid_column}
  end

  defp ensure_valid_cursor(tuples, expected_columns) do
    given_columns = Keyword.keys(tuples)

    if Enum.sort(given_columns) == Enum.sort(expected_columns) do
      {:ok, Enum.into(tuples, Map.new())}
    else
      {:error, :invalid_columns}
    end
  end

  defp hash_chunk(string, length)
       when is_binary(string) and is_integer(length) and length > 0 do
    :crypto.hash(:sha256, string)
    |> Base.encode64()
    |> Kernel.binary_part(0, length)
  end

  defp where_query(
         query,
         {:lt},
         col_name,
         col_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col_name) < ^col_val
    )
  end

  defp where_query(
         query,
         {:gt},
         col_name,
         col_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col_name) > ^col_val
    )
  end

  defp where_query(
         query,
         {:lte, :lt},
         col1_name,
         col1_val,
         col2_name,
         col2_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) < ^col1_val or
        (field(q, ^col1_name) == ^col1_val and field(q, ^col2_name) < ^col2_val)
    )
  end

  defp where_query(
         query,
         {:lte, :gt},
         col1_name,
         col1_val,
         col2_name,
         col2_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) < ^col1_val or
        (field(q, ^col1_name) == ^col1_val and field(q, ^col2_name) > ^col2_val)
    )
  end

  defp where_query(
         query,
         {:gte, :lt},
         col1_name,
         col1_val,
         col2_name,
         col2_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) > ^col1_val or
        (field(q, ^col1_name) == ^col1_val and field(q, ^col2_name) < ^col2_val)
    )
  end

  defp where_query(
         query,
         {:gte, :gt},
         col1_name,
         col1_val,
         col2_name,
         col2_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) > ^col1_val or
        (field(q, ^col1_name) == ^col1_val and field(q, ^col2_name) > ^col2_val)
    )
  end

  defp where_query(
         query,
         {:lte, :lte, :lt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) < ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) < ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) < ^col3_val)))
    )
  end

  defp where_query(
         query,
         {:lte, :lte, :gt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) < ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) < ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) > ^col3_val)))
    )
  end

  defp where_query(
         query,
         {:lte, :gte, :lt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) < ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) > ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) < ^col3_val)))
    )
  end

  defp where_query(
         query,
         {:lte, :gte, :gt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) < ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) > ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) > ^col3_val)))
    )
  end

  defp where_query(
         query,
         {:gte, :lte, :lt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) > ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) < ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) < ^col3_val)))
    )
  end

  defp where_query(
         query,
         {:gte, :lte, :gt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) > ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) < ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) > ^col3_val)))
    )
  end

  defp where_query(
         query,
         {:gte, :gte, :gt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) > ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) > ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) > ^col3_val)))
    )
  end

  defp where_query(
         query,
         {:gte, :gte, :lt},
         col1_name,
         col1_val,
         col2_name,
         col2_val,
         col3_name,
         col3_val
       ) do
    Ecto.Query.where(
      query,
      [q],
      field(q, ^col1_name) > ^col1_val or
        (field(q, ^col1_name) == ^col1_val and
           (field(q, ^col2_name) > ^col2_val or
              (field(q, ^col2_name) == ^col2_val and field(q, ^col3_name) < ^col3_val)))
    )
  end
end
