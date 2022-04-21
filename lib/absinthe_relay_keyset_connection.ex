defmodule AbsintheRelayKeysetConnection do
  @moduledoc """
  Support for paginated result sets using keyset pagination, for use in an
  Absinthe resolver module.
  Requires defining a connection with
  [Absinthe.Relay.Connection](https://hexdocs.pm/absinthe_relay/Absinthe.Relay.Connection.html).

  ## The TL;DR

  In a resolver...

  ```elixir
  AbsintheRelayKeysetConnection.from_query(
    ecto_users_query,
    &MyRepo.all/1,
    %{
      sorts: [%{name: :desc}, %{id: :desc}],
      first: 10,
      after: "0QTwn5SRWyJNbyIsMjZd"
    },
    %{unique_column: :id}
  )
  ```

  ## More Details

  ### Why keyset pagination?

  By default,
  [Absinthe.Relay.Connection](https://hexdocs.pm/absinthe_relay/Absinthe.Relay.Connection.html)
  uses offset-based pagination.
  For example, with a page size of 10, it would get the first page of records
  with a SQL query like `OFFSET 0 LIMIT 10`, the second page with `OFFSET 10
  LIMIT 10`, and so on.
  This works well for many use cases and requires no knowledge of the
  underlying database schema.
  However, when the value of `OFFSET` is large, it [can cause poor database
  performance](https://use-the-index-luke.com/no-offset).

  Keyset pagination means that, in the example above, the first page might be
  fetched with `WHERE id > 0 ORDER BY id ASC LIMIT 10`.
  If the last record on that page had id `10`, the query for the next page could be
  fetched with `WHERE id > 10 ORDER BY id ASC LIMIT 10`.
  This `WHERE` clause lets the database efficiently ignore earlier records,
  especially if the `id` column is indexed.

  ### The cursor

  With offset-based pagination, a user needs only to say "I want 10 records per
  page, and give me page 3, please."
  We can easily calculate the offset as `(page_number - 1) * limit`.

  But for keyset-based pagination, we need more information.
  To get the next page, we need to know which record appeared last on the page
  the user just got; for example, if it was record 10, we will query `WHERE id
  > 10`.
  The user needs to supply this information using a "cursor".
  In this simple case, the cursor need contain only the id.
  (Typically this value is encoded in a way that makes it opaque to the user in
  order to indicate that it's an implementation detail.)
  But sorting and the need for uniqueness add some complexity to the picture.

  ### Sorting and uniqueness

  Keyset pagination only works if our sorting (eg `ORDER BY id asc` and
  comparison (eg `WHERE id > 10`) agree and are based on a unique column or
  combination of columns.
  Imagine trying to use a non-unique column like `last_name`.
  If the last person on the current page is `Abe Able`, requesting the next
  page with `WHERE last_name > 'Able'` will accidentally skip `Beth Able`, who should
  have appeared on the next page.

  To avoid this, we need to ensure that we order by a unique combination of
  columns - such as `ORDER BY last_name ASC, id ASC` - and use the same columns for the
  `WHERE` - such as `WHERE last_name > 'Able' OR (last_name = 'Able' AND id >
    10)`.

  If your table has a unique column like `id`, `from_query/4` can automatically
  add it to the `ORDER BY` and `WHERE` clauses of queries which don't already
  use it; just indicate which column to use in the `config` argument.

  Since the cursor is the basis of the `WHERE` clause, whatever columns the
  query is being ordered by are included in the cursor value (which is opaque
  to users).
  In the example above, the cursor for each record would include the last name
  and id.

  ## Serialization

  As explained above, building the cursor involves serializing the columns
  which are used in the `ORDER BY` so that they can also be used in the
  `WHERE`.
  For example, if ordering users by name and id, the cursor for each user
  record will contain that user's name and id.

  These values are serialized using `Jason.encode/1`, which means you'll need
  an implementation of the `Jason.Encoder` protocol for the type of each column you
  sort by.
  The library covers most common data types, but you may need to implement your
  own for less common ones.

  For example, if you're using `Postgrex.INET` for a PostgreSQL `inet` column,
  you might need:

  ```elixir
  defmodule MyApp.CustomEncoders do
   defimpl Jason.Encoder, for: [Postgrex.INET] do
     def encode(struct, opts) do
       Jason.Encode.string(EctoNetwork.INET.decode(struct), opts)
     end
   end
  end
  ```

  ### Limitations

  There are a few things you can't do with this pagination style.

  First, you can't (reliably) paginate records without specifying a unique
  column or combination of columns.
  For example, if you have an `cities` table where the primary key is the
  combination of `state_id` and `city_name`, you'll have to ensure that all
  queries include both values in their `sorts`.
  (It might be simpler to add a sequential integer column and pass that as the
  `:unique_column`.)

  Second, you can't sort by columns on associations.
  This is not implemented and would be difficult to implement.

  Third, you can't paginate at more than one level.
  For example, you can't get the first page of authors, get the first page of
  posts for each author, and proceed to get subsequent pages of posts for each
  author.
  Such an access pattern is not a good idea even with `OFFSET` pagination; some
  authors will have many more pages of posts than others.
  But it becomes truly nonsensical to say "for each author, get the first 10
  posts with id greater than 10".

  Instead of attempting this, it would be better to paginate authors, then
  separately, paginate posts filtered by author id.
  """

  require Ecto.Query
  alias AbsintheRelayKeysetConnection.Cursor

  @typedoc """
  The return value of `from_query/4`, representing the paginated data.
  """
  @type t() :: %{
          edges: [edge()],
          page_info: page_info()
        }

  @typedoc """
  A pagination cursor which is encoded and opaque to users. A cursor represents
  the position of a specific record in the pagination set. For example, the
  cursor given with post `20` represents that post, so that a user can make a
  follow-up request using the same `sorts` but specifying the first 10 records
  after post `20`, the last 5 records before post `20`, or something similar.
  """
  @type encoded_cursor() :: binary()

  @typedoc """
  A wrapper for a single record which includes the record itself (the node) and
  a cursor that references it.
  """
  @type edge() :: %{
          node: edge_node(),
          cursor: encoded_cursor()
        }

  @typedoc "A single record."
  @type edge_node() :: term()

  @typedoc """
  Information about the set of records in the current page and how it relates
  to the overall set of records available for pagination.
  """
  @type page_info() :: %{
          start_cursor: encoded_cursor(),
          end_cursor: encoded_cursor(),
          has_previous_page: boolean(),
          has_next_page: boolean()
        }

  @typedoc """
  A function which can take an `Ecto.Queryable()` and use it to fetch records
  from a data store.
  A common example would be `&MyRepo.all/1`.
  """
  @type repo_fun() :: (Ecto.Queryable.t() -> [term()])

  @typedoc """
  The name of a column to be used in an `ORDER BY` clause.
  """
  @type column_name() :: atom()

  @typedoc """
  Either `:asc` or `:desc`, to be used in an `ORDER BY` clause.
  """
  @type sort_dir() :: :asc | :desc

  @typedoc """
  A single-key map, such as `%{name: :asc}`.

  This is the information needed to build a single `ORDER BY` clause.
  """
  @type sort() :: %{column_name() => sort_dir()}

  @typedoc """
  Options derived from the current query document.
  """
  @type options() :: %{
          optional(:after) => encoded_cursor(),
          optional(:before) => encoded_cursor(),
          optional(:first) => pos_integer(),
          optional(:last) => pos_integer(),
          optional(:sorts) => [sort()],
          # if other keys are present, they are ignored
          optional(any()) => any()
        }

  @typedoc """
  Options that are independent of the current query document.
  """
  @type config() :: %{
          optional(:unique_column) => atom()
        }

  @doc """
  Build a connection from an Ecto Query.

  This will automatically set an `ORDER BY` and `WHERE` value based on the
  provided options, including the cursor (if one is given), then run the query
  with the `repo_fun` argument that was given.

  Return a single page of results which contains the info specified in the
  [Relay Cursor Connections
  Specification](https://relay.dev/graphql/connections.htm).

  ## Example

      iex> AbsintheRelayKeysetConnection.from_query(
      ...>   ecto_users_query,
      ...>   &MyRepo.all/1,
      ...>   %{
      ...>     sorts: [%{name: :desc}, %{id: :desc}],
      ...>     first: 10,
      ...>     after: "0QTwn5SRWyJNbyIsMjZd"
      ...>   },
      ...>   %{unique_column: :id}
      ...> )
      {:ok, %{
        edges: [
          %{node: %MyApp.User{id: 11, name: "Jo"}, cursor: "abc123"},
          %{node: %MyApp.User{id: 12, name: "Mo"}, cursor: "def345"}
        ],
        page_info: %{
          start_cursor: "abc123",
          end_cursor: "def345",
          has_previous_page: true,
          has_next_page: false
        }
      }}
  """
  @spec from_query(
          queryable :: Ecto.Queryable.t(),
          repo_fun :: repo_fun(),
          options :: options(),
          config :: config()
        ) :: {:ok, t()} | {:error, String.t()}
  def from_query(query, repo_fun, options, config \\ %{})

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
    case Cursor.to_key(encoded_cursor, cursor_columns) do
      {:ok, key} -> {:ok, Map.put(opts, :after, key)}
      {:error, msg} -> {:error, msg}
    end
  end

  defp decode_cursor(%{before: encoded_cursor} = opts, cursor_columns) do
    case Cursor.to_key(encoded_cursor, cursor_columns) do
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
    unique_sort = %{unique_column => :asc}

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
      cursor = Cursor.from_key(node, cursor_columns)

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

  defp limit_plus_one(query, _opts) do
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

  # because we ordered the opposite way for the limit query
  defp maybe_reverse(nodes, %{last: _last}), do: Enum.reverse(nodes)
  defp maybe_reverse(nodes, _opts), do: nodes

  defp apply_where(query, %{sorts: sorts} = opts)
       when is_map_key(opts, :before) or is_map_key(opts, :after) do
    reversed? = Map.has_key?(opts, :before)
    cursor = if reversed?, do: opts.before, else: opts.after
    sorts = sorts |> Enum.flat_map(&Enum.to_list/1) |> Enum.reverse()

    {:ok, Ecto.Query.where(query, ^do_build_where(nil, sorts, reversed?, cursor))}
  end

  defp apply_where(query, _opts) do
    {:ok, query}
  end

  # Reverses the direction if the cursor is reversed
  defp normalize_direction(direction, reversed?)
  defp normalize_direction(direction, false), do: direction
  defp normalize_direction(:asc, true), do: :desc
  defp normalize_direction(:desc, true), do: :asc

  # We build the where clauses by iterating over the sorts in reverse. The last column is
  # iterated first, and `dynamic` will be nil. This is the only column to not combine with
  # the previous columns' clauses.
  defp do_build_where(dynamic, sorts, reversed?, cursor)

  # This is the first element in sorts, which is the last column to filter on
  defp do_build_where(nil, [{col_name, col_dir} | sorts], reversed?, cursor) do
    column_value = Map.fetch!(cursor, col_name)

    dynamic =
      case normalize_direction(col_dir, reversed?) do
        :asc -> Ecto.Query.dynamic([q], field(q, ^col_name) > ^column_value)
        :desc -> Ecto.Query.dynamic([q], field(q, ^col_name) < ^column_value)
      end

    case sorts do
      [_ | _] -> do_build_where(dynamic, sorts, reversed?, cursor)
      [] -> dynamic
    end
  end

  defp do_build_where(dynamic, [{col_name, col_dir} | sorts], reversed?, cursor) do
    column_value = Map.fetch!(cursor, col_name)

    dir_clause =
      case normalize_direction(col_dir, reversed?) do
        :asc -> Ecto.Query.dynamic([q], field(q, ^col_name) > ^column_value)
        :desc -> Ecto.Query.dynamic([q], field(q, ^col_name) < ^column_value)
      end

    dynamic =
      Ecto.Query.dynamic([q], ^dir_clause or (field(q, ^col_name) == ^column_value and ^dynamic))

    case sorts do
      [_ | _] -> do_build_where(dynamic, sorts, reversed?, cursor)
      [] -> dynamic
    end
  end

  defp get_page_info(%{after: _after, first: _first}, edges, more_pages?) do
    %{
      # `after` a valid cursor means we definitely skipped at least one
      has_previous_page: true,
      # `first` means we MAY have excluded some records at the end
      has_next_page: more_pages?
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{after: _after}, edges, _more_pages?) do
    %{
      # `after` a valid cursor means we definitely skipped at least one
      has_previous_page: true,
      # without `first`, we got all records after `after`
      has_next_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{before: _before, last: _last}, edges, more_pages?) do
    %{
      # `before` a valid cursor means we stopped short of at least one
      has_next_page: true,
      # `last` means we MAY have excluded some records at the beginning
      has_previous_page: more_pages?
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{before: _before}, edges, _more_pages?) do
    %{
      # `before` a valid cursor means we stopped short of at least one
      has_next_page: true,
      # without `last`, we got all records before `before`
      has_previous_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{first: _first}, edges, more_pages?) do
    %{
      # With only :first, we may have stopped short of some records
      has_next_page: more_pages?,
      # ...but we got all the ones at the start
      has_previous_page: false
    }
    |> add_start_and_end_cursors(edges)
  end

  defp get_page_info(%{last: _last}, edges, more_pages?) do
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
end
