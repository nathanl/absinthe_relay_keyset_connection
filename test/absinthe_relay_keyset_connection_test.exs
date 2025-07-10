defmodule AbsintheRelayKeysetConnectionTest do
  use AbsintheRelayKeysetConnection.DataCase, async: true

  alias AbsintheRelayKeysetConnection, as: KC
  alias AbsintheRelayKeysetConnection.{Repo, User, Users}

  describe "sorted ascending by id" do
    test "with :first" do
      users = insert_generic_users(10)

      assert {:ok, %{edges: edges, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{first: 3},
                 %{unique_column: :id}
               )

      expected_ids = users |> Enum.take(3) |> Enum.map(& &1.id)
      found_ids = Enum.map(edges, fn e -> e.node.id end)
      assert found_ids == expected_ids

      first_cursor = List.first(edges) |> Map.fetch!(:cursor)
      last_cursor = List.last(edges) |> Map.fetch!(:cursor)
      assert page_info.start_cursor == first_cursor
      assert page_info.end_cursor == last_cursor
      # We got all records at the beginning, so there is no previous page
      assert page_info.has_previous_page == false
      # We only got the first 3 records, so there are others
      # we could get by querying :after the cursor of the last
      # record on this page
      assert page_info.has_next_page == true

      for count <- [10, 100] do
        assert {:ok, %{edges: _edges, page_info: page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{first: count},
                   %{unique_column: :id}
                 )

        # This time we got all the records, so there is no next or
        # previous page
        assert page_info.has_previous_page == false
        assert page_info.has_next_page == false
      end
    end

    test "with :last" do
      users = insert_generic_users(10)

      assert {:ok, %{edges: edges, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{last: 3},
                 %{unique_column: :id}
               )

      expected_ids = users |> Enum.take(-3) |> Enum.map(& &1.id)
      found_ids = Enum.map(edges, fn e -> e.node.id end)
      assert found_ids == expected_ids

      first_cursor = List.first(edges) |> Map.fetch!(:cursor)
      last_cursor = List.last(edges) |> Map.fetch!(:cursor)
      assert page_info.start_cursor == first_cursor
      assert page_info.end_cursor == last_cursor
      # We only got the last 3 records, so there are others
      # we could get by querying :before the cursor of the first
      # record on this page
      assert page_info.has_previous_page == true
      # We got all records at the end, so there is no next page
      assert page_info.has_next_page == false

      for count <- [10, 100] do
        assert {:ok, %{edges: _edges, page_info: page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{last: count},
                   %{unique_column: :id}
                 )

        # This time we got all the records, so there is no next or
        # previous page
        assert page_info.has_previous_page == false
        assert page_info.has_next_page == false
      end
    end

    test "with :first + :after, gets :first records :after that cursor" do
      users = insert_generic_users(10)

      assert {:ok, %{edges: all_edges, page_info: _page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{first: 10},
                 %{unique_column: :id}
               )

      all_cursors = Enum.map(all_edges, fn e -> e.cursor end)

      after_cursor =
        all_cursors
        |> Enum.drop(1)
        |> List.first()

      # 'after' will drop the record matching the cursor and all those before it
      expected_users =
        users
        |> Enum.drop(2)
        |> Enum.take(3)

      assert {:ok, %{edges: edges, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{first: 3, after: after_cursor},
                 %{unique_column: :id}
               )

      expected_ids = Enum.map(expected_users, fn s -> s.id end)
      found_ids = Enum.map(edges, fn e -> e.node.id end)
      assert found_ids == expected_ids

      first_cursor = List.first(edges) |> Map.fetch!(:cursor)
      last_cursor = List.last(edges) |> Map.fetch!(:cursor)
      assert page_info.start_cursor == first_cursor
      assert page_info.end_cursor == last_cursor
      # We got records :after the cursor, so at minimum there is
      # a previous page containing that record
      assert page_info.has_previous_page == true
      # We only got the first 3 records, so there are others
      # we could get by querying :after the cursor of the last
      # record on this page
      assert page_info.has_next_page == true

      for count <- [8, 100] do
        assert {:ok, %{edges: _edges, page_info: page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{first: count, after: after_cursor},
                   %{unique_column: :id}
                 )

        # We got records :after the cursor, so at minimum there is
        # a previous page containing that record
        assert page_info.has_previous_page == true
        # This time we got all the records :after the cursor, so there is
        # no next page
        assert page_info.has_next_page == false
      end
    end

    test "with :last + :before, gets :last records :before that cursor" do
      users = insert_generic_users(10)

      assert {:ok, %{edges: all_edges, page_info: _page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{first: 10},
                 %{unique_column: :id}
               )

      all_cursors = Enum.map(all_edges, fn e -> e.cursor end)

      before_cursor =
        all_cursors
        |> Enum.drop(-1)
        |> List.last()

      # 'before' will drop the record matching the cursor and all those after it
      expected_users =
        users
        |> Enum.drop(-2)
        |> Enum.take(-3)

      assert {:ok, %{edges: edges, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{last: 3, before: before_cursor},
                 %{unique_column: :id}
               )

      expected_ids = Enum.map(expected_users, fn s -> s.id end)
      found_ids = Enum.map(edges, fn e -> e.node.id end)
      assert found_ids == expected_ids

      first_cursor = List.first(edges) |> Map.fetch!(:cursor)
      last_cursor = List.last(edges) |> Map.fetch!(:cursor)
      assert page_info.start_cursor == first_cursor
      assert page_info.end_cursor == last_cursor
      # We got records :before the cursor, so at minimum there is
      # a next page containing that record
      assert page_info.has_next_page == true
      # We only got the :last 3 before the cursor, so there
      # are other records before that which we could reach
      # by adjusting our :before cursor
      assert page_info.has_previous_page == true

      for count <- [8, 100] do
        assert {:ok, %{edges: _edges, page_info: page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{last: count, before: before_cursor},
                   %{unique_column: :id}
                 )

        # We got records :before the cursor, so at minimum there is
        # a next page containing that record
        assert page_info.has_next_page == true
        # This time we got all records :before the cursor, so there is no
        # previous page
        assert page_info.has_previous_page == false
      end
    end
  end

  describe "sorted descending by id" do
    test "with :first + :after, gets :first records :after that cursor" do
      users = insert_generic_users(10)

      assert {:ok, %{edges: all_edges, page_info: _page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{id: :desc}],
                   first: 10
                 }
               )

      cursors = Enum.map(all_edges, fn e -> e.cursor end)

      after_cursor =
        cursors
        |> Enum.drop(1)
        |> List.first()

      assert {:ok, %{edges: edges, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{sorts: [%{id: :desc}], after: after_cursor, first: 3}
               )

      expected_ids =
        users
        |> Enum.reverse()
        |> Enum.drop(2)
        |> Enum.take(3)
        |> Enum.map(& &1.id)

      found_ids = Enum.map(edges, fn e -> e.node.id end)
      assert found_ids == expected_ids

      first_cursor = List.first(edges) |> Map.fetch!(:cursor)
      last_cursor = List.last(edges) |> Map.fetch!(:cursor)
      assert page_info.start_cursor == first_cursor
      assert page_info.end_cursor == last_cursor
      # We got records :after the cursor, so at minimum there is
      # a previous page containing that record
      assert page_info.has_previous_page == true
      # We only got the first 3 records, so there are others
      # we could get by querying :after the cursor of the last
      # record on this page
      assert page_info.has_next_page == true

      for count <- [8, 100] do
        assert {:ok, %{edges: _edges, page_info: page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{first: count, after: after_cursor},
                   %{unique_column: :id}
                 )

        # We got records :after the cursor, so at minimum there is
        # a previous page containing that record
        assert page_info.has_previous_page == true
        # This time we got all the records :after the cursor, so there is
        # no next page
        assert page_info.has_next_page == false
      end
    end

    test "with :last + :before, gets :last records :before that cursor" do
      users = insert_generic_users(10)

      assert {:ok, %{edges: all_edges, page_info: _page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{id: :desc}],
                   first: 10
                 }
               )

      cursors = Enum.map(all_edges, fn e -> e.cursor end)

      before_cursor =
        cursors
        |> Enum.drop(-1)
        |> List.last()

      # 'before' will drop the record matching the cursor and all those after it
      expected_users =
        users
        # descending order
        |> Enum.reverse()
        |> Enum.drop(-2)
        |> Enum.take(-3)

      assert {:ok, %{edges: edges, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{sorts: [%{id: :desc}], last: 3, before: before_cursor}
               )

      expected_ids = Enum.map(expected_users, fn s -> s.id end)
      found_ids = Enum.map(edges, fn e -> e.node.id end)
      assert found_ids == expected_ids

      first_cursor = List.first(edges) |> Map.fetch!(:cursor)
      last_cursor = List.last(edges) |> Map.fetch!(:cursor)
      assert page_info.start_cursor == first_cursor
      assert page_info.end_cursor == last_cursor
      # We got records :before the cursor, so at minimum there is
      # a next page containing that record
      assert page_info.has_next_page == true
      # We only got the :last 3 before the cursor, so there
      # are other records before that which we could reach
      # by adjusting our :before cursor
      assert page_info.has_previous_page == true

      for count <- [8, 100] do
        assert {:ok, %{edges: _edges, page_info: page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{sorts: [%{id: :desc}], last: count, before: before_cursor}
                 )

        # We got records :before the cursor, so at minimum there is
        # a next page containing that record
        assert page_info.has_next_page == true
        # This time we got all the records :before the cursor, so there is
        # no previous page
        assert page_info.has_previous_page == false
      end
    end
  end

  describe "with one sort by a non-unique field, it appends id: :asc to ensure uniqueness so that records aren't skipped" do
    def one_sort_and_expected_results do
      [abe_1, abe_2, bea_1, bea_2, cal_1, cal_2] =
        insert_users([
          %{first_name: "Abe", id: 1},
          %{first_name: "Abe", id: 2},
          %{first_name: "Bea", id: 3},
          %{first_name: "Bea", id: 4},
          %{first_name: "Cal", id: 5},
          %{first_name: "Cal", id: 6}
        ])

      [
        {
          [%{first_name: :asc}],
          [
            abe_1,
            abe_2,
            bea_1,
            bea_2,
            cal_1,
            cal_2
          ]
        },
        {
          [%{first_name: :desc}],
          [
            cal_1,
            cal_2,
            bea_1,
            bea_2,
            abe_1,
            abe_2
          ]
        }
      ]
    end

    test "paginating forward" do
      for {sorts, expected_results} <- one_sort_and_expected_results() do
        # fetch the first page
        assert {:ok, %{edges: edges, page_info: first_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     first: 3
                   },
                   %{unique_column: :id}
                 )

        assert [expected_first, expected_second, expected_third, _, _, _] = expected_results
        assert [found_first, found_second, found_third] = Enum.map(edges, fn e -> e.node end)

        assert found_first.first_name == expected_first.first_name
        assert found_first.last_name == expected_first.last_name
        assert found_first.id == expected_first.id

        assert found_second.first_name == expected_second.first_name
        assert found_second.last_name == expected_second.last_name
        assert found_second.id == expected_second.id

        assert found_third.first_name == expected_third.first_name
        assert found_third.last_name == expected_third.last_name
        assert found_third.id == expected_third.id
        true

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert first_page_info.start_cursor == first_cursor
        assert first_page_info.end_cursor == last_cursor

        assert first_page_info.has_next_page == true
        assert first_page_info.has_previous_page == false

        # fetch the second page
        assert {:ok, %{edges: edges, page_info: second_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     first: 3,
                     after: first_page_info.end_cursor
                   },
                   %{unique_column: :id}
                 )

        assert [_, _, _, expected_fourth, expected_fifth, expected_sixth] = expected_results
        assert [found_fourth, found_fifth, found_sixth] = Enum.map(edges, fn e -> e.node end)

        assert found_fourth.first_name == expected_fourth.first_name
        assert found_fourth.last_name == expected_fourth.last_name
        assert found_fourth.id == expected_fourth.id

        assert found_fifth.first_name == expected_fifth.first_name
        assert found_fifth.last_name == expected_fifth.last_name
        assert found_fifth.id == expected_fifth.id

        assert found_sixth.first_name == expected_sixth.first_name
        assert found_sixth.last_name == expected_sixth.last_name
        assert found_sixth.id == expected_sixth.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert second_page_info.start_cursor == first_cursor
        assert second_page_info.end_cursor == last_cursor

        assert second_page_info.has_next_page == false
        assert second_page_info.has_previous_page == true
      end
    end

    test "paginating backward" do
      for {sorts, expected_results} <- one_sort_and_expected_results() do
        # fetch the first page
        assert {:ok, %{edges: edges, page_info: second_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     last: 3
                   },
                   %{unique_column: :id}
                 )

        assert [_, _, _, expected_fourth, expected_fifth, expected_sixth] = expected_results
        assert [found_fourth, found_fifth, found_sixth] = Enum.map(edges, fn e -> e.node end)

        assert found_fourth.first_name == expected_fourth.first_name
        assert found_fourth.last_name == expected_fourth.last_name
        assert found_fourth.id == expected_fourth.id

        assert found_fifth.first_name == expected_fifth.first_name
        assert found_fifth.last_name == expected_fifth.last_name
        assert found_fifth.id == expected_fifth.id

        assert found_sixth.first_name == expected_sixth.first_name
        assert found_sixth.last_name == expected_sixth.last_name
        assert found_sixth.id == expected_sixth.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert second_page_info.start_cursor == first_cursor
        assert second_page_info.end_cursor == last_cursor

        assert second_page_info.has_next_page == false
        assert second_page_info.has_previous_page == true

        # fetch the second page
        assert {:ok, %{edges: edges, page_info: first_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     last: 3,
                     before: second_page_info.start_cursor
                   },
                   %{unique_column: :id}
                 )

        assert [expected_first, expected_second, expected_third, _, _, _] = expected_results
        assert [found_first, found_second, found_third] = Enum.map(edges, fn e -> e.node end)

        assert found_first.first_name == expected_first.first_name
        assert found_first.last_name == expected_first.last_name
        assert found_first.id == expected_first.id

        assert found_second.first_name == expected_second.first_name
        assert found_second.last_name == expected_second.last_name
        assert found_second.id == expected_second.id

        assert found_third.first_name == expected_third.first_name
        assert found_third.last_name == expected_third.last_name
        assert found_third.id == expected_third.id
        true

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert first_page_info.start_cursor == first_cursor
        assert first_page_info.end_cursor == last_cursor

        assert first_page_info.has_next_page == true
        assert first_page_info.has_previous_page == false
      end
    end
  end

  describe "with two sorts" do
    def two_sorts_and_expected_results do
      [abe_1, abe_2, ann, bea, cal, dan] =
        insert_users([
          %{first_name: "Abe", id: 1},
          %{first_name: "Abe", id: 2},
          %{first_name: "Ann", id: 3},
          %{first_name: "Bea", id: 4},
          %{first_name: "Cal", id: 5},
          %{first_name: "Dan", id: 6}
        ])

      [
        {
          [%{first_name: :asc}, %{id: :asc}],
          [
            abe_1,
            abe_2,
            ann,
            bea,
            cal,
            dan
          ]
        },
        {
          [%{first_name: :asc}, %{id: :desc}],
          [
            abe_2,
            abe_1,
            ann,
            bea,
            cal,
            dan
          ]
        },
        {
          [%{first_name: :desc}, %{id: :asc}],
          [
            dan,
            cal,
            bea,
            ann,
            abe_1,
            abe_2
          ]
        },
        {
          [%{first_name: :desc}, %{id: :desc}],
          [
            dan,
            cal,
            bea,
            ann,
            abe_2,
            abe_1
          ]
        }
      ]
    end

    test "paginating forward" do
      for {sorts, expected_results} <- two_sorts_and_expected_results() do
        # fetch the first page
        assert {:ok, %{edges: edges, page_info: first_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     first: 3
                   }
                 )

        assert [expected_first, expected_second, expected_third, _, _, _] = expected_results
        assert [found_first, found_second, found_third] = Enum.map(edges, fn e -> e.node end)

        assert found_first.first_name == expected_first.first_name
        assert found_first.last_name == expected_first.last_name
        assert found_first.id == expected_first.id

        assert found_second.first_name == expected_second.first_name
        assert found_second.last_name == expected_second.last_name
        assert found_second.id == expected_second.id

        assert found_third.first_name == expected_third.first_name
        assert found_third.last_name == expected_third.last_name
        assert found_third.id == expected_third.id
        true

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert first_page_info.start_cursor == first_cursor
        assert first_page_info.end_cursor == last_cursor

        assert first_page_info.has_next_page == true
        assert first_page_info.has_previous_page == false

        # fetch the second page
        assert {:ok, %{edges: edges, page_info: second_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     first: 3,
                     after: first_page_info.end_cursor
                   }
                 )

        assert [_, _, _, expected_fourth, expected_fifth, expected_sixth] = expected_results
        assert [found_fourth, found_fifth, found_sixth] = Enum.map(edges, fn e -> e.node end)

        assert found_fourth.first_name == expected_fourth.first_name
        assert found_fourth.last_name == expected_fourth.last_name
        assert found_fourth.id == expected_fourth.id

        assert found_fifth.first_name == expected_fifth.first_name
        assert found_fifth.last_name == expected_fifth.last_name
        assert found_fifth.id == expected_fifth.id

        assert found_sixth.first_name == expected_sixth.first_name
        assert found_sixth.last_name == expected_sixth.last_name
        assert found_sixth.id == expected_sixth.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert second_page_info.start_cursor == first_cursor
        assert second_page_info.end_cursor == last_cursor

        assert second_page_info.has_next_page == false
        assert second_page_info.has_previous_page == true
      end
    end

    test "paginating backward" do
      for {sorts, expected_results} <- two_sorts_and_expected_results() do
        # fetch the second page
        assert {:ok, %{edges: edges, page_info: second_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     last: 3
                   }
                 )

        assert [_, _, _, expected_fourth, expected_fifth, expected_sixth] = expected_results
        assert [found_fourth, found_fifth, found_sixth] = Enum.map(edges, fn e -> e.node end)

        assert found_fourth.first_name == expected_fourth.first_name
        assert found_fourth.last_name == expected_fourth.last_name
        assert found_fourth.id == expected_fourth.id

        assert found_fifth.first_name == expected_fifth.first_name
        assert found_fifth.last_name == expected_fifth.last_name
        assert found_fifth.id == expected_fifth.id

        assert found_sixth.first_name == expected_sixth.first_name
        assert found_sixth.last_name == expected_sixth.last_name
        assert found_sixth.id == expected_sixth.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert second_page_info.start_cursor == first_cursor
        assert second_page_info.end_cursor == last_cursor

        assert second_page_info.has_next_page == false
        assert second_page_info.has_previous_page == true

        # fetch the first page
        assert {:ok, %{edges: edges, page_info: first_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     last: 3,
                     before: second_page_info.start_cursor
                   }
                 )

        assert [expected_first, expected_second, expected_third, _, _, _] = expected_results
        assert [found_first, found_second, found_third] = Enum.map(edges, fn e -> e.node end)

        assert found_first.first_name == expected_first.first_name
        assert found_first.last_name == expected_first.last_name
        assert found_first.id == expected_first.id

        assert found_second.first_name == expected_second.first_name
        assert found_second.last_name == expected_second.last_name
        assert found_second.id == expected_second.id

        assert found_third.first_name == expected_third.first_name
        assert found_third.last_name == expected_third.last_name
        assert found_third.id == expected_third.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert first_page_info.start_cursor == first_cursor
        assert first_page_info.end_cursor == last_cursor

        assert first_page_info.has_next_page == true
        assert first_page_info.has_previous_page == false
      end
    end
  end

  describe "with three sorts" do
    def three_sorts_and_expected_results do
      [abe_ableton, abe_avila, ann_ableton, bea_bryant_1, bea_bryant_2, cal_carter] =
        insert_users([
          %{first_name: "Abe", last_name: "Ableton", id: 1},
          %{first_name: "Abe", last_name: "Avila", id: 2},
          %{first_name: "Ann", last_name: "Ableton", id: 3},
          %{first_name: "Bea", last_name: "Bryant", id: 4},
          %{first_name: "Bea", last_name: "Bryant", id: 5},
          %{first_name: "Cal", last_name: "Carter", id: 6}
        ])

      [
        {
          [%{first_name: :asc}, %{last_name: :asc}, %{id: :asc}],
          [
            abe_ableton,
            abe_avila,
            ann_ableton,
            bea_bryant_1,
            bea_bryant_2,
            cal_carter
          ]
        },
        {
          [%{first_name: :asc}, %{last_name: :asc}, %{id: :desc}],
          [
            abe_ableton,
            abe_avila,
            ann_ableton,
            bea_bryant_2,
            bea_bryant_1,
            cal_carter
          ]
        },
        {
          [%{first_name: :asc}, %{last_name: :desc}, %{id: :asc}],
          [
            abe_avila,
            abe_ableton,
            ann_ableton,
            bea_bryant_1,
            bea_bryant_2,
            cal_carter
          ]
        },
        {
          [%{first_name: :asc}, %{last_name: :desc}, %{id: :desc}],
          [
            abe_avila,
            abe_ableton,
            ann_ableton,
            bea_bryant_2,
            bea_bryant_1,
            cal_carter
          ]
        },
        {
          [%{first_name: :desc}, %{last_name: :asc}, %{id: :asc}],
          [
            cal_carter,
            bea_bryant_1,
            bea_bryant_2,
            ann_ableton,
            abe_ableton,
            abe_avila
          ]
        },
        {
          [%{first_name: :desc}, %{last_name: :asc}, %{id: :desc}],
          [
            cal_carter,
            bea_bryant_2,
            bea_bryant_1,
            ann_ableton,
            abe_ableton,
            abe_avila
          ]
        },
        {
          [%{first_name: :desc}, %{last_name: :desc}, %{id: :asc}],
          [
            cal_carter,
            bea_bryant_1,
            bea_bryant_2,
            ann_ableton,
            abe_avila,
            abe_ableton
          ]
        },
        {
          [%{first_name: :desc}, %{last_name: :desc}, %{id: :desc}],
          [
            cal_carter,
            bea_bryant_2,
            bea_bryant_1,
            ann_ableton,
            abe_avila,
            abe_ableton
          ]
        }
      ]
    end

    test "paginating forward" do
      for {sorts, expected_results} <- three_sorts_and_expected_results() do
        # fetch the first page
        assert {:ok, %{edges: edges, page_info: first_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     first: 3
                   }
                 )

        assert [expected_first, expected_second, expected_third, _, _, _] = expected_results
        assert [found_first, found_second, found_third] = Enum.map(edges, fn e -> e.node end)

        assert found_first.first_name == expected_first.first_name
        assert found_first.last_name == expected_first.last_name
        assert found_first.id == expected_first.id

        assert found_second.first_name == expected_second.first_name
        assert found_second.last_name == expected_second.last_name
        assert found_second.id == expected_second.id

        assert found_third.first_name == expected_third.first_name
        assert found_third.last_name == expected_third.last_name
        assert found_third.id == expected_third.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert first_page_info.start_cursor == first_cursor
        assert first_page_info.end_cursor == last_cursor

        assert first_page_info.has_next_page == true
        assert first_page_info.has_previous_page == false

        # fetch the second page
        assert {:ok, %{edges: edges, page_info: second_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     first: 3,
                     after: first_page_info.end_cursor
                   }
                 )

        assert [_, _, _, expected_fourth, expected_fifth, expected_sixth] = expected_results
        assert [found_fourth, found_fifth, found_sixth] = Enum.map(edges, fn e -> e.node end)

        assert found_fourth.first_name == expected_fourth.first_name
        assert found_fourth.last_name == expected_fourth.last_name
        assert found_fourth.id == expected_fourth.id

        assert found_fifth.first_name == expected_fifth.first_name
        assert found_fifth.last_name == expected_fifth.last_name
        assert found_fifth.id == expected_fifth.id

        assert found_sixth.first_name == expected_sixth.first_name
        assert found_sixth.last_name == expected_sixth.last_name
        assert found_sixth.id == expected_sixth.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert second_page_info.start_cursor == first_cursor
        assert second_page_info.end_cursor == last_cursor

        assert second_page_info.has_next_page == false
        assert second_page_info.has_previous_page == true
      end
    end

    test "paginating backward" do
      for {sorts, expected_results} <- three_sorts_and_expected_results() do
        # fetch the second page
        assert {:ok, %{edges: edges, page_info: second_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     last: 3
                   }
                 )

        assert [_, _, _, expected_fourth, expected_fifth, expected_sixth] = expected_results
        assert [found_fourth, found_fifth, found_sixth] = Enum.map(edges, fn e -> e.node end)

        assert found_fourth.first_name == expected_fourth.first_name
        assert found_fourth.last_name == expected_fourth.last_name
        assert found_fourth.id == expected_fourth.id

        assert found_fifth.first_name == expected_fifth.first_name
        assert found_fifth.last_name == expected_fifth.last_name
        assert found_fifth.id == expected_fifth.id

        assert found_sixth.first_name == expected_sixth.first_name
        assert found_sixth.last_name == expected_sixth.last_name
        assert found_sixth.id == expected_sixth.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert second_page_info.start_cursor == first_cursor
        assert second_page_info.end_cursor == last_cursor

        assert second_page_info.has_next_page == false
        assert second_page_info.has_previous_page == true

        # fetch the first page
        assert {:ok, %{edges: edges, page_info: first_page_info}} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{
                     sorts: sorts,
                     last: 3,
                     before: second_page_info.start_cursor
                   }
                 )

        assert [expected_first, expected_second, expected_third, _, _, _] = expected_results
        assert [found_first, found_second, found_third] = Enum.map(edges, fn e -> e.node end)

        assert found_first.first_name == expected_first.first_name
        assert found_first.last_name == expected_first.last_name
        assert found_first.id == expected_first.id

        assert found_second.first_name == expected_second.first_name
        assert found_second.last_name == expected_second.last_name
        assert found_second.id == expected_second.id

        assert found_third.first_name == expected_third.first_name
        assert found_third.last_name == expected_third.last_name
        assert found_third.id == expected_third.id

        first_cursor = List.first(edges) |> Map.fetch!(:cursor)
        last_cursor = List.last(edges) |> Map.fetch!(:cursor)
        assert first_page_info.start_cursor == first_cursor
        assert first_page_info.end_cursor == last_cursor

        assert first_page_info.has_next_page == true
        assert first_page_info.has_previous_page == false
      end
    end
  end

  describe "unsupported inputs" do
    test "requesting both :first and :last is unsupported" do
      assert {:error, msg} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{first: 3, last: 3}
               )

      assert msg =~ "first"
      assert msg =~ "last"
      assert msg =~ "unsupported"
    end

    test "requesting both :before and :after is unsupported" do
      assert {:error, msg} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{before: "some_cursor", after: "some_cursor"}
               )

      assert msg =~ "before"
      assert msg =~ "after"
      assert msg =~ "unsupported"
    end

    test "requesting :first + :before is unsupported" do
      assert {:error, msg} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{first: 2, before: "some_cursor"}
               )

      assert msg =~ "first"
      assert msg =~ "before"
      assert msg =~ "unsupported"
    end

    test "requesting :last + :after is unsupported" do
      assert {:error, msg} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{last: 2, after: "some_cursor"}
               )

      assert msg =~ "last"
      assert msg =~ "after"
      assert msg =~ "unsupported"
    end

    test "The :first option must be an integer >= 1" do
      for bad_value <- [0, -2, "few"] do
        assert {:error, msg} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{first: bad_value}
                 )

        assert msg == "The value of :first must be an integer >= 1"
      end
    end

    test "The :last option must be an integer >= 1" do
      for bad_value <- [0, -2, "few"] do
        assert {:error, msg} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{last: bad_value}
                 )

        assert msg == "The value of :last must be an integer >= 1"
      end
    end

    test "querying with neither :first nor :last is unsupported" do
      for bad_params <- [
            %{},
            %{before: "abc"},
            %{after: "abc"}
          ] do
        assert {:error, msg} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   bad_params,
                   %{unique_column: :id}
                 )

        assert msg =~ "neither :first nor :last"
      end
    end

    test "the :before and :after options require a valid cursor" do
      creates_a_new_atom = "YXJyYXljb25uZWN0aW9uOoN0AAAAAWQAEnRvdGFsbHluZXdhdG9td29vdG0AAAACeW8="

      for invalid_cursor <- ["garbage", creates_a_new_atom] do
        assert {:error, msg} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{before: invalid_cursor, last: 3},
                   %{unique_column: :id}
                 )

        assert msg == :invalid_cursor

        assert {:error, msg} =
                 KC.from_query(
                   User,
                   &Repo.all/1,
                   %{after: invalid_cursor, first: 3},
                   %{unique_column: :id}
                 )

        assert msg == :invalid_cursor
      end
    end

    test "requires either one or more sorts or a configured unique column" do
      assert {:error, msg} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{first: 10}
               )

      assert msg =~ "at least one"
      assert msg =~ "sorts"
    end

    test "errors when one map contains multiple sorts" do
      assert {:error, {:invalid_sorts, msg}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [
                     %{last_name: :asc, bite_strength: :desc}
                   ],
                   first: 3
                 }
               )

      assert msg =~ "sorts"
      assert msg =~ "order"
    end
  end

  defp insert_generic_users(n) when is_integer(n) and n > 0 do
    data =
      for i <- 1..n do
        %{id: i, first_name: "First#{i}", last_name: "Last#{i}"}
      end

    insert_users(data)
  end

  defp insert_users(data) when is_list(data) do
    expected_count = Enum.count(data)
    {^expected_count, nil} = Repo.insert_all(User, data)
    Users.all()
  end

  describe "handling NULL values in sortable columns" do
    test "crashes when navigating with NULL values without coalescing" do
      # Insert 4 users where ALL values in the sorted column are NULL
      insert_users([
        %{id: 1, first_name: "Alice", last_name: nil},
        %{id: 2, first_name: "Bob", last_name: nil},
        %{id: 3, first_name: "Charlie", last_name: nil},
        %{id: 4, first_name: "David", last_name: nil}
      ])

      # Get first page - this works
      assert {:ok, %{edges: first_page, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 2
                 },
                 %{unique_column: :id}
               )

      assert length(first_page) == 2
      assert page_info.has_next_page == true

      # Try to navigate to second page - this crashes because cursor contains NULL
      assert_raise ArgumentError, ~r/comparing.*with.*nil.*is forbidden/, fn ->
        KC.from_query(
          User,
          &Repo.all/1,
          %{
            sorts: [%{last_name: :asc}],
            first: 2,
            after: page_info.end_cursor
          },
          %{unique_column: :id}
        )
      end
    end

    test "works correctly with NULL values when using null coalescing" do
      # Insert 4 users where ALL values in the sorted column are NULL
      insert_users([
        %{id: 1, first_name: "Alice", last_name: nil},
        %{id: 2, first_name: "Bob", last_name: nil},
        %{id: 3, first_name: "Charlie", last_name: nil},
        %{id: 4, first_name: "David", last_name: nil}
      ])

      # Get first page with coalescing - this works
      assert {:ok, %{edges: first_page, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 2
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      assert length(first_page) == 2
      assert page_info.has_next_page == true

      # Navigate to second page - this now works without crashing
      assert {:ok, %{edges: second_page, page_info: second_page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 2,
                   after: page_info.end_cursor
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      assert length(second_page) == 2
      assert second_page_info.has_next_page == false
      assert second_page_info.has_previous_page == true

      # All 4 users should be retrieved across both pages
      all_ids =
        (Enum.map(first_page, & &1.node.id) ++ Enum.map(second_page, & &1.node.id)) |> Enum.sort()

      assert all_ids == [1, 2, 3, 4]
    end

    test "handles mixed NULL and non-NULL values with coalescing" do
      # Insert users with mixed NULL and non-NULL values
      insert_users([
        %{id: 1, first_name: "Alice", last_name: nil},
        %{id: 2, first_name: "Bob", last_name: "Brown"},
        %{id: 3, first_name: "Charlie", last_name: nil},
        %{id: 4, first_name: "David", last_name: "Davis"}
      ])

      # With coalescing, NULL values should be sorted as empty strings (first in ASC order)
      assert {:ok, %{edges: first_page, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 2
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      # First page should have the NULL values (coalesced to "") which come first
      assert length(first_page) == 2
      assert Enum.all?(first_page, fn edge -> edge.node.last_name == nil end)

      # Second page should have the non-NULL values
      assert {:ok, %{edges: second_page}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 2,
                   after: page_info.end_cursor
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      assert length(second_page) == 2
      assert Enum.all?(second_page, fn edge -> edge.node.last_name != nil end)

      # Should be sorted alphabetically
      last_names = Enum.map(second_page, & &1.node.last_name) |> Enum.sort()
      assert last_names == ["Brown", "Davis"]
    end

    test "supports backward pagination with NULL values and coalescing" do
      # Insert 4 users with NULL values
      insert_users([
        %{id: 1, first_name: "Alice", last_name: nil},
        %{id: 2, first_name: "Bob", last_name: nil},
        %{id: 3, first_name: "Charlie", last_name: nil},
        %{id: 4, first_name: "David", last_name: nil}
      ])

      # Get last 2 records
      assert {:ok, %{edges: last_page, page_info: page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   last: 2
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      assert length(last_page) == 2
      assert page_info.has_previous_page == true

      # Navigate backward to first 2 records
      assert {:ok, %{edges: first_page, page_info: first_page_info}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   last: 2,
                   before: page_info.start_cursor
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      assert length(first_page) == 2
      assert first_page_info.has_previous_page == false

      # All 4 users should be retrieved across both pages
      all_ids =
        (Enum.map(first_page, & &1.node.id) ++ Enum.map(last_page, & &1.node.id)) |> Enum.sort()

      assert all_ids == [1, 2, 3, 4]
    end

    test "supports multiple columns with different coalesce values" do
      # Insert users with NULLs in different columns
      insert_users([
        %{id: 1, first_name: nil, last_name: nil},
        %{id: 2, first_name: "Bob", last_name: nil},
        %{id: 3, first_name: nil, last_name: "Charlie"},
        %{id: 4, first_name: "David", last_name: "Davis"}
      ])

      # Use different coalesce values for different columns
      assert {:ok, %{edges: edges}} =
               KC.from_query(
                 User,
                 &Repo.all/1,
                 %{
                   sorts: [%{first_name: :asc}, %{last_name: :asc}],
                   first: 4
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{first_name: "AAA", last_name: "ZZZ"}
                 }
               )

      assert length(edges) == 4

      # Verify sorting order with coalesced values
      # NULL first_name becomes "AAA", NULL last_name becomes "ZZZ"
      names =
        Enum.map(edges, fn edge ->
          {edge.node.first_name || "AAA", edge.node.last_name || "ZZZ", edge.node.id}
        end)

      # Should be sorted by first_name, then last_name
      assert names == Enum.sort(names)
    end

    test "works with DISTINCT and null coalescing on base queries" do
      import Ecto.Query

      # Insert users with NULL values
      insert_users([
        %{id: 1, first_name: "Alice", last_name: nil},
        %{id: 2, first_name: "Bob", last_name: nil},
        %{id: 3, first_name: "Charlie", last_name: nil},
        %{id: 4, first_name: "David", last_name: nil}
      ])

      query = from(u in User, distinct: true)

      assert {:ok, %{edges: edges}} =
               KC.from_query(
                 query,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 2
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      # Should return 2 results
      assert length(edges) == 2
      # Verify we get actual User structs back
      assert %User{} = hd(edges).node
    end

    test "throws error for DISTINCT queries with custom select and null coalescing" do
      import Ecto.Query

      # Insert users with NULL values
      insert_users([
        %{id: 1, first_name: "Alice", last_name: nil},
        %{id: 2, first_name: "Bob", last_name: "Brown"},
        %{id: 3, first_name: "Charlie", last_name: nil},
        %{id: 4, first_name: "David", last_name: "Davis"}
      ])

      # Query with a subset select - only select specific fields
      query =
        from(u in User,
          distinct: true,
          select: %{id: u.id, first_name: u.first_name, last_name: u.last_name}
        )

      # This combination is not supported and should return an error
      assert {:error, msg} =
               KC.from_query(
                 query,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 3
                 },
                 %{
                   unique_column: :id,
                   null_coalesce: %{last_name: ""}
                 }
               )

      assert msg =~
               "DISTINCT queries with custom select clauses and null_coalesce are not supported"
    end

    test "works with DISTINCT and subset selects WITHOUT null coalescing" do
      import Ecto.Query

      # Insert users without NULL values to avoid coalescing
      insert_users([
        %{id: 1, first_name: "Alice", last_name: "Anderson"},
        %{id: 2, first_name: "Bob", last_name: "Brown"},
        %{id: 3, first_name: "Charlie", last_name: "Carter"},
        %{id: 4, first_name: "David", last_name: "Davis"}
      ])

      # Query with a subset select - only select specific fields
      query =
        from(u in User,
          distinct: true,
          select: %{id: u.id, first_name: u.first_name, last_name: u.last_name}
        )

      # This should work fine without null coalescing
      assert {:ok, %{edges: edges}} =
               KC.from_query(
                 query,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 3
                 },
                 %{unique_column: :id}
               )

      # Should return 3 results
      assert length(edges) == 3

      # Verify we get maps back (not User structs) with the selected fields
      first_node = hd(edges).node
      assert is_map(first_node)
      assert Map.has_key?(first_node, :id)
      assert Map.has_key?(first_node, :first_name)
      assert Map.has_key?(first_node, :last_name)
      refute is_struct(first_node)

      # Verify sorting
      last_names = Enum.map(edges, & &1.node.last_name)
      assert ["Anderson", "Brown", "Carter"] = last_names
    end

    test "works with DISTINCT and table name queries without null coalescing" do
      import Ecto.Query

      # Insert users without NULL values
      insert_users([
        %{id: 1, first_name: "Alice", last_name: "Anderson"},
        %{id: 2, first_name: "Bob", last_name: "Brown"},
        %{id: 3, first_name: "Charlie", last_name: "Carter"},
        %{id: 4, first_name: "David", last_name: "Davis"}
      ])

      # Query using table name instead of module
      query =
        from(u in "users",
          distinct: true,
          select: %{id: u.id, first_name: u.first_name, last_name: u.last_name}
        )

      # This should work fine without null coalescing
      assert {:ok, %{edges: edges}} =
               KC.from_query(
                 query,
                 &Repo.all/1,
                 %{
                   sorts: [%{last_name: :asc}],
                   first: 3
                 },
                 %{unique_column: :id}
               )

      # Should return 3 results
      assert length(edges) == 3

      # Verify we get maps back with the selected fields
      first_node = hd(edges).node
      assert is_map(first_node)
      assert Map.has_key?(first_node, :id)
      assert Map.has_key?(first_node, :first_name)
      assert Map.has_key?(first_node, :last_name)

      # Verify sorting
      last_names = Enum.map(edges, & &1.node.last_name)
      assert ["Anderson", "Brown", "Carter"] = last_names
    end

    test "workaround for DISTINCT + custom select + null coalescing: handle COALESCE manually" do
      import Ecto.Query

      # Insert users with NULL values
      insert_users([
        %{id: 1, first_name: "Alice", last_name: nil},
        %{id: 2, first_name: "Bob", last_name: "Brown"},
        %{id: 3, first_name: "Charlie", last_name: nil},
        %{id: 4, first_name: "David", last_name: "Davis"}
      ])

      # Workaround: handle COALESCE manually in the query
      query =
        from(u in User,
          distinct: true,
          select: %{
            id: u.id,
            name: u.first_name,
            last_name_coalesced: coalesce(u.last_name, "")
          },
          order_by: [asc: coalesce(u.last_name, ""), asc: u.id]
        )

      # Don't use null_coalesce config when handling it manually
      # Also don't specify sorts since we're handling ORDER BY manually in the query
      assert {:ok, %{edges: edges}} =
               KC.from_query(
                 query,
                 &Repo.all/1,
                 %{first: 4},
                 %{unique_column: :id}
               )

      # Should return 4 results
      assert length(edges) == 4

      # Verify we get maps back with the manually coalesced field
      first_node = hd(edges).node
      assert is_map(first_node)
      assert Map.has_key?(first_node, :id)
      assert Map.has_key?(first_node, :name)
      assert Map.has_key?(first_node, :last_name_coalesced)
      refute is_struct(first_node)

      # Verify sorting: NULLs (coalesced to "") should come first
      last_names = Enum.map(edges, & &1.node.last_name_coalesced)
      assert ["", "", "Brown", "Davis"] = last_names

      # Verify the actual names are in order by id for the "" entries
      first_two_names = edges |> Enum.take(2) |> Enum.map(& &1.node.name)
      assert ["Alice", "Charlie"] = first_two_names
    end
  end
end
