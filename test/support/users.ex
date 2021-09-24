defmodule AbsintheRelayKeysetConnection.Users do
  @moduledoc """
  Query functions for tests.
  """
  import Ecto.Query
  alias AbsintheRelayKeysetConnection.{Repo, User}

  def all do
    from(u in User, order_by: :id)
    |> Repo.all()
  end
end
