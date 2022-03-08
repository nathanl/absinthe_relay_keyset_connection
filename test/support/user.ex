defmodule AbsintheRelayKeysetConnection.User do
  @moduledoc """
  Represents a user.
  """

  use Ecto.Schema

  schema "users" do
    field(:first_name, :string)
    field(:last_name, :string)
    field(:inserted_at, :naive_datetime_usec)
  end
end
