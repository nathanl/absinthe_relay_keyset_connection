defmodule AbsintheRelayKeysetConnection.User do
  @moduledoc """
  Represents a user.
  """

  use Ecto.Schema

  schema "users" do
    field(:first_name, :string)
    field(:last_name, :string)
    field(:date_of_birth, :date)
  end
end
