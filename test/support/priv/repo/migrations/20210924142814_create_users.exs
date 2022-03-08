defmodule AbsintheRelayKeysetConnection.Repo.Migrations.CreateUsers do
  use Ecto.Migration

  def change do
    create table(:users) do
      add(:first_name, :string)
      add(:last_name, :string)
      add(:inserted_at, :naive_datetime_usec)
    end
  end
end
