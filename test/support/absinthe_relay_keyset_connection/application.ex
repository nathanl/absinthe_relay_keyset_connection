defmodule AbsintheRelayKeysetConnection.Application do
  @moduledoc "The application entrypoint - used only in tests"

  use Application

  def start(_type, _args) do
    children = [
      {AbsintheRelayKeysetConnection.Repo, []}
    ]

    opts = [strategy: :one_for_one, name: AbsintheRelayKeysetConnection.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
