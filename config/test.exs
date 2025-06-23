import Config

config :absinthe_relay_keyset_connection,
  ecto_repos: [AbsintheRelayKeysetConnection.Repo]

config :logger,
  level: :warning
