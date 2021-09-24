import Config

config :absinthe_relay_keyset_connection, AbsintheRelayKeysetConnection.Repo,
  username: System.get_env("POSTGRESQL_USER") || "postgres",
  password: System.get_env("POSTGRESQL_PASSWORD") || "",
  database: System.get_env("POSTGRESQL_DATABASE") || "absinthe_relay_keyset_connection_test",
  hostname: System.get_env("POSTGRESQL_SERVICE_HOST") || "localhost",
  port: System.get_env("POSTGRESQL_SERVICE_PORT") || "5432",
  pool: Ecto.Adapters.SQL.Sandbox
