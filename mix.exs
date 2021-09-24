defmodule AbsintheRelayKeysetConnection.MixProject do
  use Mix.Project

  def project do
    [
      app: :absinthe_relay_keyset_connection,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    if Mix.env() == :test do
      [
        mod: {AbsintheRelayKeysetConnection.Application, []},
        extra_applications: [:logger]
      ]
    else
      []
    end
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.2"},
      {:ecto_sql, "~> 3.7"},
      {:postgrex, "~> 0.15", only: :test},
      {:ex_doc, "~> 0.25.2", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [
      test: [
        "ecto.create --quiet",
        "ecto.migrate --migrations-path test/support/priv/repo/migrations",
        "test"
      ]
    ]
  end
end
