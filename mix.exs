defmodule AbsintheRelayKeysetConnection.MixProject do
  use Mix.Project

  def project do
    [
      app: :absinthe_relay_keyset_connection,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      elixirc_options: [
        warnings_as_errors: true
      ],
      aliases: aliases(),
      deps: deps(),
      dialyzer: dialyzer(),
      description: description(),
      package: package(),
      source_url: "https://github.com/nathanl/absinthe_relay_keyset_connection"
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
      {:jason, "~> 1.2", optional: true},
      {:ecto_sql, "~> 3.7"},
      {:ex_doc, "~> 0.25.2", only: :dev, runtime: false},
      {:postgrex, "~> 0.15", only: :test},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false}
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

  defp dialyzer do
    [
      ignore_warnings: ".dialyzer_ignore.exs",
      list_unused_filters: true,
      plt_add_apps: [:ex_unit, :mix]
    ]
  end

  defp description do
    """
    Support for paginated result sets using keyset pagination, for use in an
    Absinthe resolver module.
    Requires defining a connection with
    [Absinthe.Relay.Connection](https://hexdocs.pm/absinthe_relay/Absinthe.Relay.Connection.html).
    """
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/nathanl/absinthe_relay_keyset_connection"}
    ]
  end
end
