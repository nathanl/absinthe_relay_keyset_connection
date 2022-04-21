# AbsintheRelayKeysetConnection

Support for paginated result sets using keyset pagination, for use in an
Absinthe resolver module.
Requires defining a connection with
[Absinthe.Relay.Connection](https://hexdocs.pm/absinthe_relay/Absinthe.Relay.Connection.html).

## Installation

Add to to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:absinthe_relay_keyset_connection, "~> 1.0"}
  ]
end
```

... and `mix deps.get`.
