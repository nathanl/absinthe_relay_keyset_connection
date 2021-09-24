# AbsintheRelayKeysetConnection

Support for paginated result sets using keyset pagination, for use in an
Absinthe resolver module.
Requires defining a connection with
[Absinthe.Relay.Connection](https://hexdocs.pm/absinthe_relay/Absinthe.Relay.Connection.html).

## Installation

Not currently on [HexDocs](https://hexdocs.pm), as I'm not sure whether this
will remain an independent library or be folded into an existing one.

The current package can be installed by adding
`absinthe_relay_keyset_connection` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:absinthe_relay_keyset_connection, git: "git@github.com:nathanl/absinthe_relay_keyset_connection.git"}
  ]
end
```
