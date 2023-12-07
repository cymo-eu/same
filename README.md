# 🛰️ Schema Automated Mapping Engine

This repository contains the source code for the Schema Automated Mapping Engine (SAME).
SAME is a tool for automatically generating mappings between schema registries.

## 👩‍💻 Usage

To compare between schema registries,
you must configure them first.
After that,
you can generate mappings between them.
Schema registries are stored inside contexts.

***Configuring schema registries***

You can configure a schema registry using the `same add` command.

```
$ same add
Enter the url for the schema registry: https://somewhere.europe-west3.gcp.confluent.cloud
Select the authentication method: Basic Auth
Enter the username: DEADBEEFCAFEBABE
Enter the password: [hidden]
Enter a name for the context: prod
```

***Generating a mapping***

Generate a mapping between two schema registries:

``` 
$ same map --from [SOURCE_CTX] --to [TARGET_CTX] -o mapping
```

Options:

- `--from`: The name of the context to map from (required).
- `--to`: The name of the context to map to (required).
- `-o`, `--output`: The output file to write the mapping to (optional).
- `-U`, `--force-update`: Force update the schemas in the cache (optional, default false).

## 🔎 Where are my configurations and mappings stored?

Credentials are stored in the platform's specific secure storage.
We use [keyring](https://lib.rs/crates/keyring) for this purpose.

We use [dirs](https://lib.rs/crates/dirs) for determining the location of configuration and cache files.

Configuration is stored in the following locations:

- Linux: `$XDG_CONFIG_HOME/io.kannika.same/config`
- macOs: `$HOME/Library/Application Support/io.kannika.same/config`
- Windows: `{FOLDERID_RoamingAppData}\io.kannika.same\config`

Schemas are cached locally to avoid unnecessary network requests in the following locations:

- Linux: `$XDG_CACHE_HOME/io.kannika.same` or `$HOME/.cache/io.kannika.same`
- macOs: `$HOME/Library/Application Support/io.kannika.same`
- Windows: `{FOLDERID_RoamingAppData}\io.kannika.same`

## 💾 Supported Protocols

Following protocols are supported:

- Avro

These are ignored for now:

- JSON Schema
- Protocol Buffers