# Schema Automated Mapping Engine

This repository contains the source code for the Schema Automated Mapping Engine (SAME).
SAME is a tool for automatically generating mappings between schemas and schema registries.

## Usage

Add a schema registry:

```
$ same add
Enter name of schema registry: local
Enter schema registry url: http://localhost:8081
Enter username: 
Enter password: 
Added registry local
```

Generate a mapping

``` 
$ same map --from prod --to qa -o mapping
Generating mapping from prod to qa...
Generated mapping
```