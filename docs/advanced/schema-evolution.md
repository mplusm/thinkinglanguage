# Schema Evolution and Migration

TL provides built-in support for schema evolution, enabling data engineers to manage schema changes over time with version tracking, compatibility checking, and automated migration.

## Schema Annotations

Schemas are defined with version annotations that track when fields were added or deprecated.

### @version

Declares the current version of a schema.

```
@version(2)
schema User {
    id: int64
    name: string
    email: string
    @since(2)
    phone: string
}
```

### @since

Marks a field as added in a specific schema version.

```
@since(2)
phone: string
```

Fields marked with `@since` are not expected to be present in data conforming to earlier schema versions.

### @deprecated

Marks a field as deprecated starting from a specific version.

```
@deprecated(3)
legacy_id: int64
```

Deprecated fields are still recognized but may generate warnings during validation.

## Full Schema Example

```
@version(3)
schema User {
    id: int64
    name: string
    email: string
    @since(2)
    phone: string
    @since(3)
    address: string
    @deprecated(3)
    legacy_id: int64
}
```

This schema has gone through three versions:
- Version 1: `id`, `name`, `email`, `legacy_id`
- Version 2: Added `phone`
- Version 3: Added `address`, deprecated `legacy_id`

## Migrate Statement

The `migrate` statement defines how to transform data from one schema version to another.

```
migrate User from 1 to 2 {
    phone = "unknown"
}

migrate User from 2 to 3 {
    address = "not provided"
}
```

Each migration block specifies default values or transformations for fields that were added in the target version.

## Schema Registry

The schema registry provides programmatic access to schema versions and validation.

### Registering a Schema

```
schema_register("User", user_schema)
```

### Retrieving Schemas

```
// Get a specific version
let v1 = schema_get("User", 1)

// Get the latest version
let latest = schema_latest("User")
```

### Listing Versions

```
// Get all versions as a list of version numbers
let versions = schema_versions("User")

// Get full history with metadata
let history = schema_history("User")
```

### Inspecting Fields

```
let fields = schema_fields("User")
```

### Validation

Check data against a schema:

```
let is_valid = schema_check("User", data)
```

### Comparing Versions

Show differences between two schema versions:

```
let diff = schema_diff("User", 1, 2)
```

## CLI Commands

### tl migrate apply

Apply schema migrations defined in a source file.

```
tl migrate apply migrations.tl
tl migrate apply migrations.tl --backend vm
```

This executes the migration statements, transforming data to match the target schema versions.

### tl migrate check

Check compatibility without applying changes. Validates that migrations are well-formed and that schema transitions are compatible.

```
tl migrate check migrations.tl
```

### tl migrate diff

Show the differences between schema versions defined in a file.

```
tl migrate diff migrations.tl
```

## Compatibility Checking

The schema evolution system performs forward and backward compatibility analysis:

- **Forward compatibility**: Can consumers using the new schema read data written with the old schema?
- **Backward compatibility**: Can consumers using the old schema read data written with the new schema?

The `tl migrate check` command reports compatibility issues, such as:

- Removed required fields (breaks backward compatibility)
- Added required fields without defaults (breaks forward compatibility)
- Type changes on existing fields

## Implementation Details

### VM Integration

The VM intercepts globals with special prefixes for schema registration:

- `__schema__:` prefix: Registers a schema definition in the SchemaRegistry.
- `__migrate__:` prefix: Registers a migration transformation.

When the VM encounters a `SetGlobal` instruction with these prefixes, it routes the value to the schema registry rather than storing it as a regular global variable.

### Schema Registry Storage

The SchemaRegistry maintains an in-memory store of all registered schemas, indexed by name and version. Each entry includes the schema definition, version metadata, and field annotations.
