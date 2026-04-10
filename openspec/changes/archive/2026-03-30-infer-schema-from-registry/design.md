# Design: Infer Schema from Registry

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  YAML Parsing                                                    │
│                                                                  │
│  valueType: "confluent_avro"  (no schema name)                   │
│       │                                                          │
│       ▼                                                          │
│  UserTypeParser.parseNotationWithOrWithoutSchema()               │
│       │                                                          │
│       ├── schema name present?                                   │
│       │     YES → SchemaLibrary.getSchema() from disk (unchanged)│
│       │     NO  → notation supports remote fetch?                │
│       │              YES → return UserType(notation, UNRESOLVED)  │
│       │              NO  → return UserType(notation, defaultType) │
│       │                    (unchanged)                            │
│       ▼                                                          │
│  StreamDefinitionParser lambda                                   │
│       │                                                          │
│       ├── topic = "my-events"                                    │
│       ├── valueType has UNRESOLVED type?                         │
│       │     YES → notation.fetchRemoteSchema("my-events-value")  │
│       │           → parse to DataSchema → DataType               │
│       │           → new UserType(notation, resolvedType)          │
│       │     NO  → use as-is (unchanged)                          │
│       ▼                                                          │
│  StreamDefinition(topic, keyType, valueType)                     │
│       │  fully resolved from here on                             │
│       ▼                                                          │
│  TopologyBuildContext, PythonFunction, etc. (unchanged)          │
└─────────────────────────────────────────────────────────────────┘
```

## Key decisions

### 1. Notation interface extension

A default method on `Notation`:

```java
default DataSchema fetchRemoteSchema(String subject) {
    return null;
}
```

Default `null` return means notations without registry support (CSV, binary, plain Avro) need no changes. Only registry-backed notations override this.

This abstraction supports both Confluent Schema Registry and Apicurio (which exposes a Confluent-compatible API), and any future registry implementations.

### 2. UnresolvedType marker

A new `DataType` subclass (or sentinel constant) that signals "this type needs to be resolved with a topic name." It should not be usable as an actual type — if it leaks past the `StreamDefinitionParser` assembly point, it should cause a clear error rather than silent misbehavior.

### 3. Resolution happens in StreamDefinitionParser

The `StreamDefinitionParser` lambda is the earliest point where both the topic name and the type are available. Resolution happens here, before the `StreamDefinition` is constructed. From that point on, the `UserType` is fully resolved and the rest of the system (topology construction, serde creation, Python function type checking) works unchanged.

The same applies to `TopicDefinitionParser`, which is used for pipeline sources and producer targets.

### 4. Subject naming: TopicNameStrategy

Schema registry subjects are derived from the topic name:
- Value schema: `{topic}-value`
- Key schema: `{topic}-key`

This is the Confluent default (`TopicNameStrategy`). Other strategies (`RecordNameStrategy`, `TopicRecordNameStrategy`) are out of scope for now.

### 5. Always fetch latest version

The registry is queried for the latest version of the subject. This matches the behavior of Confluent's own deserializer.

### 6. Caching

Fetched schemas are stored in `SchemaLibrary` using the subject name as cache key. This prevents repeated registry calls if the same topic appears in multiple places.

## Files to modify

| File | Change |
|------|--------|
| `Notation` interface | Add `fetchRemoteSchema(String subject)` default method |
| `UserTypeParser` | Return unresolved marker when notation supports remote fetch and no schema name given |
| `StreamDefinitionParser` | Resolve unresolved types using topic name + notation |
| `TopicDefinitionParser` | Same resolution logic as StreamDefinitionParser |
| `SchemaLibrary` | Support caching schemas by subject name |
| `AvroNotation` / `VendorNotation` | Implement `fetchRemoteSchema` using registry client |
| `ConfluentAvroSerdeSupplier` or `ConfluentAvroNotationProvider` | Ensure `SchemaRegistryClient` is available for schema fetching |
| New: `UnresolvedType` | Marker DataType for deferred resolution |
