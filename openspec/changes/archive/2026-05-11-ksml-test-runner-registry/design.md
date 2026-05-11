## Context

The "infer schema from registry" feature allows pipeline YAML to use `valueType: confluent_avro` (or `apicurio_avro`) without a schema name. At runtime, `resolveUserType()` in `StreamDefinitionParser` calls `notation.fetchRemoteSchema()` to get the schema from the registry. In the test runner, `MockConfluentSchemaRegistryClient` is empty, so this call fails.

The test runner already has all the pieces: schema files on disk, a mock registry client, and topic names. The missing link is populating the mock registry with the right schemas before the topology is built.

## Goals / Non-Goals

**Goals:**
- Test pipelines that use registry-inferred schemas without a real schema registry
- Keep the test definition format consistent (reuse `keyType`/`valueType` syntax everywhere)
- Avoid type duplication between `registry` and `produce` blocks
- Properly deserialize output topic records in assertions (replace hardcoded `StringDeserializer`)

**Non-Goals:**
- Supporting subject naming strategies other than TopicNameStrategy
- Fetching from a real schema registry during tests
- Changes to any module outside `ksml-test-runner`

## Decisions

### Merged type map as single source of truth

Rather than maintaining separate lookups for registry entries and produce block types, both are merged into a single `Map<String, TopicTypes>` early in the test execution flow. Produce block entries overwrite registry entries for the same topic.

```
┌──────────────────┐     ┌──────────────────┐
│  registry block  │     │  produce blocks   │
│                  │     │                   │
│  topic A: avro:X │     │  topic A: avro:X  │  ← merged (produce wins)
│  topic B: avro:Y │     │  topic C: string  │
└────────┬─────────┘     └────────┬──────────┘
         │                        │
         └──────────┬─────────────┘
                    ▼
         ┌──────────────────┐
         │  merged type map │
         │                  │
         │  topic A: avro:X │  (from produce)
         │  topic B: avro:Y │  (from registry)
         │  topic C: string │  (from produce)
         └────────┬─────────┘
                  │
         ┌───────┴────────────────────────────┐
         │                                     │
         ▼                                     ▼
  ┌──────────────┐                   ┌──────────────────┐
  │ Mock registry│                   │ Serde resolution │
  │ population   │                   │ (produce+assert) │
  └──────────────┘                   └──────────────────┘
```

**Rationale:** One map, used everywhere. No ambiguity about which source takes precedence. The produce block wins because it represents what the test is actually sending — if the test author specifies a type on the produce block, that's the strongest signal of intent.

### Schema loading and mock registry population

For each entry in the merged type map where the type references a schema (e.g., `avro:SensorData`):

1. Parse the type string with `UserTypeParser` to extract notation and schema name
2. Load the schema file from `schemaDirectory` (e.g., `SensorData.avsc`)
3. Register in `MockConfluentSchemaRegistryClient` under subject `{topic}-value` or `{topic}-key`

This happens **before** topology construction, so when `resolveUserType()` encounters an `UnresolvedType`, the mock registry already has the schema.

For types that don't use a schema (e.g., `string`, `json`), no registry entry is needed.

### AssertionRunner deserializer resolution

Today `AssertionRunner.collectOutputRecords()` creates output topics with `StringDeserializer`. With the merged type map available:

1. Look up the assert block's topic in the merged type map
2. If found with a non-string type: create a proper deserializer using `StreamDataType.serde().deserializer()`
3. If not found or string type: use `StringDeserializer` (existing behavior)

The deserialized records are then converted to Python-accessible maps as before.

### RegistryEntry data class

A new record class mirroring `ProduceBlock`'s type fields:

```java
public record RegistryEntry(
    String topic,      // required
    String keyType,    // optional, defaults to "string"
    String valueType   // optional, defaults to "string"
)
```

This keeps the YAML syntax consistent — `registry` entries look like `produce` blocks without messages.

## Files to modify

| File | Change |
|------|--------|
| New: `RegistryEntry.java` | Record with topic, keyType, valueType |
| `TestDefinition.java` | Add `List<RegistryEntry> registry` field |
| `TestDefinitionParser.java` | Parse `registry` block, build merged type map |
| `TestExecutionContext.java` | Accept type map, load schemas, register in mock registry |
| `AssertionRunner.java` | Use type map to create proper deserializer for output topics |
| `TestDataProducer.java` | Fall back to type map when produce block omits types |
| `KSMLTestRunner.java` | Wire up the merged type map through the execution flow |

## Risks / Trade-offs

- **[Risk] Schema file not found**: If a registry entry references `avro:Foo` but `Foo.avsc` doesn't exist in `schemaDirectory`, fail fast with a clear error at test setup time, not during topology construction.
- **[Trade-off] Only TopicNameStrategy**: Subject names are always `{topic}-key` and `{topic}-value`. This matches the existing `fetchRemoteSchema()` implementation and covers the common case.
- **[Trade-off] Produce block overwrites silently**: No error when produce and registry disagree — produce wins. This keeps the logic simple. If this causes confusion in practice, a warning log could be added later.
