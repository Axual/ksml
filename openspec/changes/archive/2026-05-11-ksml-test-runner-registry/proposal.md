## Why

The "infer schema from registry" feature (archived change `2026-03-30-infer-schema-from-registry`) allows KSML pipelines to declare stream types without an explicit schema name, e.g. `valueType: confluent_avro`. At runtime, KSML fetches the schema from the configured schema registry using the topic name. This works in production but breaks in the test runner, which uses a `MockConfluentSchemaRegistryClient` that is empty — the topology fails to build because `resolveUserType()` can't fetch the schema.

Additionally, the `AssertionRunner` hardcodes `StringDeserializer` for output topics, so even if the topology built successfully, Avro output records can't be deserialized for assertions.

The goal is to support testing pipelines that use registry-inferred schemas **without requiring a real schema registry**.

## What Changes

Add an optional `registry` block to the test definition YAML. Each entry maps a topic to its key/value types. Before building the topology, the test runner loads the referenced schemas from the `schemaDirectory` and registers them in the `MockConfluentSchemaRegistryClient` under the conventional subject names (`{topic}-key`, `{topic}-value`).

Type entries from `produce` blocks are merged into the same registry map (produce block wins on overlap). This avoids duplicating type declarations when a topic appears in both places.

The `AssertionRunner` uses the registry map to create the correct deserializer for output topics instead of hardcoding `StringDeserializer`.

### Test definition format

```yaml
test:
  name: "Filter pipeline passes blue sensors"
  pipeline: pipelines/test-filter.yaml
  schemaDirectory: schemas
  registry:
    - topic: ksml_sensordata_avro
      keyType: string
      valueType: "avro:SensorData"
    - topic: ksml_sensordata_filtered
      keyType: string
      valueType: "avro:SensorData"

  produce:
    - topic: ksml_sensordata_avro
      messages:
        - key: "sensor-1"
          value:
            name: "sensor-1"
            color: "blue"

  assert:
    - topic: ksml_sensordata_filtered
      code: |
        assert len(records) == 1
```

### Type resolution order

For any topic, the effective key/value types are determined by merging two sources into a single map:

1. Parse `registry` block entries into `Map<topic, {keyType, valueType}>`
2. For each `produce` block with explicit `keyType`/`valueType`, merge into the same map (overwrite if present)

This merged map is used for:
- Registering schemas in the mock registry (before topology construction)
- Resolving serdes for produce blocks that omit type declarations
- Resolving deserializers for assert block output topics

Topics not in the map default to `string` for both key and value (existing behavior).

## Capabilities

### Modified Capabilities
- `test-definition-format`: Add optional `registry` block with per-topic type declarations
- `test-runner-execution`: Populate mock schema registry from registry block + produce block types before building topology; use proper deserializers in assertion output topic reading

## Impact

- **TestDefinition**: Add `registry` field (list of `RegistryEntry` records)
- **New: RegistryEntry**: Record with `topic`, `keyType`, `valueType` fields
- **TestDefinitionParser**: Parse `registry` block, merge with produce block types
- **TestExecutionContext**: Accept merged type map, load schemas from disk, register in `MockConfluentSchemaRegistryClient`
- **AssertionRunner**: Look up topic type from merged map, create appropriate deserializer
- **TestDataProducer**: Fall back to merged map when produce block omits types
- **Existing modules**: No changes outside `ksml-test-runner`
