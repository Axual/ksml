## 1. Data Model

- [x] 1.1 Create `RegistryEntry` record with `topic`, `keyType` (default "string"), `valueType` (default "string") fields, with `@JsonSchema` annotations matching `ProduceBlock` style
- [x] 1.2 Add `List<RegistryEntry> registry` field to `TestDefinition` record

## 2. Type Map Construction

- [x] 2.1 In `TestDefinitionParser` or `KSMLTestRunner`, build the merged type map: parse `registry` entries first, then merge `produce` block types (overwriting on overlap)
- [x] 2.2 Pass the merged type map through the execution flow to `TestExecutionContext`, `TestDataProducer`, and `AssertionRunner`

## 3. Mock Registry Population

- [x] 3.1 In `TestExecutionContext`, for each entry in the merged type map with a schema-backed type (e.g., `avro:SensorData`): load the schema from `schemaDirectory`, register in `MockConfluentSchemaRegistryClient` under `{topic}-key` / `{topic}-value` subjects
- [x] 3.2 Ensure registry population happens before topology construction (before `TopologyDefinitionParser` triggers `resolveUserType()`)

## 4. Produce Block Type Fallback

- [x] 4.1 In `TestDataProducer.resolveSerde()`, when a produce block has no explicit `keyType`/`valueType`, look up the topic in the merged type map before defaulting to `string`

## 5. Assertion Deserializer

- [x] 5.1 In `AssertionRunner.collectOutputRecords()`, look up the assert block's topic in the merged type map
- [x] 5.2 If a non-string type is found, create a proper deserializer via `StreamDataType.serde().deserializer()` instead of hardcoded `StringDeserializer`
- [x] 5.3 Convert deserialized records (which may be `DataObject`s rather than strings) to Python-accessible maps

## 6. JSON Schema

- [x] 6.1 Ensure `RegistryEntry` and updated `TestDefinition` are reflected in the generated JSON schema for test definitions

## 7. Documentation

- [x] 7.1 Update `docs/getting-started/testing-your-pipeline.md`: this is the primary user-facing documentation. Add a section explaining the `registry` block — when to use it (pipelines with registry-inferred schemas like `valueType: confluent_avro`), how type merging with produce blocks works, and a complete walkthrough example. Include a collapsible block with a full test definition showing a pipeline with registry-inferred types.
- [x] 7.2 Update `ksml-test-runner/README.md` to reflect the new `registry` field in the test definition format reference

## 8. Validation

- [x] 7.1 Create a sample test definition that uses `registry` block for a pipeline with `valueType: confluent_avro`
- [x] 7.2 Create a test that produces to a topic declared only in `registry` (no explicit type on produce block)
- [x] 7.3 Create a test that asserts on an output topic with Avro deserialization via `registry` entry
- [x] 7.4 Verify existing tests without `registry` block still pass unchanged
