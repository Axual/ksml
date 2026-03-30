# Tasks: Infer Schema from Registry

## Tasks

- [ ] 1. Create `UnresolvedType` marker class
  - New `DataType` subclass or sentinel that signals deferred resolution
  - Should fail clearly if used as an actual type (e.g., in serde creation or type checking)

- [ ] 2. Add `fetchRemoteSchema(String subject)` to `Notation` interface
  - Default method returning `null`
  - No changes needed in existing notation implementations

- [ ] 3. Implement `fetchRemoteSchema` in Confluent Avro notation
  - Use `SchemaRegistryClient` (or create one from `schema.registry.url` config) to fetch latest schema for a subject
  - Parse the Avro schema string into a `DataSchema` using the existing `AvroSchemaParser`
  - Ensure the `SchemaRegistryClient` is available in the notation (may need wiring changes in `ConfluentAvroNotationProvider`)

- [ ] 4. Modify `UserTypeParser.parseNotationWithOrWithoutSchema()`
  - When `type.dataType` is empty and notation supports remote fetch (`fetchRemoteSchema` is overridden / notation has registry config): return `UserType` with `UnresolvedType`
  - When notation does not support remote fetch: keep existing behavior (return default type)

- [ ] 5. Add resolution logic in `StreamDefinitionParser` and `TopicDefinitionParser`
  - In the assembly lambda, check if `keyType` or `valueType` contain `UnresolvedType`
  - If so, derive subject name from topic (`{topic}-key` / `{topic}-value`)
  - Call `notation.fetchRemoteSchema(subject)` and resolve the `UserType`
  - Fail fast with clear error if fetch returns null or fails

- [ ] 6. Add caching in `SchemaLibrary` for registry-fetched schemas
  - Cache by subject name so repeated lookups don't hit the registry

- [ ] 7. Add tests
  - Unit test: `UserTypeParser` returns `UnresolvedType` for notation-without-schema
  - Unit test: `StreamDefinitionParser` resolves `UnresolvedType` using topic name
  - Unit test: existing `avro:SensorData` syntax still works (regression)
  - Integration test: end-to-end with schema registry (extend `ConfluentAvroSchemaRegistryIT`)

- [ ] 8. Update documentation
  - Document the new `valueType: confluent_avro` (no schema name) syntax
  - Document the requirement for schema registry configuration when using this feature
