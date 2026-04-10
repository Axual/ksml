# Tasks: Parser Cleanup and YAML Schema Generation

## Phase 1: Parser Cleanup (~90 minutes)

### Create FieldExtractor Utility (~30 minutes)
- [x] Create `FieldExtractor` class in `io.axual.ksml.testrunner` package
- [x] Implement `requireString(String field)` with rich error messages
- [x] Implement `optionalString(String field)`
- [x] Implement `optionalString(String field, String defaultValue)`
- [x] Implement `requireArray(String field)` with validation
- [x] Implement `optionalLong(String field)`
- [x] Implement `optionalMap(String field)` using existing `nodeToMap` logic
- [x] Add comprehensive unit tests for FieldExtractor

### Refactor parseProduceBlocks (~20 minutes)
- [x] Replace manual JsonNode navigation with FieldExtractor calls
- [x] Fix keyType default: `blockFields.optionalString("keyType", "string")`
- [x] Fix valueType default: `blockFields.optionalString("valueType", "string")`
- [x] Simplify message parsing loop using FieldExtractor
- [x] Remove now-unused helper methods (`requireString`, `optionalString`, etc.)

### Refactor parseAssertBlocks (~15 minutes)
- [x] Replace manual JsonNode navigation with FieldExtractor calls
- [x] Simplify stores array parsing
- [x] Use FieldExtractor for code field extraction

### Fix Default Values Implementation (~10 minutes)
- [x] Update README.md to clarify that keyType/valueType are now truly optional
- [x] Verify behavior matches documentation

### Add Tests for Defaults (~15 minutes)
- [x] Create test YAML file with missing keyType/valueType
- [x] Add test case in TestDefinitionParserTest verifying defaults work
- [x] Add test case verifying both explicit values and defaults work together

## Phase 2: Schema Annotations (~60 minutes)

### Add Schema Annotation Interface (~15 minutes)
- [x] Create `@JsonSchema` annotation with fields:
  - `String description() default ""`
  - `String defaultValue() default ""`
  - `String[] examples() default {}`
- [x] Add any required dependencies (or implement as simple annotation)

### Annotate Record Classes (~30 minutes)
- [x] Add `@JsonSchema` annotations to `TestDefinition`
  - Document name, pipeline, schemaDirectory, produce, assert fields
- [x] Add `@JsonSchema` annotations to `ProduceBlock`
  - Include examples for keyType/valueType (string, avro:*, json, binary)
  - Specify default values for keyType/valueType
- [x] Add `@JsonSchema` annotations to `AssertBlock`
  - Document topic, stores, code fields
- [x] Add `@JsonSchema` annotations to `TestMessage`
  - Document key, value, timestamp fields

### Add Validation Methods (~15 minutes)
- [x] Add `validate()` method to `TestDefinition`
- [x] Add `validate()` method to `ProduceBlock`
  - Check "either messages or generator" requirement
- [x] Add `validate()` method to `AssertBlock`
  - Check "either topic or stores" requirement
- [x] Update parser to call `validate()` methods

## Phase 3: Schema Generation (~2 hours)

### Create Schema Generator Utility (~90 minutes)
- [x] Create `TestDefinitionSchemaGenerator` class
- [x] Implement reflection-based annotation reading
- [x] Build JSON Schema structure:
  - Root schema object with $schema and type
  - Properties for each annotated field
  - Required vs optional field marking
  - Description, examples, and default values from annotations
  - Nested object schemas for ProduceBlock, AssertBlock, TestMessage
- [x] Add proper JSON Schema validation rules for each field type
- [x] Generate well-formatted JSON output

### Integrate with Build Process (~15 minutes)
- [x] Add Maven exec plugin configuration to run schema generator
- [x] Configure output to `src/main/resources/schemas/test-definition.schema.json`
- [x] Add generated schema to git (or document regeneration process)

### Test Schema Validity (~15 minutes)
- [x] Validate generated schema against JSON Schema specification
- [x] Test schema works with existing test YAML files
- [x] Verify schema catches validation errors correctly

## Phase 4: Editor Setup Documentation (~30 minutes)

### VS Code Setup (~10 minutes)
- [ ] Document settings.json configuration:
  ```json
  {
    "yaml.schemas": {
      "./schemas/test-definition.schema.json": ["*-test.yaml", "test-*.yaml"]
    }
  }
  ```
- [ ] Test auto-completion and validation in VS Code

### IntelliJ Setup (~10 minutes)
- [ ] Document schema mapping configuration
- [ ] Test auto-completion and validation in IntelliJ

### General Documentation (~10 minutes)
- [x] Update README.md with editor setup instructions
- [x] Add schema file header example:
  ```yaml
  # yaml-language-server: $schema=./schemas/test-definition.schema.json
  ```
- [x] Document schema regeneration process for developers

## Validation and Testing

### Regression Testing
- [x] Run all existing TestDefinitionParserTest tests - should pass unchanged
- [x] Run sample test files - should work identically
- [x] Verify error messages remain clear and contextual
- [x] Test that malformed YAML still produces good error messages

### New Feature Testing
- [x] Test default values work as documented
- [ ] Test schema validation in VS Code
- [ ] Test schema validation in IntelliJ
- [ ] Test schema auto-completion provides expected suggestions
- [x] Verify generated schema is valid JSON Schema

### Performance Testing
- [x] Verify parsing performance unchanged (should be identical or better)
- [x] Test schema generation time (should be fast enough for build process)

## Future Enhancements (Not in Scope)

- [ ] Schema-driven validation error messages (could replace manual validation)
- [ ] Auto-completion for topic names (requires integration with pipeline definitions)
- [ ] Dynamic schema generation based on available Avro schemas
- [ ] Integration with KSML pipeline validation
- [ ] Schema versioning and migration support
