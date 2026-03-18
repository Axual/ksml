# Tasks: Parser Cleanup and YAML Schema Generation

## Phase 1: Parser Cleanup (~90 minutes)

### Create FieldExtractor Utility (~30 minutes)
- [ ] Create `FieldExtractor` class in `io.axual.ksml.testrunner` package
- [ ] Implement `requireString(String field)` with rich error messages
- [ ] Implement `optionalString(String field)` 
- [ ] Implement `optionalString(String field, String defaultValue)`
- [ ] Implement `requireArray(String field)` with validation
- [ ] Implement `optionalLong(String field)`
- [ ] Implement `optionalMap(String field)` using existing `nodeToMap` logic
- [ ] Add comprehensive unit tests for FieldExtractor

### Refactor parseProduceBlocks (~20 minutes)
- [ ] Replace manual JsonNode navigation with FieldExtractor calls
- [ ] Fix keyType default: `blockFields.optionalString("keyType", "string")`
- [ ] Fix valueType default: `blockFields.optionalString("valueType", "string")`
- [ ] Simplify message parsing loop using FieldExtractor
- [ ] Remove now-unused helper methods (`requireString`, `optionalString`, etc.)

### Refactor parseAssertBlocks (~15 minutes)
- [ ] Replace manual JsonNode navigation with FieldExtractor calls
- [ ] Simplify stores array parsing
- [ ] Use FieldExtractor for code field extraction

### Fix Default Values Implementation (~10 minutes)
- [ ] Update README.md to clarify that keyType/valueType are now truly optional
- [ ] Verify behavior matches documentation

### Add Tests for Defaults (~15 minutes)
- [ ] Create test YAML file with missing keyType/valueType
- [ ] Add test case in TestDefinitionParserTest verifying defaults work
- [ ] Add test case verifying both explicit values and defaults work together

## Phase 2: Schema Annotations (~60 minutes)

### Add Schema Annotation Interface (~15 minutes)
- [ ] Create `@JsonSchema` annotation with fields:
  - `String description() default ""`
  - `String defaultValue() default ""`  
  - `String[] examples() default {}`
- [ ] Add any required dependencies (or implement as simple annotation)

### Annotate Record Classes (~30 minutes)
- [ ] Add `@JsonSchema` annotations to `TestDefinition`
  - Document name, pipeline, schemaDirectory, produce, assert fields
- [ ] Add `@JsonSchema` annotations to `ProduceBlock`
  - Include examples for keyType/valueType (string, avro:*, json, binary)
  - Specify default values for keyType/valueType
- [ ] Add `@JsonSchema` annotations to `AssertBlock`
  - Document topic, stores, code fields
- [ ] Add `@JsonSchema` annotations to `TestMessage`
  - Document key, value, timestamp fields

### Add Validation Methods (~15 minutes)
- [ ] Add `validate()` method to `TestDefinition`
- [ ] Add `validate()` method to `ProduceBlock` 
  - Check "either messages or generator" requirement
- [ ] Add `validate()` method to `AssertBlock`
  - Check "either topic or stores" requirement
- [ ] Update parser to call `validate()` methods

## Phase 3: Schema Generation (~2 hours)

### Create Schema Generator Utility (~90 minutes)
- [ ] Create `TestDefinitionSchemaGenerator` class
- [ ] Implement reflection-based annotation reading
- [ ] Build JSON Schema structure:
  - Root schema object with $schema and type
  - Properties for each annotated field
  - Required vs optional field marking
  - Description, examples, and default values from annotations
  - Nested object schemas for ProduceBlock, AssertBlock, TestMessage
- [ ] Add proper JSON Schema validation rules for each field type
- [ ] Generate well-formatted JSON output

### Integrate with Build Process (~15 minutes)
- [ ] Add Maven exec plugin configuration to run schema generator
- [ ] Configure output to `src/main/resources/schemas/test-definition.schema.json`
- [ ] Add generated schema to git (or document regeneration process)

### Test Schema Validity (~15 minutes)
- [ ] Validate generated schema against JSON Schema specification
- [ ] Test schema works with existing test YAML files
- [ ] Verify schema catches validation errors correctly

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
- [ ] Update README.md with editor setup instructions
- [ ] Add schema file header example:
  ```yaml
  # yaml-language-server: $schema=./schemas/test-definition.schema.json
  ```
- [ ] Document schema regeneration process for developers

## Validation and Testing

### Regression Testing
- [ ] Run all existing TestDefinitionParserTest tests - should pass unchanged
- [ ] Run sample test files - should work identically  
- [ ] Verify error messages remain clear and contextual
- [ ] Test that malformed YAML still produces good error messages

### New Feature Testing
- [ ] Test default values work as documented
- [ ] Test schema validation in VS Code
- [ ] Test schema validation in IntelliJ
- [ ] Test schema auto-completion provides expected suggestions
- [ ] Verify generated schema is valid JSON Schema

### Performance Testing
- [ ] Verify parsing performance unchanged (should be identical or better)
- [ ] Test schema generation time (should be fast enough for build process)

## Future Enhancements (Not in Scope)

- [ ] Schema-driven validation error messages (could replace manual validation)
- [ ] Auto-completion for topic names (requires integration with pipeline definitions)
- [ ] Dynamic schema generation based on available Avro schemas
- [ ] Integration with KSML pipeline validation
- [ ] Schema versioning and migration support