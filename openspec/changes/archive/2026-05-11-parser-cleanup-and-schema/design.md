# Design: Parser Cleanup and YAML Schema Generation

## Overview

This design implements a hybrid approach that combines clean JsonNode parsing with schema generation capabilities. The architecture preserves the current parser's strengths (excellent error messages, explicit control flow) while eliminating boilerplate and adding editor support.

## Architecture

```
HYBRID APPROACH:
================

Records with Schema Annotations     Clean Runtime Parser
┌─────────────────────────────┐    ┌──────────────────────────────┐
│ @JsonSchema annotations     │    │ FieldExtractor helper        │
│ ├─ descriptions            │    │ ├─ requireString()           │
│ ├─ examples                │◄───┤ ├─ optionalString()          │
│ ├─ default values          │    │ ├─ Rich error messages       │
│ └─ validation rules        │    │ └─ Clean parsing flow        │
└─────────────┬───────────────┘    └──────────────────────────────┘
              │                                   │
              ▼                                   │
    ┌──────────────────┐                          │
    │ Schema Generator │                          │
    │ (build-time)     │                          ▼
    └────────┬─────────┘                   ┌─────────────┐
             │                             │ TestDefinition │
             ▼                             │ ProduceBlock   │
    test-definition.schema.json            │ AssertBlock    │
             │                             │ TestMessage    │
             ▼                             └─────────────┘
    Editor autocompletion/validation
```

## Core Components

### 1. FieldExtractor Utility Class

Centralizes all JsonNode field extraction and validation logic:

```java
class FieldExtractor {
    private final JsonNode node;
    private final Path contextFile;
    
    public String requireString(String field);
    public String optionalString(String field);
    public String optionalString(String field, String defaultValue);
    public JsonNode requireArray(String field);
    public Long optionalLong(String field);
    public Map<String, Object> optionalMap(String field);
    
    // All methods provide rich error messages with file context
}
```

**Benefits:**
- Eliminates ~120 lines of repetitive validation code
- Consistent error message formatting
- Easy to add new field types
- Centralizes default value handling

### 2. Enhanced Record Classes

Add schema annotations to existing record classes:

```java
@JsonSchema(description = "A block that defines test data to be produced to a topic")
public record ProduceBlock(
    @JsonSchema(description = "Target Kafka topic name", 
                examples = {"input-topic", "sensor-data"})
    String topic,
    
    @JsonSchema(description = "Key serialization type", 
                defaultValue = "string",
                examples = {"string", "avro:MyKeySchema", "json", "binary"})
    String keyType,
    
    @JsonSchema(description = "Value serialization type",
                defaultValue = "string", 
                examples = {"string", "avro:SensorData", "json", "binary"})
    String valueType,
    
    // ... other fields
) {
    public void validate() {
        if (messages == null && generator == null) {
            throw new IllegalArgumentException(
                "Produce block for topic '" + topic + "' must have either 'messages' or 'generator'");
        }
    }
}
```

**Benefits:**
- Single source of truth for field metadata
- Validation logic co-located with data structure
- Enables automatic schema generation
- Self-documenting code

### 3. Clean Parser Implementation

Simplified parsing logic using FieldExtractor:

```java
public class TestDefinitionParser {
    public TestDefinition parse(Path testFile) throws IOException {
        var root = YAML_MAPPER.readTree(Files.readString(testFile));
        var testNode = root.get("test");
        
        if (testNode == null) {
            throw new TestDefinitionException("Missing required 'test' root element in " + testFile);
        }
        
        var fields = new FieldExtractor(testNode, testFile);
        
        var definition = new TestDefinition(
            fields.requireString("name"),
            fields.requireString("pipeline"),
            fields.optionalString("schemaDirectory"),
            parseProduceBlocks(fields.requireArray("produce"), testFile),
            parseAssertBlocks(fields.requireArray("assert"), testFile)
        );
        
        definition.validate();
        return definition;
    }
    
    private List<ProduceBlock> parseProduceBlocks(JsonNode produceArray, Path testFile) {
        return StreamSupport.stream(produceArray.spliterator(), false)
            .map(blockNode -> {
                var blockFields = new FieldExtractor(blockNode, testFile);
                var block = new ProduceBlock(
                    blockFields.requireString("topic"),
                    blockFields.optionalString("keyType", "string"),    // Default explicit
                    blockFields.optionalString("valueType", "string"),
                    parseMessages(blockFields.optionalArray("messages"), testFile),
                    blockFields.optionalMap("generator"),
                    blockFields.optionalLong("count")
                );
                block.validate();
                return block;
            })
            .toList();
    }
}
```

**Benefits:**
- Linear, easy-to-follow parsing flow
- Defaults explicit in the parsing logic
- Rich error messages preserved
- Easy to add new fields (one line per field)

### 4. Schema Generator

Build-time utility to generate JSON Schema from annotations:

```java
public class TestDefinitionSchemaGenerator {
    public static void main(String[] args) throws IOException {
        var schema = generateSchema();
        var outputPath = Path.of("src/main/resources/schemas/test-definition.schema.json");
        Files.writeString(outputPath, schema);
        System.out.println("Generated schema: " + outputPath);
    }
    
    private static String generateSchema() {
        return buildSchemaFor(TestDefinition.class);
    }
    
    private static String buildSchemaFor(Class<?> clazz) {
        // Use reflection to read @JsonSchema annotations
        // Build JSON Schema structure with:
        // - Field descriptions from annotations
        // - Examples from annotations  
        // - Default values from annotations
        // - Required vs optional field marking
        // - Type validation rules
    }
}
```

## Default Value Handling

Fix the current inconsistency where README documents keyType/valueType as optional with defaults, but code treats them as required:

**Current (broken):**
```java
var keyType = requireString(blockNode, "keyType", testFile);  // Treats as required
```

**Fixed:**
```java
blockFields.optionalString("keyType", "string")  // Implements documented default
```

## Error Message Preservation

Maintain current excellent error message quality:

**Before:**
```
Missing required field 'pipeline' in /Users/me/tests/my-test.yaml
```

**After (preserved):**
```
Missing required field 'pipeline' in /Users/me/tests/my-test.yaml
```

All error messages continue to include:
- Specific field name that's missing/invalid
- Full file path for context
- Clear action needed (add the field)

## Editor Integration

Generated JSON Schema enables rich editor support:

```yaml
# yaml-language-server: $schema=./schemas/test-definition.schema.json

test:
  name: |  # ← Tooltip: "Human-readable test name"
  pipeline: |  # ← Tooltip: "Path to KSML pipeline YAML"
  produce:
    - topic: |  # ← Tooltip: "Target Kafka topic name"
      keyType: |  # ← Autocomplete: string, avro:*, json, binary
                   #   Tooltip: "Key serialization type (default: string)"
      valueType: |  # ← Autocomplete: string, avro:*, json, binary  
      messages:
        - key: "sensor1"
          value: |  # ← Structure hints based on valueType
          timestamp: |  # ← Tooltip: "Optional epoch milliseconds"
```

## Future Extensibility

Adding new fields becomes trivial:

```java
// 1. Add to record with annotation:
@JsonSchema(description = "Timeout for assertions in milliseconds", defaultValue = "5000")
Long assertionTimeoutMs

// 2. Add to parser:
blockFields.optionalLong("assertionTimeoutMs", 5000L)

// 3. Regenerate schema:
mvn exec:java -Dexec.mainClass="...TestDefinitionSchemaGenerator"
```

Editors immediately get the new field with tooltips and validation.

## Migration Strategy

The design supports incremental migration:

1. **Phase 1:** Clean up parser (no external dependencies)
2. **Phase 2:** Add schema annotations (no behavior change)  
3. **Phase 3:** Add schema generation (build-time utility)
4. **Phase 4:** Document editor setup (user-facing)

Each phase delivers independent value and can be stopped at any point.