# KSML Schema Validation

KSML provides two JSON Schema specification files that enable IDE validation, autocompletion, and error checking for your KSML files. These schemas help you write correct syntax and catch errors early in development.

The KSML project is available at: [https://github.com/Axual/ksml](https://github.com/Axual/ksml)

## Understanding KSML Schemas

KSML provides **two separate JSON schemas** for different types of configuration files:

### 1. KSML Language Specification Schema

**File:** `docs/ksml-language-spec.json`  
**Validates:** KSML definition files (e.g., `processor.yaml`, `producer.yaml`, `pipeline.yaml`)

This schema validates KSML stream processing definitions by ensuring:

- Proper nesting and organization of streams, tables, functions, and pipelines
- Valid property names for operations like transformValue, filter, aggregate, and join
- Correct data types for each field (string, object, array, or boolean)
- Required properties are present in each component
- Pipeline operations and transformations follow proper formatting


??? info "Example KSML definition file"
    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/beginner-tutorial/filtering-transforming/processor-complex-filtering-multiple-filters.yaml" %}
    ```

### 2. KSML Runner Configuration Schema

**File:** `docs/ksml-runner-spec.json`  
**Validates:** KSML Runner configuration files (e.g., `ksml-runner.yaml`)

This schema validates the runner configuration that controls how KSML executes:

- Bootstrap servers, application IDs, and Kafka consumer/producer settings
- Definition file locations, storage directories, and feature toggles
- Error handling strategies for consume, produce, and process operations
- Prometheus metrics and REST API configuration for health checks and queries
- Data format serializers and deserializers (Avro, JSON, Protobuf, etc.)
- Connections to Confluent or Apicurio schema registry instances
- Security permissions and sandboxing for Python function execution

??? info "Example KSML Runner configuration file"

    ```yaml
    {%
      include "../../ksml-integration-tests/src/test/resources/docs-examples/beginner-tutorial/different-data-formats/avro/confluent_avro/ksml-runner.yaml"
    %}
    ```

### Schema Benefits

Using KSML schemas in your IDE provides:

1. Immediate feedback on syntax errors and invalid configurations
2. Smart suggestions for properties, operations, and valid values
3. Inline help text explaining each field and operation as you type
4. Verification that YAML structure is correct and required fields are present
5. Early detection of configuration mistakes before deployment
6. Type checking for strings, numbers, booleans, objects, and arrays
7. Valid value suggestions for enumerated fields like error handlers (`stopOnFail`, `continueOnFail`, `retryOnFail`)

## Setting Up Schema Validation

There are two ways to enable schema validation:

1. **Inline schema declaration** (recommended) - Add a comment at the top of each file
2. **IDE configuration** - Configure schema mappings in your IDE settings

### Method 1: Inline Schema Declaration (Recommended)

Add a schema declaration comment at the **top** of your YAML files. This method works across IDEs without configuration.

#### For KSML Definition Files

Add this line at the top of your pipeline/processor/producer files:

```yaml
# yaml-language-server: $schema=https://axual.github.io/ksml/latest/ksml-language-spec.json

streams:
  my-stream:
    topic: "my-topic"
    # ... rest of your definition

```

#### For KSML Runner Configuration Files

Add this line at the top of your `ksml-runner.yaml` file:

```yaml
# yaml-language-server: $schema=https://axual.github.io/ksml/latest/ksml-runner-spec.json

kafka:
  bootstrap.servers: localhost:9092
  # ... rest of your configuration
```

#### How Inline Schemas Work

The special comment `# yaml-language-server: $schema=URL` tells your IDE:

1. **VS Code (YAML extension)**: Automatically detects and applies the schema from the URL
2. **IntelliJ IDEA**: Recognizes the declaration and fetches the schema
3. **Other editors**: Most modern YAML-aware editors support this convention

**Benefits:**
- No IDE configuration needed
- Schema association travels with the file
- Works for all team members immediately
- Version-controlled alongside your code
- Public URL means always up-to-date schemas

**Note:** The schemas are published to GitHub Pages at `https://axual.github.io/ksml/latest/`, so an internet connection is required for initial download. IDEs typically cache schemas locally after the first fetch.

**For local development** without internet access, you can use relative paths:
```yaml
# yaml-language-server: $schema=../../docs/ksml-language-spec.json
```

---

### Method 2: IDE Configuration

If you prefer to configure schema mappings in your IDE settings, or if you're working offline with local schema files, follow these steps.

#### IntelliJ IDEA Setup

**Step 1: Access JSON Schema Settings**

1. Open **IntelliJ IDEA**
2. Go to **File** → **Preferences** (on macOS) or **File** → **Settings** (on Windows/Linux)
3. Navigate to **Languages & Frameworks** → **Schemas and DTDs** → **JSON Schema Mappings**

**Step 2: Add KSML Language Specification Schema**

Configure validation for KSML definition files (streams, pipelines, producers):

1. Click the **+** (plus) button to add a new schema mapping
2. Configure the mapping:
   - **Name**: `KSML Language Specification`
   - **Schema file or URL**: Browse to `docs/ksml-language-spec.json` in your KSML project directory
   - **Schema version**: Select **JSON Schema version 7** (Draft 7 and Draft 2019-09 are compatible)

3. Add file mappings by clicking **+** in the mappings section:
   - **For specific files**: `processor.yaml`, `producer.yaml`, `pipeline.yaml`
   - **For directory patterns**: `**/definitions/**/*.yaml`, `**/pipelines/**/*.yaml`
   - **For file patterns**: `*-pipeline.yaml`, `*-processor.yaml`

**Step 3: Add KSML Runner Configuration Schema**

Configure validation for KSML Runner configuration files:

1. Click the **+** (plus) button again to add another schema mapping
2. Configure the mapping:
   - **Name**: `KSML Runner Configuration`
   - **Schema file or URL**: Browse to `docs/ksml-runner-spec.json` in your KSML project directory
   - **Schema version**: Select **JSON Schema version 7**

3. Add file mappings by clicking **+** in the mappings section:
   - **For specific files**: `ksml-runner.yaml`, `application.yaml`
   - **For file patterns**: `ksml-runner*.yaml`, `*-runner.yaml`

**Important**: Make sure the file patterns for each schema don't overlap. KSML definition files should map to the Language Specification schema, while runner configuration files should map to the Runner Configuration schema.

#### Visual Studio Code Setup

**Note:** If you're using the inline schema declaration method (recommended), no configuration is needed in VS Code. The YAML extension automatically recognizes the `# yaml-language-server: $schema=URL` comments.

For workspace-wide configuration without inline declarations:

### Step 1: Install YAML Extension

1. Install the **YAML** extension by Red Hat from the Extensions Marketplace

### Step 2: Configure Schema Associations

You need to map different file patterns to the appropriate schema.

#### Option A: User Settings

1. Open **Settings** (Ctrl/Cmd + ,)
2. Search for "yaml schemas"
3. Add the following to your `settings.json`:

```json
{
  "yaml.schemas": {
    "file:///path/to/ksml/docs/ksml-language-spec.json": [
      "**/definitions/**/*.yaml",
      "**/pipelines/**/*.yaml",
      "*-pipeline.yaml",
      "*-processor.yaml",
      "*-producer.yaml"
    ],
    "file:///path/to/ksml/docs/ksml-runner-spec.json": [
      "**/ksml-runner.yaml",
      "**/*-runner.yaml",
      "**/application.yaml"
    ]
  }
}
```

#### Option B: Workspace Configuration

Create a `.vscode/settings.json` file in your project root for project-specific configuration:

```json
{
  "yaml.schemas": {
    "./docs/ksml-language-spec.json": [
      "definitions/**/*.yaml",
      "pipelines/**/*.yaml",
      "examples/**/processor.yaml",
      "examples/**/producer.yaml"
    ],
    "./docs/ksml-runner-spec.json": [
      "**/ksml-runner.yaml",
      "examples/**/ksml-runner.yaml"
    ]
  }
}
```

**Note**: Workspace configuration (Option B) is recommended as it's portable and version-controlled with your project.

## Verifying Schema Validation

Once configured, you should see:

### Valid KSML Syntax
- Green underlines or checkmarks indicating correct syntax
- Autocompletion suggestions appearing as you type
- Inline documentation when hovering over fields
- Type hints showing expected data types

### Invalid Syntax Detection
- Red squiggly underlines highlighting errors
- Clear error messages explaining the problem
- Suggested corrections for common mistakes
- Validation of enumerated values with all valid options

### Example 1: Testing KSML Definition Validation

Create a test KSML definition file (`test-pipeline.yaml`) with an intentional error:

```yaml
streams:
  input:
    topic: "input-topic"
    keyType: string
    valueType: json

pipelines:
  test:
    from: "input"
    via:
      - type: "invalidOperation"  # This should show an error
    to:
      topic: "output-topic"
```

The schema validator will:

- Highlight `invalidOperation` as an invalid operation type
- Suggest valid alternatives like `transformValue`, `filter`, and `aggregate`
- Display documentation for each operation when hovering

### Example 2: Testing KSML Runner Configuration Validation

Create a test runner configuration file (`test-runner.yaml`) with intentional errors:

```yaml
kafka:
  bootstrap.servers: localhost:9092
  # Missing required field: application.id

ksml:
  applicationServer:
    enabled: true
    port: 99999  # Invalid port (exceeds maximum 65535)
  errorHandling:
    consume:
      handler: "invalidHandler"  # Invalid enum value
```

The schema validator will:

- Highlight the missing required field `application.id`
- Show that port `99999` exceeds the maximum allowed value of `65535`
- Suggest valid handler values: `stopOnFail`, `continueOnFail`, or `retryOnFail`
- Display descriptions for each configuration option when hovering

## Generating Schema Files

KSML schemas are automatically generated during the build process, but you can also generate them manually when needed.

### Generating Both Schemas

To generate both the KSML Language Specification and Runner Configuration schemas:

```bash
mvn package -DskipTests -pl ksml-runner -am
```

This builds the module with its dependencies and generates both schema files:

- `docs/ksml-language-spec.json` for KSML definitions
- `docs/ksml-runner-spec.json` for runner configuration

**Note:** The `-am` (also-make) flag is required to build all dependencies needed for schema generation.

### Generating Individual Schemas

If you've already built the project and want to generate schemas individually, you can run the JAR directly:

To generate only the KSML Language Specification schema:

```bash
java -jar ksml-runner/target/ksml-runner-*.jar --schema docs/ksml-language-spec.json
```

To generate only the KSML Runner Configuration schema:

```bash
java -jar ksml-runner/target/ksml-runner-*.jar --runner-schema docs/ksml-runner-spec.json
```

**Note:** These commands require the project to be built first with `mvn clean package`.

### Build Integration

Schemas are automatically regenerated when running:

- `mvn clean package` for a full build (recommended)
- `mvn package -DskipTests -pl ksml-runner -am` for quick schema generation without tests
- Any Maven build that includes the `process-classes` phase for `ksml-runner`

The schemas are always kept in sync with the codebase, ensuring your IDE validation matches the current KSML version.

## Schema File Locations

Both KSML schemas are located in the `docs/` directory:

### KSML Language Specification
```
docs/ksml-language-spec.json
```

**Purpose:** Validates KSML definition files (streams, pipelines, functions, producers)

### KSML Runner Configuration
```
docs/ksml-runner-spec.json
```

**Purpose:** Validates KSML Runner configuration files (Kafka settings, error handling, observability)

### Schema Characteristics

Both schema files share these characteristics:

- Updated and version-controlled with each KSML release
- Comprehensive coverage of all features and configuration options
- Built-in descriptions for every property and field
- Automatically generated from Java code annotations using Jackson and Jakarta Validation

## Next Steps

With schema validation configured:

1. Follow the [KSML Basics Tutorial](basics-tutorial.md) to build your first validated application
2. Explore the examples in the `docs/` directory with full IDE support
3. Configure runner settings confidently using the Runner Configuration schema