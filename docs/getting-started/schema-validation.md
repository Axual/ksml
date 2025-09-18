# KSML Schema Validation

KSML provides a JSON Schema specification file that enables IDE validation, autocompletion, and error checking for your KSML definition files. This helps you write correct KSML syntax and catch errors early in development.

The KSML project is available at: [https://github.com/Axual/ksml](https://github.com/Axual/ksml)

## What is the KSML Language Specification?

The KSML Language Specification is a comprehensive JSON Schema file (`docs/ksml-language-spec.json`) that defines the complete structure and syntax of KSML definition files. This schema validates:

- **YAML Structure**: Correct nesting and organization of KSML components (streams, tables, globalTables, functions, pipelines, producers)
- **Field Names**: Valid property names for operations (transformValue, filter, aggregate, join, etc.)
- **Field Types**: Whether a field should be a string, object, array, or boolean
- **Required Fields**: Which properties must be present in each component
- **Configuration Structure**: Proper formatting of configuration sections

**Note**: The schema validates YAML syntax and structure, not the semantic correctness of string values. For example, it checks that `resultType` is a string, but doesn't verify if the specified type (e.g., `"json"` or `"avro:MySchema"`) is actually valid in your KSML context.

### Schema Benefits

Using the KSML schema in your IDE provides:

1. **Real-time Validation**: Immediate feedback on syntax errors and invalid configurations
2. **Autocompletion**: IDE suggestions for properties, operations, and values
3. **Documentation**: Inline help text explaining each field and operation
4. **Structure Validation**: Ensures correct YAML structure and required fields are present
5. **Error Prevention**: Catch configuration mistakes before deployment

## Setting Up Schema Validation in IntelliJ IDEA

Follow these steps to configure KSML schema validation in IntelliJ IDEA:

### Step 1: Access JSON Schema Settings

1. Open **IntelliJ IDEA**
2. Go to **File** → **Preferences** (on macOS) or **File** → **Settings** (on Windows/Linux)
3. Navigate to **Languages & Frameworks** → **Schemas and DTDs** → **JSON Schema Mappings**

### Step 2: Add KSML Schema

1. Click the **+** (plus) button to add a new schema mapping
2. Configure the mapping:
   - **Name**: `KSML Language Specification`
   - **Schema file or URL**: Browse to `docs/ksml-language-spec.json` in your KSML project directory
   - **Schema version**: Select **JSON Schema version 4**

### Step 3: Configure File Mappings

Add mappings for your KSML definition files by clicking **+** in the mappings section:

1. **For specific files**:
      - Add individual KSML definition files (e.g., `my-pipeline.yaml`)

2. **For directories**:
      - Add a directory pattern like `**/definitions/**/*.yaml`

3. **For file patterns**:
      - Add patterns like `*-pipeline.yaml` or `ksml-*.yaml`

## Setting Up Schema Validation in Visual Studio Code

For VS Code users, follow these steps:

### Step 1: Install YAML Extension

1. Install the **YAML** extension by Red Hat from the Extensions Marketplace

### Step 2: Configure Schema Association

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
      "ksml-*.yaml"
    ]
  }
}
```

### Step 3: Workspace Configuration

Alternatively, create a `.vscode/settings.json` file in your project root:

```json
{
  "yaml.schemas": {
    "./docs/ksml-language-spec.json": [
      "definitions/**/*.yaml",
      "pipelines/**/*.yaml"
    ]
  }
}
```

## Verifying Schema Validation

Once configured, you should see:

### Valid KSML Syntax
- Green underlines or checkmarks
- Autocompletion suggestions when typing
- Inline documentation on hover

### Invalid Syntax Detection
- Red squiggly underlines for errors
- Error messages explaining what's wrong
- Suggestions for corrections

### Example: Testing Validation

Create a test KSML file with an intentional error:

```yaml
name: "Test Pipeline"
streams:
  input:
    topic: "input-topic"
    keyType: "string"
    valueType: "json"

pipelines:
  test:
    from: "input"
    via:
      - type: "invalidOperation"  # This should show an error
    to:
      topic: "output-topic"
```

The schema validator should highlight `invalidOperation` as an invalid operation type and suggest valid alternatives like `transformValue`, `filter`, etc.

## Schema File Location

The KSML language specification is located at:
```
docs/ksml-language-spec.json
```

This file is:

- **Version-controlled**: Updated with each KSML release
- **Comprehensive**: Covers all KSML features and syntax
- **Self-documenting**: Includes descriptions for all properties and operations

## Next Steps

With schema validation configured:

1. **Follow the [KSML Basics Tutorial](basics-tutorial.md)** to build your first validated application