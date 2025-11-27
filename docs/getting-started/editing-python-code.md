# Editing Python Code

KSML allows you to write custom logic in Python within your pipeline definitions. This guide explains the different approaches for organizing and editing Python code, from inline to fully externalized modules.

## Python Code Options

### 1. Inline Code

The simplest approach is to write Python code directly in your YAML file:

??? example "Inline code example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/pipelines/test-filter.yaml" %}
    ```

**Use when:** Logic is short and specific to one function.

### 2. External Python Files

Reference an external Python file using the `file:` prefix in your YAML, and define the function in a separate `.py` file.

??? example "External Python file example (click to expand)"

    **YAML definition:**
    ```yaml
    {% include "../../ksml/src/test/resources/pipelines/test-filter-external-python-function.yaml" %}
    ```

    **Python file (`test-filter-external-python-function.py`):**
    ```python
    {% include "../../ksml/src/test/resources/pipelines/test-filter-external-python-function.py" %}
    ```

**Use when:** Function logic is reusable or complex enough to warrant separate files.

### 3. Global Code with External Files

Define reusable functions in a `globalCode` block that can be referenced by multiple functions:

??? example "Global code with external file (click to expand)"

    **YAML definition:**
    ```yaml
    {% include "../../ksml/src/test/resources/pipelines/test-filter-globalcode-external.yaml" %}
    ```

    **Python file (`test-filter-globalcode-external.py`):**
    ```python
    {% include "../../ksml/src/test/resources/pipelines/test-filter-globalcode-external.py" %}
    ```

**Use when:** Multiple functions share common logic or utilities.

### 4. Python Module Imports

For advanced scenarios, import Python modules using `globalCode`:

??? example "Python module import example (click to expand)"

    **YAML definition:**
    ```yaml
    {% include "../../ksml/src/test/resources/pipelines/test-filter-module-import.yaml" %}
    ```

    **Python module (`test_filter_module.py`):**
    ```python
    {% include "../../ksml/src/test/resources/pipelines/test_filter_module.py" %}
    ```

**Use when:** You have a library of reusable functions or need to organize complex logic into modules.

**Note:** When using module imports, configure the `pythonModulePath` parameter in  runner configuration:

```yaml
  # Python context configuration for module imports
  ksml:
    pythonContext:
      pythonModulePath: /ksml  # Directory where Python modules are located (mounted volume)
```

## Editor Support with Type Stubs

KSML provides a Python stub file (`ksml_runtime.pyi`) that enables code completion and type checking in your IDE for KSML runtime variables.

### Setting Up Code Completion

Place the stub file in your Python path and use `TYPE_CHECKING` to enable editor support without runtime overhead.

See the example in the [External Python Files](#2-external-python-files) section above for the pattern:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  # This import only happens during development, enables code completion
  from ksml_runtime import log
```

??? info "Complete ksml_runtime.pyi stub file (click to expand)"

    ```python
    {% include "../../ksml/src/test/resources/ksml_runtime.pyi" %}
    ```

The stub file defines the KSML runtime environment:

- **`log`**: SLF4J-style logger with trace, debug, info, warn, error methods
- **`metrics`**: Access to counters, meters, and timers for custom metrics
- **`stores`**: Dictionary of state stores available to your function

### Available Runtime Objects

The stub file provides type hints for:

#### Logger
- `log.trace()`, `log.debug()`, `log.info()`, `log.warn()`, `log.error()`
- Level checks: `isTraceEnabled()`, `isDebugEnabled()`, etc.

#### Metrics
- `metrics.counter(name)` - Create/get a counter metric
- `metrics.meter(name)` - Create/get a meter metric
- `metrics.timer(name)` - Create/get a timer metric

#### State Stores
- Access via `stores[store_name]`
- Key-value operations: `get()`, `put()`, `delete()`, `range()`, `all()`

## Best Practices

1. **Start inline**, move to external files as complexity grows
2. **Use `globalCode`** for shared utilities and helper functions
3. **Import modules** when you need a full library of functions
4. **Enable type stubs** for better IDE support and fewer runtime errors
5. **Keep functions pure** - avoid side effects outside of logging and metrics
6. **Handle None values** explicitly to prevent runtime errors

## Example File Organization

Place the `ksml_runtime.pyi` stub at the root of your definitions, or somewhere on your editor import path:
```
my-project/
├── pipelines/
│   ├── my-pipeline.yaml         # Main pipeline definition
│   ├── filter_helpers.py        # Shared function files
│   ├── transform_logic.py       # External Python code
│   └── modules/
│       └── data_validation.py   # Importable Python modules
└── ksml_runtime.pyi             # Type stub for IDE support
```

## Summary

This guide covered four approaches to organizing Python code in KSML:

1. **Inline code** - Quick and simple for short logic
2. **External files** - Better for reusable or complex functions
3. **Global code** - Share utilities across multiple functions
4. **Module imports** - Full Python module system for libraries

All examples shown are working code from the KSML test suite. Use the `ksml_runtime.pyi` stub file for full IDE support including code completion, type checking, and inline documentation.

This approach keeps your YAML clean, enables code reuse, and provides full IDE support for Python development.
