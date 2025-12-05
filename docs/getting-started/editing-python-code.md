# Editing Python Code

KSML allows you to write custom logic in Python within your pipeline definitions. This guide explains the different approaches for organizing and editing Python code, from inline to fully externalized modules.

## Python Code Options

### 1. Inline Code

The simplest approach is to write Python code directly in your YAML file:

??? info "Inline code example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/pipelines/test-filter.yaml" %}
    ```

**Use when:** Logic is short and specific to one function.

### 2. Global Code

Define reusable functions in a `globalCode` block that can be referenced by multiple functions:

??? info "Global code example (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/pipelines/test-filter-globalcode.yaml" %}
    ```

**Use when:** Multiple functions share common logic or utilities.

### 3. Python Module Imports

For advanced scenarios, import Python modules using `globalCode`:

??? info "Python module import example (click to expand)"

    **YAML definition:**
    ```yaml
    {% include "../../ksml/src/test/resources/pipelines/test-filter-module-import.yaml" %}
    ```

    **Python module (`test_filter_module.py`):**
    ```python
    {% include "../../ksml/src/test/resources/pipelines/test_filter_module.py" %}
    ```

**Use when:** You have a library of reusable functions or need to organize complex logic into modules.

**Note:** When using module imports, configure the `pythonModulePath` parameter in runner configuration:

```yaml
  # Python context configuration for module imports
  ksml:
    pythonContext:
      pythonModulePath: /ksml  # Directory where Python modules are located (mounted volume)
```

## Passing KSML Runtime Objects to Modules

When using Python modules, you cannot directly access KSML runtime variables (`log`, `metrics`, `stores`) inside the module because they are injected into the function's local scope, not the module's global scope.

To use these objects in your module functions, **pass them as arguments** from the YAML function definition.

### Passing the Logger

In your YAML, pass the `log` object to your module function:

```yaml
functions:
  module_imports:
    globalCode: from my_module import calculate_score

  calculate_fraud_score:
    type: valueTransformer
    # Pass the KSML-provided 'log' object as an argument
    expression: calculate_score(value, log)
    resultType: json
```

In your Python module, accept the logger as a parameter:

```python
from ksml import PythonLogger

def calculate_score(value, log: PythonLogger):
    """Calculate score using the KSML logger."""
    risk_score = value.get("risk_score", 0)
    log.info("Calculated risk score: {}", risk_score)
    return {"score": risk_score}
```

### Passing State Stores

For functions that use state stores, pass the store as an argument:

```yaml
functions:
  check_location:
    type: valueTransformer
    resultType: json
    code: |
      result = check_unusual_location(value, card_location_history)
    expression: result
    stores:
      - card_location_history
```

In your Python module:

```python
from ksml import KeyValueStore

def check_unusual_location(value, store: KeyValueStore):
    """Check location using the state store."""
    card_id = value.get("card_id")
    history = store.get(card_id)
    # ... process and update store
    store.put(card_id, updated_history)
    return result
```

### Passing Metrics

Similarly, pass the `metrics` object when you need custom metrics:

```yaml
functions:
  process_with_metrics:
    type: valueTransformer
    expression: process_value(value, metrics)
    resultType: json
```

```python
from ksml import MetricsBridge

def process_value(value, metrics: MetricsBridge):
    """Process value and record metrics."""
    counter = metrics.counter("processed_records")
    counter.increment()
    return value
```

## Editor Support with the `ksml` Module

KSML provides a `ksml.py` module that enables code completion and type checking in your IDE while also providing the actual Java types at runtime.

### Setting Up Code Completion

Place the `ksml.py` module in your Python module directory (the same directory configured as `pythonModulePath`). The module works in two modes:

- **At edit time:** Provides Protocol classes for IDE code completion and type checking
- **At runtime:** Imports the actual Java types via GraalVM

Simply import the types you need in your module:

```python
from ksml import PythonLogger, KeyValueStore, MetricsBridge

def my_function(value, log: PythonLogger, store: KeyValueStore):
    log.info("Processing value: {}", value)
    # Your IDE will provide full code completion for log and store
```

### Available Runtime Objects

The `ksml.py` module provides type definitions for:

#### Logger (`PythonLogger`)
- `log.trace()`, `log.debug()`, `log.info()`, `log.warn()`, `log.error()`
- Level checks: `isTraceEnabled()`, `isDebugEnabled()`, etc.

#### Metrics (`MetricsBridge`)
- `metrics.counter(name)` - Create/get a counter metric
- `metrics.meter(name)` - Create/get a meter metric
- `metrics.timer(name)` - Create/get a timer metric

#### State Stores
Three store types are available:

- **`KeyValueStore`** - Simple key-value operations: `get()`, `put()`, `delete()`, `range()`, `all()`
- **`SessionStore`** - Session-windowed store: `fetch()`, `findSessions()`, `put()`, `remove()`
- **`WindowStore`** - Time-windowed store: `fetch()`, `fetchAll()`, `put()`, `all()`

??? info "Complete ksml.py module (click to expand)"

    ```python
    {% include "../../ksml/src/test/resources/ksml.py" %}
    ```

## Complete Example: Fraud Detection

For a comprehensive example showing all these patterns together, see the [Fraud Detection use case](../use-cases/fraud-detection.md):

- `fraud-detection-python-module.yaml` - YAML pipeline definition
- `fraud_detection_module.py` - Python module with type-hinted functions
- `ksml.py` - Type module for IDE support and runtime Java type imports

This example demonstrates:

- Importing functions from a Python module
- Passing the `log` object to module functions
- Passing `KeyValueStore` state stores to module functions
- Using type hints for full IDE support

## Best Practices

1. **Start inline**, move to modules as complexity grows
2. **Use `globalCode`** for shared utilities and helper functions
3. **Import modules** when you need a library of reusable functions
4. **Pass runtime objects as arguments** to module functions (`log`, `metrics`, stores)
5. **Use the `ksml.py` module** for better IDE support and fewer runtime errors
6. **Keep functions pure** - avoid side effects outside of logging and metrics
7. **Handle None values** explicitly to prevent runtime errors

## Example File Organization

```
my-project/
├── pipelines/
│   ├── my-pipeline.yaml           # Main pipeline definition
│   └── modules/
│       ├── data_validation.py     # Importable Python modules
│       └── ksml.py                # Type module for IDE support and runtime
└── config/
    └── ksml-runner.yaml           # Runner config with pythonModulePath
```

## Summary

This guide covered approaches to organizing Python code in KSML:

1. **Inline code** - Quick and simple for short logic
2. **Global code** - Share utilities across multiple functions
3. **Module imports** - Full Python module system for libraries

Key points for module imports:

- Pass `log`, `metrics`, and state stores as function arguments
- Use the `ksml.py` module for IDE support and runtime type imports
- Configure `pythonModulePath` in runner configuration

See the [Fraud Detection use case](../use-cases/fraud-detection.md) for a complete working example with all patterns demonstrated.
