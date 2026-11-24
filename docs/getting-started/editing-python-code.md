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

#### A note on `globalCode` vs `code`
Be aware that code in a `globalCode` block will be evaluated once, whereas code in a `code` block will be executed for every message.
This means that for example `import` statements _should not_ be in the `code` block, as doing so will carry a significant runtime overhead.<br>
_One time initializations and `import` statements should always be in `globalCode`._

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

## Using KSML Runtime Objects in Modules

`log` and `metrics` are available everywhere: in functions, in helper functions in `globalCode`, and in your own modules, without passing them as arguments. In a module, import them from the `ksml` module so your IDE knows them too, or use them without import:

```python
from ksml import log, metrics

def calculate_score(value):
    """Calculate score, logging under the name of the KSML function that called it."""
    risk_score = value.get("risk_score", 0)
    log.info("Calculated risk score: {}", risk_score)
    metrics.counter("scores_calculated").increment()
    return {"score": risk_score}
```

`log` always logs under the logger name of the KSML function that is running, also when it is called from a helper function. Outside a KSML function, for example in module code that runs at import, it logs under the name `ksml`.

State stores are different: a function only gets the stores it declares, so pass them to module functions as arguments.
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

## Editor Support with the `ksml` Module

KSML ships a `ksml` Python module that enables code completion and type checking in your IDE, and provides the actual types at runtime.

### Setting Up Code Completion

KSML loads the `ksml` module into every Python context, so `from ksml import ...` works in inline code, `globalCode` and your own modules, without any setup. For code completion, let KSML write the module for its version next to your modules, or anywhere your IDE finds it:

```bash
# With the Docker image
docker run --rm axual/ksml:latest --python-module > modules/ksml.py

# With the runner jar
java -jar ksml-runner.jar --python-module modules/ksml.py
```

Without a file name, `--python-module` prints the module to the standard output. The first line of the file names the KSML version it belongs to, so run the command again after upgrading KSML. The module works in two modes:

- **At edit time:** Provides Protocol classes for IDE code completion and type checking
- **At runtime:** Imports the actual Java types via GraalVM

At runtime, KSML always uses the `ksml` module it ships with. A `ksml.py` in your `pythonModulePath` is not loaded, so an outdated copy only affects your IDE.

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

#### Processor Context (`ProcessorContext`)
- `context.forward(key, value, timestamp=None, headers=None, to=None)` - Emit a record from a punctuator
- `context.currentSystemTimeMs()`, `context.currentStreamTimeMs()`, `context.applicationId()`, `context.taskId()`, `context.commit()`

#### State Stores
The main store types are:

- **`KeyValueStore`** - Simple key-value operations: `get()`, `put()`, `putAll()`, `putIfAbsent()`, `delete()`, `range()`, `reverseRange()`, `all()`, `reverseAll()`, `prefixScan()`, `approximateNumEntries()`
- **`SessionStore`** - Session-windowed store: `fetch()`, `findSessions()`, `put()`, `remove()`
- **`WindowStore`** - Time-windowed store: `fetch()`, `fetchAll()`, `backwardFetch()`, `put()`, `all()`

Timestamped stores have the same methods, plus `*WithTimestamp` methods. See [State Store Reference](../reference/state-store-reference.md#python-store-methods).

Store methods that return many entries, like `all()`, return a **`StoreIterator`**. Use it in a `for`
loop, a `with` block, or with `next()` and `list()`. See [Reading Many Entries with Iterators](../reference/state-store-reference.md#reading-many-entries-with-iterators).

??? info "Complete ksml.py module (click to expand)"

    ```python
    {% include "../../ksml/src/main/resources/io/axual/ksml/python/ksml.py" %}
    ```

## Complete Example: Fraud Detection

For a comprehensive example showing all these patterns together, see the [Fraud Detection use case](../use-cases/fraud-detection.md):

- `fraud-detection-python-module.yaml` - YAML pipeline definition
- `fraud_detection_module.py` - Python module with type-hinted functions that imports from `ksml`

This example demonstrates:

- Importing functions from a Python module
- Logging from a module function with `log` imported from `ksml`
- Passing `KeyValueStore` state stores to module functions
- Using type hints for full IDE support

## Best Practices

1. **Start inline**, move to modules as complexity grows
2. **Use `globalCode`** for shared utilities and helper functions
3. **Import modules** when you need a library of reusable functions
4. **Pass state stores as arguments** to module functions; `log` and `metrics` are available everywhere
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
│       └── ksml.py                # Copy of the ksml module, for IDE support only
└── config/
    └── ksml-runner.yaml           # Runner config with pythonModulePath
```

## Summary

This guide covered approaches to organizing Python code in KSML:

1. **Inline code** - Quick and simple for short logic
2. **Global code** - Share utilities across multiple functions
3. **Module imports** - Full Python module system for libraries

Key points for module imports:

- Use `log` and `metrics` directly, or import them from `ksml`; pass state stores as function arguments
- Import types from the `ksml` module; copy `ksml.py` next to your modules for IDE support
- Configure `pythonModulePath` in runner configuration

See the [Fraud Detection use case](../use-cases/fraud-detection.md) for a complete working example with all patterns demonstrated.
