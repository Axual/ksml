# Editing Python Code

KSML allows you to write custom logic in Python within your pipeline definitions. This guide explains the different approaches for organizing and editing Python code, from inline to fully externalized modules.

## Python Code Options

### 1. Inline Code

The simplest approach is to write Python code directly in your YAML file:

```yaml
functions:
  sensor_is_blue:
    type: predicate
    code: |
      if value == None:
        log.warn("No value in message with key={}", key)
        return False
      if value["color"] != "blue":
        log.warn("Unknown color: {}", value["color"])
        return False
    expression: True
```

**Use when:** Logic is short and specific to one function.

### 2. External Python Files

Reference an external Python file using the `file:` prefix:

```yaml
functions:
  sensor_is_blue:
    type: predicate
    code: file:filter_logic.py
    expression: values_is_blue(key, value)
```

The external file contains a function definition:

```python
# filter_logic.py
def values_is_blue(somekey, someval):
  if someval == None:
    log.warn("No value in message with key={}", somekey)
    return False
  if someval["color"] != "blue":
    log.warn("Unknown color: {}", someval["color"])
    return False
  return True
```

**Use when:** Function logic is reusable or complex enough to warrant separate files.

### 3. Global Code with External Files

Define reusable functions in a `globalCode` block that can be referenced by multiple functions:

```yaml
functions:
  sensor_is_blue:
    type: predicate
    globalCode: file:shared_filters.py
    expression: check_sensor_is_blue(value, key)
```

The external file:

```python
# shared_filters.py
def check_sensor_is_blue(value, key):
  if value == None:
    log.warn("No value in message with key={}", key)
    return False
  if value["color"] != "blue":
    log.warn("Unknown color: {}", value["color"])
    return False
  return True
```

**Use when:** Multiple functions share common logic or utilities.

### 4. Python Module Imports

For advanced scenarios, import Python modules using `globalCode`:

```yaml
functions:
  sensor_is_blue:
    type: predicate
    globalCode: |
      from test_filter_module import is_blue
    code: |
      return is_blue(value)
    expression: True
```

The module file (`test_filter_module.py`):

```python
def is_blue(val):
    """Filter values with color attribute 'blue'"""
    if val == None:
        return False
    if val["color"] != "blue":
        return False
    return True

def is_red(val):
    """Filter values with color attribute 'red'"""
    if val == None:
        return False
    if val["color"] != "red":
        return False
    return True
```

**Use when:** You have a library of reusable functions or need to organize complex logic into modules.

**Note:** When using module imports, configure the `modulesDirectory` parameter in your test or runner:

```java
@KSMLTest(
    topology = "pipelines/my-pipeline.yaml",
    modulesDirectory = "pipelines"
)
```

## Editor Support with Type Stubs

KSML provides a Python stub file (`ksml_runtime.pyi`) that enables code completion and type checking in your IDE for KSML runtime variables.

### Setting Up Code Completion

Place the stub file in your Python path and use TYPE_CHECKING to enable editor support:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  # Import only happens during development for IDE support
  from ksml_runtime import log, metrics, stores

def check_sensor_is_blue(value, key):
  if value == None:
    log.warn("No value in message with key={}", key)  # ← IDE provides completion
    return False
  if value["color"] != "blue":
    return False
  return True
```

The stub file defines the KSML runtime environment:

- **`log`**: SLF4J-style logger with trace, debug, info, warn, error methods
- **`metrics`**: Access to counters, meters, and timers for custom metrics
- **`stores`**: Dictionary of state stores available to your function

### Available Runtime Objects

#### Logger

```python
log.info("Processing message with key={}", key)
log.warn("Invalid value: {}", value)
log.error("Failed to process: {}", error)
```

#### Metrics

```python
counter = metrics.counter("messages_processed")
counter.increment()

timer = metrics.timer("processing_time")
timer.updateMillis(elapsed_ms)
```

#### State Stores

```python
# Access configured state stores
history = stores["transaction_history"].get(customer_id)
stores["transaction_history"].put(customer_id, updated_history)
```

## Best Practices

1. **Start inline**, move to external files as complexity grows
2. **Use `globalCode`** for shared utilities and helper functions
3. **Import modules** when you need a full library of functions
4. **Enable type stubs** for better IDE support and fewer runtime errors
5. **Keep functions pure** - avoid side effects outside of logging and metrics
6. **Handle None values** explicitly to prevent runtime errors

## File Organization

```
my-project/
├── pipelines/
│   ├── my-pipeline.yaml
│   ├── filter_helpers.py        # Shared functions
│   └── modules/
│       └── data_validation.py   # Importable modules
└── ksml_runtime.pyi             # Type stubs for IDE
```

## Example: Complete Filter Pipeline

```yaml
streams:
  sensor_source:
    topic: sensors
    keyType: string
    valueType: avro:SensorData

functions:
  sensor_is_valid:
    type: predicate
    globalCode: file:sensor_validation.py
    expression: validate_sensor(value, key)

pipelines:
  validation_pipeline:
    from: sensor_source
    via:
      - type: filter
        if: sensor_is_valid
    to: validated_sensors
```

With external file:

```python
# sensor_validation.py
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  # this import happens in the editor only
  from ksml_runtime import log

def validate_sensor(value, key):
  """Validate sensor data"""
  if value is None:
    log.warn("Null value for key={}", key)
    return False

  if "temperature" not in value:
    log.warn("Missing temperature field")
    return False

  if value["temperature"] < -273.15:  # Below absolute zero
    log.error("Invalid temperature: {}", value["temperature"])
    return False

  return True
```

This approach keeps your YAML clean, enables code reuse, and provides full IDE support for Python development.
