# Error Handling and Recovery in KSML

This tutorial explores comprehensive strategies for handling errors and implementing recovery mechanisms in KSML applications, helping you build robust and resilient stream processing pipelines.

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic incoming_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic valid_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic invalid_orders && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_readings && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic processed_sensors && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_errors && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic payment_requests && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic processed_payments && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic failed_payments && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic api_operations && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic successful_operations && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic failed_operations && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic service_requests && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic service_responses && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic circuit_events && \
    ```

## Introduction to Error Handling

Error handling is critical for production stream processing applications. Common error scenarios include:

- **Data Quality Issues**: Invalid, malformed, or missing data
- **External Service Failures**: Network timeouts, API errors, service unavailability  
- **Resource Constraints**: Memory limitations, disk space issues
- **Business Rule Violations**: Data that doesn't meet application requirements
- **Transient Failures**: Temporary network issues, rate limiting, service overload

Without proper error handling, these issues can cause application crashes, data loss, incorrect results, or inconsistent state.

## Core Error Handling Patterns

### 1. Validation and Filtering

Proactively validate data and filter out problematic messages before they cause downstream errors.

??? info "Order Events Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/producer-order-validation.yaml" %}
    ```

??? info "Validation and Filtering Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/processor-validation-filtering.yaml" %}
    ```

**What it does:**

- **Produces order events**: Creates orders with varying quality - some valid with all fields, some missing product_id, some with invalid quantity, some marked as "malformed" 
- **Validates with branching**: Uses branch operation with expression to check `value and "malformed" not in value and "product_id" in value and value.get("quantity", 0) > 0`
- **Routes by validity**: Valid orders go to valid_orders topic with "valid" status, everything else goes to invalid_orders topic
- **Categorizes errors**: Adds specific error_reason (malformed_data, missing_required_fields, invalid_quantity, validation_failed) to invalid orders
- **Logs decisions**: Tracks processing with logging for valid/invalid determinations and specific error reasons for debugging

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Input[(incoming_orders)]
    LogIn["<code>peek</code><br/>Log order"]

    %% Validation
    Branch{"<code>branch</code><br/>valid order?"}

    %% Valid path
    AddStatus["<code>transformValue</code><br/>add_validation_status"]
    LogValid["<code>peek</code><br/>Log valid"]
    Valid[(valid_orders)]

    %% Invalid path
    AddError["<code>transformValue</code><br/>add_error_details<br/>error_reason: malformed_data,<br/>missing_required_fields,<br/>invalid_quantity, validation_failed"]
    LogInvalid["<code>peek</code><br/>Log error reason"]
    Invalid[(invalid_orders)]

    %% Data Flow
    Input --> LogIn
    LogIn --> Branch
    Branch -->|"not malformed, has product_id,<br/>quantity > 0"| AddStatus
    AddStatus --> LogValid
    LogValid --> Valid
    Branch -->|"default branch"| AddError
    AddError --> LogInvalid
    LogInvalid --> Invalid

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"
    classDef errorTopic fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000

    class Input,Valid topic
    class LogIn,Branch,AddStatus,LogValid,AddError,LogInvalid operation
    class Invalid errorTopic
```

### 2. Try-Catch Error Handling

Use try-catch blocks in Python functions to handle exceptions gracefully and provide fallback behavior.

??? info "Sensor Data Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/producer-sensor-data.yaml" %}
    ```

??? info "Try-Catch Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/processor-try-catch.yaml" %}
    ```

**What it does:**

- **Produces sensor data**: Creates sensor readings with varying quality - some valid with all fields, some with missing sensor_id or temperature, some with non-numeric temperature values
- **Handles with try-catch**: Uses try-catch in valueTransformer to catch ValueError and TypeError exceptions during processing
- **Processes safely**: Attempts to convert temperature to float, validate sensor_id, set default humidity values within exception handling
- **Creates error objects**: When exceptions occur, returns error object with sensor_id, error message, status="error", original_data for debugging
- **Routes by success**: Uses branch to send successful processing to processed_sensors, errors to sensor_errors topic based on status field

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Input[(sensor_readings)]

    %% Processing with try/except inside the function
    Process["<code>transformValue</code><br/>process_sensor_reading<br/><br/><b>try</b>: convert temperature,<br/>validate sensor_id, set defaults<br/><b>except</b> ValueError, TypeError:<br/>return an error object"]
    Log["<code>peek</code><br/>Log result"]

    %% Routing by status
    Branch{"<code>branch</code><br/>status != error?"}

    %% Outputs
    Processed[(processed_sensors)]
    Errors[(sensor_errors)]

    %% Data Flow
    Input --> Process
    Process --> Log
    Log --> Branch
    Branch -->|"yes"| Processed
    Branch -->|"default branch:<br/>error object with<br/>original_data"| Errors

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"
    classDef errorTopic fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000

    class Input,Processed topic
    class Process,Log,Branch operation
    class Errors errorTopic
```

The exception never leaves the function: instead of crashing the pipeline, the function returns a normal message with
`status: "error"`, and the `branch` decides where it goes.

### 3. Dead Letter Queue Pattern

Route messages that cannot be processed to dedicated error topics for later analysis or reprocessing.

??? info "Payment Requests Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/producer-payment-requests.yaml" %}
    ```

??? info "Dead Letter Queue Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/processor-dead-letter-queue.yaml" %}
    ```

**What it does:**

- **Produces payment requests**: Creates payment events with varying scenarios - some successful, some with insufficient funds, some with invalid cards, some with network issues
- **Simulates processing**: Mock payment processing with different failure types - permanent (invalid_card, insufficient_funds) vs transient (network_error, timeout)
- **Classifies errors**: Determines retry eligibility based on error type - network/timeout errors are retryable, invalid card/insufficient funds are permanent
- **Enriches with context**: Adds processing metadata, error classification, retry recommendations, original request data to failed messages
- **Routes by result**: Uses branch to send successful payments to processed_payments, failures to failed_payments topic with full error context

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Input[(payment_requests)]

    %% Processing and classification
    Process["<code>transformValue</code><br/>process_payment"]
    Log["<code>peek</code><br/>Log result"]
    Branch{"<code>branch</code><br/>status == processed?"}

    %% Outputs
    Processed[(processed_payments)]
    Failed[("failed_payments<br/>(dead letter queue)")]

    %% Data Flow
    Input --> Process
    Process --> Log
    Log --> Branch
    Branch -->|"processed"| Processed
    Branch -->|"permanent_failure, retry_eligible: false<br/>malformed_request, invalid_amount,<br/>missing_required_fields, unsupported_currency"| Failed
    Branch -->|"temporary_failure, retry_eligible: true<br/>network_timeout"| Failed

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"
    classDef errorTopic fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000

    class Input,Processed topic
    class Process,Log,Branch operation
    class Failed errorTopic
```

Every failed payment ends up in the same dead letter queue, but the `retry_eligible` field tells a later consumer
which messages are worth sending again.

### 4. Retry Strategies with Exponential Backoff

Implement sophisticated retry logic for transient failures with exponential backoff and jitter.

??? info "API Operations Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/producer-api-operations.yaml" %}
    ```

??? info "Retry Strategies Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/processor-retry-strategies.yaml" %}
    ```

**What it does:**

- **Produces API operations**: Creates operations (fetch_user, update_profile, delete_account) with deliberate failures for some operations to test retry logic
- **Simulates API calls**: Mock external API with different failure scenarios (timeout, rate_limit, server_error) and success cases  
- **Implements retry logic**: Calculates exponential backoff delays (1s, 2s, 4s, 8s) with added jitter to prevent retry storms
- **Tracks attempts**: Maintains retry count, determines retry eligibility based on error type and max attempts (3), stores original operation data
- **Routes by outcome**: Successful operations go to successful_operations, failed/exhausted retries go to failed_operations with retry history

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Input[(api_operations)]

    %% Processing with retry decision
    Process["<code>transformValue</code><br/>process_with_retry"]
    Decide{"Operation<br/>failed?"}
    Retryable{"Retryable error<br/>and retry_count < 3?"}

    %% Outcomes
    Success["status: success"]
    SuccessRetry["status: success_after_retry"]
    RetryNeeded["status: retry_needed<br/>backoff_delay: 1s, 2s, 4s<br/>(± 20% jitter)"]
    Permanent["status: permanent_failure"]

    %% Routing
    Branch{"<code>branch</code><br/>success or<br/>success_after_retry?"}
    Succeeded[(successful_operations)]
    Failed[(failed_operations)]

    %% Data Flow
    Input --> Process
    Process --> Decide
    Decide -->|"no"| Success
    Decide -->|"yes"| Retryable
    Retryable -->|"yes, simulated retry works"| SuccessRetry
    Retryable -->|"yes, still failing"| RetryNeeded
    Retryable -->|"no"| Permanent
    Success & SuccessRetry & RetryNeeded & Permanent --> Branch
    Branch -->|"yes"| Succeeded
    Branch -->|"default branch"| Failed

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"
    classDef errorTopic fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000
    classDef outcome fill:#ffffff,stroke:#616161,stroke-width:1px,color:#000

    class Input,Succeeded topic
    class Process,Decide,Retryable,Branch operation
    class Success,SuccessRetry,RetryNeeded,Permanent outcome
    class Failed errorTopic
```

Note that this example only **decides** whether an operation should be retried and calculates the delay: it simulates
the outcome of a retry within the same function call. Messages with `status: retry_needed` are written to
`failed_operations` together with their `retry_count` and `backoff_delay`, and are not sent through the pipeline again.

### 5. Circuit Breaker Pattern

Prevent cascading failures by temporarily stopping calls to failing services, allowing them to recover.

??? info "Service Requests Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/producer-service-requests.yaml" %}
    ```

??? info "Circuit Breaker Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/intermediate-tutorial/error-handling/processor-circuit-breaker.yaml" %}
    ```

**What it does:**

- **Produces service requests**: Creates requests with request_ids, some designed to fail to trigger circuit breaker state changes  
- **Tracks circuit state**: Uses global variables (failure_count, circuit_state) to maintain CLOSED/OPEN/HALF_OPEN states across all requests
- **Opens on failures**: After 3 consecutive failures, circuit transitions from CLOSED to OPEN state to protect failing service
- **Rejects when open**: In OPEN state, immediately returns circuit_open status without attempting processing, showing current failure count
- **Processes requests**: In CLOSED state, simulates service calls with success/failure scenarios, updating failure counter and circuit state accordingly

The circuit breaker is a state machine that is shared by all requests. The diagram below shows its states and the
events that move it from one state to another:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Circuit states
    Start(( )) --> Closed
    Closed(["CLOSED<br/>requests are processed"])
    Open(["OPEN<br/>requests are rejected"])
    HalfOpen(["HALF_OPEN<br/>requests are processed again"])

    %% Transitions
    Closed -->|"success, or failure<br/>with failure_count below 3"| Closed
    Closed -->|"failure_count reaches 3"| Open
    Open -->|"request rejected<br/>(status circuit_open)"| Open
    Open -->|"next request more than<br/>10s after the last failure"| HalfOpen
    HalfOpen -->|"2 successes,<br/>counters reset"| Closed
    HalfOpen -->|"failure"| Open

    %% Styling
    classDef closed fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#000
    classDef open fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000
    classDef halfOpen fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#000
    classDef start fill:#616161,stroke:#616161

    class Closed closed
    class Open open
    class HalfOpen halfOpen
    class Start start
```

Every request then goes through the same pipeline, and the `status` set by the circuit breaker decides where it goes:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Input[(service_requests)]

    %% Processing
    Handler["<code>transformValue</code><br/>circuit_breaker_handler"]
    Log["<code>peek</code><br/>Log status and state"]
    Branch{"<code>branch</code><br/>status == success?"}

    %% Outputs
    Responses[(service_responses)]
    Events[(circuit_events)]

    %% Data Flow
    Input --> Handler
    Handler --> Log
    Log --> Branch
    Branch -->|"success"| Responses
    Branch -->|"failure or circuit_open"| Events

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"
    classDef errorTopic fill:#ffcdd2,stroke:#c62828,stroke-width:2px,color:#000

    class Input,Responses topic
    class Handler,Log,Branch operation
    class Events errorTopic
```

## Error Handling Best Practices

### Data Type Recommendations
- **Use JSON types**: Provides flexibility for error objects and easy inspection in Kowl UI
- **Include context**: Add timestamps, retry counts, and error classifications to all error messages
- **Preserve original data**: Keep original messages in error objects for debugging

### Function Design Patterns  
- **Handle null values**: Always check for `None`/`null` values explicitly
- **Use appropriate exceptions**: Choose specific exception types for different error conditions
- **Provide meaningful errors**: Include context about what went wrong and potential solutions
- **Log appropriately**: Use different log levels (info/warn/error) based on severity

### Monitoring and Alerting
- **Track error rates**: Monitor error percentages by type and set appropriate thresholds
- **Circuit breaker metrics**: Alert when circuits open and track recovery times  
- **Retry success rates**: Measure effectiveness of retry strategies
- **Dead letter queue size**: Monitor unprocessable message volume

### Testing Error Scenarios
- **Simulate failures**: Test error handling code with various failure scenarios
- **Load testing**: Ensure error handling works under high load conditions
- **Recovery testing**: Verify systems can recover from failure states

## Error Pattern Selection Guide

| Pattern | Use Case | Benefits | When to Use |
|:--------|:---------|:---------|:------------|
| **Validation & Filtering** | Data quality issues | Early detection, clear routing | Input data validation, format checking |
| **Try-Catch** | Function-level errors | Graceful degradation | Type conversion, calculations, parsing |
| **Dead Letter Queue** | Permanent failures | No data loss, failure analysis | Malformed data, business rule violations |
| **Retry Strategies** | Transient failures | Fault tolerance, automatic recovery | Network timeouts, rate limits, temporary errors |
| **Circuit Breaker** | External service failures | Prevents cascading failures | API calls, database connections, service dependencies |

## Next Steps

- [Stream Processing Operations Reference](../../reference/operation-reference.md)
- [Function Types Reference](../../reference/function-reference.md)
- [Advanced Stream Processing Patterns](../advanced/index.md)