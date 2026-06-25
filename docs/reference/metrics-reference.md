# Metrics Reference

This document describes the metrics that the KSML Runner exposes for Prometheus, including the custom
KSML metrics, their labels and units, and the other metrics that are available on the same endpoint.

## Enabling the Metrics Endpoint

Metrics are exposed over HTTP by the Prometheus JMX Exporter that is built into the KSML Runner. The
endpoint is disabled by default and is enabled through the `prometheus` section of `ksml-runner.yaml`:

```yaml
ksml:
  prometheus:
    enabled: true
    host: 0.0.0.0
    port: 9999
```

See the [Configuration Reference](configuration-reference.md) for all available options. Once enabled,
the metrics are served at:

```
http://<host>:<port>/metrics
```

Every metric carries a `# HELP` line describing what it measures, so the quickest way to discover the
metrics exposed by a running pipeline is:

```bash
curl -s localhost:9999/metrics | grep '^# HELP'
```

## KSML Custom Metrics

KSML publishes its own metrics under the `ksml_` prefix. Each metric is labelled so that values can be
attributed to a specific function, pipeline or processor node.

The metrics listed in this section are the complete set of metrics that KSML defines itself. Each one
is given a descriptive `# HELP` line at runtime.

### Function Execution Time

`ksml_execution_time` measures the wall-clock time spent in a user function for every invocation. It is
recorded for all function types (forEach, mapper, predicate, etc.).

| Property | Value |
|----------|-------|
| Metric   | `ksml_execution_time` |
| Type     | Timer |
| Unit     | Durations in milliseconds, rates per second |
| Labels   | `function_name`, `function_type`, `namespace`, `pipeline`, `operation_name` (and `step` or `branch` where applicable) |

### Record End-to-End Latency

The `ksml_record_e2e_latency_*_ms` gauges report the latency of a record measured from the source topic
to a specific processor node, as reported by Kafka Streams and enriched with KSML context.

| Property | Value |
|----------|-------|
| Metrics  | `ksml_record_e2e_latency_avg_ms`, `ksml_record_e2e_latency_min_ms`, `ksml_record_e2e_latency_max_ms` |
| Type     | Gauge |
| Unit     | Milliseconds |
| Labels   | `namespace`, `pipeline`, `operation_name`, `processor_node_id`, `step`, `task_id`, `subtopology`, `partition`, `unit` |

### User-Defined Metrics

Pipeline functions can publish their own metrics through the `metrics` object that is injected into
every Python function. These appear under the `ksml_user_defined_*` prefix.

| Property | Value |
|----------|-------|
| Metrics  | `ksml_user_defined_counter`, `ksml_user_defined_meter`, `ksml_user_defined_timer` |
| Type     | Counter, Meter, Timer |
| Unit     | Timer durations in milliseconds, meter and timer rates per second |
| Labels   | `custom_name` (the metric name passed in code), plus any custom tags supplied |

The `metrics` object exposes three factory methods, each with an optional tags map:

```python
# A counter that increments on every invocation
calls = metrics.counter("log_message-calls")
calls.increment()

# A meter that tracks the rate of processed records, tagged by format
rate = metrics.meter("records-in", {"format": "AVRO"})
rate.mark()

# A timer that records processing durations, tagged by format
duration = metrics.timer("received-records-processing-time", {"format": "AVRO"})
duration.updateMillis(42)
```

The `name` argument becomes the `custom_name` label and the tags map adds further labels, so the same
metric name can be reused with different tag combinations to produce distinct series.

### Application Information

`ksml_app` is a constant gauge (value `1`) that carries build and version information as labels.

| Property | Value |
|----------|-------|
| Metric   | `ksml_app` |
| Type     | Gauge |
| Labels   | `app_id`, `name`, `version`, `build_time` |

## Exported Series per Metric Type

A single counter, meter, timer or gauge is exported as several Prometheus series that share the metric
name as a prefix. For example, a timer named `ksml_execution_time` produces `ksml_execution_time_count`,
`ksml_execution_time_max`, `ksml_execution_time_50thpercentile`, and so on.

| Type    | Exported series |
|---------|-----------------|
| Counter | `_count` |
| Gauge   | `_value` (and `_number`) |
| Meter   | `_count`, `_meanrate`, `_oneminuterate`, `_fiveminuterate`, `_fifteenminuterate` |
| Timer   | `_count`, `_min`, `_max`, `_mean`, `_stddev`, `_meanrate`, `_oneminuterate`, `_fiveminuterate`, `_fifteenminuterate`, and the `_50thpercentile` through `_999thpercentile` quantiles |

## Other Exposed Metrics

The same endpoint also exposes standard metrics collected by the underlying libraries. These are not
defined by KSML and are documented by their respective projects, so they are listed here by category
rather than individually:

- Kafka Streams, producer, consumer and admin client metrics, under their original `kafka.*` names. These are described in the [Apache Kafka Monitoring documentation](https://kafka.apache.org/documentation/#monitoring).
- JVM metrics (memory, garbage collection, threads) under the `jvm_` prefix.
- Operating system metrics (memory and CPU) under the `os_` prefix.
- Exporter operational metrics such as `jmx_scrape_duration_seconds` and `jmx_config_reload_success_total`.

## Example Output

The following is a trimmed scrape of the metrics endpoint while a pipeline is processing records.

??? info "Example /metrics output (click to expand)"

    ```
    # HELP ksml_execution_time_count Execution time statistics of a KSML user function per invocation; durations are in milliseconds and rates are per second
    # TYPE ksml_execution_time_count gauge
    ksml_execution_time_count{function_name="pipelines_consume_avro_forEach",function_type="forEach",namespace="inspect",operation_name="inspect_pipelines_consume_avro_forEach",pipeline="consume_avro"} 18.0

    # HELP ksml_record_e2e_latency_avg_ms_value Average end-to-end latency of records from the source topic to this KSML processor node, in milliseconds
    # TYPE ksml_record_e2e_latency_avg_ms_value gauge
    ksml_record_e2e_latency_avg_ms_value{namespace="inspect",operation_name="peek",pipeline="consume_avro",processor_node_id="...",task_id="0_0",unit="ms"} 12.5

    # HELP ksml_user_defined_counter_count User-defined counter metric registered from KSML user code
    # TYPE ksml_user_defined_counter_count counter
    ksml_user_defined_counter_count{custom_name="received-records",format="AVRO"} 18.0
    ```

## Related Documentation

- [Configuration Reference](configuration-reference.md) for the `prometheus` configuration options.
- [Logging and Monitoring](../tutorials/beginner/logging-monitoring.md) for logging and peek-based monitoring.
- [Function Reference](function-reference.md) for writing the Python functions that publish user-defined metrics.
