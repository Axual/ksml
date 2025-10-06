# Complex Event Processing in KSML

This tutorial explores how to implement complex event processing (CEP) patterns in KSML, allowing you to detect meaningful patterns across multiple events and streams in real-time.

## Introduction to Complex Event Processing

Complex Event Processing (CEP) is a method of tracking and analyzing streams of data to identify patterns, correlate events, and derive higher-level insights. CEP enables real-time decision making by processing events as they occur rather than in batch.

Key capabilities of CEP in KSML:

- **Pattern detection**: Identify sequences of events that form meaningful patterns
- **Temporal analysis**: Detect time-based patterns and relationships
- **Event correlation**: Connect related events from different sources
- **Anomaly detection**: Identify unusual patterns or deviations
- **Stateful processing**: Maintain context across multiple events

## Prerequisites

Before starting this tutorial:

- Have [Docker Compose KSML environment setup running](../../getting-started/basics-tutorial.md#choose-your-setup-method)
- Add the following topics to your `kafka-setup` service in docker-compose.yml to run the examples:

??? info "Topic creation commands - click to expand"

    ```yaml
    # Pattern Detection
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic pattern_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic detected_patterns && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic temporal_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic temporal_patterns && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic user_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic system_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic correlated_events && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sensor_metrics && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic anomalies_detected && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic credit_card_transactions && \
    kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic fraud_alerts && \
    ```

## Pattern Detection

Pattern detection identifies specific sequences of events within a stream. This example detects an A→B→C pattern across events.

**What it does**:

- **Produces events**: Creates events with types A, B, C, D, E - deliberately generates A→B→C sequences for session_001 to demonstrate pattern completion
- **Tracks sequences**: Uses a state store to remember where each session is in the A→B→C pattern (stores "A", "AB", or deletes when complete)
- **Detects completion**: When event C arrives and the state shows "AB", it recognizes the full A→B→C pattern is complete
- **Outputs results**: Only emits a detection message when the complete pattern A→B→C is found, otherwise filters out partial matches
- **Resets state**: Clears the pattern tracking after successful detection or if the sequence breaks (e.g., gets A→D instead of A→B)

??? info "Pattern Events Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/producer-pattern-detection.yaml" %}
    ```

??? info "Pattern Detection Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/processor-pattern-detection.yaml" %}
    ```

**Key concepts demonstrated**:

- State store usage for tracking partial patterns
- Sequential event processing
- Pattern completion and reset logic

## Temporal Pattern Matching

Temporal patterns add time constraints to event sequences. This example detects quick checkout behavior (cart→checkout within 5 minutes).

**What it does**:

- **Produces shopping events**: Creates events like "add_to_cart" and "checkout" with realistic timestamps and shopping details
- **Stores cart events**: When "add_to_cart" happens, saves the cart timestamp and details in state store as JSON
- **Measures time gaps**: When "checkout" arrives, calculates milliseconds between cart and checkout events  
- **Classifies by speed**: If checkout happens within 5 minutes (300,000ms) = "QUICK_CHECKOUT", otherwise "SLOW_CHECKOUT"
- **Outputs results**: Only emits a message when both cart and checkout events are found, showing the time difference and classification

??? info "Temporal Events Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/producer-temporal-events.yaml" %}
    ```

??? info "Temporal Pattern Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/processor-temporal-pattern.yaml" %}
    ```

**Key concepts demonstrated**:

- Time-based pattern constraints
- Timestamp extraction and comparison
- Temporal window analysis

## Event Correlation

Event correlation combines related events from different streams to provide enriched context.

**What it does**:

- **Produces two event streams**: Creates user events (page_view, click, form_submit) and system events (api_call, db_query, error) with the same user IDs
- **Joins streams by user**: Uses leftJoin to connect system events with the latest user activity for each user ID
- **Detects specific patterns**: Looks for meaningful combinations like "form_submit + error", "page_view + db_query", or "click + api_call"
- **Measures timing**: Calculates milliseconds between user action and system response to determine correlation strength (HIGH/MEDIUM)
- **Outputs correlations**: Only emits results when it finds the specific patterns, showing both events with timing analysis and relationship details

??? info "Correlation Events Producer (generates both user and system events) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/producer-correlation-events.yaml" %}
    ```

??? info "Event Correlation Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/processor-event-correlation.yaml" %}
    ```

**Key concepts demonstrated**:

- Stream-table joins for context enrichment
- Cross-stream event correlation
- Cause-effect relationship detection

## Anomaly Detection

Anomaly detection identifies unusual patterns using statistical analysis.

**What it does**:

- **Produces sensor readings**: Creates temperature values that are normally 40-60°C, but every 20th reading is a spike (90-100°C) and every 25th is a drop (0-10°C)
- **Tracks statistics per sensor**: Stores running count, sum, sum-of-squares, min, max in state store to calculate mean and standard deviation
- **Calculates z-scores**: After 10+ readings, computes how many standard deviations each new reading is from the mean
- **Detects outliers**: When z-score > 3.0, flags as anomaly (spike if above mean, drop if below mean)
- **Outputs anomalies**: Only emits detection messages when statistical threshold is exceeded, showing z-score, mean, and severity level

??? info "Metrics Producer (with occasional anomalies) - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/producer-metrics.yaml" %}
    ```

??? info "Anomaly Detection Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/processor-anomaly-detection.yaml" %}
    ```

**Key concepts demonstrated**:

- Statistical anomaly detection (z-score)
- Running statistics calculation
- Threshold-based alerting

## Fraud Detection System

A practical example combining multiple CEP techniques for real-time fraud detection.

**What it does**:

- **Produces transactions**: Creates credit card purchases with amounts, merchants, locations - deliberately generates suspicious patterns (high amounts every 15th, rapid transactions every 20th)
- **Stores transaction history**: Keeps last location, timestamp, and totals for each card number in state store
- **Scores fraud risk**: Adds points for patterns: +40 for amounts >$5000, +30 for transactions <60s apart, +20 for location changes <1hr, +20 for suspicious merchants
- **Classifies threats**: If score ≥70 = "FRAUD_ALERT", if 30-69 = "SUSPICIOUS_TRANSACTION", otherwise no output
- **Outputs alerts**: Only emits results when fraud score thresholds are met, showing detected patterns, risk factors, and recommended actions

??? info "Credit Card Transactions Producer - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/producer-transactions.yaml" %}
    ```

??? info "Fraud Detection Processor - click to expand"

    ```yaml
    {% include "../../../ksml/src/test/resources/docs-examples/advanced-tutorial/complex-event-processing/processor-fraud-detection.yaml" %}
    ```

**Key concepts demonstrated**:

- Multi-factor pattern analysis
- Risk scoring algorithms
- Transaction velocity checks
- Geographic anomaly detection

## Conclusion

Complex Event Processing in KSML provides capabilities for real-time pattern detection and analysis. By combining state management, temporal operations, and correlation techniques, you can build sophisticated event processing applications that derive actionable insights from streaming data.