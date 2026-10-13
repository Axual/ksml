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

Most examples in this tutorial share the same pipeline shape. A Python function remembers what it has seen so far in a state store, and returns a result only when it recognizes a pattern. For all other events it returns `None`, and a `filter` drops those:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Input[(Input topic)]

    %% Pipeline Operations
    Detect["<code>mapValues</code><br/>detection function in Python"]
    Store[("State store<br/>what was seen so far, per key")]
    Filter{"<code>filter</code><br/>value is not None"}

    %% Outputs
    Output[(Output topic)]
    Drop[❌ Dropped]

    %% Data Flow
    Input --> Detect
    Detect <-.->|"read & update"| Store
    Detect -->|"result or None"| Filter
    Filter -->|"pattern found"| Output
    Filter -.->|"None"| Drop

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Input,Output topic
    class Detect,Filter operation
    class Store store
    class Drop dropped
```

The sections below show, for each example, what the detection function keeps in its state store and when it returns a result.

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

The diagram below follows an example sequence of six events for the same session through the pipeline (the producer sends its own sequence). The `pattern_tracker` store keeps the progress for each key (session), and every event either moves the pattern forward, starts it over, or breaks it:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with six events for one session
    Topic[(pattern_events)]
    Events@{ shape: docs, label: "<b>key: session_001</b> (all six events)<br/>A, B, D, A, B, C" }

    %% detect_abc_pattern, one event at a time
    Detect["<code>mapValues</code><br/>detect_abc_pattern"]
    subgraph Steps["Event by event, with the state stored in pattern_tracker"]
        direction LR
        Step1["<b>A</b><br/>state: none → A<br/>returns None"]
        Step2["<b>B</b><br/>state: A → AB<br/>returns None"]
        Step3["<b>D</b><br/>sequence broken<br/>state: AB → deleted<br/>returns None"]
        Step4["<b>A</b><br/>state: none → A<br/>returns None"]
        Step5["<b>B</b><br/>state: A → AB<br/>returns None"]
        Step6["<b>C</b><br/>A → B → C complete<br/>state: AB → deleted<br/>returns ABC_SEQUENCE"]
        Step1 --> Step2 --> Step3 --> Step4 --> Step5 --> Step6
    end

    %% Filter and output
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(detected_patterns)]
    Drop["❌ Dropped<br/>events A, B, D, A, B"]

    %% Data Flow
    Topic --> Events
    Events --> Detect
    Detect --> Steps
    Steps --> Filter
    Filter -->|"key: session_001<br/>value: {pattern_type: ABC_SEQUENCE, status: DETECTED,<br/>completing_event: {event_type: C, ...}, ...}"| Output
    Filter -.->|"None"| Drop

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Output topic
    class Detect,Filter operation
    class Events,Step1,Step2,Step4,Step5 message
    class Step3 partial
    class Step6 result
    class Drop dropped
    style Steps fill:none,stroke:#616161,stroke-dasharray:5 5
```

Out of six events, only the `C` that completes `A → B → C` produces an output message. Event `D` breaks the first attempt, so the pattern has to start again from `A`.

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

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Incoming event
    Event[/"Event for a user"/]
    Action{"action?"}

    %% Cart handling
    SaveCart["Store cart event<br/>with its timestamp<br/>in temporal_state"]
    NoOutput1["No output"]

    %% Checkout handling
    HasCart{"Cart event<br/>stored?"}
    TimeCheck{"checkout - cart<br/>≤ 5 minutes?"}
    Quick["QUICK_CHECKOUT"]
    Slow["SLOW_CHECKOUT"]
    NoOutput2["No output"]

    %% Data Flow
    Event --> Action
    Action -->|"add_to_cart"| SaveCart
    SaveCart --> NoOutput1
    Action -->|"checkout"| HasCart
    HasCart -->|"yes"| TimeCheck
    HasCart -.->|"no"| NoOutput2
    TimeCheck -->|"yes"| Quick
    TimeCheck -->|"no"| Slow

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Event message
    class Action,HasCart,TimeCheck,SaveCart operation
    class Quick external
    class Slow partial
    class NoOutput1,NoOutput2 dropped
```

In both cases the stored cart event is deleted after the checkout, so the next checkout needs a new `add_to_cart`.

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

Unlike the other examples, this pipeline uses a join instead of its own state store. The `user_events` topic is read as a table, so for each user it always holds the latest user activity. The diagram below follows three example system events through the pipeline:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% User activity, read as a table
    UserTopic[(user_events)]
    UserEvents@{ shape: docs, label: "<b>key: alice</b> form_submit at 10:00:00<br/><b>key: bob</b> page_view at 10:00:01<br/><b>key: charlie</b> scroll at 10:00:02" }
    UserContext@{ shape: docs, label: "<b>Table user_context</b><br/>latest activity per user<br/><b>alice</b>: form_submit<br/><b>bob</b>: page_view<br/><b>charlie</b>: scroll" }

    %% System events
    SystemTopic[(system_events)]
    SystemEvents@{ shape: docs, label: "<b>key: alice</b> error at 10:00:03<br/><b>key: bob</b> db_query at 10:00:04<br/><b>key: charlie</b> api_call at 10:00:05" }

    %% Step 1: join each system event with the latest user activity
    Join["<code>leftJoin</code> with table<br/>join_events"]
    subgraph Joined["System event + latest user activity"]
        JoinAlice["<b>alice</b><br/>error + form_submit"]
        JoinBob["<b>bob</b><br/>db_query + page_view"]
        JoinCharlie["<b>charlie</b><br/>api_call + scroll"]
    end

    %% Step 2: look for known combinations
    Correlate["<code>mapValues</code><br/>correlate_events"]
    subgraph Results["Correlation results"]
        ResultAlice["<b>alice</b><br/>FORM_ERROR_CORRELATION<br/>status CRITICAL<br/>3s after activity: HIGH"]
        ResultBob["<b>bob</b><br/>PAGE_LOAD_CORRELATION<br/>status NORMAL<br/>3s after activity: MEDIUM"]
        ResultCharlie["<b>charlie</b><br/>scroll + api_call is not<br/>a known combination:<br/>returns None"]
    end

    %% Step 3: drop the None results
    Filter{"<code>filter</code><br/>value is not None"}
    Output[(correlated_events)]
    Drop[❌ Dropped]

    %% Data Flow
    UserTopic --> UserEvents
    UserEvents --> UserContext
    SystemTopic --> SystemEvents
    SystemEvents --> Join
    UserContext -.->|"lookup by user"| Join
    Join --> JoinAlice & JoinBob & JoinCharlie
    JoinAlice & JoinBob & JoinCharlie --> Correlate
    Correlate --> ResultAlice & ResultBob & ResultCharlie
    ResultAlice & ResultBob --> Filter
    ResultCharlie -.->|"None: dropped by the filter"| Drop
    Filter -->|"key: alice<br/>value: {pattern_type: FORM_ERROR_CORRELATION, status: CRITICAL, ...}"| Output

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class UserTopic,SystemTopic,Output topic
    class Join,Correlate,Filter operation
    class UserEvents,UserContext,SystemEvents,JoinAlice,JoinBob,JoinCharlie message
    class ResultAlice,ResultBob result
    class ResultCharlie,Drop dropped
    style Joined fill:none,stroke:#616161,stroke-dasharray:5 5
    style Results fill:none,stroke:#616161,stroke-dasharray:5 5
```

The `correlate_events` function only reports three combinations of user activity and system event. The correlation strength depends on how soon the system event followed the user activity:

| User activity | System event | Pattern type             | Status   | HIGH strength if within |
|---------------|--------------|--------------------------|----------|-------------------------|
| `form_submit` | `error`      | `FORM_ERROR_CORRELATION` | CRITICAL | 5 seconds               |
| `page_view`   | `db_query`   | `PAGE_LOAD_CORRELATION`  | NORMAL   | 2 seconds               |
| `click`       | `api_call`   | `USER_API_CORRELATION`   | NORMAL   | 1 second                |

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
- **Calculates z-scores**: Once more than 10 readings are stored for a sensor, computes how many standard deviations each new reading is from the mean
- **Detects outliers**: When z-score > 3.0, flags as anomaly (spike if above mean, drop if below mean)
- **Outputs anomalies**: Only emits detection messages when statistical threshold is exceeded, showing z-score, mean, and severity level

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Incoming reading
    Reading[/"Sensor reading"/]
    Stats["Load running statistics for this sensor<br/>from stats_store: count, sum, sum of squares"]

    %% Detection
    Enough{"More than 10<br/>readings so far?"}
    ZScore["z = | value − mean | / standard deviation"]
    Outlier{"z > 3?"}
    Anomaly["Anomaly detected<br/>spike if above mean, drop if below<br/>severity HIGH if z > 5, else MEDIUM"]
    NoOutput["No output"]

    %% Update
    Update["Add this reading<br/>to the statistics"]

    %% Data Flow
    Reading --> Stats
    Stats --> Enough
    Enough -->|"yes"| ZScore
    Enough -.->|"no: still learning"| NoOutput
    ZScore --> Outlier
    Outlier -->|"yes"| Anomaly
    Outlier -.->|"no"| NoOutput
    Anomaly --> Update
    NoOutput --> Update

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Reading message
    class Stats,Enough,ZScore,Outlier,Update operation
    class Anomaly partial
    class NoOutput dropped
```

Every reading, including an anomaly, is added to the statistics afterwards, so the mean and standard deviation keep adapting to the sensor.

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
- **Classifies threats**: If score ≥50 = "FRAUD_ALERT", if 30-49 = "SUSPICIOUS_TRANSACTION", otherwise no output
- **Outputs alerts**: Only emits results when fraud score thresholds are met, showing detected patterns, risk factors, and recommended actions

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Incoming transaction
    Txn[/"Transaction for a card"/]
    History["Load last location and time<br/>from transaction_history"]

    %% Risk checks
    subgraph Checks["Risk checks, points are added up"]
        Amount["amount > 5000<br/>+40"]
        Rapid["less than 60s since<br/>previous transaction<br/>+30"]
        Location["different location<br/>within 1 hour<br/>+20"]
        Merchant["luxury_goods, online_gambling<br/>or crypto_exchange<br/>+20"]
    end

    %% Classification
    Score{"Total score"}
    Alert["FRAUD_ALERT<br/>HIGH_RISK"]
    Suspicious["SUSPICIOUS_TRANSACTION<br/>MEDIUM_RISK"]
    NoOutput["No output"]

    %% Data Flow
    Txn --> History
    History --> Amount & Rapid & Location & Merchant
    Amount & Rapid & Location & Merchant --> Score
    Score -->|"50 or more"| Alert
    Score -->|"30 to 49"| Suspicious
    Score -.->|"below 30"| NoOutput

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Txn message
    class History,Amount,Rapid,Location,Merchant,Score operation
    class Alert alert
    class Suspicious partial
    class NoOutput message
    style Checks fill:none,stroke:#616161,stroke-dasharray:5 5
```

After the checks, the transaction's location and time are stored as the new "last" values for the card, whatever the score.

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