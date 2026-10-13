# Fraud Detection with KSML

This guide demonstrates how to build a real-time fraud detection system using KSML. You'll learn how to analyze transaction streams, identify suspicious patterns, and generate alerts for potential fraud.

## Introduction

Fraud detection is a critical application of stream processing, allowing organizations to identify and respond to fraudulent activities in real-time. Key benefits include:

- Minimizing financial losses by detecting fraud as it happens
- Reducing false positives through multi-factor analysis
- Adapting to evolving fraud patterns with flexible detection rules
- Providing immediate alerts to security teams and customers

KSML provides powerful capabilities for building sophisticated fraud detection systems that can process high volumes of transactions and apply complex detection algorithms in real-time.

## Prerequisites

Before starting this guide, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../getting-started/basics-tutorial.md)
- Be familiar with [Aggregations](../tutorials/intermediate/aggregations.md)
- Have a basic understanding of [Windowed Operations](../tutorials/intermediate/windowing.md)
- Be familiar with [State Stores](../tutorials/intermediate/state-stores.md)

## The Use Case

Imagine you're building a fraud detection system for a financial institution that processes millions of credit card transactions daily. You want to:

1. Identify suspicious transactions based on multiple risk factors
2. Track unusual patterns in customer behavior
3. Generate real-time alerts for high-risk activities
4. Maintain a low rate of false positives

## Define the topics for the use case

In earlier tutorials, you created a Docker Compose file with all the necessary containers. For this use case guide, some other topics
are needed.
To have these created, open the `docker-compose.yml` in the examples directory, and find the definitions for the `kafka-setup` container
which creates the topics.
<br>
Change the definition so that the startup command for the setup container (the `command` section) looks like the following:

??? info "`command` section for the kafka-setup container (click to expand)"

    ```yaml
    command: "bash -c 'echo Creating topics... && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic credit_card_transactions && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic high_value_transactions && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic unusual_location_alerts && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic transaction_velocity_alerts && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic transaction_pattern_alerts && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic fraud_alerts && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic fraud_notifications'"
    ```

## Defining the Data Model

Our transaction data will have the following structure:

```json
{
  "transaction_id": "tx-123456",
  "timestamp": 1625097600000,
  "card_id": "card-789",
  "customer_id": "cust-456",
  "merchant": {
    "id": "merch-123",
    "name": "Online Electronics Store",
    "category": "electronics",
    "location": {
      "country": "US",
      "state": "CA",
      "city": "San Francisco"
    }
  },
  "amount": 299.99,
  "currency": "USD",
  "transaction_type": "online",
  "ip_address": "203.0.113.45"
}
```

## Creating the KSML Definition

Now, let's create our KSML definition file. This defines operations that check for transactions coming from unusual
locations, coming at an unusual speed, or are for a high amount:

??? info "Basic fraud detection pipeline (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/fraud-detection/fraud-detection.yaml" %}
    ```


The definition runs three detection pipelines on every transaction. Each one writes its alerts to its own topic. Three scoring pipelines then read those topics, calculate a final risk score, and write all alerts to `fraud_alerts`. The pattern recognition, machine learning and alerting definitions further down this page also use `fraud_alerts`:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Input
    Tx[(credit_card_transactions)]

    %% Detection
    HV["<b>high_value_detection</b><br/>amount above category threshold"]
    Loc["<b>unusual_location_detection</b><br/>new country, or new state<br/>within 2 hours"]
    Vel["<b>velocity_monitoring</b><br/>more than 5 transactions<br/>in 1 hour"]
    LocStore[("card_location_history")]
    VelStore[("customer_transaction_history")]

    %% Alert topics
    HVT[(high_value_transactions)]
    LocT[(unusual_location_alerts)]
    VelT[(transaction_velocity_alerts)]

    %% Scoring
    Score["<b>fraud_scoring_*</b><br/>3 pipelines with<br/>calculate_fraud_score"]
    Alerts[(fraud_alerts)]

    %% Other definitions on this page
    Pattern["Pattern recognition"]
    ML["Machine learning"]
    Notify["Real-time alerting"]
    Notifications[(fraud_notifications)]

    %% Data Flow
    Tx --> HV & Loc & Vel
    Loc <-.-> LocStore
    Vel <-.-> VelStore
    HV --> HVT
    Loc --> LocT
    Vel --> VelT
    HVT & LocT & VelT --> Score
    Score --> Alerts
    Tx -.-> Pattern & ML
    Pattern -.-> Alerts
    ML -.-> Alerts
    Alerts -.-> Notify
    Notify -.-> Notifications

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Tx,HVT,LocT,VelT,Alerts,Notifications topic
    class HV,Loc,Vel,Score operation
    class LocStore,VelStore store
    class Pattern,ML,Notify partial
```

The orange boxes are the other definitions on this page.

The diagram below follows an example card (not from the test data) through the basic definition. The card first pays 299.99 in a store in California; 30 minutes later, it pays 1,499.00 in a store in New York:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Topic[(credit_card_transactions)]
    Events@{ shape: docs, label: "<b>card-789</b>, in store<br/>10:00, 299.99, electronics, US / CA<br/>10:30, 1,499.00, electronics, US / NY" }

    %% Detection of the second transaction
    subgraph HV["high_value_detection"]
        direction TB
        HV1["electronics threshold: 1,000<br/>1,499.00 is higher"]
        HV2["alert, risk_score 100<br/>(amount / 10, at most 100)"]
        HV1 --> HV2
    end
    subgraph LOC["unusual_location_detection"]
        direction TB
        L1["history: US, CA at 10:00<br/>new state NY, 30 minutes later"]
        L2["alert, risk_score 40<br/>(70 for a new country)"]
        L1 --> L2
    end
    subgraph VEL["velocity_monitoring"]
        direction TB
        V1["history: 2 transactions<br/>in the last hour"]
        V2["no alert<br/>(more than 5 needed)"]
        V1 --> V2
    end

    %% Scoring
    Score["<code>calculate_fraud_score</code><br/>+10 for online transactions<br/>+15 for high-risk categories<br/>at most 100"]
    R1@{ shape: doc, label: "<b>high_value_transaction</b><br/>final_risk_score 100<br/>is_likely_fraud: true" }
    R2@{ shape: doc, label: "<b>unusual_location</b><br/>final_risk_score 55<br/>is_likely_fraud: false" }
    Alerts[(fraud_alerts)]

    %% Data Flow
    Topic --> Events
    Events -->|"10:30 transaction"| HV & LOC & VEL
    HV --> Score
    LOC --> Score
    Score --> R1 & R2
    R1 --> Alerts
    R2 --> Alerts

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Alerts topic
    class Score operation
    class Events,HV1,L1,V1 message
    class HV2,L2 partial
    class V2 dropped
    class R1 result
    class R2 partial
    style HV fill:none,stroke:#616161,stroke-dasharray:5 5
    style LOC fill:none,stroke:#616161,stroke-dasharray:5 5
    style VEL fill:none,stroke:#616161,stroke-dasharray:5 5
```

The first transaction only fills the location and velocity history, so it doesn't create any alert. The scoring step adds 15 points to both alerts because electronics is a high-risk category: the high-value alert stays at the maximum of 100, and the location alert goes from 40 to 55. Both transactions are made in a store, so the 10 extra points for online transactions don't apply.

## Advanced Fraud Detection Techniques

### Pattern Recognition

To detect complex fraud patterns, you can implement more sophisticated algorithms:

??? info "Pattern recognition processor (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/fraud-detection/fraud-pattern-detection.yaml" %}
    ```


The pattern detector looks for a "test then charge" pattern: a small transaction of less than 5, followed within 30 minutes by a transaction of more than 100. Fraudsters often test a stolen card with a small amount first. The diagram below follows three transactions of one card:

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph LR
    %% Transactions of one card
    T1["<b>09:00</b><br/>45.00"]
    T2["<b>10:00</b><br/>1.00<br/>small: below 5"]
    T3["<b>10:10</b><br/>450.00<br/>large: above 100"]
    Alert["<b>pattern alert</b><br/>test_then_charge<br/>risk_score 85"]
    Alerts[(fraud_alerts)]

    %% Data Flow
    T1 --> T2
    T2 -->|"10 minutes later"| T3
    T3 --> Alert
    Alert -->|"via transaction_pattern_alerts<br/>and scoring"| Alerts

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class T1 message
    class T2 partial
    class T3 alert
    class Alert result
    class Alerts topic
```

The card's recent transactions (last 24 hours) are kept in the `card_pattern_history` store. The pattern alert keeps the merchant and transaction type, so the scoring step can add points for online and high-risk transactions.

### Machine Learning Integration

For more advanced fraud detection, you can integrate machine learning models:

??? info "Machine learning fraud detector (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/fraud-detection/fraud-detection-machine-learning.yaml" %}
    ```


The example replaces a real model with simple rules: every risk feature adds points, and a transaction with more than 60 points is written to `fraud_alerts`. The diagram below scores transaction `tx-000014` from the test data further down this page:

<div class="diagram-w45" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Tx@{ shape: doc, label: "<b>tx-000014</b><br/>1,899.00, jewelry, Germany<br/>online, 23:15 UTC" }

    %% Features
    Features["<code>ml_fraud_prediction</code><br/>amount above 1,000: +20<br/>outside the US: +15<br/>online: +10<br/>high-risk category: +15<br/>between 22:00 and 06:00: +10"]

    %% Result
    Score["ml_fraud_score 70<br/>ml_fraud_probability 0.7"]
    Filter{"<code>filter</code><br/>ml_is_fraud<br/>score above 60"}
    Alerts[(fraud_alerts)]

    %% Data Flow
    Tx --> Features
    Features --> Score
    Score --> Filter
    Filter -->|"70 > 60"| Alerts

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Tx message
    class Features operation
    class Score result
    class Filter operation
    class Alerts topic
```

</div>

The hour of the day is calculated in UTC from the `timestamp` field. The function also sets `alert_type` and `final_risk_score` (equal to the model score), so the alerting pipeline below turns these alerts into notifications too.

## Real-time Alerting

To make your fraud detection system actionable, you need to generate alerts:

??? info "Real-time alerting processor (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/fraud-detection/fraud-notifications.yaml" %}
    ```


The alerting pipeline turns every alert with a `final_risk_score` above 30 into a notification, with an alert level and a recommended action:

<div class="diagram-w70" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input
    Alerts[(fraud_alerts)]
    Filter{"<code>filter</code><br/>final_risk_score above 30"}
    Drop["❌ Dropped<br/>score 30 or lower"]
    Gen["<code>transformValue</code><br/>generate_notification"]

    %% Levels
    High["score above 70<br/><b>high</b>: block"]
    Medium["score above 40, up to 70<br/><b>medium</b>: review"]
    Low["score above 30, up to 40<br/><b>low</b>: review"]
    Out[(fraud_notifications)]

    %% Data Flow
    Alerts --> Filter
    Filter -->|"above 30"| Gen
    Filter -.-> Drop
    Gen --> High & Medium & Low
    High --> Out
    Medium --> Out
    Low --> Out

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Alerts,Out topic
    class Filter,Gen operation
    class High alert
    class Medium,Low partial
    class Drop message
```

</div>

In the basic definition walkthrough above, the high-value alert (score 100) becomes a `high` notification with the action `block`, and the location alert (score 55) becomes a `medium` notification with the action `review`.

## Testing and Validation

To test your fraud detection system, generate sample transactions with known fraud patterns, monitor the alert topics, and adjust thresholds and rules to balance the detection rate and false positives.

The following producer writes 14 transactions once, one per second, and then stops. Each card except `card-001` triggers a detection rule. The `timestamp` fields start at 23:00 UTC, so the transactions also fall in the night hours used by the machine learning example:

??? info "Fraud test data producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/fraud-detection/fraud-transaction-producer.yaml" %}
    ```

To run the producer and all four definitions together:

1. Create the topics as described in [Define the topics for the use case](#define-the-topics-for-the-use-case).
2. Save the definitions from this page as `fraud-detection.yaml`, `fraud-pattern-detection.yaml`, `fraud-detection-machine-learning.yaml` and `fraud-notifications.yaml`, and the producer as `fraud-transaction-producer.yaml`, in your `examples` directory.
3. Set up your `ksml-runner.yaml` configuration to load all five files (see the [Configuration Reference](../reference/configuration-reference.md) for all options):

    ??? info "KSML runner configuration (click to expand)"

        ```yaml
        {% include "../../ksml/src/test/resources/docs-examples/use-cases/fraud-detection/ksml-runner.yaml" %}
        ```

4. Start the Docker Compose environment: `docker compose up -d`
5. Monitor the `fraud_alerts` and `fraud_notifications` topics.

With this test data, `fraud_alerts` and `fraud_notifications` receive one message for each of these transactions:

| Transaction | Card | Rule | Final risk score | Notification |
|---|---|---|---|---|
| tx-000003 | card-002 | unusual location: new state within 2 hours | 40 | low, review |
| tx-000004 | card-002 | unusual location: new country, online | 80 | high, block |
| tx-000005 | card-003 | high value: 1,499.00 for electronics, online | 100 | high, block |
| tx-000007 | card-004 | pattern: 1.00 followed by 450.00 within 30 minutes | 100 | high, block |
| tx-000013 | card-005 | velocity: 6 transactions within 1 hour | 60 | medium, review |
| tx-000014 | card-006 | machine learning: score 70 | 70 | medium, review |

The other transactions don't create an alert: they are the first transaction of a card, or the earlier transactions that the rules compare with.

## Production Considerations

When deploying fraud detection systems to production:

1. **Performance**: Ensure your system can handle peak transaction volumes
2. **Latency**: Minimize processing time to detect fraud quickly
3. **False Positives**: Continuously tune your system to reduce false alarms
4. **Adaptability**: Implement mechanisms to update rules and models as fraud patterns evolve
5. **Compliance**: Ensure your system meets regulatory requirements for financial monitoring
6. **Security**: Protect sensitive transaction data with appropriate encryption and access controls

## Conclusion

KSML provides a powerful platform for building sophisticated fraud detection systems that can process high volumes of transactions in real-time. By combining multiple detection techniques, including pattern recognition, anomaly detection, and machine learning, you can create a robust system that effectively identifies fraudulent activities while minimizing false positives.

For more advanced fraud detection scenarios, explore:

- [Complex Event Processing](../tutorials/advanced/complex-event-processing.md) for detecting multi-stage fraud patterns
- [External Service Integration](../tutorials/advanced/external-integration.md) for incorporating third-party risk scores
- [KSML Definition Reference](../reference/definition-reference.md) for a full explanation of the KSML definition syntax
