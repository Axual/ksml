# Real-Time Analytics with KSML

This tutorial demonstrates how to build a real-time analytics application using KSML. You'll learn how to process streaming data, calculate metrics in real-time, and visualize the results.

## Introduction

Real-time analytics is one of the most common use cases for stream processing. By analyzing data as it arrives, you can:

- Detect trends and patterns as they emerge
- Respond quickly to changing conditions
- Make data-driven decisions with minimal latency
- Provide up-to-date dashboards and visualizations

In this tutorial, we'll build a real-time analytics pipeline that processes a stream of e-commerce transactions, calculates various metrics, and makes the results available for visualization.

## Prerequisites

Before starting this tutorial, you should:

- Understand basic KSML concepts (streams, functions, pipelines)
- Have completed the [KSML Basics Tutorial](../getting-started/basics-tutorial.md)
- Be familiar with [Aggregations](../tutorials/intermediate/aggregations.md)
- Have a basic understanding of [Windowed Operations](../tutorials/intermediate/windowing.md)

## The Use Case

Imagine you're running an e-commerce platform and want to analyze transaction data in real-time. You want to track:

- Total sales by product category
- Average order value
- Transaction volume by region

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
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic ecommerce_transactions && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic sales_by_category && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic avg_order_value && \
                           kafka-topics.sh --create --if-not-exists --bootstrap-server broker:9093 --partitions 1 --replication-factor 1 --topic transactions_by_region'"
    ```

## Defining the Data Model

Our transaction data will have the following structure:

```json
{
  "transaction_id": "12345",
  "timestamp": 1625097600000,
  "customer_id": "cust-789",
  "product_id": "prod-456",
  "product_category": "electronics",
  "quantity": 1,
  "price": 499.99,
  "region": "north_america",
  "marketing_channel": "social_media"
}
```

## Creating the KSML Definition

Now, let's create our KSML definition file:

??? info "Real-time analytics processor (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/real-time-analytics/analytics.yaml" %}
    ```

The definition runs three independent pipelines on the same `ecommerce_transactions` topic. Each pipeline chooses its
own grouping key and calculates one metric. The diagram below follows the four transactions from the example producer
(further down this page):

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Input topic with example transactions
    Topic[(ecommerce_transactions)]
    Events@{ shape: docs, label: "<b>12345</b>: electronics<br/>499.99, north_america<br/><b>12346</b>: gardening<br/>229.99, europe<br/><b>12349</b>: electronics<br/>229.99, europe<br/><b>12347</b>: gardening<br/>12.99, north_america" }

    %% Sales by category
    subgraph C["sales_by_category_pipeline"]
        direction TB
        CKey["<code>selectKey</code><br/>key: product_category"]
        CAgg["<code>groupByKey</code> + <code>aggregate</code><br/>total_sales, count"]
        CResult@{ shape: docs, label: "<b>electronics</b><br/>499.99, count 1<br/>729.98, count 2<br/><b>gardening</b><br/>229.99, count 1<br/>242.98, count 2" }
        CKey --> CAgg --> CResult
    end

    %% Average order value
    subgraph A["avg_order_value_pipeline"]
        direction TB
        AKey["<code>groupBy</code><br/>key: all"]
        AWin["<code>windowByTime</code><br/>tumbling, 10 seconds"]
        AAgg["<code>aggregate</code> + <code>suppress</code><br/>one result per closed window"]
        AAvg["<code>mapValues</code><br/>total_sales / count"]
        AResult@{ shape: docs, label: "<b>key: all + window</b><br/>avg_order_value<br/>per 10-second window" }
        AKey --> AWin --> AAgg --> AAvg --> AResult
    end

    %% Transactions by region
    subgraph R["transactions_by_region_pipeline"]
        direction TB
        RKey["<code>selectKey</code><br/>key: region"]
        RCount["<code>groupByKey</code> + <code>count</code>"]
        RResult@{ shape: docs, label: "<b>europe</b>: 2<br/><b>north_america</b>: 2" }
        RKey --> RCount --> RResult
    end

    %% Outputs
    Sales[(sales_by_category)]
    Avg[(avg_order_value)]
    Region[(transactions_by_region)]

    %% Data Flow
    Topic --> Events
    Events --> C & A & R
    C --> Sales
    A --> Avg
    R --> Region

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class Topic,Sales,Avg,Region topic
    class CKey,CAgg,AKey,AWin,AAgg,AAvg,RKey,RCount operation
    class Events message
    class CResult,AResult,RResult result
    style C fill:none,stroke:#616161,stroke-dasharray:5 5
    style A fill:none,stroke:#616161,stroke-dasharray:5 5
    style R fill:none,stroke:#616161,stroke-dasharray:5 5
```

The category and region pipelines update their result with every transaction, so the output topics receive running
totals. Kafka Streams buffers these updates for a short time, so updates that follow each other quickly can be
combined: in a test run, `sales_by_category` received all four updates, while `transactions_by_region` only received
the final count of 2 for each region. The counts are written as strings (`"2"`), because the pipeline converts them
with `convertValue`.

The average order value pipeline works differently: `suppress` holds back the result of a window until the window has
closed, so `avg_order_value` receives one final result per 10-second window.

The windows are based on the time the transactions are written to Kafka, not on the `timestamp` field in the data, and
they start at whole multiples of 10 seconds. The example producer writes a transaction every 7 seconds, so a window
contains one or two transactions. Which transactions share a window depends on when the producer starts. The diagram
below shows the windows from one test run:

<div class="diagram-w80" markdown>

```mermaid
--8<-- "snippets/mermaid-init.txt"
graph TB
    %% Windows
    subgraph W3["Window 13:58:00–13:58:10"]
        direction TB
        T4["<b>13:58:06</b>: 12347<br/>12.99"]
    end
    subgraph W2["Window 13:57:50–13:58:00"]
        direction TB
        T2["<b>13:57:52</b>: 12346<br/>229.99"]
        T3["<b>13:57:59</b>: 12349<br/>229.99"]
        T2 ~~~ T3
    end
    subgraph W1["Window 13:57:40–13:57:50"]
        direction TB
        T1["<b>13:57:45</b>: 12345<br/>499.99"]
    end

    %% Results
    R1["avg_order_value<br/>499.99"]
    R2["avg_order_value<br/>229.99"]
    R3["no result:<br/>no later transaction<br/>arrives to close<br/>the window"]

    %% Data Flow
    W3 -.-> R3
    W2 --> R2
    W1 --> R1

    %% Styling
    --8<-- "snippets/mermaid-classes.txt"

    class T1,T2,T3,T4 message
    class R1,R2 result
    class R3 partial
    style W1 fill:none,stroke:#616161,stroke-dasharray:5 5
    style W2 fill:none,stroke:#616161,stroke-dasharray:5 5
    style W3 fill:none,stroke:#616161,stroke-dasharray:5 5
```

</div>

A window only closes when a transaction with a later time arrives. The example producer stops after four transactions,
so the last window never produces a result.

## Running the Application

You can use the following producer pipeline example as a starting point to write some simulated sale data. It writes
four hard-coded transactions to the input topic, one every 7 seconds, using the `transaction_id` as the message key, and
then stops.

??? info "Transaction data producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/real-time-analytics/example-producer.yaml" %}
    ```

To run the application:

1. Create the topics as described in [Define the topics for the use case](#define-the-topics-for-the-use-case).
2. Save the processor definition to `analytics.yaml` and the producer to `example-producer.yaml` in your `examples`
   directory.
3. Set up your `ksml-runner.yaml` configuration to load both definitions:

    ??? info "KSML runner configuration (click to expand)"

        ```yaml
        {% include "../../ksml/src/test/resources/docs-examples/use-cases/real-time-analytics/ksml-runner.yaml" %}
        ```

4. Start the Docker Compose environment: `docker compose up -d`
5. Monitor the `sales_by_category`, `avg_order_value` and `transactions_by_region` topics to see the results.

## Visualizing the Results

Setting up visualizations is outside the scope of this tutorial. You can connect the output topics to visualization tools like:

- Grafana
- Kibana
- Custom dashboards using web frameworks

For example, to create a simple dashboard with Grafana:

1. Set up Grafana to connect to your Kafka topics
2. Create dashboards for each metric:
    - Bar chart for sales by category
    - Line chart for average order value over time
    - Map visualization for transactions by region

## Extending the Application

You can extend this application in several ways:

- Add anomaly detection to identify unusual patterns
- Implement trend analysis to detect changing consumer behavior
- Create alerts for specific conditions (e.g., sales dropping below a threshold)
- Enrich the data with additional information from reference data sources
- Calculate conversion rates per marketing channel, using the `marketing_channel` field in the transactions

## Conclusion

In this tutorial, you've learned how to:

- Process streaming transaction data in real-time
- Calculate various business metrics using KSML
- Structure a real-time analytics application
- Prepare the results for visualization

Real-time analytics is a powerful use case for KSML, allowing you to gain immediate insights from your streaming data without complex coding.

## Next Steps

- Learn about [Data Transformation](data-transformation.md) for more complex processing
- Explore [Event-Driven Applications](event-driven-applications.md) to trigger actions based on analytics
- Check out [Performance Optimization](../tutorials/advanced/performance-optimization.md) for handling high-volume data