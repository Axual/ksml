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
- Conversion rates from different marketing channels

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

## Running the Application

You can use the following producer pipeline example as a starting point to write some simulated sale data; it will write
four hard coded sales records into the input stream and exit.

??? info "Transaction data producer (click to expand)"

    ```yaml
    {% include "../../ksml/src/test/resources/docs-examples/use-cases/real-time-analytics/example-producer.yaml" %}
    ```

To run the application:

1. Save the KSML definition to `definitions/analytics.yaml`
2. Add the `example-data-producer.yaml` above if desired, or create test data in some other way
3. Create a configuration file at `config/application.properties` with your Kafka connection details
4. Start the Docker Compose environment: `docker-compose up -d`
5. Monitor the output topics to see the real-time analytics results

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
   - Gauge charts for conversion rates

## Extending the Application

You can extend this application in several ways:

- Add anomaly detection to identify unusual patterns
- Implement trend analysis to detect changing consumer behavior
- Create alerts for specific conditions (e.g., sales dropping below a threshold)
- Enrich the data with additional information from reference data sources

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