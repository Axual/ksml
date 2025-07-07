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
- Have completed the [KSML Basics Tutorial](../../getting-started/basics-tutorial.md)
- Be familiar with [Aggregations](../intermediate/aggregations.md)
- Have a basic understanding of [Windowed Operations](../intermediate/windowed-operations.md)

## The Use Case

Imagine you're running an e-commerce platform and want to analyze transaction data in real-time. You want to track:

- Total sales by product category
- Average order value
- Transaction volume by region
- Conversion rates from different marketing channels

## Setting Up the Environment

First, let's set up our environment with Docker Compose:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      
  ksml-runner:
    image: axual/ksml-runner:latest
    depends_on:
      - kafka
    volumes:
      - ./config:/config
      - ./definitions:/definitions
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

```yaml
streams:
  transactions:
    topic: ecommerce_transactions
    keyType: string  # transaction_id
    valueType: json  # transaction data
    
  sales_by_category:
    topic: sales_by_category
    keyType: string  # product_category
    valueType: json  # aggregated sales data
    
  avg_order_value:
    topic: avg_order_value
    keyType: string  # time window
    valueType: json  # average order value
    
  transactions_by_region:
    topic: transactions_by_region
    keyType: string  # region
    valueType: json  # transaction count
    
  conversion_by_channel:
    topic: conversion_by_channel
    keyType: string  # marketing_channel
    valueType: json  # conversion metrics

functions:
  extract_category:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      return value.get("product_category")
      
  calculate_total:
    type: aggregate
    parameters:
      - name: value
        type: object
      - name: aggregate
        type: object
    code: |
      if aggregate is None:
        return {"total_sales": value.get("price") * value.get("quantity"), "count": 1}
      else:
        return {
          "total_sales": aggregate.get("total_sales") + (value.get("price") * value.get("quantity")),
          "count": aggregate.get("count") + 1
        }

pipelines:
  # Pipeline for sales by category
  sales_by_category_pipeline:
    from: transactions
    via:
      - type: selectKey
        keySelector:
          expression: value.get("product_category")
      - type: groupByKey
      - type: aggregate
        initializer:
          expression: {"total_sales": 0, "count": 0}
        aggregator:
          code: calculate_total(value, aggregate)
    to: sales_by_category
    
  # Pipeline for average order value (windowed)
  avg_order_value_pipeline:
    from: transactions
    via:
      - type: groupByKey
      - type: windowedBy
        timeDifference: 60000  # 1 minute window
      - type: aggregate
        initializer:
          expression: {"total_sales": 0, "count": 0}
        aggregator:
          code: calculate_total(value, aggregate)
      - type: mapValues
        mapper:
          expression: {"avg_order_value": aggregate.get("total_sales") / aggregate.get("count")}
    to: avg_order_value
    
  # Pipeline for transactions by region
  transactions_by_region_pipeline:
    from: transactions
    via:
      - type: selectKey
        keySelector:
          expression: value.get("region")
      - type: groupByKey
      - type: count
    to: transactions_by_region
    
  # Pipeline for conversion by marketing channel
  conversion_by_channel_pipeline:
    from: transactions
    via:
      - type: selectKey
        keySelector:
          expression: value.get("marketing_channel")
      - type: groupByKey
      - type: aggregate
        initializer:
          expression: {"views": 0, "purchases": 1, "conversion_rate": 0}
        aggregator:
          code: |
            if aggregate is None:
              return {"views": 0, "purchases": 1, "conversion_rate": 0}
            else:
              purchases = aggregate.get("purchases") + 1
              views = aggregate.get("views")
              return {
                "views": views,
                "purchases": purchases,
                "conversion_rate": purchases / views if views > 0 else 0
              }
    to: conversion_by_channel
```

## Running the Application

To run the application:

1. Save the KSML definition to `definitions/analytics.yaml`
2. Create a configuration file at `config/application.properties` with your Kafka connection details
3. Start the Docker Compose environment: `docker-compose up -d`
4. Monitor the output topics to see the real-time analytics results

## Visualizing the Results

You can connect the output topics to visualization tools like:

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
- Check out [Performance Optimization](../advanced/performance-optimization.md) for handling high-volume data