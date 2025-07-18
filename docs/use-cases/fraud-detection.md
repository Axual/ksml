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
- Have a basic understanding of [Windowed Operations](../tutorials/intermediate/windowed-operations.md)
- Be familiar with [State Stores](../tutorials/intermediate/state-stores.md)

## The Use Case

Imagine you're building a fraud detection system for a financial institution that processes millions of credit card transactions daily. You want to:

1. Identify suspicious transactions based on multiple risk factors
2. Track unusual patterns in customer behavior
3. Generate real-time alerts for high-risk activities
4. Maintain a low rate of false positives

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

Now, let's create our KSML definition file:

```yaml
{% include "../definitions/use-cases/fraud-detection/fraud-detection.yaml" %}
```

## Advanced Fraud Detection Techniques

### Pattern Recognition

To detect complex fraud patterns, you can implement more sophisticated algorithms:

```yaml
functions:
  detect_fraud_pattern:
    type: process
    parameters:
      - name: key
        type: string
      - name: value
        type: object
      - name: store
        type: keyValueStore
    code: |
      # Pattern: Small test transaction followed by large transaction
      card_id = value.get("card_id")
      current_amount = value.get("amount", 0)
      
      # Get transaction history
      history = store.get(card_id)
      
      if history is None or "recent_transactions" not in history:
        # Initialize history
        history = {"recent_transactions": [{"amount": current_amount, "time": value.get("timestamp")}]}
        store.put(card_id, history)
        return None
      
      # Add current transaction to history
      recent_transactions = history.get("recent_transactions", [])
      recent_transactions.append({"amount": current_amount, "time": value.get("timestamp")})
      
      # Keep only recent transactions (last 24 hours)
      one_day_ago = value.get("timestamp") - 86400000
      recent_transactions = [t for t in recent_transactions if t.get("time", 0) > one_day_ago]
      
      # Sort by time
      recent_transactions.sort(key=lambda x: x.get("time", 0))
      
      # Look for pattern: small transaction (< $5) followed by large transaction within 30 minutes
      pattern_found = False
      for i in range(len(recent_transactions) - 1):
        if (recent_transactions[i].get("amount", 0) < 5 and 
            recent_transactions[i+1].get("amount", 0) > 100 and
            recent_transactions[i+1].get("time", 0) - recent_transactions[i].get("time", 0) < 1800000):  # 30 minutes
          pattern_found = True
          break
      
      # Update history
      history["recent_transactions"] = recent_transactions
      store.put(card_id, history)
      
      if pattern_found:
        return {
          "transaction_id": value.get("transaction_id"),
          "timestamp": value.get("timestamp"),
          "customer_id": value.get("customer_id"),
          "card_id": card_id,
          "alert_type": "fraud_pattern_detected",
          "pattern_type": "test_then_charge",
          "risk_score": 85
        }
      
      return None
```

### Machine Learning Integration

For more advanced fraud detection, you can integrate machine learning models:

```yaml
functions:
  ml_fraud_prediction:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      # In a real implementation, you would call an external ML service
      # This is a simplified example
      
      # Extract features
      features = {
        "amount": value.get("amount", 0),
        "is_international": value.get("merchant", {}).get("location", {}).get("country") != "US",
        "is_online": value.get("transaction_type") == "online",
        "high_risk_merchant": value.get("merchant", {}).get("category") in ["electronics", "jewelry", "digital_goods"],
        "transaction_hour": (value.get("timestamp", 0) / 3600000) % 24,  # Hour of day
        "transaction_count_1h": value.get("transactions_last_hour", 1),
        "transaction_count_24h": value.get("transactions_last_day", 1)
      }
      
      # Simple rule-based model (in reality, this would be a trained ML model)
      score = 0
      if features["amount"] > 1000: score += 20
      if features["is_international"]: score += 15
      if features["is_online"]: score += 10
      if features["high_risk_merchant"]: score += 15
      if features["transaction_hour"] > 22 or features["transaction_hour"] < 6: score += 10
      if features["transaction_count_1h"] > 3: score += 15
      if features["transaction_count_24h"] > 10: score += 15
      
      # Normalize score to 0-100
      score = min(100, score)
      
      # Add prediction to the transaction
      value["ml_fraud_score"] = score
      value["ml_fraud_probability"] = score / 100.0
      value["ml_is_fraud"] = score > 60
      
      return value
```

## Real-time Alerting

To make your fraud detection system actionable, you need to generate alerts:

```yaml
functions:
  generate_alert:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      risk_score = value.get("final_risk_score", 0)
      
      # Determine alert level
      alert_level = "low"
      if risk_score > 70:
        alert_level = "high"
      elif risk_score > 40:
        alert_level = "medium"
      
      # Create alert message
      alert = {
        "transaction_id": value.get("transaction_id"),
        "timestamp": value.get("timestamp"),
        "customer_id": value.get("customer_id"),
        "card_id": value.get("card_id"),
        "merchant": value.get("merchant"),
        "amount": value.get("amount"),
        "alert_type": value.get("alert_type"),
        "risk_score": risk_score,
        "alert_level": alert_level,
        "alert_message": f"Potential fraud detected: {value.get('alert_type')} with risk score {risk_score}",
        "recommended_action": "block" if alert_level == "high" else "review"
      }
      
      return alert

pipelines:
  alert_generation:
    from: fraud_alerts
    filter:
      code: |
        return value.get("final_risk_score", 0) > 30  # Only alert on medium to high risk
    mapValues:
      function: generate_alert
    to: fraud_notifications
```

## Testing and Validation

To test your fraud detection system:

1. Generate sample transaction data with known fraud patterns
2. Deploy your KSML application using the [KSML Runner](../reference/runner-reference.md)
3. Monitor the alert topics to verify detection accuracy
4. Adjust thresholds and rules to balance detection rate and false positives

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

- [Machine Learning Integration](../tutorials/advanced/ml-integration.md) for more sophisticated fraud models
- [Complex Event Processing](../tutorials/advanced/complex-event-processing.md) for detecting multi-stage fraud patterns
- [External Service Integration](../tutorials/advanced/external-services.md) for incorporating third-party risk scores