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
streams:
  transactions:
    topic: credit_card_transactions
    keyType: string  # transaction_id
    valueType: json  # transaction data
    
  high_value_transactions:
    topic: high_value_transactions
    keyType: string  # transaction_id
    valueType: json  # transaction data
    
  unusual_location_alerts:
    topic: unusual_location_alerts
    keyType: string  # card_id
    valueType: json  # alert data
    
  velocity_alerts:
    topic: transaction_velocity_alerts
    keyType: string  # card_id
    valueType: json  # alert data
    
  fraud_alerts:
    topic: fraud_alerts
    keyType: string  # transaction_id
    valueType: json  # consolidated alert data

stores:
  customer_transaction_history:
    type: keyValue
    persistent: true
    
  card_location_history:
    type: keyValue
    persistent: true

functions:
  check_high_value:
    type: filter
    parameters:
      - name: value
        type: object
    code: |
      # Define thresholds for different merchant categories
      thresholds = {
        "electronics": 1000,
        "jewelry": 2000,
        "travel": 3000,
        "default": 500
      }
      
      category = value.get("merchant", {}).get("category", "default")
      threshold = thresholds.get(category, thresholds["default"])
      
      return value.get("amount", 0) > threshold
      
  create_high_value_alert:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      return {
        "transaction_id": value.get("transaction_id"),
        "timestamp": value.get("timestamp"),
        "customer_id": value.get("customer_id"),
        "card_id": value.get("card_id"),
        "merchant": value.get("merchant"),
        "amount": value.get("amount"),
        "alert_type": "high_value_transaction",
        "risk_score": min(100, value.get("amount") / 10)  # Simple scoring based on amount
      }
      
  check_unusual_location:
    type: process
    parameters:
      - name: key
        type: string
      - name: value
        type: object
      - name: store
        type: keyValueStore
    code: |
      card_id = value.get("card_id")
      current_country = value.get("merchant", {}).get("location", {}).get("country")
      current_state = value.get("merchant", {}).get("location", {}).get("state")
      
      # Get location history for this card
      location_history = store.get(card_id)
      
      if location_history is None:
        # First transaction for this card, initialize history
        location_history = {
          "last_countries": [current_country],
          "last_states": [current_state],
          "last_transaction_time": value.get("timestamp")
        }
        store.put(card_id, location_history)
        return None  # No alert for first transaction
      
      # Check for unusual location
      unusual_location = False
      time_since_last = value.get("timestamp") - location_history.get("last_transaction_time", 0)
      
      # If transaction is in a different country than any in history
      if current_country not in location_history.get("last_countries", []):
        unusual_location = True
      
      # If transaction is in a different state and happened within 2 hours of last transaction
      elif (current_state not in location_history.get("last_states", []) and 
            time_since_last < 7200000):  # 2 hours in milliseconds
        unusual_location = True
      
      # Update location history (keep last 3)
      last_countries = location_history.get("last_countries", [])
      if current_country not in last_countries:
        last_countries.append(current_country)
      if len(last_countries) > 3:
        last_countries = last_countries[-3:]
      
      last_states = location_history.get("last_states", [])
      if current_state not in last_states:
        last_states.append(current_state)
      if len(last_states) > 3:
        last_states = last_states[-3:]
      
      location_history = {
        "last_countries": last_countries,
        "last_states": last_states,
        "last_transaction_time": value.get("timestamp")
      }
      store.put(card_id, location_history)
      
      if unusual_location:
        return {
          "transaction_id": value.get("transaction_id"),
          "timestamp": value.get("timestamp"),
          "customer_id": value.get("customer_id"),
          "card_id": card_id,
          "current_location": {
            "country": current_country,
            "state": current_state
          },
          "previous_locations": {
            "countries": location_history.get("last_countries", [])[:-1],
            "states": location_history.get("last_states", [])[:-1]
          },
          "time_since_last_transaction": time_since_last,
          "alert_type": "unusual_location",
          "risk_score": 70 if current_country not in location_history.get("last_countries", [])[:-1] else 40
        }
      else:
        return None
      
  check_transaction_velocity:
    type: process
    parameters:
      - name: key
        type: string
      - name: value
        type: object
      - name: store
        type: keyValueStore
    code: |
      card_id = value.get("card_id")
      current_time = value.get("timestamp")
      
      # Get transaction history for this card
      history = store.get(card_id)
      
      if history is None:
        # First transaction for this card, initialize history
        history = {
          "transaction_times": [current_time],
          "transaction_count_1h": 1,
          "transaction_count_24h": 1,
          "total_amount_24h": value.get("amount", 0)
        }
        store.put(card_id, history)
        return None  # No alert for first transaction
      
      # Update transaction history
      transaction_times = history.get("transaction_times", [])
      transaction_times.append(current_time)
      
      # Keep only transactions from the last 24 hours
      one_day_ago = current_time - 86400000  # 24 hours in milliseconds
      transaction_times = [t for t in transaction_times if t > one_day_ago]
      
      # Count transactions in the last hour
      one_hour_ago = current_time - 3600000  # 1 hour in milliseconds
      transaction_count_1h = sum(1 for t in transaction_times if t > one_hour_ago)
      
      # Calculate total amount in the last 24 hours
      total_amount_24h = history.get("total_amount_24h", 0) + value.get("amount", 0)
      if len(transaction_times) < len(history.get("transaction_times", [])):
        # Some transactions dropped out of the 24h window, recalculate total
        # In a real system, you would store individual transaction amounts
        # This is simplified for the example
        total_amount_24h = value.get("amount", 0) * len(transaction_times)
      
      # Update history
      history = {
        "transaction_times": transaction_times,
        "transaction_count_1h": transaction_count_1h,
        "transaction_count_24h": len(transaction_times),
        "total_amount_24h": total_amount_24h
      }
      store.put(card_id, history)
      
      # Check for velocity anomalies
      velocity_alert = None
      
      # More than 5 transactions in an hour
      if transaction_count_1h > 5:
        velocity_alert = {
          "transaction_id": value.get("transaction_id"),
          "timestamp": value.get("timestamp"),
          "customer_id": value.get("customer_id"),
          "card_id": card_id,
          "transactions_last_hour": transaction_count_1h,
          "transactions_last_day": len(transaction_times),
          "total_amount_24h": total_amount_24h,
          "alert_type": "high_transaction_velocity",
          "risk_score": min(100, transaction_count_1h * 10)
        }
      
      return velocity_alert

  calculate_fraud_score:
    type: mapValues
    parameters:
      - name: value
        type: object
    code: |
      # Base risk score from the alert
      risk_score = value.get("risk_score", 0)
      
      # Additional factors
      alert_type = value.get("alert_type", "")
      
      # Adjust score based on transaction type
      if value.get("transaction_type") == "online":
        risk_score += 10
      
      # Adjust score based on merchant category
      high_risk_categories = ["electronics", "jewelry", "digital_goods"]
      if value.get("merchant", {}).get("category") in high_risk_categories:
        risk_score += 15
      
      # Cap the score at 100
      risk_score = min(100, risk_score)
      
      # Add the calculated score to the alert
      value["final_risk_score"] = risk_score
      value["is_likely_fraud"] = risk_score > 70
      
      return value

pipelines:
  # Pipeline for high-value transaction detection
  high_value_detection:
    from: transactions
    filter:
      function: check_high_value
    mapValues:
      function: create_high_value_alert
    to: high_value_transactions
    
  # Pipeline for unusual location detection
  unusual_location_detection:
    from: transactions
    process:
      function: check_unusual_location
      stores:
        - card_location_history
    to: unusual_location_alerts
    
  # Pipeline for transaction velocity monitoring
  velocity_monitoring:
    from: transactions
    process:
      function: check_transaction_velocity
      stores:
        - customer_transaction_history
    to: velocity_alerts
    
  # Pipeline for consolidating alerts and calculating final fraud score
  fraud_scoring:
    from:
      - high_value_transactions
      - unusual_location_alerts
      - velocity_alerts
    mapValues:
      function: calculate_fraud_score
    to: fraud_alerts
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