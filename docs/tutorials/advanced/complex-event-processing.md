# Complex Event Processing in KSML

This tutorial explores how to implement complex event processing (CEP) patterns in KSML, allowing you to detect meaningful patterns across multiple events and streams in real-time.

## Introduction to Complex Event Processing

Complex Event Processing (CEP) is a method of tracking and analyzing streams of data about things that happen (events), and deriving conclusions from them. In streaming contexts, CEP allows you to:

- Detect patterns across multiple events
- Identify sequences of events that occur over time
- Correlate events from different sources
- Derive higher-level insights from lower-level events

KSML provides powerful capabilities for implementing CEP patterns through its combination of stateful processing, windowing operations, and Python functions.

## Prerequisites

Before starting this tutorial, you should:

- Understand intermediate KSML concepts (streams, functions, pipelines)
- Have completed the [Windowed Operations](../intermediate/windowed-operations.md) tutorial
- Be familiar with [Joins](../intermediate/joins.md) and [Aggregations](../intermediate/aggregations.md)
- Have a basic understanding of state management in stream processing

## Key CEP Patterns in KSML

### 1. Pattern Detection

Pattern detection involves identifying specific sequences or combinations of events within a stream:

```yaml
functions:
  detect_pattern:
    type: valueTransformer
    code: |
      # Check if this event completes a pattern
      if value.get("event_type") == "C" and state_store.get(key + "_has_A") and state_store.get(key + "_has_B"):
        # Pattern A -> B -> C detected
        result = {
          "pattern_detected": "A_B_C",
          "completion_time": value.get("timestamp"),
          "key": key
        }
        # Reset pattern tracking
        state_store.delete(key + "_has_A")
        state_store.delete(key + "_has_B")
        return result

      # Track events that are part of the pattern
      if value.get("event_type") == "A":
        state_store.put(key + "_has_A", True)
      elif value.get("event_type") == "B":
        state_store.put(key + "_has_B", True)

      # Don't emit anything for partial patterns
      return None
    stores:
      - pattern_state_store

pipelines:
  detect_abc_pattern:
    from: input_events
    mapValues: detect_pattern
    filter: is_not_null  # Only pass through completed patterns
    to: detected_patterns
```

### 2. Temporal Pattern Matching

Temporal pattern matching adds time constraints to pattern detection:

```yaml
functions:
  detect_temporal_pattern:
    type: valueTransformer
    code: |
      current_time = value.get("timestamp", int(time.time() * 1000))

      if value.get("event_type") == "A":
        # Start tracking a new potential pattern
        state_store.put(key + "_pattern_start", current_time)
        state_store.put(key + "_has_A", True)
        return None

      if value.get("event_type") == "B" and state_store.get(key + "_has_A"):
        # Check if B occurred within 5 minutes of A
        pattern_start = state_store.get(key + "_pattern_start")
        if pattern_start and (current_time - pattern_start) <= 5 * 60 * 1000:
          state_store.put(key + "_has_B", True)
          state_store.put(key + "_B_time", current_time)
        return None

      if value.get("event_type") == "C" and state_store.get(key + "_has_B"):
        # Check if C occurred within 2 minutes of B
        b_time = state_store.get(key + "_B_time")
        if b_time and (current_time - b_time) <= 2 * 60 * 1000:
          # Pattern A -> B -> C detected within time constraints
          pattern_start = state_store.get(key + "_pattern_start")
          result = {
            "pattern_detected": "A_B_C",
            "start_time": pattern_start,
            "end_time": current_time,
            "duration_ms": current_time - pattern_start,
            "key": key
          }
          # Reset pattern tracking
          state_store.delete(key + "_has_A")
          state_store.delete(key + "_has_B")
          state_store.delete(key + "_pattern_start")
          state_store.delete(key + "_B_time")
          return result

      return None
    stores:
      - temporal_pattern_store
```

### 3. Event Correlation and Enrichment

Event correlation involves combining related events from different streams:

```yaml
streams:
  user_logins:
    topic: user_login_events
    keyType: string  # User ID
    valueType: json  # Login details

  user_actions:
    topic: user_action_events
    keyType: string  # User ID
    valueType: json  # Action details

  user_logouts:
    topic: user_logout_events
    keyType: string  # User ID
    valueType: json  # Logout details

  user_sessions:
    topic: user_session_events
    keyType: string  # User ID
    valueType: json  # Complete session information

functions:
  correlate_session_events:
    type: valueTransformer
    code: |
      event_type = value.get("event_type")

      # Get current session state
      session = state_store.get(key + "_session")
      if session is None:
        session = {"events": []}

      # Add this event to the session
      event_copy = value.copy()
      event_copy["processed_time"] = int(time.time() * 1000)
      session["events"].append(event_copy)

      # Update session based on event type
      if event_type == "login":
        session["login_time"] = value.get("timestamp")
        session["device"] = value.get("device")
        session["ip_address"] = value.get("ip_address")
        session["status"] = "active"
      elif event_type == "action":
        session["last_activity_time"] = value.get("timestamp")
        session["last_action"] = value.get("action_type")
      elif event_type == "logout":
        session["logout_time"] = value.get("timestamp")
        session["status"] = "completed"
        session["duration_ms"] = session["logout_time"] - session.get("login_time", 0)

        # Return the complete session and clear state
        result = session.copy()
        state_store.delete(key + "_session")
        return result

      # Update session state and don't emit for incomplete sessions
      state_store.put(key + "_session", session)
      return None
    stores:
      - session_state_store

pipelines:
  process_login_events:
    from: user_logins
    mapValues: correlate_session_events
    filter: is_not_null
    to: user_sessions

  process_action_events:
    from: user_actions
    mapValues: correlate_session_events
    filter: is_not_null
    to: user_sessions

  process_logout_events:
    from: user_logouts
    mapValues: correlate_session_events
    filter: is_not_null
    to: user_sessions
```

### 4. Anomaly Detection

Anomaly detection identifies unusual patterns or deviations from normal behavior:

```yaml
functions:
  detect_anomalies:
    type: valueTransformer
    code: |
      # Get historical values for this key
      history = state_store.get(key + "_history")
      if history is None:
        history = {"values": [], "sum": 0, "count": 0}

      # Extract the value to monitor
      metric_value = value.get("metric_value", 0)

      # Update history
      history["values"].append(metric_value)
      history["sum"] += metric_value
      history["count"] += 1

      # Keep only the last 10 values
      if len(history["values"]) > 10:
        removed_value = history["values"].pop(0)
        history["sum"] -= removed_value
        history["count"] -= 1

      # Calculate statistics
      avg = history["sum"] / history["count"] if history["count"] > 0 else 0

      # Calculate standard deviation
      variance_sum = sum((x - avg) ** 2 for x in history["values"])
      std_dev = (variance_sum / history["count"]) ** 0.5 if history["count"] > 0 else 0

      # Check for anomaly (value more than 3 standard deviations from mean)
      is_anomaly = False
      if std_dev > 0 and abs(metric_value - avg) > 3 * std_dev:
        is_anomaly = True

      # Update state
      state_store.put(key + "_history", history)

      # Return anomaly information if detected
      if is_anomaly:
        return {
          "key": key,
          "timestamp": value.get("timestamp"),
          "metric_value": metric_value,
          "average": avg,
          "std_dev": std_dev,
          "deviation": abs(metric_value - avg) / std_dev if std_dev > 0 else 0,
          "original_event": value
        }

      return None
    stores:
      - anomaly_detection_store
```

## Practical Example: Fraud Detection System

Let's build a complete example that implements a real-time fraud detection system using CEP patterns:

```yaml
streams:
  credit_card_transactions:
    topic: cc_transactions
    keyType: string  # Card number
    valueType: json  # Transaction details

  location_changes:
    topic: location_events
    keyType: string  # User ID
    valueType: json  # Location information

  authentication_events:
    topic: auth_events
    keyType: string  # User ID
    valueType: json  # Authentication details

  fraud_alerts:
    topic: fraud_alerts
    keyType: string  # Alert ID
    valueType: json  # Alert details

stores:
  transaction_history_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true

  user_profile_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true

  alert_state_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: false

functions:
  detect_transaction_anomalies:
    type: keyValueTransformer
    code: |
      card_number = key
      transaction = value
      current_time = transaction.get("timestamp", int(time.time() * 1000))

      # Get transaction history
      history = transaction_history_store.get(card_number + "_history")
      if history is None:
        history = {
          "transactions": [],
          "avg_amount": 0,
          "max_amount": 0,
          "locations": set(),
          "merchants": set(),
          "last_transaction_time": 0
        }

      # Calculate time since last transaction
      time_since_last = current_time - history.get("last_transaction_time", 0)

      # Extract transaction details
      amount = transaction.get("amount", 0)
      location = transaction.get("location", "unknown")
      merchant = transaction.get("merchant", "unknown")
      merchant_category = transaction.get("merchant_category", "unknown")

      # Check for anomalies
      anomalies = []

      # 1. Unusual amount
      if amount > 3 * history.get("avg_amount", 0) and amount > 100:
        anomalies.append("unusual_amount")

      # 2. New location
      if location not in history.get("locations", set()) and len(history.get("locations", set())) > 0:
        anomalies.append("new_location")

      # 3. Rapid succession (multiple transactions in short time)
      if time_since_last < 5 * 60 * 1000 and time_since_last > 0:  # Less than 5 minutes
        anomalies.append("rapid_succession")

      # 4. High-risk merchant category
      if merchant_category in ["gambling", "cryptocurrency", "money_transfer"]:
        anomalies.append("high_risk_category")

      # Update history
      history["transactions"].append({
        "timestamp": current_time,
        "amount": amount,
        "location": location,
        "merchant": merchant
      })

      # Keep only last 20 transactions
      if len(history["transactions"]) > 20:
        history["transactions"] = history["transactions"][-20:]

      # Update statistics
      history["avg_amount"] = sum(t.get("amount", 0) for t in history["transactions"]) / len(history["transactions"])
      history["max_amount"] = max(history["max_amount"], amount)
      history["locations"].add(location)
      history["merchants"].add(merchant)
      history["last_transaction_time"] = current_time

      # Store updated history
      transaction_history_store.put(card_number + "_history", history)

      # Generate alert if anomalies detected
      if anomalies:
        alert_id = str(uuid.uuid4())
        alert = {
          "alert_id": alert_id,
          "card_number": card_number,
          "timestamp": current_time,
          "transaction": transaction,
          "anomalies": anomalies,
          "risk_score": len(anomalies) * 25,  # Simple scoring: 25 points per anomaly
          "status": "new"
        }
        return (alert_id, alert)

      return None
    stores:
      - transaction_history_store

  correlate_with_location:
    type: valueTransformer
    code: |
      if value is None:
        return None

      alert = value
      card_number = alert.get("card_number")
      transaction = alert.get("transaction", {})

      # Get user profile
      user_id = transaction.get("user_id")
      if user_id:
        user_profile = user_profile_store.get(user_id)
        if user_profile:
          # Check for impossible travel
          last_known_location = user_profile.get("last_known_location")
          current_location = transaction.get("location")

          if last_known_location and current_location and last_known_location != current_location:
            last_location_time = user_profile.get("last_location_time", 0)
            current_time = transaction.get("timestamp", 0)

            # Simple check: if locations changed too quickly, flag as impossible travel
            if current_time - last_location_time < 3 * 60 * 60 * 1000:  # Less than 3 hours
              alert["anomalies"].append("impossible_travel")
              alert["risk_score"] += 50  # Higher score for impossible travel

              # Add location context
              alert["location_context"] = {
                "previous_location": last_known_location,
                "previous_location_time": last_location_time,
                "current_location": current_location,
                "travel_time_ms": current_time - last_location_time
              }

      return alert
    stores:
      - user_profile_store

  enrich_with_user_data:
    type: valueTransformer
    code: |
      if value is None:
        return None

      alert = value
      transaction = alert.get("transaction", {})
      user_id = transaction.get("user_id")

      if user_id:
        user_profile = user_profile_store.get(user_id)
        if user_profile:
          # Add user context to alert
          alert["user_context"] = {
            "user_id": user_id,
            "account_age_days": user_profile.get("account_age_days"),
            "previous_fraud_alerts": user_profile.get("fraud_alert_count", 0)
          }

          # Adjust risk score based on user history
          if user_profile.get("fraud_alert_count", 0) > 0:
            alert["risk_score"] += 25  # Increase risk for users with previous alerts

          if user_profile.get("account_age_days", 0) < 30:
            alert["risk_score"] += 15  # Increase risk for new accounts

      # Categorize risk level
      if alert["risk_score"] >= 90:
        alert["risk_level"] = "high"
      elif alert["risk_score"] >= 60:
        alert["risk_level"] = "medium"
      else:
        alert["risk_level"] = "low"

      return alert
    stores:
      - user_profile_store

pipelines:
  # Process credit card transactions - first stage
  detect_transaction_anomalies:
    from: credit_card_transactions
    transformKeyValue: detect_transaction_anomalies
    filter: is_not_null
    to: potential_fraud_alerts

  # Process credit card transactions - second stage
  correlate_location_data:
    from: potential_fraud_alerts
    mapValues: correlate_with_location
    to: location_correlated_alerts

  # Process credit card transactions - final stage
  enrich_and_score_alerts:
    from: location_correlated_alerts
    mapValues: enrich_with_user_data
    to: fraud_alerts

  # Update user profiles with location data
  track_locations:
    from: location_changes
    mapValues: update_user_location
    to: updated_user_profiles
```

This example:
1. Processes credit card transactions in real-time
2. Detects anomalies based on transaction amount, location, frequency, and merchant category
3. Correlates transactions with user location data to detect impossible travel patterns
4. Enriches alerts with user context and history
5. Calculates a risk score and categorizes alerts by risk level

## Advanced CEP Techniques

### State Management for Long-Running Patterns

For patterns that span long periods, consider using persistent state stores:

```yaml
stores:
  long_term_pattern_store:
    type: keyValue
    keyType: string
    valueType: json
    persistent: true
    historyRetention: 7d  # Keep state for 7 days
```

### Handling Out-of-Order Events

Use windowing with grace periods to handle events that arrive out of order:

```yaml
pipelines:
  handle_out_of_order:
    from: input_stream
    groupByKey:
    windowByTime:
      size: 1h
      advanceBy: 1h
      grace: 15m  # Allow events up to 15 minutes late
    aggregate:
      initializer: initialize_pattern_state
      aggregator: update_pattern_state
    to: detected_patterns
```

### Hierarchical Pattern Detection

Implement hierarchical patterns by building higher-level patterns from lower-level ones:

```yaml
pipelines:
  # Detect basic patterns
  detect_basic_patterns:
    from: raw_events
    mapValues: detect_basic_pattern
    to: basic_patterns

  # Detect composite patterns from basic patterns
  detect_composite_patterns:
    from: basic_patterns
    mapValues: detect_composite_pattern
    to: composite_patterns
```

## Best Practices for Complex Event Processing

### Performance Considerations

- **State Size**: CEP often requires maintaining state. Monitor state store sizes and use windowing to limit state growth.
- **Computation Complexity**: Complex pattern detection can be CPU-intensive. Keep pattern matching logic efficient.
- **Event Volume**: High-volume streams may require pre-filtering to focus on relevant events.

### Design Patterns

- **Pattern Decomposition**: Break complex patterns into simpler sub-patterns that can be detected independently.
- **Incremental Processing**: Update pattern state incrementally as events arrive rather than reprocessing all events.
- **Hierarchical Patterns**: Build complex patterns by combining simpler patterns.

### Error Handling

Implement robust error handling to prevent pattern detection failures:

```yaml
functions:
  robust_pattern_detection:
    type: valueTransformer
    code: |
      try:
        # Pattern detection logic
        return detected_pattern
      except Exception as e:
        log.error("Error in pattern detection: {}", str(e))
        # Return None to avoid emitting erroneous patterns
        return None
```

## Conclusion

Complex Event Processing in KSML allows you to detect sophisticated patterns across multiple events and streams. By combining stateful processing, windowing operations, and custom Python functions, you can implement powerful CEP applications that derive meaningful insights from streaming data.

In the next tutorial, we'll explore [Custom State Stores](custom-state-stores.md) to learn how to implement and optimize state management for advanced stream processing applications.

## Further Reading

- [Core Concepts: Operations](../../core-concepts/operations.md)
- [Core Concepts: Functions](../../core-concepts/functions.md)
- [Intermediate Tutorial: Windowed Operations](../intermediate/windowed-operations.md)
- [Reference: State Stores](../../reference/data-types-reference.md)
