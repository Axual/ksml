from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ksml_runtime_stub import log,stores

def is_high_value(value):
    " determine if the message in the pipeline holds a high value transaction."
    log.info("is_high_value()")
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

def high_value_alert(value):
    " create an alert from the value taken from the pipeline. "
    log.info("create_high_value_alert()")
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

def check_unusual_loc(value):
    "  check if the message contains a transaction in an unusual location. "
    import json
    log.info("check_unusual_location()")

    # need to get the card_location_history from the 'stores' dict
    card_location_history = stores.get('card_location_history')

    card_id = value.get("card_id")
    current_country = value.get("merchant", {}).get("location", {}).get("country")
    current_state = value.get("merchant", {}).get("location", {}).get("state")
    time_since_last = 0  # Initialize to ensure it's always defined

    # Get location history for this card (stored as JSON string)
    location_history_json = card_location_history.get(card_id)
    location_history = json.loads(location_history_json) if location_history_json else None
    unusual_location = False

    if location_history is None:
        # First transaction for this card, initialize history
        location_history = {
            "last_countries": [current_country],
            "last_states": [current_state],
            "last_transaction_time": value.get("timestamp")
        }
        # Store as JSON string
        card_location_history.put(card_id, json.dumps(location_history))
    else:
        # Check for unusual location
        time_since_last = value.get("timestamp") - location_history.get("last_transaction_time", 0)

        # If transaction is in a different country than any in history
        if current_country not in location_history.get("last_countries", []):
            unusual_location = True

        # If transaction is in a different state and happened within 2 hours of last transaction
        elif (current_state not in location_history.get("last_states", []) and time_since_last < 7200000):  # 2 hours in milliseconds
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
        # Store updated history as JSON string
        card_location_history.put(card_id, json.dumps(location_history))

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

def transaction_velocity_check(value):
    " return message if transaction are high velocity"
    import json
    customer_transaction_history = stores.get("customer_transaction_history")
    log.info("transation_velocity_check()")

    card_id = value.get("card_id")
    current_time = value.get("timestamp")
    result = None

    # Get transaction history for this card (stored as JSON string)
    history_json = customer_transaction_history.get(card_id)
    history = json.loads(history_json) if history_json else None

    if history is None:
        # First transaction for this card, initialize history
        history = {
            "transaction_times": [current_time],
            "transaction_count_1h": 1,
            "transaction_count_24h": 1,
            "total_amount_24h": value.get("amount", 0)
        }
        # Store as JSON string
        customer_transaction_history.put(card_id, json.dumps(history))
        result = None  # No alert for first transaction
    else:

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
        # Store updated history as JSON string
        customer_transaction_history.put(card_id, json.dumps(history))

        # More than 5 transactions in an hour
        if transaction_count_1h > 5:
            result = {
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

    return result

def fraud_score(value):
    log.info("fraud_score()")

    " calculate the fraud score for the given message. "
    # Base risk score from the alert
    risk_score = value.get("risk_score", 0)

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



