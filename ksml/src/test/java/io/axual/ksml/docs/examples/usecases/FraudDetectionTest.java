package io.axual.ksml.docs.examples.usecases;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import io.axual.ksml.testutil.KSMLTopic;
import io.axual.ksml.testutil.KSMLTopologyTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for fraud detection use case pipeline.
 * <p>
 * This example demonstrates credit card fraud detection using multiple pipelines:
 * 1. High-value transaction detection: Filters transactions above category-specific thresholds
 * 2. Unusual location detection: Uses state stores to track card location history
 * 3. Transaction velocity monitoring: Detects suspicious transaction patterns
 * 4. Fraud scoring: Consolidates alerts and calculates final risk scores
 * <p>
 * What these tests validate:
 * 1. testHighValueTransactionAlert: High-value transaction detection and alerting
 * 2. testUnusualLocationAlert: Location-based anomaly detection with state stores
 * 3. testHighVelocityAlert: Transaction velocity monitoring
 * 4. testTestThenChargePattern / testTestThenChargePatternTooLate: Pattern recognition
 * 5. testMlFraudPrediction / testMlLowScoreNoAlert: Machine learning fraud scoring
 * 6. testNotificationLevels: Real-time alerting with notification levels
 */
@Slf4j
public class FraudDetectionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // 2025-10-12 23:15 UTC, within the night hours used by the machine learning example
    private static final long NIGHT_TIME_UTC = 1760310900000L;

    @KSMLTopic(topic = "credit_card_transactions")
    TestInputTopic<String, String> transactionsInput;

    @KSMLTopic(topic = "high_value_transactions")
    TestOutputTopic<String, String> highValueOutput;

    @KSMLTopic(topic = "unusual_location_alerts")
    TestOutputTopic<String, String> unusualLocationOutput;

    @KSMLTopic(topic = "fraud_alerts")
    TestOutputTopic<String, String> fraudAlertsOutput;

    @KSMLTopic(topic = "transaction_velocity_alerts")
    TestOutputTopic<String, String> velocityOutput;

    @KSMLTopic(topic = "transaction_pattern_alerts")
    TestOutputTopic<String, String> patternOutput;

    @KSMLTopic(topic = "fraud_alerts")
    TestInputTopic<String, String> fraudAlertsInput;

    @KSMLTopic(topic = "fraud_notifications")
    TestOutputTopic<String, String> notificationsOutput;

    @KSMLTopologyTest(topologies = {
            "docs-examples/use-cases/fraud-detection/fraud-detection.yaml",
            "docs-examples/use-cases/fraud-detection/fraud-detection-python-module.yaml",
    }, modulesDirectory = "docs-examples/use-cases/fraud-detection")
    @DisplayName("Test high-value transaction detection and alerting using different pipeline variants")
    void testBelowThresholdTransaction() throws Exception {
        // Create a low-value transaction (below electronics threshold of 1000)
        String transactionJson = createTransactionJson(
                "txn_002",
                "cust_124",
                "card_457",
                500.0,
                "electronics",
                "USA",
                "NY",
                System.currentTimeMillis()
        );

        transactionsInput.pipeInput("txn_002", transactionJson);

        // Should NOT trigger high-value alert
        assertThat(highValueOutput.isEmpty()).isTrue();

        // Should still be processed by unusual_location_detection (first transaction for card)
        // But won't generate alert as it's the first transaction
        assertThat(unusualLocationOutput.isEmpty()).isTrue();
    }

    @KSMLTopologyTest(topologies = {
            "docs-examples/use-cases/fraud-detection/fraud-detection.yaml",
            "docs-examples/use-cases/fraud-detection/fraud-detection-python-module.yaml",
    }, modulesDirectory = "docs-examples/use-cases/fraud-detection")
    @DisplayName("Test unusual location detection and alerting using different pipeline variants")
    void testUnusualLocationAlert() throws Exception {
        long currentTime = System.currentTimeMillis();
        String cardId = "card_789";
        String customerId = "cust_125";

        // First transaction in USA
        String txn1 = createTransactionJson(
                "txn_003",
                customerId,
                cardId,
                100.0,
                "groceries",
                "USA",
                "CA",
                currentTime
        );
        transactionsInput.pipeInput("txn_003", txn1);

        // First transaction - no alert expected (initializing location history)
        assertThat(unusualLocationOutput.isEmpty()).isTrue();

        // Second transaction in a different country (France) - should trigger alert
        String txn2 = createTransactionJson(
                "txn_004",
                customerId,
                cardId,
                200.0,
                "dining",
                "France",
                "Paris",
                currentTime + 1000
        );
        transactionsInput.pipeInput("txn_004", txn2);

        // Should trigger unusual location alert
        assertThat(unusualLocationOutput.isEmpty()).isFalse();
        String alertJson = unusualLocationOutput.readValue();
        JsonNode locationAlert = objectMapper.readTree(alertJson);

        // Verify alert structure
        assertThat(locationAlert.get("transaction_id").asString()).isEqualTo("txn_004");
        assertThat(locationAlert.get("alert_type").asString()).isEqualTo("unusual_location");
        assertThat(locationAlert.get("card_id").asString()).isEqualTo(cardId);
        assertThat(locationAlert.get("customer_id").asString()).isEqualTo(customerId);

        // Verify current location
        assertThat(locationAlert.get("current_location").get("country").asString()).isEqualTo("France");
        assertThat(locationAlert.get("current_location").get("state").asString()).isEqualTo("Paris");

        // Verify previous locations
        assertThat(locationAlert.has("previous_locations")).isTrue();

        // Verify risk score (70 for new country)
        assertThat(locationAlert.get("risk_score").asInt()).isEqualTo(70);

        // Should also appear in fraud_alerts after scoring
        assertThat(fraudAlertsOutput.isEmpty()).isFalse();
    }

    @KSMLTopologyTest(topologies = {
            "docs-examples/use-cases/fraud-detection/fraud-detection.yaml",
            "docs-examples/use-cases/fraud-detection/fraud-detection-python-module.yaml",
    }, modulesDirectory = "docs-examples/use-cases/fraud-detection")
    @DisplayName("Test unusual location detection and alerting using different pipeline variants")
    void testSameLocationNoAlert() throws Exception {
        long currentTime = System.currentTimeMillis();
        String cardId = "card_999";
        String customerId = "cust_126";

        // First transaction in USA, CA
        String txn1 = createTransactionJson(
                "txn_005",
                customerId,
                cardId,
                50.0,
                "coffee",
                "USA",
                "CA",
                currentTime
        );
        transactionsInput.pipeInput("txn_005", txn1);

        // Second transaction in same location
        String txn2 = createTransactionJson(
                "txn_006",
                customerId,
                cardId,
                75.0,
                "groceries",
                "USA",
                "CA",
                currentTime + 3600000 // 1 hour later
        );
        transactionsInput.pipeInput("txn_006", txn2);

        // Should NOT trigger unusual location alert (same location)
        assertThat(unusualLocationOutput.isEmpty()).isTrue();
    }

    @KSMLTopologyTest(topologies = {
            "docs-examples/use-cases/fraud-detection/fraud-detection.yaml",
            "docs-examples/use-cases/fraud-detection/fraud-detection-python-module.yaml",
    }, modulesDirectory = "docs-examples/use-cases/fraud-detection")
    @DisplayName("Test suspiciously quick location change using different pipeline variants")
    void testDifferentStateWithinTwoHours() throws Exception {
        long currentTime = System.currentTimeMillis();
        String cardId = "card_888";
        String customerId = "cust_127";

        // First transaction in CA
        String txn1 = createTransactionJson(
                "txn_007",
                customerId,
                cardId,
                100.0,
                "fuel",
                "USA",
                "CA",
                currentTime
        );
        transactionsInput.pipeInput("txn_007", txn1);

        // Second transaction in different state (NY) within 2 hours - should trigger alert
        String txn2 = createTransactionJson(
                "txn_008",
                customerId,
                cardId,
                150.0,
                "dining",
                "USA",
                "NY",
                currentTime + 3600000 // 1 hour later (< 2 hours)
        );
        transactionsInput.pipeInput("txn_008", txn2);

        // Should trigger unusual location alert (different state, within 2 hours)
        assertThat(unusualLocationOutput.isEmpty()).isFalse();
        String alertJson = unusualLocationOutput.readValue();
        JsonNode locationAlert = objectMapper.readTree(alertJson);

        assertThat(locationAlert.get("alert_type").asString()).isEqualTo("unusual_location");
        assertThat(locationAlert.get("current_location").get("state").asString()).isEqualTo("NY");
        // Verify risk score (40 for a new state in a known country)
        assertThat(locationAlert.get("risk_score").asInt()).isEqualTo(40);
    }

    @KSMLTopologyTest(topologies = {
            "docs-examples/use-cases/fraud-detection/fraud-detection.yaml",
            "docs-examples/use-cases/fraud-detection/fraud-detection-python-module.yaml",
    }, modulesDirectory = "docs-examples/use-cases/fraud-detection")
    @DisplayName("Test transaction velocity alert using different pipeline variants")
    void testHighVelocityAlert() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 1; i <= 6; i++) {
            transactionsInput.pipeInput("txn_v" + i, createTransactionJson(
                    "txn_v" + i, "cust_200", "card_200", 10.25, "coffee", "USA", "CA", start + i * 60_000L));
        }

        // Only the 6th transaction within 1 hour gives an alert
        assertThat(velocityOutput.getQueueSize()).isEqualTo(1);
        JsonNode alert = objectMapper.readTree(velocityOutput.readValue());
        assertThat(alert.get("transaction_id").asString()).isEqualTo("txn_v6");
        assertThat(alert.get("transactions_last_hour").asInt()).isEqualTo(6);
        assertThat(alert.get("total_amount_24h").asDouble()).isEqualTo(61.5);
        assertThat(alert.get("risk_score").asInt()).isEqualTo(60);
    }

    @KSMLTopologyTest(topologies = "docs-examples/use-cases/fraud-detection/fraud-pattern-detection.yaml")
    @DisplayName("Test 'test then charge' pattern: small transaction followed by a large one within 30 minutes")
    void testTestThenChargePattern() throws Exception {
        long start = System.currentTimeMillis();
        transactionsInput.pipeInput("txn_p1", createTransactionJson(
                "txn_p1", "cust_300", "card_300", 1.00, "digital_goods", "USA", "CA", start, "online"));
        transactionsInput.pipeInput("txn_p2", createTransactionJson(
                "txn_p2", "cust_300", "card_300", 450.00, "digital_goods", "USA", "CA", start + 10 * 60_000L, "online"));

        // Only the large transaction completes the pattern
        assertThat(patternOutput.getQueueSize()).isEqualTo(1);
        JsonNode alert = objectMapper.readTree(patternOutput.readValue());
        assertThat(alert.get("transaction_id").asString()).isEqualTo("txn_p2");
        assertThat(alert.get("pattern_type").asString()).isEqualTo("test_then_charge");
        assertThat(alert.get("risk_score").asInt()).isEqualTo(85);

        // Scoring adds 10 (online) and 15 (digital_goods), capped at 100
        JsonNode scored = objectMapper.readTree(fraudAlertsOutput.readValue());
        assertThat(scored.get("final_risk_score").asInt()).isEqualTo(100);
        assertThat(scored.get("is_likely_fraud").asBoolean()).isTrue();
    }

    @KSMLTopologyTest(topologies = "docs-examples/use-cases/fraud-detection/fraud-pattern-detection.yaml")
    @DisplayName("Test no pattern alert when the large transaction comes more than 30 minutes later")
    void testTestThenChargePatternTooLate() throws Exception {
        long start = System.currentTimeMillis();
        transactionsInput.pipeInput("txn_p3", createTransactionJson(
                "txn_p3", "cust_301", "card_301", 1.00, "digital_goods", "USA", "CA", start, "online"));
        transactionsInput.pipeInput("txn_p4", createTransactionJson(
                "txn_p4", "cust_301", "card_301", 450.00, "digital_goods", "USA", "CA", start + 40 * 60_000L, "online"));

        assertThat(patternOutput.isEmpty()).isTrue();
        assertThat(fraudAlertsOutput.isEmpty()).isTrue();
    }

    @KSMLTopologyTest(topologies = "docs-examples/use-cases/fraud-detection/fraud-detection-machine-learning.yaml")
    @DisplayName("Test machine learning score above 60 creates a fraud alert")
    void testMlFraudPrediction() throws Exception {
        // 1,899.00 jewelry, online, in Germany, at 23:15 UTC: 20 + 15 + 10 + 15 + 10 = 70
        transactionsInput.pipeInput("txn_m1", createTransactionJson(
                "txn_m1", "cust_400", "card_400", 1899.00, "jewelry", "DE", "BE", NIGHT_TIME_UTC, "online"));

        assertThat(fraudAlertsOutput.getQueueSize()).isEqualTo(1);
        JsonNode alert = objectMapper.readTree(fraudAlertsOutput.readValue());
        assertThat(alert.get("ml_fraud_score").asInt()).isEqualTo(70);
        assertThat(alert.get("ml_is_fraud").asBoolean()).isTrue();
        assertThat(alert.get("alert_type").asString()).isEqualTo("ml_fraud_prediction");
        assertThat(alert.get("final_risk_score").asInt()).isEqualTo(70);
    }

    @KSMLTopologyTest(topologies = "docs-examples/use-cases/fraud-detection/fraud-detection-machine-learning.yaml")
    @DisplayName("Test machine learning score of 60 or lower creates no alert")
    void testMlLowScoreNoAlert() throws Exception {
        // Same purchase in the US during the day: 20 + 10 + 15 = 45
        transactionsInput.pipeInput("txn_m2", createTransactionJson(
                "txn_m2", "cust_401", "card_401", 1899.00, "jewelry", "USA", "CA", NIGHT_TIME_UTC - 11 * 3_600_000L, "online"));

        assertThat(fraudAlertsOutput.isEmpty()).isTrue();
    }

    @KSMLTopologyTest(topologies = "docs-examples/use-cases/fraud-detection/fraud-notifications.yaml")
    @DisplayName("Test notification levels: high above 70, medium above 40, and no notification at 30 or lower")
    void testNotificationLevels() throws Exception {
        fraudAlertsInput.pipeInput("txn_n1", createScoredAlertJson("txn_n1", "high_value_transaction", 85));
        fraudAlertsInput.pipeInput("txn_n2", createScoredAlertJson("txn_n2", "unusual_location", 55));
        fraudAlertsInput.pipeInput("txn_n3", createScoredAlertJson("txn_n3", "unusual_location", 30));

        // The alert with score 30 is filtered out
        assertThat(notificationsOutput.getQueueSize()).isEqualTo(2);

        JsonNode high = objectMapper.readTree(notificationsOutput.readValue());
        assertThat(high.get("transaction_id").asString()).isEqualTo("txn_n1");
        assertThat(high.get("alert_level").asString()).isEqualTo("high");
        assertThat(high.get("recommended_action").asString()).isEqualTo("block");

        JsonNode medium = objectMapper.readTree(notificationsOutput.readValue());
        assertThat(medium.get("transaction_id").asString()).isEqualTo("txn_n2");
        assertThat(medium.get("alert_level").asString()).isEqualTo("medium");
        assertThat(medium.get("recommended_action").asString()).isEqualTo("review");
    }

    /**
     * Helper method to create a transaction JSON object.
     */
    private String createTransactionJson(
            String transactionId,
            String customerId,
            String cardId,
            double amount,
            String merchantCategory,
            String country,
            String state,
            long timestamp
    ) throws Exception {
        return createTransactionJson(transactionId, customerId, cardId, amount, merchantCategory, country, state, timestamp, null);
    }

    /**
     * Helper method to create a transaction JSON object with a transaction type, such as "online".
     */
    private String createTransactionJson(
            String transactionId,
            String customerId,
            String cardId,
            double amount,
            String merchantCategory,
            String country,
            String state,
            long timestamp,
            String transactionType
    ) throws Exception {
        Map<String, Object> transaction = new HashMap<>();
        transaction.put("transaction_id", transactionId);
        transaction.put("customer_id", customerId);
        transaction.put("card_id", cardId);
        transaction.put("amount", amount);
        transaction.put("timestamp", timestamp);

        Map<String, Object> merchant = new HashMap<>();
        merchant.put("category", merchantCategory);

        Map<String, Object> location = new HashMap<>();
        location.put("country", country);
        location.put("state", state);

        merchant.put("location", location);
        transaction.put("merchant", merchant);

        if (transactionType != null) {
            transaction.put("transaction_type", transactionType);
        }

        return objectMapper.writeValueAsString(transaction);
    }

    /**
     * Helper method to create an alert as it arrives on the fraud_alerts topic.
     */
    private String createScoredAlertJson(String transactionId, String alertType, int finalRiskScore) throws Exception {
        Map<String, Object> alert = new HashMap<>();
        alert.put("transaction_id", transactionId);
        alert.put("alert_type", alertType);
        alert.put("final_risk_score", finalRiskScore);
        return objectMapper.writeValueAsString(alert);
    }
}
