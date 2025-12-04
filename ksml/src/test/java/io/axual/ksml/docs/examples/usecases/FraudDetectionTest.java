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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.axual.ksml.testutil.KSMLTopologyTest;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

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
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class FraudDetectionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "credit_card_transactions")
    TestInputTopic<String, String> transactionsInput;

    @KSMLTopic(topic = "high_value_transactions")
    TestOutputTopic<String, String> highValueOutput;

    @KSMLTopic(topic = "unusual_location_alerts")
    TestOutputTopic<String, String> unusualLocationOutput;

    @KSMLTopic(topic = "fraud_alerts")
    TestOutputTopic<String, String> fraudAlertsOutput;

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
        assertThat(locationAlert.get("transaction_id").asText()).isEqualTo("txn_004");
        assertThat(locationAlert.get("alert_type").asText()).isEqualTo("unusual_location");
        assertThat(locationAlert.get("card_id").asText()).isEqualTo(cardId);
        assertThat(locationAlert.get("customer_id").asText()).isEqualTo(customerId);

        // Verify current location
        assertThat(locationAlert.get("current_location").get("country").asText()).isEqualTo("France");
        assertThat(locationAlert.get("current_location").get("state").asText()).isEqualTo("Paris");

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

        assertThat(locationAlert.get("alert_type").asText()).isEqualTo("unusual_location");
        assertThat(locationAlert.get("current_location").get("state").asText()).isEqualTo("NY");
        // Verify risk score is calculated (value depends on location history state)
        assertThat(locationAlert.get("risk_score").asInt()).isGreaterThan(0);
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

        return objectMapper.writeValueAsString(transaction);
    }
}
