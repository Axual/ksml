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

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for fraud detection use case pipeline.
 * <p>
 * This example demonstrates credit card fraud detection using multiple pipelines:
 * 1. High-value transaction detection: Filters transactions above category-specific thresholds
 * 2. Fraud scoring: Consolidates alerts and calculates final risk scores
 * <p>
 * What these tests validate:
 * 1. testHighValueTransactionAlert: High-value transaction detection and alerting
 * 2. testBelowThresholdTransaction: Verify transactions below threshold are not flagged
 * <p>
 * Note: This test focuses on the high-value detection pipeline. The fraud-detection.yaml
 * also includes unusual location and velocity monitoring pipelines with state stores.
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

    @KSMLTest(topology = "docs-examples/use-cases/fraud-detection/fraud-detection.yaml")
    void testHighValueTransactionAlert() throws Exception {
        // Create a high-value electronics transaction (threshold: 1000)
        String transactionJson = createTransactionJson(
                "txn_001",
                "cust_123",
                "card_456",
                1500.0,
                "electronics",
                "USA",
                "CA",
                System.currentTimeMillis()
        );

        transactionsInput.pipeInput("txn_001", transactionJson);

        // Should trigger high-value alert
        assertThat(highValueOutput.isEmpty()).isFalse();
        String highValueAlert = highValueOutput.readValue();
        JsonNode alert = objectMapper.readTree(highValueAlert);

        // Verify alert structure
        assertThat(alert.get("transaction_id").asText()).isEqualTo("txn_001");
        assertThat(alert.get("alert_type").asText()).isEqualTo("high_value_transaction");
        assertThat(alert.get("amount").asDouble()).isEqualTo(1500.0);
        assertThat(alert.get("customer_id").asText()).isEqualTo("cust_123");
        assertThat(alert.get("card_id").asText()).isEqualTo("card_456");

        // Verify risk score calculation (amount / 10)
        assertThat(alert.get("risk_score").asDouble()).isEqualTo(100.0); // min(100, 1500/10)

        // Should also appear in fraud_alerts after scoring
        assertThat(fraudAlertsOutput.isEmpty()).isFalse();
        String fraudAlert = fraudAlertsOutput.readValue();
        JsonNode scoredAlert = objectMapper.readTree(fraudAlert);

        assertThat(scoredAlert.has("final_risk_score")).isTrue();
        assertThat(scoredAlert.has("is_likely_fraud")).isTrue();
    }

    @KSMLTest(topology = "docs-examples/use-cases/fraud-detection/fraud-detection.yaml")
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
