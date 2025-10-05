package io.axual.ksml.docs.examples.intermediate_tutorial.aggregations;

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
 * Test for reduce aggregation with custom reducer in KSML.
 * <p>
 * The reduce aggregation demonstrates:
 * 1. transformValue to extract amount from transaction JSON
 * 2. groupByKey to group transactions by account
 * 3. reduce with custom reducer function to sum amounts
 * 4. transformValue to format the result back to JSON
 * <p>
 * What these Tests Validate (KSML Translation):
 * <p>
 * 1. testReduceSumsTransactionAmounts: reducer correctly sums transaction amounts
 * 2. testExtractAmountTransformation: transformValue extracts amount_cents from JSON
 * 3. testFormatTotalTransformation: transformValue converts cents to JSON with dollars
 * 4. testMultipleAccountsIndependent: reduce maintains separate sums per account key
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class ReduceAggregationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "financial_transactions")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "transaction_sums")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-reduce.yaml")
    void testReduceSumsTransactionAmounts() throws Exception {
        String accountId = "ACC001";

        // Send multiple transactions
        inputTopic.pipeInput(accountId, createTransactionJson(accountId, 10000, "TXN001")); // $100.00
        inputTopic.pipeInput(accountId, createTransactionJson(accountId, 5000, "TXN002"));  // $50.00
        inputTopic.pipeInput(accountId, createTransactionJson(accountId, 2500, "TXN003"));  // $25.00

        // Should receive 3 running total updates
        assertThat(outputTopic.getQueueSize()).isEqualTo(3);

        // First transaction
        String result1 = outputTopic.readValue();
        JsonNode json1 = objectMapper.readTree(result1);
        assertThat(json1.get("total_cents").asLong()).isEqualTo(10000);
        assertThat(json1.get("total_dollars").asDouble()).isEqualTo(100.00);

        // Second transaction (100 + 50 = 150)
        String result2 = outputTopic.readValue();
        JsonNode json2 = objectMapper.readTree(result2);
        assertThat(json2.get("total_cents").asLong()).isEqualTo(15000);
        assertThat(json2.get("total_dollars").asDouble()).isEqualTo(150.00);

        // Third transaction (150 + 25 = 175)
        String result3 = outputTopic.readValue();
        JsonNode json3 = objectMapper.readTree(result3);
        assertThat(json3.get("total_cents").asLong()).isEqualTo(17500);
        assertThat(json3.get("total_dollars").asDouble()).isEqualTo(175.00);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-reduce.yaml")
    void testExtractAmountTransformation() throws Exception {
        String accountId = "ACC002";

        // Send transaction with specific amount
        inputTopic.pipeInput(accountId, createTransactionJson(accountId, 12345, "TXN004"));

        String result = outputTopic.readValue();
        JsonNode json = objectMapper.readTree(result);

        // Verify Python function correctly extracted and summed amount_cents
        assertThat(json.get("total_cents").asLong()).isEqualTo(12345);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-reduce.yaml")
    void testFormatTotalTransformation() throws Exception {
        String accountId = "ACC003";

        // Send transaction
        inputTopic.pipeInput(accountId, createTransactionJson(accountId, 9999, "TXN005"));

        String result = outputTopic.readValue();
        JsonNode json = objectMapper.readTree(result);

        // Verify format_total Python function created correct JSON structure
        assertThat(json.has("total_cents")).isTrue();
        assertThat(json.has("total_dollars")).isTrue();
        assertThat(json.get("total_dollars").asDouble()).isEqualTo(99.99);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-reduce.yaml")
    void testMultipleAccountsIndependent() throws Exception {
        // Send transactions for multiple accounts
        inputTopic.pipeInput("ACC001", createTransactionJson("ACC001", 10000, "TXN006"));
        inputTopic.pipeInput("ACC002", createTransactionJson("ACC002", 20000, "TXN007"));
        inputTopic.pipeInput("ACC001", createTransactionJson("ACC001", 5000, "TXN008"));
        inputTopic.pipeInput("ACC002", createTransactionJson("ACC002", 3000, "TXN009"));

        assertThat(outputTopic.getQueueSize()).isEqualTo(4);

        // Read and verify each account maintains separate totals
        var record1 = outputTopic.readRecord();
        assertThat(record1.getKey()).isEqualTo("ACC001");
        JsonNode json1 = objectMapper.readTree(record1.getValue());
        assertThat(json1.get("total_cents").asLong()).isEqualTo(10000);

        var record2 = outputTopic.readRecord();
        assertThat(record2.getKey()).isEqualTo("ACC002");
        JsonNode json2 = objectMapper.readTree(record2.getValue());
        assertThat(json2.get("total_cents").asLong()).isEqualTo(20000);

        var record3 = outputTopic.readRecord();
        assertThat(record3.getKey()).isEqualTo("ACC001");
        JsonNode json3 = objectMapper.readTree(record3.getValue());
        assertThat(json3.get("total_cents").asLong()).isEqualTo(15000); // 10000 + 5000

        var record4 = outputTopic.readRecord();
        assertThat(record4.getKey()).isEqualTo("ACC002");
        JsonNode json4 = objectMapper.readTree(record4.getValue());
        assertThat(json4.get("total_cents").asLong()).isEqualTo(23000); // 20000 + 3000
    }

    private String createTransactionJson(String accountId, long amountCents, String transactionId) throws Exception {
        Map<String, Object> transaction = new HashMap<>();
        transaction.put("account_id", accountId);
        transaction.put("amount_cents", amountCents);
        transaction.put("amount_dollars", amountCents / 100.0);
        transaction.put("transaction_id", transactionId);
        transaction.put("timestamp", System.currentTimeMillis());

        return objectMapper.writeValueAsString(transaction);
    }
}
