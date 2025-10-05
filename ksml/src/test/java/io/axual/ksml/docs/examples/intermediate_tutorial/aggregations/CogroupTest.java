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
 * Test for cogroup aggregation with custom aggregator in KSML.
 * <p>
 * The cogroup operation demonstrates:
 * 1. groupByKey to group orders by customer
 * 2. cogroup with custom aggregator function
 * 3. aggregate with initializer to materialize the aggregation
 * <p>
 * What these Tests Validate (KSML Translation):
 * <p>
 * 1. testCogroupAggregatesOrders: cogroup aggregates order amounts correctly
 * 2. testCogroupIncrementsCounts: cogroup tracks order counts per customer
 * 3. testCogroupMultipleCustomers: cogroup maintains separate aggregates per key
 * 4. testCogroupInitializer: initializer creates correct initial state
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class CogroupTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "customer_orders")
    TestInputTopic<String, String> ordersInput;

    @KSMLTopic(topic = "customer_totals")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-cogroup.yaml")
    void testCogroupAggregatesOrders() throws Exception {
        String customer = "alice";

        // Send multiple orders
        ordersInput.pipeInput(customer, createOrderJson("ORD001", customer, 100.00));
        ordersInput.pipeInput(customer, createOrderJson("ORD002", customer, 50.00));

        assertThat(outputTopic.getQueueSize()).isEqualTo(2);

        // Skip first, read final result
        outputTopic.readValue();
        String result = outputTopic.readValue();

        JsonNode json = objectMapper.readTree(result);

        // Verify orders aggregated correctly
        assertThat(json.get("total_amount").asDouble()).isEqualTo(150.00);
        assertThat(json.get("order_count").asInt()).isEqualTo(2);
        assertThat(json.get("customer").asText()).isEqualTo(customer);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-cogroup.yaml")
    void testCogroupIncrementsCounts() throws Exception {
        String customer = "bob";

        // Send three orders
        ordersInput.pipeInput(customer, createOrderJson("ORD003", customer, 10.00));
        ordersInput.pipeInput(customer, createOrderJson("ORD004", customer, 20.00));
        ordersInput.pipeInput(customer, createOrderJson("ORD005", customer, 30.00));

        assertThat(outputTopic.getQueueSize()).isEqualTo(3);

        // Read final result
        outputTopic.readValue();
        outputTopic.readValue();
        String result = outputTopic.readValue();

        JsonNode json = objectMapper.readTree(result);

        // Verify count incremented correctly
        assertThat(json.get("order_count").asInt()).isEqualTo(3);
        assertThat(json.get("total_amount").asDouble()).isEqualTo(60.00);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-cogroup.yaml")
    void testCogroupMultipleCustomers() throws Exception {
        // Send orders for different customers
        ordersInput.pipeInput("alice", createOrderJson("ORD006", "alice", 100.00));
        ordersInput.pipeInput("bob", createOrderJson("ORD007", "bob", 200.00));
        ordersInput.pipeInput("alice", createOrderJson("ORD008", "alice", 50.00));

        assertThat(outputTopic.getQueueSize()).isEqualTo(3);

        // Read alice's first order
        var record1 = outputTopic.readRecord();
        assertThat(record1.getKey()).isEqualTo("alice");
        JsonNode json1 = objectMapper.readTree(record1.getValue());
        assertThat(json1.get("total_amount").asDouble()).isEqualTo(100.00);

        // Read bob's order
        var record2 = outputTopic.readRecord();
        assertThat(record2.getKey()).isEqualTo("bob");
        JsonNode json2 = objectMapper.readTree(record2.getValue());
        assertThat(json2.get("total_amount").asDouble()).isEqualTo(200.00);

        // Read alice's second order
        var record3 = outputTopic.readRecord();
        assertThat(record3.getKey()).isEqualTo("alice");
        JsonNode json3 = objectMapper.readTree(record3.getValue());
        assertThat(json3.get("total_amount").asDouble()).isEqualTo(150.00);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-cogroup.yaml")
    void testCogroupInitializer() throws Exception {
        String customer = "charlie";

        // Send first order
        ordersInput.pipeInput(customer, createOrderJson("ORD009", customer, 75.00));

        String result = outputTopic.readValue();
        JsonNode json = objectMapper.readTree(result);

        // Verify initializer created correct structure
        assertThat(json.has("total_amount")).isTrue();
        assertThat(json.has("order_count")).isTrue();
        assertThat(json.has("customer")).isTrue();

        assertThat(json.get("total_amount").asDouble()).isEqualTo(75.00);
        assertThat(json.get("order_count").asInt()).isEqualTo(1);
    }

    private String createOrderJson(String orderId, String customer, double amount) throws Exception {
        Map<String, Object> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("customer", customer);
        order.put("amount", amount);
        order.put("timestamp", System.currentTimeMillis());

        return objectMapper.writeValueAsString(order);
    }
}
