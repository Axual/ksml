package io.axual.ksml.docs.examples.intermediate_tutorial.joins;

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
 * Test for stream-table join (order enrichment with customer data) in KSML.
 * <p>
 * The stream-table join demonstrates:
 * 1. Rekeying stream from order_id to customer_id for join
 * 2. Joining stream with table to enrich orders with customer data
 * 3. Restoring original order_id key after join
 * <p>
 * What these Tests Validate (KSML Translation):
 *
 * 1. testOrderEnrichedWithCustomer: join operation combines order and customer data correctly
 * 2. testRekeyingForJoin: transformKey correctly extracts customer_id from order
 * 3. testRestoreOriginalKey: transformKey correctly restores order_id after join
 * 4. testMultipleOrdersSameCustomer: Multiple orders for same customer all get enriched
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class StreamTableJoinTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "new_orders")
    TestInputTopic<String, String> ordersInput;

    @KSMLTopic(topic = "customer_data")
    TestInputTopic<String, String> customersInput;

    @KSMLTopic(topic = "orders_with_customer_data")
    TestOutputTopic<String, String> enrichedOutput;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-join.yaml")
    void testOrderEnrichedWithCustomer() throws Exception {
        // First populate customer table
        customersInput.pipeInput("CUST001", createCustomerJson("CUST001", "Alice Johnson", "alice@email.com", "US-WEST"));

        // Send order for that customer
        ordersInput.pipeInput("ORD0001", createOrderJson("ORD0001", "CUST001", "PROD001", 2, 100.00));

        // Should receive enriched order
        assertThat(enrichedOutput.isEmpty()).isFalse();

        String result = enrichedOutput.readValue();
        JsonNode enriched = objectMapper.readTree(result);

        // Verify order data preserved
        assertThat(enriched.get("order_id").asText()).isEqualTo("ORD0001");
        assertThat(enriched.get("customer_id").asText()).isEqualTo("CUST001");
        assertThat(enriched.get("quantity").asInt()).isEqualTo(2);

        // Verify customer data added
        assertThat(enriched.has("customer")).isTrue();
        assertThat(enriched.get("customer").get("name").asText()).isEqualTo("Alice Johnson");
        assertThat(enriched.get("customer").get("email").asText()).isEqualTo("alice@email.com");
        assertThat(enriched.get("customer").get("region").asText()).isEqualTo("US-WEST");
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-join.yaml")
    void testRekeyingForJoin() throws Exception {
        // Populate customer table
        customersInput.pipeInput("CUST002", createCustomerJson("CUST002", "Bob Smith", "bob@email.com", "US-EAST"));

        // Send order with different order_id but same customer
        ordersInput.pipeInput("ORD0002", createOrderJson("ORD0002", "CUST002", "PROD002", 1, 50.00));

        assertThat(enrichedOutput.isEmpty()).isFalse();

        String result = enrichedOutput.readValue();
        JsonNode enriched = objectMapper.readTree(result);

        // Verify rekeying worked - customer data matched by customer_id
        assertThat(enriched.get("customer").get("name").asText()).isEqualTo("Bob Smith");
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-join.yaml")
    void testRestoreOriginalKey() throws Exception {
        // Populate customer
        customersInput.pipeInput("CUST003", createCustomerJson("CUST003", "Carol Davis", "carol@email.com", "EU-WEST"));

        // Send order
        ordersInput.pipeInput("ORD0003", createOrderJson("ORD0003", "CUST003", "PROD003", 3, 150.00));

        assertThat(enrichedOutput.isEmpty()).isFalse();

        // Read with key
        var record = enrichedOutput.readRecord();

        // Verify key is restored to order_id (not customer_id)
        assertThat(record.getKey()).isEqualTo("ORD0003");

        JsonNode enriched = objectMapper.readTree(record.getValue());
        assertThat(enriched.get("order_id").asText()).isEqualTo("ORD0003");
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-join.yaml")
    void testMultipleOrdersSameCustomer() throws Exception {
        // Populate customer
        customersInput.pipeInput("CUST004", createCustomerJson("CUST004", "David Wilson", "david@email.com", "ASIA-PACIFIC"));

        // Send multiple orders for same customer
        ordersInput.pipeInput("ORD0004", createOrderJson("ORD0004", "CUST004", "PROD001", 1, 25.00));
        ordersInput.pipeInput("ORD0005", createOrderJson("ORD0005", "CUST004", "PROD002", 2, 50.00));
        ordersInput.pipeInput("ORD0006", createOrderJson("ORD0006", "CUST004", "PROD003", 1, 75.00));

        // Should receive 3 enriched orders
        assertThat(enrichedOutput.getQueueSize()).isEqualTo(3);

        // Verify all have same customer data
        for (int i = 0; i < 3; i++) {
            String result = enrichedOutput.readValue();
            JsonNode enriched = objectMapper.readTree(result);
            assertThat(enriched.get("customer").get("name").asText()).isEqualTo("David Wilson");
            assertThat(enriched.get("customer").get("region").asText()).isEqualTo("ASIA-PACIFIC");
        }
    }

    private String createOrderJson(String orderId, String customerId, String productId, int quantity, double amount) throws Exception {
        Map<String, Object> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("customer_id", customerId);
        order.put("product_id", productId);
        order.put("quantity", quantity);
        order.put("amount", amount);
        order.put("timestamp", System.currentTimeMillis());

        return objectMapper.writeValueAsString(order);
    }

    private String createCustomerJson(String customerId, String name, String email, String region) throws Exception {
        Map<String, Object> customer = new HashMap<>();
        customer.put("name", name);
        customer.put("email", email);
        customer.put("region", region);
        customer.put("status", "active");

        return objectMapper.writeValueAsString(customer);
    }
}
