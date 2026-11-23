package io.axual.ksml.docs.examples.intermediate_tutorial.joins;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import tools.jackson.databind.ObjectMapper;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for the table-table foreign-key join (orders enriched with customers, both kept as tables) in KSML.
 * <p>
 * What these Tests Validate (KSML Translation):
 * <p>
 * 1. testOrderEnrichedWithCustomer: the foreignKeyExtractor finds the customer, and the result keeps the order key
 * 2. testCustomerUpdateUpdatesAllOrders: a customer update sends a new result for every order of that customer
 * 3. testOrderArrivesBeforeCustomer: an order without a customer is joined when the customer arrives
 * 4. testOrderMovesToOtherCustomer: an order that changes its customer_id is joined with the new customer
 * 5. testOrderDeleteSendsTombstone: a deleted order sends the order key with a null value
 * 6. testCustomerDeleteSendsTombstone: a deleted customer sends the order key with a null value
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class TableForeignKeyJoinTest {

    private static final String TOPOLOGY = "docs-examples/intermediate-tutorial/joins/processor-table-foreign-key-join.yaml";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "new_orders")
    TestInputTopic<String, String> ordersInput;

    @KSMLTopic(topic = "customer_data")
    TestInputTopic<String, String> customersInput;

    @KSMLTopic(topic = "orders_with_customer_details")
    TestOutputTopic<String, String> enrichedOutput;

    @KSMLTest(topology = TOPOLOGY)
    void testOrderEnrichedWithCustomer() {
        customersInput.pipeInput("CUST001", customer("Alice Johnson"));
        ordersInput.pipeInput("ORD0001", order("ORD0001", "CUST001"));

        final var outputRecord = enrichedOutput.readRecord();
        assertThat(outputRecord.getKey()).isEqualTo("ORD0001");
        final var enriched = objectMapper.readTree(outputRecord.getValue());
        assertThat(enriched.get("customer_id").asString()).isEqualTo("CUST001");
        assertThat(enriched.get("customer").get("name").asString()).isEqualTo("Alice Johnson");
        assertThat(enriched.get("customer").get("region").asString()).isEqualTo("US-WEST");
        assertThat(enrichedOutput.isEmpty()).isTrue();
    }

    @KSMLTest(topology = TOPOLOGY)
    void testCustomerUpdateUpdatesAllOrders() {
        customersInput.pipeInput("CUST001", customer("Alice Johnson"));
        ordersInput.pipeInput("ORD0001", order("ORD0001", "CUST001"));
        ordersInput.pipeInput("ORD0002", order("ORD0002", "CUST001"));
        enrichedOutput.readRecordsToList();

        customersInput.pipeInput("CUST001", customer("Alice Smith"));

        assertThat(enrichedOutput.readKeyValuesToList()).map(kv -> new KeyValue<>(kv.key, customerName(kv.value)))
                .containsExactlyInAnyOrder(new KeyValue<>("ORD0001", "Alice Smith"), new KeyValue<>("ORD0002", "Alice Smith"));
    }

    @KSMLTest(topology = TOPOLOGY)
    void testOrderArrivesBeforeCustomer() {
        ordersInput.pipeInput("ORD0001", order("ORD0001", "CUST002"));
        assertThat(enrichedOutput.isEmpty()).isTrue();

        customersInput.pipeInput("CUST002", customer("Bob Smith"));

        final var outputRecord = enrichedOutput.readRecord();
        assertThat(outputRecord.getKey()).isEqualTo("ORD0001");
        assertThat(customerName(outputRecord.getValue())).isEqualTo("Bob Smith");
    }

    @KSMLTest(topology = TOPOLOGY)
    void testOrderMovesToOtherCustomer() {
        customersInput.pipeInput("CUST001", customer("Alice Johnson"));
        customersInput.pipeInput("CUST002", customer("Bob Smith"));
        ordersInput.pipeInput("ORD0001", order("ORD0001", "CUST001"));
        ordersInput.pipeInput("ORD0001", order("ORD0001", "CUST002"));

        assertThat(enrichedOutput.readValuesToList()).map(this::customerName).containsExactly("Alice Johnson", "Bob Smith");
    }

    @KSMLTest(topology = TOPOLOGY)
    void testOrderDeleteSendsTombstone() {
        customersInput.pipeInput("CUST001", customer("Alice Johnson"));
        ordersInput.pipeInput("ORD0001", order("ORD0001", "CUST001"));
        enrichedOutput.readRecordsToList();

        ordersInput.pipeInput("ORD0001", (String) null);

        final var outputRecord = enrichedOutput.readRecord();
        assertThat(outputRecord.getKey()).isEqualTo("ORD0001");
        assertThat(outputRecord.getValue()).isNull();
    }

    @KSMLTest(topology = TOPOLOGY)
    void testCustomerDeleteSendsTombstone() {
        customersInput.pipeInput("CUST001", customer("Alice Johnson"));
        ordersInput.pipeInput("ORD0001", order("ORD0001", "CUST001"));
        enrichedOutput.readRecordsToList();

        customersInput.pipeInput("CUST001", (String) null);

        final var outputRecord = enrichedOutput.readRecord();
        assertThat(outputRecord.getKey()).isEqualTo("ORD0001");
        assertThat(outputRecord.getValue()).isNull();
    }

    private String customer(String name) {
        return objectMapper.writeValueAsString(Map.of("name", name, "email", "customer@email.com", "region", "US-WEST", "status", "active"));
    }

    private String order(String orderId, String customerId) {
        return objectMapper.writeValueAsString(Map.of("order_id", orderId, "customer_id", customerId, "amount", 100.0));
    }

    private String customerName(String value) {
        return objectMapper.readTree(value).get("customer").get("name").asString();
    }
}
