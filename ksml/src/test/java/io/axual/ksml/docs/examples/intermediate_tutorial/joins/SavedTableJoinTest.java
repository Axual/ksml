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

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for joining with a table saved with "as" by an earlier pipeline (orders enriched with customers and their
 * loyalty records) in KSML.
 * <p>
 * What these Tests Validate (KSML Translation):
 * <p>
 * 1. testOrderEnrichedWithCustomerAndLoyalty: the order is joined with the table saved by the first pipeline
 * 2. testCustomerWithoutLoyaltyIsNotEnriched: the saved table is an inner table-table join, so a customer without
 *    a loyalty record is not in it and the order is dropped
 * 3. testLoyaltyUpdateIsUsedByLaterOrders: an update of the loyalty table flows through the saved table
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class SavedTableJoinTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "new_orders")
    TestInputTopic<String, String> ordersInput;

    @KSMLTopic(topic = "customer_data")
    TestInputTopic<String, String> customersInput;

    @KSMLTopic(topic = "customer_loyalty")
    TestInputTopic<String, String> loyaltyInput;

    @KSMLTopic(topic = "orders_with_customer_loyalty")
    TestOutputTopic<String, String> enrichedOutput;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-join-saved-table.yaml")
    void testOrderEnrichedWithCustomerAndLoyalty() throws Exception {
        customersInput.pipeInput("CUST001", toJson(Map.of("name", "Alice Johnson", "region", "US-WEST")));
        loyaltyInput.pipeInput("CUST001", toJson(Map.of("tier", "gold", "points", 1200)));
        ordersInput.pipeInput("ORD0001", toJson(Map.of("order_id", "ORD0001", "customer_id", "CUST001", "amount", 100.0)));

        final var outputRecord = enrichedOutput.readRecord();
        assertThat(outputRecord.getKey()).isEqualTo("ORD0001");
        final JsonNode enriched = objectMapper.readTree(outputRecord.getValue());
        assertThat(enriched.get("order_id").asString()).isEqualTo("ORD0001");
        assertThat(enriched.get("customer").get("name").asString()).isEqualTo("Alice Johnson");
        assertThat(enriched.get("customer").get("loyalty").get("tier").asString()).isEqualTo("gold");
        assertThat(enriched.get("customer").get("loyalty").get("points").asInt()).isEqualTo(1200);
        assertThat(enrichedOutput.isEmpty()).isTrue();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-join-saved-table.yaml")
    void testCustomerWithoutLoyaltyIsNotEnriched() {
        customersInput.pipeInput("CUST005", toJson(Map.of("name", "Eva Brown", "region", "EU-CENTRAL")));
        ordersInput.pipeInput("ORD0002", toJson(Map.of("order_id", "ORD0002", "customer_id", "CUST005", "amount", 50.0)));

        assertThat(enrichedOutput.isEmpty()).isTrue();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-join-saved-table.yaml")
    void testLoyaltyUpdateIsUsedByLaterOrders() {
        customersInput.pipeInput("CUST002", toJson(Map.of("name", "Bob Smith", "region", "US-EAST")));
        loyaltyInput.pipeInput("CUST002", toJson(Map.of("tier", "silver", "points", 450)));
        ordersInput.pipeInput("ORD0003", toJson(Map.of("order_id", "ORD0003", "customer_id", "CUST002", "amount", 20.0)));
        loyaltyInput.pipeInput("CUST002", toJson(Map.of("tier", "gold", "points", 1050)));
        ordersInput.pipeInput("ORD0004", toJson(Map.of("order_id", "ORD0004", "customer_id", "CUST002", "amount", 30.0)));

        final var tiers = enrichedOutput.readValuesToList().stream()
                .map(value -> objectMapper.readTree(value).get("customer").get("loyalty").get("tier").asString())
                .toList();
        assertThat(tiers).containsExactly("silver", "gold");
    }

    private String toJson(Map<String, Object> value) {
        return objectMapper.writeValueAsString(value);
    }
}
