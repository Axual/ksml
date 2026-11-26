package io.axual.ksml.docs.examples.reference.functions;

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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

// The foreignKeyExtractor example from the function reference: orders joined with customers by customer_id
@ExtendWith(KSMLTestExtension.class)
class ForeignKeyExtractorTest {

    private static final String TOPOLOGY = "docs-examples/reference/functions/foreignkeyextractor-processor.yaml";
    private static final String KEY_TOPOLOGY = "docs-examples/reference/functions/foreignkeyextractor-key-processor.yaml";
    private static final String ALICE = "{\"customer_id\": \"c001\", \"name\": \"Alice\", \"email\": \"alice@example.com\", \"tier\": \"gold\"}";
    private static final String ORDER = "{\"order_id\": \"o001\", \"customer_id\": \"c001\", \"product\": \"book\", \"amount\": 10, \"timestamp\": 1}";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "orders")
    TestInputTopic<String, String> orders;

    @KSMLTopic(topic = "customers")
    TestInputTopic<String, String> customers;

    @KSMLTopic(topic = "enriched_orders")
    TestOutputTopic<String, String> enrichedOrders;

    @KSMLTopic(topic = "keyed_orders")
    TestInputTopic<String, String> keyedOrders;

    @KSMLTopic(topic = "enriched_keyed_orders")
    TestOutputTopic<String, String> enrichedKeyedOrders;

    @KSMLTest(topology = TOPOLOGY)
    @DisplayName("an order is enriched with the customer that the foreignKeyExtractor finds, and keeps its order key")
    void testOrderEnrichedWithCustomer() {
        customers.pipeInput("c001", ALICE);
        orders.pipeInput("o001", ORDER);

        final var result = enrichedOrders.readKeyValue();
        assertThat(result.key).isEqualTo("o001");
        final var customer = objectMapper.readTree(result.value).get("customer");
        assertThat(customer.get("customer_id").asString()).isEqualTo("c001");
        assertThat(customer.get("name").asString()).isEqualTo("Alice");
        assertThat(customer.get("tier").asString()).isEqualTo("gold");
    }

    @KSMLTest(topology = TOPOLOGY)
    @DisplayName("an order that arrives before its customer is enriched when the customer arrives")
    void testCustomerArrivesAfterOrder() {
        orders.pipeInput("o001", ORDER);
        assertThat(enrichedOrders.isEmpty()).isTrue();

        customers.pipeInput("c001", ALICE);

        final var result = enrichedOrders.readKeyValue();
        assertThat(result.key).isEqualTo("o001");
        assertThat(objectMapper.readTree(result.value).get("customer").get("name").asString()).isEqualTo("Alice");
    }

    @KSMLTest(topology = TOPOLOGY)
    @DisplayName("a deleted customer sends the order key with a null value, which the peek accepts")
    void testDeletedCustomerSendsNullValue() {
        customers.pipeInput("c001", ALICE);
        orders.pipeInput("o001", ORDER);
        enrichedOrders.readKeyValue();

        customers.pipeInput("c001", (String) null);

        final var result = enrichedOrders.readKeyValue();
        assertThat(result.key).isEqualTo("o001");
        assertThat(result.value).isNull();
    }

    @KSMLTest(topology = KEY_TOPOLOGY)
    @DisplayName("a foreignKeyExtractor that reads the key finds the customer from the order key")
    void testForeignKeyFromKey() {
        customers.pipeInput("c001", ALICE);
        keyedOrders.pipeInput("c001/ord_001", "{\"product\": \"laptop\", \"amount\": 500}");

        final var result = enrichedKeyedOrders.readKeyValue();
        assertThat(result.key).isEqualTo("c001/ord_001");
        final var enriched = objectMapper.readTree(result.value);
        assertThat(enriched.get("customer").asString()).isEqualTo("Alice");
        assertThat(enriched.get("tier").asString()).isEqualTo("gold");
        assertThat(enriched.get("product").asString()).isEqualTo("laptop");
    }

    @KSMLTest(topology = KEY_TOPOLOGY)
    @DisplayName("an order whose key names an unknown customer is not joined")
    void testUnknownCustomerInKey() {
        customers.pipeInput("c001", ALICE);
        keyedOrders.pipeInput("c002/ord_002", "{\"product\": \"phone\", \"amount\": 200}");

        assertThat(enrichedKeyedOrders.isEmpty()).isTrue();
    }

    @KSMLTest(topology = KEY_TOPOLOGY)
    @DisplayName("an empty order value for a known customer does not stop the stream")
    void testEmptyOrderForKnownCustomer() {
        customers.pipeInput("c001", ALICE);
        keyedOrders.pipeInput("c001/ord_001", "");

        // The valueJoiner gets None for the order and returns None, so the order is deleted from the result
        final var result = enrichedKeyedOrders.readKeyValue();
        assertThat(result.key).isEqualTo("c001/ord_001");
        assertThat(result.value).isNull();
        assertThat(enrichedKeyedOrders.isEmpty()).isTrue();
    }

    @KSMLTest(topology = KEY_TOPOLOGY)
    @DisplayName("an empty order value, then its customer, does not stop the stream")
    void testEmptyOrderBeforeCustomer() {
        keyedOrders.pipeInput("c001/ord_001", "");
        customers.pipeInput("c001", ALICE);

        // The valueJoiner gets None for the order and returns None, so the order is deleted from the result
        final var result = enrichedKeyedOrders.readKeyValue();
        assertThat(result.key).isEqualTo("c001/ord_001");
        assertThat(result.value).isNull();
        assertThat(enrichedKeyedOrders.isEmpty()).isTrue();
    }
}
