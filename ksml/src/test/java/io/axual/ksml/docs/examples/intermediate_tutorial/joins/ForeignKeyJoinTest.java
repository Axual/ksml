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
 * Test for stream-globalTable foreign key join (order enrichment with product catalog) in KSML.
 * <p>
 * The foreign key join demonstrates:
 * 1. Using keyValueMapper to extract foreign key (product_id) from order
 * 2. Joining with globalTable (no co-partitioning required)
 * 3. Enriching orders with product details from catalog
 * <p>
 * What these Tests Validate (KSML Translation):
 *
 * 1. testOrderEnrichedWithProduct: join with globalTable adds product details
 * 2. testForeignKeyExtraction: keyValueMapper correctly extracts product_id
 * 3. testTotalPriceCalculation: Python valueJoiner computes total_price
 * 4. testMultipleProductsInCatalog: GlobalTable lookup works for different products
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class ForeignKeyJoinTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "new_orders")
    TestInputTopic<String, String> ordersInput;

    @KSMLTopic(topic = "product_catalog")
    TestInputTopic<String, String> productsInput;

    @KSMLTopic(topic = "orders_with_product_details")
    TestOutputTopic<String, String> enrichedOutput;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-foreign-key-join.yaml")
    void testOrderEnrichedWithProduct() throws Exception {
        // Populate product catalog (globalTable)
        productsInput.pipeInput("PROD001", createProductJson("Laptop", "Electronics", 999.99));

        // Send order referencing that product
        ordersInput.pipeInput("ORD0001", createOrderJson("ORD0001", "CUST001", "PROD001", 2));

        // Should receive enriched order
        assertThat(enrichedOutput.isEmpty()).isFalse();

        String result = enrichedOutput.readValue();
        JsonNode enriched = objectMapper.readTree(result);

        // Verify order data preserved
        assertThat(enriched.get("order_id").asText()).isEqualTo("ORD0001");
        assertThat(enriched.get("product_id").asText()).isEqualTo("PROD001");
        assertThat(enriched.get("quantity").asInt()).isEqualTo(2);

        // Verify product details added
        assertThat(enriched.has("product_details")).isTrue();
        assertThat(enriched.get("product_details").get("name").asText()).isEqualTo("Laptop");
        assertThat(enriched.get("product_details").get("category").asText()).isEqualTo("Electronics");
        assertThat(enriched.get("product_details").get("price").asDouble()).isEqualTo(999.99);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-foreign-key-join.yaml")
    void testForeignKeyExtraction() throws Exception {
        // Populate multiple products
        productsInput.pipeInput("PROD002", createProductJson("Headphones", "Electronics", 149.99));
        productsInput.pipeInput("PROD003", createProductJson("Coffee Maker", "Appliances", 79.99));

        // Send orders with different product_ids (foreign keys)
        ordersInput.pipeInput("ORD0002", createOrderJson("ORD0002", "CUST002", "PROD002", 1));
        ordersInput.pipeInput("ORD0003", createOrderJson("ORD0003", "CUST003", "PROD003", 3));

        assertThat(enrichedOutput.getQueueSize()).isEqualTo(2);

        // Verify first order matched correct product
        String result1 = enrichedOutput.readValue();
        JsonNode enriched1 = objectMapper.readTree(result1);
        assertThat(enriched1.get("product_details").get("name").asText()).isEqualTo("Headphones");

        // Verify second order matched correct product
        String result2 = enrichedOutput.readValue();
        JsonNode enriched2 = objectMapper.readTree(result2);
        assertThat(enriched2.get("product_details").get("name").asText()).isEqualTo("Coffee Maker");
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-foreign-key-join.yaml")
    void testTotalPriceCalculation() throws Exception {
        // Populate product
        productsInput.pipeInput("PROD004", createProductJson("Backpack", "Accessories", 49.99));

        // Send order with quantity
        ordersInput.pipeInput("ORD0004", createOrderJson("ORD0004", "CUST004", "PROD004", 5));

        String result = enrichedOutput.readValue();
        JsonNode enriched = objectMapper.readTree(result);

        // Verify Python valueJoiner calculated total_price = quantity * price
        assertThat(enriched.has("total_price")).isTrue();
        double expectedTotal = 5 * 49.99;
        assertThat(enriched.get("total_price").asDouble()).isEqualTo(expectedTotal);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-foreign-key-join.yaml")
    void testMultipleProductsInCatalog() throws Exception {
        // Populate full product catalog
        productsInput.pipeInput("PROD001", createProductJson("Laptop", "Electronics", 999.99));
        productsInput.pipeInput("PROD002", createProductJson("Headphones", "Electronics", 149.99));
        productsInput.pipeInput("PROD003", createProductJson("Coffee Maker", "Appliances", 79.99));
        productsInput.pipeInput("PROD004", createProductJson("Backpack", "Accessories", 49.99));
        productsInput.pipeInput("PROD005", createProductJson("Desk Lamp", "Furniture", 39.99));

        // Send orders for various products
        ordersInput.pipeInput("ORD0005", createOrderJson("ORD0005", "CUST001", "PROD001", 1));
        ordersInput.pipeInput("ORD0006", createOrderJson("ORD0006", "CUST002", "PROD003", 2));
        ordersInput.pipeInput("ORD0007", createOrderJson("ORD0007", "CUST003", "PROD005", 4));

        assertThat(enrichedOutput.getQueueSize()).isEqualTo(3);

        // Verify each order got correct product details
        String result1 = enrichedOutput.readValue();
        JsonNode enriched1 = objectMapper.readTree(result1);
        assertThat(enriched1.get("product_details").get("name").asText()).isEqualTo("Laptop");
        assertThat(enriched1.get("product_details").get("category").asText()).isEqualTo("Electronics");

        String result2 = enrichedOutput.readValue();
        JsonNode enriched2 = objectMapper.readTree(result2);
        assertThat(enriched2.get("product_details").get("name").asText()).isEqualTo("Coffee Maker");
        assertThat(enriched2.get("product_details").get("category").asText()).isEqualTo("Appliances");

        String result3 = enrichedOutput.readValue();
        JsonNode enriched3 = objectMapper.readTree(result3);
        assertThat(enriched3.get("product_details").get("name").asText()).isEqualTo("Desk Lamp");
        assertThat(enriched3.get("product_details").get("category").asText()).isEqualTo("Furniture");
    }

    private String createOrderJson(String orderId, String customerId, String productId, int quantity) throws Exception {
        Map<String, Object> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("customer_id", customerId);
        order.put("product_id", productId);
        order.put("quantity", quantity);
        order.put("timestamp", System.currentTimeMillis());

        return objectMapper.writeValueAsString(order);
    }

    private String createProductJson(String name, String category, double price) throws Exception {
        Map<String, Object> product = new HashMap<>();
        product.put("name", name);
        product.put("category", category);
        product.put("price", price);
        product.put("in_stock", true);

        return objectMapper.writeValueAsString(product);
    }
}
