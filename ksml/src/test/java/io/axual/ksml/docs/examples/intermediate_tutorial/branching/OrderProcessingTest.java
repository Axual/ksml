package io.axual.ksml.docs.examples.intermediate_tutorial.branching;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class OrderProcessingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "order_input")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "priority_orders")
    TestOutputTopic<String, String> priorityOutput;

    @KSMLTopic(topic = "regional_orders")
    TestOutputTopic<String, String> regionalOutput;

    @KSMLTopic(topic = "international_orders")
    TestOutputTopic<String, String> internationalOutput;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml")
    void testBranchingLogic() throws Exception {
        // Test priority (premium + high value)
        inputTopic.pipeInput("k1", createOrder("premium", "US", 1500.00));
        
        // Test regional (US/EU, not priority)
        inputTopic.pipeInput("k2", createOrder("standard", "EU", 500.00));
        
        // Test international (non-US/EU)
        inputTopic.pipeInput("k3", createOrder("basic", "APAC", 300.00));
        
        assertEquals(1, priorityOutput.readValuesToList().size());
        assertEquals(1, regionalOutput.readValuesToList().size());  
        assertEquals(1, internationalOutput.readValuesToList().size());
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml")
    void testPriorityBoundary() throws Exception {
        // Test boundary: exactly $1000 should NOT be priority
        inputTopic.pipeInput("k1", createOrder("premium", "US", 1000.00));
        
        // Test just over boundary: $1000.01 should be priority
        inputTopic.pipeInput("k2", createOrder("premium", "US", 1000.01));
        
        assertEquals(1, priorityOutput.readValuesToList().size());
        assertEquals(1, regionalOutput.readValuesToList().size());
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/branching/processor-order-processing.yaml")
    void testTransformations() throws Exception {
        inputTopic.pipeInput("key", createOrder("premium", "US", 1500.00));
        
        String result = priorityOutput.readValue();
        JsonNode order = objectMapper.readTree(result);
        
        assertEquals("priority", order.get("processing_tier").asText());
        assertEquals(4, order.get("sla_hours").asInt());
        assertNotNull(order.get("processed_at"));
    }
    
    private String createOrder(String customerType, String region, double totalAmount) throws Exception {
        Map<String, Object> order = new HashMap<>();
        order.put("order_id", "test_order");
        order.put("customer_type", customerType);
        order.put("region", region);
        order.put("total_amount", totalAmount);
        return objectMapper.writeValueAsString(order);
    }
}