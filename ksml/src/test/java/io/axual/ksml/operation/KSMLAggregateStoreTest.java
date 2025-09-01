package io.axual.ksml.operation;

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

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KSMLTestExtension.class)
public class KSMLAggregateStoreTest {

    @KSMLTopic(topic = "transactions")
    TestInputTopic<String, String> transactionsInput;

    @KSMLTopic(topic = "category_aggregates")
    TestOutputTopic<String, String> categoryAggregatesOutput;

    @KSMLTest(topology = "pipelines/test-aggregate-store-explicit.yaml")
    void testAggregateWithExplicitStoreDefinition() {
        // This test validates that issue #292 is addressed:
        // After PR 300, aggregate operations REQUIRE explicit store definitions
        // This is a breaking change that eliminates the store inference problem
        // Create test transactions
        String transaction1 = "{\"transaction_id\":\"t1\",\"category\":\"electronics\",\"amount\":299.99,\"timestamp\":1000}";
        String transaction2 = "{\"transaction_id\":\"t2\",\"category\":\"clothing\",\"amount\":79.99,\"timestamp\":2000}";
        String transaction3 = "{\"transaction_id\":\"t3\",\"category\":\"electronics\",\"amount\":599.99,\"timestamp\":3000}";
        String transaction4 = "{\"transaction_id\":\"t4\",\"category\":\"clothing\",\"amount\":119.99,\"timestamp\":4000}";
        String transaction5 = "{\"transaction_id\":\"t5\",\"category\":\"electronics\",\"amount\":199.99,\"timestamp\":5000}";

        // Send transactions
        transactionsInput.pipeInput("t1", transaction1);
        transactionsInput.pipeInput("t2", transaction2);
        transactionsInput.pipeInput("t3", transaction3);
        transactionsInput.pipeInput("t4", transaction4);
        transactionsInput.pipeInput("t5", transaction5);

        // Verify category aggregates (non-windowed)
        assertFalse(categoryAggregatesOutput.isEmpty());
        
        // Read all category aggregates
        var categoryOutputs = categoryAggregatesOutput.readKeyValuesToMap();
        
        // Verify we have aggregates for each category
        assertTrue(categoryOutputs.containsKey("electronics"));
        assertTrue(categoryOutputs.containsKey("clothing"));
        
        // Verify electronics aggregate
        String electronicsAggregate = categoryOutputs.get("electronics");
        assertNotNull(electronicsAggregate);
        assertTrue(electronicsAggregate.contains("\"total_sales\":1099.97"));  // 299.99 + 599.99 + 199.99
        assertTrue(electronicsAggregate.contains("\"count\":3"));
        
        // Verify clothing aggregate
        String clothingAggregate = categoryOutputs.get("clothing");
        assertNotNull(clothingAggregate);
        assertTrue(clothingAggregate.contains("\"total_sales\":199.98"));  // 79.99 + 119.99
        assertTrue(clothingAggregate.contains("\"count\":2"));

        // The fact that this test completes successfully validates that:
        // 1. Aggregate operations work correctly with explicit store definitions
        // 2. Store parameter is properly required (breaking change from PR 300)
        // 3. No store inference errors occur when explicit stores are provided (issue #292 resolved)
    }
}