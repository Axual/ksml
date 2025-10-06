package io.axual.ksml.docs.examples.reference.operations;

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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for repartition operation in KSML.
 * <p>
 * The repartition operation demonstrates intelligent data redistribution:
 * 1. Changes the key from region to user_id (using mapKey)
 * 2. Adds processing metadata to track the transformation (using transformValue)
 * 3. Applies custom partitioning logic to distribute data across 4 partitions
 * <p>
 * Custom partitioner logic:
 * - Even user numbers (user_002, user_004) -> partitions 0-1
 * - Odd user numbers (user_001, user_003, user_005) -> partitions 2-3
 * <p>
 * What these tests validate (KSML Translation):
 * 1. testKeyTransformation: mapKey Python function correctly extracts user_id
 * 2. testValueEnrichment: transformValue Python function adds fields and preserves originals
 * 3. testMultipleUsersRepartitioning: Repartition processes all records
 * 4. testRepartitionWithMissingUserIdField: Python mapKey fallback to original key works
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class RepartitionExampleTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "user_activities")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "repartitioned_activities")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/reference/operations/repartition-example-processor.yaml")
    void testKeyTransformation() throws Exception {
        // Send activity with region as key
        String activityJson = createActivityJson("user_001", "login", "north");
        inputTopic.pipeInput("north", activityJson);

        // Verify key changed from region to user_id
        KeyValue<String, String> result = outputTopic.readKeyValue();
        assertThat(result.key).isEqualTo("user_001");
    }

    @KSMLTest(topology = "docs-examples/reference/operations/repartition-example-processor.yaml")
    void testValueEnrichment() throws Exception {
        // Send activity
        String activityJson = createActivityJson("user_002", "purchase", "south");
        inputTopic.pipeInput("south", activityJson);

        // Verify value enrichment
        KeyValue<String, String> result = outputTopic.readKeyValue();
        JsonNode output = objectMapper.readTree(result.value);

        // Check processing_info was added
        assertThat(output.has("processing_info")).isTrue();
        assertThat(output.get("processing_info").asText())
                .contains("Repartitioned by user: user_002");

        // Check original_region was added (note: this is the new key, not the original region)
        // The transformValue happens after mapKey, so "key" in the mapper refers to user_id
        assertThat(output.has("original_region")).isTrue();
        assertThat(output.get("original_region").asText()).isEqualTo("user_002");

        // Verify original fields preserved
        assertThat(output.get("user_id").asText()).isEqualTo("user_002");
        assertThat(output.get("activity_type").asText()).isEqualTo("purchase");
        assertThat(output.get("region").asText()).isEqualTo("south");
    }

    @KSMLTest(topology = "docs-examples/reference/operations/repartition-example-processor.yaml")
    void testMultipleUsersRepartitioning() throws Exception {
        // Send activities for multiple users from different regions
        inputTopic.pipeInput("north", createActivityJson("user_001", "login", "north"));
        inputTopic.pipeInput("south", createActivityJson("user_002", "browse", "south"));
        inputTopic.pipeInput("east", createActivityJson("user_003", "purchase", "east"));
        inputTopic.pipeInput("west", createActivityJson("user_004", "search", "west"));
        inputTopic.pipeInput("north", createActivityJson("user_005", "logout", "north"));

        // Read all results
        List<KeyValue<String, String>> results = outputTopic.readKeyValuesToList();
        assertThat(results).hasSize(5);

        // Verify all users were repartitioned correctly by user_id
        assertThat(results).extracting(kv -> kv.key)
                .containsExactlyInAnyOrder("user_001", "user_002", "user_003", "user_004", "user_005");

        // Verify all activities have processing info
        for (KeyValue<String, String> result : results) {
            JsonNode output = objectMapper.readTree(result.value);
            assertThat(output.has("processing_info")).isTrue();
            assertThat(output.get("processing_info").asText())
                    .startsWith("Repartitioned by user:");
        }
    }

    @KSMLTest(topology = "docs-examples/reference/operations/repartition-example-processor.yaml")
    void testRepartitionWithMissingUserIdField() throws Exception {
        // Send activity without user_id field
        Map<String, Object> activity = new HashMap<>();
        activity.put("activity_id", "activity_9999");
        activity.put("activity_type", "unknown");
        activity.put("region", "west");

        String activityJson = objectMapper.writeValueAsString(activity);
        inputTopic.pipeInput("west", activityJson);

        // Should still be processed (mapKey falls back to original key)
        assertThat(outputTopic.isEmpty()).isFalse();

        KeyValue<String, String> result = outputTopic.readKeyValue();
        // Key should remain as original (region) when user_id is missing
        assertThat(result.key).isEqualTo("west");
    }

    private String createActivityJson(String userId, String activityType, String region) throws Exception {
        Map<String, Object> activity = new HashMap<>();
        activity.put("activity_id", "activity_" + System.currentTimeMillis());
        activity.put("user_id", userId);
        activity.put("activity_type", activityType);
        activity.put("timestamp", System.currentTimeMillis());
        activity.put("region", region);
        return objectMapper.writeValueAsString(activity);
    }
}
