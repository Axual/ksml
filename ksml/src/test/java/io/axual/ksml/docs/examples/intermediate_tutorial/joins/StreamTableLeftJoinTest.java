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
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for stream-table left join (activity enrichment with location) in KSML.
 * <p>
 * The stream-table left join demonstrates:
 * 1. Preserving all stream records (activities) even when table data is missing
 * 2. Enriching with location data when available
 * 3. Handling null values gracefully with default fallbacks
 * <p>
 * What these Tests Validate (KSML Translation):
 * <p>
 * 1. testActivityEnrichedWithLocation: leftJoin adds location data when available
 * 2. testActivityWithoutLocation: leftJoin preserves activity when location missing
 * 3. testEnrichmentMetadata: Python function correctly sets enriched flag
 * 4. testDefaultLocationValues: Python null-handling provides default values
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class StreamTableLeftJoinTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "user_activity_events")
    TestInputTopic<String, String> activityInput;

    @KSMLTopic(topic = "user_location_data")
    TestInputTopic<String, String> locationInput;

    @KSMLTopic(topic = "activity_with_location")
    TestOutputTopic<String, String> enrichedOutput;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-left-join.yaml")
    void testActivityEnrichedWithLocation() throws Exception {
        // Populate location table
        locationInput.pipeInput("user001", createLocationJson("user001", "USA", "New York", "EST"));

        // Send activity for user with location
        activityInput.pipeInput("user001", createActivityJson("activity_001", "user001", "login", "session_001"));

        // Should receive enriched activity
        assertThat(enrichedOutput.isEmpty()).isFalse();

        String result = enrichedOutput.readValue();
        JsonNode enriched = objectMapper.readTree(result);

        // Verify activity data preserved
        assertThat(enriched.get("activity_id").asText()).isEqualTo("activity_001");
        assertThat(enriched.get("user_id").asText()).isEqualTo("user001");
        assertThat(enriched.get("activity_type").asText()).isEqualTo("login");

        // Verify location data added
        assertThat(enriched.get("location").get("country").asText()).isEqualTo("USA");
        assertThat(enriched.get("location").get("city").asText()).isEqualTo("New York");
        assertThat(enriched.get("location").get("timezone").asText()).isEqualTo("EST");

        // Verify enrichment flag
        assertThat(enriched.get("enriched").asBoolean()).isTrue();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-left-join.yaml")
    void testActivityWithoutLocation() throws Exception {
        // Do NOT populate location for user004

        // Send activity for user without location data
        activityInput.pipeInput("user004", createActivityJson("activity_002", "user004", "page_view", "session_004"));

        // Should still receive activity (left join preserves all)
        assertThat(enrichedOutput.isEmpty()).isFalse();

        String result = enrichedOutput.readValue();
        JsonNode enriched = objectMapper.readTree(result);

        // Verify activity data preserved
        assertThat(enriched.get("activity_id").asText()).isEqualTo("activity_002");
        assertThat(enriched.get("user_id").asText()).isEqualTo("user004");
        assertThat(enriched.get("activity_type").asText()).isEqualTo("page_view");

        // Verify default location values used
        assertThat(enriched.get("location").get("country").asText()).isEqualTo("UNKNOWN");
        assertThat(enriched.get("location").get("city").asText()).isEqualTo("UNKNOWN");
        assertThat(enriched.get("location").get("timezone").asText()).isEqualTo("UTC");

        // Verify enrichment flag
        assertThat(enriched.get("enriched").asBoolean()).isFalse();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-left-join.yaml")
    void testEnrichmentMetadata() throws Exception {
        // Populate location
        locationInput.pipeInput("user002", createLocationJson("user002", "UK", "London", "GMT"));

        // Send activity
        activityInput.pipeInput("user002", createActivityJson("activity_003", "user002", "purchase", "session_002"));

        String result = enrichedOutput.readValue();
        JsonNode enriched = objectMapper.readTree(result);

        // Verify enrichment metadata added by Python function
        assertThat(enriched.has("enriched")).isTrue();
        assertThat(enriched.has("enriched_at")).isTrue();
        assertThat(enriched.get("enriched").asBoolean()).isTrue();
        assertThat(enriched.get("enriched_at").asLong()).isGreaterThan(0);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/joins/processor-stream-table-left-join.yaml")
    void testDefaultLocationValues() throws Exception {
        // Test multiple users without location data
        activityInput.pipeInput("user005", createActivityJson("activity_004", "user005", "search", "session_005"));
        activityInput.pipeInput("user006", createActivityJson("activity_005", "user006", "logout", "session_006"));

        assertThat(enrichedOutput.getQueueSize()).isEqualTo(2);

        // Verify both get default values
        for (int i = 0; i < 2; i++) {
            String result = enrichedOutput.readValue();
            JsonNode enriched = objectMapper.readTree(result);

            assertThat(enriched.get("location").get("country").asText()).isEqualTo("UNKNOWN");
            assertThat(enriched.get("location").get("city").asText()).isEqualTo("UNKNOWN");
            assertThat(enriched.get("location").get("timezone").asText()).isEqualTo("UTC");
            assertThat(enriched.get("enriched").asBoolean()).isFalse();
        }
    }

    private String createActivityJson(String activityId, String userId, String activityType, String sessionId) throws Exception {
        Map<String, Object> activity = new HashMap<>();
        activity.put("activity_id", activityId);
        activity.put("user_id", userId);
        activity.put("activity_type", activityType);
        activity.put("timestamp", System.currentTimeMillis());
        activity.put("session_id", sessionId);

        return objectMapper.writeValueAsString(activity);
    }

    private String createLocationJson(String userId, String country, String city, String timezone) throws Exception {
        Map<String, Object> location = new HashMap<>();
        location.put("user_id", userId);
        location.put("country", country);
        location.put("city", city);
        location.put("timezone", timezone);
        location.put("updated_at", System.currentTimeMillis());

        return objectMapper.writeValueAsString(location);
    }
}
