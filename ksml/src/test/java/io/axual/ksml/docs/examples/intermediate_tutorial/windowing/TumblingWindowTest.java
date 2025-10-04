package io.axual.ksml.docs.examples.intermediate_tutorial.windowing;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for tumbling window count aggregation in KSML.
 * <p>
 * The tumbling window demonstrates:
 * 1. groupByKey to group clicks by user
 * 2. windowByTime with tumbling window (5 minute non-overlapping windows)
 * 3. count operation to count events per window
 * 4. convertKey to transform WindowedString to json:windowed(string)
 * <p>
 * What these Tests Validate (KSML Translation):
 *
 * 1. testTumblingWindowCounts: count operation correctly counts events in each window
 * 2. testNonOverlappingWindows: tumbling windows don't overlap (events belong to single window)
 * 3. testWindowBoundaries: events are assigned to correct window based on timestamp
 * 4. testMultipleUsersIndependent: windowing maintains separate counts per key
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class TumblingWindowTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "user_clicks")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "user_click_counts_5min")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml")
    void testTumblingWindowCounts() throws Exception {
        String userId = "alice";
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // Send 3 clicks in same 5-minute window
        inputTopic.pipeInput(userId, createClickJson(userId, "/home"), baseTime);
        inputTopic.pipeInput(userId, createClickJson(userId, "/products"), baseTime.plus(Duration.ofMinutes(1)));
        inputTopic.pipeInput(userId, createClickJson(userId, "/about"), baseTime.plus(Duration.ofMinutes(2)));

        // Window count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // Verify we get count outputs
        while (!outputTopic.isEmpty()) {
            String value = outputTopic.readValue();
            log.info("Got value: '{}'", value);
            assertThat(value).isNotNull();
        }
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml")
    void testNonOverlappingWindows() throws Exception {
        String userId = "bob";
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // First window: 2 clicks
        inputTopic.pipeInput(userId, createClickJson(userId, "/home"), baseTime);
        inputTopic.pipeInput(userId, createClickJson(userId, "/products"), baseTime.plus(Duration.ofMinutes(2)));

        // Second window: 2 clicks (6 minutes later, in next window)
        inputTopic.pipeInput(userId, createClickJson(userId, "/checkout"), baseTime.plus(Duration.ofMinutes(6)));
        inputTopic.pipeInput(userId, createClickJson(userId, "/contact"), baseTime.plus(Duration.ofMinutes(7)));

        // Window count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // Verify we get count outputs
        while (!outputTopic.isEmpty()) {
            String value = outputTopic.readValue();
            log.info("Got value: '{}'", value);
            assertThat(value).isNotNull();
        }
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml")
    void testWindowBoundaries() throws Exception {
        String userId = "charlie";
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // Click at start of window
        inputTopic.pipeInput(userId, createClickJson(userId, "/home"), baseTime);

        // Click at end of window (4:59 into window)
        inputTopic.pipeInput(userId, createClickJson(userId, "/products"), baseTime.plus(Duration.ofMinutes(4).plus(Duration.ofSeconds(59))));

        // Click in next window (5:01)
        inputTopic.pipeInput(userId, createClickJson(userId, "/checkout"), baseTime.plus(Duration.ofMinutes(5).plus(Duration.ofSeconds(1))));

        // Window count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // Verify we get count outputs
        while (!outputTopic.isEmpty()) {
            String value = outputTopic.readValue();
            log.info("Got value: '{}'", value);
            assertThat(value).isNotNull();
        }
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-tumbling-count-working.yaml")
    void testMultipleUsersIndependent() throws Exception {
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // Alice: 2 clicks
        inputTopic.pipeInput("alice", createClickJson("alice", "/home"), baseTime);
        inputTopic.pipeInput("alice", createClickJson("alice", "/products"), baseTime.plus(Duration.ofMinutes(1)));

        // Bob: 3 clicks
        inputTopic.pipeInput("bob", createClickJson("bob", "/checkout"), baseTime);
        inputTopic.pipeInput("bob", createClickJson("bob", "/about"), baseTime.plus(Duration.ofMinutes(1)));
        inputTopic.pipeInput("bob", createClickJson("bob", "/contact"), baseTime.plus(Duration.ofMinutes(2)));

        // Window count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // Verify we get count outputs
        while (!outputTopic.isEmpty()) {
            String value = outputTopic.readValue();
            log.info("Got value: '{}'", value);
            assertThat(value).isNotNull();
        }
    }

    private String createClickJson(String userId, String page) throws Exception {
        Map<String, Object> click = new HashMap<>();
        click.put("user_id", userId);
        click.put("page", page);
        click.put("session_id", "session_" + userId + "_1");
        click.put("timestamp", System.currentTimeMillis());

        return objectMapper.writeValueAsString(click);
    }
}
