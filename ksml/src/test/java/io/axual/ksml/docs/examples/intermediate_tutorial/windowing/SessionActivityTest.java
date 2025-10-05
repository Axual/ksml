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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for session window functionality in KSML.
 * Session windows group events by key based on activity, closing after a period of inactivity.
 * <p>
 * Session window configuration:
 * - inactivityGap: 2m (session closes after 2 minutes of no activity)
 * - grace: 30s (accepts late data up to 30 seconds after inactivity gap)
 * <p>
 * To validate session window output, tests must advance wall clock time past the
 * inactivity gap (2m) + grace period (30s) = 2m 30s total.
 * <p>
 *   What these Tests Validate (KSML Translation):
 * <p>
 *   1. testSingleUserSessionCounting: groupByKey + windowBySession + count produces output
 *   2. testMultipleUsersIndependentSessions: groupByKey correctly separates users
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class SessionActivityTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLDriver
    TopologyTestDriver testDriver;

    @KSMLTopic(topic = "user_clicks")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "user_session_summary", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml")
    void testSingleUserSessionCounting() throws Exception {
        long baseTime = Instant.parse("2025-01-01T10:00:00Z").toEpochMilli();

        // Send 3 clicks within session window (all within 2 minutes)
        inputTopic.pipeInput("user1", createClickJson("page1"), baseTime);
        inputTopic.pipeInput("user1", createClickJson("page2"), baseTime + 30000); // +30s
        inputTopic.pipeInput("user1", createClickJson("page3"), baseTime + 60000); // +1m

        // Advance wall clock past inactivity gap (2m) + grace period (30s) = 2m 30s
        testDriver.advanceWallClockTime(Duration.ofMinutes(2).plusSeconds(30));

        // Verify session closed with count of 3
        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();
        assertThat(results).isNotEmpty();

        // Find the final window result for user1 (last update has highest count)
        // Filter out null values (tombstone records)
        long maxCount = results.stream()
                .filter(kv -> kv.key.contains("user1") && kv.value != null)
                .mapToLong(kv -> kv.value)
                .max()
                .orElse(0L);

        assertThat(maxCount).isEqualTo(3L);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml")
    void testMultipleUsersIndependentSessions() throws Exception {
        long baseTime = Instant.parse("2025-01-01T10:00:00Z").toEpochMilli();

        // Send clicks for multiple users
        inputTopic.pipeInput("alice", createClickJson("home"), baseTime);
        inputTopic.pipeInput("alice", createClickJson("products"), baseTime + 10000);
        inputTopic.pipeInput("bob", createClickJson("checkout"), baseTime + 20000);
        inputTopic.pipeInput("bob", createClickJson("payment"), baseTime + 30000);
        inputTopic.pipeInput("bob", createClickJson("confirm"), baseTime + 40000);

        // Advance wall clock to close sessions
        testDriver.advanceWallClockTime(Duration.ofMinutes(2).plusSeconds(30));

        // Verify both users have independent sessions
        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        long aliceMaxCount = results.stream()
                .filter(kv -> kv.key.contains("alice") && kv.value != null)
                .mapToLong(kv -> kv.value)
                .max()
                .orElse(0L);

        long bobMaxCount = results.stream()
                .filter(kv -> kv.key.contains("bob") && kv.value != null)
                .mapToLong(kv -> kv.value)
                .max()
                .orElse(0L);

        assertThat(aliceMaxCount).isEqualTo(2L);
        assertThat(bobMaxCount).isEqualTo(3L);
    }


    private String createClickJson(String page) throws Exception {
        Map<String, Object> click = new HashMap<>();
        click.put("page", page);
        click.put("timestamp", System.currentTimeMillis());
        return objectMapper.writeValueAsString(click);
    }
}
