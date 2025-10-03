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
 *
 *   What these Tests Validate:
 *
 *   1. testSingleUserSessionCounting: Basic counting works (3 clicks -> count of 3)
 *   2. testMultipleUsersIndependentSessions: Each user gets their own session
 *     - Alice's 2 clicks don't mix with Bob's 3 clicks
 *   3. testSessionWindowInactivityGap: The 2-minute gap actually works
 *     - Sends 2 clicks, advances time 2m30s -> session closes
 *     - Sends 3 more clicks -> new session created
 *   4. testSessionWindowMerging: Clicks within 2 minutes merge into one session
 *     - Click at 0s, click at 1m30s, click at 3m -> all in ONE session (each extends the window)
 *   5. testEmptySessionBehavior: Single clicks create valid sessions
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

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml")
    void testSessionWindowInactivityGap() throws Exception {
        long baseTime = Instant.parse("2025-01-01T10:00:00Z").toEpochMilli();

        // First session: 2 clicks close together
        inputTopic.pipeInput("user1", createClickJson("page1"), baseTime);
        inputTopic.pipeInput("user1", createClickJson("page2"), baseTime + 30000);

        // Wait beyond inactivity gap (2 minutes) to close first session
        testDriver.advanceWallClockTime(Duration.ofMinutes(2).plusSeconds(30));

        // Second session: 3 clicks after gap
        long secondSessionStart = baseTime + Duration.ofMinutes(3).toMillis();
        inputTopic.pipeInput("user1", createClickJson("page3"), secondSessionStart);
        inputTopic.pipeInput("user1", createClickJson("page4"), secondSessionStart + 10000);
        inputTopic.pipeInput("user1", createClickJson("page5"), secondSessionStart + 20000);

        // Close second session
        testDriver.advanceWallClockTime(Duration.ofMinutes(2).plusSeconds(30));

        // Verify two separate sessions were created
        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        // Should have multiple session windows for user1
        long sessionCount = results.stream()
                .filter(kv -> kv.key.contains("user1"))
                .count();

        assertThat(sessionCount).isGreaterThan(2); // Multiple updates per session

        // Verify we have both a session with count=2 and count=3
        boolean hasSession2 = results.stream()
                .anyMatch(kv -> kv.key.contains("user1") && kv.value != null && kv.value == 2L);
        boolean hasSession3 = results.stream()
                .anyMatch(kv -> kv.key.contains("user1") && kv.value != null && kv.value == 3L);

        assertThat(hasSession2).isTrue();
        assertThat(hasSession3).isTrue();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml")
    void testSessionWindowMerging() throws Exception {
        long baseTime = Instant.parse("2025-01-01T10:00:00Z").toEpochMilli();

        // Send clicks that should merge into one session (all within 2m of each other)
        inputTopic.pipeInput("user1", createClickJson("page1"), baseTime);
        inputTopic.pipeInput("user1", createClickJson("page2"), baseTime + 90000);  // +1m30s
        inputTopic.pipeInput("user1", createClickJson("page3"), baseTime + 180000); // +3m (extends session)

        // Close the session
        testDriver.advanceWallClockTime(Duration.ofMinutes(2).plusSeconds(30));

        // Verify all clicks counted in single merged session
        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        long maxCount = results.stream()
                .filter(kv -> kv.key.contains("user1") && kv.value != null)
                .mapToLong(kv -> kv.value)
                .max()
                .orElse(0L);

        assertThat(maxCount).isEqualTo(3L);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-session-activity.yaml")
    void testEmptySessionBehavior() throws Exception {
        long baseTime = Instant.parse("2025-01-01T10:00:00Z").toEpochMilli();

        // Send single click
        inputTopic.pipeInput("user1", createClickJson("page1"), baseTime);

        // Close the session
        testDriver.advanceWallClockTime(Duration.ofMinutes(2).plusSeconds(30));

        // Verify single-click session produces output
        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        assertThat(results).isNotEmpty();
        assertThat(results).anyMatch(kv -> kv.key.contains("user1") && kv.value != null && kv.value == 1L);
    }

    private String createClickJson(String page) throws Exception {
        Map<String, Object> click = new HashMap<>();
        click.put("page", page);
        click.put("timestamp", System.currentTimeMillis());
        return objectMapper.writeValueAsString(click);
    }
}
