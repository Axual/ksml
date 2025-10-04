package io.axual.ksml.docs.examples.advanced_tutorial.complex_event_processing;

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
 * Test for pattern detection (A->B->C sequence) in KSML.
 * <p>
 * The pattern detection demonstrates stateful event sequence processing:
 * 1. Tracks event sequences per session using state store
 * 2. Detects A->B->C pattern completion
 * 3. Filters out incomplete patterns
 * <p>
 * What these Tests Validate (KSML Translation):
 *
 * 1. testCompleteABCPattern: valueTransformer with state store detects A->B->C sequence
 * 2. testIncompletePattern: Filter correctly excludes partial patterns (A->B without C)
 * 3. testPatternReset: State reset works when sequence breaks (A->B->D resets)
 * 4. testMultipleSessionsIndependent: State stores track patterns independently per key
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class PatternDetectionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "pattern_events")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "detected_patterns")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/advanced-tutorial/complex-event-processing/processor-pattern-detection.yaml")
    void testCompleteABCPattern() throws Exception {
        String sessionId = "session_001";

        // Send A->B->C sequence
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "A", 1));
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "B", 2));
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "C", 3));

        // Should detect pattern on C event
        assertThat(outputTopic.isEmpty()).isFalse();

        String result = outputTopic.readValue();
        JsonNode detection = objectMapper.readTree(result);

        // Verify pattern detection structure
        assertThat(detection.get("pattern_type").asText()).isEqualTo("ABC_SEQUENCE");
        assertThat(detection.get("status").asText()).isEqualTo("DETECTED");
        assertThat(detection.get("session_id").asText()).isEqualTo(sessionId);
        assertThat(detection.has("completing_event")).isTrue();
        assertThat(detection.get("completing_event").get("event_type").asText()).isEqualTo("C");
    }

    @KSMLTest(topology = "docs-examples/advanced-tutorial/complex-event-processing/processor-pattern-detection.yaml")
    void testIncompletePattern() throws Exception {
        String sessionId = "session_002";

        // Send only A->B (incomplete pattern)
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "A", 1));
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "B", 2));

        // Should NOT detect pattern (filter removes null results)
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @KSMLTest(topology = "docs-examples/advanced-tutorial/complex-event-processing/processor-pattern-detection.yaml")
    void testPatternReset() throws Exception {
        String sessionId = "session_003";

        // Send A->B->D (breaks pattern, should reset)
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "A", 1));
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "B", 2));
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "D", 3));

        // No pattern detected (reset occurred)
        assertThat(outputTopic.isEmpty()).isTrue();

        // Now send fresh A->B->C sequence
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "A", 4));
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "B", 5));
        inputTopic.pipeInput(sessionId, createEventJson(sessionId, "C", 6));

        // Should detect pattern after reset
        assertThat(outputTopic.isEmpty()).isFalse();

        String result = outputTopic.readValue();
        JsonNode detection = objectMapper.readTree(result);
        assertThat(detection.get("pattern_type").asText()).isEqualTo("ABC_SEQUENCE");
    }

    @KSMLTest(topology = "docs-examples/advanced-tutorial/complex-event-processing/processor-pattern-detection.yaml")
    void testMultipleSessionsIndependent() throws Exception {
        // Session 1: Complete pattern
        inputTopic.pipeInput("session_001", createEventJson("session_001", "A", 1));
        inputTopic.pipeInput("session_001", createEventJson("session_001", "B", 2));

        // Session 2: Different pattern (should not interfere)
        inputTopic.pipeInput("session_002", createEventJson("session_002", "A", 3));

        // Session 1: Complete pattern
        inputTopic.pipeInput("session_001", createEventJson("session_001", "C", 4));

        // Only session_001 should have detected pattern
        assertThat(outputTopic.getQueueSize()).isEqualTo(1);

        String result = outputTopic.readValue();
        JsonNode detection = objectMapper.readTree(result);
        assertThat(detection.get("session_id").asText()).isEqualTo("session_001");
    }

    private String createEventJson(String sessionId, String eventType, int sequenceNum) throws Exception {
        Map<String, Object> event = new HashMap<>();
        event.put("event_id", "evt_" + String.format("%04d", sequenceNum));
        event.put("session_id", sessionId);
        event.put("event_type", eventType);
        event.put("timestamp", System.currentTimeMillis());
        event.put("sequence_number", sequenceNum);
        event.put("data", "event_data_" + eventType);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("simulation", true);
        metadata.put("pattern_type", "abc_sequence");
        event.put("metadata", metadata);

        return objectMapper.writeValueAsString(event);
    }
}
