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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KSMLTestExtension.class)
public class KSMLStreamPartitionerTest {

    @KSMLTopic(topic = "user_events")
    TestInputTopic<String, String> userEventsInput;

    @KSMLTopic(topic = "partitioned_user_events")
    TestOutputTopic<String, String> partitionedOutput;

    @KSMLTest(topology = "pipelines/test-stream-partitioner.yaml")
    void testStreamPartitionerWithExplicitResultType() {
        // This test validates that issue #287 is fixed:
        // StreamPartitioner with resultType: integer should work correctly
        // and not be inferred as UnionOfIntegerOrListOfInteger
        
        // Send test events with different user types
        userEventsInput.pipeInput("A", "{\"user_id\":\"A\",\"user_type\":\"admin\"}");
        userEventsInput.pipeInput("B", "{\"user_id\":\"B\",\"user_type\":\"premium\"}");
        userEventsInput.pipeInput("C", "{\"user_id\":\"C\",\"user_type\":\"standard\"}");
        userEventsInput.pipeInput("D", "{\"user_id\":\"D\",\"user_type\":\"trial\"}");

        // Verify output - all events should be partitioned correctly
        assertFalse(partitionedOutput.isEmpty());
        
        // Read all events at once to verify they were processed correctly
        var allEvents = partitionedOutput.readKeyValuesToList();
        assertEquals(4, allEvents.size());

        // Verify we can read all partitioned events
        // This validates that the partitioner function with resultType: integer works correctly
        var eventKeys = allEvents.stream().map(kv -> kv.key).toList();
        assertTrue(eventKeys.contains("A"));
        assertTrue(eventKeys.contains("B"));
        assertTrue(eventKeys.contains("C"));
        assertTrue(eventKeys.contains("D"));
        
        // The fact that this test completes successfully validates that:
        // 1. The streamPartitioner function with resultType: integer is properly recognized
        // 2. No "UnionOfIntegerOrListOfInteger" type inference error occurs
        // 3. The partitioner function correctly processes events without errors
    }
}