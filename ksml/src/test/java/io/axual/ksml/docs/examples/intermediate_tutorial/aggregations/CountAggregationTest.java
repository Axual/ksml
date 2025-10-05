package io.axual.ksml.docs.examples.intermediate_tutorial.aggregations;

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

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for count aggregation in KSML.
 * <p>
 * The count aggregation demonstrates:
 * 1. groupByKey operation to group events by key
 * 2. count operation to count events per key
 * 3. toStream operation to convert KTable back to KStream
 * <p>
 * What these Tests Validate (KSML Translation):
 * <p>
 * 1. testCountIncrementsPerKey: count aggregation increments count for each event per key
 * 2. testMultipleKeys: count aggregation tracks separate counts for different keys
 * 3. testConvertToString: convertValue operation correctly converts count to string
 * 4. testGroupByKeyPreservesKey: groupByKey maintains the original key
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class CountAggregationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "user_actions")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "user_action_counts")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-count.yaml")
    void testCountIncrementsPerKey() throws Exception {
        String userId = "alice";

        // Send 3 actions for same user
        inputTopic.pipeInput(userId, createActionJson(userId, "login"));
        inputTopic.pipeInput(userId, createActionJson(userId, "view"));
        inputTopic.pipeInput(userId, createActionJson(userId, "click"));

        // Should receive 3 count updates (1, 2, 3)
        assertThat(outputTopic.getQueueSize()).isEqualTo(3);

        assertThat(outputTopic.readValue()).isEqualTo("1");
        assertThat(outputTopic.readValue()).isEqualTo("2");
        assertThat(outputTopic.readValue()).isEqualTo("3");
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-count.yaml")
    void testMultipleKeys() throws Exception {
        // Send actions for multiple users
        inputTopic.pipeInput("alice", createActionJson("alice", "login"));
        inputTopic.pipeInput("bob", createActionJson("bob", "login"));
        inputTopic.pipeInput("alice", createActionJson("alice", "view"));
        inputTopic.pipeInput("bob", createActionJson("bob", "view"));
        inputTopic.pipeInput("alice", createActionJson("alice", "click"));

        // Should receive 5 count updates
        assertThat(outputTopic.getQueueSize()).isEqualTo(5);

        // Read all and verify counts
        var record1 = outputTopic.readRecord();
        assertThat(record1.getKey()).isEqualTo("alice");
        assertThat(record1.getValue()).isEqualTo("1");

        var record2 = outputTopic.readRecord();
        assertThat(record2.getKey()).isEqualTo("bob");
        assertThat(record2.getValue()).isEqualTo("1");

        var record3 = outputTopic.readRecord();
        assertThat(record3.getKey()).isEqualTo("alice");
        assertThat(record3.getValue()).isEqualTo("2");

        var record4 = outputTopic.readRecord();
        assertThat(record4.getKey()).isEqualTo("bob");
        assertThat(record4.getValue()).isEqualTo("2");

        var record5 = outputTopic.readRecord();
        assertThat(record5.getKey()).isEqualTo("alice");
        assertThat(record5.getValue()).isEqualTo("3");
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-count.yaml")
    void testConvertToString() throws Exception {
        String userId = "charlie";

        inputTopic.pipeInput(userId, createActionJson(userId, "login"));

        // Verify the value is converted to string (not a long)
        String result = outputTopic.readValue();
        assertThat(result).isEqualTo("1");
        assertThat(result).isInstanceOf(String.class);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-count.yaml")
    void testGroupByKeyPreservesKey() throws Exception {
        String userId = "david";

        inputTopic.pipeInput(userId, createActionJson(userId, "purchase"));

        var record = outputTopic.readRecord();

        // Verify groupByKey preserved the key
        assertThat(record.getKey()).isEqualTo(userId);
        assertThat(record.getValue()).isEqualTo("1");
    }

    private String createActionJson(String userId, String action) throws Exception {
        Map<String, Object> actionEvent = new HashMap<>();
        actionEvent.put("user_id", userId);
        actionEvent.put("action", action);
        actionEvent.put("timestamp", System.currentTimeMillis());

        return objectMapper.writeValueAsString(actionEvent);
    }
}
