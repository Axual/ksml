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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for windowed aggregation with custom initializer and aggregator in KSML.
 * <p>
 * The windowed aggregation demonstrates:
 * 1. groupByKey to group temperature readings by sensor
 * 2. windowByTime to create 30-second tumbling windows
 * 3. aggregate with initializer and aggregator to compute statistics
 * 4. map to transform WindowedString key to regular string key with window metadata
 * <p>
 * What these Tests Validate (KSML Translation):
 *
 * 1. testAggregateComputesStatistics: aggregator computes count, sum, min, max, avg
 * 2. testInitializerCreatesEmptyStats: initializer creates correct initial state
 * 3. testMapTransformsWindowedKey: map function extracts window metadata from WindowedString
 * 4. testMultipleSensorsIndependent: windowed aggregation tracks separate windows per sensor
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class WindowedAggregationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "sensor_temperatures")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "sensor_window_stats")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-windowed-aggregate.yaml")
    void testAggregateComputesStatistics() throws Exception {
        String sensorId = "temp001";

        // Send temperature readings
        inputTopic.pipeInput(sensorId, "20.5");
        inputTopic.pipeInput(sensorId, "22.3");
        inputTopic.pipeInput(sensorId, "19.8");

        // Should receive 3 window updates
        assertThat(outputTopic.getQueueSize()).isEqualTo(3);

        // Read final result
        outputTopic.readValue(); // Skip first two
        outputTopic.readValue();
        String result = outputTopic.readValue();

        JsonNode json = objectMapper.readTree(result);
        JsonNode stats = json.get("stats");

        // Verify aggregator computed correct statistics
        assertThat(stats.get("count").asInt()).isEqualTo(3);
        assertThat(stats.get("sum").asDouble()).isCloseTo(62.6, org.assertj.core.data.Offset.offset(0.1));
        assertThat(stats.get("min").asDouble()).isEqualTo(19.8);
        assertThat(stats.get("max").asDouble()).isEqualTo(22.3);
        assertThat(stats.get("avg").asDouble()).isCloseTo(20.87, org.assertj.core.data.Offset.offset(0.01));
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-windowed-aggregate.yaml")
    void testInitializerCreatesEmptyStats() throws Exception {
        String sensorId = "temp002";

        // Send first reading
        inputTopic.pipeInput(sensorId, "25.0");

        String result = outputTopic.readValue();
        JsonNode json = objectMapper.readTree(result);
        JsonNode stats = json.get("stats");

        // Verify initializer created correct initial state and first value was added
        assertThat(stats.get("count").asInt()).isEqualTo(1);
        assertThat(stats.get("sum").asDouble()).isEqualTo(25.0);
        assertThat(stats.get("min").asDouble()).isEqualTo(25.0);
        assertThat(stats.get("max").asDouble()).isEqualTo(25.0);
        assertThat(stats.get("avg").asDouble()).isEqualTo(25.0);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-windowed-aggregate.yaml")
    void testMapTransformsWindowedKey() throws Exception {
        String sensorId = "temp003";

        inputTopic.pipeInput(sensorId, "30.0");

        String result = outputTopic.readValue();
        JsonNode json = objectMapper.readTree(result);

        // Verify map function extracted window metadata
        assertThat(json.has("sensor_id")).isTrue();
        assertThat(json.has("window_start")).isTrue();
        assertThat(json.has("window_end")).isTrue();
        assertThat(json.has("stats")).isTrue();

        assertThat(json.get("sensor_id").asText()).isEqualTo(sensorId);
        // Verify window metadata exists (values depend on test timing, so just check they're present)
        assertThat(json.get("window_start")).isNotNull();
        assertThat(json.get("window_end")).isNotNull();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/aggregations/processor-windowed-aggregate.yaml")
    void testMultipleSensorsIndependent() throws Exception {
        // Send readings for multiple sensors
        inputTopic.pipeInput("temp001", "20.0");
        inputTopic.pipeInput("temp002", "30.0");
        inputTopic.pipeInput("temp001", "22.0");
        inputTopic.pipeInput("temp002", "32.0");

        assertThat(outputTopic.getQueueSize()).isEqualTo(4);

        // Read and verify each sensor maintains separate statistics
        String result1 = outputTopic.readValue();
        JsonNode json1 = objectMapper.readTree(result1);
        assertThat(json1.get("sensor_id").asText()).isEqualTo("temp001");
        assertThat(json1.get("stats").get("count").asInt()).isEqualTo(1);
        assertThat(json1.get("stats").get("avg").asDouble()).isEqualTo(20.0);

        String result2 = outputTopic.readValue();
        JsonNode json2 = objectMapper.readTree(result2);
        assertThat(json2.get("sensor_id").asText()).isEqualTo("temp002");
        assertThat(json2.get("stats").get("count").asInt()).isEqualTo(1);
        assertThat(json2.get("stats").get("avg").asDouble()).isEqualTo(30.0);

        String result3 = outputTopic.readValue();
        JsonNode json3 = objectMapper.readTree(result3);
        assertThat(json3.get("sensor_id").asText()).isEqualTo("temp001");
        assertThat(json3.get("stats").get("count").asInt()).isEqualTo(2);
        assertThat(json3.get("stats").get("avg").asDouble()).isEqualTo(21.0);

        String result4 = outputTopic.readValue();
        JsonNode json4 = objectMapper.readTree(result4);
        assertThat(json4.get("sensor_id").asText()).isEqualTo("temp002");
        assertThat(json4.get("stats").get("count").asInt()).isEqualTo(2);
        assertThat(json4.get("stats").get("avg").asDouble()).isEqualTo(31.0);
    }
}
