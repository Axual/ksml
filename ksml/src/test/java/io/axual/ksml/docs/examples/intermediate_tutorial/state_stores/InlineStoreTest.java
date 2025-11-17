package io.axual.ksml.docs.examples.intermediate_tutorial.state_stores;

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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/**
 * Test for inline state store configuration in KSML.
 * <p>
 * This example demonstrates aggregating sensor data by type using an inline state store.
 * The pipeline:
 * 1. Groups sensor data by sensor_type (temperature, humidity, pressure, light)
 * 2. Aggregates using inline keyValue store configuration
 * 3. Calculates running average, total value, and count
 * 4. Outputs aggregated results per sensor type
 * <p>
 * Inline store configuration:
 * - name: sensor_type_aggregates
 * - type: keyValue
 * - retention: 3m
 * - caching: true
 * - persistent: false (in-memory only)
 * - logging: false
 * <p>
 * What these tests validate:
 * 1. testSingleSensorTypeAggregation: Basic aggregation for one sensor type
 * 2. testMultipleSensorTypesAggregation: Independent aggregation per sensor type
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class InlineStoreTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "sensor_ownership_data")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "sensor_type_totals")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-inline-store.yaml")
    void testSingleSensorTypeAggregation() throws Exception {
        // Send 3 temperature sensor readings
        inputTopic.pipeInput("temp_001", createSensorJson("temp_001", "alice", "temperature", 25.5));
        inputTopic.pipeInput("temp_002", createSensorJson("temp_002", "bob", "temperature", 26.5));
        inputTopic.pipeInput("temp_003", createSensorJson("temp_003", "charlie", "temperature", 24.0));

        // Read all aggregation updates (one per input)
        List<KeyValue<String, String>> results = outputTopic.readKeyValuesToList();
        assertThat(results).hasSize(3);

        // Verify final aggregation (last result)
        JsonNode finalResult = objectMapper.readTree(results.get(2).value);

        assertThat(finalResult.get("total_value").asDouble()).isCloseTo(76.0, offset(0.01)); // 25.5 + 26.5 + 24.0
        assertThat(finalResult.get("count").asInt()).isEqualTo(3);
        assertThat(finalResult.get("average").asDouble()).isCloseTo(25.33, offset(0.01)); // 76.0 / 3
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-inline-store.yaml")
    void testMultipleSensorTypesAggregation() throws Exception {
        // Send readings for different sensor types
        inputTopic.pipeInput("temp_001", createSensorJson("temp_001", "alice", "temperature", 25.0));
        inputTopic.pipeInput("hum_001", createSensorJson("hum_001", "bob", "humidity", 60.0));
        inputTopic.pipeInput("temp_002", createSensorJson("temp_002", "charlie", "temperature", 27.0));
        inputTopic.pipeInput("pres_001", createSensorJson("pres_001", "diana", "pressure", 1013.25));
        inputTopic.pipeInput("hum_002", createSensorJson("hum_002", "eve", "humidity", 65.0));

        // Read all aggregation updates
        List<KeyValue<String, String>> results = outputTopic.readKeyValuesToList();
        assertThat(results).hasSize(5);

        // Find final results for each sensor type
        JsonNode tempResult = null;
        JsonNode humResult = null;
        JsonNode presResult = null;

        for (KeyValue<String, String> kv : results) {
            if (kv.key.equals("temperature")) tempResult = objectMapper.readTree(kv.value);
            if (kv.key.equals("humidity")) humResult = objectMapper.readTree(kv.value);
            if (kv.key.equals("pressure")) presResult = objectMapper.readTree(kv.value);
        }

        // Verify temperature aggregation (2 readings: 25.0, 27.0)
        assertThat(tempResult).isNotNull();
        assertThat(tempResult.get("count").asInt()).isEqualTo(2);
        assertThat(tempResult.get("average").asDouble()).isCloseTo(26.0, offset(0.01));

        // Verify humidity aggregation (2 readings: 60.0, 65.0)
        assertThat(humResult).isNotNull();
        assertThat(humResult.get("count").asInt()).isEqualTo(2);
        assertThat(humResult.get("average").asDouble()).isCloseTo(62.5, offset(0.01));

        // Verify pressure aggregation (1 reading: 1013.25)
        assertThat(presResult).isNotNull();
        assertThat(presResult.get("count").asInt()).isEqualTo(1);
        assertThat(presResult.get("average").asDouble()).isCloseTo(1013.25, offset(0.01));
    }



    private String createSensorJson(String sensorId, String owner, String sensorType, double value) throws Exception {
        Map<String, Object> sensor = new HashMap<>();
        sensor.put("sensor_id", sensorId);
        sensor.put("owner", owner);
        sensor.put("sensor_type", sensorType);
        sensor.put("value", value);
        sensor.put("timestamp", System.currentTimeMillis());
        return objectMapper.writeValueAsString(sensor);
    }
}
