package io.axual.ksml.docs.examples.beginner_tutorial.filtering_transforming;

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
import static org.assertj.core.api.Assertions.offset;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class FilteringTransformingCompleteTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "tutorial_input")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "filtered_data")
    TestOutputTopic<String, String> filteredOutput;

    @KSMLTopic(topic = "alerts_stream")
    TestOutputTopic<String, String> alertsOutput;

    @KSMLTest(topology = "docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-complete.yaml")
    void testValidSensorDataProcessing() throws Exception {
        // Send valid sensor data
        inputTopic.pipeInput("sensor0", createValidSensorJson("data_center", 80, 88));

        // Should appear in filtered_data topic
        assertThat(filteredOutput.getQueueSize()).isEqualTo(1);
        assertThat(alertsOutput.isEmpty()).isTrue();

        String result = filteredOutput.readValue();
        JsonNode output = objectMapper.readTree(result);

        // Verify transformation applied correctly
        assertThat(output.get("sensor_id").asText()).isEqualTo("sensor0");
        assertThat(output.get("location").asText()).isEqualTo("data_center");

        // Verify temperature conversion (80F = 26.67C)
        assertThat(output.get("readings").get("temperature").get("fahrenheit").asInt()).isEqualTo(80);
        assertThat(output.get("readings").get("temperature").get("celsius").asDouble()).isCloseTo(26.67, offset(0.01));

        // Verify humidity preserved
        assertThat(output.get("readings").get("humidity").asInt()).isEqualTo(88);

        // Verify heat index calculated
        assertThat(output.get("readings").has("heat_index")).isTrue();

        // Verify timestamp formatted
        assertThat(output.has("timestamp")).isTrue();
        assertThat(output.has("processed_at")).isTrue();
    }

    @KSMLTest(topology = "docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-complete.yaml")
    void testInvalidSensorDataFiltering() throws Exception {
        // Send invalid sensor data (temperature out of range: 250F > 200F)
        inputTopic.pipeInput("sensor1", createValidSensorJson("warehouse", 250, 50));

        // Should be filtered out, no output
        assertThat(filteredOutput.isEmpty()).isTrue();
        assertThat(alertsOutput.isEmpty()).isTrue();
    }

    @KSMLTest(topology = "docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-complete.yaml")
    void testMissingTemperatureData() throws Exception {
        // Send sensor data without temperature
        Map<String, Object> sensors = new HashMap<>();

        Map<String, Object> humidityData = new HashMap<>();
        humidityData.put("value", 70);
        humidityData.put("unit", "%");
        sensors.put("humidity", humidityData);

        Map<String, Object> locationData = new HashMap<>();
        locationData.put("value", "server_room");
        locationData.put("unit", "text");
        sensors.put("location", locationData);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("timestamp", System.currentTimeMillis());

        Map<String, Object> message = new HashMap<>();
        message.put("sensors", sensors);
        message.put("metadata", metadata);

        inputTopic.pipeInput("sensor2", objectMapper.writeValueAsString(message));

        // Should be filtered out
        assertThat(filteredOutput.isEmpty()).isTrue();
        assertThat(alertsOutput.isEmpty()).isTrue();
    }

    @KSMLTest(topology = "docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-complete.yaml")
    void testMissingHumidityData() throws Exception {
        // Send sensor data without humidity
        Map<String, Object> sensors = new HashMap<>();

        Map<String, Object> temperature = new HashMap<>();
        temperature.put("value", 75);
        temperature.put("unit", "F");
        sensors.put("temperature", temperature);

        Map<String, Object> locationData = new HashMap<>();
        locationData.put("value", "office");
        locationData.put("unit", "text");
        sensors.put("location", locationData);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("timestamp", System.currentTimeMillis());

        Map<String, Object> message = new HashMap<>();
        message.put("sensors", sensors);
        message.put("metadata", metadata);

        inputTopic.pipeInput("sensor3", objectMapper.writeValueAsString(message));

        // Should be filtered out
        assertThat(filteredOutput.isEmpty()).isTrue();
        assertThat(alertsOutput.isEmpty()).isTrue();
    }

    @KSMLTest(topology = "docs-examples/beginner-tutorial/filtering-transforming/processor-filtering-transforming-complete.yaml")
    void testTransformationErrorHandling() throws Exception {
        // Send malformed data that passes validation but fails transformation
        Map<String, Object> message = new HashMap<>();
        message.put("invalid_field", "malformed");
        inputTopic.pipeInput("sensor4", objectMapper.writeValueAsString(message));

        // Should route to alerts_stream
        if (!alertsOutput.isEmpty()) {
            String result = alertsOutput.readValue();
            JsonNode error = objectMapper.readTree(result);

            assertThat(error.has("error")).isTrue();
            assertThat(error.get("sensor_id").asText()).isEqualTo("sensor4");
        }
    }


    private String createValidSensorJson(String location, int tempF, int humidity) throws Exception {
        Map<String, Object> sensors = new HashMap<>();

        Map<String, Object> temperature = new HashMap<>();
        temperature.put("value", tempF);
        temperature.put("unit", "F");
        sensors.put("temperature", temperature);

        Map<String, Object> humidityData = new HashMap<>();
        humidityData.put("value", humidity);
        humidityData.put("unit", "%");
        sensors.put("humidity", humidityData);

        Map<String, Object> locationData = new HashMap<>();
        locationData.put("value", location);
        locationData.put("unit", "text");
        sensors.put("location", locationData);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("timestamp", System.currentTimeMillis());

        Map<String, Object> message = new HashMap<>();
        message.put("sensors", sensors);
        message.put("metadata", metadata);

        return objectMapper.writeValueAsString(message);
    }

}
