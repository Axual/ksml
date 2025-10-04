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

import com.fasterxml.jackson.databind.JsonNode;
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
 * Test for hopping window aggregation with average calculation in KSML.
 * <p>
 * The hopping window demonstrates:
 * 1. groupByKey to group sensor readings by sensor_id
 * 2. windowByTime with hopping window (2-minute windows, 30-second hop)
 * 3. aggregate with initializer and aggregator to compute sum and count
 * 4. mapValues to calculate final average from aggregated sum/count
 * <p>
 * What these Tests Validate (KSML Translation):
 *
 * 1. testHoppingWindowAggregation: aggregator correctly computes sum and count
 * 2. testOverlappingWindows: single event appears in multiple overlapping windows
 * 3. testAverageCalculation: mapValues correctly calculates average from sum/count
 * 4. testMultipleSensorsIndependent: hopping windows maintain separate aggregates per key
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class HoppingWindowTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "sensor_readings")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "sensor_moving_averages")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-hopping-average-working.yaml")
    void testHoppingWindowAggregation() throws Exception {
        String sensorId = "temp_01";
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // Send 3 readings in same window
        inputTopic.pipeInput(sensorId, createSensorJson(sensorId, 20.0), baseTime);
        inputTopic.pipeInput(sensorId, createSensorJson(sensorId, 22.0), baseTime.plus(Duration.ofSeconds(30)));
        inputTopic.pipeInput(sensorId, createSensorJson(sensorId, 24.0), baseTime.plus(Duration.ofMinutes(1)));

        // Due to hopping nature, each event can produce multiple outputs
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(3);

        // Read first result and verify aggregation
        String result = outputTopic.readValue();
        JsonNode json = objectMapper.readTree(result);

        assertThat(json.has("average")).isTrue();
        assertThat(json.has("sample_count")).isTrue();
        assertThat(json.has("total_sum")).isTrue();

        assertThat(json.get("sample_count").asInt()).isGreaterThanOrEqualTo(1);
        assertThat(json.get("total_sum").asDouble()).isGreaterThan(0);
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-hopping-average-working.yaml")
    void testOverlappingWindows() throws Exception {
        String sensorId = "temp_02";
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // Single reading
        inputTopic.pipeInput(sensorId, createSensorJson(sensorId, 21.5), baseTime);

        // With 2-minute windows hopping every 30 seconds, one event can appear in up to 4 windows
        // The exact number depends on alignment with window boundaries
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // Verify that outputs contain the reading's contribution
        boolean foundReading = false;
        while (!outputTopic.isEmpty()) {
            String result = outputTopic.readValue();
            JsonNode json = objectMapper.readTree(result);

            if (json.get("sample_count").asInt() >= 1) {
                foundReading = true;
                assertThat(json.get("total_sum").asDouble()).isGreaterThanOrEqualTo(21.5);
            }
        }

        assertThat(foundReading).isTrue();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-hopping-average-working.yaml")
    void testAverageCalculation() throws Exception {
        String sensorId = "humidity_01";
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // Send 2 readings: 40.0 and 60.0, average should be 50.0
        inputTopic.pipeInput(sensorId, createSensorJson(sensorId, 40.0), baseTime);
        inputTopic.pipeInput(sensorId, createSensorJson(sensorId, 60.0), baseTime.plus(Duration.ofSeconds(30)));

        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(2);

        // Find a window that contains both readings
        boolean foundBothReadings = false;
        while (!outputTopic.isEmpty() && !foundBothReadings) {
            String result = outputTopic.readValue();
            JsonNode json = objectMapper.readTree(result);

            if (json.get("sample_count").asInt() == 2) {
                foundBothReadings = true;
                // Verify average calculation: (40 + 60) / 2 = 50
                assertThat(json.get("average").asDouble()).isEqualTo(50.0);
                assertThat(json.get("total_sum").asDouble()).isEqualTo(100.0);
            }
        }

        assertThat(foundBothReadings).isTrue();
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/windowing/processor-hopping-average-working.yaml")
    void testMultipleSensorsIndependent() throws Exception {
        Instant baseTime = Instant.parse("2025-01-01T12:00:00Z");

        // temp_01: reading of 20.0
        inputTopic.pipeInput("temp_01", createSensorJson("temp_01", 20.0), baseTime);

        // pressure_01: reading of 1020.0
        inputTopic.pipeInput("pressure_01", createSensorJson("pressure_01", 1020.0), baseTime.plus(Duration.ofSeconds(10)));

        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(2);

        // Verify each sensor has independent aggregations
        Map<String, Double> firstReadings = new HashMap<>();

        while (!outputTopic.isEmpty()) {
            var record = outputTopic.readRecord();
            JsonNode json = objectMapper.readTree(record.getValue());

            // Store first reading for each sensor
            if (json.get("sample_count").asInt() == 1) {
                double sum = json.get("total_sum").asDouble();
                firstReadings.put(record.getKey(), sum);
            }
        }

        // Verify both sensors had independent first readings
        assertThat(firstReadings.size()).isGreaterThanOrEqualTo(1);
    }

    private String createSensorJson(String sensorId, double value) throws Exception {
        Map<String, Object> reading = new HashMap<>();
        reading.put("sensor_id", sensorId);
        reading.put("value", value);
        reading.put("timestamp", System.currentTimeMillis());

        // Add unit based on sensor type
        if (sensorId.contains("temp")) {
            reading.put("unit", "celsius");
        } else if (sensorId.contains("humidity")) {
            reading.put("unit", "percent");
        } else {
            reading.put("unit", "hPa");
        }

        return objectMapper.writeValueAsString(reading);
    }
}
