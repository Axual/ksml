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
 * Test for predefined state store configuration with groupBy operation in KSML.
 * <p>
 * This example demonstrates:
 * 1. Predefined store configuration in global 'stores' section
 * 2. groupBy operation with custom mapper (extracts 'owner' field from value)
 * 3. count operation using the predefined store
 * 4. Difference between groupBy (arbitrary key extraction) and groupByKey (uses existing key)
 * <p>
 * What these Tests Validate (KSML Translation):
 * <p>
 * 1. testGroupByCustomMapper: groupBy correctly extracts owner field from value
 * 2. testCountWithPredefinedStore: count operation works with predefined store
 * 3. testUnknownOwnerHandling: mapper returns "unknown" for null or missing owner
 * 4. testMultipleOwnersIndependent: groupBy maintains separate counts per extracted key
 */
@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class PredefinedStoreTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "sensor_ownership_data")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "owner_sensor_counts")
    TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml")
    void testGroupByCustomMapper() throws Exception {
        String sensorId = "sensor_001";

        // Send sensor data with owner "alice"
        inputTopic.pipeInput(sensorId, createSensorOwnershipJson(sensorId, "alice", "temperature"));
        inputTopic.pipeInput(sensorId, createSensorOwnershipJson(sensorId, "alice", "humidity"));

        // Count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // Verify we get outputs grouped by owner "alice"
        while (!outputTopic.isEmpty()) {
            var record = outputTopic.readRecord();
            log.info("Got key: '{}', value: '{}'", record.getKey(), record.getValue());
            assertThat(record.getKey()).isEqualTo("alice");
            assertThat(record.getValue()).isNotNull();
        }
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml")
    void testCountWithPredefinedStore() throws Exception {
        // Send 3 sensors owned by "bob"
        inputTopic.pipeInput("sensor_002", createSensorOwnershipJson("sensor_002", "bob", "temperature"));
        inputTopic.pipeInput("sensor_003", createSensorOwnershipJson("sensor_003", "bob", "pressure"));
        inputTopic.pipeInput("sensor_004", createSensorOwnershipJson("sensor_004", "bob", "humidity"));

        // Count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // Verify we get outputs
        while (!outputTopic.isEmpty()) {
            var record = outputTopic.readRecord();
            log.info("Got key: '{}', value: '{}'", record.getKey(), record.getValue());
            assertThat(record.getKey()).isEqualTo("bob");
            assertThat(record.getValue()).isNotNull();
        }
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml")
    void testUnknownOwnerHandling() throws Exception {
        // Send sensor with null owner
        inputTopic.pipeInput("sensor_005", createSensorOwnershipJson("sensor_005", null, "temperature"));

        // Send sensor with missing owner field
        Map<String, Object> sensorNoOwner = new HashMap<>();
        sensorNoOwner.put("sensor_id", "sensor_006");
        sensorNoOwner.put("type", "pressure");
        inputTopic.pipeInput("sensor_006", objectMapper.writeValueAsString(sensorNoOwner));

        // Count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(1);

        // All should be grouped under "unknown"
        while (!outputTopic.isEmpty()) {
            var record = outputTopic.readRecord();
            log.info("Got key: '{}', value: '{}'", record.getKey(), record.getValue());
            assertThat(record.getKey()).isEqualTo("unknown");
            assertThat(record.getValue()).isNotNull();
        }
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/state-stores/processor-predefined-store.yaml")
    void testMultipleOwnersIndependent() throws Exception {
        // Alice: 2 sensors
        inputTopic.pipeInput("sensor_008", createSensorOwnershipJson("sensor_008", "alice", "temperature"));
        inputTopic.pipeInput("sensor_009", createSensorOwnershipJson("sensor_009", "alice", "humidity"));

        // Bob: 3 sensors
        inputTopic.pipeInput("sensor_010", createSensorOwnershipJson("sensor_010", "bob", "pressure"));
        inputTopic.pipeInput("sensor_011", createSensorOwnershipJson("sensor_011", "bob", "temperature"));
        inputTopic.pipeInput("sensor_012", createSensorOwnershipJson("sensor_012", "bob", "humidity"));

        // Charlie: 1 sensor
        inputTopic.pipeInput("sensor_013", createSensorOwnershipJson("sensor_013", "charlie", "temperature"));

        // Count operations produce output
        log.info("Queue size: {}", outputTopic.getQueueSize());
        assertThat(outputTopic.getQueueSize()).isGreaterThanOrEqualTo(3);

        // Verify each owner has independent grouping
        java.util.Set<String> seenOwners = new java.util.HashSet<>();
        while (!outputTopic.isEmpty()) {
            var record = outputTopic.readRecord();
            log.info("Got key: '{}', value: '{}'", record.getKey(), record.getValue());
            seenOwners.add(record.getKey());
            assertThat(record.getValue()).isNotNull();
        }

        // Verify all three owners produced output
        assertThat(seenOwners).contains("alice", "bob", "charlie");
    }

    private String createSensorOwnershipJson(String sensorId, String owner, String type) throws Exception {
        Map<String, Object> sensor = new HashMap<>();
        sensor.put("sensor_id", sensorId);
        sensor.put("owner", owner);
        sensor.put("type", type);
        sensor.put("timestamp", System.currentTimeMillis());

        return objectMapper.writeValueAsString(sensor);
    }
}
