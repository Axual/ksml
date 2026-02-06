package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test for field modification example that modifies schema fields at runtime.
 * This test verifies that Python code can delete elements from schema field lists
 * using 'del schema["fields"][index]'.
 */
@Slf4j
@ExtendWith({KSMLTestExtension.class})
class KSMLFieldModificationTest {

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    protected TestInputTopic<String, GenericRecord> inputTopic;

    @KSMLTopic(topic = "ksml_sensordata_modified", valueSerde = KSMLTopic.SerdeType.AVRO)
    protected TestOutputTopic<String, GenericRecord> outputTopic;

    @KSMLTest(topology = "pipelines/test-field-modification.yaml", schemaDirectory = "schemas")
    @DisplayName("Schema fields can be modified and removed at runtime")
    void testFieldModification() {
        log.debug("testFieldModification()");

        // Create a sensor data record with all fields including "color"
        SensorData sensorData = SensorData.builder()
                .name("sensor1")
                .owner("Alice")
                .color("blue")
                .city("Amsterdam")
                .build();

        inputTopic.pipeInput("key1", sensorData.toRecord());

        // The pipeline should:
        // 1. Change owner to "Zack"
        // 2. Remove the "color" field from the schema
        assertFalse(outputTopic.isEmpty(), "Output topic should have a record");

        GenericRecord outputRecord = outputTopic.readValue();

        // Verify owner was changed
        assertEquals("Zack", outputRecord.get("owner").toString(),
                "Owner should be changed to 'Zack'");

        // Verify color field is removed from the output schema
        assertNull(outputRecord.getSchema().getField("color"),
                "Color field should be removed from output schema");

        // Verify other fields are preserved
        assertEquals("sensor1", outputRecord.get("name").toString());
        assertEquals("Amsterdam", outputRecord.get("city").toString());
    }
}
