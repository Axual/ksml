package io.axual.ksml.docs.examples.intermediate.tutorial.branching;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class LocationRoutingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "sensor_input")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "datacenter_sensors")
    TestOutputTopic<String, String> datacenterOutput;

    @KSMLTopic(topic = "warehouse_sensors")
    TestOutputTopic<String, String> warehouseOutput;

    @KSMLTopic(topic = "office_sensors")
    TestOutputTopic<String, String> officeOutput;

    @KSMLTopic(topic = "unknown_sensors")
    TestOutputTopic<String, String> unknownOutput;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/branching/processor-location-routing.yaml")
    void testLocationBasedRouting() throws Exception {
        inputTopic.pipeInput("k1", createSensorJson("data_center"));
        inputTopic.pipeInput("k2", createSensorJson("warehouse"));
        inputTopic.pipeInput("k3", createSensorJson("office"));
        inputTopic.pipeInput("k4", createSensorJson("unknown"));

        assertEquals(1, datacenterOutput.readValuesToList().size());
        assertEquals(1, warehouseOutput.readValuesToList().size());
        assertEquals(1, officeOutput.readValuesToList().size());
        assertEquals(1, unknownOutput.readValuesToList().size());
    }

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/branching/processor-location-routing.yaml")
    void testMissingLocation() throws Exception {
        inputTopic.pipeInput("key", createSensorJson(null));
        
        assertEquals(1, unknownOutput.readValuesToList().size());
        assertTrue(datacenterOutput.isEmpty());
    }

    private String createSensorJson(String location) throws Exception {
        Map<String, Object> sensor = new HashMap<>();
        sensor.put("name", "test_sensor");
        if (location != null) {
            sensor.put("location", location);
        }
        sensor.put("value", 25.0);
        return objectMapper.writeValueAsString(sensor);
    }
}