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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class KSMLRoutingTest {

    @KSMLTopic(topic = "ksml_sensordata_avro")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "ksml_sensordata_sensor0")
    TestOutputTopic<String, String> outputSensor0;

    @KSMLTopic(topic = "ksml_sensordata_sensor1")
    TestOutputTopic<String, String> outputSensor1;

    @KSMLTopic(topic = "ksml_sensordata_sensor2")
    TestOutputTopic<String, String> outputSensor2;

    @KSMLTest(topology = "pipelines/test-routing.yaml")
    void testRouting() {
        // the pipeline routes readings based on key: generate some records
        inputTopic.pipeInput("sensor1", "some_value");
        inputTopic.pipeInput("sensor2", "some_value");
        inputTopic.pipeInput("sensor3", "some_value");
        inputTopic.pipeInput("sensor0", "some_value");
        inputTopic.pipeInput("sensor1", "some_value");
        inputTopic.pipeInput("sensor1", "some_value");
        inputTopic.pipeInput("sensor2", "some_value");
        inputTopic.pipeInput("sensor99", "some_value");
        inputTopic.pipeInput("random_key", "some_value");
        inputTopic.pipeInput("sensor1", "some_value");

        assertFalse(outputSensor0.isEmpty());
        assertFalse(outputSensor1.isEmpty());
        assertFalse(outputSensor2.isEmpty());

        List<KeyValue<String, String>> keyValues0 = outputSensor0.readKeyValuesToList();
        List<KeyValue<String, String>> keyValues1 = outputSensor1.readKeyValuesToList();
        List<KeyValue<String, String>> keyValues2 = outputSensor2.readKeyValuesToList();

        assertEquals(2, keyValues2.size(), "2 sensor2 readings were routed to output2");
        assertEquals(4, keyValues1.size(), "4 sensor1 readings were routed to output1");
        assertEquals(4, keyValues0.size(), "4 other readings were routed to output0");
    }
}
