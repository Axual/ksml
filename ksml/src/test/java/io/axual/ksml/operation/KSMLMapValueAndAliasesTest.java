package io.axual.ksml.operation;

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

import io.axual.ksml.testutil.KSMLTopic;
import io.axual.ksml.testutil.KSMLTopologyTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static io.axual.ksml.operation.SensorData.SensorType.HUMIDITY;
import static io.axual.ksml.operation.SensorData.SensorType.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
/**
 * Test for mapValues and its aliases mapValue and transformValue
 */
public class KSMLMapValueAndAliasesTest {

    @KSMLTopic(topic = "input_topic", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> inputTopic;

    @KSMLTopic(topic = "output_topic")
    TestOutputTopic<String, String> outputTopic;

    List<GenericRecord> inputs = List.of(
            SensorData.builder().city("AMS").type(HUMIDITY).unit("%").value("80").build().toRecord(),
            SensorData.builder().city("UTR").type(HUMIDITY).unit("%").value("75").build().toRecord(),
            SensorData.builder().city("AMS").type(TEMPERATURE).unit("C").value("25").build().toRecord(),
            SensorData.builder().city("UTR").type(TEMPERATURE).unit("C").value("27").build().toRecord()
    );

    @KSMLTopologyTest(
            topologies = {
                    "pipelines/test-mapvalue-expression.yaml",
                    "pipelines/test-mapvalues-expression.yaml",
                    "pipelines/test-transformvalue-expression.yaml",
                    "pipelines/test-mapvalue-code.yaml",
                    "pipelines/test-mapvalues-code.yaml",
                    "pipelines/test-transformvalue-code.yaml"},
            schemaDirectory = "schemas")
    @DisplayName("Values can be mapped with mapValue and aliases")
    void testMapValueByExpression() {
        inputs.forEach(rec -> inputTopic.pipeInput("key", rec));

        var keyValues = outputTopic.readKeyValuesToList();
        assertEquals(4, keyValues.size(), "All records should be transformed");

        // verify first and last value; the pipeline creates them from fields in the input value
        assertEquals("HUMIDITY 80", keyValues.get(0).value);
        assertEquals("HUMIDITY 75", keyValues.get(1).value);
        assertEquals("TEMPERATURE 25", keyValues.get(2).value);
        assertEquals("TEMPERATURE 27", keyValues.get(3).value);
    }
}


