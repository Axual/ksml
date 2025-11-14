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
import io.axual.ksml.testutil.KSMLTopologyTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@ExtendWith({KSMLTestExtension.class})
class KSMLFilterTest {

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    protected TestInputTopic<String, GenericRecord> inputTopic;

    @KSMLTopic(topic = "ksml_sensordata_filtered", valueSerde = KSMLTopic.SerdeType.AVRO)
    protected TestOutputTopic<String, GenericRecord> outputTopic;

    @KSMLTest(topology = "pipelines/test-filter.yaml", schemaDirectory = "schemas")
    @DisplayName("Records can be filtered by KSML")
    void testFilterAvroRecords() {
        log.debug("testFilterAvroRecords()");

        // the KSML pipeline filters on color "blue": generate some records with varying colors
        List<SensorData> sensorDatas = new ArrayList<>();
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());
        sensorDatas.add(SensorData.builder().color("green").build());
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());

        for (SensorData sensorData : sensorDatas) {
            inputTopic.pipeInput("key", sensorData.toRecord());
        }

        // only the two records with "blue" should be kept
        assertFalse(outputTopic.isEmpty());
        List<GenericRecord> outputValues = outputTopic.readValuesToList();
        assertEquals(2, outputValues.size());
        assertTrue(outputValues.stream()
                .map(rec -> rec.get("color").toString())
                .allMatch(color -> color.equals("blue")));
    }

    @KSMLTest(topology = "pipelines/test-filter-module-import.yaml", schemaDirectory = "schemas")
    @Disabled("Module import is WIP")
    @DisplayName("Records can be filtered by KSML using imported functions")
    void testFilterAvroRecordsImportedFunctions() {
        log.debug("testFilterAvroRecordsImportedFunctions()");

        // the KSML pipeline filters on color "blue": generate some records with varying colors
        List<SensorData> sensorDatas = new ArrayList<>();
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());
        sensorDatas.add(SensorData.builder().color("green").build());
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());

        for (SensorData sensorData : sensorDatas) {
            inputTopic.pipeInput("key", sensorData.toRecord());
        }

        // only the two records with "blue" should be kept
        assertFalse(outputTopic.isEmpty());
        List<GenericRecord> outputValues = outputTopic.readValuesToList();
        assertEquals(2, outputValues.size());
        assertTrue(outputValues.stream()
                .map(rec -> rec.get("color").toString())
                .allMatch(color -> color.equals("blue")));
    }

    @KSMLTopologyTest(topologies = {"pipelines/test-filter.yaml", "pipelines/test-filter-external-python.yaml"}, schemaDirectory = "schemas")
    @DisplayName("Records can be filtered by KSML using inline or externalized Python")
    void testFilterAvroRecordsExternalPython() {
        log.debug("testFilterAvroRecordsExternalPython()");

        // the KSML pipeline filters on color "blue": generate some records with varying colors
        List<SensorData> sensorDatas = new ArrayList<>();
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());
        sensorDatas.add(SensorData.builder().color("green").build());
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());

        for (SensorData sensorData : sensorDatas) {
            inputTopic.pipeInput("key", sensorData.toRecord());
        }

        // only the two records with "blue" should be kept
        assertFalse(outputTopic.isEmpty());
        List<GenericRecord> outputValues = outputTopic.readValuesToList();
        assertEquals(2, outputValues.size());
        assertTrue(outputValues.stream()
                .map(rec -> rec.get("color").toString())
                .allMatch(color -> color.equals("blue")));
    }
}
