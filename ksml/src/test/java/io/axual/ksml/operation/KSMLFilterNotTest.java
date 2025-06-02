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
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith({KSMLTestExtension.class})
class KSMLFilterNotTest {

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    protected TestInputTopic inputTopic;

    @KSMLTopic(topic = "ksml_sensordata_filtered", valueSerde = KSMLTopic.SerdeType.AVRO)
    protected TestOutputTopic outputTopic;

    @KSMLTest(topology = "pipelines/test-filternot.yaml", schemaDirectory = "schemas")
    @DisplayName("Records can be filtered with filterNot by KSML")
    void testFilterNotAvroRecords() {
        log.debug("testFilterNotAvroRecords()");

        // the KSML pipeline filters on color other than "blue": generate some records with varying colors
        List<SensorData> sensorDatas = new ArrayList<>();
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());
        sensorDatas.add(SensorData.builder().color("green").build());
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());

        // given that we pipe these records into KSML
        for (SensorData sensorData : sensorDatas) {
            inputTopic.pipeInput("key", sensorData.toRecord());
        }

        // only the records with color other than "blue" should be kept
        assertFalse(outputTopic.isEmpty());
        List<GenericRecord> outputValues = outputTopic.readValuesToList();
        assertEquals(3, outputValues.size());
        assertTrue(outputValues.stream()
                .map(rec -> rec.get("color").toString())
                .noneMatch(color -> color.equals("blue")));
    }
}
