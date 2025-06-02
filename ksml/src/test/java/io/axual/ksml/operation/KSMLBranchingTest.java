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
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class KSMLBranchingTest {

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic inputTopic;

    @KSMLTopic(topic = "ksml_sensordata_blue", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestOutputTopic outputBlue;

    @KSMLTopic(topic = "ksml_sensordata_red", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestOutputTopic outputRed;

    @KSMLTest(topology = "pipelines/test-branching.yaml", schemaDirectory = "schemas")
    void testBranching() {
        // the pipeline routes readings based on color: generate some records
        List<SensorData> sensorDatas = new ArrayList<>();
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("green").build());
        sensorDatas.add(SensorData.builder().color("blue").build());
        sensorDatas.add(SensorData.builder().color("red").build());
        sensorDatas.add(SensorData.builder().color("green").build());

        for (SensorData sensorData : sensorDatas) {
            inputTopic.pipeInput("key", sensorData.toRecord());
        }

        // three "blue" records routed to outputBlue, two "red" to outputRed
        List<GenericRecord> blueRecords = outputBlue.readValuesToList();
        List<GenericRecord> redRecords = outputRed.readValuesToList();

        assertEquals(3, blueRecords.size(), "3 blue records should be routed to outputBlue");
        assertEquals(2, redRecords.size(), "red 2 records should be routed to ouputRed");
    }
}
