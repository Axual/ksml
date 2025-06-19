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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;

import static io.axual.ksml.operation.SensorData.SensorType.HUMIDITY;
import static io.axual.ksml.operation.SensorData.SensorType.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KSMLTestExtension.class)
@Slf4j
public class KSMLGroupByTest {

    @KSMLTopic(topic = "input_topic", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> inputTopic;

    @KSMLTopic(topic = "output_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> outputTopic;

    List<GenericRecord> inputs = List.of(
            SensorData.builder().city("AMS").type(HUMIDITY).unit("%").value("80").build().toRecord(),
            SensorData.builder().city("UTR").type(HUMIDITY).unit("%").value("75").build().toRecord(),
            SensorData.builder().city("AMS").type(TEMPERATURE).unit("C").value("25").build().toRecord(),
            SensorData.builder().city("UTR").type(TEMPERATURE).unit("C").value("27").build().toRecord()
    );

    @KSMLTest(topology = "pipelines/test-groupby-expression.yaml", schemaDirectory = "schemas")
    @DisplayName("Records can be grouped with an expression")
    void testGroupByExpressionStore() {
        inputs.forEach(rec -> inputTopic.pipeInput(rec));

        Map<String, Long> groupedRecords = outputTopic.readKeyValuesToMap();
        assertEquals(2, groupedRecords.size(), "records should be grouped by city");
        assertEquals(2L, groupedRecords.get("AMS"), "records for AMS should be counted");
        assertEquals(2L, groupedRecords.get("UTR"), "records for UTR should be counted");
    }

    @KSMLTest(topology = "pipelines/test-groupby-code.yaml", schemaDirectory = "schemas")
    @DisplayName("Records can be grouped with a code block")
    void testGroupBycodeStore() {
        inputs.forEach(rec -> inputTopic.pipeInput(rec));

        Map<String, Long> groupedRecords = outputTopic.readKeyValuesToMap();
        assertEquals(2, groupedRecords.size(), "records should be grouped by city");
        assertEquals(2L, groupedRecords.get("AMS"), "records for AMS should be counted");
        assertEquals(2L, groupedRecords.get("UTR"), "records for UTR should be counted");
    }
}


