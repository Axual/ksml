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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@ExtendWith({KSMLTestExtension.class})
@SuppressWarnings("java:S2187")
class KSMLCountTest {

    @KSMLTopic(topic = "input_topic")
    protected TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "output_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    protected TestOutputTopic<String, Long> outputTopic;

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    protected TestInputTopic<String, GenericRecord> avroTopic;

    @KSMLTest(topology = "pipelines/test-count.yaml")
    void testCount() {
        log.debug("testCount()");

        // given that we have some keys occurring multiple times
        inputTopic.pipeInput("keyFirst", "value1");
        inputTopic.pipeInput("keySecond", "value1");
        inputTopic.pipeInput("keyThird", "value1");
        inputTopic.pipeInput("keyFirst", "value2");
        inputTopic.pipeInput("keySecond", "value2");
        inputTopic.pipeInput("keyFirst", "value3");

        // the pipeline should count the records for each key
        assertThat(outputTopic.isEmpty()).as("records should be counted").isFalse();
        Map<String, Long> keyValueMap = outputTopic.readKeyValuesToMap();

        assertThat(keyValueMap.get("keyFirst")).as("keyFirst should have counted 3 records").isEqualTo(3);
        assertThat(keyValueMap.get("keySecond")).as("keySecond should have counted 2 records").isEqualTo(2);
        assertThat(keyValueMap.get("keyThird")).as("keyThird should have counted 1 records").isEqualTo(1);
    }

    @KSMLTest(topology = "pipelines/test-count-tombstone.yaml")
    void testCountSkipsTombstones() {
        // given a tombstone between three orders, on a json topic
        inputTopic.pipeInput("a", "{\"amount\": 1}");
        inputTopic.pipeInput("a", "{\"amount\": 2}");
        inputTopic.pipeInput("a", (String) null);
        inputTopic.pipeInput("a", "{\"amount\": 4}");

        // the tombstone should not be counted, like in Kafka Streams
        assertThat(outputTopic.readKeyValuesToList()).containsExactly(
                new KeyValue<>("a", 1L),
                new KeyValue<>("a", 2L),
                new KeyValue<>("a", 3L));
    }

    @KSMLTest(topology = "pipelines/test-count-tombstone-avro.yaml", schemaDirectory = "schemas")
    void testCountSkipsTombstonesOnAvroTopic() {
        // given a tombstone between two records, on an avro topic
        avroTopic.pipeInput("a", SensorData.builder().color("blue").build().toRecord());
        avroTopic.pipeInput("a", (GenericRecord) null);
        avroTopic.pipeInput("a", SensorData.builder().color("red").build().toRecord());

        // the tombstone should not be counted, like in Kafka Streams
        assertThat(outputTopic.readKeyValuesToList()).containsExactly(
                new KeyValue<>("a", 1L),
                new KeyValue<>("a", 2L));
    }

    @KSMLTest(topology = "pipelines/test-count-null-key.yaml")
    void testCountSkipsNullKeys() {
        // given records with a null key, on a topic with a json key
        inputTopic.pipeInput("{\"id\": 1}", "x");
        inputTopic.pipeInput((String) null, "y");
        inputTopic.pipeInput((String) null, "z");

        // the records with a null key should be skipped, like in Kafka Streams
        final var result = outputTopic.readKeyValuesToList();
        assertThat(result).hasSize(1);
        assertThat(result.getFirst().key).isEqualToIgnoringWhitespace("{\"id\":1}");
        assertThat(result.getFirst().value).isEqualTo(1L);
    }
}
