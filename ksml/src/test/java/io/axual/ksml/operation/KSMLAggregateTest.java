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

import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith( KSMLTestExtension.class)
public class KSMLAggregateTest {

    @KSMLDriver
    TopologyTestDriver testDriver;

    @KSMLTopic(topic = "input_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestInputTopic<String, Long> inputTopic;

    @KSMLTopic(topic = "output_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> outputTopic;

    @KSMLTest(topology = "pipelines/test-aggregate-store.yaml")
    @DisplayName("aggregate should work with a named keyValue store")
    void testAggregate() {
        // given that we send some numbers with the same key
        inputTopic.pipeInput("key1", 1L);
        inputTopic.pipeInput("key1", 2L);
        inputTopic.pipeInput("key1", 3L);

        // the table as a topic should show the values aggregagting
        List<KeyValue<String, Long>> keyValues = outputTopic.readKeyValuesToList();
        assertThat(keyValues).contains(
                new KeyValue<>("key1", 1L),
                new KeyValue<>("key1", 3L),
                new KeyValue<>("key1", 6L)
        );

        // and the named keyvalue store should have the final result
        KeyValueStore<String, Long> aggregateStore = testDriver.getKeyValueStore("aggregate_store");
        assertThat(aggregateStore.get("key1")).isEqualTo(6L);
    }

    @KSMLTest(topology = "pipelines/test-aggregate-inline.yaml")
    @DisplayName("aggregate should work with an inline keyvalue store definition")
    void testAggregateInline() {
        // given that we send some numbers with the same key
        inputTopic.pipeInput("key1", 1L);
        inputTopic.pipeInput("key1", 2L);
        inputTopic.pipeInput("key1", 3L);

        // the table as a topic should show the values aggregagting
        List<KeyValue<String, Long>> keyValues = outputTopic.readKeyValuesToList();
        assertThat(keyValues).contains(
                new KeyValue<>("key1", 1L),
                new KeyValue<>("key1", 3L),
                new KeyValue<>("key1", 6L)
        );
    }
}
