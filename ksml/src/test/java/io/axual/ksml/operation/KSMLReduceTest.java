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

import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLReduceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @KSMLTopic(topic = "inMessages")
    TestInputTopic<String, String> inMessages;

    @KSMLTopic(topic = "outMessages")
    TestOutputTopic<String, String> outMessages;

    @KSMLTopic(topic = "count_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> countTopic;

    @KSMLDriver
    TopologyTestDriver testDriver;

    @KSMLTest(topology = "pipelines/test-reduce.yaml")
    void testReduce() {

        // Given that we receive 5 messages grouped by 2 keys
        inMessages.pipeInput("key1", "Hello ");
        inMessages.pipeInput("key1", "World");
        inMessages.pipeInput("key1", " from KSML");
        inMessages.pipeInput("key2", "foo");
        inMessages.pipeInput("key2", "bar");

        // we should see 5 output messages with the values for each key concatenated
        assertThat(outMessages.getQueueSize()).isEqualTo(5);
        List<KeyValue<String, String>> keyValuesList = outMessages.readKeyValuesToList();
        assertThat(keyValuesList). contains(
                new KeyValue<>("key1", "Hello "),
                new KeyValue<>("key1", "Hello World"),
                new KeyValue<>("key1", "Hello World from KSML"),
                new KeyValue<>("key2", "foo"),
                new KeyValue<>("key2", "foobar")
        );
    }

    @KSMLTest(topology = "pipelines/test-reduce-tombstone.yaml")
    void testReduceSkipsTombstones() {
        // given a tombstone between three orders, on a json topic
        inMessages.pipeInput("a", "{\"amount\": 1}");
        inMessages.pipeInput("a", "{\"amount\": 2}");
        inMessages.pipeInput("a", (String) null);
        inMessages.pipeInput("a", "{\"amount\": 4}");

        // the reducer should not be called for the tombstone, like in Kafka Streams
        final List<JsonNode> sums = outMessages.readValuesToList().stream().map(MAPPER::readTree).toList();
        assertThat(sums).containsExactly(
                MAPPER.readTree("{\"amount\": 1}"),
                MAPPER.readTree("{\"amount\": 3}"),
                MAPPER.readTree("{\"amount\": 7}"));
    }

    @KSMLTest(topology = "pipelines/test-reduce-none.yaml")
    void testReducerNoneDeletesKey() {
        // given that the reducer returns None for an amount of 0
        inMessages.pipeInput("a", "{\"amount\": 1}");
        inMessages.pipeInput("a", "{\"amount\": 2}");
        inMessages.pipeInput("a", "{\"amount\": 0}");

        // the None deletes the key from the store
        final KeyValueStore<String, Object> store = testDriver.getKeyValueStore("sum_store");
        assertThat(store.get("a")).isNull();

        // and the next step gets a tombstone, so the count of the results skips it
        inMessages.pipeInput("a", "{\"amount\": 5}");
        assertThat(countTopic.readKeyValuesToList()).containsExactly(
                new KeyValue<>("a", 1L),
                new KeyValue<>("a", 2L),
                new KeyValue<>("a", 3L));
    }
}
