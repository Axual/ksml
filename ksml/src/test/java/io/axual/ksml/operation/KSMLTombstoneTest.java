package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A tombstone (a record with a null value) means "delete" in Kafka. Kafka Streams skips tombstones in
 * count, reduce and aggregate, and KSML should do the same (AXPD-12444).
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
class KSMLTombstoneTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @KSMLTopic(topic = "tomb_in")
    TestInputTopic<String, String> ordersIn;

    @KSMLTopic(topic = "tomb_count_out", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> countsOut;

    @KSMLTopic(topic = "tomb_reduce_out")
    TestOutputTopic<String, String> sumsOut;

    private void pipeOrdersWithTombstone() {
        ordersIn.pipeInput("a", "{\"amount\": 1}");
        ordersIn.pipeInput("a", "{\"amount\": 2}");
        ordersIn.pipeInput("a", (String) null);
        ordersIn.pipeInput("a", "{\"amount\": 4}");
    }

    @KSMLTest(topology = "pipelines/test-tombstone-count.yaml")
    void countSkipsTombstones() {
        pipeOrdersWithTombstone();

        assertThat(countsOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("a", 1L),
                new KeyValue<>("a", 2L),
                new KeyValue<>("a", 3L));
    }

    @KSMLTest(topology = "pipelines/test-tombstone-reduce.yaml")
    void reduceSkipsTombstones() {
        pipeOrdersWithTombstone();

        final List<JsonNode> sums = sumsOut.readValuesToList().stream().map(MAPPER::readTree).toList();
        assertThat(sums).containsExactly(
                MAPPER.readTree("{\"amount\": 1}"),
                MAPPER.readTree("{\"amount\": 3}"),
                MAPPER.readTree("{\"amount\": 7}"));
    }
}
