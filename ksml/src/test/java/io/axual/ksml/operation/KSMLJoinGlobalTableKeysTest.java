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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In a join with a globalTable, the valueJoiner gets the key of the stream record in {@code key} and
 * the key that the mapper returned in {@code mappedKey}. In other joins, {@code mappedKey} is None.
 */
@ExtendWith(KSMLTestExtension.class)
class KSMLJoinGlobalTableKeysTest {

    @KSMLTopic(topic = "orders")
    TestInputTopic<String, String> orders;

    @KSMLTopic(topic = "customers")
    TestInputTopic<String, String> customers;

    @KSMLTopic(topic = "orderNotes")
    TestInputTopic<String, String> orderNotes;

    @KSMLTopic(topic = "joinOut")
    TestOutputTopic<String, String> joinOut;

    @KSMLTopic(topic = "leftJoinOut")
    TestOutputTopic<String, String> leftJoinOut;

    @KSMLTopic(topic = "tableJoinOut")
    TestOutputTopic<String, String> tableJoinOut;

    @KSMLTest(topology = "pipelines/test-join-globaltable-keys.yaml")
    void joinerGetsStreamKeyAndMappedKey() {
        customers.pipeInput("customer1", "Alice");
        orderNotes.pipeInput("order1", "fragile");

        orders.pipeInput("order1", "customer1");
        orders.pipeInput("order2", "customer9");

        assertThat(joinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "order1|customer1|customer1|Alice"));
        assertThat(leftJoinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "order1|customer1|customer1|Alice"),
                new KeyValue<>("order2", "order2|customer9|customer9|None"));
        assertThat(tableJoinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "order1|None|customer1|fragile"));
    }

    @KSMLTest(topology = "pipelines/test-join-globaltable-none-key.yaml")
    void mapperThatReturnsNoneMeansNoLookup() {
        customers.pipeInput("customer1", "Alice");

        orders.pipeInput("order1", "customer1");
        orders.pipeInput("order2", "no-customer");

        assertThat(joinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "order1|Alice"));
        assertThat(leftJoinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "order1|Alice"),
                new KeyValue<>("order2", "order2|None"));
    }
}
