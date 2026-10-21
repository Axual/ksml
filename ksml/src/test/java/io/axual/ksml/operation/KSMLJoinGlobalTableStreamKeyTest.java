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
 * The valueJoiner of a stream-globalTable join gets the key of the stream record, not the key that
 * the mapper looks up in the globalTable.
 */
@ExtendWith(KSMLTestExtension.class)
class KSMLJoinGlobalTableStreamKeyTest {

    @KSMLTopic(topic = "orders")
    TestInputTopic<String, String> orders;

    @KSMLTopic(topic = "customers")
    TestInputTopic<String, String> customers;

    @KSMLTopic(topic = "joinOut")
    TestOutputTopic<String, String> joinOut;

    @KSMLTopic(topic = "leftJoinOut")
    TestOutputTopic<String, String> leftJoinOut;

    @KSMLTest(topology = "pipelines/test-join-globaltable-stream-key.yaml")
    void joinerGetsStreamKey() {
        customers.pipeInput("customer1", "Alice");

        orders.pipeInput("order1", "customer1");
        orders.pipeInput("order2", "customer9");

        assertThat(joinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "order1|customer1|Alice"));
        assertThat(leftJoinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "order1|customer1|Alice"),
                new KeyValue<>("order2", "order2|customer9|None"));
    }
}
