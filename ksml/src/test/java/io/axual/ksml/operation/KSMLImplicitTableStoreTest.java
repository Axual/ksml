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
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tables and globalTables without a declared store are not declared stores, so the Kafka Streams store
 * defaults (AXPD-12445) do not apply to them: their stores stay in memory and have no changelog topic.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLImplicitTableStoreTest {

    @KSMLTopic(topic = "orderNotes")
    TestInputTopic<String, String> orderNotes;

    @KSMLTopic(topic = "customers")
    TestInputTopic<String, String> customers;

    @KSMLDriver
    TopologyTestDriver topologyTestDriver;

    @KSMLTest(topology = "pipelines/test-join-globaltable-keys.yaml")
    @DisplayName("table and globalTable stores without a declared store are in memory and not logged")
    void implicitTableStoresAreInMemoryWithoutLogging() {
        orderNotes.pipeInput("o1", "note");
        customers.pipeInput("c1", "customer");

        assertThat(topologyTestDriver.getAllStateStores().get("orderNotes").persistent()).isFalse();
        assertThat(topologyTestDriver.getAllStateStores().get("customers").persistent()).isFalse();
        assertThat(topologyTestDriver.producedTopicNames()).noneMatch(topic -> topic.endsWith("-changelog"));
    }
}
