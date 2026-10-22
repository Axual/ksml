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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Custom parameters of a valueJoiner come before the optional built-in parameter mappedKey. Custom
 * parameters of a streamPartitioner come after its built-in parameters.
 */
@ExtendWith(KSMLTestExtension.class)
class KSMLCustomParametersTest {

    @KSMLTopic(topic = "orders")
    TestInputTopic<String, String> orders;

    @KSMLTopic(topic = "orderNotes")
    TestInputTopic<String, String> orderNotes;

    @KSMLTopic(topic = "customers")
    TestInputTopic<String, String> customers;

    @KSMLTopic(topic = "events")
    TestInputTopic<String, String> events;

    @KSMLTopic(topic = "tableJoinOut")
    TestOutputTopic<String, String> tableJoinOut;

    @KSMLTopic(topic = "globalJoinOut")
    TestOutputTopic<String, String> globalJoinOut;

    @KSMLTopic(topic = "routedEvents")
    TestOutputTopic<String, String> routedEvents;

    @KSMLTest(topology = "pipelines/test-custom-parameters-with-optional-builtins.yaml")
    void customParametersKeepTheirPosition() {
        customers.pipeInput("customer1", "Alice");
        orderNotes.pipeInput("order1", "fragile");
        orders.pipeInput("order1", "customer1");

        // Positional call from Python: "X" goes to the custom parameter, mappedKey keeps its default
        assertThat(tableJoinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "X|order1|None|customer1|fragile"));
        // Called by KSML in a globalTable join: the custom parameter gets its default, mappedKey is filled in
        assertThat(globalJoinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("order1", "D|order1|customer1|customer1|Alice"));
    }

    @KSMLTest(topology = "pipelines/test-custom-parameters-with-optional-builtins.yaml")
    void partitionerWithCustomParameterGetsHeaders() {
        events.pipeInput(new TestRecord<>("event1", "keep me", headers("route", "keep")));
        events.pipeInput(new TestRecord<>("event2", "drop me", headers("route", "drop")));

        assertThat(routedEvents.readKeyValuesToList()).containsExactly(new KeyValue<>("event1", "keep me"));
    }

    private static RecordHeaders headers(String key, String value) {
        final var headers = new RecordHeaders();
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        return headers;
    }
}
