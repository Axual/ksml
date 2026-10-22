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
 * A streamPartitioner gets the record headers. The partitioner in the fixture drops records with the
 * header route=drop, so only the other records reach the output topic.
 */
@ExtendWith(KSMLTestExtension.class)
class KSMLStreamPartitionerHeadersTest {

    @KSMLTopic(topic = "events")
    TestInputTopic<String, String> events;

    @KSMLTopic(topic = "routedEvents")
    TestOutputTopic<String, String> routedEvents;

    @KSMLTopic(topic = "oldStyleEvents")
    TestOutputTopic<String, String> oldStyleEvents;

    @KSMLTest(topology = "pipelines/test-stream-partitioner-headers.yaml")
    void partitionerReadsHeaders() {
        events.pipeInput(new TestRecord<>("event1", "keep me", headers("route", "keep")));
        events.pipeInput(new TestRecord<>("event2", "drop me", headers("route", "drop")));
        events.pipeInput(new TestRecord<>("event3", "no headers"));

        assertThat(routedEvents.readKeyValuesToList()).containsExactly(
                new KeyValue<>("event1", "keep me"),
                new KeyValue<>("event3", "no headers"));
        // A generic partitioner without the headers parameter still works
        assertThat(oldStyleEvents.readKeyValuesToList()).containsExactly(
                new KeyValue<>("event1", "keep me"),
                new KeyValue<>("event3", "no headers"));
    }

    private static RecordHeaders headers(String key, String value) {
        final var headers = new RecordHeaders();
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        return headers;
    }
}
