package io.axual.ksml.docs.examples.reference.operations;

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

import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

// The punctuate example from the operation reference: order amounts summed per customer, sent every minute
@ExtendWith(KSMLTestExtension.class)
class PunctuateTest {

    private static final String TOPOLOGY = "docs-examples/reference/operations/punctuate-processor.yaml";

    @KSMLTopic(topic = "orders")
    TestInputTopic<String, String> orders;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = TOPOLOGY)
    @DisplayName("every minute the punctuator sends the total per customer with a task header, then starts again")
    void testTotalsPerMinute() {
        final var totals = driver.createOutputTopic("order_totals", new StringDeserializer(), new DoubleDeserializer());
        orders.pipeInput("alice", "{\"order_id\": \"o1\", \"customer\": \"alice\", \"amount\": 10.5}");
        orders.pipeInput("alice", "{\"order_id\": \"o2\", \"customer\": \"alice\", \"amount\": 2}");
        orders.pipeInput("bob", "{\"order_id\": \"o3\", \"customer\": \"bob\", \"amount\": 7.25}");
        // The orders themselves are not sent on
        assertThat(totals.isEmpty()).isTrue();

        driver.advanceWallClockTime(Duration.ofMinutes(1));
        final var records = totals.readRecordsToList();
        assertThat(records).extracting(TestRecord::key, TestRecord::value)
                .containsExactlyInAnyOrder(tuple("alice", 12.5), tuple("bob", 7.25));
        assertThat(records).allSatisfy(rec ->
                assertThat(new String(rec.headers().lastHeader("task").value(), StandardCharsets.UTF_8)).isEqualTo("0_0"));

        // The store was cleared, so the next minute sends nothing
        driver.advanceWallClockTime(Duration.ofMinutes(1));
        assertThat(totals.isEmpty()).isTrue();
    }
}
