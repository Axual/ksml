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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KSMLTestExtension.class)
class KSMLCogroupTest {

    @KSMLTopic(topic = "first_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestInputTopic<String, Long> firstInput;

    @KSMLTopic(topic = "second_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestInputTopic<String, Long> secondInput;

    @KSMLTopic(topic = "third_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestInputTopic<String, Long> thirdInput;

    @KSMLTopic(topic = "output_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> outputTopic;

    @KSMLTest(topology = "pipelines/test-cogroup-three-streams.yaml")
    @DisplayName("cogroup of three streams applies the aggregator of each stream to one aggregate")
    void testCogroupThreeStreams() {
        firstInput.pipeInput("key1", 1L);
        secondInput.pipeInput("key1", 2L);
        thirdInput.pipeInput("key1", 3L);
        secondInput.pipeInput("key2", 4L);

        assertThat(outputTopic.readKeyValuesToList()).containsExactly(
                new KeyValue<>("key1", 1L),
                new KeyValue<>("key1", 21L),
                new KeyValue<>("key1", 321L),
                new KeyValue<>("key2", 40L));
    }

    @KSMLTest(topology = "pipelines/test-cogroup-windowed.yaml")
    @DisplayName("cogroup followed by windowByTime aggregates both streams per time window")
    void testCogroupWindowByTime() {
        final var firstWindow = Instant.ofEpochMilli(60_000);
        final var secondWindow = Instant.ofEpochMilli(120_000);
        firstInput.pipeInput("key1", 1L, firstWindow);
        secondInput.pipeInput("key1", 2L, firstWindow.plusSeconds(10));
        firstInput.pipeInput("key1", 3L, secondWindow);
        secondInput.pipeInput("key1", 4L, secondWindow.plusSeconds(10));

        assertThat(outputTopic.readKeyValuesToList()).containsExactly(
                new KeyValue<>("key1@60000", 1L),
                new KeyValue<>("key1@60000", 21L),
                new KeyValue<>("key1@120000", 3L),
                new KeyValue<>("key1@120000", 43L));
    }
}
