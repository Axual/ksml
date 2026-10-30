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

@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
class KSMLConvertValueTest {

    @KSMLTopic(topic = "input_topic")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "count_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> countTopic;

    @KSMLTest(topology = "pipelines/test-convertvalue-tombstone.yaml")
    void testConvertValueKeepsTombstones() {
        // given a tombstone between two records, converted from json to string
        inputTopic.pipeInput("a", "{\"v\": 1}");
        inputTopic.pipeInput("a", (String) null);
        inputTopic.pipeInput("a", "{\"v\": 2}");

        // the tombstone should stay a tombstone, so the count skips it
        assertThat(countTopic.readKeyValuesToList()).containsExactly(
                new KeyValue<>("a", 1L),
                new KeyValue<>("a", 2L));
    }
}
