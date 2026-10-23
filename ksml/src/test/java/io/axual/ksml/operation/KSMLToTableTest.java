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

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KSMLTestExtension.class)
class KSMLToTableTest {

    @KSMLTopic(topic = "left_topic")
    TestInputTopic<String, String> left;

    @KSMLTopic(topic = "right_topic")
    TestInputTopic<String, String> right;

    @KSMLTopic(topic = "output_topic")
    TestOutputTopic<String, String> output;

    @KSMLTest(topology = "pipelines/test-totable-without-store.yaml")
    @DisplayName("a toTable without a store, after a filter, can be joined with another table")
    void testToTableWithoutStoreJoinsWithTable() {
        left.pipeInput("key1", "l");
        right.pipeInput("key1", "skip");
        right.pipeInput("key1", "r");

        assertThat(output.readKeyValuesToList()).containsExactly(new KeyValue<>("key1", "r+l"));
    }
}
