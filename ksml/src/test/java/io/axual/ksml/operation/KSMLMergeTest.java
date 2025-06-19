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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KSMLTestExtension.class)
@Slf4j
public class KSMLMergeTest {

    @KSMLTopic(topic = "input1")
    TestInputTopic<String, String> inputTopic1;

    @KSMLTopic(topic = "input2")
    TestInputTopic<String, String> inputTopic2;

    @KSMLTopic(topic = "merged")
    TestOutputTopic<String, String> mergedTopic;

    @KSMLTest(topology = "pipelines/test-merge.yaml")
    @DisplayName("Two topics can be merged")
    void testMerge() {
        // given that both topics contain some records
        inputTopic1.pipeInput("1", "one");
        inputTopic1.pipeInput("2", "two");

        inputTopic2.pipeInput("1", "eins");
        inputTopic2.pipeInput("2", "zwei");

        // we expect the output to contain all messages.
        // there are no guarantees for ordering but all records should be present; check values
        List<KeyValue<String, String>> keyValues = mergedTopic.readKeyValuesToList();
        assertThat(keyValues)
                .hasSize(4)
                .containsExactlyInAnyOrder(
                        new KeyValue<>("1", "one"),
                        new KeyValue<>("2", "two"),
                        new KeyValue<>("1", "eins"),
                        new KeyValue<>("2", "zwei"));
    }

}


