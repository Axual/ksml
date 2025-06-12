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

import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KSMLTestExtension.class)
@Slf4j
public class KSMLOuterJoinTest {

    @KSMLTopic(topic = "input1")
    TestInputTopic<String,String> inputTopic1;

    @KSMLTopic(topic = "input2")
    TestInputTopic<String,String> inputTopic2;

    @KSMLTopic(topic = "joined")
    TestOutputTopic<String,String> mergedTopic;

    @KSMLTest(topology = "pipelines/test-outerjoin-reference.yaml")
    @DisplayName("Two topics can be outer joined with function reference")
    void testJoinByReference() {
        // given that both topics contain some timestamped records
        inputTopic1.pipeInput("1", "one", 100);
        inputTopic1.pipeInput("2", "two", 100);
        inputTopic1.pipeInput("3", "three", 100);

        inputTopic2.pipeInput("1", "eins",120);
        inputTopic2.pipeInput("2", "zwei",120);
        inputTopic2.pipeInput("4", "vier",120);

        // and the join window has passed (by sending a message into the next window)
        inputTopic1.pipeInput("5", "trois", 6000);

        // the result should contain all keys
        Map<String, String> keyValues = mergedTopic.readKeyValuesToMap();

        assertThat(keyValues).hasSize(4)
                .containsEntry("1", "one,eins")
                .containsEntry("2", "two,zwei")
                .containsEntry("3", "three,?")
                .containsEntry("4", "?,vier");
    }

    @KSMLTest(topology = "pipelines/test-outerjoin-inline.yaml")
    @DisplayName("Two topics can be outer joined with inline value joiner")
    void testJoinInline() {
        // given that both topics contain some timestamped records
        inputTopic1.pipeInput("1", "one", 100);
        inputTopic1.pipeInput("2", "two", 100);
        inputTopic1.pipeInput("3", "three", 100);

        inputTopic2.pipeInput("1", "eins",120);
        inputTopic2.pipeInput("2", "zwei",120);
        inputTopic2.pipeInput("4", "vier",120);

        // and the join window has passed (by sending a message into the next window)
        inputTopic1.pipeInput("5", "trois", 6000);

        // the result should contain all keys
        Map<String, String> keyValues = mergedTopic.readKeyValuesToMap();

        assertThat(keyValues).hasSize(4)
                .containsEntry("1", "one,eins")
                .containsEntry("2", "two,zwei")
                .containsEntry("3", "three,?")
                .containsEntry("4", "?,vier");
    }

}


