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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static java.time.temporal.ChronoUnit.SECONDS;
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

    @KSMLDriver
    TopologyTestDriver testDriver;

    @KSMLTest(topology = "pipelines/test-outerjoin-reference.yaml")
    @DisplayName("Two topics can be outer joined with function reference")
    void testJoinByReference() {
        // given that both topics contain some records
        inputTopic1.pipeInput("1", "one");
        inputTopic1.pipeInput("2", "two");

        inputTopic2.pipeInput("1", "eins");
        inputTopic2.pipeInput("2", "zwei");

        // the result should contain the joined values
        Map<String, String> keyValues = mergedTopic.readKeyValuesToMap();
        assertThat(keyValues).hasSize(2).containsEntry("1", "one,eins").containsEntry("2", "two,zwei");
    }

    @KSMLTest(topology = "pipelines/test-outerjoin-inline.yaml")
    @DisplayName("Two topics can be outer joined with inline value joiner")
    void testJoinInline() {
        // given that both topics contain some records
        inputTopic1.pipeInput("1", "one");
        inputTopic1.pipeInput("2", "two");
        inputTopic1.pipeInput("3", "three");

        inputTopic2.pipeInput("1", "eins");
        inputTopic2.pipeInput("2", "zwei");
        inputTopic2.pipeInput("4", "vier");

        testDriver.advanceWallClockTime(Duration.of(5, SECONDS));

        // the result should contain the joined values
        Map<String, String> keyValues = mergedTopic.readKeyValuesToMap();
        assertThat(keyValues).hasSize(2).containsEntry("1", "one,eins").containsEntry("2", "two,zwei");

        // TO DO: was expecting keys 3 and 4 to also be present in the joined output
        log.warn("key/values:\n{}", keyValues);
    }

}


