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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@ExtendWith({KSMLTestExtension.class})
@SuppressWarnings("java:S2187")
class KSMLFlatMapTest {

    @KSMLTopic(topic = "input_topic")
    protected TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "output_topic")
    protected TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "pipelines/test-flatmap.yaml")
    void testFlatMap() {
        log.debug("testFlatMap()");

        // given that we pipe a message into the stream
        inputTopic.pipeInput("someKey", "someValue");

        // we expect the output to contain this record, duplicated
        assertThat(outputTopic.getQueueSize()).as("output should contain 2 records").isEqualTo(2);

        List<KeyValue<String, String>> keyValues = outputTopic.readKeyValuesToList();
        assertThat(keyValues.get(0).key).as("key should be copied").isEqualTo("someKey");
        assertThat(keyValues.get(0).value).as("value should be copied").isEqualTo("someValue");
        assertThat(keyValues.get(1).key).as("key should be copied and changed").isEqualTo("someKey-b");
        assertThat(keyValues.get(1).value).as("value should be copied and changed").isEqualTo("someValue-b");
    }
}
