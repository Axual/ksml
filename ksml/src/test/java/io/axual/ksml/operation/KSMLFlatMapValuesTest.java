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
class KSMLFlatMapValuesTest {

    @KSMLTopic(topic = "input_topic")
    protected TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "output_topic")
    protected TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "pipelines/test-flatmapvalues-code.yaml")
    void testFlatMapValuesCode() {
        log.debug("testFlatMapValuesCode()");

        // given that we pipe a single message into the stream
        inputTopic.pipeInput("keyFirst", "value1");

        // we expect the output to contain this record, duplicated
        assertThat(outputTopic.getQueueSize()).as("output should contain 3 records").isEqualTo(3);

        List<KeyValue<String, String>> keyValues = outputTopic.readKeyValuesToList();
        assertThat(keyValues.get(0).value).isEqualTo("value1a");
        assertThat(keyValues.get(1).value).isEqualTo("value1b");
        assertThat(keyValues.get(2).value).isEqualTo("value1c");

        // and the key to be unchanged
        assertThat(keyValues.get(0).key).isEqualTo("keyFirst");
        assertThat(keyValues.get(1).key).isEqualTo("keyFirst");
        assertThat(keyValues.get(2).key).isEqualTo("keyFirst");
    }

    @KSMLTest(topology = "pipelines/test-flatmapvalues-expression.yaml")
    void testFlatMapValuesExpression() {
        log.debug("testFlatMapValuesExpression()");

        // given that we pipe a single message into the stream
        inputTopic.pipeInput("keyFirst", "value1");

        // we expect the output to contain this record, duplicated
        assertThat(outputTopic.getQueueSize()).as("output should contain 3 records").isEqualTo(3);

        List<KeyValue<String, String>> keyValues = outputTopic.readKeyValuesToList();
        assertThat(keyValues.get(0).value).isEqualTo("value1-1");
        assertThat(keyValues.get(1).value).isEqualTo("value1-2");
        assertThat(keyValues.get(2).value).isEqualTo("value1-3");

        // and the key to be unchanged
        assertThat(keyValues.get(0).key).isEqualTo("keyFirst");
        assertThat(keyValues.get(1).key).isEqualTo("keyFirst");
        assertThat(keyValues.get(2).key).isEqualTo("keyFirst");
    }
}
