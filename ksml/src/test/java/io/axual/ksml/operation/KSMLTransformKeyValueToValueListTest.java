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

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@ExtendWith({KSMLTestExtension.class})
class KSMLTransformKeyValueToValueListTest {

    @KSMLTopic(topic = "input_topic")
    protected TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "output_topic")
    protected TestOutputTopic<String, String> outputTopic;

    @KSMLTest(topology = "pipelines/test-transformkeyvaluetovaluelist-code.yaml")
    void testFlatMapValuesCode() {
        log.debug("testFlatMapValues()");

        // given that we pipe a single message into the stream
        inputTopic.pipeInput("keyFirst", "value1");

        // we expect the output to contain this record, duplicated
        assertEquals(3, outputTopic.getQueueSize(), "output should contain 3 records");

        List<KeyValue<String, String>> keyValues = outputTopic.readKeyValuesToList();
        assertEquals("value1a", keyValues.get(0).value);
        assertEquals("value1b", keyValues.get(1).value);
        assertEquals("value1c", keyValues.get(2).value);

        // and the key to be duplicated as is
        assertEquals("keyFirst", keyValues.get(0).key);
        assertEquals("keyFirst", keyValues.get(1).key);
        assertEquals("keyFirst", keyValues.get(2).key);
    }

    @KSMLTest(topology = "pipelines/test-transformkeyvaluetovaluelist-expression.yaml")
    void testFlatMapValuesExpression() {
        log.debug("testFlatMapValues()");

        // given that we pipe a single message into the stream
        inputTopic.pipeInput("keyFirst", "value1");

        // we expect the output to contain this record, duplicated
        assertEquals(3, outputTopic.getQueueSize(), "output should contain 3 records");

        List<KeyValue<String, String>> keyValues = outputTopic.readKeyValuesToList();
        assertEquals("value1-1", keyValues.get(0).value);
        assertEquals("value1-2", keyValues.get(1).value);
        assertEquals("value1-3", keyValues.get(2).value);

        // and the key to be duplicated as is
        assertEquals("keyFirst", keyValues.get(0).key);
        assertEquals("keyFirst", keyValues.get(1).key);
        assertEquals("keyFirst", keyValues.get(2).key);
    }
}
