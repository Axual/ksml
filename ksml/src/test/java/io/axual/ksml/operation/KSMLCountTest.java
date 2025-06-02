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
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
@ExtendWith({KSMLTestExtension.class})
class KSMLCountTest {

    @KSMLTopic(topic = "input_topic")
    protected TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "output_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    protected TestOutputTopic<String, Long> outputTopic;

    @KSMLTest(topology = "pipelines/test-count.yaml")
    void testCount() {
        log.debug("testCount()");

        // given that we have some keys occurring multiple times
        inputTopic.pipeInput("keyFirst", "value1");
        inputTopic.pipeInput("keySecond", "value1");
        inputTopic.pipeInput("keyThird", "value1");
        inputTopic.pipeInput("keyFirst", "value2");
        inputTopic.pipeInput("keySecond", "value2");
        inputTopic.pipeInput("keyFirst", "value3");

        // the pipeline should count the records for each key
        assertFalse(outputTopic.isEmpty(), "records should be counted");
        Map<String, Long> keyValueMap = outputTopic.readKeyValuesToMap();

        assertEquals(3, keyValueMap.get("keyFirst"), "keyFirst should have counted 3 records");
        assertEquals(2, keyValueMap.get("keySecond"), "keySecond should have counted 2 records");
        assertEquals(1, keyValueMap.get("keyThird"), "keyThird should have counted 1 records");
    }
}
