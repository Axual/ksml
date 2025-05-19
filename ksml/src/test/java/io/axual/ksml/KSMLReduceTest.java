package io.axual.ksml;

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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KSMLTestExtension.class)
public class KSMLReduceTest {

    @KSMLTopic(topic = "inMessages")
    TestInputTopic inMessages;

    @KSMLTopic(topic = "outMessages")
    TestOutputTopic outMessages;

    @KSMLTest(topology = "pipelines/test-reduce.yaml")
    void testReduce() {

        // Given that we receive 5 messages grouped by 2 keys
        inMessages.pipeInput("key1", "Hello ");
        inMessages.pipeInput("key1", "World");
        inMessages.pipeInput("key1", " from KSML");
        inMessages.pipeInput("key2", "foo");
        inMessages.pipeInput("key2", "bar");

        // we should see 5 output messages with the values for each key concatenated
        assertEquals(5, outMessages.getQueueSize());
        List<KeyValue> keyValuesList = outMessages.readKeyValuesToList();
        assertThat(keyValuesList). contains(
                new KeyValue<>("key1", "Hello "),
                new KeyValue<>("key1", "Hello World"),
                new KeyValue<>("key1", "Hello World from KSML"),
                new KeyValue<>("key2", "foo"),
                new KeyValue<>("key2", "foobar")
        );
    }
}
