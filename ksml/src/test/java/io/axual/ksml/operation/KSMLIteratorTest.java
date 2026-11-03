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
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLIteratorTest {

    @KSMLTopic(topic = "sensor_ownership_data")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "store_iterator_input")
    TestInputTopic<String, String> iteratorInput;

    @KSMLTopic(topic = "store_iterator_output")
    TestOutputTopic<String, String> iteratorOutput;

    /**
     * Test KeyValueIterator proxy with KeyValueStore.all() and related methods.
     * Verifies that hasNext(), next(), and close() are accessible from Python by calling them in the pipeline.
     * This test currently checks for exceptions, as the proxy does not support all methods yet.
     */
    @KSMLTest(topology = "pipelines/test-keyvalue-iterator.yaml")
    void testKeyValueStoreIterator() {
        // Send data to trigger the forEach function which tests the iterator
        inputTopic.pipeInput("sensor1", "value1");
        inputTopic.pipeInput("sensor2", "value2");
        inputTopic.pipeInput("sensor3", "value3");

        // No exception is thrown and all messages could be processed
    }

    /**
     * Test WindowStoreIterator proxy with WindowStore.fetch() and
     * KeyValueIterator proxy with WindowStore.all().
     * Verifies that hasNext(), next(), and close() are accessible from Python.
     */
    @KSMLTest(topology = "pipelines/test-window-iterator.yaml")
    void testWindowStoreIterator() {
        // Send data to trigger the forEach function which tests the iterator
        inputTopic.pipeInput("sensor1", "value1");
        inputTopic.pipeInput("sensor2", "value2");

        // No exception is thrown and all messages could be processed
    }

    /**
     * Test KeyValueIterator proxy with SessionStore.fetch() and findSessions().
     * Verifies that hasNext(), next(), and close() are accessible from Python.
     */
    @KSMLTest(topology = "pipelines/test-session-iterator.yaml")
    void testSessionStoreIterator() {
        // Send data to trigger the forEach function which tests the iterator
        inputTopic.pipeInput("sensor3", "value3");
        inputTopic.pipeInput("sensor4", "value4");

        // No exception is thrown and all messages could be processed
    }

    /**
     * Python iterates over key-value, window and session stores (in-memory and RocksDB) with for, list(),
     * next() and contextlib.closing. Each line of the output is the result of one store method.
     */
    @KSMLTest(topology = "pipelines/test-store-iterator-python.yaml")
    @DisplayName("store iterators work with Python for, list(), next() and contextlib.closing")
    void testPythonIteration() {
        iteratorInput.pipeInput("a", "1");
        iteratorInput.pipeInput("b", "2");

        final var values = iteratorOutput.readValuesToList();
        assertThat(values).hasSize(2);
        assertThat(List.of(values.get(1).split("\n"))).containsExactly(
                "kv_memory.all for=a:1,b:2 list=a:1,b:2 next=a:1 closing=a:1,b:2",
                "kv_rocksdb.all for=a:1,b:2 list=a:1,b:2 next=a:1 closing=a:1,b:2",
                "window_memory.all for=a@0:1,b@0:2 list=a@0:1,b@0:2 next=a@0:1 closing=a@0:1,b@0:2",
                "window_memory.fetch for=0:2 list=0:2 next=0:2 closing=0:2",
                "window_rocksdb.all for=a@0:1,b@0:2 list=a@0:1,b@0:2 next=a@0:1 closing=a@0:1,b@0:2",
                "window_rocksdb.fetch for=0:2 list=0:2 next=0:2 closing=0:2",
                "session_memory.fetch for=b@0:2 list=b@0:2 next=b@0:2 closing=b@0:2",
                "session_rocksdb.fetch for=b@0:2 list=b@0:2 next=b@0:2 closing=b@0:2");
    }
}
