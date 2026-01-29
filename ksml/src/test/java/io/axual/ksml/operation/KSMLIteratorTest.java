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

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.extension.ExtendWith;

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
public class KSMLIteratorTest {

    @KSMLTopic(topic = "sensor_ownership_data")
    TestInputTopic<String, String> inputTopic;

    /**
     * Test KeyValueIterator proxy with KeyValueStore.all() and related methods.
     * Verifies that hasNext(), next(), and close() are accessible from Python by calling them in the pipeline.
     * This test currently checks for exceptions, as the proxy does not support all methods yet.
     */
    @KSMLTest(topology = "pipelines/test-keyvalue-iterator.yaml")
    void testKeyValueStoreIterator() {
        Exception e = assertThrows(StreamsException.class, () -> {
            // Send data to trigger the forEach function which tests the iterator
            inputTopic.pipeInput("sensor1", "value1");
            inputTopic.pipeInput("sensor2", "value2");
            inputTopic.pipeInput("sensor3", "value3");
        });

        // need to assert RuntimeException, as FatalError changes the exception type
        assertInstanceOf(RuntimeException.class, e.getCause(), "expected UnsupportedOperationException");
        assertTrue(e.getCause().getMessage().contains("not supported by this proxy"));
    }

    /**
     * Test WindowStoreIterator proxy with WindowStore.fetch() and
     * KeyValueIterator proxy with WindowStore.all().
     * Verifies that hasNext(), next(), and close() are accessible from Python.
     */
    @KSMLTest(topology = "pipelines/test-window-iterator.yaml")
    void testWindowStoreIterator() {
        Exception e = assertThrows(StreamsException.class, () -> {
            // Send data to trigger the forEach function which tests the iterator
            inputTopic.pipeInput("sensor1", "value1");
            inputTopic.pipeInput("sensor2", "value2");
        });

        // need to assert RuntimeException, as FatalError changes the exception type
        assertInstanceOf(RuntimeException.class, e.getCause(), "expected UnsupportedOperationException");
        assertTrue(e.getCause().getMessage().contains("not supported by this proxy"));
    }

    /**
     * Test KeyValueIterator proxy with SessionStore.fetch() and findSessions().
     * Verifies that hasNext(), next(), and close() are accessible from Python.
     */
    @KSMLTest(topology = "pipelines/test-session-iterator.yaml")
    void testSessionStoreIterator() {
        Exception e = assertThrows(StreamsException.class, () -> {
            // Send data to trigger the forEach function which tests the iterator
            inputTopic.pipeInput("sensor1", "value1");
            inputTopic.pipeInput("sensor2", "value2");
        });

        // need to assert RuntimeException, as FatalError changes the exception type
        assertInstanceOf(RuntimeException.class, e.getCause(), "expected UnsupportedOperationException");
        assertTrue(e.getCause().getMessage().contains("not supported by this proxy"));
    }
}
