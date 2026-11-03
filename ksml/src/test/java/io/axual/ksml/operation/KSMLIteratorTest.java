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

    @KSMLTopic(topic = "store_iterator_input")
    TestInputTopic<String, String> iteratorInput;

    @KSMLTopic(topic = "store_iterator_output")
    TestOutputTopic<String, String> iteratorOutput;

    /**
     * Python iterates over key-value, window and session stores (in-memory and RocksDB) with for, list(),
     * next() and contextlib.closing. Each line of the output is the result of one store method.
     */
    @KSMLTest(topology = "pipelines/test-store-iterator-python.yaml")
    @DisplayName("store iterators work with Python for, list(), next() and contextlib.closing")
    void testPythonIteration() {
        // Fixed record timestamps, so the window entries at time 0 are within the store retention
        iteratorInput.pipeInput("a", "1", 1000L);
        iteratorInput.pipeInput("b", "2", 1000L);

        final var values = iteratorOutput.readValuesToList();
        assertThat(values).hasSize(2);
        assertThat(List.of(values.get(1).split("\n"))).containsExactly(
                "kv_memory.all for=a:1,b:2 list=a:1,b:2 next=a:1 closing=a:1,b:2",
                "kv_rocksdb.all for=a:1,b:2 list=a:1,b:2 next=a:1 closing=a:1,b:2",
                "window_memory.all for=a@0:1,b@0:2 list=a@0:1,b@0:2 next=a@0:1 closing=a@0:1,b@0:2",
                "window_memory.fetch for=0:2 list=0:2 next=0:2 closing=0:2",
                "window_rocksdb.all for=a@0:1,b@0:2 list=a@0:1,b@0:2 next=a@0:1 closing=a@0:1,b@0:2",
                "window_rocksdb.fetch for=0:2 list=0:2 next=0:2 closing=0:2",
                "session_memory.fetch for=b@3000000000:2 list=b@3000000000:2 next=b@3000000000:2 closing=b@3000000000:2",
                "session_rocksdb.fetch for=b@3000000000:2 list=b@3000000000:2 next=b@3000000000:2 closing=b@3000000000:2");
    }
}
