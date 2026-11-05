package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static io.axual.ksml.testutil.StoreMetrics.openIterators;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for plain window stores (AXPD-11068): Python calls every window store method on an in-memory and a
 * RocksDB store, reads each iterator with a for loop and with a hasNext() loop, and leaves no iterator open.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLWindowStoreMethodsTest {

    @KSMLTopic(topic = "window_methods_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "window_methods_output")
    TestOutputTopic<String, String> output;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = "pipelines/test-window-store-methods-python.yaml", modulesDirectory = "pipelines")
    @DisplayName("Window store methods work from Python on in-memory and RocksDB stores, and close their iterators")
    void testWindowStoreMethods() {
        // Record timestamps near the window times (BASE = 3000000000 in the pipeline), so the windows are within the
        // store retention. The pipeline shows window times minus BASE.
        input.pipeInput("a", "1", 3_000_001_000L);
        input.pipeInput("b", "2", 3_000_002_000L);
        input.pipeInput("a", "3", 3_000_061_000L);
        input.pipeInput("a", "4", 3_001_801_000L);

        final var lines = output.readValuesToList().stream()
                .filter(v -> !v.isEmpty())
                .flatMap(String::lines)
                .toList();
        final var single = " fetch=1 missing=None/None fetchRange=0:1,60000:3 backwardFetchRange=60000:3,0:1";
        // The in-memory store orders by window start, then key. RocksDB orders by time segment, then key, then window
        // start: the window at 1800000 is in the next segment, so it comes after key b. The time-range fetches leave it
        // out.
        final var memoryOrder = "a@0:1,b@0:2,a@60000:3";
        final var rocksdbOrder = "a@0:1,a@60000:3,b@0:2";
        final var memoryAll = memoryOrder + ",a@1800000:4";
        final var rocksdbAll = rocksdbOrder + ",a@1800000:4";
        assertThat(lines).containsExactly(
                "memory" + single + iterators(memoryOrder, memoryAll),
                "rocksdb" + single + iterators(rocksdbOrder, rocksdbAll));

        assertThat(openIterators(driver, "window_memory")).isZero();
        assertThat(openIterators(driver, "window_rocksdb")).isZero();
    }

    private static String iterators(String inRange, String all) {
        return " fetchKeys=" + inRange + " backwardFetchKeys=" + reversed(inRange)
                + " all=" + all + " backwardAll=" + reversed(all)
                + " fetchAll=" + inRange + " backwardFetchAll=" + reversed(inRange);
    }

    private static String reversed(String entries) {
        return String.join(",", List.of(entries.split(",")).reversed());
    }
}
