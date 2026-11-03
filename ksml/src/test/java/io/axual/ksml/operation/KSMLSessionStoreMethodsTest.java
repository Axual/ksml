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

import static io.axual.ksml.testutil.StoreMetrics.openIterators;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for session stores (AXPD-11068): Python calls every session store method on an in-memory and a
 * RocksDB store, reads each iterator with a for loop and with a hasNext() loop, and leaves no iterator open.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLSessionStoreMethodsTest {

    @KSMLTopic(topic = "session_methods_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "session_methods_output")
    TestOutputTopic<String, String> output;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = "pipelines/test-session-store-methods-python.yaml")
    @DisplayName("Session store methods work from Python on in-memory and RocksDB stores, and close their iterators")
    void testSessionStoreMethods() {
        input.pipeInput("a", "1", 1000L);
        input.pipeInput("b", "2", 2000L);
        input.pipeInput("a", "3", 3000L);

        final var lines = output.readValuesToList().stream()
                .filter(v -> !v.isEmpty())
                .flatMap(v -> v.lines())
                .toList();
        final var singleKey = " fetchSession=1 missing=None"
                + " fetch=a@10:1,a@30:3 backwardFetch=a@30:3,a@10:1";
        final var findSingleKey = " findSessions=a@10:1,a@30:3 backwardFindSessions=a@30:3,a@10:1";
        final var afterRemove = " afterRemove=a@30:3";
        // The in-memory store orders by session end, then key; RocksDB orders by key, then session end
        assertThat(lines).containsExactly(
                "memory" + singleKey
                        + " fetchKeys=a@10:1,b@20:2,a@30:3 backwardFetchKeys=a@30:3,b@20:2,a@10:1"
                        + findSingleKey
                        + " findSessionsKeys=b@20:2,a@30:3 backwardFindSessionsKeys=a@30:3,b@20:2"
                        + afterRemove,
                "rocksdb" + singleKey
                        + " fetchKeys=a@10:1,a@30:3,b@20:2 backwardFetchKeys=b@20:2,a@30:3,a@10:1"
                        + findSingleKey
                        + " findSessionsKeys=a@30:3,b@20:2 backwardFindSessionsKeys=b@20:2,a@30:3"
                        + afterRemove);

        assertThat(openIterators(driver, "session_memory")).isZero();
        assertThat(openIterators(driver, "session_rocksdb")).isZero();
    }
}
