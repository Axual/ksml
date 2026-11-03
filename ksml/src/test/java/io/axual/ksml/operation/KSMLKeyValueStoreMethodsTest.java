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
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import io.axual.ksml.testutil.KSMLTopologyTest;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Calls the key-value store methods from Python, on an in-memory and a RocksDB store (AXPD-11068). The test runs once
 * on plain stores and once on the same stores with timestamped: true, the default after AXPD-12445: the results must
 * be the same.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLKeyValueStoreMethodsTest {

    @KSMLTopic(topic = "keyvalue_methods_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "keyvalue_methods_output")
    TestOutputTopic<String, String> output;

    @KSMLTopologyTest(topologies = {"pipelines/test-keyvalue-store-methods-python.yaml",
            "pipelines/test-keyvalue-store-methods-default-timestamped-python.yaml"})
    @DisplayName("Key-value store methods work from Python on in-memory and RocksDB stores, plain and timestamped")
    void testKeyValueStoreMethods() {
        input.pipeInput("trigger", "go");

        final var values = output.readValuesToList();
        assertThat(values).hasSize(1);
        final var expected = " range=a:1,ab:4,b:2 reverseRange=b:2,ab:4,a:1 reverseAll=c:3,b:2,ab:4,a:1"
                + " prefixScan=a:1,ab:4 afterDelete=a:1,ab:4,b:2 badEntry=TypeError x=None copy=a:1,ab:4,b:2"
                + " get=5 putIfAbsent=5 delete=5 afterPutDelete=a:1,ab:4,b:2";
        assertThat(List.of(values.get(0).split("\\n"))).containsExactly("memory" + expected, "rocksdb" + expected);
    }
}
