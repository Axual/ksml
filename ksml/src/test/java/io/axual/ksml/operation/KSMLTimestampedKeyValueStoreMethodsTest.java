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
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Calls the key-value store methods and the *WithTimestamp methods from Python on an in-memory and a RocksDB
 * timestamped key-value store (AXPD-11068). Methods without a timestamp in their name return the same results as on a
 * plain key-value store.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLTimestampedKeyValueStoreMethodsTest {

    @KSMLTopic(topic = "timestamped_methods_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "timestamped_methods_output")
    TestOutputTopic<String, String> output;

    @KSMLTest(topology = "pipelines/test-timestamped-keyvalue-store-methods-python.yaml")
    @DisplayName("Timestamped key-value stores have the key-value API plus *WithTimestamp methods, on in-memory and RocksDB stores")
    void testTimestampedKeyValueStoreMethods() {
        // Writes without a timestamp use the timestamp of this record
        input.pipeInput("trigger", "go", 1000L);

        final var values = output.readValuesToList();
        assertThat(values).hasSize(1);
        final var expected = " get=1 getWithTimestamp=1000 missing=None"
                + " all=a:1,ab:4,b:2,c:3,d:5 range=a:1,ab:4,b:2 reverseRange=b:2,ab:4,a:1"
                + " reverseAll=d:5,c:3,b:2,ab:4,a:1 prefixScan=a:1,ab:4"
                + " allWithTimestamp=a:1@1000,ab:4@700,b:2@500,c:3@1000,d:5@1000"
                + " rangeWithTimestamp=a:1@1000,ab:4@700,b:2@500"
                + " reverseRangeWithTimestamp=b:2@500,ab:4@700,a:1@1000"
                + " reverseAllWithTimestamp=d:5@1000,c:3@1000,b:2@500,ab:4@700,a:1@1000"
                + " prefixScanWithTimestamp=a:1@1000,ab:4@700"
                + " putIfAbsent=1 delete=3 afterDelete=a:1,ab:4,b:2"
                + " badEntry=TypeError threeTuple=ValueError x=None"
                + " copy=a:1@1000,ab:4@700,b:2@500";
        assertThat(List.of(values.get(0).split("\\n"))).containsExactly("memory" + expected, "rocksdb" + expected);
    }
}
