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
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static io.axual.ksml.testutil.StoreMetrics.openIterators;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Calls the key-value store methods from Python on in-memory, RocksDB and cached RocksDB stores, plain and with
 * timestamped: true. The results must be the same.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLKeyValueStoreMethodsTest {

    @KSMLTopic(topic = "keyvalue_methods_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "keyvalue_methods_output")
    TestOutputTopic<String, String> output;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = "pipelines/test-keyvalue-store-methods-python.yaml")
    @DisplayName("Key-value store methods work from Python on in-memory, RocksDB and cached stores, plain and timestamped")
    void testKeyValueStoreMethods() {
        input.pipeInput("trigger", "go");

        final var values = output.readValuesToList();
        assertThat(values).hasSize(1);
        final var expected = " range=a:1,ab:4,b:2 reverseRange=b:2,ab:4,a:1 reverseAll=c:3,b:2,ab:4,a:1"
                + " prefixScan=a:1,ab:4 prefixScanEmpty=a:1,ab:4,b:2,c:3"
                + " rangeFromNone=a:1,ab:4 rangeToNone=b:2,c:3 rangeAll=a:1,ab:4,b:2,c:3"
                + " reverseRangeFromNone=ab:4,a:1 reverseRangeToNone=c:3,b:2"
                + " afterDelete=a:1,ab:4,b:2 badEntry=TypeError x=None copy=a:1,ab:4,b:2"
                + " get=5 putIfAbsent=5 delete=5 afterPutDelete=a:1,ab:4,b:2";
        assertThat(List.of(values.get(0).split("\\n"))).containsExactly(
                "memory" + expected, "rocksdb" + expected, "cached" + expected,
                "memory_ts" + expected, "rocksdb_ts" + expected, "cached_ts" + expected);
        for (final var store : List.of("kv_memory", "kv_memory_copy", "kv_rocksdb", "kv_rocksdb_copy",
                "kv_cached", "kv_cached_copy", "ts_memory", "ts_memory_copy", "ts_rocksdb", "ts_rocksdb_copy",
                "ts_cached", "ts_cached_copy")) {
            assertThat(openIterators(driver, store)).as(store).isZero();
        }
    }

    @KSMLTest(topology = "pipelines/test-keyvalue-store-methods-python.yaml", headersStoreFormat = true)
    @DisplayName("Key-value store methods work from Python on in-memory, RocksDB and cached stores, plain and timestamped, also with the headers store format")
    void testKeyValueStoreMethodsWithHeadersStoreFormat() {
        testKeyValueStoreMethods();
        assertThat(driver.getAllStateStores().values()).anyMatch(store -> store instanceof TimestampedKeyValueStoreWithHeaders<?, ?>);
    }
}
