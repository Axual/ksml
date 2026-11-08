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
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.axual.ksml.testutil.StoreMetrics.openIterators;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Calls the window store methods and the *WithTimestamp methods from Python on the timestamped window store of a
 * windowed aggregate, for an in-memory and a RocksDB store. Methods without a timestamp in their name
 * return the same results as on a plain window store.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLTimestampedWindowStoreMethodsTest {

    @KSMLTopic(topic = "timestamped_window_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "timestamped_window_output")
    TestOutputTopic<String, String> output;

    @KSMLTopic(topic = "timestamped_window_avro_input", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> avroInput;

    @KSMLTopic(topic = "timestamped_window_avro_output")
    TestOutputTopic<String, String> avroOutput;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = "pipelines/test-timestamped-window-store-methods-python.yaml", modulesDirectory = "pipelines")
    @DisplayName("Timestamped window stores have the window store API plus *WithTimestamp methods, on in-memory and RocksDB stores")
    void testTimestampedWindowStoreMethods() {
        // All records fall in the window [0, 60000); the aggregate stores the latest record timestamp per window
        input.pipeInput("a", "x", 1000L);
        input.pipeInput("b", "x", 2000L);
        input.pipeInput("a", "x", 3000L);

        final var lines = output.readValuesToList().stream().filter(v -> !v.isEmpty()).toList();
        final var expected = " fetch=2 fetchWithTimestamp=3000 missing=None"
                + " fetchRange=0:2 backwardFetchRange=0:2"
                + " fetchKeys=a:2,b:1 backwardFetchKeys=b:1,a:2"
                + " all=a:2,b:1 backwardAll=b:1,a:2 fetchAll=a:2,b:1 backwardFetchAll=b:1,a:2"
                + " fetchRangeWithTimestamp=0:2@3000 backwardFetchRangeWithTimestamp=0:2@3000"
                + " fetchKeysWithTimestamp=a:2@3000,b:1@2000 backwardFetchKeysWithTimestamp=b:1@2000,a:2@3000"
                + " allWithTimestamp=a:2@3000,b:1@2000 backwardAllWithTimestamp=b:1@2000,a:2@3000"
                + " fetchAllWithTimestamp=a:2@3000,b:1@2000 backwardFetchAllWithTimestamp=b:1@2000,a:2@3000"
                + " put=c:9@3000,d:7@777";
        assertThat(lines).containsExactlyInAnyOrder("memory" + expected, "rocksdb" + expected);
        assertThat(openIterators(driver, "window_memory")).isZero();
        assertThat(openIterators(driver, "window_rocksdb")).isZero();
    }

    @KSMLTest(topology = "pipelines/test-timestamped-window-store-avro-put-python.yaml", schemaDirectory = "schemas")
    @DisplayName("put() writes an Avro value from Python to a timestamped window store")
    void testPutAvroValue() {
        avroInput.pipeInput("sensor1", SensorData.builder()
                .city("Amsterdam")
                .type(SensorData.SensorType.HUMIDITY)
                .unit("%")
                .value("80")
                .build().toRecord(), 3000L);

        // put without a timestamp uses the record timestamp (3000)
        assertThat(avroOutput.readValuesToList()).containsExactly("Amsterdam@3000 Amsterdam@777");
    }

    @KSMLTest(topology = "pipelines/test-timestamped-window-store-methods-python.yaml", modulesDirectory = "pipelines", headersStoreFormat = true)
    @DisplayName("Timestamped window stores have the window store API plus *WithTimestamp methods, on in-memory and RocksDB stores, also with the headers store format")
    void testTimestampedWindowStoreMethodsWithHeadersStoreFormat() {
        // Persistent stores are headers-aware here; Python gets the same results as without headers (AXPD-12445)
        testTimestampedWindowStoreMethods();
        assertThat(driver.getAllStateStores().values()).anyMatch(store -> store instanceof TimestampedWindowStoreWithHeaders<?, ?>);
    }

    @KSMLTest(topology = "pipelines/test-timestamped-window-store-avro-put-python.yaml", schemaDirectory = "schemas", headersStoreFormat = true)
    @DisplayName("put() writes an Avro value from Python to a timestamped window store, also with the headers store format")
    void testPutAvroValueWithHeadersStoreFormat() {
        // Persistent stores are headers-aware here; Python gets the same results as without headers (AXPD-12445)
        testPutAvroValue();
        assertThat(driver.getAllStateStores().values()).anyMatch(store -> store instanceof TimestampedWindowStoreWithHeaders<?, ?>);
    }
}
