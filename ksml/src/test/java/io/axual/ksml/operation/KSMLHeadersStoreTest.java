package io.axual.ksml.operation;

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

import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With the headers store format, a store that Python writes to keeps the headers of the record that was processed.
 * Interactive queries can read it as a headers-aware store and as a plain key-value store (AXPD-12445).
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLHeadersStoreTest {

    @KSMLTopic(topic = "headers_store_input")
    TestInputTopic<String, String> input;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = "pipelines/test-headers-store-python.yaml", headersStoreFormat = true)
    @DisplayName("a Python write keeps the record headers, and the store can be queried as a plain key-value store")
    void pythonWriteKeepsRecordHeaders() {
        final var headers = new RecordHeaders().add("trace", "abc".getBytes(StandardCharsets.UTF_8));
        input.pipeInput(new TestRecord<>("k", "v", headers, Instant.ofEpochMilli(1000L)));

        final var stored = driver.<Object, Object>getTimestampedKeyValueStoreWithHeaders("headers_store").get("k");
        assertThat(stored.value()).isEqualTo("v");
        assertThat(stored.timestamp()).isEqualTo(1000L);
        assertThat(new String(stored.headers().lastHeader("trace").value(), StandardCharsets.UTF_8)).isEqualTo("abc");

        assertThat(driver.<Object, Object>getKeyValueStore("headers_store").get("k")).isEqualTo("v");
    }
}
