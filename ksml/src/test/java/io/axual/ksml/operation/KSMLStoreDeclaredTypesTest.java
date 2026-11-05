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
 * Writes and reads Python values on every kind of store with declared simple key and value types (AXPD-11068). Keys
 * and values are converted to the declared type, so a Python int works on a long store; a value that does not fit
 * raises TypeError or ValueError and writes nothing.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLStoreDeclaredTypesTest {

    @KSMLTopic(topic = "declared_types_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "declared_types_output")
    TestOutputTopic<String, String> output;

    @KSMLTest(topology = "pipelines/test-store-declared-types-python.yaml")
    @DisplayName("Python keys and values are converted to the declared simple types of every kind of store")
    void testStoreDeclaredTypes() {
        input.pipeInput("trigger", "go", 1000L);

        final var values = output.readValuesToList();
        assertThat(values).hasSize(1);
        final var longStore = " put=ok get=5 increment=ok after=6 large=ok/1099511627776 huge=ValueError"
                + " fraction=TypeError wholeFloat=TypeError str=TypeError bool=TypeError floatKey=TypeError"
                + " hugeKey=ValueError missing=None";
        final var putAll = " putAll=ok putAllRead=[6, 6, 7] putAllHuge=ValueError putAllFloat=TypeError"
                + " putAllMissing=None delete=ok/None";
        assertThat(List.of(values.get(0).split("\\n"))).containsExactly(
                "keyValue" + longStore + putAll,
                "keyValueRocksdb" + longStore + putAll,
                "timestamped" + longStore + putAll,
                "versioned" + longStore,
                "window" + longStore,
                "timestampedWindow" + longStore,
                "session" + longStore,
                "int put=ok/5 large=ValueError largeKey=ValueError missing=None",
                "double int=ok/5.0 float=ok/1.5 str=TypeError",
                "string put=ok/b intValue=TypeError intKey=TypeError dictValue=TypeError putAllInt=TypeError after=b",
                "float float=ok/1.5 int=ok/5.0 str=TypeError",
                "boolean bool=ok/True int=TypeError",
                "short put=ok/5 large=ValueError",
                // A list of ints is also bytes; reads return bytes as a list of ints
                "bytes bytes=ok/[97, 98] list=ok/[97, 98] str=TypeError",
                "byte put=ok/5 large=ValueError");
    }
}
