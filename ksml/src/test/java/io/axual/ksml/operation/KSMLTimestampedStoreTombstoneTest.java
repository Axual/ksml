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

/** How a None value and a tombstone show in the *WithTimestamp methods of timestamped key-value stores. */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLTimestampedStoreTombstoneTest {

    @KSMLTopic(topic = "put_none_input")
    TestInputTopic<String, String> putNoneInput;

    @KSMLTopic(topic = "put_none_output")
    TestOutputTopic<String, String> putNoneOutput;

    @KSMLTopic(topic = "table_json_input")
    TestInputTopic<String, String> tableInput;

    @KSMLTopic(topic = "table_trigger")
    TestInputTopic<String, String> tableTrigger;

    @KSMLTopic(topic = "table_store_output")
    TestOutputTopic<String, String> tableOutput;

    @KSMLTest(topology = "pipelines/test-timestamped-store-put-none.yaml")
    @DisplayName("put(key, None, timestamp) deletes the key, so the *WithTimestamp methods do not see it")
    void putNoneWithTimestampDeletes() {
        putNoneInput.pipeInput("trigger", "go", 1000L);

        final var expected = " getWithTimestamp=None get=None putIfAbsentMissing=None c=None putIfAbsentExisting=%1$s"
                + " b=b:%1$s@600 allWithTimestamp=b:%1$s@600 rangeWithTimestamp=b:%1$s@600 prefixScanWithTimestamp=b:%1$s@600";
        final var string = expected.formatted("2");
        final var json = expected.formatted("{'v': 2}");
        assertThat(List.of(putNoneOutput.readValue().split("\\n"))).containsExactly(
                "string_memory" + string, "string_rocksdb" + string, "json_memory" + json, "json_rocksdb" + json);
    }

    @KSMLTest(topology = "pipelines/test-table-store-tombstone-empty-value.yaml")
    @DisplayName("A tombstone deletes the key from the table store, an empty value stays a value")
    void tableStoreTombstoneAndEmptyValue() {
        tableInput.pipeInput("value", "{\"v\":1}", 100L);
        tableInput.pipeInput("empty", "{\"v\":2}", 200L);
        tableInput.pipeInput("empty", "", 300L);
        tableInput.pipeInput("deleted", "{\"v\":3}", 400L);
        tableInput.pipeInput("deleted", (String) null, 500L);
        tableInput.pipeInput("emptyThenValue", "", 600L);
        tableInput.pipeInput("emptyThenValue", "{\"v\":4}", 700L);
        tableTrigger.pipeInput("trigger", "go", 1000L);

        // An empty value stays in the store as None with its timestamp; a tombstone removes the key
        assertThat(tableOutput.readValue()).isEqualTo(
                "value={'timestamp': 100, 'value': {'v': 1}} empty={'timestamp': 300, 'value': None} deleted=None"
                        + " emptyThenValue={'timestamp': 700, 'value': {'v': 4}}"
                        + " allWithTimestamp=empty:None@300,emptyThenValue:{'v': 4}@700,value:{'v': 1}@100"
                        + " all=empty:None,emptyThenValue:{'v': 4},value:{'v': 1}");
    }
}
