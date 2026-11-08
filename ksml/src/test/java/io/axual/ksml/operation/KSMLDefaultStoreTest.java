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
 * A store declared without options is now persistent, timestamped and logged (AXPD-12445). Python get() and all() on
 * it give the same results as on a store with the options that a store without options got before.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLDefaultStoreTest {

    @KSMLTopic(topic = "default_store_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "default_store_output")
    TestOutputTopic<String, String> output;

    @KSMLTest(topology = "pipelines/test-default-store-python.yaml")
    @DisplayName("get() and all() on a store without options give the same results as before the new defaults")
    void defaultStoreKeepsPythonBehavior() {
        input.pipeInput("trigger", "go", 1000L);

        final var lines = List.of(output.readValuesToList().getFirst().split("\\n"));
        final var expected = " get={'count': 1} missing=None all={'key': 'a', 'value': {'count': 1}};{'key': 'b', 'value': {'count': 2}}";
        assertThat(lines).containsExactly("default" + expected, "old_default" + expected);
    }

    @KSMLTest(topology = "pipelines/test-default-store-python.yaml", headersStoreFormat = true)
    @DisplayName("get() and all() on a store without options give the same results, also with the headers store format")
    void defaultStoreKeepsPythonBehaviorWithHeadersStoreFormat() {
        defaultStoreKeepsPythonBehavior();
    }
}
