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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for developerdocs/bugs-and-dead-code-candidates.md finding 6:
 * {@code JoinTargetDefinitionParser} used to reject inline {@code table:}/{@code globalTable:}
 * join targets with an "Expected type string, found object" parse error, even though the
 * language spec documents inline definitions as supported for both fields (matching the
 * behaviour that already worked for inline {@code stream:} join targets).
 */
@ExtendWith(KSMLTestExtension.class)
public class KSMLJoinInlineTargetTest {

    @KSMLTopic(topic = "streamIn")
    TestInputTopic<String, String> streamIn;

    @KSMLTopic(topic = "joinTargetIn")
    TestInputTopic<String, String> joinTargetIn;

    @KSMLTopic(topic = "joinOut")
    TestOutputTopic<String, String> joinOut;

    @KSMLTest(topology = "pipelines/test-join-inline-table.yaml")
    void testJoinWithInlineTable() {
        // Given a row in the (inline-defined) table
        joinTargetIn.pipeInput("key1", "tableValue");

        // When a matching stream record arrives
        streamIn.pipeInput("key1", "streamValue");

        // Then the join result combines both values
        assertThat(joinOut.getQueueSize()).isEqualTo(1);
        assertThat(joinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("key1", "streamValue-tableValue"));
    }

    @KSMLTest(topology = "pipelines/test-join-inline-globaltable.yaml")
    void testJoinWithInlineGlobalTable() {
        // Given a row in the (inline-defined) globalTable
        joinTargetIn.pipeInput("key1", "globalTableValue");

        // When a matching stream record arrives
        streamIn.pipeInput("key1", "streamValue");

        // Then the join result combines both values
        assertThat(joinOut.getQueueSize()).isEqualTo(1);
        assertThat(joinOut.readKeyValuesToList()).containsExactly(
                new KeyValue<>("key1", "streamValue-globalTableValue"));
    }
}
