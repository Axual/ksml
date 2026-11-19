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

import io.axual.ksml.testutil.KSMLTopic;
import io.axual.ksml.testutil.KSMLTopologyTest;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A branch entry can contain its own {@code branch}, to split records again inside a branch.
 */
class KSMLNestedBranchingTest {

    @KSMLTopic(topic = "input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "expensive_electronics")
    TestOutputTopic<String, String> expensiveElectronics;

    @KSMLTopic(topic = "affordable_electronics")
    TestOutputTopic<String, String> affordableElectronics;

    @KSMLTopic(topic = "clothing_items")
    TestOutputTopic<String, String> clothingItems;

    @KSMLTopic(topic = "level1_out")
    TestOutputTopic<String, String> level1Out;

    @KSMLTopic(topic = "level2_out")
    TestOutputTopic<String, String> level2Out;

    @KSMLTopic(topic = "level3_yes")
    TestOutputTopic<String, String> level3Yes;

    @KSMLTopic(topic = "level3_no")
    TestOutputTopic<String, String> level3No;

    @KSMLTopologyTest(topologies = "pipelines/test-branching-nested.yaml")
    @DisplayName("The nested branching example from the branching tutorial routes records by category and price")
    void nestedBranchRoutesRecords() {
        input.pipeInput("tv", "{\"category\": \"electronics\", \"price\": 900}");
        input.pipeInput("radio", "{\"category\": \"electronics\", \"price\": 50}");
        input.pipeInput("shirt", "{\"category\": \"clothing\", \"price\": 20}");
        input.pipeInput("apple", "{\"category\": \"food\", \"price\": 1}");

        // the inner branch gets the records after the "via" of the outer branch entry
        assertThat(expensiveElectronics.readKeyValuesToMap()).containsOnlyKeys("tv")
                .hasEntrySatisfying("tv", value -> assertThat(value).contains("\"enriched\":true"));
        assertThat(affordableElectronics.readKeyValuesToMap()).containsOnlyKeys("radio")
                .hasEntrySatisfying("radio", value -> assertThat(value).contains("\"enriched\":true"));
        assertThat(clothingItems.readKeyValuesToMap()).containsOnlyKeys("shirt");
    }

    @KSMLTopologyTest(topologies = "pipelines/test-branching-nested-three-levels.yaml")
    @DisplayName("Branches can be nested three levels deep")
    void threeLevelsOfNesting() {
        input.pipeInput("abc", "{\"a\": true, \"b\": true, \"c\": true}");
        input.pipeInput("ab", "{\"a\": true, \"b\": true, \"c\": false}");
        input.pipeInput("a", "{\"a\": true, \"b\": false, \"c\": false}");
        input.pipeInput("none", "{\"a\": false, \"b\": false, \"c\": false}");

        assertThat(level3Yes.readKeyValuesToMap()).containsOnlyKeys("abc");
        assertThat(level3No.readKeyValuesToMap()).containsOnlyKeys("ab");
        assertThat(level2Out.readKeyValuesToMap()).containsOnlyKeys("a");
        assertThat(level1Out.readKeyValuesToMap()).containsOnlyKeys("none");
    }
}
