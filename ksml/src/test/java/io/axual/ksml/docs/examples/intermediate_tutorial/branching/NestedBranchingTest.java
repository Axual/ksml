package io.axual.ksml.docs.examples.intermediate_tutorial.branching;

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
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
class NestedBranchingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KSMLTopic(topic = "product_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "expensive_electronics")
    TestOutputTopic<String, String> expensiveElectronics;

    @KSMLTopic(topic = "affordable_electronics")
    TestOutputTopic<String, String> affordableElectronics;

    @KSMLTopic(topic = "clothing_items")
    TestOutputTopic<String, String> clothingItems;

    @KSMLTest(topology = "docs-examples/intermediate-tutorial/branching/processor-nested-branching.yaml")
    @DisplayName("Records are routed by category, and electronics again by price")
    void nestedBranchRoutesRecords() {
        input.pipeInput("tv", "{\"category\": \"electronics\", \"price\": 900}");
        input.pipeInput("radio", "{\"category\": \"electronics\", \"price\": 50}");
        input.pipeInput("shirt", "{\"category\": \"clothing\", \"price\": 20}");
        input.pipeInput("apple", "{\"category\": \"food\", \"price\": 1}");

        // the inner branch gets the records after the "via" of the outer branch entry
        assertThat(expensiveElectronics.readKeyValuesToMap()).containsOnlyKeys("tv")
                .hasEntrySatisfying("tv", value -> assertThat(isEnriched(value)).isTrue());
        assertThat(affordableElectronics.readKeyValuesToMap()).containsOnlyKeys("radio")
                .hasEntrySatisfying("radio", value -> assertThat(isEnriched(value)).isTrue());
        assertThat(clothingItems.readKeyValuesToMap()).containsOnlyKeys("shirt");
    }

    private boolean isEnriched(String value) {
        return objectMapper.readTree(value).path("enriched").asBoolean(false);
    }
}
