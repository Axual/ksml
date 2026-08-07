package io.axual.ksml.generator;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Guards the duplicate-key rule for KSML definition files. KSML 1.x accepted a repeated key and kept
 * the last value, which could hide a copy-and-paste mistake in a pipeline. From 2.0.0 it is an error,
 * so this test pins that behaviour and the fact that the message points at the offending key.
 */
class YAMLObjectMapperTest {

    @Test
    @DisplayName("A definition without duplicate keys parses normally")
    void parsesDefinitionWithoutDuplicates() {
        final var yaml = """
                streams:
                  input:
                    topic: input_topic
                    keyType: string
                    valueType: json
                """;

        final var node = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);

        assertThat(node.at("/streams/input/topic").asString()).isEqualTo("input_topic");
    }

    @Test
    @DisplayName("A repeated key is rejected instead of the last one silently winning")
    void rejectsDuplicateKey() {
        final var yaml = """
                streams:
                  input:
                    topic: first_topic
                    topic: second_topic
                """;

        assertThatThrownBy(() -> YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class))
                .isInstanceOf(JacksonException.class)
                .hasMessageContaining("topic");
    }

    @Test
    @DisplayName("A repeated key is rejected at the top level too")
    void rejectsDuplicateTopLevelKey() {
        final var yaml = """
                functions:
                  one:
                    type: predicate
                functions:
                  two:
                    type: predicate
                """;

        assertThatThrownBy(() -> YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class))
                .isInstanceOf(JacksonException.class)
                .hasMessageContaining("functions");
    }
}
