package io.axual.ksml.runner.config.internal;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link KsmlFileOrDefinitionSerializer}, verifying that the sealed union is serialized as a
 * JSON string for a file path and as a JSON object for an inline definition.
 */
class KsmlFileOrDefinitionSerializerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("A file-path definition is serialized as a plain JSON string")
    void serializesFilePathAsString() throws Exception {
        final KsmlFileOrDefinition value = new KsmlFilePath("definitions/my-app.yaml");

        final var json = mapper.writerFor(KsmlFileOrDefinition.class).writeValueAsString(value);

        assertThat(json).isEqualTo("\"definitions/my-app.yaml\"");
    }

    @Test
    @DisplayName("An inline definition is serialized as the embedded JSON object")
    void serializesInlineDefinitionAsObject() throws Exception {
        final var node = mapper.createObjectNode();
        node.put("name", "my-pipeline");
        final KsmlFileOrDefinition value = new KsmlInlineDefinition(node);

        final var json = mapper.writerFor(KsmlFileOrDefinition.class).writeValueAsString(value);

        // The object is written verbatim, so it round-trips back to the same tree.
        assertThat(mapper.readTree(json)).isEqualTo(node);
    }

    @Test
    @DisplayName("A string deserializes to a file path and an object to an inline definition")
    void deserializesBothVariants() throws Exception {
        assertThat(mapper.readValue("\"my/app.yaml\"", KsmlFileOrDefinition.class))
                .isInstanceOf(KsmlFilePath.class);
        assertThat(mapper.readValue("{\"name\":\"x\"}", KsmlFileOrDefinition.class))
                .isInstanceOf(KsmlInlineDefinition.class);
    }

    @Test
    @DisplayName("A JSON kind that is neither a string nor an object is rejected")
    void deserializeRejectsUnsupportedKind() {
        // A JSON array is neither a file path (string) nor an inline definition (object).
        assertThatThrownBy(() -> mapper.readValue("[1, 2, 3]", KsmlFileOrDefinition.class))
                .isInstanceOf(JsonMappingException.class);
    }
}
