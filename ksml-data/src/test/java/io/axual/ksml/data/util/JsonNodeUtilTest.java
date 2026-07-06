package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.axual.ksml.data.exception.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonNodeUtilTest {

    @Test
    @DisplayName("convertStringToList parses a JSON array, and returns null for non-arrays/invalid/null")
    void convertStringToList() {
        assertThat(JsonNodeUtil.convertStringToList("[1, 2, 3]")).containsExactly(1, 2, 3);
        assertThat(JsonNodeUtil.convertStringToList("{}")).isNull();      // not an array
        assertThat(JsonNodeUtil.convertStringToList("not-json")).isNull();// parse failure
        assertThat(JsonNodeUtil.convertStringToList(null)).isNull();      // null input
    }

    @Test
    @DisplayName("convertStringToMap parses a JSON object, and returns null for null input")
    void convertStringToMap() {
        assertThat(JsonNodeUtil.convertStringToMap("{\"a\": 1, \"b\": \"x\"}"))
                .containsEntry("a", 1)
                .containsEntry("b", "x");
        assertThat(JsonNodeUtil.convertStringToMap(null)).isNull();
    }

    @Test
    @DisplayName("convertStringToJsonNode yields a null node for null, a tree for valid JSON, and null for invalid")
    void convertStringToJsonNode() {
        assertThat(JsonNodeUtil.convertStringToJsonNode(null)).isNotNull().isInstanceOf(NullNode.class);
        assertThat(JsonNodeUtil.convertStringToJsonNode("[1,2]")).isNotNull();
        assertThat(JsonNodeUtil.convertStringToJsonNode("invalid{")).isNull();
    }

    @Test
    @DisplayName("convertNativeToJsonNode handles null, lists and maps, and rejects unsupported types")
    void convertNativeToJsonNode() {
        assertThat(JsonNodeUtil.convertNativeToJsonNode(null)).isInstanceOf(NullNode.class);
        assertThat(JsonNodeUtil.convertNativeToJsonNode(List.of(1, "x", true))).isInstanceOf(ArrayNode.class);
        assertThat(JsonNodeUtil.convertNativeToJsonNode(Map.of("k", "v"))).isInstanceOf(ObjectNode.class);
        assertThatThrownBy(() -> JsonNodeUtil.convertNativeToJsonNode("a string"))
                .isInstanceOf(DataException.class);
    }

    @Test
    @DisplayName("A native map with mixed value types round-trips through JsonNode")
    void mixedMapRoundTrips() {
        final var source = new LinkedHashMap<String, Object>();
        source.put("bool", true);
        source.put("int", 1);
        source.put("long", 2L);
        source.put("double", 3.5);
        source.put("string", "text");
        source.put("list", List.of(10, 20));
        source.put("nested", Map.of("inner", 9));

        final var node = JsonNodeUtil.convertNativeToJsonNode(source);
        final Object roundTripped = JsonNodeUtil.convertJsonNodeToNative(node);

        assertThat(roundTripped).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        final var result = (Map<String, Object>) roundTripped;
        assertThat(result)
                .containsEntry("bool", true)
                .containsEntry("int", 1)
                .containsEntry("string", "text")
                .containsEntry("list", List.of(10, 20));
    }

    @Test
    @DisplayName("convertJsonNodeToNative rejects a scalar node that is neither array nor object")
    void convertJsonNodeToNativeRejectsScalar() {
        final var scalar = JsonNodeUtil.convertStringToJsonNode("42");

        assertThatThrownBy(() -> JsonNodeUtil.convertJsonNodeToNative(scalar))
                .isInstanceOf(DataException.class);
    }
}
