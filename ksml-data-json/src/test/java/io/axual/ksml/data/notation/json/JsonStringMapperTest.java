package io.axual.ksml.data.notation.json;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axual.ksml.data.exception.DataException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link JsonStringMapper} ensuring correct String <-> native mapping,
 * null handling, pretty printing, and error propagation.
 */
@DisplayName("JsonStringMapper - String <-> native mapping and formatting")
class JsonStringMapperTest {
    private static final ObjectMapper JACKSON = new ObjectMapper();

    @Test
    @DisplayName("fromString(null) returns null; toString(null) returns null")
    void nullHandling() {
        var mapper = new JsonStringMapper(false);
        assertThat(mapper.fromString(null)).isNull();
        assertThat(mapper.toString(null)).isNull();
    }

    @Test
    @DisplayName("Invalid JSON throws DataException")
    void invalidJsonThrows() {
        var mapper = new JsonStringMapper(false);
        assertThatThrownBy(() -> mapper.fromString("{invalid"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Could not parse string to object");
    }

    @Test
    @DisplayName("Round-trip JSON object")
    void roundTripObject() throws Exception {
        var mapper = new JsonStringMapper(false);
        var json = "{\"a\":1,\"b\":\"x\",\"c\":{\"d\":true}}";

        var nativeObj = mapper.fromString(json);
        assertThat(nativeObj).isInstanceOf(Map.class);

        var out = mapper.toString(nativeObj);
        var inTree = JACKSON.readTree(json);
        var outTree = JACKSON.readTree(out);
        assertThat(outTree).isEqualTo(inTree);
    }

    @Test
    @DisplayName("Round-trip JSON array")
    void roundTripArray() throws Exception {
        var mapper = new JsonStringMapper(false);
        var json = "[1,\"x\",false,{\"k\":\"v\"}]";

        var nativeArr = mapper.fromString(json);
        assertThat(nativeArr).isInstanceOf(List.class);

        var out = mapper.toString(nativeArr);
        var inTree = JACKSON.readTree(json);
        var outTree = JACKSON.readTree(out);
        assertThat(outTree).isEqualTo(inTree);
    }

    @Test
    @DisplayName("Pretty print produces formatted JSON with equal tree")
    void prettyPrintFormatting() throws Exception {
        var pretty = new JsonStringMapper(true);

        // Build a nested native object
        final var nestedMap = new HashMap<String, Object>();
        nestedMap.put("x", true);
        nestedMap.put("y", null);
        var nativeObj = Map.of(
                "a", 1,
                "b", List.of(1, 2, 3),
                "c", nestedMap
        );

        var prettyJson = pretty.toString(nativeObj);

        // Ensure it's non-blank and contains line breaks or indentation characters
        var actualString = assertThat(prettyJson)
                .isNotBlank()
                .asInstanceOf(InstanceOfAssertFactories.STRING)
                .actual();

        var softly = new SoftAssertions();
        softly.assertThat(actualString.contains("\n") || actualString.contains("  "))
                .as("pretty JSON should be formatted").isTrue();

        // Trees must be equal regardless of formatting
        var compact = new JsonStringMapper(false);
        var compactJson = compact.toString(nativeObj);
        var prettyTree = JACKSON.readTree(prettyJson);
        var compactTree = JACKSON.readTree(compactJson);
        softly.assertThat(prettyTree).isEqualTo(compactTree);
        softly.assertAll();
    }
}
