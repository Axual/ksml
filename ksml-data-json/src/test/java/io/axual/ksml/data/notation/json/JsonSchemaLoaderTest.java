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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static io.axual.ksml.data.schema.DataSchema.ANY_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.BOOLEAN_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.DOUBLE_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.LONG_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.NULL_SCHEMA;
import static io.axual.ksml.data.schema.DataSchema.STRING_SCHEMA;
import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link JsonSchemaLoader}, validating that it parses JSON Schema strings to
 * KSML {@link DataSchema} objects and propagates parse errors.
 *
 * <p>Assertions follow the AssertJ chained style and make use of SoftAssertions, in line
 * with JsonSchemaMapperTest.</p>
 */
@DisplayName("JsonSchemaLoader - parsing JSON Schema into DataSchema")
class JsonSchemaLoaderTest {
    private final JsonSchemaLoader loader = new JsonSchemaLoader();

    @Test
    @DisplayName("Parses object_with_primitives.json to StructSchema with expected fields")
    void parsesPrimitivesObject() throws Exception {
        // Load JSON Schema resource used elsewhere in tests
        var json = readResource("/jsonschema/object_with_primitives.json");

        // When
        var schema = loader.parse("ns", "ObjWithPrims", json);

        // Then: verify StructSchema name and key properties using chained assertions
        var struct = assertThat(schema)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(StructSchema.class))
                .returns("ObjWithPrims", StructSchema::name)
                .returns(true, StructSchema::additionalFieldsAllowed)
                .returns(ANY_SCHEMA, StructSchema::additionalFieldsSchema)
                .actual();

        // Validate fields set using soft assertions
        var softly = new SoftAssertions();
        softly.assertThat(struct.fields())
                .hasSize(5)
                .containsExactlyInAnyOrder(
                        new StructSchema.Field("aString", STRING_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("aBoolean", BOOLEAN_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("anInteger", LONG_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("aNumber", DOUBLE_SCHEMA, null, NO_TAG, false),
                        new StructSchema.Field("aNull", NULL_SCHEMA, null, NO_TAG, false)
                );
        softly.assertAll();
    }

    @Test
    @DisplayName("Invalid JSON Schema string throws DataException")
    void invalidSchemaThrows() {
        assertThatThrownBy(() -> loader.parse("ns", "Broken", "{not valid json"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Could not parse");
    }

    // Helper to read test resources as String (UTF-8)
    private static String readResource(String resourcePath) throws Exception {
        try (var in = JsonSchemaLoaderTest.class.getResourceAsStream(resourcePath)) {
            if (in == null)
                throw new IllegalArgumentException("Resource not found: " + resourcePath);
            try (var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }
}
