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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link JsonNotation} ensuring it exposes correct defaults,
 * delegates to BaseNotation for naming/extension, and provides a JSON Serde
 * for supported data types.
 *
 * <p>Tests follow the AssertJ chained assertions and SoftAssertions style as
 * used in JsonSchemaMapperTest.</p>
 */
@DisplayName("JsonNotation - defaults, serde selection, and wiring")
class JsonNotationTest {

    @Test
    @DisplayName("Default properties, name/extension, converter and schema parser types")
    void basicProperties() {
        // Given a standard JSON notation context (no vendor)
        var context = new NotationContext(JsonNotation.NOTATION_NAME);

        // When constructing JsonNotation
        var notation = new JsonNotation(context);

        // Then: verify defaults and wiring using chained assertions
        assertThat(notation)
                .returns("json", BaseNotation::name)
                .returns(".json", BaseNotation::filenameExtension)
                .returns(JsonNotation.DEFAULT_TYPE, JsonNotation::defaultType)
                .extracting(BaseNotation::converter, InstanceOfAssertFactories.type(JsonDataObjectConverter.class))
                .isNotNull();

        // And: schema parser is JsonSchemaLoader
        assertThat(notation.schemaParser()).isInstanceOf(JsonSchemaLoader.class);
    }

    @Test
    @DisplayName("Name includes vendor prefix when provided by context")
    void nameWithVendor() {
        var context = new NotationContext(JsonNotation.NOTATION_NAME, "vendorX");
        var notation = new JsonNotation(context);
        assertThat(notation.name()).isEqualTo("vendorX_json");
    }

    @Test
    @DisplayName("Serde is provided for MapType, ListType, StructType, and DEFAULT_TYPE (Union)")
    void serdeForSupportedTypes() {
        var notation = new JsonNotation(new NotationContext(JsonNotation.NOTATION_NAME));

        // Use soft assertions to validate multiple serde creations
        var softly = new SoftAssertions();
        softly.assertThat(notation.serde(new MapType(), false)).isInstanceOf(JsonSerde.class);
        softly.assertThat(notation.serde(new ListType(), true)).isInstanceOf(JsonSerde.class);
        softly.assertThat(notation.serde(new StructType(), false)).isInstanceOf(JsonSerde.class);

        // DEFAULT_TYPE is a union of Struct|List and should be accepted directly
        var defaultUnion = JsonNotation.DEFAULT_TYPE;
        softly.assertThat(notation.serde(defaultUnion, false)).isInstanceOf(JsonSerde.class);

        // A different union that is assignable from DEFAULT_TYPE should also be supported
        var compatibleUnion = new UnionType(
                new UnionType.Member(new StructType()),
                new UnionType.Member(new ListType())
        );
        softly.assertThat(notation.serde(compatibleUnion, false)).isInstanceOf(JsonSerde.class);

        softly.assertAll();
    }

    @Test
    @DisplayName("Serde rejects unsupported types with DataException")
    void serdeForUnsupportedTypeThrows() {
        var notation = new JsonNotation(new NotationContext(JsonNotation.NOTATION_NAME));

        // DataString is not supported directly by JsonNotation serde selection
        assertThatThrownBy(() -> notation.serde(DataString.DATATYPE, true))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("JSON serde not found for data type:");
    }
}
