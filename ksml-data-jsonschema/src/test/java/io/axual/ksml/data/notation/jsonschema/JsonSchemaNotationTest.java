package io.axual.ksml.data.notation.jsonschema;

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
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonSchemaLoader;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorSerdeSupplier;
import io.axual.ksml.data.serde.DataObjectSerde;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link JsonSchemaNotation} ensuring defaults, wiring, and serde selection
 * behave as expected. Uses AssertJ chained assertions and SoftAssertions style.
 */
@DisplayName("JsonSchemaNotation - defaults, wiring, and serde selection")
class JsonSchemaNotationTest {

    private static VendorNotationContext createContext(String vendor) {
        var base = new NotationContext(JsonSchemaNotation.NOTATION_NAME, vendor, new NativeDataObjectMapper(), new java.util.HashMap<>());
        @SuppressWarnings("unchecked")
        var serdeMapper = (DataObjectMapper<Object>) new JsonSchemaDataObjectMapper(new NativeDataObjectMapper());
        var supplier = new VendorSerdeSupplier() {
            @Override
            public String vendorName() { return vendor == null ? "" : vendor; }
            @Override
            public Serde<Object> get(DataType type, boolean isKey) {
                // Return a simple String serde; we won't actually serialize in this test
                @SuppressWarnings({"rawtypes", "unchecked"})
                Serde<Object> raw = (Serde) Serdes.String();
                return raw;
            }
        };
        return new VendorNotationContext(base, supplier, serdeMapper);
    }

    @Test
    @DisplayName("Default properties and wiring: name, extension, default type, converter, parser")
    void basicPropertiesAndWiring() {
        var context = createContext(null);
        var notation = new JsonSchemaNotation(context);

        assertThat(notation)
                .returns(JsonSchemaNotation.NOTATION_NAME, BaseNotation::name)
                .returns(".json", BaseNotation::filenameExtension)
                .returns(JsonSchemaNotation.DEFAULT_TYPE, BaseNotation::defaultType)
                .extracting(BaseNotation::converter, InstanceOfAssertFactories.type(JsonDataObjectConverter.class))
                .isNotNull();

        assertThat(notation.schemaParser()).isInstanceOf(JsonSchemaLoader.class);
    }

    @Test
    @DisplayName("Name includes vendor prefix when provided by context")
    void nameWithVendor() {
        var context = createContext("vendorZ");
        var notation = new JsonSchemaNotation(context);
        assertThat(notation.name()).isEqualTo("vendorZ_" + JsonSchemaNotation.NOTATION_NAME);
    }

    @Test
    @DisplayName("Serde is provided for StructType and ListType; rejects unsupported MapType")
    void serdeSelection() {
        var notation = new JsonSchemaNotation(createContext("vendorY"));

        var softly = new SoftAssertions();
        softly.assertThat(notation.serde(new StructType(), false)).isInstanceOf(DataObjectSerde.class);
        softly.assertThat(notation.serde(new ListType(), true)).isInstanceOf(DataObjectSerde.class);
        softly.assertAll();

        final var wantedType = new MapType();
        assertThatThrownBy(() -> notation.serde(wantedType, true))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("serde not available for data type");
    }
}
