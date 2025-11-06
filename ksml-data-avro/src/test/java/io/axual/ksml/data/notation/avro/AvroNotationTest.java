package io.axual.ksml.data.notation.avro;

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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorSerdeSupplier;
import io.axual.ksml.data.serde.DataObjectSerde;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroNotationTest {

    private static VendorNotationContext createContext(String vendorName) {
        var base = new NotationContext(AvroNotation.NOTATION_NAME, vendorName, new HashMap<>());
        // We don't exercise (de)serialization in these tests, so any DataObjectMapper will do.
        var serdeMapper = new NativeDataObjectMapper();
        var supplier = new VendorSerdeSupplier() {
            @Override
            public String vendorName() {
                return vendorName;
            }

            @Override
            @SuppressWarnings("unchecked")
            public Serde<Object> get(DataType type, boolean isKey) {
                @SuppressWarnings({"rawtypes"}) final var rawSerde = (Serde) Serdes.ByteArray();
                return rawSerde;
            }
        };
        return new VendorNotationContext(base, supplier, serdeMapper);
    }

    @Test
    @DisplayName("AvroNotation wires defaults: name, extension, default type, parser, null converter")
    void avroNotation_defaults_areWired() {
        var context = createContext("vendorX");

        final var contextAssert = assertThat(new AvroNotation(context))
                // Name comes from context (vendor_notation)
                .returns("vendorX_" + AvroNotation.NOTATION_NAME, AvroNotation::name)
                // File extension
                .returns(".avsc", AvroNotation::filenameExtension)
                // Default type
                .returns(AvroNotation.DEFAULT_TYPE, AvroNotation::defaultType);

        // Schema parser and converter
        contextAssert
                .extracting(AvroNotation::schemaParser)
                .isInstanceOf(AvroSchemaParser.class);
        contextAssert
                .extracting(BaseNotation::converter)
                .isNull();
    }

    @ParameterizedTest
    @DisplayName("serde() returns DataObjectSerde for StructType and throws for unsupported type")
    @ValueSource(booleans = {true, false})
    void serde_behavior_supportedAndUnsupportedTypes(boolean forKey) {
        var context = createContext("vendorY");
        var notation = new AvroNotation(context);

        // Supported: StructType
        assertThat(notation.serde(new StructType(), forKey))
                .as("Serde for StructType should be created")
                .isNotNull()
                .isInstanceOf(DataObjectSerde.class);

        // Unsupported: a simple type not assignable from StructType
        var wrongType = new SimpleType(Integer.class, "int");
        assertThatThrownBy(() -> notation.serde(wrongType, forKey))
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class)
                .hasMessageEndingWith(notation.name() + " serde not available for data type: " + wrongType)
                .hasMessageContaining(notation.name());
    }

    @Test
    @DisplayName("When no vendor specified, name() equals 'avro'")
    void name_withoutVendor_isJustNotation() {
        var context = createContext(null);
        var notation = new AvroNotation(context);
        assertThat(notation.name()).isEqualTo(AvroNotation.NOTATION_NAME);
    }
}
