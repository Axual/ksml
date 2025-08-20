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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.notation.vendor.VendorSerdeSupplier;
import io.axual.ksml.data.serde.DataObjectSerde;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.StructType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroNotationTest {

    private static VendorNotationContext createContext(String vendorName) {
        var base = new NotationContext(AvroNotation.NOTATION_NAME, vendorName, new NativeDataObjectMapper(), new HashMap<>());
        // We don't exercise (de)serialization in these tests, so any DataObjectMapper will do.
        @SuppressWarnings("unchecked")
        var serdeMapper = (DataObjectMapper<Object>) (DataObjectMapper<?>) new NativeDataObjectMapper();
        var supplier = new VendorSerdeSupplier() {
            @Override
            public String vendorName() { return vendorName; }
            @Override
            public Serde<Object> get(DataType type, boolean isKey) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                Serde<Object> raw = (Serde) Serdes.ByteArray();
                return raw;
            }
        };
        return new VendorNotationContext(base, supplier, serdeMapper);
    }

    @Test
    @DisplayName("AvroNotation wires defaults: name, extension, default type, parser, null converter")
    void avroNotation_defaults_areWired() {
        var context = createContext("vendorX");
        var notation = new AvroNotation(context);

        // Name comes from context (vendor_notation)
        assertThat(notation.name()).isEqualTo("vendorX_" + AvroNotation.NOTATION_NAME);
        // File extension
        assertThat(notation.filenameExtension()).isEqualTo(".avsc");
        // Default type
        assertThat(notation.defaultType()).isSameAs(AvroNotation.DEFAULT_TYPE);
        assertThat(notation.defaultType()).isInstanceOf(StructType.class);
        // Schema parser and converter
        assertThat(notation.schemaParser()).isInstanceOf(AvroSchemaParser.class);
        assertThat(notation.converter()).isNull();
    }

    @Test
    @DisplayName("serde() returns DataObjectSerde for StructType and throws for unsupported type")
    void serde_behavior_supportedAndUnsupportedTypes() {
        var context = createContext("vendorY");
        var notation = new AvroNotation(context);

        // Supported: StructType
        assertThatCode(() -> notation.serde(new StructType(), false))
                .doesNotThrowAnyException();
        assertThat(notation.serde(new StructType(), true))
                .as("Serde for StructType should be created")
                .isNotNull()
                .isInstanceOf(DataObjectSerde.class);

        // Unsupported: a simple type not assignable from StructType
        var wrongType = new SimpleType(Integer.class, "int");
        assertThatThrownBy(() -> notation.serde(wrongType, true))
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
