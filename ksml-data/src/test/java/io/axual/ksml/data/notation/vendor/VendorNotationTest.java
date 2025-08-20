package io.axual.ksml.data.notation.vendor;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.mapper.StringDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VendorNotationTest {
    private static class ConcreteVendorNotation extends VendorNotation {
        ConcreteVendorNotation(VendorNotationContext context,
                               String filenameExtension,
                               DataType defaultType,
                               Notation.Converter converter,
                               Notation.SchemaParser schemaParser) {
            super(context, filenameExtension, defaultType, converter, schemaParser);
        }
    }

    private static VendorNotationContext createContext() {
        var base = new NotationContext("avro", "vendorY", new NativeDataObjectMapper(), new java.util.HashMap<>());
        @SuppressWarnings("unchecked")
        var serdeMapper = (DataObjectMapper<Object>) (DataObjectMapper<?>) new StringDataObjectMapper();
        var supplier = new VendorSerdeSupplier() {
            @Override
            public String vendorName() { return "vendorY"; }
            @Override
            public Serde<Object> get(DataType type, boolean isKey) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                Serde<Object> raw = (Serde) Serdes.String();
                return raw;
            }
        };
        return new VendorNotationContext(base, supplier, serdeMapper);
    }

    @Test
    @DisplayName("serde() returns DataObjectSerde that roundtrips DataString via vendor String serde when type is supported")
    void serdeRoundTripsForSupportedType() {
        var context = createContext();
        var notation = new ConcreteVendorNotation(context, ".avsc", DataString.DATATYPE, (v, t) -> v, (c, n, s) -> null);

        var serde = notation.serde(DataString.DATATYPE, false);
        var bytes = serde.serializer().serialize("t", new DataString("abc"));
        var result = serde.deserializer().deserialize("t", bytes);
        assertThat(result).isInstanceOf(DataString.class);
        assertThat(((DataString) result).value()).isEqualTo("abc");
        assertThat(notation.name()).isEqualTo("vendorY_avro");
    }

    @Test
    @DisplayName("serde() throws DataException with helpful message when requested type not assignable from default type")
    void serdeThrowsForUnsupportedType() {
        var context = createContext();
        var notation = new ConcreteVendorNotation(context, ".avsc", DataString.DATATYPE, (v, t) -> v, (c, n, s) -> null);
        var wrongType = new SimpleType(Integer.class, "int");

        assertThatThrownBy(() -> notation.serde(wrongType, true))
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class)
                .hasMessageEndingWith("vendorY_avro serde not available for data type: " + wrongType);
    }
}
