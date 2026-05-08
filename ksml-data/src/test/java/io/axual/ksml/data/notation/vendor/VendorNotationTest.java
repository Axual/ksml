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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.StringDataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.serde.SerdeSupplier;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class VendorNotationTest {
    private static class ConcreteVendorNotation extends VendorNotation {
        ConcreteVendorNotation(VendorNotationContext context) {
            super(context);
        }

        @Override
        public String notationName() {
            return "avro";
        }

        @Override
        public String filenameExtension() {
            return ".avsc";
        }

        @Override
        public DataType defaultType() {
            return DataString.DATATYPE;
        }

        @Override
        public Converter converter() {
            return (v, t) -> v;
        }

        @Override
        public SchemaParser schemaParser() {
            return (c, n, s) -> null;
        }
    }

    private static VendorNotationContext createContext() {
        final var context = new NotationContext(new java.util.HashMap<>());
        @SuppressWarnings("unchecked") final var serdeMapper = (DataObjectMapper<Object>) (DataObjectMapper<?>) new StringDataObjectMapper();
        @SuppressWarnings({"rawtypes", "unchecked"}) final SerdeSupplier supplier = (type, isKey) -> (Serde) Serdes.String();
        return new VendorNotationContext("vendorY", context, supplier, serdeMapper);
    }

    @Test
    @DisplayName("serde() returns DataObjectSerde that round trips DataString via vendor String serde when type is supported")
    void serdeRoundTripsForSupportedType() {
        final var context = createContext();
        final var notation = new ConcreteVendorNotation(context);

        try (final var serde = notation.serde(DataString.DATATYPE, false)) {
            final var bytes = serde.serializer().serialize("t", new DataString("abc"));
            final var result = serde.deserializer().deserialize("t", bytes);
            assertThat(result).isInstanceOf(DataString.class);
            assertThat(((DataString) result).value()).isEqualTo("abc");
            assertThat(notation.name()).isEqualTo("vendorY_avro");
        }
    }

    @Test
    @DisplayName("serde() throws DataException with helpful message when requested type not assignable from default type")
    void serdeThrowsForUnsupportedType() {
        final var context = createContext();
        final var notation = new ConcreteVendorNotation(context);
        final var wrongType = new SimpleType(Integer.class, "int");

        assertThatThrownBy(() -> notation.serde(wrongType, true))
                .isInstanceOf(io.axual.ksml.data.exception.DataException.class)
                .hasMessageEndingWith("vendorY_avro serde not available for data type: " + wrongType);
    }
}
