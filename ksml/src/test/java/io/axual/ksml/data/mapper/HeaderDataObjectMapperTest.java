package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA_KEY_FIELD;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA_VALUE_FIELD;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeaderDataObjectMapperTest {

    private final HeaderDataObjectMapper mapper = new HeaderDataObjectMapper();

    private DataList headersWithByteList(DataObject... bytes) {
        final var byteList = new DataList(DataType.UNKNOWN);
        for (final var b : bytes) byteList.add(b);

        final var header = new DataStruct(HEADER_SCHEMA);
        header.put(HEADER_SCHEMA_KEY_FIELD, new DataString("k"));
        header.put(HEADER_SCHEMA_VALUE_FIELD, byteList);

        final var headers = new DataList(HEADER_TYPE);
        headers.add(header);
        return headers;
    }

    @Test
    @DisplayName("fromDataObject: DataByte list converts unchanged")
    void fromDataObject_byteList_ok() {
        final var headers = headersWithByteList(new DataByte((byte) 1), new DataByte((byte) 2));
        final var result = mapper.fromDataObject(headers);
        assertThat(result.lastHeader("k").value()).containsExactly((byte) 1, (byte) 2);
    }

    @Test
    @DisplayName("fromDataObject: DataShort/DataInteger/DataLong within byte range convert unchanged")
    void fromDataObject_inRangeWideNumerics_ok() {
        final var headers = headersWithByteList(
                new DataShort((short) 42), new DataInteger(7), new DataLong(-5L));
        final var result = mapper.fromDataObject(headers);
        assertThat(result.lastHeader("k").value()).containsExactly((byte) 42, (byte) 7, (byte) -5);
    }

    @Test
    @DisplayName("fromDataObject: DataInteger out of byte range throws DataException (no silent truncation)")
    void fromDataObject_integerOverflow_throws() {
        final var headers = headersWithByteList(new DataInteger(300));
        assertThatCode(() -> mapper.fromDataObject(headers))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds BYTE range");
    }

    @Test
    @DisplayName("fromDataObject: DataLong out of byte range throws DataException (no silent truncation)")
    void fromDataObject_longOverflow_throws() {
        final var headers = headersWithByteList(new DataLong(5_000_000_000L));
        assertThatCode(() -> mapper.fromDataObject(headers))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds BYTE range");
    }

    @Test
    @DisplayName("fromDataObject: DataShort out of byte range throws DataException")
    void fromDataObject_shortOverflow_throws() {
        final var headers = headersWithByteList(new DataShort((short) 200));
        assertThatCode(() -> mapper.fromDataObject(headers))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds BYTE range");
    }

    private static DataList singleHeader(DataObject key, DataObject value) {
        final var header = new DataStruct(HEADER_SCHEMA);
        header.put(HEADER_SCHEMA_KEY_FIELD, key);
        header.put(HEADER_SCHEMA_VALUE_FIELD, value);
        final var headers = new DataList(HEADER_TYPE);
        headers.add(header);
        return headers;
    }

    @Test
    @DisplayName("toDataObject: printable, binary and null header values map to the right data types")
    void toDataObject_mapsHeaderValues() {
        final var headers = new RecordHeaders();
        headers.add("printable", "hello".getBytes());
        headers.add("binary", new byte[]{0, 1, 2});
        headers.add("empty", null);

        // The null-valued header exercises the DataNull branch of convertHeaderValue.
        final var result = mapper.toDataObject(headers);

        assertThat(result).isInstanceOf(DataList.class);
        final var list = (DataList) result;
        assertThat(list).hasSize(3);
        assertThat(((DataStruct) list.get(0)).get(HEADER_SCHEMA_VALUE_FIELD)).isInstanceOf(DataString.class);
        assertThat(((DataStruct) list.get(1)).get(HEADER_SCHEMA_VALUE_FIELD)).isInstanceOf(DataBytes.class);
    }

    @Test
    @DisplayName("fromDataObject: DataString value is serialized to bytes")
    void fromDataObject_stringValue() {
        final var result = mapper.fromDataObject(singleHeader(new DataString("k"), new DataString("hello")));
        assertThat(result.lastHeader("k").value()).isEqualTo("hello".getBytes());
    }

    @Test
    @DisplayName("fromDataObject: DataBytes value is passed through unchanged")
    void fromDataObject_bytesValue() {
        final var raw = new byte[]{9, 8, 7};
        final var result = mapper.fromDataObject(singleHeader(new DataString("k"), new DataBytes(raw)));
        assertThat(result.lastHeader("k").value()).isEqualTo(raw);
    }

    @Test
    @DisplayName("fromDataObject: non-list input is rejected")
    void fromDataObject_nonList_throws() {
        final DataObject value = new DataString("not a list");
        assertThatThrownBy(() -> mapper.fromDataObject(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Kafka Headers");
    }

    @Test
    @DisplayName("fromDataObject: non-string header key is rejected")
    void fromDataObject_nonStringKey_throws() {
        final var headers = singleHeader(new DataInteger(1), new DataString("value"));
        assertThatThrownBy(() -> mapper.fromDataObject(headers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Header key");
    }

    @Test
    @DisplayName("fromDataObject: header with wrong number of fields is rejected")
    void fromDataObject_wrongFieldCount_throws() {
        final var header = new DataStruct(HEADER_SCHEMA);
        header.put(HEADER_SCHEMA_KEY_FIELD, new DataString("k"));
        final var headers = new DataList(HEADER_TYPE);
        headers.add(header);
        assertThatThrownBy(() -> mapper.fromDataObject(headers))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("fromDataObject: unsupported element type in byte list is rejected")
    void fromDataObject_unsupportedByteElement_throws() {
        final var headers = headersWithByteList(new DataString("x"));
        assertThatThrownBy(() -> mapper.fromDataObject(headers))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
