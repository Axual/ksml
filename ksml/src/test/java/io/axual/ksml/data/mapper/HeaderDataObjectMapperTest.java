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
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA_KEY_FIELD;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA_VALUE_FIELD;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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
}
