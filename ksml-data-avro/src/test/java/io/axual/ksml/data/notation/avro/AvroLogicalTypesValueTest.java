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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_LOGICAL_TYPES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroLogicalTypesValueTest {
    private final AvroDataObjectMapper mapper = new AvroDataObjectMapper();
    private final Schema schema = AvroTestUtil.loadSchema(SCHEMA_LOGICAL_TYPES);

    private GenericData.Record sampleRecord() {
        final var record = new GenericData.Record(schema);
        record.put("date", 19785);
        record.put("timeMillis", 3723000);
        record.put("tsMillis", 1700000000123L);
        record.put("uuid", "123e4567-e89b-12d3-a456-426614174000");
        record.put("decimal", ByteBuffer.wrap(new byte[]{0x30, 0x39})); // unscaled 12345, scale 2 -> 123.45
        return record;
    }

    private DataStruct read() {
        return (DataStruct) mapper.toDataObject(sampleRecord());
    }

    @Test
    @DisplayName("Reading decodes a decimal to a canonical string and keeps the other logical values")
    void read_decodesLogicalValues() {
        final var struct = read();
        assertThat(struct.get("decimal")).isEqualTo(new DataString("123.45"));
        assertThat(struct.get("uuid")).isEqualTo(new DataString("123e4567-e89b-12d3-a456-426614174000"));
        assertThat(struct.get("date")).isEqualTo(new DataInteger(19785));
        assertThat(struct.get("timeMillis")).isEqualTo(new DataInteger(3723000));
        assertThat(struct.get("tsMillis")).isEqualTo(new DataLong(1700000000123L));
    }

    @Test
    @DisplayName("Writing re-encodes the decimal string back to the original bytes")
    void write_encodesDecimalBackToBytes() {
        final var record = (GenericRecord) mapper.fromDataObject(read());
        assertThat(record.get("decimal")).isInstanceOf(ByteBuffer.class);
        assertThat(toArray((ByteBuffer) record.get("decimal"))).containsExactly((byte) 0x30, (byte) 0x39);
        assertThat(record.get("uuid").toString()).isEqualTo("123e4567-e89b-12d3-a456-426614174000");
    }

    @Test
    @DisplayName("Writing rejects values that violate their logical type")
    void write_rejectsInvalidValues() {
        final var badUuid = read();
        badUuid.put("uuid", new DataString("not-a-uuid"));
        assertThatThrownBy(() -> mapper.fromDataObject(badUuid)).isInstanceOf(DataException.class);

        final var badTime = read();
        badTime.put("timeMillis", new DataInteger(-1));
        assertThatThrownBy(() -> mapper.fromDataObject(badTime)).isInstanceOf(DataException.class);

        final var badDecimal = read();
        badDecimal.put("decimal", new DataString("123456789.99"));
        assertThatThrownBy(() -> mapper.fromDataObject(badDecimal)).isInstanceOf(DataException.class);
    }

    @Test
    @DisplayName("Reading rejects an inbound value that violates its logical type")
    void read_rejectsInvalidInbound() {
        final var record = sampleRecord();
        record.put("timeMillis", -1);
        assertThatThrownBy(() -> mapper.toDataObject(record)).isInstanceOf(DataException.class);
    }

    private static byte[] toArray(ByteBuffer buffer) {
        final var dup = buffer.duplicate();
        final var arr = new byte[dup.remaining()];
        dup.get(arr);
        return arr;
    }
}
