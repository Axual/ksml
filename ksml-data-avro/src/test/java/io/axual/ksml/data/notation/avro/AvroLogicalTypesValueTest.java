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
        final var avroRecord = new GenericData.Record(schema);
        avroRecord.put("date", 19785);
        avroRecord.put("timeMillis", 3723000);
        avroRecord.put("tsMillis", 1700000000123L);
        avroRecord.put("uuid", "123e4567-e89b-12d3-a456-426614174000");
        avroRecord.put("decimal", ByteBuffer.wrap(new byte[]{0x30, 0x39})); // unscaled 12345, scale 2 -> 123.45
        return avroRecord;
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
        final var avroRecord = (GenericRecord) mapper.fromDataObject(read());
        assertThat(avroRecord.get("decimal")).isInstanceOf(ByteBuffer.class);
        assertThat(toArray((ByteBuffer) avroRecord.get("decimal"))).containsExactly((byte) 0x30, (byte) 0x39);
        assertThat(avroRecord.get("uuid")).hasToString("123e4567-e89b-12d3-a456-426614174000");
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
        final var avroRecord = sampleRecord();
        avroRecord.put("timeMillis", -1);
        assertThatThrownBy(() -> mapper.toDataObject(avroRecord)).isInstanceOf(DataException.class);
    }

    @Test
    @DisplayName("An optional decimal field round-trips a present value and a null")
    void optionalDecimal_roundTrips() {
        final var optSchema = new Schema.Parser().parse("""
                {"type":"record","name":"OptDecimal","namespace":"io.axual.test","fields":[
                  {"name":"amount","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}],"default":null}]}""");

        final var present = new GenericData.Record(optSchema);
        present.put("amount", ByteBuffer.wrap(new byte[]{0x30, 0x39}));
        final var presentStruct = (DataStruct) mapper.toDataObject(present);
        assertThat(presentStruct.get("amount")).isEqualTo(new DataString("123.45"));
        final var backPresent = (GenericRecord) mapper.fromDataObject(presentStruct);
        assertThat(toArray((ByteBuffer) backPresent.get("amount"))).containsExactly((byte) 0x30, (byte) 0x39);

        final var absent = new GenericData.Record(optSchema);
        final var absentStruct = (DataStruct) mapper.toDataObject(absent);
        final var backAbsent = (GenericRecord) mapper.fromDataObject(absentStruct);
        assertThat(backAbsent.get("amount")).isNull();
    }

    private static byte[] toArray(ByteBuffer buffer) {
        final var dup = buffer.duplicate();
        final var arr = new byte[dup.remaining()];
        dup.get(arr);
        return arr;
    }
}
