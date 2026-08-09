package io.axual.ksml.data.notation.avro;

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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Guards the assumption behind the decimal handling: default Avro reads and writes a decimal as raw bytes. */
class AvroDecimalWireAssumptionTest {
    @Test
    @DisplayName("Default Avro GenericData has no decimal conversion and round-trips a decimal field as raw bytes")
    void defaultGenericData_roundTripsDecimalAsRawBytes() throws Exception {
        final var schema = new Schema.Parser().parse("""
                {"type":"record","name":"D","namespace":"io.axual.test","fields":[
                  {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}""");
        final var fieldSchema = schema.getField("amount").schema();

        assertThat(GenericData.get().getConversionFor(fieldSchema.getLogicalType()))
                .as("default GenericData registers no decimal conversion, so KSML must hand it raw bytes")
                .isNull();

        final var avroRecord = new GenericData.Record(schema);
        avroRecord.put("amount", ByteBuffer.wrap(new byte[]{0x30, 0x39}));

        final var out = new ByteArrayOutputStream();
        final var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema).write(avroRecord, encoder);
        encoder.flush();

        final var decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        final var back = new GenericDatumReader<GenericRecord>(schema).read(null, decoder);

        assertThat(back.get("amount")).isInstanceOf(ByteBuffer.class);
        assertThat(toArray((ByteBuffer) back.get("amount"))).containsExactly((byte) 0x30, (byte) 0x39);
    }

    private static byte[] toArray(ByteBuffer buffer) {
        final var dup = buffer.duplicate();
        final var arr = new byte[dup.remaining()];
        dup.get(arr);
        return arr;
    }
}
