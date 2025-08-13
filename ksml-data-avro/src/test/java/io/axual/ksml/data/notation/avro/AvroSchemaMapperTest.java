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

import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.schema.*;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for AvroSchemaMapper schema conversion rules.
 *
 * References:
 * - ksml-data/DEVELOPER_GUIDE.md for KSML Schema model
 * - AvroTestUtil to load Avro schemas from resources
 */
class AvroSchemaMapperTest {
    private final AvroSchemaMapper schemaMapper = new AvroSchemaMapper();

    private StructSchema toKsml(Schema avroSchema) {
        return schemaMapper.toDataSchema(avroSchema.getNamespace(), avroSchema.getName(), avroSchema);
    }

    @Test
    void primitivesRecord_avroToKsml_roundTripsToSameKsml() {
        // Arrange
        Schema avroPrimitives = AvroTestUtil.loadSchema(SCHEMA_PRIMITIVES);

        // Act
        StructSchema ksmlSchema1 = toKsml(avroPrimitives);
        Schema backToAvro = schemaMapper.fromDataSchema(ksmlSchema1);
        StructSchema ksmlSchema2 = toKsml(backToAvro);

        // Assert
        assertThat(ksmlSchema2).isEqualTo(ksmlSchema1);
        assertThat(ksmlSchema1.fields()).hasSize(7);
        assertThat(ksmlSchema1.field("str").required()).isTrue();
        assertThat(ksmlSchema1.field("i").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA);
        assertThat(ksmlSchema1.field("l").schema()).isEqualTo(DataSchema.LONG_SCHEMA);
        assertThat(ksmlSchema1.field("f").schema()).isEqualTo(DataSchema.FLOAT_SCHEMA);
        assertThat(ksmlSchema1.field("d").schema()).isEqualTo(DataSchema.DOUBLE_SCHEMA);
        assertThat(ksmlSchema1.field("b").schema()).isEqualTo(DataSchema.BOOLEAN_SCHEMA);
        assertThat(ksmlSchema1.field("bytes").schema()).isEqualTo(DataSchema.BYTES_SCHEMA);
    }

    @Test
    void collectionsAndUnions_avroToKsml_expectedStructure_andRoundTripStable() {
        // Arrange
        Schema avroCollections = AvroTestUtil.loadSchema(SCHEMA_COLLECTIONS);

        // Act
        StructSchema ksml = toKsml(avroCollections);

        // Assert structure
        assertThat(ksml.name()).isEqualTo("Collections");
        assertThat(ksml.namespace()).isEqualTo("io.axual.test");
        assertThat(ksml.fields()).hasSize(4);

        // strs: array of strings, required
        DataField strs = ksml.field("strs");
        assertThat(strs.required()).isTrue();
        assertThat(strs.schema()).isInstanceOf(ListSchema.class);
        assertThat(((ListSchema) strs.schema()).valueSchema()).isEqualTo(DataSchema.STRING_SCHEMA);

        // intMap: map of ints, required
        DataField intMap = ksml.field("intMap");
        assertThat(intMap.required()).isTrue();
        assertThat(intMap.schema()).isInstanceOf(MapSchema.class);
        assertThat(((MapSchema) intMap.schema()).valueSchema()).isEqualTo(DataSchema.INTEGER_SCHEMA);

        // singleUnion: [null, string, int, record X, enum Y] with default null -> optional
        DataField singleUnion = ksml.field("singleUnion");

        assertThat(singleUnion.required()).isFalse();
        assertThat(singleUnion.schema()).isInstanceOf(UnionSchema.class);
        UnionSchema singleUnionSchema = (UnionSchema) singleUnion.schema();
        assertThat(singleUnionSchema.memberSchemas()).hasSize(4);
        // Ensure record X and enum Y are present among union member schemas
        boolean hasRecordX = false;
        boolean hasEnumY = false;
        boolean hasString = false;
        boolean hasInt = false;
        boolean hasNull = false;
        for (DataField member : singleUnionSchema.memberSchemas()) {
            DataSchema ms = member.schema();
            if (ms == DataSchema.STRING_SCHEMA) hasString = true;
            if (ms == DataSchema.INTEGER_SCHEMA) hasInt = true;
            if (ms == DataSchema.NULL_SCHEMA) hasNull = true;
            if (ms instanceof StructSchema s && s.name().equals("X")) hasRecordX = true;
            if (ms instanceof EnumSchema e && e.name().equals("Y")) hasEnumY = true;
        }
        assertThat(hasNull).as("The null schema in the union should be filtered out").isFalse();
        assertThat(hasString).isTrue();
        assertThat(hasInt).isTrue();
        assertThat(hasRecordX).isTrue();
        assertThat(hasEnumY).isTrue();

        // unionList: array of union(null|string|int|X|Y), required
        DataField unionList = ksml.field("unionList");
        assertThat(unionList.required()).isTrue();
        assertThat(unionList.schema()).isInstanceOf(ListSchema.class);
        DataSchema listValue = ((ListSchema) unionList.schema()).valueSchema();
        assertThat(listValue).isInstanceOf(UnionSchema.class);
        UnionSchema unionListValue = (UnionSchema) listValue;
        assertThat(unionListValue.memberSchemas()).hasSize(4);

        // Round-trip stability
        Schema backToAvro = schemaMapper.fromDataSchema(ksml);
        StructSchema ksmlAgain = toKsml(backToAvro);
        assertThat(ksmlAgain).isEqualTo(ksml);
    }

    @Test
    void logicalTypes_avroToKsml_mapsToUnderlyingPrimitives_andRoundTripStable() {
        // Arrange: logical types schema uses Avro logicalType annotations; mapper maps to base primitive schemas
        Schema avroLogical = AvroTestUtil.loadSchema(SCHEMA_LOGICAL_TYPES);

        // Act
        StructSchema ksml = toKsml(avroLogical);

        // Assert underlying primitive schemas
        assertThat(ksml.field("date").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA); // date -> int
        assertThat(ksml.field("timeMillis").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA); // time-millis -> int
        assertThat(ksml.field("tsMillis").schema()).isEqualTo(DataSchema.LONG_SCHEMA); // timestamp-millis -> long
        assertThat(ksml.field("uuid").schema()).isEqualTo(DataSchema.STRING_SCHEMA); // uuid -> string
        assertThat(ksml.field("decimal").schema()).isEqualTo(DataSchema.BYTES_SCHEMA); // decimal -> bytes

        // Round-trip
        Schema backToAvro = schemaMapper.fromDataSchema(ksml);
        StructSchema again = toKsml(backToAvro);
        assertThat(again).isEqualTo(ksml);
    }
}
