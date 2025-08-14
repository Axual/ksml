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

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.util.List;

import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.Symbol;

import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_COLLECTIONS;
import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_LOGICAL_TYPES;
import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_OPTIONAL;
import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_PRIMITIVES;
import static java.util.Objects.requireNonNull;
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
        assertThat(singleUnionSchema.memberSchemas()).hasSize(5);
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
        assertThat(hasNull).as("The null schema in the union should NOT be filtered out").isTrue();
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
        assertThat(unionListValue.memberSchemas()).hasSize(5);

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

    @Test
    void optionalFields_avroToKsml_optionalAndTypes_andRoundTripStable() {
        Schema avro = AvroTestUtil.loadSchema(SCHEMA_OPTIONAL);
        StructSchema ksml = schemaMapper.toDataSchema(avro.getNamespace(), avro.getName(), avro);

        // All fields should be optional
        for (DataField f : ksml.fields()) {
            assertThat(f.required()).isFalse();
        }

        // Type assertions

        assertThat(ksml.field("optStr").schema()).isEqualTo(optionalSchema(DataSchema.STRING_SCHEMA));
        assertThat(ksml.field("optInt").schema()).isEqualTo(optionalSchema(DataSchema.INTEGER_SCHEMA));
        assertThat(ksml.field("optLong").schema()).isEqualTo(optionalSchema(DataSchema.LONG_SCHEMA));
        assertThat(ksml.field("optFloat").schema()).isEqualTo(optionalSchema(DataSchema.FLOAT_SCHEMA));
        assertThat(ksml.field("optDouble").schema()).isEqualTo(optionalSchema(DataSchema.DOUBLE_SCHEMA));
        assertThat(ksml.field("optBool").schema()).isEqualTo(optionalSchema(DataSchema.BOOLEAN_SCHEMA));
        assertThat(ksml.field("optBytes").schema()).isEqualTo(optionalSchema(DataSchema.BYTES_SCHEMA));

        assertThat(ksml.field("optStrList").schema()).isEqualTo(optionalSchema(new ListSchema(DataSchema.STRING_SCHEMA)));
        assertThat(ksml.field("optIntMap").schema()).isEqualTo(optionalSchema(new MapSchema(DataSchema.INTEGER_SCHEMA)));

        final UnionSchema expectedOptRec = optionalSchema(new StructSchema("io.axual.test", "OptInner", null, List.of(new DataField("id", DataSchema.INTEGER_SCHEMA))));
        assertThat(ksml.field("optRec").schema()).isEqualTo(expectedOptRec);

        final UnionSchema expectedOptEnum = optionalSchema(new EnumSchema("io.axual.test", "OptColor", null, List.of(new Symbol("RED"), new Symbol("GREEN"), new Symbol("BLUE"))));
        assertThat(ksml.field("optEnum").schema()).isEqualTo(expectedOptEnum);

        // Round-trip back to Avro and check defaults & union w/ null
        Schema back = schemaMapper.fromDataSchema(ksml);
        assertThat(back).isEqualTo(avro);
        StructSchema ksmlAgain = schemaMapper.toDataSchema(back.getNamespace(), back.getName(), back);
        assertThat(ksmlAgain).isEqualTo(ksml);

        // In back schema, all fields should be union with null first and default null
        for (Schema.Field af : back.getFields()) {
            assertThat(af)
                    .as("Verifying field %s", af.name())
                    .returns(true, Schema.Field::hasDefaultValue)
                    .returns(JsonProperties.NULL_VALUE, Schema.Field::defaultVal)
                    .extracting(Schema.Field::schema, InstanceOfAssertFactories.type(Schema.class))
                    .returns(Schema.Type.UNION, Schema::getType)
                    .extracting(Schema::getTypes, InstanceOfAssertFactories.list(Schema.class))
                    .first().returns(Schema.Type.NULL, Schema::getType);
        }
    }

    static UnionSchema optionalSchema(DataSchema... dataSchemas) {
        requireNonNull(dataSchemas);
        if (dataSchemas.length == 0) throw new IllegalArgumentException("No data schemas provided");

        final var dataFields = new DataField[dataSchemas.length + 1];
        dataFields[0] = new DataField(DataSchema.NULL_SCHEMA);
        for (int i = 1; i <= dataSchemas.length; i++) {
            dataFields[i] = new DataField(dataSchemas[i - 1]);
        }
        return new UnionSchema(dataFields);
    }
}
