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
import org.apache.avro.SchemaBuilder;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.FixedSchema;
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
import static org.junit.jupiter.api.Named.named;

/**
 * Unit tests for AvroSchemaMapper schema conversion rules.
 *
 * References:
 * - ksml-data/DEVELOPER_GUIDE.md for KSML Schema model
 * - AvroTestUtil to load Avro schemas from resources
 */
class AvroSchemaMapperTest {
    private final AvroSchemaMapper schemaMapper = new AvroSchemaMapper();

    private StructSchema toKsmlStruct(Schema avroSchema) {
        var mappedSchema = schemaMapper.toDataSchema(avroSchema.getNamespace(), avroSchema.getName(), avroSchema);
        assertThat(mappedSchema).isInstanceOf(StructSchema.class);
        return (StructSchema) mappedSchema;
    }

    @Test
    void basicAvroSchemaTypeConversion(){
       Schema avroSchema = Schema.create(Schema.Type.NULL);
       DataSchema mappedDataSchema = schemaMapper.toDataSchema(null,null, avroSchema);
       Schema remappedAvroSchema = schemaMapper.fromDataSchema(mappedDataSchema);

       assertThat(remappedAvroSchema).isEqualTo(avroSchema);
    }

    @Test
    void primitivesRecord_avroToKsml_roundTripsToSameKsml() {
        // Arrange
        Schema avroPrimitives = AvroTestUtil.loadSchema(SCHEMA_PRIMITIVES);

        // Act
        StructSchema ksmlSchema1 = toKsmlStruct(avroPrimitives);
        Schema backToAvro = schemaMapper.fromDataSchema(ksmlSchema1);
        StructSchema ksmlSchema2 = toKsmlStruct(backToAvro);

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
        StructSchema ksml = toKsmlStruct(avroCollections);

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
        StructSchema ksmlAgain = toKsmlStruct(backToAvro);
        assertThat(ksmlAgain).isEqualTo(ksml);
    }

    @Test
    void logicalTypes_avroToKsml_mapsToUnderlyingPrimitives_andRoundTripStable() {
        // Arrange: logical types schema uses Avro logicalType annotations; mapper maps to base primitive schemas
        Schema avroLogical = AvroTestUtil.loadSchema(SCHEMA_LOGICAL_TYPES);

        // Act
        StructSchema ksml = toKsmlStruct(avroLogical);

        // Assert underlying primitive schemas
        assertThat(ksml.field("date").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA); // date -> int
        assertThat(ksml.field("timeMillis").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA); // time-millis -> int
        assertThat(ksml.field("tsMillis").schema()).isEqualTo(DataSchema.LONG_SCHEMA); // timestamp-millis -> long
        assertThat(ksml.field("uuid").schema()).isEqualTo(DataSchema.STRING_SCHEMA); // uuid -> string
        assertThat(ksml.field("decimal").schema()).isEqualTo(DataSchema.BYTES_SCHEMA); // decimal -> bytes

        // Round-trip
        Schema backToAvro = schemaMapper.fromDataSchema(ksml);
        StructSchema again = toKsmlStruct(backToAvro);
        assertThat(again).isEqualTo(ksml);
    }

    @Test
    void optionalFields_avroToKsml_optionalAndTypes_andRoundTripStable() {
        Schema avro = AvroTestUtil.loadSchema(SCHEMA_OPTIONAL);
        StructSchema ksml = (StructSchema) schemaMapper.toDataSchema(avro.getNamespace(), avro.getName(), avro);

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
        StructSchema ksmlAgain = (StructSchema) schemaMapper.toDataSchema(back.getNamespace(), back.getName(), back);
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

    @Test
    @DisplayName("Verify null and null schema avro conversions")
    void nullAvroSchemaToKsmlSchemaConversion() {
        final var softly = new SoftAssertions();

        softly.assertThat(schemaMapper.toDataSchema(null))
                .as("Schema object is null returns KSML NullSchema ")
                .isEqualTo(DataSchema.NULL_SCHEMA);
        softly.assertThat(schemaMapper.toDataSchema(Schema.create(Schema.Type.NULL)))
                .as("NullSchema object returns KSML NullSchema ")
                .isEqualTo(DataSchema.NULL_SCHEMA);

        softly.assertAll();
    }

    @Test
    @DisplayName("Verify null and null schema ksml data schema conversions")
    void nullKsmlSchemaToAvroSchemaConversion() {
        final var softly = new SoftAssertions();

        final var expectedAvroNullSchema = Schema.create(Schema.Type.NULL);
        softly.assertThat(schemaMapper.fromDataSchema(null))
                .as("Schema object is null returns Avro NullSchema ")
                .isEqualTo(expectedAvroNullSchema);
        softly.assertThat(schemaMapper.fromDataSchema(DataSchema.NULL_SCHEMA))
                .as("NullSchema object returns Avro NullSchema ")
                .isEqualTo(expectedAvroNullSchema);

        softly.assertAll();
    }

    @ParameterizedTest
    @MethodSource
    void avroSchemaToKsmlSchemaConversion(Schema avroSchema, DataSchema expectedDataSchema) {
        final var convertedDataSchema = schemaMapper.toDataSchema(avroSchema);
        assertThat(convertedDataSchema)
                .as("Verify conversion to KSML Data Schema")
                .isEqualTo(expectedDataSchema);
        final var convertedAvroSchema = schemaMapper.fromDataSchema(convertedDataSchema);
        assertThat(convertedAvroSchema)
                .as("Verify conversion back to Avro Schema")
                .isEqualTo(avroSchema);
    }

    @ParameterizedTest
    @MethodSource
    void ksmlSchemaToAvroSchemaConversion(DataSchema dataSchema, Schema expectedAvroSchema) {
        final var convertedAvroSchema = schemaMapper.fromDataSchema(dataSchema);
        assertThat(convertedAvroSchema)
                .as("Verify conversion to Avro Schema")
                .isEqualTo(expectedAvroSchema);
        final var convertedDataSchema = schemaMapper.toDataSchema(convertedAvroSchema);
        assertThat(convertedDataSchema)
                .as("Verify conversion back to KSML Data Schema")
                .isEqualTo(dataSchema);
    }

    public static Stream<Arguments> avroSchemaToKsmlSchemaConversion() {
        return getSchemaTestData().stream()
                .map(testData -> Arguments.of(
                                named(testData.description, testData.avroSchema()), testData.ksmlDataSchema()
                        )
                );
    }

    public static Stream<Arguments> ksmlSchemaToAvroSchemaConversion() {
        return getSchemaTestData().stream()
                .map(testData -> Arguments.of(
                                named(testData.description, testData.ksmlDataSchema()), testData.avroSchema()
                        )
                );
    }

    record SchemaPairAndDescription(String description, Schema avroSchema,
                                    DataSchema ksmlDataSchema) {
    }

    static List<SchemaPairAndDescription> getSchemaTestData() {
        final var testNamespace = "io.axual.test";
        final var avroSchemaBuilder = SchemaBuilder.builder(testNamespace);

        final var testData = new LinkedList<SchemaPairAndDescription>();
        testData.add(new SchemaPairAndDescription("Null Schema", avroSchemaBuilder.nullType(), DataSchema.NULL_SCHEMA));
        testData.add(new SchemaPairAndDescription("Boolean Schema", avroSchemaBuilder.booleanType(), DataSchema.BOOLEAN_SCHEMA));
        testData.add(new SchemaPairAndDescription("String Schema", avroSchemaBuilder.stringType(), DataSchema.STRING_SCHEMA));
        testData.add(new SchemaPairAndDescription("Bytes Schema", avroSchemaBuilder.bytesType(), DataSchema.BYTES_SCHEMA));
        testData.add(new SchemaPairAndDescription("Integer Schema", avroSchemaBuilder.intType(), DataSchema.INTEGER_SCHEMA));
        testData.add(new SchemaPairAndDescription("Long Schema", avroSchemaBuilder.longType(), DataSchema.LONG_SCHEMA));
        testData.add(new SchemaPairAndDescription("Double Schema", avroSchemaBuilder.doubleType(), DataSchema.DOUBLE_SCHEMA));
        testData.add(new SchemaPairAndDescription("Float Schema", avroSchemaBuilder.floatType(), DataSchema.FLOAT_SCHEMA));

        final var avroFixed = avroSchemaBuilder.fixed("TestingFixed").doc("Some fixed").size(5);
        final var ksmlFixed = new FixedSchema(testNamespace, "TestingFixed", "Some fixed", 5);
        testData.add(new SchemaPairAndDescription("Fixed Schema", avroFixed, ksmlFixed));

        final var avroEnum = avroSchemaBuilder.enumeration("TestingEnum").doc("Some enum").defaultSymbol("B").symbols("A", "B", "C");
        final var ksmlEnum = new EnumSchema(testNamespace, "TestingEnum", "Some enum", List.of(new Symbol("A"), new Symbol("B"), new Symbol("C")), new Symbol("B"));
        testData.add(new SchemaPairAndDescription("Enum Schema", avroEnum, ksmlEnum));

        final var avroUnionPrimitive = avroSchemaBuilder.unionOf()
                .stringType().and().booleanType().and().bytesType().and().doubleType().and().floatType().and().intType().and().longType().endUnion();
        final var ksmlUnionPrimitive = new UnionSchema(
                new DataField(DataSchema.STRING_SCHEMA),
                new DataField(DataSchema.BOOLEAN_SCHEMA),
                new DataField(DataSchema.BYTES_SCHEMA),
                new DataField(DataSchema.DOUBLE_SCHEMA),
                new DataField(DataSchema.FLOAT_SCHEMA),
                new DataField(DataSchema.INTEGER_SCHEMA),
                new DataField(DataSchema.LONG_SCHEMA)
        );
        testData.add(new SchemaPairAndDescription("Union Schema - Primitives", avroUnionPrimitive, ksmlUnionPrimitive));

        final var avroMapPrimitive = avroSchemaBuilder.map().values(avroSchemaBuilder.stringType());
        final var ksmlMapPrimitive = new MapSchema(DataSchema.STRING_SCHEMA);
        testData.add(new SchemaPairAndDescription("Map Schema - String", avroMapPrimitive, ksmlMapPrimitive));

        final var avroArrayPrimitive = avroSchemaBuilder.array().items(avroSchemaBuilder.stringType());
        final var ksmlArrayPrimitive = new ListSchema(DataSchema.STRING_SCHEMA);
        testData.add(new SchemaPairAndDescription("Array Schema - String", avroArrayPrimitive, ksmlArrayPrimitive));

        final var avroMapPrimitiveNullable = avroSchemaBuilder.map().values(avroSchemaBuilder.unionOf().stringType().and().nullType().endUnion());
        final var ksmlMapPrimitiveNullable = new MapSchema(ksmlNullable(DataSchema.STRING_SCHEMA));
        testData.add(new SchemaPairAndDescription("Map Schema - Nullable String ", avroMapPrimitiveNullable, ksmlMapPrimitiveNullable));

        final var avroArrayPrimitiveNullable = avroSchemaBuilder.array().items(avroSchemaBuilder.unionOf().stringType().and().nullType().endUnion());
        final var ksmlArrayPrimitiveNullable = new ListSchema(ksmlNullable(DataSchema.STRING_SCHEMA));
        testData.add(new SchemaPairAndDescription("Array Schema - Nullable String", avroArrayPrimitiveNullable, ksmlArrayPrimitiveNullable));

        final var avroRecordSimple = avroSchemaBuilder.record("TestingSimpleRecord")
                .doc("Some simple record")
                .fields()
                .requiredString("simple")
                .endRecord();
        final var ksmlRecordSimple = StructSchema.builder()
                .namespace(testNamespace)
                .name("TestingSimpleRecord")
                .doc("Some simple record")
                .field(new DataField("simple", DataSchema.STRING_SCHEMA))
                .build();
        testData.add(new SchemaPairAndDescription("Record Schema - Simple", avroRecordSimple, ksmlRecordSimple));

        final var avroArrayRecordSimple = avroSchemaBuilder.array().items(avroRecordSimple);
        final var ksmlArrayRecordSimple = new ListSchema(ksmlRecordSimple);
        testData.add(new SchemaPairAndDescription("Array Schema - Record Simple", avroArrayRecordSimple, ksmlArrayRecordSimple));

        final var avroMapRecordSimple = avroSchemaBuilder.map().values(avroRecordSimple);
        final var ksmlMapRecordSimple = new MapSchema(ksmlRecordSimple);
        testData.add(new SchemaPairAndDescription("Map Schema - Record Simple", avroMapRecordSimple, ksmlMapRecordSimple));

        final var avroRecordAdvanced = avroSchemaBuilder.record("TestingAdvancedRecord")
                .doc("Some Advanced record")
                .fields()
                .requiredBoolean("booleanRequired")
                .optionalBoolean("booleanOptional")
                .nullableBoolean("booleanNullable", true)
                .name("recordSimple").type(avroRecordSimple).noDefault()
                .name("enumeration").type(avroEnum).noDefault()
                .name("mapPrimitive").type(avroMapPrimitive).noDefault()
                .name("arrayPrimitive").type(avroArrayPrimitive).noDefault()
                .endRecord();

        final var ksmlRecordAdvanced = StructSchema.builder()
                .namespace(testNamespace)
                .name("TestingAdvancedRecord")
                .doc("Some Advanced record")
                .field(new DataField("booleanRequired", DataSchema.BOOLEAN_SCHEMA))
                .field(new DataField("booleanOptional", ksmlOptional(DataSchema.BOOLEAN_SCHEMA), null, DataField.NO_TAG, false))
                .field(new DataField("booleanNullable", ksmlNullable(DataSchema.BOOLEAN_SCHEMA), null, DataField.NO_TAG, true, false, new DataValue(true)))
                .field(new DataField("recordSimple", ksmlRecordSimple, null, DataField.NO_TAG, true))
                .field(new DataField("enumeration", ksmlEnum, null, DataField.NO_TAG, true))
                .field(new DataField("mapPrimitive", ksmlMapPrimitive, null, DataField.NO_TAG, true))
                .field(new DataField("arrayPrimitive", ksmlArrayPrimitive, null, DataField.NO_TAG, true))
                .build();
        testData.add(new SchemaPairAndDescription("Record Schema - Advanced", avroRecordAdvanced, ksmlRecordAdvanced));

        final var avroArrayRecordAdvanced = avroSchemaBuilder.array().items(avroRecordAdvanced);
        final var ksmlArrayRecordAdvanced = new ListSchema(ksmlRecordAdvanced);
        testData.add(new SchemaPairAndDescription("Array Schema - Record Advanced", avroArrayRecordAdvanced, ksmlArrayRecordAdvanced));

        final var avroMapRecordAdvanced = avroSchemaBuilder.map().values(avroRecordAdvanced);
        final var ksmlMapRecordAdvanced = new MapSchema(ksmlRecordAdvanced);
        testData.add(new SchemaPairAndDescription("Map Schema - Record Advanced", avroMapRecordAdvanced, ksmlMapRecordAdvanced));

        return testData;
    }

    // Create a KSML DataSchema counterpart of the Avro nullable schemas
    static UnionSchema ksmlNullable(DataSchema dataSchema, DataSchema... additionalSchemas) {
        final var schemas = new ArrayList<DataField>();
        addSchemas(dataSchema, additionalSchemas, schemas);

        // add Null last to make it nullable
        schemas.add(new DataField(DataSchema.NULL_SCHEMA));
        return new UnionSchema(schemas.toArray(new DataField[0]));
    }

    // Create a KSML DataSchema counterpart of the Avro optional schemas
    static UnionSchema ksmlOptional(DataSchema dataSchema, DataSchema... additionalSchemas) {
        final var schemas = new ArrayList<DataField>();
        // add Null as first type to make it optional
        schemas.add(new DataField(DataSchema.NULL_SCHEMA));
        addSchemas(dataSchema, additionalSchemas, schemas);

        return new UnionSchema(schemas.toArray(new DataField[0]));
    }

    private static void addSchemas(final DataSchema dataSchema, final DataSchema[] additionalSchemas, final ArrayList<DataField> schemas) {
        schemas.add(new DataField(dataSchema));
        if (additionalSchemas != null) {
            for (DataSchema additionalSchema : additionalSchemas) {
                schemas.add(new DataField(additionalSchema));
            }
        }
    }


}
