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
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.FixedSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.MapSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
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

import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_COLLECTIONS;
import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_LOGICAL_TYPES;
import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_OPTIONAL;
import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.SCHEMA_PRIMITIVES;
import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;

/**
 * Unit tests for AvroSchemaMapper schema conversion rules.
 * <p>
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
    void basicAvroSchemaTypeConversion() {
        var avroSchema = Schema.create(Schema.Type.NULL);
        var mappedDataSchema = schemaMapper.toDataSchema(null, null, avroSchema);
        var remappedAvroSchema = schemaMapper.fromDataSchema(mappedDataSchema);

        assertThat(remappedAvroSchema).isEqualTo(avroSchema);
    }

    @Test
    void primitivesRecord_avroToKsml_roundTripsToSameKsml() {
        // Arrange
        var avroPrimitives = AvroTestUtil.loadSchema(SCHEMA_PRIMITIVES);

        // Act
        var ksmlSchema1 = toKsmlStruct(avroPrimitives);
        var backToAvro = schemaMapper.fromDataSchema(ksmlSchema1);
        var ksmlSchema2 = toKsmlStruct(backToAvro);

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
        var avroCollections = AvroTestUtil.loadSchema(SCHEMA_COLLECTIONS);

        // Act
        var ksml = toKsmlStruct(avroCollections);

        // Assert structure
        assertThat(ksml.name()).isEqualTo("Collections");
        assertThat(ksml.namespace()).isEqualTo("io.axual.test");
        assertThat(ksml.fields()).hasSize(4);

        // strs: array of strings, required
        var strs = ksml.field("strs");
        assertThat(strs.required()).isTrue();
        assertThat(strs.schema()).isInstanceOf(ListSchema.class);
        assertThat(((ListSchema) strs.schema()).valueSchema()).isEqualTo(DataSchema.STRING_SCHEMA);

        // intMap: map of ints, required
        var intMap = ksml.field("intMap");
        assertThat(intMap.required()).isTrue();
        assertThat(intMap.schema()).isInstanceOf(MapSchema.class);
        assertThat(((MapSchema) intMap.schema()).valueSchema()).isEqualTo(DataSchema.INTEGER_SCHEMA);

        // singleUnion: [null, string, int, record X, enum Y] with default null -> optional
        var singleUnion = ksml.field("singleUnion");

        assertThat(singleUnion.required()).isFalse();
        assertThat(singleUnion.schema()).isInstanceOf(UnionSchema.class);
        var singleUnionSchema = (UnionSchema) singleUnion.schema();
        assertThat(singleUnionSchema.members()).hasSize(4);
        // Ensure record X and enum Y are present among union member schemas
        var hasRecordX = false;
        var hasEnumY = false;
        var hasString = false;
        var hasInt = false;
        var hasNull = false;
        for (var member : singleUnionSchema.members()) {
            var ms = member.schema();
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
        var unionList = ksml.field("unionList");
        assertThat(unionList.required()).isTrue();
        assertThat(unionList.schema()).isInstanceOf(ListSchema.class);
        var listValue = ((ListSchema) unionList.schema()).valueSchema();
        assertThat(listValue).isInstanceOf(UnionSchema.class);
        var unionListValue = (UnionSchema) listValue;
        assertThat(unionListValue.members()).hasSize(4);

        // Round-trip stability
        var backToAvro = schemaMapper.fromDataSchema(ksml);
        var ksmlAgain = toKsmlStruct(backToAvro);
        assertThat(ksmlAgain).isEqualTo(ksml);
    }

    @Test
    void logicalTypes_avroToKsml_mapsToUnderlyingPrimitives_andRoundTripStable() {
        // Arrange: logical types schema uses Avro logicalType annotations; mapper maps to base primitive schemas
        var avroLogical = AvroTestUtil.loadSchema(SCHEMA_LOGICAL_TYPES);

        // Act
        var ksml = toKsmlStruct(avroLogical);

        // Assert underlying primitive schemas
        assertThat(ksml.field("date").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA); // date -> int
        assertThat(ksml.field("timeMillis").schema()).isEqualTo(DataSchema.INTEGER_SCHEMA); // time-millis -> int
        assertThat(ksml.field("tsMillis").schema()).isEqualTo(DataSchema.LONG_SCHEMA); // timestamp-millis -> long
        assertThat(ksml.field("uuid").schema()).isEqualTo(DataSchema.STRING_SCHEMA); // uuid -> string
        assertThat(ksml.field("decimal").schema()).isEqualTo(DataSchema.BYTES_SCHEMA); // decimal -> bytes

        // Round-trip
        var backToAvro = schemaMapper.fromDataSchema(ksml);
        var again = toKsmlStruct(backToAvro);
        assertThat(again).isEqualTo(ksml);
    }

    @Test
    void optionalFields_avroToKsml_optionalAndTypes_andRoundTripStable() {
        var avro = AvroTestUtil.loadSchema(SCHEMA_OPTIONAL);
        var ksml = (StructSchema) schemaMapper.toDataSchema(avro.getNamespace(), avro.getName(), avro);

        // All fields should be optional
        for (var f : ksml.fields()) {
            assertThat(f.required()).isFalse();
        }

        // Type assertions
        assertThat(ksml.field("optStr"))
                .returns(false, StructSchema.Field::required)
                .returns(DataSchema.STRING_SCHEMA, StructSchema.Field::schema);
        assertThat(ksml.field("optInt"))
                .returns(false, StructSchema.Field::required)
                .returns(DataSchema.INTEGER_SCHEMA, StructSchema.Field::schema);
        assertThat(ksml.field("optLong"))
                .returns(false, StructSchema.Field::required)
                .returns(DataSchema.LONG_SCHEMA, StructSchema.Field::schema);
        assertThat(ksml.field("optFloat"))
                .returns(false, StructSchema.Field::required)
                .returns(DataSchema.FLOAT_SCHEMA, StructSchema.Field::schema);
        assertThat(ksml.field("optDouble"))
                .returns(false, StructSchema.Field::required)
                .returns(DataSchema.DOUBLE_SCHEMA, StructSchema.Field::schema);
        assertThat(ksml.field("optBool"))
                .returns(false, StructSchema.Field::required)
                .returns(DataSchema.BOOLEAN_SCHEMA, StructSchema.Field::schema);
        assertThat(ksml.field("optBytes"))
                .returns(false, StructSchema.Field::required)
                .returns(DataSchema.BYTES_SCHEMA, StructSchema.Field::schema);

        assertThat(ksml.field("optStrList"))
                .returns(false, StructSchema.Field::required)
                .returns(new ListSchema(DataSchema.STRING_SCHEMA), StructSchema.Field::schema);
        assertThat(ksml.field("optIntMap"))
                .returns(false, StructSchema.Field::required)
                .returns(new MapSchema(DataSchema.INTEGER_SCHEMA), StructSchema.Field::schema);

        final var expectedOptRec = new StructSchema("io.axual.test", "OptInner", null, List.of(new StructSchema.Field("id", DataSchema.INTEGER_SCHEMA)), false);
        assertThat(ksml.field("optRec"))
                .returns(false, StructSchema.Field::required)
                .returns(expectedOptRec, StructSchema.Field::schema);

        final var expectedOptEnum = new EnumSchema("io.axual.test", "OptColor", null, List.of(new EnumSchema.Symbol("RED"), new EnumSchema.Symbol("GREEN"), new EnumSchema.Symbol("BLUE")));
        assertThat(ksml.field("optEnum"))
                .returns(false, StructSchema.Field::required)
                .returns(expectedOptEnum, StructSchema.Field::schema);

        // Round-trip back to Avro and check defaults & union w/ null
        var back = schemaMapper.fromDataSchema(ksml);
        assertThat(back).isEqualTo(avro);
        var ksmlAgain = (StructSchema) schemaMapper.toDataSchema(back.getNamespace(), back.getName(), back);
        assertThat(ksmlAgain).isEqualTo(ksml);

        // In back schema, all fields should be union with null first and default null
        for (var af : back.getFields()) {
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
        final var ksmlEnum = new EnumSchema(testNamespace, "TestingEnum", "Some enum", List.of(new EnumSchema.Symbol("A"), new EnumSchema.Symbol("B"), new EnumSchema.Symbol("C")), new EnumSchema.Symbol("B"));
        testData.add(new SchemaPairAndDescription("Enum Schema", avroEnum, ksmlEnum));

        final var avroUnionPrimitive = avroSchemaBuilder.unionOf()
                .stringType().and().booleanType().and().bytesType().and().doubleType().and().floatType().and().intType().and().longType().endUnion();
        final var ksmlUnionPrimitive = new UnionSchema(
                new UnionSchema.Member(DataSchema.STRING_SCHEMA),
                new UnionSchema.Member(DataSchema.BOOLEAN_SCHEMA),
                new UnionSchema.Member(DataSchema.BYTES_SCHEMA),
                new UnionSchema.Member(DataSchema.DOUBLE_SCHEMA),
                new UnionSchema.Member(DataSchema.FLOAT_SCHEMA),
                new UnionSchema.Member(DataSchema.INTEGER_SCHEMA),
                new UnionSchema.Member(DataSchema.LONG_SCHEMA)
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
                .field(new StructSchema.Field("simple", DataSchema.STRING_SCHEMA))
                .additionalFieldsAllowed(false)
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
                .field(new StructSchema.Field("booleanRequired", DataSchema.BOOLEAN_SCHEMA))
                .field(new StructSchema.Field("booleanOptional", DataSchema.BOOLEAN_SCHEMA, null, NO_TAG, false, false, DataNull.INSTANCE))
                .field(new StructSchema.Field("booleanNullable", ksmlNullable(DataSchema.BOOLEAN_SCHEMA), null, NO_TAG, true, false, new DataBoolean(true)))
                .field(new StructSchema.Field("recordSimple", ksmlRecordSimple, null, NO_TAG, true))
                .field(new StructSchema.Field("enumeration", ksmlEnum, null, NO_TAG, true))
                .field(new StructSchema.Field("mapPrimitive", ksmlMapPrimitive, null, NO_TAG, true))
                .field(new StructSchema.Field("arrayPrimitive", ksmlArrayPrimitive, null, NO_TAG, true))
                .additionalFieldsAllowed(false)
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
        final var schemas = new ArrayList<UnionSchema.Member>();
        addMembers(dataSchema, additionalSchemas, schemas);

        // add Null last to make it nullable
        schemas.add(new UnionSchema.Member(DataSchema.NULL_SCHEMA));
        return new UnionSchema(schemas.toArray(UnionSchema.Member[]::new));
    }

    private static void addMembers(final DataSchema dataSchema, final DataSchema[] additionalSchemas, final ArrayList<UnionSchema.Member> schemas) {
        schemas.add(new UnionSchema.Member(dataSchema));
        if (additionalSchemas != null) {
            for (var additionalSchema : additionalSchemas) {
                schemas.add(new UnionSchema.Member(additionalSchema));
            }
        }
    }

    @Test
    @DisplayName("Optional AVRO field without explicit null default")
    void testOptionalFieldWithoutNullDefault() {
        // Test optional field with no default value, using JSON to create a union type without specifying a default
        String schemaJson = """
                {
                  "type": "record",
                  "name": "Test",
                  "fields": [
                    {
                      "name": "optionalField",
                      "type": ["null", "string"]
                    }
                  ]
                }
                """;
        Schema avroSchema = new Schema.Parser().parse(schemaJson);

        DataSchema ksmlSchema = schemaMapper.toDataSchema(avroSchema);
        StructSchema structSchema = (StructSchema) ksmlSchema;
        StructSchema.Field field = structSchema.field("optionalField");

        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(field.required())
                    .as("Field should not be required")
                    .isFalse();
            softly.assertThat(field.defaultValue())
                    .as("Default value should be null (no default specified)")
                    .isNull();
        });
    }

    @Test
    @DisplayName("Optional AVRO field with explicit null default")
    void testOptionalFieldWithExplicitNullDefault() {
        // Create a schema with an explicit null default using JSON
        String schemaJson = """
                {
                  "type": "record",
                  "name": "Test",
                  "fields": [
                    {
                      "name": "optionalField",
                      "type": ["null", "string"],
                      "default": null
                    }
                  ]
                }
                """;
        Schema avroSchema = new Schema.Parser().parse(schemaJson);

        DataSchema ksmlSchema = schemaMapper.toDataSchema(avroSchema);
        StructSchema structSchema = (StructSchema) ksmlSchema;
        StructSchema.Field field = structSchema.field("optionalField");

        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(field.required())
                    .as("Field should not be required")
                    .isFalse();
            softly.assertThat(field.defaultValue())
                    .as("Default value should be DataNull.INSTANCE for explicit null")
                    .isEqualTo(DataNull.INSTANCE);
        });
    }

    // ============================================================================
    // Regression tests for an Avro default-value bug.
    //
    // Background:
    // When KSML produces a message, it has to hand its internal schema back to
    // Apache Avro. Avro then asks "what is the default value of this field?"
    // and tries to write that default into the schema as JSON. Avro can only
    // turn very specific Java types into JSON: plain Maps, Lists, byte arrays,
    // strings, numbers, booleans, and a special sentinel for null. If we hand
    // it anything else (for example, an Avro record object), it crashes with
    //     "Unknown datum class: GenericData$Record"
    // and the producer never sends a single message.
    //
    // KSML used to give Avro a record object for record-typed defaults, which
    // is what caused the bug. The fix is in AvroSchemaMapper: instead of giving
    // Avro a record object, we hand it a plain Map.
    //
    // Each test below sends a small Avro schema through KSML's converter twice
    // (Avro -> KSML schema -> Avro) and checks that the conversion succeeds and
    // produces the right shape of default value.
    // ============================================================================

    @Test
    @DisplayName("Record field with empty default {} survives the round-trip (the original bug)")
    void recordDefault_emptyObject_roundTrips() {
        // This is the simplest reproduction of the original bug as reported
        // The "inner" field is itself a record, and its default is {} (empty record).
        // Before the fix, just running this test would crash with
        // "Unknown datum class: GenericData$Record".
        final var schemaJson = """
                {
                  "type": "record",
                  "name": "Outer",
                  "namespace": "io.axual.test",
                  "fields": [
                    {
                      "name": "inner",
                      "type": {
                        "type": "record",
                        "name": "Inner",
                        "fields": [
                          { "name": "content", "type": "string", "default": "" }
                        ]
                      },
                      "default": {}
                    }
                  ]
                }
                """;
        final var avroSchema = new Schema.Parser().parse(schemaJson);

        // Avro -> KSML schema -> Avro again. The third line is the one that used
        // to crash before the fix.
        final var ksml = schemaMapper.toDataSchema(avroSchema);
        final var back = schemaMapper.fromDataSchema(ksml);

        // Going one more lap should give the same KSML schema back. If anything
        // went wrong inside the conversion, the two won't match.
        final var ksml2 = schemaMapper.toDataSchema(back.getNamespace(), back.getName(), back);
        assertThat(ksml2).isEqualTo(ksml);

        // The default for the inner record must be a plain Java Map (which Avro
        // can JSON-encode). A Map is also what `{}` looks like when written as
        // JSON. If the fix regresses, this would be an Avro record object instead
        // and Avro's Schema.Field constructor would have already thrown above.
        final var innerField = back.getField("inner");
        assertThat(innerField.hasDefaultValue()).isTrue();
        assertThat(innerField.defaultVal()).isInstanceOf(java.util.Map.class);
    }

    @Test
    @DisplayName("Record default that has 'null' inside it must use Avro's null sentinel, not raw null")
    void recordDefault_withExplicitNullInside_mapsToNullValue() {
        // This is a case I uncovered while testing partial fixes. If a record
        // default contains a literal null somewhere (here: typeCode is null),
        // the converter must turn that null into Avro's special "null sentinel"
        // (JsonProperties.NULL_VALUE). If we leave it as a plain Java null,
        // Avro crashes with a NullPointerException when it tries to encode the
        // default map. Easy to miss because most schemas don't hit this case.
        final var schemaJson = """
                {
                  "type": "record",
                  "name": "Outer",
                  "namespace": "io.axual.test",
                  "fields": [
                    {
                      "name": "id",
                      "type": {
                        "type": "record",
                        "name": "OpenIDType",
                        "fields": [
                          { "name": "content", "type": "string", "default": "" },
                          { "name": "typeCode", "type": ["null", "string"], "default": null }
                        ]
                      },
                      "default": { "content": "x", "typeCode": null }
                    }
                  ]
                }
                """;
        final var avroSchema = new Schema.Parser().parse(schemaJson);
        final var ksml = schemaMapper.toDataSchema(avroSchema);
        final var back = schemaMapper.fromDataSchema(ksml);

        // Pull out the default map for the "id" field.
        @SuppressWarnings("unchecked")
        final var defaultMap = (java.util.Map<String, Object>) back.getField("id").defaultVal();

        // Plain string default — should be the string "x".
        assertThat(defaultMap.get("content")).isEqualTo("x");

        // The important assertion: the null inside the default must be Avro's
        // null sentinel, NOT a raw Java null. A raw null here is what makes
        // Avro crash when it tries to JSON-encode the default.
        assertThat(defaultMap.get("typeCode")).isEqualTo(JsonProperties.NULL_VALUE);
    }

    @Test
    @DisplayName("Array of records as a default value (with nulls inside) survives the round-trip")
    void arrayDefault_roundTrips() {
        // Same idea as the previous test, but for arrays instead of plain records.
        // The schema has two array fields:
        //   - "tags" — empty list of strings as default. Easy case.
        //   - "items" — a list of records, each with a null in it. This is the
        //     hard case: KSML has to recurse into the list, into each record,
        //     and turn the inner nulls into Avro's null sentinel — same rule as
        //     the previous test, but going through a different code branch.
        final var schemaJson = """
                {
                  "type": "record",
                  "name": "Outer",
                  "namespace": "io.axual.test",
                  "fields": [
                    {
                      "name": "tags",
                      "type": { "type": "array", "items": "string" },
                      "default": []
                    },
                    {
                      "name": "items",
                      "type": {
                        "type": "array",
                        "items": {
                          "type": "record",
                          "name": "Item",
                          "fields": [
                            { "name": "k", "type": "string" },
                            { "name": "v", "type": ["null", "string"], "default": null }
                          ]
                        }
                      },
                      "default": [{ "k": "a", "v": null }, { "k": "b", "v": null }]
                    }
                  ]
                }
                """;
        final var avroSchema = new Schema.Parser().parse(schemaJson);
        final var ksml = schemaMapper.toDataSchema(avroSchema);
        final var back = schemaMapper.fromDataSchema(ksml);

        // Empty array default should be a plain Java List.
        assertThat(back.getField("tags").defaultVal()).isInstanceOf(java.util.List.class);

        // The "items" default is a list with two records in it.
        @SuppressWarnings("unchecked")
        final var items = (java.util.List<Object>) back.getField("items").defaultVal();
        assertThat(items).hasSize(2);

        // Each item should have been turned into a plain Java Map.
        assertThat(items.getFirst()).isInstanceOf(java.util.Map.class);

        // And the null inside each record must be Avro's null sentinel — same
        // rule as the previous test, but reached through the list branch.
        @SuppressWarnings("unchecked")
        final var first = (java.util.Map<String, Object>) items.getFirst();
        assertThat(first.get("v")).isEqualTo(JsonProperties.NULL_VALUE);
    }

    @Test
    @DisplayName("End-to-end: a deep schema like the original Enexis report survives the round-trip")
    void recordDefault_roundTrips() {
        // A trimmed copy of the schema from the original bug report. It combines
        // everything the previous tests cover, in one realistic shape:
        //   - top-level record field with default {}
        //   - record inside that with its own default {}
        //   - a named-type reference (OpenIDType) used twice, also with default {}
        //   - a union field "typeCode" with default null
        //   - an array-of-records field with default []
        //
        // If any one piece of the fix breaks, this test will fail too. Think of
        // it as the "does the real-world case work end-to-end" check.
        final var schemaJson = """
                {
                  "type": "record",
                  "name": "AssetEventValue",
                  "namespace": "io.axual.test.deep",
                  "fields": [
                    {
                      "name": "AssetEvent",
                      "type": {
                        "type": "record",
                        "name": "AssetEvent",
                        "fields": [
                          {
                            "name": "MRID",
                            "type": {
                              "type": "record",
                              "name": "OpenIDType",
                              "fields": [
                                { "name": "content", "type": "string", "default": "" },
                                { "name": "typeCode", "type": ["null", "string"], "default": null }
                              ]
                            },
                            "default": {}
                          },
                          {
                            "name": "Asset",
                            "type": {
                              "type": "record",
                              "name": "Asset",
                              "fields": [
                                { "name": "MRID", "type": "OpenIDType", "default": {} },
                                {
                                  "name": "Attributes",
                                  "type": { "type": "array", "items": {
                                    "type": "record",
                                    "name": "Attribute",
                                    "fields": [
                                      { "name": "name", "type": "string" },
                                      { "name": "value", "type": "string" }
                                    ]
                                  } },
                                  "default": []
                                }
                              ]
                            },
                            "default": {}
                          }
                        ]
                      },
                      "default": {}
                    }
                  ]
                }
                """;
        final var avroSchema = new Schema.Parser().parse(schemaJson);
        final var ksml = schemaMapper.toDataSchema(avroSchema);

        // First check: just don't throw. Before the fix, this line would crash
        // with "Unknown datum class: GenericData$Record".
        final var back = schemaMapper.fromDataSchema(ksml);
        assertThat(back.getField("AssetEvent").hasDefaultValue()).isTrue();

        // Second check: the round-trip is stable. Going through the conversion
        // twice should give us the same KSML schema each time.
        final var ksml2 = schemaMapper.toDataSchema(back.getNamespace(), back.getName(), back);
        assertThat(ksml2).isEqualTo(ksml);
    }
}
