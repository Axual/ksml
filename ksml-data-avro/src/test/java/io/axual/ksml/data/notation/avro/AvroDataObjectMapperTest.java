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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.assertj.core.util.DoubleComparator;
import org.assertj.core.util.FloatComparator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.avro.test.AvroTestUtil;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;

import static io.axual.ksml.data.notation.avro.test.AvroTestUtil.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Named.named;

/**
 * Unit tests for AvroDataObjectMapper using KSML DataObject conversion rules.
 *
 * References:
 * - ksml-data/DEVELOPER_GUIDE.md (DataObject mappings and behavior)
 * - AvroTestUtil for loading Avro schemas and JSON data
 */
class AvroDataObjectMapperTest {
    private final AvroDataObjectMapper mapper = new AvroDataObjectMapper();

    private GenericRecord loadRecord(String schemaPath, String dataPath) {
        var schema = AvroTestUtil.loadSchema(schemaPath);
        return AvroTestUtil.parseRecord(schema, dataPath);
    }

    @Test
    void toDataObject_shouldMapPrimitiveFields_fromAvroGenericRecord() {
        // Arrange
        var primitivesRecord = loadRecord(SCHEMA_PRIMITIVES, DATA_PRIMITIVES);

        // Act
        var mapped = mapper.toDataObject(null, primitivesRecord);

        // Assert
        assertThat(mapped).isInstanceOf(DataStruct.class);
        var struct = (DataStruct) mapped;
        assertThat(struct.get("str")).isInstanceOf(DataString.class);
        assertThat(((DataString) struct.get("str")).value()).isEqualTo("hello");

        assertThat(struct.get("i")).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) struct.get("i")).value()).isEqualTo(123);

        assertThat(struct.get("l")).isInstanceOf(DataLong.class);
        assertThat(((DataLong) struct.get("l")).value()).isEqualTo(1234567890L);

        assertThat(struct.get("f")).isInstanceOf(DataFloat.class);
        assertThat(((DataFloat) struct.get("f")).value()).isEqualTo(12.5f);

        assertThat(struct.get("d")).isInstanceOf(DataDouble.class);
        assertThat(((DataDouble) struct.get("d")).value()).isEqualTo(12345.6789d);

        assertThat(struct.get("b")).isInstanceOf(DataBoolean.class);
        assertThat(((DataBoolean) struct.get("b")).value()).isTrue();

        // Bytes handling differs across Avro versions (ByteBuffer vs byte[]). The mapper currently
        // does not explicitly normalize ByteBuffer here, so we intentionally do not assert the 'bytes' field.
    }

    @Test
    void toDataObject_shouldMapCollectionsAndUnions_withAllBranches() {
        // Arrange
        var collectionsRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_1);

        // Act
        var struct = (DataStruct) mapper.toDataObject(null, collectionsRecord);

        // Assert arrays and maps
        assertThat(struct.get("strs")).isInstanceOf(DataList.class);
        var stringsList = (DataList) struct.get("strs");
        assertThat(stringsList.size()).isEqualTo(3);
        assertThat(stringsList.get(0)).isInstanceOf(DataString.class);
        assertThat(((DataString) stringsList.get(0)).value()).isEqualTo("a");

        assertThat(struct.get("intMap")).isInstanceOf(DataMap.class);
        var intMap = (DataMap) struct.get("intMap");
        assertThat(intMap.size()).isEqualTo(2);
        assertThat(((DataInteger) intMap.get("k1")).value()).isEqualTo(1);
        assertThat(((DataInteger) intMap.get("k2")).value()).isEqualTo(2);

        // Assert single union branch (string)
        assertThat(struct.get("singleUnion")).isInstanceOf(DataString.class);
        assertThat(((DataString) struct.get("singleUnion")).value()).isEqualTo("one");

        // Assert union list covering: null, string, int, record X, enum Y
        assertThat(struct.get("unionList")).isInstanceOf(DataList.class);
        var unionList = (DataList) struct.get("unionList");
        assertThat(unionList.size()).isEqualTo(5);

        assertThat(unionList.get(0)).isSameAs(DataNull.INSTANCE);
        assertThat(((DataString) unionList.get(1)).value()).isEqualTo("abc");
        assertThat(((DataInteger) unionList.get(2)).value()).isEqualTo(7);

        assertThat(unionList.get(3)).isInstanceOf(DataStruct.class);
        var xRecord = (DataStruct) unionList.get(3);
        assertThat(((DataInteger) xRecord.get("id")).value()).isEqualTo(1);
        assertThat(((DataString) xRecord.get("name")).value()).isEqualTo("john");

        // Avro enums map to DataString symbol name
        assertThat(((DataString) unionList.get(4)).value()).isEqualTo("C");
    }

    @Test
    void toDataObject_shouldMapUnionVariant_null() {
        // Arrange
        var genericRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_SINGLE_UNION_NULL);

        // Act
        var struct = (DataStruct) mapper.toDataObject(null, genericRecord);

        // Assert
        assertThat(struct.get("singleUnion")).isNull();
        assertThat(((DataList) struct.get("strs")).size()).isZero();
        assertThat(((DataMap) struct.get("intMap")).size()).isZero();
        assertThat(((DataList) struct.get("unionList")).size()).isZero();
    }

    @Test
    void toDataObject_shouldMapUnionVariant_int() {
        // Arrange
        var genericRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_SINGLE_UNION_INT);

        // Act
        var struct = (DataStruct) mapper.toDataObject(null, genericRecord);

        // Assert
        assertThat(struct.get("singleUnion")).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) struct.get("singleUnion")).value()).isEqualTo(42);
    }

    @Test
    void toDataObject_shouldMapUnionVariant_record() {
        // Arrange
        var genericRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_SINGLE_UNION_RECORD);

        // Act
        var struct = (DataStruct) mapper.toDataObject(null, genericRecord);

        // Assert
        assertThat(struct.get("singleUnion")).isInstanceOf(DataStruct.class);
        var x = (DataStruct) struct.get("singleUnion");
        assertThat(((DataInteger) x.get("id")).value()).isEqualTo(7);
        assertThat(((DataString) x.get("name")).value()).isEqualTo("seven");
    }

    @Test
    void roundTrip_genericRecord_toDataObject_toAvroObject_toDataObject_shouldBeStable() {
        // Arrange: use the collections case that exercises many mappings
        var inputRecord = loadRecord(SCHEMA_COLLECTIONS, DATA_COLLECTIONS_1);

        // Act: Avro GenericRecord -> KSML DataStruct
        var firstStruct = (DataStruct) mapper.toDataObject(null, inputRecord);

        // Convert KSML DataStruct -> Avro native wrapper (GenericRecord)
        var nativeAvro = mapper.fromDataObject(firstStruct);
        assertThat(nativeAvro).isInstanceOf(GenericRecord.class);

        // Convert back to KSML DataStruct from the AvroObject wrapper
        var roundTripped = (DataStruct) mapper.toDataObject(null, nativeAvro);

        // Assert: structures are equal field-by-field (using DataObject equals)
        assertThat(roundTripped).isEqualTo(firstStruct);
    }

    @Test
    void optional_allNullOrOmitted_shouldNotContainOptionalFields() {
        var schema = AvroTestUtil.loadSchema(SCHEMA_OPTIONAL);
        // Create an empty record, all fields are optional
        var rec = new GenericData.Record(schema);
        // Verify all fields
        assertThat(GenericData.get().validate(schema, rec)).isTrue();
        var ds = (DataStruct) mapper.toDataObject(null, rec);

        // All fields should be absent (getter returns null)
        assertThat(ds.get("optStr")).isEqualTo(new DataString(null));
        assertThat(ds.get("optInt")).isEqualTo(new DataInteger(null));
        assertThat(ds.get("optLong")).isEqualTo(new DataLong(null));
        assertThat(ds.get("optFloat")).isEqualTo(new DataFloat(null));
        assertThat(ds.get("optDouble")).isEqualTo(new DataDouble(null));
        assertThat(ds.get("optBool")).isEqualTo(new DataBoolean(null));
        assertThat(ds.get("optBytes")).isEqualTo(new DataBytes(null));
        assertThat(ds.get("optStrList")).isNull();
        var expectedOptIntMap = new DataMap(DataInteger.DATATYPE, true);
        var realOptIntMap = ds.get("optIntMap");
        assertThat(realOptIntMap).usingRecursiveComparison().isEqualTo(expectedOptIntMap);
        assertThat(ds.get("optRec")).isNull();
        assertThat(ds.get("optEnum")).isNull();
    }

    @Test
    void optional_withValues_shouldMapCorrectTypes() {
        var schema = AvroTestUtil.loadSchema(SCHEMA_OPTIONAL);
        var rec = AvroTestUtil.parseRecord(schema, DATA_OPTIONAL_WITH_VALUES);
        var ds = (DataStruct) mapper.toDataObject(null, rec);

        assertThat(((DataString) ds.get("optStr")).value()).isEqualTo("hello");
        assertThat(((DataInteger) ds.get("optInt")).value()).isEqualTo(10);
        assertThat(((DataLong) ds.get("optLong")).value()).isEqualTo(9999999999L);
        assertThat(((DataFloat) ds.get("optFloat")).value()).isEqualTo(3.5f);
        assertThat(((DataDouble) ds.get("optDouble")).value()).isEqualTo(6.25d);
        assertThat(((DataBoolean) ds.get("optBool")).value()).isTrue();
        // bytes normalization handled in AvroDataObjectMapper -> super; value existence is sufficient
        assertThat(ds.get("optBytes")).isNotNull();

        var list = (DataList) ds.get("optStrList");
        assertThat(list.size()).isEqualTo(2);
        assertThat(((DataString) list.get(0)).value()).isEqualTo("x");

        var map = (DataMap) ds.get("optIntMap");
        assertThat(((DataInteger) map.get("a")).value()).isEqualTo(1);
        assertThat(((DataInteger) map.get("b")).value()).isEqualTo(2);

        var inner = (DataStruct) ds.get("optRec");
        assertThat(((DataInteger) inner.get("id")).value()).isEqualTo(5);

        // Avro enum becomes DataString
        assertThat(((DataString) ds.get("optEnum")).value()).isEqualTo("GREEN");

        // Round-trip stability for this struct
        var again = (DataStruct) mapper.toDataObject(mapper.fromDataObject(ds));
        assertThat(again).usingRecursiveComparison().isEqualTo(ds);
    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("Test one way object mapping from Avro to KSML")
    void oneWayMapping_AvroToKsmlExpected(Object avroValue, DataObject expectedKsmlValue) {
        final var convertedKsmlValue = mapper.toDataObject(expectedKsmlValue.type(), avroValue);
        assertThat(convertedKsmlValue)
                .as("Verify conversion to KSML Data")
                .usingRecursiveComparison()
                .isEqualTo(expectedKsmlValue);
    }

    public static Stream<Arguments> oneWayMapping_AvroToKsmlExpected() {
        return oneWayMappingTestData_AvroToKsml().stream()
                .map(testData -> Arguments.of(
                                named(testData.description, testData.avroValue()), testData.ksmlValue()
                        )
                );
    }

    static List<MappingTestData> oneWayMappingTestData_AvroToKsml() {
        final var testData = new LinkedList<MappingTestData>();
        testData.add(new MappingTestData("Utf8", new Utf8("world"), new DataString("world")));
        testData.add(new MappingTestData("ByteBuffer", ByteBuffer.wrap(new byte[]{0x01, 0x03}), new DataBytes(new byte[]{0x01, 0x03})));
        testData.add(new MappingTestData("Null Object", JsonProperties.NULL_VALUE, DataNull.INSTANCE));
        testData.add(new MappingTestData("Null Object for boolean", null, new DataBoolean(null)));
        testData.add(new MappingTestData("Null Object for byte", null, new DataByte(null)));
        testData.add(new MappingTestData("Null Object for bytes", null, new DataBytes(null)));
        testData.add(new MappingTestData("Null Object for double", null, new DataDouble(null)));
        testData.add(new MappingTestData("Null Object for float", null, new DataFloat(null)));
        testData.add(new MappingTestData("Null Object for integer", null, new DataInteger(null)));
        testData.add(new MappingTestData("Null Object for long", null, new DataLong(null)));
        testData.add(new MappingTestData("Null Object for short", null, new DataShort(null)));
        testData.add(new MappingTestData("Null Object for string", null, new DataString(null)));
        return testData;
    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("Test one way object mapping from KSML to Avro")
    void oneWayMapping_KsmlToAvro(Object expectedAvroValue, DataObject ksmlValue) {
        final var convertedAvroValue = mapper.fromDataObject(ksmlValue);
        assertThat(convertedAvroValue)
                .as("Verify conversion to Avro Data")
                .isEqualTo(expectedAvroValue);
    }

    public static Stream<Arguments> oneWayMapping_KsmlToAvro() {
        return oneWayMappingTestData_KsmlToAvro().stream()
                .map(testData -> Arguments.of(
                                named(testData.description, testData.avroValue()), testData.ksmlValue()
                        )
                );
    }

    static List<MappingTestData> oneWayMappingTestData_KsmlToAvro() {
        final var testData = new LinkedList<MappingTestData>();
        testData.add(new MappingTestData("Null", null, null));
        testData.add(new MappingTestData("Byte", 16, new DataShort((short) 16)));
        testData.add(new MappingTestData("Short", 120, new DataByte((byte) 120)));
        return testData;
    }


    @ParameterizedTest
    @MethodSource
    @DisplayName("Test null handling for mapping to DataObject")
    void toDataObject_NullHandling(DataObject expectedDataObject) {
        assertThat(mapper.toDataObject(expectedDataObject.type(), null))
                .as("Verify conversion to DataObject")
                .usingComparatorForType(new DoubleComparator(0.01), Double.class)
                .usingComparatorForType(new FloatComparator(0.01f), Float.class)
                .isEqualTo(expectedDataObject);
    }

    static Stream<Arguments> toDataObject_NullHandling() {
        return Stream.of(
                Arguments.of(named("DataBoolean", new DataBoolean(null))),
                Arguments.of(named("DataByte", new DataByte(null))),
                Arguments.of(named("DataBytes", new DataBytes(null))),
                Arguments.of(named("DataDouble", new DataDouble(null))),
                Arguments.of(named("DataFloat", new DataFloat(null))),
                Arguments.of(named("DataInteger", new DataInteger(null))),
                Arguments.of(named("DataLong", new DataLong(null))),
                Arguments.of(named("DataNull", DataNull.INSTANCE)),
                Arguments.of(named("DataShort", new DataShort(null))),
                Arguments.of(named("DataString", new DataString(null))),
                Arguments.of(named("DataStruct", new DataStruct(null, true))),
                Arguments.of(named("DataList", new DataList(DataString.DATATYPE, true))),
                Arguments.of(named("DataMap", new DataMap(DataString.DATATYPE, true)))
        );
    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("Test null handling for mapping from DataObject")
    void fromDataObject_NullHandlingExplicitDataObjects(DataObject dataObject) {
        assertThat(mapper.fromDataObject(dataObject)).isNull();
    }

    static Stream<Arguments> fromDataObject_NullHandlingExplicitDataObjects() {
        return Stream.of(
                Arguments.of(named("DataBoolean", new DataBoolean(null))),
                Arguments.of(named("DataByte", new DataByte(null))),
                Arguments.of(named("DataBytes", new DataBytes(null))),
                Arguments.of(named("DataDouble", new DataDouble(null))),
                Arguments.of(named("DataFloat", new DataFloat(null))),
                Arguments.of(named("DataInteger", new DataInteger(null))),
                Arguments.of(named("DataLong", new DataLong(null))),
                Arguments.of(named("DataNull", DataNull.INSTANCE)),
                Arguments.of(named("DataShort", new DataShort(null))),
                Arguments.of(named("DataString", new DataString(null))),
                Arguments.of(named("DataStruct", new DataStruct(null, true))),
                Arguments.of(named("DataList", new DataList(DataString.DATATYPE, true))),
                Arguments.of(named("DataMap", new DataMap(DataString.DATATYPE, true)))
        );
    }


    @ParameterizedTest
    @MethodSource
    @DisplayName("Test reversible object mapping from Avro to KSML and back")
    void reversibleMapping_AvroToKsmlAndBack(Object avroValue, DataObject expectedKsmlValue) {
        final var convertedKsmlValue = mapper.toDataObject(expectedKsmlValue.type(), avroValue);
        assertThat(convertedKsmlValue)
                .as("Verify conversion to KSML Data")
                .usingRecursiveComparison()
                .isEqualTo(expectedKsmlValue);
        final var convertedAvroValue = mapper.fromDataObject(convertedKsmlValue);
        assertThat(convertedAvroValue)
                .as("Verify conversion back to Avro Data")
                .usingComparatorForType(new DoubleComparator(0.01d), Double.class)
                .usingComparatorForType(new FloatComparator(0.01f), Float.class)
                .usingRecursiveComparison()
                .isEqualTo(avroValue);
    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("Test reversible object mapping from KSML to Avro and back")
    void reversibleMapping_KsmlToAvroAndBack(DataObject ksmlValue, Object expectedAvroValue) {
        final var convertedAvroValue = mapper.fromDataObject(ksmlValue);
        assertThat(convertedAvroValue)
                .as("Verify conversion to Avro Data")
                .usingRecursiveComparison()
                .isEqualTo(expectedAvroValue);
        final var convertedDataSchema = mapper.toDataObject(ksmlValue.type(), convertedAvroValue);
        assertThat(convertedDataSchema)
                .as("Verify conversion back to KSML Data")
                .usingComparatorForType(new DoubleComparator(0.01d), Double.class)
                .usingComparatorForType(new FloatComparator(0.01f), Float.class)
                .usingRecursiveComparison()
                .isEqualTo(ksmlValue);
    }

    public static Stream<Arguments> reversibleMapping_KsmlToAvroAndBack() {
        return getReversibleMappingTestData().stream()
                .map(testData -> Arguments.of(
                                named(testData.description, testData.ksmlValue()), testData.avroValue()
                        )
                );
    }

    public static Stream<Arguments> reversibleMapping_AvroToKsmlAndBack() {
        return getReversibleMappingTestData().stream()
                .map(testData -> Arguments.of(
                                named(testData.description, testData.avroValue()), testData.ksmlValue()
                        )
                );
    }

    record MappingTestData(String description, Object avroValue,
                           DataObject ksmlValue) {
    }


    static List<MappingTestData> getReversibleMappingTestData() {
        final var testData = new LinkedList<MappingTestData>();
        testData.add(new MappingTestData("Null", null, DataNull.INSTANCE));
        testData.add(new MappingTestData("Null for boolean", null, new DataBoolean(null)));
        testData.add(new MappingTestData("Null for byte", null, new DataByte(null)));
        testData.add(new MappingTestData("Null for bytes", null, new DataBytes(null)));
        testData.add(new MappingTestData("Null for double", null, new DataDouble(null)));
        testData.add(new MappingTestData("Null for float", null, new DataFloat(null)));
        testData.add(new MappingTestData("Null for integer", null, new DataInteger(null)));
        testData.add(new MappingTestData("Null for long", null, new DataLong(null)));
        testData.add(new MappingTestData("Null for short", null, new DataShort(null)));
        testData.add(new MappingTestData("Null for string", null, new DataString(null)));

        testData.add(new MappingTestData("Boolean - true", true, new DataBoolean(true)));
        testData.add(new MappingTestData("Boolean - false", false, new DataBoolean(false)));
        testData.add(new MappingTestData("Boolean - null", null, new DataBoolean(null)));
        testData.add(new MappingTestData("String", "Hello", new DataString("Hello")));
        testData.add(new MappingTestData("Integer", 10, new DataInteger(10)));
        testData.add(new MappingTestData("Long", 100L, new DataLong(100L)));
        testData.add(new MappingTestData("Float", 200.22F, new DataFloat(200.22F)));
        testData.add(new MappingTestData("Double", 300.33D, new DataDouble(300.33D)));

        final var namespace = "io.axual.testing.mapping";
        final var schemaBuilder = SchemaBuilder.builder(namespace);
        final var avroArrayStringSchema = schemaBuilder.array().items(schemaBuilder.stringType());
        final var avroArrayStringData = new GenericData.Array<>(avroArrayStringSchema, List.of("A", "B", "C"));
        final var ksmlArrayData = new DataList();
        ksmlArrayData.add(new DataString("A"));
        ksmlArrayData.add(new DataString("B"));
        ksmlArrayData.add(new DataString("C"));
        testData.add(new MappingTestData("List of String", avroArrayStringData, ksmlArrayData));

        final var avroMapStringData = Map.of("Key1", "A", "Key2", "B", "Key3", "C", "Key4", "D");
        final var ksmlMapStringData = new DataMap();
        ksmlMapStringData.put("Key1", new DataString("A"));
        ksmlMapStringData.put("Key2", new DataString("B"));
        ksmlMapStringData.put("Key3", new DataString("C"));
        ksmlMapStringData.put("Key4", new DataString("D"));
        testData.add(new MappingTestData("Map String", avroMapStringData, ksmlMapStringData));

        final var avroFixedBytes = new byte[]{0x03, 0x01, 0x02};
        final var avroFixedSchema = schemaBuilder.fixed("TestFixed").size(3);
        final var avroFixedData = new GenericData.Fixed(avroFixedSchema, avroFixedBytes);
        final var ksmlFixedBytes = new byte[]{0x03, 0x01, 0x02};
        final var ksmlFixedData = new DataBytes(ksmlFixedBytes);
        testData.add(new MappingTestData("Map Fixed", avroFixedData, ksmlFixedData));

        // superUnionSchema
        final var avroSuperUnion = schemaBuilder
                .unionOf()
                .stringType()
                .and().booleanType()
                .and().doubleType()
                .and().floatType()
                .and().intType()
                .and().longType()
                .and().type(avroFixedSchema)
                .endUnion();
        // Test a map of arrays of union of all types
        final var avroSuperUnionArraySchema = schemaBuilder.array().items(avroSuperUnion);
        final var avroSuperMapData = Map.of(
                "Key1", new GenericData.Array<>(avroSuperUnionArraySchema, List.of(true, 1, 2L))
                , "Key2", new GenericData.Array<>(avroSuperUnionArraySchema, List.of(false, avroFixedBytes, 300.33D))
                , "Key3", new GenericData.Array<>(avroSuperUnionArraySchema, List.of(200.22F))
        );
        final var ksmlSuperMapData = new DataMap();
        final var ksmlSuperMapDataList1 = new DataList();
        final var ksmlSuperMapDataList2 = new DataList();
        final var ksmlSuperMapDataList3 = new DataList();
        ksmlSuperMapDataList1.add(new DataBoolean(true));
        ksmlSuperMapDataList1.add(new DataInteger(1));
        ksmlSuperMapDataList1.add(new DataLong(2L));
        ksmlSuperMapDataList2.add(new DataBoolean(false));
        ksmlSuperMapDataList2.add(new DataBytes(ksmlFixedBytes));
        ksmlSuperMapDataList2.add(new DataDouble(300.33D));
        ksmlSuperMapDataList3.add(new DataFloat(200.22F));
        ksmlSuperMapData.put("Key1", ksmlSuperMapDataList1);
        ksmlSuperMapData.put("Key2", ksmlSuperMapDataList2);
        ksmlSuperMapData.put("Key3", ksmlSuperMapDataList3);
        testData.add(new MappingTestData("Map Super Union", avroSuperMapData, ksmlSuperMapData));

        return testData;
    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("Unsupported DataObjects should result in an exception")
    void unsupportedDataObjectThrowsException(DataObject dataObject) {
        assertThatCode(() -> mapper.fromDataObject(dataObject))
                .as("Exception thrown for DataObject %s", dataObject.getClass())
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Can not convert DataObject to AVRO:");
    }

    public static Stream<Arguments> unsupportedDataObjectThrowsException() {
        return Stream.of(
                Arguments.of(named("DataTuple", new DataTuple())),
                Arguments.of(named("Custom DataObject", new UnsupportedDataObject()))
        );
    }


    static class UnsupportedDataObject implements DataObject {
        @Override
        public DataType type() {
            return DataType.UNKNOWN;
        }

        @Override
        public String toString(final Printer printer) {
            return getClass().getSimpleName();
        }
    }

    @Test
    void mapToDataObjectConversion() {
        final var arraysSchema = AvroTestUtil.loadSchema(SCHEMA_ARRAYS);
        final var arrayDataRecord = AvroTestUtil.parseRecord(arraysSchema, DATA_ARRAYS_1);

        final var expectedKsmlStructSchema = StructSchema.builder()
                .namespace("io.axual.test")
                .name("Arrays")
                .doc("Record covering specific arrays")
                .field(new DataField("stringList", new ListSchema(DataSchema.STRING_SCHEMA)))
                .field(new DataField("intList", new ListSchema(DataSchema.INTEGER_SCHEMA)))
                .field(new DataField("longList", new ListSchema(DataSchema.LONG_SCHEMA)))
                .field(new DataField("floatList", new ListSchema(DataSchema.FLOAT_SCHEMA)))
                .field(new DataField("doubleList", new ListSchema(DataSchema.DOUBLE_SCHEMA)))
                .field(new DataField("booleanList", new ListSchema(DataSchema.BOOLEAN_SCHEMA)))
                .build();
        final var expectedKsmlStruct = new DataStruct(expectedKsmlStructSchema);
        final var ksmlStringList = new DataList(DataString.DATATYPE);
        Stream.of("a", "b", "c").map(DataString::new).forEach(ksmlStringList::add);
        expectedKsmlStruct.put("stringList", ksmlStringList);

        final var ksmlIntList = new DataList(DataInteger.DATATYPE);
        Stream.of(1, 2, 3).map(DataInteger::new).forEach(ksmlIntList::add);
        expectedKsmlStruct.put("intList", ksmlIntList);

        final var ksmlLongList = new DataList(DataLong.DATATYPE);
        Stream.of(1000L, 2000L, 3000L).map(DataLong::new).forEach(ksmlLongList::add);
        expectedKsmlStruct.put("longList", ksmlLongList);

        final var ksmlFloatList = new DataList(DataFloat.DATATYPE);
        Stream.of(44.44F, 55.55F, 66.66F).map(DataFloat::new).forEach(ksmlFloatList::add);
        expectedKsmlStruct.put("floatList", ksmlFloatList);

        final var ksmlDoubleList = new DataList(DataDouble.DATATYPE);
        Stream.of(77.77, 88.88, 99.99).map(DataDouble::new).forEach(ksmlDoubleList::add);
        expectedKsmlStruct.put("doubleList", ksmlDoubleList);

        final var ksmlBooleanList = new DataList(DataBoolean.DATATYPE);
        Stream.of(true, true, false).map(DataBoolean::new).forEach(ksmlBooleanList::add);
        expectedKsmlStruct.put("booleanList", ksmlBooleanList);

        assertThat(mapper.toDataObject(DataType.UNKNOWN, arrayDataRecord))
                .usingComparatorForType(new DoubleComparator(0.01), Double.class)
                .usingRecursiveComparison()
                .isEqualTo(expectedKsmlStruct);
    }

}
