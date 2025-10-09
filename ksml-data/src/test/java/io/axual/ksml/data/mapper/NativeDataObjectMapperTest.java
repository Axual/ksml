package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.exception.DataException;
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
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.Flags;
import io.axual.ksml.data.value.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.within;

@Slf4j
class NativeDataObjectMapperTest {
    private final NativeDataObjectMapper mapper = new NativeDataObjectMapper();

    @Test
    @DisplayName("convertDataListToList: null DataList -> null; unwrap values including DataNull -> null")
    void convertDataListToListNullAndUnwraps() {
        log.info("start: convertDataListToListNullAndUnwraps");
        final var nullList = new DataList(DataType.UNKNOWN, true);
        assertThat(mapper.convertDataListToList(nullList)).isNull();

        final var list = new DataList(DataType.UNKNOWN);
        list.add(new DataString("a"));
        list.add(DataNull.INSTANCE);
        list.add(new DataInteger(3));

        final var nativeList = mapper.convertDataListToList(list);
        assertThat(nativeList).containsExactly("a", null, 3);
        log.info("end: convertDataListToListNullAndUnwraps");
    }

    @Test
    @DisplayName("convertDataMapToMap: null DataMap -> null; unwrap values and sort keys alphabetically")
    void convertDataMapToMapNullAndOrdering() {
        log.info("start: convertDataMapToMapNullAndOrdering");
        final var nullMap = new DataMap(DataType.UNKNOWN, true);
        assertThat(mapper.convertDataMapToMap(nullMap)).isNull();

        final var map = new DataMap(DataType.UNKNOWN);
        map.put("b", new DataInteger(1));
        map.put("a", DataNull.INSTANCE);

        final var nativeMap = mapper.convertDataMapToMap(map);
        assertThat(nativeMap)
                .containsEntry("a", null)
                .containsEntry("b", 1)
                .extracting(Map::keySet, InstanceOfAssertFactories.set(String.class))
                .containsExactly("a", "b");
        log.info("end: convertDataMapToMapNullAndOrdering");
    }

    @Test
    @DisplayName("convertDataStructToMap: typed struct preserves required (as null if absent) and present optional fields only")
    void convertDataStructToMapTypedRequiredVsOptional() {
        log.info("start: convertDataStructToMapTypedRequiredVsOptional");
        final var requiredName = new DataField("name", DataSchema.STRING_SCHEMA); // required by default
        final var optionalAge = new DataField("age", DataSchema.INTEGER_SCHEMA, null, NO_TAG, false);
        final var personSchema = new StructSchema("example", "Person", "doc", List.of(requiredName, optionalAge));

        final var struct = new DataStruct(personSchema);
        // Only set the optional age; leave required name absent
        struct.put("age", new DataInteger(42));

        final var nativeStruct = mapper.convertDataStructToMap(struct);
        // Required field must be present (null because it wasn't set); optional field included because present
        assertThat(nativeStruct).containsOnly(
                entry("name", null),
                entry("age", 42)
        );
        log.info("end: convertDataStructToMapTypedRequiredVsOptional");
    }

    @Test
    @DisplayName("convertDataStructToMap: schemaless copies all present entries as-is (unwrapped)")
    void convertDataStructToMapSchemalessCopiesAll() {
        log.info("start: convertDataStructToMapSchemalessCopiesAll");
        final var struct = new DataStruct(); // schemaless
        struct.put("x", new DataString("val"));
        struct.put("y", DataNull.INSTANCE);

        final var nativeStruct = mapper.convertDataStructToMap(struct);
        assertThat(nativeStruct).containsOnly(
                entry("x", "val"),
                entry("y", null)
        );
        log.info("end: convertDataStructToMapSchemalessCopiesAll");
    }

    @Test
    @DisplayName("convertDataTupleToTuple unwraps elements and preserves order")
    void convertDataTupleToTupleUnwraps() {
        log.info("start: convertDataTupleToTupleUnwraps");
        final var tuple = new DataTuple(new DataString("first"), DataNull.INSTANCE, new DataInteger(7));
        final var nativeTuple = mapper.convertDataTupleToTuple(tuple);
        assertThat(nativeTuple.elements()).containsExactly("first", null, 7);
    }

    @Test
    @DisplayName("fromDataObject returns native for primitives and delegates collections to helper methods")
    void fromDataObjectPrimitiveAndCollections() {
        log.info("start: fromDataObjectPrimitiveAndCollections");
        // Primitive unwrap
        assertThat(mapper.fromDataObject(new DataString("hello"))).isEqualTo("hello");
        assertThat(mapper.fromDataObject(new DataInteger(9))).isEqualTo(9);
        assertThat(mapper.fromDataObject(DataNull.INSTANCE)).isNull();

        // Collections delegate to conversion helpers we tested above
        final var list = new DataList(DataType.UNKNOWN);
        list.add(new DataString("a"));
        final var unwrappedList = mapper.fromDataObject(list);
        assertThat(unwrappedList).isInstanceOf(List.class);
        final var asList = (List<?>) unwrappedList;
        assertThat(asList).hasSize(1);
        assertThat(asList.get(0)).isEqualTo("a");

        final var map = new DataMap(DataType.UNKNOWN);
        map.put("k", new DataInteger(1));
        final var unwrappedMap = mapper.fromDataObject(map);
        assertThat(unwrappedMap).isInstanceOf(Map.class);
        final var asMap = (Map<?, ?>) unwrappedMap;
        assertThat(asMap).hasSize(1);
        assertThat(asMap.get("k")).isEqualTo(1);
        log.info("end: fromDataObjectPrimitiveAndCollections");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("fromDataObjectArguments")
    @DisplayName("fromDataObject: unwraps primitives, bytes, strings; converts lists, maps, structs, tuples")
    void fromDataObject(String description, DataObject dataInput, Object expectedNative) {
        log.info("start: fromDataObjectParameterized - {}", description);
        final var actualNative = mapper.fromDataObject(dataInput);

        // Special handling for collections and tuple to assert content
        if (expectedNative instanceof List<?> expectedList) {
            assertThat(actualNative).isInstanceOf(List.class);
            assertThat(actualNative).isEqualTo(expectedList);
        } else if (expectedNative instanceof Map<?, ?> expectedMap) {
            assertThat(actualNative).isInstanceOf(Map.class);
            assertThat(actualNative).isEqualTo(expectedMap);
        } else if (expectedNative instanceof Tuple<?> expectedTuple) {
            assertThat(actualNative).isInstanceOf(Tuple.class);
            assertThat(actualNative).isEqualTo(expectedTuple);
        } else if (expectedNative instanceof byte[] expectedBytes) {
            assertThat(actualNative).isInstanceOf(byte[].class);
            assertThat((byte[]) actualNative).isEqualTo(expectedBytes);
        } else {
            assertThat(actualNative).isEqualTo(expectedNative);
        }
        log.info("end: fromDataObjectParameterized - {}", description);
    }

    private static Stream<Arguments> fromDataObjectArguments() {
        // Prepare a typed struct schema with one required and one optional field
        final var requiredName = new DataField("name", DataSchema.STRING_SCHEMA); // required by default
        final var optionalAge = new DataField("age", DataSchema.INTEGER_SCHEMA, null, NO_TAG, false);
        final var personSchema = new StructSchema("example", "Person", "doc", List.of(requiredName, optionalAge));

        // Build data objects to be mapped
        final var simpleList = new DataList(DataType.UNKNOWN);
        simpleList.add(new DataString("a"));
        simpleList.add(DataNull.INSTANCE);
        simpleList.add(new DataInteger(3));

        final var nullList = new DataList(DataType.UNKNOWN, true);

        final var simpleMap = new DataMap(DataType.UNKNOWN);
        simpleMap.put("b", new DataInteger(1));
        simpleMap.put("a", DataNull.INSTANCE);

        final var nullMap = new DataMap(DataType.UNKNOWN, true);

        final var typedStruct = new DataStruct(personSchema);
        typedStruct.put("age", new DataInteger(42));

        final var schemalessStruct = new DataStruct();
        schemalessStruct.put("x", new DataString("val"));
        schemalessStruct.put("y", DataNull.INSTANCE);

        final var tuple = new DataTuple(new DataString("first"), DataNull.INSTANCE, new DataInteger(7));

        // Expected native representations
        final var expectedList = Arrays.asList("a", null, 3);
        Map<String, Object> expectedMap = new TreeMap<>();
        expectedMap.put("a", null);
        expectedMap.put("b", 1);

        Map<String, Object> expectedTypedStruct = new TreeMap<>(DataStruct.COMPARATOR);
        expectedTypedStruct.put("age", 42);
        expectedTypedStruct.put("name", null);

        Map<String, Object> expectedSchemalessStruct = new TreeMap<>(DataStruct.COMPARATOR);
        expectedSchemalessStruct.put("x", "val");
        expectedSchemalessStruct.put("y", null);

        final var expectedTuple = new Tuple<>("first", null, 7);

        return Stream.of(
                Arguments.of("DataNull -> null", DataNull.INSTANCE, null),
                Arguments.of("DataBoolean(true) -> true", new DataBoolean(true), true),
                Arguments.of("DataByte(5) -> (byte)5", new DataByte((byte) 5), (byte) 5),
                Arguments.of("DataShort(12) -> (short)12", new DataShort((short) 12), (short) 12),
                Arguments.of("DataInteger(123) -> 123", new DataInteger(123), 123),
                Arguments.of("DataLong(123456789L) -> 123456789L", new DataLong(123456789L), 123456789L),
                Arguments.of("DataDouble(1.5) -> 1.5", new DataDouble(1.5), 1.5),
                Arguments.of("DataFloat(2.5f) -> 2.5f", new DataFloat(2.5f), 2.5f),
                Arguments.of("DataBytes([01,02]) -> byte[] {1,2}", new DataBytes(new byte[]{1, 2}), new byte[]{1, 2}),
                Arguments.of("DataString('abc') -> 'abc'", new DataString("abc"), "abc"),
                Arguments.of("DataList -> List with unwrapped values", simpleList, expectedList),
                Arguments.of("DataList(NULL) -> null", nullList, null),
                Arguments.of("DataMap -> Map with unwrapped values (sorted)", simpleMap, expectedMap),
                Arguments.of("DataMap(NULL) -> null", nullMap, null),
                Arguments.of("DataStruct(typed) -> Map with required null and present optional", typedStruct, expectedTypedStruct),
                Arguments.of("DataStruct(schemaless) -> Map with all entries", schemalessStruct, expectedSchemalessStruct),
                Arguments.of("DataTuple -> Tuple with unwrapped elements", tuple, expectedTuple)
        );
    }

    @Test
    @DisplayName("fromDataObject: Fail when unknown or unsupported DataObject is provided")
    void fromNativeDataObjectUnsupported() {
        final var unknownDataObject = new UnknownDataObject();

        assertThatCode(() -> mapper.fromDataObject(unknownDataObject))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("Can not convert DataObject to native dataType: UnknownDataObject");
    }

    @Test
    @DisplayName("fromDataObject: Fail when a null DataObject is provided")
    void fromNativeDataObjectNull() {
        assertThatCode(() -> mapper.fromDataObject(null))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("Can not convert DataObject to native dataType: null");
    }

    @Test
    @DisplayName("toDataObject: null -> DataNull")
    void toDataObjectNull() {
        final var result = mapper.toDataObject(null, null);
        assertThat(result).isEqualTo(DataNull.INSTANCE);
    }

    @Test
    @DisplayName("toDataObject: Boolean -> DataBoolean")
    void toDataObjectBoolean() {
        final var result = mapper.toDataObject(null, true);
        assertThat(result).isEqualTo(new DataBoolean(true));
    }

    @Test
    @DisplayName("toDataObject: Integer/Long/Short/Byte -> DataByte")
    void toDataObjectByte() {
        final var softly = new SoftAssertions();

        // Test with Byte values
        softly.assertThat(mapper.toDataObject(null, (byte) 120))
                .as("Byte value with no expected type should have value 120")
                .asInstanceOf(InstanceOfAssertFactories.type(DataByte.class))
                .returns((byte) 120, DataByte::value);
        softly.assertThat(mapper.toDataObject(DataByte.DATATYPE, (byte) 121))
                .as("Byte value with expected Byte type should have value  121")
                .asInstanceOf(InstanceOfAssertFactories.type(DataByte.class))
                .returns((byte) 121, DataByte::value);

        // Test with Integer values
        softly.assertThat(mapper.toDataObject(DataByte.DATATYPE, 122))
                .as("Integer value with expected Byte type should have value 122")
                .asInstanceOf(InstanceOfAssertFactories.type(DataByte.class))
                .returns((byte) 122, DataByte::value);

        // Test with Long values
        softly.assertThat(mapper.toDataObject(DataByte.DATATYPE, 123L))
                .as("Long value with expected Byte type should have value 123")
                .asInstanceOf(InstanceOfAssertFactories.type(DataByte.class))
                .returns((byte) 123, DataByte::value);

        // Test with Short values
        softly.assertThat(mapper.toDataObject(DataByte.DATATYPE, (short) 124))
                .as("Short value with expected Byte type should have value 124")
                .asInstanceOf(InstanceOfAssertFactories.type(DataByte.class))
                .returns((byte) 124, DataByte::value);

        softly.assertAll();
    }

    @Test
    @DisplayName("toDataObject: Integer/Long/Short/Byte -> DataShort")
    void toDataObjectShort() {
        final var softly = new SoftAssertions();

        // Test with Short values
        softly.assertThat(mapper.toDataObject(null, (short) 120))
                .as("Short value with no expected type should have value 120")
                .asInstanceOf(InstanceOfAssertFactories.type(DataShort.class))
                .returns((short) 120, DataShort::value);
        softly.assertThat(mapper.toDataObject(DataShort.DATATYPE, (short) 121))
                .as("Short value with expected short type should have value  121")
                .asInstanceOf(InstanceOfAssertFactories.type(DataShort.class))
                .returns((short) 121, DataShort::value);

        // Test with Integer values
        softly.assertThat(mapper.toDataObject(DataShort.DATATYPE, 122))
                .as("Integer value with expected short type should have value 122")
                .asInstanceOf(InstanceOfAssertFactories.type(DataShort.class))
                .returns((short) 122, DataShort::value);

        // Test with Long values
        softly.assertThat(mapper.toDataObject(DataShort.DATATYPE, 123L))
                .as("Long value with expected short type should have value 123")
                .asInstanceOf(InstanceOfAssertFactories.type(DataShort.class))
                .returns((short) 123, DataShort::value);

        // Test with Byte values
        softly.assertThat(mapper.toDataObject(DataShort.DATATYPE, (byte) 124))
                .as("Byte value with expected short type should have value 124")
                .asInstanceOf(InstanceOfAssertFactories.type(DataShort.class))
                .returns((short) 124, DataShort::value);

        softly.assertAll();
    }

    @Test
    @DisplayName("toDataObject: Integer/Long/Short/Byte -> DataInteger")
    void toDataObjectInteger() {
        final var softly = new SoftAssertions();

        // Test with Integer values
        softly.assertThat(mapper.toDataObject(null, 120))
                .as("Integer value with no expected type should have value 120")
                .asInstanceOf(InstanceOfAssertFactories.type(DataInteger.class))
                .returns(120, DataInteger::value);
        softly.assertThat(mapper.toDataObject(DataInteger.DATATYPE, 121))
                .as("Integer value with expected integer type should have value  121")
                .asInstanceOf(InstanceOfAssertFactories.type(DataInteger.class))
                .returns(121, DataInteger::value);

        // Test with Long values
        softly.assertThat(mapper.toDataObject(DataInteger.DATATYPE, 122L))
                .as("Long value with expected integer type should have value 122")
                .asInstanceOf(InstanceOfAssertFactories.type(DataInteger.class))
                .returns(122, DataInteger::value);

        // Test with Short values
        softly.assertThat(mapper.toDataObject(DataInteger.DATATYPE, (short) 123))
                .as("Short value with expected integer type should have value 123")
                .asInstanceOf(InstanceOfAssertFactories.type(DataInteger.class))
                .returns(123, DataInteger::value);

        // Test with Byte values
        softly.assertThat(mapper.toDataObject(DataInteger.DATATYPE, (byte) 124))
                .as("Byte value with expected integer type should have value 124")
                .asInstanceOf(InstanceOfAssertFactories.type(DataInteger.class))
                .returns(124, DataInteger::value);

        softly.assertAll();
    }

    @Test
    @DisplayName("toDataObject: Integer/Long/Short/Byte -> DataLong")
    void toDataObjectLong() {
        final var softly = new SoftAssertions();

        // Test with Long values
        softly.assertThat(mapper.toDataObject(null, 120L))
                .as("Long value with no expected type should have value 120")
                .asInstanceOf(InstanceOfAssertFactories.type(DataLong.class))
                .returns(120L, DataLong::value);
        softly.assertThat(mapper.toDataObject(DataLong.DATATYPE, 121L))
                .as("Long value with expected long type should have value  121")
                .asInstanceOf(InstanceOfAssertFactories.type(DataLong.class))
                .returns(121L, DataLong::value);

        // Test with Integer values
        softly.assertThat(mapper.toDataObject(DataLong.DATATYPE, 122))
                .as("Integer value with expected long type should have value 122")
                .asInstanceOf(InstanceOfAssertFactories.type(DataLong.class))
                .returns(122L, DataLong::value);

        // Test with Short values
        softly.assertThat(mapper.toDataObject(DataLong.DATATYPE, (short) 123))
                .as("Short value with expected long type should have value 123")
                .asInstanceOf(InstanceOfAssertFactories.type(DataLong.class))
                .returns(123L, DataLong::value);

        // Test with Byte values
        softly.assertThat(mapper.toDataObject(DataLong.DATATYPE, (byte) 124))
                .as("Byte value with expected long type should have value 124")
                .asInstanceOf(InstanceOfAssertFactories.type(DataLong.class))
                .returns(124L, DataLong::value);

        softly.assertAll();
    }

    @Test
    @DisplayName("toDataObject: Double/Float -> DataDouble")
    void toDataObjectDouble() {
        final var softly = new SoftAssertions();

        // Test with Double values
        softly.assertThat(mapper.toDataObject(null, 1.5))
                .as("Double value with no expected type should have value 1.5")
                .asInstanceOf(InstanceOfAssertFactories.type(DataDouble.class))
                .returns(1.5, DataDouble::value);
        softly.assertThat(mapper.toDataObject(DataDouble.DATATYPE, 1.7))
                .as("Double value with expected double type should have value  1.7")
                .asInstanceOf(InstanceOfAssertFactories.type(DataDouble.class))
                .returns(1.7, DataDouble::value);

        // Test with Float values
        softly.assertThat(mapper.toDataObject(DataDouble.DATATYPE, 1.9F))
                .as("Float value with expected double type should have value 1.9")
                .asInstanceOf(InstanceOfAssertFactories.type(DataDouble.class))
                .extracting(DataDouble::value, InstanceOfAssertFactories.DOUBLE)
                .isEqualTo(1.9, within(0.01));

        softly.assertAll();
    }

    @Test
    @DisplayName("toDataObject: Double/Float -> DataFloat")
    void toDataObjectFloat() {
        final var softly = new SoftAssertions();

        // Test with Double values
        softly.assertThat(mapper.toDataObject(null, 2.5F))
                .as("Float value with no expected type should have value 2.5")
                .asInstanceOf(InstanceOfAssertFactories.type(DataFloat.class))
                .extracting(DataFloat::value, InstanceOfAssertFactories.FLOAT)
                .isEqualTo(2.5F, within(0.01F));
        softly.assertThat(mapper.toDataObject(DataFloat.DATATYPE, 2.7F))
                .as("Float value with expected float type should have value 2.7")
                .asInstanceOf(InstanceOfAssertFactories.type(DataFloat.class))
                .extracting(DataFloat::value, InstanceOfAssertFactories.FLOAT)
                .isEqualTo(2.7F, within(0.01F));

        // Test with Float values
        softly.assertThat(mapper.toDataObject(DataFloat.DATATYPE, 2.9))
                .as("Double value with expected float type should have value 2.9")
                .asInstanceOf(InstanceOfAssertFactories.type(DataFloat.class))
                .extracting(DataFloat::value, InstanceOfAssertFactories.FLOAT)
                .isEqualTo(2.9F, within(0.01F));

        softly.assertAll();
    }

    @Test
    @DisplayName("toDataObject: byte[] -> DataBytes")
    void toDataObjectBytes() {
        final var nativeBytes = new byte[]{1, 2};
        final var result = mapper.toDataObject(null, nativeBytes);
        assertThat(result).isInstanceOf(DataBytes.class);
        assertThat(((DataBytes) result).value()).isEqualTo(nativeBytes);
    }

    @Test
    @DisplayName("toDataObject: CharSequence -> DataString")
    void toDataObjectCharSequence() {
        final var result = mapper.toDataObject(null, new StringBuilder("abc"));
        assertThat(result).isEqualTo(new DataString("abc"));
    }

    @Test
    @DisplayName("toDataObject: Byte with expected Short -> DataShort(5)")
    void toDataObjectByteToShort() {
        final var result = mapper.toDataObject(DataShort.DATATYPE, (byte) 5);
        assertThat(result).isEqualTo(new DataShort((short) 5));
    }

    @Test
    @DisplayName("toDataObject: Integer with expected Long -> DataLong(7)")
    void toDataObjectIntegerToLong() {
        final var result = mapper.toDataObject(DataLong.DATATYPE, 7);
        assertThat(result).isEqualTo(new DataLong(7L));
    }

    @Test
    @DisplayName("toDataObject: Float with expected Double -> DataDouble(2.5)")
    void toDataObjectFloatToDouble() {
        final var result = mapper.toDataObject(DataDouble.DATATYPE, 2.5f);
        assertThat(result).isEqualTo(new DataDouble(2.5));
    }

    @Test
    @DisplayName("toDataObject: List with nested struct and list -> DataList")
    void toDataObjectNestedListToDataList() {
        final var nativeInput = List.of(
                "a",
                Map.of("m", List.of(1, 2)),
                List.of(Map.of("k", "v"))
        );
        final var expected = new DataList(DataType.UNKNOWN);
        expected.add(new DataString("a"));
        final var innerStruct = new DataStruct();
        innerStruct.put("m", new DataList(DataType.UNKNOWN));
        innerStruct.getAs("m", DataList.class).add(new DataInteger(1));
        innerStruct.getAs("m", DataList.class).add(new DataInteger(2));
        expected.add(innerStruct);
        final var innerListOfStructs = new DataList(DataType.UNKNOWN);
        final var innerStruct2 = new DataStruct();
        innerStruct2.put("k", new DataString("v"));
        innerListOfStructs.add(innerStruct2);
        expected.add(innerListOfStructs);

        final var result = mapper.toDataObject(null, nativeInput);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("toDataObject: Map with nested list and struct -> DataStruct")
    void toDataObjectNestedMapToDataStruct() {
        final var nativeInput = Map.of(
                "list", List.of("x", Map.of("y", 2)),
                "struct", Map.of("z", List.of(3))
        );
        final var expected = new DataStruct();
        final var listInStruct = new DataList(DataType.UNKNOWN);
        listInStruct.add(new DataString("x"));
        final var structInList = new DataStruct();
        structInList.put("y", new DataInteger(2));
        listInStruct.add(structInList);
        expected.put("list", listInStruct);
        final var structVal = new DataStruct();
        final var listUnderZ = new DataList(DataType.UNKNOWN);
        listUnderZ.add(new DataInteger(3));
        structVal.put("z", listUnderZ);
        expected.put("struct", structVal);

        final var result = mapper.toDataObject(null, nativeInput);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("toDataObject: Map with expected MapType -> DataMap with nested list/struct values")
    void toDataObjectNestedMapToDataMapWithExpectedType() {
        final var nativeInput = Map.of(
                "list", List.of("x", Map.of("y", 2)),
                "struct", Map.of("z", List.of(3))
        );
        final var expected = new DataMap(DataType.UNKNOWN);
        final var listInStruct = new DataList(DataType.UNKNOWN);
        listInStruct.add(new DataString("x"));
        final var structInList = new DataStruct();
        structInList.put("y", new DataInteger(2));
        listInStruct.add(structInList);
        expected.put("list", listInStruct);
        final var structVal = new DataStruct();
        final var listUnderZ = new DataList(DataType.UNKNOWN);
        listUnderZ.add(new DataInteger(3));
        structVal.put("z", listUnderZ);
        expected.put("struct", structVal);

        final var result = mapper.toDataObject(new io.axual.ksml.data.type.MapType(DataType.UNKNOWN), nativeInput);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("toDataObject: List of numbers with expected Integer -> DataList of DataInteger")
    void toDataObjectListWithExpectedValueType() {
        final var nativeInput = List.of((byte) 1, (short) 2, 3L);
        final var expected = new DataList(DataInteger.DATATYPE);
        expected.add(new DataInteger(1));
        expected.add(new DataInteger(2));
        expected.add(new DataInteger(3));

        final var result = mapper.toDataObject(new io.axual.ksml.data.type.ListType(DataInteger.DATATYPE), nativeInput);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("toDataObject: Struct with nested map of String List")
    void toDataObjectSchemalessStructWithNested() {
        final var nativeInput = Map.of(
                "name", "Alice",
                "attributes", Map.of("likes", List.of("coffee", "chess"))
        );
        final var expected = new DataStruct();
        expected.put("name", new DataString("Alice"));
        final var attributes = new DataStruct();
        final var likesList = new DataList(DataType.UNKNOWN);
        likesList.add(new DataString("coffee"));
        likesList.add(new DataString("chess"));
        attributes.put("likes", likesList);
        expected.put("attributes", attributes);

        final var result = mapper.toDataObject(expected.type(), nativeInput);
        assertThat(result)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(DataStruct.class))
                .isEqualTo(expected);
    }

    @Test
    @DisplayName("toDataObject: Tuple -> DataTuple")
    void toDataObjectTuple() {
        final var nativeInput = new Tuple<>("first", null, 7);
        final var expected = new DataTuple(new DataString("first"), DataNull.INSTANCE, new DataInteger(7));
        final var result = mapper.toDataObject(null, nativeInput);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("toDataObject: Class<?> throws DataException")
    void toDataObjectWithUnknownValue() {
        final var failingValue = getClass();
        final var softly = new SoftAssertions();
        softly.assertThatCode(() -> mapper.toDataObject(DataType.UNKNOWN, failingValue))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("Can not convert value to DataObject: Class");
        softly.assertThatCode(() -> mapper.toDataObject(null, failingValue))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("Can not convert value to DataObject: Class");
        softly.assertAll();
    }

    static class UnknownDataObject implements DataObject {
        @Override
        public DataType type() {
            return DataType.UNKNOWN;
        }

        @Override
        public String toString(final Printer printer) {
            return "UNKNOWN";
        }

        @Override
        public Equal equals(Object obj, Flags flags) {
            return Equal.notEqual("Fake error");
        }
    }
}
