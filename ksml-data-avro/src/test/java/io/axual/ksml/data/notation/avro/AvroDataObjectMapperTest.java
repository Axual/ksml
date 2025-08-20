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

import com.google.common.collect.Maps;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.util.DoubleComparator;
import org.assertj.core.util.FloatComparator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;

/**
 * Unit tests for AvroDataObjectMapper using KSML DataObject conversion rules.
 *
 * References:
 * - ksml-data/DEVELOPER_GUIDE.md (DataObject mappings and behavior)
 * - AvroTestUtil for loading Avro schemas and JSON data
 */
class AvroDataObjectMapperTest {
    static final Comparator<Double> DOUBLE_COMPARATOR = new DoubleComparator(0.01);
    static final Comparator<Float> FLOAT_COMPARATOR = new FloatComparator(0.01f);
    static final Comparator<DataBytes> DATA_BYTES_COMPARATOR = (o1, o2) -> {
        if (o1 == null) {
            if (o2 == null) return 0;
            else return -1;
        } else {
            if (o2 == null) return 1;
            else return Arrays.compare(o1.value(), o2.value());
        }
    };

    <T> ObjectAssert<T> addDataObjectComparators(ObjectAssert<T> objectAssert) {
        return objectAssert
                .usingComparatorForType(DATA_BYTES_COMPARATOR, DataBytes.class)
                .usingComparatorForType(DOUBLE_COMPARATOR, Double.class)
                .usingComparatorForType(FLOAT_COMPARATOR, Float.class);
    }

    private final AvroDataObjectMapper mapper = new AvroDataObjectMapper();

    // TODO GenericRecord to DataStruct tests
    // TODO GenericArray to DataList tests
    // TODO Map to DataMap tests

    @ParameterizedTest
    @MethodSource
    @DisplayName("Test handling primitive data for mapping to DataObject")
    void toDataObject_PrimitiveValueHandling(Object value, DataObject expectedDataObject) {
        final var softly = new SoftAssertions();
        addDataObjectComparators(softly.assertThat(mapper.toDataObject(expectedDataObject.type(), value)))
                .as("Verify conversion to DataObject with expected type")
                .usingRecursiveComparison()
                .isEqualTo(expectedDataObject);
        addDataObjectComparators(softly.assertThat(mapper.toDataObject(value)))
                .as("Verify conversion to DataObject without expected type")
                .usingRecursiveComparison()
                .isEqualTo(expectedDataObject);

        softly.assertAll();
    }

    static Stream<Arguments> toDataObject_PrimitiveValueHandling() {
        final var schemaBuilder = SchemaBuilder.builder("io.axual.test");
        final var recordAvroSchema = schemaBuilder.record("Simple").doc("Really").fields().requiredString("value").endRecord();
        final var recordAvroData = new GenericData.Record(recordAvroSchema);
        recordAvroData.put("value", "testing");
        final var recordKsmlSchema = new StructSchema("io.axual.test", "Simple", "Really", List.of(new DataField("value", DataSchema.STRING_SCHEMA)));
        final var recordKsmlData = new DataStruct(recordKsmlSchema);
        recordKsmlData.put("value", new DataString("testing"));

        final var listData = List.of("1", "2", "3");
        final var listAvroSchema = schemaBuilder.array().items(schemaBuilder.stringType());
        final var listAvroData = new GenericData.Array(listAvroSchema, listData);
        final var listKsmlData = new DataList(DataString.DATATYPE);
        listData.stream().map(DataString::new).forEach(listKsmlData::add);

        final var mapData = Map.of("value-1", "testing-1", "value-2", "testing-2");
        final var mapAvroData = new HashMap<>(mapData);
        final var mapKsmlData = new DataMap(DataType.UNKNOWN);
        Maps.transformValues(mapData, DataString::new).forEach(mapKsmlData::put);

        final var enumSymbols = new String[]{"HELLO", "THERE", "WORLD"};
        final var enumData = enumSymbols[1];
        final var enumAvroSchema = schemaBuilder.enumeration("SimpleEnum").symbols(enumSymbols);
        final var enumAvroData = new GenericData.EnumSymbol(enumAvroSchema, enumData);
        final var enumKsmlData = new DataString(enumData);

        final var fixedData = new byte[]{0x01, 0x02, 0x03, 0x04};
        final var fixedAvroSchema = schemaBuilder.fixed("SimpleFixed").size(fixedData.length);
        final var fixedAvroData = new GenericData.Fixed(fixedAvroSchema, fixedData);
        final var fixedKsmlData = new DataBytes(fixedData);

        return Stream.of(
                Arguments.of(named("DataBoolean", Boolean.TRUE), new DataBoolean(Boolean.TRUE)),
                Arguments.of(named("DataBoolean", Boolean.FALSE), new DataBoolean(Boolean.FALSE)),
                Arguments.of(named("DataByte", (byte) 0x01), new DataByte((byte) 0x01)),
                Arguments.of(named("DataBytes", ByteBuffer.wrap(new byte[]{0x00, 0x01, 0x0f})), new DataBytes(new byte[]{0x00, 0x01, 0x0f})),
                Arguments.of(named("DataDouble", 2.22D), new DataDouble(2.22D)),
                Arguments.of(named("DataFloat", 3.33F), new DataFloat(3.33F)),
                Arguments.of(named("DataInteger", 10), new DataInteger(10)),
                Arguments.of(named("DataLong", 50L), new DataLong(50L)),
                Arguments.of(named("DataNull", null), DataNull.INSTANCE),
                Arguments.of(named("DataShort", (short) 5), new DataShort((short) 5)),
                Arguments.of(named("DataString", "testing"), new DataString("testing")),
                Arguments.of(named("DataString Utf8", new Utf8("testing2")), new DataString("testing2")),
                Arguments.of(named("Enum", enumAvroData), enumKsmlData),
                Arguments.of(named("Fixed", fixedAvroData), fixedKsmlData),
                Arguments.of(named("DataStruct", recordAvroData), recordKsmlData),
                Arguments.of(named("DataList", listAvroData), listKsmlData),
                Arguments.of(named("DataMap", mapAvroData), mapKsmlData)
        );
    }

    // TODO DataPrimitive to Avro tests
//
//    @ParameterizedTest
//    @MethodSource
//    @DisplayName("Test null handling for mapping from DataObject")
//    void fromDataObject_NullHandlingExplicitDataObjects(DataObject dataObject) {
//        assertThat(mapper.fromDataObject(dataObject)).isNull();
//    }
//
//    static Stream<Arguments> fromDataObject_NullHandlingExplicitDataObjects() {
//        return Stream.of(
//                Arguments.of(named("DataBoolean", new DataBoolean(null))),
//                Arguments.of(named("DataByte", new DataByte(null))),
//                Arguments.of(named("DataBytes", new DataBytes(null))),
//                Arguments.of(named("DataDouble", new DataDouble(null))),
//                Arguments.of(named("DataFloat", new DataFloat(null))),
//                Arguments.of(named("DataInteger", new DataInteger(null))),
//                Arguments.of(named("DataLong", new DataLong(null))),
//                Arguments.of(named("DataNull", DataNull.INSTANCE)),
//                Arguments.of(named("DataShort", new DataShort(null))),
//                Arguments.of(named("DataString", new DataString(null))),
//                Arguments.of(named("DataStruct", new DataStruct(null, true))),
//                Arguments.of(named("DataList", new DataList(DataString.DATATYPE, true))),
//                Arguments.of(named("DataMap", new DataMap(DataString.DATATYPE, true)))
//        );
//    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("Test null handling for mapping to DataObject")
    void toDataObject_NullHandling(DataObject expectedDataObject) {
        addDataObjectComparators(assertThat(mapper.toDataObject(expectedDataObject.type(), null)))
                .as("Verify conversion to DataObject")
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
}
