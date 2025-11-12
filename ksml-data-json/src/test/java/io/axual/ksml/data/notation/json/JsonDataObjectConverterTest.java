package io.axual.ksml.data.notation.json;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link JsonDataObjectConverter} ensuring correct conversions between:
 * - structured JSON DataObjects (DataStruct/DataMap/DataList) and DataString JSON text
 * - JSON text (DataString) and structured DataObjects for List/Map/Struct/Union target types
 *
 * <p>These tests follow the AssertJ style used in {@code JsonSchemaMapperTest}, making use of
 * chained assertions and soft assertions for validating multiple properties.</p>
 */
@DisplayName("JsonDataObjectConverter - conversions between DataObject and JSON string")
class JsonDataObjectConverterTest {
    private static final ObjectMapper JACKSON = new ObjectMapper();

    private final JsonDataObjectConverter converter = new JsonDataObjectConverter();
    private final JsonDataObjectMapper mapper = new JsonDataObjectMapper(false);

    @Test
    @DisplayName("Structured DataStruct -> DataString JSON")
    void structToString() throws Exception {
        // Build a schema-less DataStruct with mixed fields
        var inner = new DataStruct();
        inner.put("a", new DataInteger(1));
        inner.put("b", new DataInteger(2));

        var struct = new DataStruct();
        struct.put("str", new DataString("hello"));
        struct.put("int", new DataInteger(42));
        struct.put("arr", new DataList());
        ((DataList) struct.get("arr")).add(new DataInteger(1));
        ((DataList) struct.get("arr")).add(new DataInteger(2));
        ((DataList) struct.get("arr")).add(new DataInteger(3));
        struct.put("obj", inner);
        struct.put("bool", new DataBoolean(true));
        struct.put("nil", DataNull.INSTANCE);

        // When converting to a DataString
        var out = converter.convert(struct, DataString.DATATYPE);

        // Then
        var jsonString = assertThat(out)
                .isInstanceOf(DataString.class)
                .asInstanceOf(InstanceOfAssertFactories.type(DataString.class))
                .extracting(DataString::value, InstanceOfAssertFactories.STRING)
                .isNotBlank()
                .actual();

        // Compare JSON trees using Jackson
        var expected = JACKSON.createObjectNode();
        expected.put("str", "hello");
        expected.put("int", 42);
        var arr = JACKSON.createArrayNode().add(1).add(2).add(3);
        expected.set("arr", arr);
        expected.set("obj", JACKSON.createObjectNode().put("a", 1).put("b", 2));
        expected.put("bool", true);
        expected.putNull("nil");

        var actual = JACKSON.readTree(jsonString);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("Structured DataList -> DataString JSON")
    void listToString() throws Exception {
        var list = new DataList();
        list.add(new DataInteger(10));
        list.add(new DataString("abc"));
        list.add(new DataBoolean(true));
        var obj = new DataStruct();
        obj.put("x", new DataInteger(1));
        obj.put("y", new DataInteger(2));
        list.add(obj);

        var out = converter.convert(list, DataString.DATATYPE);

        var jsonString = assertThat(out)
                .isInstanceOf(DataString.class)
                .asInstanceOf(InstanceOfAssertFactories.type(DataString.class))
                .extracting(DataString::value, InstanceOfAssertFactories.STRING)
                .isNotBlank()
                .actual();

        var expected = JACKSON.createArrayNode();
        expected.add(10);
        expected.add("abc");
        expected.add(true);
        expected.add(JACKSON.createObjectNode().put("x", 1).put("y", 2));
        var actual = JACKSON.readTree(jsonString);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("Structured DataMap -> DataString JSON")
    void mapToString() throws Exception {
        var map = new DataMap();
        map.put("k1", new DataInteger(1));
        map.put("k2", new DataString("v2"));
        var nested = new DataStruct();
        nested.put("a", new DataInteger(10));
        map.put("obj", nested);

        var out = converter.convert(map, DataString.DATATYPE);

        var jsonString = assertThat(out)
                .isInstanceOf(DataString.class)
                .asInstanceOf(InstanceOfAssertFactories.type(DataString.class))
                .extracting(DataString::value, InstanceOfAssertFactories.STRING)
                .isNotBlank()
                .actual();

        var expected = JACKSON.createObjectNode();
        expected.put("k1", 1);
        expected.put("k2", "v2");
        expected.set("obj", JACKSON.createObjectNode().put("a", 10));
        var actual = JACKSON.readTree(jsonString);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("DataString JSON -> StructType/DataStruct")
    void stringToStruct() throws Exception {
        var root = JACKSON.createObjectNode();
        root.put("s", "hello");
        root.put("n", 123);
        root.set("obj", JACKSON.createObjectNode().put("a", 1));
        var json = JACKSON.writeValueAsString(root);

        // When converting to a StructType (schemaless)
        var out = converter.convert(new DataString(json), new StructType());

        var softly = new SoftAssertions();
        softly.assertThat(out).isInstanceOf(DataStruct.class);

        // Round-trip using the mapper to validate content equivalence
        var roundTrip = mapper.fromDataObject(out);
        var treeIn = JACKSON.readTree(json);
        var treeOut = JACKSON.readTree(roundTrip);
        softly.assertThat(treeOut).isEqualTo(treeIn);
        softly.assertAll();
    }

    @Test
    @DisplayName("DataString JSON array -> ListType/DataList")
    void stringToList() throws Exception {
        var arr = JACKSON.createArrayNode();
        arr.add(1);
        arr.add("x");
        arr.add(JACKSON.createObjectNode().put("a", 1));
        var json = JACKSON.writeValueAsString(arr);

        var out = converter.convert(new DataString(json), new ListType());
        assertThat(out).isInstanceOf(DataList.class);

        var roundTrip = mapper.fromDataObject(out);
        var treeIn = JACKSON.readTree(json);
        var treeOut = JACKSON.readTree(roundTrip);
        assertThat(treeOut).isEqualTo(treeIn);
    }

    @Test
    @DisplayName("DataString JSON object -> MapType/DataMap")
    void stringToMap() throws Exception {
        var obj = JACKSON.createObjectNode();
        obj.put("a", 1);
        obj.put("b", 2);
        obj.set("c", JACKSON.createObjectNode().put("x", 5));
        var json = JACKSON.writeValueAsString(obj);

        var out = converter.convert(new DataString(json), new MapType());
        assertThat(out).isInstanceOf(DataMap.class);

        var roundTrip = mapper.fromDataObject(out);
        var treeIn = JACKSON.readTree(json);
        var treeOut = JACKSON.readTree(roundTrip);
        assertThat(treeOut).isEqualTo(treeIn);
    }

    @Test
    @DisplayName("DataString JSON -> UnionType (Struct|List)")
    void stringToUnion() {
        // Union of Struct or List
        var union = new UnionType(
                new UnionType.Member(new StructType()),
                new UnionType.Member(new ListType())
        );

        // Case 1: JSON object should yield DataStruct
        var jsonObj = "{\"a\":1,\"b\":2}";
        var out1 = converter.convert(new DataString(jsonObj), union);
        // Use soft assertions to validate multiple expectations at once
        var softly = new SoftAssertions();
        softly.assertThat(out1).isInstanceOf(DataStruct.class);

        // Case 2: JSON array should yield DataList
        var jsonArr = "[1,2,3]";
        var out2 = converter.convert(new DataString(jsonArr), union);
        softly.assertThat(out2).isInstanceOf(DataList.class);
        softly.assertAll();
    }

    @Test
    @DisplayName("Non-convertible cases return null")
    void nonConvertibleCases() {
        // DataString -> DataString is not handled by the converter
        assertThat(converter.convert(new DataString("{\"a\":1}"), DataString.DATATYPE)).isNull();
        // DataStruct -> StructType is not handled (converter only struct->string or string->struct)
        assertThat(converter.convert(new DataStruct(), new StructType())).isNull();
        // Primitive to structured is not handled
        assertThat(converter.convert(new DataInteger(1), new StructType())).isNull();
    }

    @Test
    @DisplayName("Invalid JSON throws DataException")
    void invalidJsonThrows() {
        final var conversionInput = new DataString("{invalid");
        final var targetType = new StructType();
        assertThrows(DataException.class, () ->
                converter.convert(conversionInput, targetType)
        );
    }
}
