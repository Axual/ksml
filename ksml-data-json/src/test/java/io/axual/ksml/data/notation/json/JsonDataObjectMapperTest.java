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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataPrimitive;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;

/**
 * Tests for {@link JsonDataObjectMapper} verifying JSON <-> DataObject conversions.
 *
 * <p>Scenarios covered: round-trips for JSON objects and arrays, null handling,
 * and primitive value mapping. JSON trees are compared using Jackson to ensure
 * semantic equality. Assertions follow the AssertJ chained style used in the
 * module's other tests (e.g., JsonSchemaMapperTest).</p>
 */
@DisplayName("JsonDataObjectMapper - JSON <-> DataObject conversions")
class JsonDataObjectMapperTest {
    private static final ObjectMapper JACKSON = new ObjectMapper();

    private final JsonDataObjectMapper mapper = new JsonDataObjectMapper(false);

    @Test
    @DisplayName("Converts JSON object string to DataStruct and back (round-trip)")
    void objectRoundTrip() throws Exception {
        // Build JSON object with mixed primitives and nested structures
        ObjectNode root = JACKSON.createObjectNode();
        root.put("str", "hello");
        root.put("int", 42);
        root.put("long", 1234567890123L);
        root.put("dbl", 3.14159);
        root.put("bool", true);
        root.set("arr", JACKSON.createArrayNode().add(1).add(2).add(3));
        root.set("obj", JACKSON.createObjectNode().put("a", 1).put("b", 2));
        root.putNull("nil");

        String json = JACKSON.writeValueAsString(root);

        // When
        DataObject data = mapper.toDataObject((DataType) null, json);

        // Then: should be a DataStruct
        assertThat(data).isInstanceOf(DataStruct.class);

        // And when converting back to JSON string
        String jsonOut = mapper.fromDataObject(data);

        // Validate by parsing with Jackson and comparing trees
        JsonNode treeIn = JACKSON.readTree(json);
        JsonNode treeOut = JACKSON.readTree(jsonOut);
        assertThat(treeOut).isEqualTo(treeIn);
    }

    @Test
    @DisplayName("Converts JSON array string to DataList and back (round-trip)")
    void arrayRoundTrip() throws Exception {
        ArrayNode arr = JACKSON.createArrayNode();
        arr.add(10);
        arr.add("abc");
        arr.add(true);
        ObjectNode obj = JACKSON.createObjectNode();
        obj.put("x", 1);
        obj.put("y", 2);
        arr.add(obj);

        String json = JACKSON.writeValueAsString(arr);

        DataObject data = mapper.toDataObject(null, json);
        assertThat(data).isInstanceOf(DataList.class);

        String jsonOut = mapper.fromDataObject(data);
        JsonNode treeIn = JACKSON.readTree(json);
        JsonNode treeOut = JACKSON.readTree(jsonOut);
        assertThat(treeOut).isEqualTo(treeIn);
    }

    @Test
    @DisplayName("Converts null input string to DataNull and back")
    void nullRoundTrip() {
        DataObject data = mapper.toDataObject(null, null);
        assertThat(data).isInstanceOf(DataNull.class);
        assertThat(((DataNull) data).value()).isNull();

        // Back to JSON string: JsonStringMapper returns null for null native value
        String jsonOut = mapper.fromDataObject(data);
        assertThat(jsonOut).isNull();
    }

    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("primitiveCases")
    @DisplayName("Correct DataObject type and value, and object round-trip")
    void primitiveFieldRoundTrip(Object primitiveValue,
                                 Class<? extends DataObject> expectedDataObjectClass, Object expectedValue) throws Exception {
        // Build object with one field named value
        ObjectNode obj = JACKSON.createObjectNode();
        putValue(obj, "value", primitiveValue);
        String json = JACKSON.writeValueAsString(obj);

        // Map to DataObject
        DataObject data = mapper.toDataObject(null, json);
        assertThat(data).isInstanceOf(DataStruct.class);
        DataObject valueDo = ((DataStruct) data).get("value");
        assertThat(valueDo).isInstanceOf(expectedDataObjectClass);

        // Assert underlying primitive value
        if (valueDo instanceof DataPrimitive<?> prim) {
            assertThat(prim.value()).isEqualTo(expectedValue);
        } else {
            Assertions.fail("Expected DataPrimitive but got: " + valueDo.getClass().getSimpleName());
        }

        // Round-trip back to JSON string and compare
        String jsonOut = mapper.fromDataObject(data);
        JsonNode treeIn = JACKSON.readTree(json);
        JsonNode treeOut = JACKSON.readTree(jsonOut);
        assertThat(treeOut).isEqualTo(treeIn);
    }

    // Parameters for primitive tests: description, native value, expected DataObject class
    static Stream<Arguments> primitiveCases() {
        return Stream.of(
                Arguments.of(named("string", "hello"), DataString.class, "hello"),
                Arguments.of(named("boolean true", true), DataBoolean.class, true),
                Arguments.of(named("boolean false", false), DataBoolean.class, false),
                Arguments.of(named("int", 123), DataInteger.class, 123),
                Arguments.of(named("long", 1234567890123L), DataLong.class, 1234567890123L),
                Arguments.of(named("double", 12.5d), DataDouble.class, 12.5d),
                Arguments.of(named("float", 3.5f), DataDouble.class, 3.5d),
                Arguments.of(named("Null field", null), DataNull.class, null)
        );
    }

    // Helper to put values into ObjectNode with correct JSON typing
    private static void putValue(ObjectNode node, String key, Object value) {
        if (value == null) {
            node.putNull(key);
        } else if (value instanceof String v) {
            node.put(key, v);
        } else if (value instanceof Boolean v) {
            node.put(key, v);
        } else if (value instanceof Integer v) {
            node.put(key, v);
        } else if (value instanceof Long v) {
            node.put(key, v);
        } else if (value instanceof Double v) {
            node.put(key, v);
        } else if (value instanceof Float v) {
            node.put(key, v);
        } else if (value instanceof byte[] v) {
            node.put(key, v);
        } else if (value instanceof List<?> list) {
            ArrayNode array = JACKSON.createArrayNode();
            list.forEach(elem -> addToArray(array, elem));
            node.set(key, array);
        } else if (value instanceof Map<?, ?> map) {
            ObjectNode on = JACKSON.createObjectNode();
            map.forEach((k, v) -> putValue(on, String.valueOf(k), v));
            node.set(key, on);
        } else {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
        }
    }

    private static void addToArray(ArrayNode array, Object value) {
        if (value == null) array.addNull();
        else if (value instanceof String v) array.add(v);
        else if (value instanceof Boolean v) array.add(v);
        else if (value instanceof Integer v) array.add(v);
        else if (value instanceof Long v) array.add(v);
        else if (value instanceof Double v) array.add(v);
        else if (value instanceof Float v) array.add(v);
        else if (value instanceof byte[] v) array.add(v);
        else if (value instanceof List<?> list) {
            ArrayNode nested = JACKSON.createArrayNode();
            list.forEach(elem -> addToArray(nested, elem));
            array.add(nested);
        } else if (value instanceof Map<?, ?> map) {
            ObjectNode on = JACKSON.createObjectNode();
            map.forEach((k, v) -> putValue(on, String.valueOf(k), v));
            array.add(on);
        } else {
            throw new IllegalArgumentException("Unsupported array element type: " + value.getClass());
        }
    }
}
