package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema
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
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataPrimitive;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.StructType;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Named.named;

class JsonSchemaDataObjectMapperTest {
    private static final ObjectMapper JACKSON = new ObjectMapper();

    private final NativeDataObjectMapper objectMapper = new NativeDataObjectMapper();
    private final JsonSchemaDataObjectMapper mapper = new JsonSchemaDataObjectMapper(objectMapper);

    @Test
    @DisplayName("Unsupported value object throws exception")
    void invalidValue() {
        final var expectedType = new StructType();
        assertThatCode(() -> mapper.toDataObject(expectedType, "Invalid value"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Cannot convert value to DataObject");
    }

    @Test
    @DisplayName("Converts JSON object to DataStruct and back (round-trip)")
    void objectRoundTrip() {
        // Build JSON object with mixed primitives and nested structures
        var root = JACKSON.createObjectNode();
        root.put("str", "hello");
        root.put("int", 42);
        root.put("long", 1234567890123L);
        root.put("dbl", 3.14159);
        root.put("bool", true);
        root.set("arr", JACKSON.createArrayNode().add(1).add(2).add(3));
        root.set("obj", JACKSON.createObjectNode().put("a", 1).put("b", 2));
        root.putNull("nil");

        var data = mapper.toDataObject(null, root);

        assertThat(data).isInstanceOf(DataStruct.class);
        // And when converting back to compare original root to converted back
        assertThat(mapper.fromDataObject(data))
                .isEqualTo(root);
    }

    @Test
    @DisplayName("Converts JSON array string to DataList and back (round-trip)")
    void arrayRoundTrip() {
        var arr = JACKSON.createArrayNode();
        arr.add(10);
        arr.add("abc");
        arr.add(true);
        var obj = JACKSON.createObjectNode();
        obj.put("x", 1);
        obj.put("y", 2);
        arr.add(obj);

        var data = mapper.toDataObject(null, arr);
        assertThat(data).isInstanceOf(DataList.class);
        // And when converting back to compare original root to converted back
        assertThat(mapper.fromDataObject(data))
                .isEqualTo(arr);
    }

    @Test
    @DisplayName("Converts null input string to DataNull and back")
    void nullRoundTrip() {
        var data = mapper.toDataObject(null, null);
        assertThat(data).isInstanceOf(DataNull.class);
        assertThat(((DataNull) data).value()).isNull();

        // Back to JSON node: JsonStringMapper returns null for null native value
        assertThat(mapper.fromDataObject(data))
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(JsonNode.class))
                .returns(true, JsonNode::isNull);
    }

    @ParameterizedTest(name = "{index} => {0}")
    @MethodSource("primitiveCases")
    @DisplayName("Correct DataObject type and value, and object round-trip")
    void primitiveFieldRoundTrip(Object primitiveValue,
                                 Class<? extends DataObject> expectedDataObjectClass, Object expectedValue)  {
        // Build object with one field named value
        var root = JACKSON.createObjectNode();
        putValue(root, "value", primitiveValue);

        // Map to DataObject
        var data = mapper.toDataObject(null, root);
        assertThat(data).isInstanceOf(DataStruct.class);
        var valueDo = ((DataStruct) data).get("value");
        assertThat(valueDo).isInstanceOf(expectedDataObjectClass);

        // Assert underlying primitive value
        assertThat(valueDo)
                .isInstanceOf(DataPrimitive.class)
                .asInstanceOf(InstanceOfAssertFactories.type(DataPrimitive.class))
                .extracting(DataPrimitive::value)
                .isEqualTo(expectedValue);

        // Back to JSON node: JsonStringMapper returns null for null native value
        assertThat(mapper.fromDataObject(data)).isEqualTo(root);

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
                Arguments.of(named("float", 3.5f), DataFloat.class, 3.5f),
                Arguments.of(named("Null field", null), DataNull.class, null)
        );
    }

    // Helper to put values into ObjectNode with correct JSON typing
    private static void putValue(ObjectNode node, String key, Object value) {
        switch (value) {
            case null -> node.putNull(key);
            case String v -> node.put(key, v);
            case Boolean v -> node.put(key, v);
            case Integer v -> node.put(key, v);
            case Long v -> node.put(key, v);
            case Double v -> node.put(key, v);
            case Float v -> node.put(key, v);
            case byte[] v -> node.put(key, v);
            case List<?> list -> {
                var array = JACKSON.createArrayNode();
                list.forEach(elem -> addToArray(array, elem));
                node.set(key, array);
            }
            case Map<?, ?> map -> {
                var on = JACKSON.createObjectNode();
                map.forEach((k, v) -> putValue(on, String.valueOf(k), v));
                node.set(key, on);
            }
            default ->
                    throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
        }
    }

    private static void addToArray(ArrayNode array, Object value) {
        switch (value) {
            case null -> array.addNull();
            case String v -> array.add(v);
            case Boolean v -> array.add(v);
            case Integer v -> array.add(v);
            case Long v -> array.add(v);
            case Double v -> array.add(v);
            case Float v -> array.add(v);
            case byte[] v -> array.add(v);
            case List<?> list -> {
                var nested = JACKSON.createArrayNode();
                list.forEach(elem -> addToArray(nested, elem));
                array.add(nested);
            }
            case Map<?, ?> map -> {
                var on = JACKSON.createObjectNode();
                map.forEach((k, v) -> putValue(on, String.valueOf(k), v));
                array.add(on);
            }
            default ->
                    throw new IllegalArgumentException("Unsupported array element type: " + value.getClass());
        }
    }
}
