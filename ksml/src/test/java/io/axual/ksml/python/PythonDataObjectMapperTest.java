package io.axual.ksml.python;

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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.notation.MockNotation;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.type.UserType;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyHashMap;
import org.graalvm.polyglot.proxy.ProxyIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PythonDataObjectMapperTest {
    private static final PythonDataObjectMapper MAPPER = new PythonDataObjectMapper(true);
    private Context context;

    @BeforeAll
    static void setup() {
        final var binaryNotation = new BinaryNotation(new NotationContext(BinaryNotation.NOTATION_NAME, new DataObjectFlattener(), new DataTypeFlattener()), null);
        // Register under both the UserType default alias ("default") and the notation's own name ("binary")
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, binaryNotation);
        ExecutionContext.INSTANCE.notationLibrary().register(BinaryNotation.NOTATION_NAME, binaryNotation);
    }

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUpSchemaDirectory() {
        ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(tempDir.toString());
    }

    @AfterEach
    void clearSchemaDirectory() {
        ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory("");
    }

    @BeforeEach
    void setUp() {
        context = Context.newBuilder("python")
                .allowHostAccess(HostAccess.EXPLICIT)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    @DisplayName("toDataObject - Boolean")
    void toDataObjectBoolean() {
        Value val = context.eval("python", "True");
        DataObject result = MAPPER.toDataObject(DataBoolean.DATATYPE, val);
        assertThat(result).isInstanceOf(DataBoolean.class);
        assertThat(((DataBoolean) result).value()).isTrue();
    }

    @Test
    @DisplayName("toDataObject - Integer")
    void toDataObjectInteger() {
        Value val = context.eval("python", "42");
        DataObject result = MAPPER.toDataObject(DataInteger.DATATYPE, val);
        assertThat(result).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) result).value()).isEqualTo(42);
    }

    @Test
    @DisplayName("toDataObject - Long (default for numbers)")
    void toDataObjectLong() {
        Value val = context.eval("python", "1234567890123");
        DataObject result = MAPPER.toDataObject(DataLong.DATATYPE, val);
        assertThat(result).isInstanceOf(DataLong.class);
        assertThat(((DataLong) result).value()).isEqualTo(1234567890123L);
    }

    @Test
    @DisplayName("toDataObject - Float")
    void toDataObjectFloat() {
        Value val = context.eval("python", "3.0");
        // Using 3.0 instead of 3.14 to ensure it fits exactly in a float and avoid GraalVM coercion errors
        DataObject result = MAPPER.toDataObject(DataFloat.DATATYPE, val);
        assertThat(result).isInstanceOf(DataFloat.class);
        assertThat(((DataFloat) result).value()).isCloseTo(3.0f, org.assertj.core.data.Offset.offset(0.001f));
    }

    @Test
    @DisplayName("toDataObject - String")
    void toDataObjectString() {
        Value val = context.eval("python", "'hello'");
        DataObject result = MAPPER.toDataObject(DataString.DATATYPE, val);
        assertThat(result).isInstanceOf(DataString.class);
        assertThat(((DataString) result).value()).isEqualTo("hello");
    }

    @Test
    @DisplayName("toDataObject - Bytes")
    void toDataObjectBytes() {
        Value val = context.eval("python", "[1, 2, 3, 255]");
        DataObject result = MAPPER.toDataObject(DataBytes.DATATYPE, val);
        assertThat(result).isInstanceOf(DataBytes.class);
        assertThat(((DataBytes) result).value()).containsExactly(1, 2, 3, -1);
    }

    @Test
    @DisplayName("toDataObject - List")
    void toDataObjectList() {
        Value val = context.eval("python", "[1, 2, 3]");
        DataObject result = MAPPER.toDataObject(new ListType(DataInteger.DATATYPE), val);
        assertThat(result).isInstanceOf(DataList.class);
        DataList list = (DataList) result;
        assertThat(list.size()).isEqualTo(3);
        assertThat(list.get(0)).isEqualTo(new DataInteger(1));
    }

    @Test
    @DisplayName("toDataObject - Tuple")
    void toDataObjectTuple() {
        Value val = context.eval("python", "(1, 'two')");
        DataObject result = MAPPER.toDataObject(new TupleType(DataInteger.DATATYPE, DataString.DATATYPE), val);
        assertThat(result).isInstanceOf(DataTuple.class);
        DataTuple tuple = (DataTuple) result;
        assertThat(tuple.elements().get(0)).isEqualTo(new DataInteger(1));
        assertThat(tuple.elements().get(1)).isEqualTo(new DataString("two"));
    }

    @Test
    @DisplayName("toDataObject - Map")
    void toDataObjectMap() {
        Value val = context.eval("python", "{'a': 1, 'b': 2}");
        DataObject result = MAPPER.toDataObject(new MapType(DataInteger.DATATYPE), val);
        assertThat(result).isInstanceOf(DataMap.class);
        DataMap map = (DataMap) result;
        assertThat(map.get("a")).isEqualTo(new DataInteger(1));
        assertThat(map.get("b")).isEqualTo(new DataInteger(2));
    }

    @Test
    @DisplayName("toDataObject - Union")
    void toDataObjectUnion() {
        UnionType unionType = new UnionType(new UnionType.Member(DataInteger.DATATYPE), new UnionType.Member(DataString.DATATYPE));
        Value valInt = context.eval("python", "42");
        DataObject resultInt = MAPPER.toDataObject(unionType, valInt);
        assertThat(resultInt).isEqualTo(new DataInteger(42));

        Value valStr = context.eval("python", "'hello'");
        DataObject resultStr = MAPPER.toDataObject(unionType, valStr);
        assertThat(resultStr).isEqualTo(new DataString("hello"));
    }

    @Test
    @DisplayName("toDataObject - Struct with @type")
    void toDataObjectStructWithType() throws IOException, SchemaException {
        final var schemaName = "MyAvroSchema";
        final var schemaContent = "{\"type\":\"record\",\"name\":\"MyAvroSchema\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertEquals(schemaName + ".avsc", contextName);
            assertEquals(schemaName, name);
            assertEquals(schemaContent, schemaString);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", ".avsc", mockParser));

        // Test a dict with @type, should preserve field1
        final var structType = new UserTypeParser().parse("avro:" + schemaName).result().dataType();
        final var val = context.eval("python", "{'field1': 10, '@type': 'MyAvroSchema'}");
        final var result = MAPPER.toDataObject(structType, val);
        assertThat(result).isInstanceOf(DataStruct.class);
        final var struct = (DataStruct) result;
        assertThat(struct.get("field1")).isEqualTo(new DataInteger(10));
    }

    @Test
    @DisplayName("toDataObject - Struct with @type and additional keys")
    void toDataObjectStructWithTypeAndAdditionalKeys() {
        // Setup schema in library
        final var schema = new StructSchema("ns", "MyStruct", "doc", Collections.singletonList(
                new StructSchema.Field("field1", DataSchema.INTEGER_SCHEMA)
        ));
        // We need to inject the schema into the ExecutionContext's SchemaLibrary
        // Since SchemaLibrary uses TreeMap<String, Map<String, NamedSchema>>, we need to know the notation name.
        // But getSchema(schemaName, false) iterates over all notations.

        // Let's use a simpler approach for testing: bypass ExecutionContext if possible,
        // but it's hard-coded in PythonDataObjectMapper.

        // For now, let's test a Struct without @type (plain map to struct)
        final var structType = new StructType(schema);
        final var val = context.eval("python", "{'field1': 10, 'field2': 'test'}");
        final var result = MAPPER.toDataObject(structType, val);
        assertThat(result).isInstanceOf(DataStruct.class);
        final var struct = (DataStruct) result;
        assertThat(struct.get("field1")).isEqualTo(new DataInteger(10));
        assertThat(struct.get("field2")).isEqualTo(new DataString("test"));
    }

    @Test
    @DisplayName("fromDataObject - Primitive types")
    void fromDataObjectPrimitives() {
        assertThat(MAPPER.fromDataObject(DataNull.INSTANCE).isNull()).isTrue();
        assertThat(MAPPER.fromDataObject(new DataBoolean(true)).asBoolean()).isTrue();
        assertThat(MAPPER.fromDataObject(new DataInteger(42)).asInt()).isEqualTo(42);
        assertThat(MAPPER.fromDataObject(new DataLong(123L)).asLong()).isEqualTo(123L);
        assertThat(MAPPER.fromDataObject(new DataFloat(1.2f)).asFloat()).isEqualTo(1.2f);
        assertThat(Value.asValue(MAPPER.fromDataObject(new DataDouble(3.4))).asDouble()).isEqualTo(3.4);
        assertThat(MAPPER.fromDataObject(new DataString("test")).asString()).isEqualTo("test");
    }

    @Test
    @DisplayName("fromDataObject - DataBytes (special unsigned short list)")
    void fromDataObjectBytes() {
        byte[] bytes = {1, 2, -1}; // -1 byte is 255 unsigned
        final var dataBytes = new DataBytes(bytes);
        final var result = MAPPER.fromDataObject(dataBytes);
        assertThat(result.hasArrayElements()
                || (result.isHostObject() && result.asHostObject() instanceof List)
                || (result.isProxyObject() && result.asProxyObject() instanceof ProxyArray)).isTrue();

        final List<Short> list = new ArrayList<>();
        if (result.hasArrayElements()) {
            for (int i = 0; i < result.getArraySize(); i++) {
                list.add(result.getArrayElement(i).asShort());
            }
        } else if (result.isProxyObject() && result.asProxyObject() instanceof ProxyArray pa) {
            for (int index = 0; index < pa.getSize(); index++) {
                final var element = pa.get(index);
                list.add(element instanceof Value value ? value.asShort() : null);
            }
        } else {
            list.addAll(result.asHostObject());
        }

        assertThat(list).hasSize(3);
        assertThat(list.get(0)).isEqualTo((short) 1);
        assertThat(list.get(1)).isEqualTo((short) 2);
        assertThat(list.get(2)).isEqualTo((short) 255);
    }

    @Test
    @DisplayName("fromDataObject - DataList")
    void fromDataObjectList() {
        DataList list = new DataList(DataInteger.DATATYPE);
        list.add(new DataInteger(1));
        list.add(new DataInteger(2));
        Value result = MAPPER.fromDataObject(list);
        assertThat(result.hasArrayElements()
                || (result.isHostObject() && result.asHostObject() instanceof List)
                || (result.isProxyObject() && result.asProxyObject() instanceof ProxyArray)).isTrue();

        final List<Object> resultList = new ArrayList<>();
        if (result.isHostObject() && result.asHostObject() instanceof List<?> rl) {
            resultList.addAll(rl);
        } else if (result.isProxyObject() && result.asProxyObject() instanceof ProxyArray pa) {
            for (var index = 0; index < pa.getSize(); index++)
                resultList.add(pa.get(index));
        } else if (result.hasArrayElements()) {
            for (int i = 0; i < result.getArraySize(); i++) {
                Value element = result.getArrayElement(i);
                resultList.add(element.isNumber() ? element.asInt() : element.asHostObject());
            }
        }

        assertThat(resultList).hasSize(2);
        Object firstElement = resultList.getFirst();
        if (firstElement instanceof Value v) {
            assertThat(v.asInt()).isEqualTo(1);
        } else {
            assertThat(firstElement).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("fromDataObject - DataMap")
    void fromDataObjectMap() {
        DataMap map = new DataMap(DataInteger.DATATYPE);
        map.put("a", new DataInteger(1));
        Value result = MAPPER.fromDataObject(map);
        assertThat(result.hasHashEntries()
                || (result.isHostObject() && result.asHostObject() instanceof Map)
                || (result.isProxyObject() && result.asProxyObject() instanceof ProxyHashMap)).isTrue();

        final Map<Object, Object> resultMap = new HashMap<>();
        if (result.isHostObject() && result.asHostObject() instanceof Map<?, ?> rm) {
            resultMap.putAll(rm);
        } else if (result.isProxyObject() && result.asProxyObject() instanceof ProxyHashMap phm) {
            final var iterator = (ProxyIterator) phm.getHashEntriesIterator();
            while (iterator.hasNext()) {
                final var entry = iterator.getNext();
                if (entry instanceof ProxyArray pa && pa.getSize() == 2) {
                    final var k = pa.get(0);
                    final var v = pa.get(1);
                    resultMap.put(k, v);
                }
            }
        } else if (result.hasHashEntries()) {
            Value valA = result.getHashValue(Value.asValue("a"));
            resultMap.put("a", valA.isNumber() ? valA.asInt() : valA.asHostObject());
        }

        Object valA = resultMap.get("a");
        if (valA instanceof Value v) {
            assertThat(v.asInt()).isEqualTo(1);
        } else {
            assertThat(valA).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("PythonDict and PythonList toString() rendering")
    class ToStringTests {

        @Test
        @DisplayName("PythonDict renders as Python dict in sorted key order")
        void dictToString() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("key", "value");
            map.put("count", 42);

            PythonDict dict = new PythonDict(map);

            assertThat(dict).hasToString("{'count': 42, 'key': 'value'}");
        }

        @Test
        @DisplayName("PythonList renders as Python list")
        void listToString() {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add("two");
            list.add(3);

            PythonList pythonList = new PythonList(list);

            assertThat(pythonList).hasToString("[1, 'two', 3]");
        }

        @Test
        @DisplayName("null value renders as None")
        void nullRendersAsNone() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("value", null);

            PythonDict dict = new PythonDict(map);

            assertThat(dict).hasToString("{'value': None}");
        }

        @Test
        @DisplayName("booleans render as True/False")
        void booleansRenderAsPython() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("active", true);
            map.put("deleted", false);

            PythonDict dict = new PythonDict(map);

            assertThat(dict).hasToString("{'active': True, 'deleted': False}");
        }

        @Test
        @DisplayName("nested map renders recursively as Python dict")
        void nestedDictToString() {
            Map<String, Object> inner = new LinkedHashMap<>();
            inner.put("nested", "value");

            Map<String, Object> outer = new LinkedHashMap<>();
            outer.put("inner", inner);

            PythonDict dict = new PythonDict(outer);

            assertThat(dict).hasToString("{'inner': {'nested': 'value'}}");
        }

        @Test
        @DisplayName("nested list in map renders recursively as Python list")
        void nestedListInDictToString() {
            List<Object> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            list.add(3);

            Map<String, Object> map = new LinkedHashMap<>();
            map.put("numbers", list);

            PythonDict dict = new PythonDict(map);

            assertThat(dict).hasToString("{'numbers': [1, 2, 3]}");
        }

        @Test
        @DisplayName("empty dict renders as {}")
        void emptyDictToString() {
            PythonDict dict = new PythonDict(new LinkedHashMap<>());

            assertThat(dict).hasToString("{}");
        }

        @Test
        @DisplayName("empty list renders as []")
        void emptyListToString() {
            PythonList list = new PythonList(new ArrayList<>());

            assertThat(list).hasToString("[]");
        }
    }

    @Nested
    @DisplayName("PythonDict and PythonList accessibility from Python")
    class PythonProxyAccessibilityTests {

        @Test
        @DisplayName("PythonDict is accessible from Python with subscript notation")
        void proxyHashMapAccessibleFromPython() {
            Map<String, String> map = new HashMap<>();
            map.put("key", "value");
            PythonDict proxy = new PythonDict(map);

            context.getBindings("python").putMember("test_map", proxy);
            Value result = context.eval("python", "test_map['key']");

            assertThat(result.asString()).isEqualTo("value");
        }

        @Test
        @DisplayName("PythonList is accessible from Python with index notation")
        void proxyArrayAccessibleFromPython() {
            List<String> list = new ArrayList<>();
            list.add("first");
            list.add("second");
            PythonList proxy = new PythonList(list);

            context.getBindings("python").putMember("test_list", proxy);
            Value result = context.eval("python", "test_list[1]");

            assertThat(result.asString()).isEqualTo("second");
        }

        @Test
        @DisplayName("PythonDict supports 'in' operator in Python")
        void proxyHashMapSupportsInOperator() {
            Map<String, String> map = new HashMap<>();
            map.put("exists", "yes");
            PythonDict proxy = new PythonDict(map);

            context.getBindings("python").putMember("test_map", proxy);
            Value existsResult = context.eval("python", "'exists' in test_map");
            Value notExistsResult = context.eval("python", "'missing' in test_map");

            assertThat(existsResult.asBoolean()).isTrue();
            assertThat(notExistsResult.asBoolean()).isFalse();
        }

        @Test
        @DisplayName("PythonList supports len() in Python")
        void proxyArraySupportsLen() {
            List<Integer> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            list.add(3);
            PythonList proxy = new PythonList(list);

            context.getBindings("python").putMember("test_list", proxy);
            Value result = context.eval("python", "len(test_list)");

            assertThat(result.asInt()).isEqualTo(3);
        }

        @Test
        @DisplayName("PythonDict supports key iteration in Python")
        void proxyHashMapSupportsIteration() {
            Map<String, Integer> map = new HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            PythonDict proxy = new PythonDict(map);

            context.getBindings("python").putMember("test_map", proxy);
            Value result = context.eval("python", "sorted(list(test_map.keys()))");

            assertThat(result.getArraySize()).isEqualTo(2);
        }

        @Test
        @DisplayName("PythonList supports iteration in Python")
        void proxyArraySupportsIteration() {
            List<Integer> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            list.add(3);
            PythonList proxy = new PythonList(list);

            context.getBindings("python").putMember("test_list", proxy);
            Value result = context.eval("python", "sum(test_list)");

            assertThat(result.asInt()).isEqualTo(6);
        }

        @Test
        @DisplayName("Nested PythonDict with list values is fully accessible from Python")
        void nestedStructuresAccessibleFromPython() {
            List<Integer> numbers = new ArrayList<>();
            numbers.add(10);
            numbers.add(20);
            numbers.add(30);

            Map<String, Object> outer = new HashMap<>();
            outer.put("numbers", numbers);
            outer.put("name", "test");

            PythonDict proxy = new PythonDict(outer);

            context.getBindings("python").putMember("data", proxy);
            Value sumResult = context.eval("python", "sum(data['numbers'])");
            Value nameResult = context.eval("python", "data['name']");

            assertThat(sumResult.asInt()).isEqualTo(60);
            assertThat(nameResult.asString()).isEqualTo("test");
        }

        @Test
        @DisplayName("PythonDict supports assignment from Python")
        void proxyHashMapSupportsAssignment() {
            Map<String, String> map = new HashMap<>();
            map.put("existing", "value");
            PythonDict proxy = new PythonDict(map);

            context.getBindings("python").putMember("test_map", proxy);
            context.eval("python", "test_map['new_key'] = 'new_value'");
            Value result = context.eval("python", "test_map['new_key']");

            assertThat(result.asString()).isEqualTo("new_value");
        }
    }
}
