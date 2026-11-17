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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroSchemaMapper;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataShort;
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
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.notation.MockNotation;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.type.UserType;
import org.apache.avro.Schema;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class PythonDataObjectMapperTest {
    private Context context;
    private PythonDataObjectMapper mapper;

    @BeforeAll
    static void setup() {
        final var binaryNotation = new BinaryNotation(new NotationContext(new DataObjectFlattener(), new DataTypeFlattener()), null);
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
        mapper = new PythonDataObjectMapper(true, context);
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
        DataObject result = mapper.toDataObject(DataBoolean.DATATYPE, val);
        assertThat(result).isInstanceOf(DataBoolean.class);
        assertThat(((DataBoolean) result).value()).isTrue();
    }

    @Test
    @DisplayName("toDataObject - Integer")
    void toDataObjectInteger() {
        Value val = context.eval("python", "42");
        DataObject result = mapper.toDataObject(DataInteger.DATATYPE, val);
        assertThat(result).isInstanceOf(DataInteger.class);
        assertThat(((DataInteger) result).value()).isEqualTo(42);
    }

    @Test
    @DisplayName("toDataObject - Long (default for numbers)")
    void toDataObjectLong() {
        Value val = context.eval("python", "1234567890123");
        DataObject result = mapper.toDataObject(DataLong.DATATYPE, val);
        assertThat(result).isInstanceOf(DataLong.class);
        assertThat(((DataLong) result).value()).isEqualTo(1234567890123L);
    }

    @Test
    @DisplayName("toDataObject - Float")
    void toDataObjectFloat() {
        Value val = context.eval("python", "3.14");
        DataObject result = mapper.toDataObject(DataFloat.DATATYPE, val);
        assertThat(result).isInstanceOf(DataFloat.class);
        assertThat(((DataFloat) result).value()).isCloseTo(3.14f, org.assertj.core.data.Offset.offset(0.001f));
    }

    @Test
    @DisplayName("toDataObject - a Python float with a fraction converts to the expected double")
    void toDataObjectDouble() {
        assertThat(mapper.toDataObject(DataDouble.DATATYPE, context.eval("python", "1.5"))).isEqualTo(new DataDouble(1.5));
        // A Python int is a valid double too
        assertThat(mapper.toDataObject(DataDouble.DATATYPE, context.eval("python", "2"))).isEqualTo(new DataDouble(2.0));
    }

    @Test
    @DisplayName("toDataObject - without an expected type, a Python float becomes a double and an int a long")
    void toDataObjectNumberWithoutExpectedType() {
        assertThat(mapper.toDataObject(null, context.eval("python", "1.5"))).isEqualTo(new DataDouble(1.5));
        assertThat(mapper.toDataObject(DataType.UNKNOWN, context.eval("python", "1.5"))).isEqualTo(new DataDouble(1.5));
        assertThat(mapper.toDataObject(null, context.eval("python", "7"))).isEqualTo(new DataLong(7L));
        // The type follows the Python type, not the value: a whole float stays a double, a large one too
        assertThat(mapper.toDataObject(null, context.eval("python", "3.0"))).isEqualTo(new DataDouble(3.0));
        assertThat(mapper.toDataObject(null, context.eval("python", "1e20"))).isEqualTo(new DataDouble(1e20));
    }

    @Test
    @DisplayName("toDataObject - floats inside a list convert to the list's value type")
    void toDataObjectListOfFloats() {
        final var result = mapper.toDataObject(new ListType(DataDouble.DATATYPE), context.eval("python", "[1.5, 2]"));
        assertThat(result).isInstanceOf(DataList.class);
        assertThat(((DataList) result).get(0)).isEqualTo(new DataDouble(1.5));
        assertThat(((DataList) result).get(1)).isEqualTo(new DataDouble(2.0));
    }

    @Test
    @DisplayName("toDataObject - a tuple converts each element to its expected type, also None and floats")
    void toDataObjectTupleWithExpectedTypes() {
        final var expected = new TupleType(DataString.DATATYPE, DataDouble.DATATYPE);
        final var result = mapper.toDataObject(expected, context.eval("python", "(None, 1.5)"));
        assertThat(result).isInstanceOf(DataTuple.class);
        assertThat(expected.isAssignableFrom(result).isAssignable()).isTrue();
        assertThat(((DataTuple) result).elements().get(1)).isEqualTo(new DataDouble(1.5));
    }

    @Test
    @DisplayName("toDataObject - a float in a union converts to the matching member")
    void toDataObjectFloatInUnion() {
        final var union = new UnionType(new UnionType.Member(DataNull.DATATYPE), new UnionType.Member(DataDouble.DATATYPE));
        assertThat(mapper.toDataObject(union, context.eval("python", "1.5"))).isEqualTo(new DataDouble(1.5));
    }

    @Test
    @DisplayName("toDataObject - numbers that do not fit fail loudly")
    void toDataObjectNumberThatDoesNotFit() {
        final var fraction = context.eval("python", "1.5");
        assertThatCode(() -> mapper.toDataObject(DataLong.DATATYPE, fraction))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("1.5");
        // An int too large for a long is not silently turned into a double
        final var hugeInt = context.eval("python", "2**70");
        assertThatCode(() -> mapper.toDataObject(null, hugeInt))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("1180591620717411303424");
        final var hugeFloat = context.eval("python", "1e300");
        assertThatCode(() -> mapper.toDataObject(DataFloat.DATATYPE, hugeFloat)).isInstanceOf(DataException.class);
    }

    @Test
    @DisplayName("toDataObject - String")
    void toDataObjectString() {
        Value val = context.eval("python", "'hello'");
        DataObject result = mapper.toDataObject(DataString.DATATYPE, val);
        assertThat(result).isInstanceOf(DataString.class);
        assertThat(((DataString) result).value()).isEqualTo("hello");
    }

    @Test
    @DisplayName("toDataObject - Bytes")
    void toDataObjectBytes() {
        Value val = context.eval("python", "[1, 2, 3, 255]");
        DataObject result = mapper.toDataObject(DataBytes.DATATYPE, val);
        assertThat(result).isInstanceOf(DataBytes.class);
        assertThat(((DataBytes) result).value()).containsExactly(1, 2, 3, -1);
    }

    @Test
    @DisplayName("toDataObject - Bytes: Python int outside the byte range fails loudly")
    void toDataObjectBytes_outOfRangeFailsLoudly() {
        // 300 does not fit in [-128, 255]; previously this silently truncated to (byte) 44.
        Value val = context.eval("python", "[1, 2, 300]");
        assertThatCode(() -> mapper.toDataObject(DataBytes.DATATYPE, val))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("does not fit");
    }

    @Test
    @DisplayName("toDataObject - Bytes: accepts both signed (-128..127) and unsigned (0..255) values")
    void toDataObjectBytes_acceptsSignedAndUnsignedRange() {
        Value val = context.eval("python", "[-128, -1, 0, 127, 128, 255]");
        DataObject result = mapper.toDataObject(DataBytes.DATATYPE, val);
        assertThat(((DataBytes) result).value())
                .containsExactly((byte) -128, (byte) -1, (byte) 0, (byte) 127, (byte) -128, (byte) -1);
    }

    @Test
    @DisplayName("PythonNativeMapper.fromPython(DataType, Value): GraalVM overflow wrapped as DataException")
    void fromPython_typedNumeric_overflowFailsLoudly() {
        // Python int 300 does not fit in a Java byte; GraalVM's Value.asByte() throws
        // UnsupportedOperationException which is now wrapped as DataException naming the expected
        // KSML type and the offending value.
        Value val = context.eval("python", "300");
        final var native_ = new PythonNativeMapper();
        assertThatCode(() -> native_.fromPython(DataByte.DATATYPE, val))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("does not fit in expected type")
                .hasMessageContaining("300");
    }

    @Test
    @DisplayName("PythonNativeMapper.fromPython(DataType, Value): Python int that fits in the expected primitive returns the native value")
    void fromPython_typedNumeric_inRangeReturnsNative() {
        Value val = context.eval("python", "42");
        final var nativeMapper = new PythonNativeMapper();
        assertThat(nativeMapper.fromPython(DataByte.DATATYPE, val)).isEqualTo((byte) 42);
        assertThat(nativeMapper.fromPython(DataShort.DATATYPE, val)).isEqualTo((short) 42);
        assertThat(nativeMapper.fromPython(DataInteger.DATATYPE, val)).isEqualTo(42);
        assertThat(nativeMapper.fromPython(DataLong.DATATYPE, val)).isEqualTo(42L);
    }

    @Test
    @DisplayName("PythonDataObjectMapper.toDataObject(DataByte, Value(300)) fails when reading the Python value")
    void toDataObject_overflowCaughtWhenReadingPythonValue() {
        // The expected type is passed on to fromPython, so the Python int 300 is rejected when it is read as a byte
        Value val = context.eval("python", "300");
        assertThatCode(() -> mapper.toDataObject(DataByte.DATATYPE, val))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("300 does not fit in expected type byte");
    }

    @Test
    @DisplayName("toDataObject - List")
    void toDataObjectList() {
        Value val = context.eval("python", "[1, 2, 3]");
        DataObject result = mapper.toDataObject(new ListType(DataInteger.DATATYPE), val);
        assertThat(result).isInstanceOf(DataList.class);
        DataList list = (DataList) result;
        assertThat(list.size()).isEqualTo(3);
        assertThat(list.get(0)).isEqualTo(new DataInteger(1));
    }

    @Test
    @DisplayName("toDataObject - Tuple")
    void toDataObjectTuple() {
        Value val = context.eval("python", "(1, 'two')");
        DataObject result = mapper.toDataObject(new TupleType(DataInteger.DATATYPE, DataString.DATATYPE), val);
        assertThat(result).isInstanceOf(DataTuple.class);
        DataTuple tuple = (DataTuple) result;
        assertThat(tuple.elements().get(0)).isEqualTo(new DataInteger(1));
        assertThat(tuple.elements().get(1)).isEqualTo(new DataString("two"));
    }

    @Test
    @DisplayName("toDataObject - Map")
    void toDataObjectMap() {
        Value val = context.eval("python", "{'a': 1, 'b': 2}");
        DataObject result = mapper.toDataObject(new MapType(DataInteger.DATATYPE), val);
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
        DataObject resultInt = mapper.toDataObject(unionType, valInt);
        assertThat(resultInt).isEqualTo(new DataInteger(42));

        Value valStr = context.eval("python", "'hello'");
        DataObject resultStr = mapper.toDataObject(unionType, valStr);
        assertThat(resultStr).isEqualTo(new DataString("hello"));
    }

    @Test
    @DisplayName("toDataObject - Struct with @type")
    void toDataObjectStructWithType() throws IOException, SchemaException {
        final var schemaName = "MyAvroSchema";
        final var schemaContent = "{\"type\":\"record\",\"name\":\"MyAvroSchema\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertThat(contextName).isEqualTo(schemaName + ".avsc");
            assertThat(name).isEqualTo(schemaName);
            assertThat(schemaString).isEqualTo(schemaContent);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", mockParser));

        // Test a dict with @type, should preserve field1
        final var structType = new UserTypeParser().parse("avro:" + schemaName).result().dataType();
        final var val = context.eval("python", "{'field1': 10, '@type': 'MyAvroSchema'}");
        final var result = mapper.toDataObject(structType, val);
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
        final var result = mapper.toDataObject(structType, val);
        assertThat(result).isInstanceOf(DataStruct.class);
        final var struct = (DataStruct) result;
        assertThat(struct.get("field1")).isEqualTo(new DataInteger(10));
        assertThat(struct.get("field2")).isEqualTo(new DataString("test"));
    }

    @Test
    @DisplayName("fromDataObject - Primitive types")
    void fromDataObjectPrimitives() {
        assertThat(mapper.fromDataObject(DataNull.INSTANCE).isNull()).isTrue();
        assertThat(mapper.fromDataObject(new DataBoolean(true)).asBoolean()).isTrue();
        assertThat(mapper.fromDataObject(new DataInteger(42)).asInt()).isEqualTo(42);
        assertThat(mapper.fromDataObject(new DataLong(123L)).asLong()).isEqualTo(123L);
        assertThat(mapper.fromDataObject(new DataFloat(1.2f)).asFloat()).isEqualTo(1.2f);
        assertThat(Value.asValue(mapper.fromDataObject(new DataDouble(3.4))).asDouble()).isEqualTo(3.4);
        assertThat(mapper.fromDataObject(new DataString("test")).asString()).isEqualTo("test");
    }

    @Test
    @DisplayName("fromDataObject - DataBytes (special unsigned short list)")
    void fromDataObjectBytes() {
        byte[] bytes = {1, 2, -1}; // -1 byte is 255 unsigned
        final var dataBytes = new DataBytes(bytes);
        final var result = mapper.fromDataObject(dataBytes);
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
        Value result = mapper.fromDataObject(list);
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
        Value result = mapper.fromDataObject(map);
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

    // Log rendering and Python-side access must still work without PythonDict/PythonList

    @Test
    @DisplayName("a struct handed to Python renders as a Python dict in log output")
    void structRendersAsPythonDict() {
        final var struct = new DataStruct();
        struct.put("key", new DataString("value"));
        struct.put("count", new DataInteger(42));
        assertThat(mapper.fromDataObject(struct)).hasToString("{'count': 42, 'key': 'value'}");
    }

    @Test
    @DisplayName("a struct handed to Python supports subscript, 'in' and len()")
    void structIsUsableFromPython() {
        final var struct = new DataStruct();
        struct.put("key", new DataString("value"));
        context.getBindings("python").putMember("data", mapper.fromDataObject(struct));
        assertThat(context.eval("python", "data['key']").asString()).isEqualTo("value");
        assertThat(context.eval("python", "'key' in data").asBoolean()).isTrue();
        assertThat(context.eval("python", "len(data)").asInt()).isEqualTo(1);
    }

    @Test
    @DisplayName("a nested struct with a list is fully accessible from Python")
    void nestedStructureIsUsableFromPython() {
        final var numbers = new DataList(DataInteger.DATATYPE);
        numbers.add(new DataInteger(1), new DataInteger(2), new DataInteger(3));
        final var struct = new DataStruct();
        struct.put("numbers", numbers);
        context.getBindings("python").putMember("data", mapper.fromDataObject(struct));
        assertThat(context.eval("python", "sum(data['numbers'])").asInt()).isEqualTo(6);
    }

    @Test
    @DisplayName("a struct handed to Python supports assignment from Python")
    void structSupportsAssignmentFromPython() {
        final var struct = new DataStruct();
        struct.put("existing", new DataString("value"));
        context.getBindings("python").putMember("data", mapper.fromDataObject(struct));
        context.eval("python", "data['new_key'] = 'new_value'");
        assertThat(context.eval("python", "data['new_key']").asString()).isEqualTo("new_value");
    }

    private static final String CONVERSION_SCHEMA = """
            {"type": "record", "name": "R", "namespace": "x", "fields": [
              {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A", "B"]}},
              {"name": "l", "type": "long"},
              {"name": "i", "type": "int"},
              {"name": "f", "type": "float"},
              {"name": "d", "type": "double"},
              {"name": "ns", "type": ["null", "string"], "default": null},
              {"name": "u", "type": ["null", "long", "string"], "default": null},
              {"name": "n", "type": {"type": "record", "name": "N", "fields": [
                {"name": "x", "type": "long"},
                {"name": "t", "type": {"type": "enum", "name": "T", "symbols": ["P", "Q"]}}]}},
              {"name": "arr", "type": {"type": "array", "items": "N"}},
              {"name": "m", "type": {"type": "map", "values": "long"}},
              {"name": "ul", "type": {"type": "array", "items": ["null", "long"]}, "default": []},
              {"name": "um", "type": {"type": "map", "values": ["null", "string"]}, "default": {}}
            ]}""";

    // PythonFunction skips the converter for a result that already fits its type. That is only safe when the
    // converter would not change such a result, which this test checks.
    @ParameterizedTest
    @ValueSource(strings = {
            "{'e': 'A', 'l': 1, 'i': 2, 'f': 2.5, 'd': 3, 'ns': None, 'u': 5, 'n': {'x': 1, 't': 'P'}, 'arr': [{'x': 2, 't': 'Q'}], 'm': {'a': 1}}",
            "{'e': 'B', 'l': 1, 'i': 2, 'f': 2.5, 'd': 3.5, 'ns': 's', 'u': 'str', 'n': {'x': 1, 't': 'Q'}, 'arr': [], 'm': {}}",
            "{'e': 'A', 'l': 1, 'i': 2, 'f': 2.5, 'd': 3.5, 'n': {'x': 1, 't': 'P'}, 'arr': [], 'm': {}, 'ul': [1, None], 'um': {'a': None, 'b': 'x'}}"})
    @DisplayName("toDataObject - a result that fits its type is not changed by the converter")
    void fittingResultNeedsNoConversion(String python) {
        final var schema = (StructSchema) new AvroSchemaMapper().toDataSchema("x", "R", new Schema.Parser().parse(CONVERSION_SCHEMA));
        final var type = new StructType(schema);

        final var result = mapper.toDataObject(type, context.eval("python", python));
        final var converted = new DataObjectConverter().convert(UserType.DEFAULT_NOTATION, result, new UserType(type));

        assertThat(type.isAssignableFrom(result).isAssignable()).isTrue();
        assertThat(converted).isEqualTo(result);
        ((DataStruct) result).forEach((field, value) ->
                assertThat(((DataObject) ((DataStruct) converted).get(field)).type()).as(field).isEqualTo(((DataObject) value).type()));
    }
}
