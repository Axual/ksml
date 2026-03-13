package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserTypeParserTest {
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

    @ParameterizedTest
    @DisplayName("Test all known types")
    @ValueSource(strings = {"boolean", "byte", "bytes", "short", "double", "float", "int", "long", "?", "none", "str", "string"})
    void testParseValidTypes(String type) {
        var userType = new UserTypeParser().parse(type);
        assertTrue(userType.isOk());
        assertNotNull(userType);
        assertEquals(UserType.DEFAULT_NOTATION, userType.result().notation(), "notation for " + type + "should default to " + UserType.DEFAULT_NOTATION);
    }

    @ParameterizedTest
    @DisplayName("Test parsing for dataType String (types 'str' and 'string'")
    @ValueSource(strings = {"str", "string"})
    void testParseStringType(String type) {
        final var userType = new UserTypeParser().parse(type).result();
        assertNotNull(userType);
        final var dataType = userType.dataType();
        assertEquals(String.class, dataType.containerClass());
        assertTrue(dataType.isAssignableFrom("some random string").isAssignable());
        assertTrue(dataType.isAssignableFrom(String.class).isAssignable());
    }

    @ParameterizedTest
    @DisplayName("Test mapping of dataType names to correct user types class")
    @MethodSource("typesAndDataTypes")
    void testDataTypes(String type, DataType dataType) {
        final var userType = new UserTypeParser().parse(type).result();
        assertNotNull(userType);

        assertEquals(dataType, userType.dataType(), "DataType for '" + type + "' should be set to " + dataType);
        if (type.equals("?")) {
            assertEquals(DataType.UNKNOWN, userType.dataType(), "Datatype for '?' should be UNKNOWN (anonymous subclass)");
        } else {
            assertTrue(SimpleType.class.isAssignableFrom(userType.dataType().getClass()), "Class for " + type + " should be subclass of SimpleType");
        }
    }

    static Stream<Arguments> typesAndDataTypes() {
        return Stream.of(
                Arguments.arguments("boolean", DataBoolean.DATATYPE),
                Arguments.arguments("byte", DataByte.DATATYPE),
                Arguments.arguments("short", DataShort.DATATYPE),
                Arguments.arguments("int", DataInteger.DATATYPE),
                Arguments.arguments("long", DataLong.DATATYPE),
                Arguments.arguments("double", DataDouble.DATATYPE),
                Arguments.arguments("float", DataFloat.DATATYPE),
                Arguments.arguments("bytes", DataBytes.DATATYPE),
                Arguments.arguments("str", DataString.DATATYPE),
                Arguments.arguments("string", DataString.DATATYPE),
                Arguments.arguments("none", DataNull.DATATYPE),
                Arguments.arguments("?", DataType.UNKNOWN)
        );
    }

    @ParameterizedTest
    @DisplayName("List parsing: [T] and list(T)")
    @ValueSource(strings = {"[int]", "list(int)"})
    void testListTypes(String type) {
        final var ut = new UserTypeParser().parse(type);
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        final var dt = ut.result().dataType();
        assertInstanceOf(ListType.class, dt);
        assertEquals(DataInteger.DATATYPE, ((ListType) dt).valueType());
    }

    @Test
    @DisplayName("Map parsing: map(T)")
    void testMapType() {
        final var ut = new UserTypeParser().parse("map(string)");
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        final var dt = ut.result().dataType();
        assertInstanceOf(MapType.class, dt);
        assertEquals(DataString.DATATYPE, ((MapType) dt).valueType());
    }

    @ParameterizedTest
    @DisplayName("Enum parsing with/without quotes")
    @ValueSource(strings = {"enum(A,B)"})
    void testEnumTypes(String type) {
        final var ut = new UserTypeParser().parse(type);
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        final var dt = ut.result().dataType();
        assertInstanceOf(EnumType.class, dt);
        final var enumType = (EnumType) dt;
        assertEquals(2, enumType.schema().symbols().size());
        assertEquals("A", enumType.schema().symbols().get(0).name());
        assertEquals("B", enumType.schema().symbols().get(1).name());
    }

    @Test
    @DisplayName("Union parsing: union(T1, T2)")
    void testUnionType() {
        final var ut = new UserTypeParser().parse("union(int, string)");
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        final var dt = ut.result().dataType();
        assertInstanceOf(UnionType.class, dt);
        final var u = (UnionType) dt;
        assertEquals(2, u.subTypeCount());
        assertEquals(DataInteger.DATATYPE, u.subType(0));
        assertEquals(DataString.DATATYPE, u.subType(1));
    }

    @ParameterizedTest
    @DisplayName("Tuple parsing: (T1,T2) and tuple(T1,T2)")
    @ValueSource(strings = {"(int, string)", "tuple(int, string)"})
    void testTupleTypes(String type) {
        final var ut = new UserTypeParser().parse(type);
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        final var dt = ut.result().dataType();
        assertInstanceOf(TupleType.class, dt);
        final var t = (TupleType) dt;
        assertEquals(2, t.subTypeCount());
        assertEquals(DataInteger.DATATYPE, t.subType(0));
        assertEquals(DataString.DATATYPE, t.subType(1));
    }

    @Test
    @DisplayName("Windowed parsing: windowed(T)")
    void testWindowedType() {
        final var ut = new UserTypeParser().parse("windowed(string)");
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        final var dt = ut.result().dataType();
        assertInstanceOf(WindowedType.class, dt);
        final var w = (WindowedType) dt;
        assertEquals(DataString.DATATYPE, w.keyType());
    }

    @Test
    @DisplayName("Nested types parsing: list(map(string))")
    void testNestedTypes() {
        final var ut = new UserTypeParser().parse("list(map(string))");
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        final var dt = ut.result().dataType();
        assertInstanceOf(ListType.class, dt);
        final var lt = (ListType) dt;
        assertInstanceOf(MapType.class, lt.valueType());
        assertEquals(DataString.DATATYPE, ((MapType) lt.valueType()).valueType());
    }

    @Test
    @DisplayName("Notation only returns default type")
    void testNotationOnly() {
        final var ut = new UserTypeParser().parse(UserType.DEFAULT_NOTATION);
        assertTrue(ut.isOk(), ut.isError() ? ut.errorMessage() : "");
        assertEquals(BinaryNotation.NOTATION_NAME, ut.result().notation());
        // Parser should not error and returns the concrete notation name
        assertNotNull(ut.result());
    }

    @ParameterizedTest
    @DisplayName("Error cases for unclosed constructs")
    @ValueSource(strings = {"[int", "list(int", "map(int", "enum(A,B", "union(int,string", "(int,string", "tuple(int,string", "windowed(string"})
    void testUnclosedErrors(String type) {
        final var ut = new UserTypeParser().parse(type);
        assertTrue(ut.isError());
        assertNotNull(ut.errorMessage());
    }

    @Test
    @DisplayName("Null input yields UNKNOWN user type")
    void testNullInput() {
        final var ut = new UserTypeParser().parse(null);
        assertTrue(ut.isOk());
        assertEquals(DataType.UNKNOWN, ut.result().dataType());
    }


    private static class MockNotation implements Notation {
        private final String name;
        private final String extension;
        private final SchemaParser schemaParser;

        public MockNotation(String name, String extension, SchemaParser schemaParser) {
            this.name = name;
            this.extension = extension;
            this.schemaParser = schemaParser;
        }

        @Override
        public DataType defaultType() {
            return new StructType();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String filenameExtension() {
            return extension;
        }

        @Override
        public Serde<Object> serde(DataType type, boolean isKey) {
            return null;
        }

        @Override
        public Converter converter() {
            return null;
        }

        @Override
        public SchemaParser schemaParser() {
            return schemaParser;
        }
    }

    @Test
    @DisplayName("Test AVRO schema loading from disk")
    void testAvroSchemaLoading() throws IOException {
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

        final var userType = new UserTypeParser().parse("avro:" + schemaName);
        assertTrue(userType.isOk());
        assertEquals("avro", userType.result().notation());
        assertInstanceOf(StructType.class, userType.result().dataType());
    }

    @Test
    @DisplayName("Test JSONSCHEMA schema loading from disk")
    void testJsonSchemaLoading() throws IOException {
        final var schemaName = "MyJsonSchema";
        final var schemaContent = "{}";
        Files.writeString(tempDir.resolve(schemaName + ".json"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertEquals(schemaName + ".json", contextName);
            assertEquals(schemaName, name);
            assertEquals(schemaContent, schemaString);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("jsonschema", new MockNotation("jsonschema", ".json", mockParser));

        final var userType = new UserTypeParser().parse("jsonschema:" + schemaName);
        assertTrue(userType.isOk());
        assertEquals("jsonschema", userType.result().notation());
        assertInstanceOf(StructType.class, userType.result().dataType());
    }

    @Test
    @DisplayName("Test PROTOBUF schema loading from disk")
    void testProtobufSchemaLoading() throws IOException {
        final var schemaName = "MyProtoSchema";
        final var schemaContent = "syntax = \"proto3\";";
        Files.writeString(tempDir.resolve(schemaName + ".proto"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertEquals(schemaName + ".proto", contextName);
            assertEquals(schemaName, name);
            assertEquals(schemaContent, schemaString);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("protobuf", new MockNotation("protobuf", ".proto", mockParser));

        final var userType = new UserTypeParser().parse("protobuf:" + schemaName);
        assertTrue(userType.isOk());
        assertEquals("protobuf", userType.result().notation());
        assertInstanceOf(StructType.class, userType.result().dataType());
    }

    @Test
    @DisplayName("Test schema loading error: missing file")
    void testMissingSchemaFile() {
        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", ".avsc", (c, n, s) -> null));

        // When schema is not found, SchemaLibrary.getSchema returns null because it returns null
        // when loader.load returns null.
        // Then UserTypeParser.parseNotationWithOrWithoutSchema calls
        // new DataTypeDataSchemaMapper().fromDataSchema(null) which returns DataType.UNKNOWN.
        // Finally, it tries to check assignability: not.defaultType().isAssignableFrom(DataType.UNKNOWN)
        // Since DataType.UNKNOWN is a wildcard that's NOT a ComplexType, StructType.isAssignableFrom(UNKNOWN)
        // returns typeMismatch error in ComplexType.isAssignableFrom.

        final var exception = assertThrows(SchemaException.class, () -> new UserTypeParser().parse("avro:MissingSchema"));

        String expectedMessage = "Can not load schema";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @DisplayName("Test avro:windowed(SomeSchema) uses avro notation for SomeSchema")
    void testAvroWindowedSomeSchema() throws IOException {
        final var schemaName = "SomeSchema";
        final var schemaContent = "{\"type\":\"record\",\"name\":\"SomeSchema\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertEquals(schemaName + ".avsc", contextName);
            assertEquals(schemaName, name);
            assertEquals(schemaContent, schemaString);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", ".avsc", mockParser));

        final var userType = new UserTypeParser().parse("avro:windowed(" + schemaName + ")");
        assertTrue(userType.isOk(), userType.isError() ? userType.errorMessage() : "");
        assertEquals("avro", userType.result().notation());
        assertInstanceOf(WindowedType.class, userType.result().dataType());
        final var windowedType = (WindowedType) userType.result().dataType();
        assertInstanceOf(StructType.class, windowedType.keyType());
    }

    @Test
    @DisplayName("Test schema loading error: missing file")
    void testAvroWindowedMissingSchemaFile() {
        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", ".avsc", (c, n, s) -> null));

        // When schema is not found, SchemaLibrary.getSchema returns null because it returns null
        // when loader.load returns null.
        // Then UserTypeParser.parseNotationWithOrWithoutSchema calls
        // new DataTypeDataSchemaMapper().fromDataSchema(null) which returns DataType.UNKNOWN.
        // Finally, it tries to check assignability: not.defaultType().isAssignableFrom(DataType.UNKNOWN)
        // Since DataType.UNKNOWN is a wildcard that's NOT a ComplexType, StructType.isAssignableFrom(UNKNOWN)
        // returns typeMismatch error in ComplexType.isAssignableFrom.

        final var exception = assertThrows(SchemaException.class, () -> new UserTypeParser().parse("avro:windowed(MissingSchema)"));

        String expectedMessage = "Can not load schema";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @DisplayName("Test schema loading windowed default type")
    void testAvroWindowedStandardType() {
        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", ".avsc", (c, n, s) -> null));

        // When schema is not found, SchemaLibrary.getSchema returns null because it returns null
        // when loader.load returns null.
        // Then UserTypeParser.parseNotationWithOrWithoutSchema calls
        // new DataTypeDataSchemaMapper().fromDataSchema(null) which returns DataType.UNKNOWN.
        // Finally, it tries to check assignability: not.defaultType().isAssignableFrom(DataType.UNKNOWN)
        // Since DataType.UNKNOWN is a wildcard that's NOT a ComplexType, StructType.isAssignableFrom(UNKNOWN)
        // returns typeMismatch error in ComplexType.isAssignableFrom.

        final var userType = new UserTypeParser().parse("avro:windowed(struct)");
        assertTrue(userType.isOk());
        assertEquals("avro", userType.result().notation());
        assertInstanceOf(WindowedType.class, userType.result().dataType());
        final var windowedType = (WindowedType) userType.result().dataType();
        assertInstanceOf(StructType.class, windowedType.keyType());
    }

    @Test
    @DisplayName("Test avro:[SomeSchema] uses avro notation for SomeSchema")
    void testAvroListSomeSchema() throws IOException {
        final var schemaName = "SomeSchemaList";
        final var schemaContent = "{\"type\":\"record\",\"name\":\"SomeSchemaList\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertEquals(schemaName + ".avsc", contextName);
            assertEquals(schemaName, name);
            assertEquals(schemaContent, schemaString);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", ".avsc", mockParser));

        final var userType = new UserTypeParser().parse("avro:[" + schemaName + "]");
        assertTrue(userType.isOk(), userType.isError() ? userType.errorMessage() : "");
        assertEquals("avro", userType.result().notation());
        assertInstanceOf(ListType.class, userType.result().dataType());
        final var listType = (ListType) userType.result().dataType();
        assertInstanceOf(StructType.class, listType.valueType());
    }
}
