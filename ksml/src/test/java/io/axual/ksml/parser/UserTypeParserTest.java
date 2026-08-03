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
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UnresolvedType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.notation.MockNotation;
import io.axual.ksml.type.UserType;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserTypeParserTest {
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

    private Parsed<UserType> parse(String userType) {
        return new UserTypeParser().parse(userType, true);
    }

    @ParameterizedTest
    @DisplayName("Test all known types")
    @ValueSource(strings = {"boolean", "byte", "bytes", "short", "double", "float", "int", "long", "?", "none", "str", "string"})
    void testParseValidTypes(String type) {
        var userType = parse(type);
        assertThat(userType.isOk()).isTrue();
        assertThat(userType).isNotNull();
        assertThat(userType.result().notation()).as("notation for " + type + "should default to " + UserType.DEFAULT_NOTATION).isEqualTo(UserType.DEFAULT_NOTATION);
    }

    @ParameterizedTest
    @DisplayName("Test parsing for dataType String (types 'str' and 'string'")
    @ValueSource(strings = {"str", "string"})
    void testParseStringType(String type) {
        final var userType = parse(type).result();
        assertThat(userType).isNotNull();
        final var dataType = userType.dataType();
        assertThat(dataType.containerClass()).isEqualTo(String.class);
        assertThat(dataType.isAssignableFrom("some random string").isAssignable()).isTrue();
        assertThat(dataType.isAssignableFrom(String.class).isAssignable()).isTrue();
    }

    @ParameterizedTest
    @DisplayName("Test mapping of dataType names to correct user types class")
    @MethodSource("typesAndDataTypes")
    void testDataTypes(String type, DataType dataType) {
        final var userType = parse(type).result();
        assertThat(userType).isNotNull();

        assertThat(userType.dataType()).as("DataType for '" + type + "' should be set to " + dataType).isEqualTo(dataType);
        if (type.equals("?")) {
            assertThat(userType.dataType()).as("Datatype for '?' should be UNKNOWN (anonymous subclass)").isEqualTo(DataType.UNKNOWN);
        } else {
            assertThat(SimpleType.class.isAssignableFrom(userType.dataType().getClass())).as("Class for " + type + " should be subclass of SimpleType").isTrue();
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
        final var ut = parse(type);
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        final var dt = ut.result().dataType();
        assertThat(dt).isInstanceOf(ListType.class);
        assertThat(((ListType) dt).valueType()).isEqualTo(DataInteger.DATATYPE);
    }

    @Test
    @DisplayName("Map parsing: map(T)")
    void testMapType() {
        final var ut = parse("map(string)");
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        final var dt = ut.result().dataType();
        assertThat(dt).isInstanceOf(MapType.class);
        assertThat(((MapType) dt).valueType()).isEqualTo(DataString.DATATYPE);
    }

    @ParameterizedTest
    @DisplayName("Enum parsing with/without quotes")
    @ValueSource(strings = {"enum(A,B)"})
    void testEnumTypes(String type) {
        final var ut = parse(type);
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        final var dt = ut.result().dataType();
        assertThat(dt).isInstanceOf(EnumType.class);
        final var enumType = (EnumType) dt;
        assertThat(enumType.schema().symbols().size()).isEqualTo(2);
        assertThat(enumType.schema().symbols().get(0).name()).isEqualTo("A");
        assertThat(enumType.schema().symbols().get(1).name()).isEqualTo("B");
    }

    @Test
    @DisplayName("Union parsing: union(T1, T2)")
    void testUnionType() {
        final var ut = parse("union(int, string)");
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        final var dt = ut.result().dataType();
        assertThat(dt).isInstanceOf(UnionType.class);
        final var u = (UnionType) dt;
        assertThat(u.subTypeCount()).isEqualTo(2);
        assertThat(u.subType(0)).isEqualTo(DataInteger.DATATYPE);
        assertThat(u.subType(1)).isEqualTo(DataString.DATATYPE);
    }

    @ParameterizedTest
    @DisplayName("Tuple parsing: (T1,T2) and tuple(T1,T2)")
    @ValueSource(strings = {"(int, string)", "tuple(int, string)"})
    void testTupleTypes(String type) {
        final var ut = parse(type);
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        final var dt = ut.result().dataType();
        assertThat(dt).isInstanceOf(TupleType.class);
        final var t = (TupleType) dt;
        assertThat(t.subTypeCount()).isEqualTo(2);
        assertThat(t.subType(0)).isEqualTo(DataInteger.DATATYPE);
        assertThat(t.subType(1)).isEqualTo(DataString.DATATYPE);
    }

    @Test
    @DisplayName("Windowed parsing: windowed(T)")
    void testWindowedType() {
        final var ut = parse("windowed(string)");
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        final var dt = ut.result().dataType();
        assertThat(dt).isInstanceOf(WindowedType.class);
        final var w = (WindowedType) dt;
        assertThat(w.keyType()).isEqualTo(DataString.DATATYPE);
    }

    @Test
    @DisplayName("Nested types parsing: list(map(string))")
    void testNestedTypes() {
        final var ut = parse("list(map(string))");
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        final var dt = ut.result().dataType();
        assertThat(dt).isInstanceOf(ListType.class);
        final var lt = (ListType) dt;
        assertThat(lt.valueType()).isInstanceOf(MapType.class);
        assertThat(((MapType) lt.valueType()).valueType()).isEqualTo(DataString.DATATYPE);
    }

    @Test
    @DisplayName("Notation only returns default type")
    void testNotationOnly() {
        final var ut = parse(UserType.DEFAULT_NOTATION);
        assertThat(ut.isOk()).as(ut.isError() ? ut.errorMessage() : "").isTrue();
        assertThat(ut.result().notation()).isEqualTo(UserType.DEFAULT_NOTATION);
        // Parser should not error and returns the concrete notation name
        assertThat(ut.result()).isNotNull();
    }

    @ParameterizedTest
    @DisplayName("Error cases for unclosed constructs")
    @ValueSource(strings = {"[int", "list(int", "map(int", "enum(A,B", "union(int,string", "(int,string", "tuple(int,string", "windowed(string"})
    void testUnclosedErrors(String type) {
        final var ut = parse(type);
        assertThat(ut.isError()).isTrue();
        assertThat(ut.errorMessage()).isNotNull();
    }

    @Test
    @DisplayName("Null input yields UNKNOWN user type")
    void testNullInput() {
        final var ut = parse(null);
        assertThat(ut.isOk()).isTrue();
        assertThat(ut.result().dataType()).isEqualTo(DataType.UNKNOWN);
    }

    @Test
    @DisplayName("Test AVRO schema loading from disk (without namespace)")
    void testAvroSchemaLoading() throws IOException {
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

        final var userType = parse("avro:" + schemaName);
        assertThat(userType.isOk()).isTrue();
        assertThat(userType.result().notation()).isEqualTo("avro");
        assertThat(userType.result().dataType()).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Test AVRO schema loading from disk (with namespace)")
    void testAvroSchemaLoadingWithNamespace() throws IOException {
        final var schemaName = "MyAvroSchema";
        final var namespace = "io.axual.ksml.test";
        final var fullName = namespace + "." + schemaName;
        final var schemaContent = "{\"type\":\"record\",\"name\":\"" + schemaName + "\",\"namespace\":\"" + namespace + "\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertThat(contextName).isEqualTo(fullName + ".avsc");
            assertThat(name).isEqualTo(fullName);
            assertThat(schemaString).isEqualTo(schemaContent);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", mockParser));

        final var userType = parse("avro:" + namespace + "." + schemaName);
        assertThat(userType.isOk()).isTrue();
        assertThat(userType.result().notation()).isEqualTo("avro");
        assertThat(userType.result().dataType()).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Test JSONSCHEMA schema loading from disk")
    void testJsonSchemaLoading() throws IOException {
        final var schemaName = "MyJsonSchema";
        final var schemaContent = "{}";
        Files.writeString(tempDir.resolve(schemaName + ".json"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertThat(contextName).isEqualTo(schemaName + ".json");
            assertThat(name).isEqualTo(schemaName);
            assertThat(schemaString).isEqualTo(schemaContent);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("jsonschema", new MockNotation("jsonschema", Notation.SchemaUsage.SCHEMA_REQUIRED, ".json", mockParser));

        final var userType = parse("jsonschema:" + schemaName);
        assertThat(userType.isOk()).isTrue();
        assertThat(userType.result().notation()).isEqualTo("jsonschema");
        assertThat(userType.result().dataType()).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Test PROTOBUF schema loading from disk")
    void testProtobufSchemaLoading() throws IOException {
        final var schemaName = "MyProtoSchema";
        final var schemaContent = "syntax = \"proto3\";";
        Files.writeString(tempDir.resolve(schemaName + ".proto"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertThat(contextName).isEqualTo(schemaName + ".proto");
            assertThat(name).isEqualTo(schemaName);
            assertThat(schemaString).isEqualTo(schemaContent);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("protobuf", new MockNotation("protobuf", Notation.SchemaUsage.SCHEMA_REQUIRED, ".proto", mockParser));

        final var userType = parse("protobuf:" + schemaName);
        assertThat(userType.isOk()).isTrue();
        assertThat(userType.result().notation()).isEqualTo("protobuf");
        assertThat(userType.result().dataType()).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Test schema loading error: missing file")
    void testMissingSchemaFile() {
        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", (c, n, s) -> null));

        // When schema is not found, SchemaLibrary.getSchema returns null because it returns null
        // when loader.load returns null.
        // Then UserTypeParser.parseNotationWithOrWithoutSchema calls
        // new DataTypeDataSchemaMapper().fromDataSchema(null) which returns DataType.UNKNOWN.
        // Finally, it tries to check assignability: not.defaultType().isAssignableFrom(DataType.UNKNOWN)
        // Since DataType.UNKNOWN is a wildcard that's NOT a ComplexType, StructType.isAssignableFrom(UNKNOWN)
        // returns typeMismatch error in ComplexType.isAssignableFrom.

        assertThatThrownBy(() -> parse("avro:MissingSchema"))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("Can not load schema");
    }

    @Test
    @DisplayName("Test avro:windowed(SomeSchema) uses avro notation for SomeSchema")
    void testAvroWindowedSomeSchema() throws IOException {
        final var schemaName = "SomeSchema";
        final var schemaContent = "{\"type\":\"record\",\"name\":\"SomeSchema\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertThat(contextName).isEqualTo(schemaName + ".avsc");
            assertThat(name).isEqualTo(schemaName);
            assertThat(schemaString).isEqualTo(schemaContent);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", mockParser));

        final var userType = parse("avro:windowed(" + schemaName + ")");
        assertThat(userType.isOk()).as(userType.isError() ? userType.errorMessage() : "").isTrue();
        assertThat(userType.result().notation()).isEqualTo("avro");
        assertThat(userType.result().dataType()).isInstanceOf(WindowedType.class);
        final var windowedType = (WindowedType) userType.result().dataType();
        assertThat(windowedType.keyType()).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Test schema loading error: missing file")
    void testAvroWindowedMissingSchemaFile() {
        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", (c, n, s) -> null));

        // When schema is not found, SchemaLibrary.getSchema returns null because it returns null
        // when loader.load returns null.
        // Then UserTypeParser.parseNotationWithOrWithoutSchema calls
        // new DataTypeDataSchemaMapper().fromDataSchema(null) which returns DataType.UNKNOWN.
        // Finally, it tries to check assignability: not.defaultType().isAssignableFrom(DataType.UNKNOWN)
        // Since DataType.UNKNOWN is a wildcard that's NOT a ComplexType, StructType.isAssignableFrom(UNKNOWN)
        // returns typeMismatch error in ComplexType.isAssignableFrom.

        assertThatThrownBy(() -> parse("avro:windowed(MissingSchema)"))
                .isInstanceOf(SchemaException.class)
                .hasMessageContaining("Can not load schema");
    }

    @Test
    @DisplayName("Test schema loading windowed default type")
    void testAvroWindowedStandardType() {
        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", (c, n, s) -> null));

        // When schema is not found, SchemaLibrary.getSchema returns null because it returns null
        // when loader.load returns null.
        // Then UserTypeParser.parseNotationWithOrWithoutSchema calls
        // new DataTypeDataSchemaMapper().fromDataSchema(null) which returns DataType.UNKNOWN.
        // Finally, it tries to check assignability: not.defaultType().isAssignableFrom(DataType.UNKNOWN)
        // Since DataType.UNKNOWN is a wildcard that's NOT a ComplexType, StructType.isAssignableFrom(UNKNOWN)
        // returns typeMismatch error in ComplexType.isAssignableFrom.

        final var userType = parse("avro:windowed(struct)");
        assertThat(userType.isOk()).isTrue();
        assertThat(userType.result().notation()).isEqualTo("avro");
        assertThat(userType.result().dataType()).isInstanceOf(WindowedType.class);
        final var windowedType = (WindowedType) userType.result().dataType();
        assertThat(windowedType.keyType()).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Test avro:[SomeSchema] uses avro notation for SomeSchema")
    void testAvroListSomeSchema() throws IOException {
        final var schemaName = "SomeSchemaList";
        final var schemaContent = "{\"type\":\"record\",\"name\":\"SomeSchemaList\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) -> {
            assertThat(contextName).isEqualTo(schemaName + ".avsc");
            assertThat(name).isEqualTo(schemaName);
            assertThat(schemaString).isEqualTo(schemaContent);
            return new StructSchema(null, schemaName, null, Collections.emptyList());
        };

        ExecutionContext.INSTANCE.notationLibrary().register("avro", new MockNotation("avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", mockParser));

        final var userType = parse("avro:[" + schemaName + "]");
        assertThat(userType.isOk()).as(userType.isError() ? userType.errorMessage() : "").isTrue();
        assertThat(userType.result().notation()).isEqualTo("avro");
        assertThat(userType.result().dataType()).isInstanceOf(ListType.class);
        final var listType = (ListType) userType.result().dataType();
        assertThat(listType.valueType()).isInstanceOf(StructType.class);
    }

    @Test
    @DisplayName("Test notation without schema name returns UnresolvedType when notation supports remote schema")
    void testNotationWithoutSchemaReturnsUnresolvedType() {
        final var remoteNotation = new MockNotation("remote_avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", null) {
            @Override
            public boolean supportsRemoteSchema() {
                return true;
            }

            @Override
            public DataSchema fetchRemoteSchema(String topic, boolean isKey) {
                return new StructSchema(null, topic, null, Collections.emptyList());
            }
        };
        ExecutionContext.INSTANCE.notationLibrary().register("remote_avro", remoteNotation);

        final var userType = parse("remote_avro");
        assertThat(userType.isOk()).isTrue();
        assertThat(userType.result().notation()).isEqualTo("remote_avro");
        assertThat(userType.result().dataType()).isInstanceOf(UnresolvedType.class);
    }

    @Test
    @DisplayName("Test notation without schema name returns an error when notation does not support remote schema")
    void testNotationWithoutSchemaReturnsDefaultType() {
        final var localNotation = new MockNotation("local_avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", null);
        ExecutionContext.INSTANCE.notationLibrary().register("local_avro", localNotation);

        final var userType = parse("local_avro");

        assertThat(userType.isError()).isTrue();
        String expectedMessage = "Schema is required for notation local_avro";
        assertThat(userType.errorMessage()).contains(expectedMessage);
    }

    @Test
    @DisplayName("Test notation with explicit schema name still loads from disk (regression)")
    void testExplicitSchemaStillLoadsFromDisk() throws IOException {
        final var schemaName = "RegressionSchema";
        final var schemaContent = "{\"type\":\"record\",\"name\":\"RegressionSchema\",\"fields\":[]}";
        Files.writeString(tempDir.resolve(schemaName + ".avsc"), schemaContent);

        final var mockParser = (Notation.SchemaParser) (contextName, name, schemaString) ->
                new StructSchema(null, schemaName, null, Collections.emptyList());

        final var remoteNotation = new MockNotation("regression_avro", Notation.SchemaUsage.SCHEMA_REQUIRED, ".avsc", mockParser) {
            @Override
            public boolean supportsRemoteSchema() {
                return true;
            }

            @Override
            public DataSchema fetchRemoteSchema(String topic, boolean isKey) {
                throw new RuntimeException("Should not call fetchRemoteSchema when schema name is specified");
            }
        };
        ExecutionContext.INSTANCE.notationLibrary().register("regression_avro", remoteNotation);

        final var userType = parse("regression_avro:" + schemaName);
        assertThat(userType.isOk()).isTrue();
        assertThat(userType.result().notation()).isEqualTo("regression_avro");
        assertThat(userType.result().dataType()).isInstanceOf(StructType.class);
    }
}
