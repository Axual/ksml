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

import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class UserTypeParserTest {

    @ParameterizedTest
    @DisplayName("Test all known types")
    @ValueSource(strings = {"boolean", "byte", "bytes", "short", "double", "float", "int", "long", "?", "none", "str", "string"})
    void testParseValidTypes(String type) {
        var userType = UserTypeParser.parse(type);
        assertNotNull(userType);
        assertEquals(UserType.DEFAULT_NOTATION, userType.notation(), "notation for " + type + "should default to " + UserType.DEFAULT_NOTATION);
    }

    @ParameterizedTest
    @DisplayName("Test parsing for dataType String (types 'str' and 'string'")
    @ValueSource(strings = {"str", "string"})
    void testParseStringType(String type) {
        final var userType = UserTypeParser.parse(type);
        assertNotNull(userType);
        final var dataType = userType.dataType();
        assertEquals(String.class, dataType.containerClass());
        assertTrue(dataType.isAssignableFrom("some random string"));
        assertTrue(dataType.isAssignableFrom(String.class));
    }

    @ParameterizedTest
    @DisplayName("Test mapping of dataType names to correct user types class")
    @MethodSource("typesAndDataTypes")
    void testDataTypes(String type, DataType dataType) {
        final var userType = UserTypeParser.parse(type);
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
}
