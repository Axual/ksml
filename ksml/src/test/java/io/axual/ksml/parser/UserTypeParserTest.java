package io.axual.ksml.parser;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import io.axual.ksml.data.object.user.UserBoolean;
import io.axual.ksml.data.object.user.UserByte;
import io.axual.ksml.data.object.user.UserBytes;
import io.axual.ksml.data.object.user.UserDouble;
import io.axual.ksml.data.object.user.UserFloat;
import io.axual.ksml.data.object.user.UserInteger;
import io.axual.ksml.data.object.user.UserLong;
import io.axual.ksml.data.object.user.UserNone;
import io.axual.ksml.data.object.user.UserShort;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.base.SimpleType;
import io.axual.ksml.data.type.user.UserType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UserTypeParserTest {

    @ParameterizedTest
    @DisplayName("Test all known types")
    @ValueSource(strings = {"boolean", "byte", "bytes", "short", "double", "float", "int", "long", "?", "none", "str", "string"})
    void testParseValidTypes(String type) {
        var userType = UserTypeParser.parse(type);
        assertNotNull(userType);
        assertEquals(UserType.DEFAULT_NOTATION, userType.notation(), "notation for " + type + "should default to " + UserType.DEFAULT_NOTATION);
        final var dataType = userType.type();
        if (!type.equals("?")) {
            assertTrue(dataType.getClass().isAssignableFrom(SimpleType.class), "DataType is some subclass of SimpleType");
        }
    }

    @ParameterizedTest
    @DisplayName("Test parsing for type String (types 'str' and 'string'")
    @ValueSource(strings = {"str", "string"})
    void testParseStringType(String type) {
        final var userType = UserTypeParser.parse(type);
        assertNotNull(userType);
        final var dataType = userType.type();
        assertEquals(String.class, dataType.containerClass());
        assertTrue(dataType.isAssignableFrom("some random string"));
        assertTrue(dataType.isAssignableFrom(String.class));
    }

    @ParameterizedTest
    @DisplayName("Test mapping of type names to correct user types class")
    @MethodSource("typesAndDataTypes")
    void testDataTypes(String type, DataType dataType) {
        final var userType = UserTypeParser.parse(type);
        assertNotNull(userType);

        assertEquals(dataType, userType.type(), "DataType for '" + type + "' should be set to " + dataType);
        if (type.equals("?")) {
            assertEquals(DataType.UNKNOWN, userType.type(), "datatype for '?' should be UNKNOWN (anonymous subclass)");
        } else {
            assertEquals(SimpleType.class, userType.type().getClass(), "class for " + type + " should be SimpleType");
        }

    }

    static Stream<Arguments> typesAndDataTypes() {
        return Stream.of(
                Arguments.arguments("boolean", UserBoolean.DATATYPE),
                Arguments.arguments("byte", UserByte.DATATYPE),
                Arguments.arguments("bytes", UserBytes.DATATYPE),
                Arguments.arguments("short", UserShort.DATATYPE),
                Arguments.arguments("double", UserDouble.DATATYPE),
                Arguments.arguments("float", UserFloat.DATATYPE),
                Arguments.arguments("int", UserInteger.DATATYPE),
                Arguments.arguments("long", UserLong.DATATYPE),
                Arguments.arguments("str", UserString.DATATYPE),
                Arguments.arguments("string", UserString.DATATYPE),
                Arguments.arguments("none", UserNone.DATATYPE),
                Arguments.arguments("?", DataType.UNKNOWN)
        );
    }
}
