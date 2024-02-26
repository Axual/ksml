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

import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.notation.UserTupleType;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.csv.CsvNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.notation.soap.SOAPNotation;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.*;
import io.axual.ksml.exception.TopologyException;

import java.util.ArrayList;

import static io.axual.ksml.data.notation.UserType.UNKNOWN;

public class UserTypeParser {
    private static final String NOTATION_SEPARATOR = ":";
    private static final String DOUBLE_QUOTE = "\"";
    private static final String TYPE_SEPARATOR = ",";
    private static final String ROUND_BRACKET_OPEN = "(";
    private static final String ROUND_BRACKET_CLOSE = ")";
    private static final String SQUARE_BRACKET_OPEN = "[";
    private static final String SQUARE_BRACKET_CLOSE = "]";
    private static final String ENUM_TYPE = "enum";
    private static final String UNION_TYPE = "union";
    private static final String WINDOWED_TYPE = "windowed";
    private static final String UNKNOWN_TYPE = "?";
    private static final String ALLOWED_LITERAL_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    private static final String ALLOWED_TYPE_CHARACTERS = ALLOWED_LITERAL_CHARACTERS + NOTATION_SEPARATOR + UNKNOWN_TYPE;

    private UserTypeParser() {
    }

    public static UserType parse(String type) {
        UserType[] types = parseListOfTypesAndNotation(type, UserType.DEFAULT_NOTATION);
        if (types.length == 1) {
            return types[0];
        }
        throw new ParseException("Could not parse data type: " + type);
    }

    // Parses a list of comma-separated user data types. If no comma is found, then the returned
    // list only contains one dataType.
    private static UserType[] parseListOfTypesAndNotation(String type, String defaultNotation) {
        if (type == null || type.isEmpty()) {
            return new UserType[]{UNKNOWN};
        }
        type = type.trim();

        String leftTerm = parseLeftMostTerm(type);
        String remainder = type.substring(leftTerm.length()).trim();
        UserType leftTermType = parseTypeAndNotation(leftTerm, defaultNotation);
        var remainderTypes = new UserType[0];
        if (remainder.startsWith(TYPE_SEPARATOR)) {
            remainderTypes = parseListOfTypesAndNotation(remainder.substring(1), defaultNotation);
        } else if (!remainder.isEmpty()) {
            throw new ParseException("Could not parse data type: " + type);
        }

        var result = new UserType[remainderTypes.length + 1];
        result[0] = leftTermType;
        System.arraycopy(remainderTypes, 0, result, 1, remainderTypes.length);
        return result;
    }

    private record DecomposedType(String notation, String datatype) {
    }

    private static UserType parseTypeAndNotation(String composedType, String defaultNotation) {
        final var decomposedType = decompose(composedType, defaultNotation);
        final var datatype = decomposedType.datatype();
        final var notation = decomposedType.notation();

        // List type
        if (datatype.startsWith(SQUARE_BRACKET_OPEN)) {
            if (!datatype.endsWith(SQUARE_BRACKET_CLOSE)) {
                throw new ParseException("List type not properly closed: " + datatype);
            }
            // Parse the type in brackets separately as the type of list elements. Notation overrides are not allowed
            // for list elements. If specified (eg. "[avro:SomeSchema]") then the value notation is ignored.
            var valueType = parseTypeAndNotation(datatype.substring(1, datatype.length() - 1), notation);
            // Return the parsed value type with the above parsed notation
            return new UserType(notation, new ListType(valueType.dataType()));
        }

        // Tuple type
        if (datatype.startsWith(ROUND_BRACKET_OPEN)) {
            if (!datatype.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("Tuple type not properly closed: " + datatype);
            }
            var valueTypes = parseListOfTypesAndNotation(datatype.substring(1, datatype.length() - 1), notation);
            return new UserType(notation, new UserTupleType(valueTypes));
        }

        // enum(literal1,literal2,...)
        if (datatype.startsWith(ENUM_TYPE + ROUND_BRACKET_OPEN) && datatype.endsWith(ROUND_BRACKET_CLOSE)) {
            var literals = datatype.substring(ENUM_TYPE.length() + 1, datatype.length() - 1);
            return new UserType(notation, new EnumType(parseListOfLiterals(literals)));
        }

        // union(type1,type2,...)
        if (datatype.startsWith(UNION_TYPE + ROUND_BRACKET_OPEN) && datatype.endsWith(ROUND_BRACKET_CLOSE)) {
            var unionSubtypes = datatype.substring(UNION_TYPE.length() + 1, datatype.length() - 1);
            return new UserType(notation, new UnionType(UserType.userTypesToDataTypes(parseListOfTypesAndNotation(unionSubtypes, notation))));
        }

        // windowed(type)
        if (datatype.startsWith(WINDOWED_TYPE + ROUND_BRACKET_OPEN) && datatype.endsWith(ROUND_BRACKET_CLOSE)) {
            var windowedType = datatype.substring(WINDOWED_TYPE.length() + 1, datatype.length() - 1);
            return new UserType(notation, new WindowedType(parseType(windowedType)));
        }

        // AVRO with schema
        if (notation.equalsIgnoreCase(AvroNotation.NOTATION_NAME)) {
            var schema = SchemaLibrary.getSchema(AvroNotation.NOTATION_NAME, datatype, false);
            if (!(schema instanceof StructSchema structSchema))
                throw new ParseException("AVRO schema is not a STRUCT: " + datatype);
            return new UserType(AvroNotation.NOTATION_NAME, new StructType(structSchema));
        }

        // AVRO without schema
        if (datatype.equalsIgnoreCase(AvroNotation.NOTATION_NAME)) {
            return new UserType(AvroNotation.NOTATION_NAME, AvroNotation.DEFAULT_TYPE);
        }

        // CSV with schema
        if (notation.equalsIgnoreCase(CsvNotation.NOTATION_NAME)) {
            var schema = SchemaLibrary.getSchema(CsvNotation.NOTATION_NAME, datatype, false);
            if (!(schema instanceof StructSchema structSchema))
                throw new ParseException("CSV schema is not a STRUCT: " + datatype);
            return new UserType(CsvNotation.NOTATION_NAME, new StructType(structSchema));
        }

        // CSV without schema
        if (datatype.equalsIgnoreCase(CsvNotation.NOTATION_NAME)) {
            return new UserType(CsvNotation.NOTATION_NAME, CsvNotation.DEFAULT_TYPE);
        }

        // JSON with schema
        if (notation.equalsIgnoreCase(JsonNotation.NOTATION_NAME)) {
            var schema = SchemaLibrary.getSchema(JsonNotation.NOTATION_NAME, datatype, false);
            if (!(schema instanceof StructSchema structSchema))
                throw new ParseException("JSON schema is not a STRUCT: " + datatype);
            return new UserType(JsonNotation.NOTATION_NAME, new StructType(structSchema));
        }

        // JSON without schema
        if (datatype.equalsIgnoreCase(JsonNotation.NOTATION_NAME)) {
            return new UserType(JsonNotation.NOTATION_NAME, JsonNotation.DEFAULT_TYPE);
        }

        // SOAP with schema (not implemented yet)
        if (notation.equalsIgnoreCase(SOAPNotation.NOTATION_NAME)) {
            return new UserType(SOAPNotation.NOTATION_NAME, SOAPNotation.DEFAULT_TYPE);
        }

        // SOAP without schema
        if (datatype.equalsIgnoreCase(SOAPNotation.NOTATION_NAME)) {
            return new UserType(SOAPNotation.NOTATION_NAME, SOAPNotation.DEFAULT_TYPE);
        }

        // XML with schema
        if (notation.equalsIgnoreCase(XmlNotation.NOTATION_NAME)) {
            var schema = SchemaLibrary.getSchema(XmlNotation.NOTATION_NAME, datatype, false);
            if (!(schema instanceof StructSchema structSchema))
                throw new ParseException("XML schema is not a STRUCT: " + datatype);
            return new UserType(XmlNotation.NOTATION_NAME, new StructType(structSchema));
        }

        // XML without schema
        if (datatype.equalsIgnoreCase(XmlNotation.NOTATION_NAME)) {
            return new UserType(XmlNotation.NOTATION_NAME, XmlNotation.DEFAULT_TYPE);
        }

        // Parse the type as an in-built primary data type and return it
        return new UserType(notation, parseType(datatype));
    }

    // This method decomposes a user type into its components. User types are always of the form "notation:datatype".
    private static DecomposedType decompose(String composedType, String defaultNotation) {
        var posColon = composedType.contains(NOTATION_SEPARATOR) ? composedType.indexOf(NOTATION_SEPARATOR) : composedType.length();
        var posOpenRound = composedType.contains(ROUND_BRACKET_OPEN) ? composedType.indexOf(ROUND_BRACKET_OPEN) : composedType.length();
        var posOpenSquare = composedType.contains(SQUARE_BRACKET_OPEN) ? composedType.indexOf(SQUARE_BRACKET_OPEN) : composedType.length();

        // Check if the user type contains a colon (user type is of the form "notation:datatype") AND that this colon
        // appears before any brackets (ie. ignore tuples, lists and other types that use brackets). If so then parse
        // it as a user type with explicit notation.
        if (posColon < posOpenRound && posColon < posOpenSquare) {
            final var parsedNotation = composedType.substring(0, composedType.indexOf(NOTATION_SEPARATOR));
            final var parsedType = composedType.substring(composedType.indexOf(NOTATION_SEPARATOR) + 1);

            // Return the parsed notation and datatype
            return new DecomposedType(parsedNotation.toUpperCase(), parsedType);
        }

        // Return the whole type string to be processed further, along with default notation
        return new DecomposedType(defaultNotation, composedType);
    }

    private static String[] parseListOfLiterals(String literals) {
        // Literals are optionally double-quoted
        var splitLiterals = literals.split(TYPE_SEPARATOR);
        var result = new String[splitLiterals.length];
        for (int index = 0; index < splitLiterals.length; index++) {
            var literal = splitLiterals[index];
            if (literal.length() > 2 && literal.startsWith(DOUBLE_QUOTE) && literal.endsWith(DOUBLE_QUOTE)) {
                literal = literal.substring(1, literal.length() - 2);
            }
            result[index] = literal;
        }
        return result;
    }

    private static DataType parseType(String type) {
        return switch (type) {
            case "null", "none" -> DataNull.DATATYPE;
            case "boolean" -> DataBoolean.DATATYPE;
            case "byte" -> DataByte.DATATYPE;
            case "short" -> DataShort.DATATYPE;
            case "double" -> DataDouble.DATATYPE;
            case "float" -> DataFloat.DATATYPE;
            case "int", "integer" -> DataInteger.DATATYPE;
            case "long" -> DataLong.DATATYPE;
            case "bytes" -> DataBytes.DATATYPE;
            case "string", "str" -> DataString.DATATYPE;
            case "struct" -> new StructType();
            case "any", UNKNOWN_TYPE -> DataType.UNKNOWN;
            default -> throw new TopologyException("Can not derive dataType: " + type);
        };
    }

    private static String parseLeftMostTerm(String type) {
        // Check for bracketed expression
        if (type.startsWith(ROUND_BRACKET_OPEN))
            return parseBracketedExpression(type, ROUND_BRACKET_OPEN, ROUND_BRACKET_CLOSE);
        if (type.startsWith(SQUARE_BRACKET_OPEN))
            return parseBracketedExpression(type, SQUARE_BRACKET_OPEN, SQUARE_BRACKET_CLOSE);

        // Scan the literal at the beginning of the string until a non-literal character is found
        var index = 0;
        while (index < type.length()) {
            var ch = type.substring(index, index + 1);

            if (ch.equals(ROUND_BRACKET_OPEN) || ch.equals(SQUARE_BRACKET_OPEN)) {
                var bracketedTerm = parseLeftMostTerm(type.substring(index));
                // Skip past the bracketed expression
                index += bracketedTerm.length() - 1;
            } else if (!ALLOWED_TYPE_CHARACTERS.contains(ch)) {
                return type.substring(0, index);
            }
            index++;
        }
        return type;
    }

    private static String parseBracketedExpression(String type, String openBracket, String
            closeBracket) {
        var openCount = 1;
        for (var index = 1; index < type.length(); index++) {
            var ch = type.substring(index, index + 1);
            if (ch.equals(openBracket)) openCount++;
            if (ch.equals(closeBracket)) openCount--;
            if (openCount == 0) {
                // Return string including both brackets
                return type.substring(0, index + 1);
            }
        }
        throw new ParseException("Error in expression: no closing bracket found: " + type);
    }

    public static DataSchema getSchema() {
        final var types = new ArrayList<String>();
        types.add("null");
        types.add("none");
        types.add("boolean");
        types.add("byte");
        types.add("double");
        types.add("float");
        types.add("int");
        types.add("integer");
        types.add("long");
        types.add("bytes");
        types.add("str");
        types.add("string");
        types.add("struct");
        types.add("any");
        types.add("?");
        types.add("notation:schema");
        return new EnumSchema(DefinitionParser.SCHEMA_NAMESPACE, "UserType", "UserTypes are the basic types used in streams and pipelines", types);
    }
}
