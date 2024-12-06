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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.mapper.DataTypeSchemaMapper;
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.UserTupleType;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.parser.schema.DataSchemaDSL;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.data.type.*;
import io.axual.ksml.exception.TopologyException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.axual.ksml.data.notation.UserType.UNKNOWN;

public class UserTypeParser {
    private static final String NOTATION_SEPARATOR = ":";
    private static final String DOUBLE_QUOTE = "\"";
    private static final String TYPE_SEPARATOR = ",";
    private static final String ROUND_BRACKET_OPEN = "(";
    private static final String ROUND_BRACKET_CLOSE = ")";
    private static final String SQUARE_BRACKET_OPEN = "[";
    private static final String SQUARE_BRACKET_CLOSE = "]";
    private static final String ENUM_TYPE = DataSchemaDSL.ENUM_TYPE;
    private static final String UNION_TYPE = DataSchemaDSL.UNION_TYPE;
    private static final String WINDOWED_TYPE = DataSchemaDSL.WINDOWED_TYPE;
    private static final String ALLOWED_LITERAL_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    private static final String ALLOWED_TYPE_CHARACTERS = ALLOWED_LITERAL_CHARACTERS + NOTATION_SEPARATOR + DataSchemaDSL.UNKNOWN_TYPE;

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
        final var notation = decomposedType.notation();
        final var datatype = decomposedType.datatype();

        // Internal types
        final var internalType = parseType(datatype);
        if (internalType != null) {
            final var n = NotationLibrary.get(notation);
            if (n.defaultType() != null && !n.defaultType().isAssignableFrom(internalType)) {
                throw new DataException("Notation " + notation + " does not allow for data type " + datatype);
            }
            return new UserType(notation, internalType);
        }

        // List type
        if (datatype.startsWith(SQUARE_BRACKET_OPEN)) {
            if (!datatype.endsWith(SQUARE_BRACKET_CLOSE)) {
                throw new ParseException("List type not properly closed: " + datatype);
            }
            // Parse the type in brackets separately as the type of list elements. Notation overrides are not allowed
            // for list elements. If specified (eg. "[avro:SomeSchema]") then the notation is ignored.
            var valueType = parseTypeAndNotation(datatype.substring(1, datatype.length() - 1), notation);
            // Return as list of parsed value type using notation specified by the above parsed parent list type
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
            return new UserType(notation, new EnumType(parseListOfLiterals(literals).stream().map(Symbol::new).toList()));
        }

        // union(type1,type2,...)
        if (datatype.startsWith(UNION_TYPE + ROUND_BRACKET_OPEN) && datatype.endsWith(ROUND_BRACKET_CLOSE)) {
            var unionSubtypes = datatype.substring(UNION_TYPE.length() + 1, datatype.length() - 1);
            return new UserType(notation, new UnionType(Arrays.stream(UserType.userTypesToDataTypes(parseListOfTypesAndNotation(unionSubtypes, notation))).map(UnionType.ValueType::new).toArray(UnionType.ValueType[]::new)));
        }

        // windowed(type)
        if (datatype.startsWith(WINDOWED_TYPE + ROUND_BRACKET_OPEN) && datatype.endsWith(ROUND_BRACKET_CLOSE)) {
            var windowedType = datatype.substring(WINDOWED_TYPE.length() + 1, datatype.length() - 1);
            return new UserType(notation, new WindowedType(parseType(windowedType)));
        }

        // Notation type with specific schema
        if (NotationLibrary.exists(notation)) {
            final var loader = NotationLibrary.get(notation).loader();
            if (loader != null) {
                // If we reach this point, then we assume that the datatype refers to a schema to load
                final var schema = SchemaLibrary.getSchema(notation, datatype, false);
                final var schemaType = new DataTypeSchemaMapper().fromDataSchema(schema);
                if (!(NotationLibrary.get(notation).defaultType().isAssignableFrom(schemaType))) {
                    throw new DataException("Notation " + notation + " does not allow for data type " + datatype);
                }
                return new UserType(notation, schemaType);
            }
        }

        // Notation without specific schema
        if (NotationLibrary.exists(datatype)) {
            return new UserType(datatype, NotationLibrary.get(datatype).defaultType());
        }

        throw new TopologyException("Unknown type: " + notation + ":" + datatype);
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
            return new DecomposedType(parsedNotation, parsedType);
        }

        // Return the whole type string to be processed further, along with default notation
        return new DecomposedType(defaultNotation, composedType);
    }

    private static List<String> parseListOfLiterals(String literals) {
        // Literals are optionally double-quoted
        final var splitLiterals = literals.split(TYPE_SEPARATOR);
        final var result = new ArrayList<String>();
        for (final var literal : splitLiterals) {
            if (literal.length() > 2 && literal.startsWith(DOUBLE_QUOTE) && literal.endsWith(DOUBLE_QUOTE)) {
                result.add(literal.substring(1, literal.length() - 2));
            } else {
                result.add(literal);
            }
        }
        return result;
    }

    private static DataType parseType(String type) {
        return switch (type) {
            case DataSchemaDSL.NULL_TYPE, DataSchemaDSL.NONE_TYPE -> DataNull.DATATYPE;
            case DataSchemaDSL.BOOLEAN_TYPE -> DataBoolean.DATATYPE;
            case DataSchemaDSL.BYTE_TYPE -> DataByte.DATATYPE;
            case DataSchemaDSL.SHORT_TYPE -> DataShort.DATATYPE;
            case DataSchemaDSL.DOUBLE_TYPE -> DataDouble.DATATYPE;
            case DataSchemaDSL.FLOAT_TYPE -> DataFloat.DATATYPE;
            case DataSchemaDSL.INTEGER_TYPE, DataSchemaDSL.INTEGER_TYPE_ALTERNATIVE -> DataInteger.DATATYPE;
            case DataSchemaDSL.LONG_TYPE -> DataLong.DATATYPE;
            case DataSchemaDSL.BYTES_TYPE -> DataBytes.DATATYPE;
            case DataSchemaDSL.STRING_TYPE, DataSchemaDSL.STRING_TYPE_ALTERNATIVE -> DataString.DATATYPE;
            case DataSchemaDSL.STRUCT_TYPE -> new StructType();
            case DataSchemaDSL.ANY_TYPE, DataSchemaDSL.UNKNOWN_TYPE -> DataType.UNKNOWN;
            default -> null;
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
        return new EnumSchema(
                DefinitionParser.SCHEMA_NAMESPACE,
                "UserType",
                "UserTypes are the basic types used in streams and pipelines",
                types.stream().map(Symbol::new).toList());
    }
}
