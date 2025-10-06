package io.axual.ksml.parser;

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
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.type.*;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.schema.parser.DataSchemaDSL;
import io.axual.ksml.type.UserTupleType;
import io.axual.ksml.type.UserType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.axual.ksml.type.UserType.DEFAULT_NOTATION;

public class UserTypeParser {
    private static final String NOTATION_SEPARATOR = ":";
    private static final String DOUBLE_QUOTE = "\"";
    private static final String TYPE_SEPARATOR = ",";
    private static final String ROUND_BRACKET_OPEN = "(";
    private static final String ROUND_BRACKET_CLOSE = ")";
    private static final String SQUARE_BRACKET_OPEN = "[";
    private static final String SQUARE_BRACKET_CLOSE = "]";
    private static final String ENUM_TYPE = DataSchemaConstants.ENUM_TYPE;
    private static final String LIST_TYPE = DataSchemaConstants.LIST_TYPE;
    private static final String MAP_TYPE = DataSchemaConstants.MAP_TYPE;
    private static final String TUPLE_TYPE = DataSchemaConstants.TUPLE_TYPE;
    private static final String UNION_TYPE = DataSchemaConstants.UNION_TYPE;
    private static final String WINDOWED_TYPE = DataSchemaDSL.WINDOWED_TYPE;
    private static final String ALLOWED_LITERAL_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    private static final String ALLOWED_TYPE_CHARACTERS = ALLOWED_LITERAL_CHARACTERS + NOTATION_SEPARATOR + DataSchemaDSL.UNKNOWN_TYPE;

    private UserTypeParser() {
    }

    public static UserType parse(String type) {
        final var types = parseListOfTypesAndNotation(type, DEFAULT_NOTATION);
        if (types.length == 1) {
            return types[0];
        }
        throw new ParseException("Could not parse data type: " + type);
    }

    // Parses a list of comma-separated user data types. If no comma is found, then the returned
    // list only contains one dataType.
    private static UserType[] parseListOfTypesAndNotation(String type, String defaultNotation) {
        if (type == null || type.isEmpty()) {
            return new UserType[]{UserType.UNKNOWN};
        }
        type = type.trim();

        final var leftTerm = parseLeftMostTerm(type);
        final var remainder = type.substring(leftTerm.length()).trim();
        final var leftTermType = parseTypeAndNotation(leftTerm, defaultNotation);
        var remainderTypes = new UserType[0];
        if (remainder.startsWith(TYPE_SEPARATOR)) {
            remainderTypes = parseListOfTypesAndNotation(remainder.substring(1), defaultNotation);
        } else if (!remainder.isEmpty()) {
            throw new ParseException("Could not parse data type: " + type);
        }

        final var result = new UserType[remainderTypes.length + 1];
        result[0] = leftTermType;
        System.arraycopy(remainderTypes, 0, result, 1, remainderTypes.length);
        return result;
    }

    private record DecomposedType(String notation, String dataType) {
    }

    private static UserType parseTypeAndNotation(String composedType, String defaultNotation) {
        final var decomposedType = decompose(composedType, defaultNotation);
        final var notation = decomposedType.notation();
        final var dataType = decomposedType.dataType();

        // Internal types
        final var internalType = parseType(dataType);
        if (internalType != null) {
            final var n = ExecutionContext.INSTANCE.notationLibrary().get(notation);
            if (n.defaultType() != null && !n.defaultType().checkAssignableFrom(internalType).isOK()) {
                throw new DataException("Notation " + n.name() + " does not allow for data type " + dataType);
            }
            return new UserType(notation, internalType);
        }

        // [type]
        if (dataType.startsWith(SQUARE_BRACKET_OPEN)) {
            if (!dataType.endsWith(SQUARE_BRACKET_CLOSE)) {
                throw new ParseException("List type not properly closed: " + dataType);
            }
            // Parse the type in brackets separately as the type of list elements
            final var valueTypeStr = dataType.substring(1, dataType.length() - 1);
            final var valueType = parseTypeAndNotation(valueTypeStr, DEFAULT_NOTATION).dataType();
            // Return a typed list
            return new UserType(notation, new ListType(valueType));
        }

        // list(type)
        if (dataType.startsWith(LIST_TYPE + ROUND_BRACKET_OPEN)) {
            if (!dataType.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("List type not properly closed: " + dataType);
            }
            // Parse the type in brackets separately as the type of list elements
            final var valueTypeStr = dataType.substring(LIST_TYPE.length() + 1, dataType.length() - 1);
            final var valueType = parseTypeAndNotation(valueTypeStr, DEFAULT_NOTATION).dataType();
            // Return a typed list
            return new UserType(notation, new ListType(valueType));
        }

        // enum(literal1, literal2,...)
        if (dataType.startsWith(ENUM_TYPE + ROUND_BRACKET_OPEN)) {
            if (!dataType.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("Enum type not properly closed: " + dataType);
            }
            // Parse the literals in brackets separately as possible enum values
            final var literalsStr = dataType.substring(ENUM_TYPE.length() + 1, dataType.length() - 1);
            final var literals = parseListOfLiterals(literalsStr).stream().map(Symbol::new).toList();
            // Return a new enum type with those literals
            return new UserType(notation, new EnumType(literals));
        }

        // map(type)
        if (dataType.startsWith(MAP_TYPE + ROUND_BRACKET_OPEN)) {
            if (!dataType.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("Map type not properly closed: " + dataType);
            }
            // Parse the type in brackets separately as the type of map elements
            final var valueTypeStr = dataType.substring(MAP_TYPE.length() + 1, dataType.length() - 1);
            final var valueType = parseTypeAndNotation(valueTypeStr, DEFAULT_NOTATION).dataType();
            // Return a typed map
            return new UserType(notation, new MapType(valueType));
        }

        // union(type1, type2,...)
        if (dataType.startsWith(UNION_TYPE + ROUND_BRACKET_OPEN)) {
            if (!dataType.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("Union type not properly closed: " + dataType);
            }
            // Parse the types in brackets separately as the union's member types
            final var memberTypesStr = dataType.substring(UNION_TYPE.length() + 1, dataType.length() - 1);
            final var memberTypes = parseListOfTypesAndNotation(memberTypesStr, DEFAULT_NOTATION);
            // Return a new union type with parsed member types
            return new UserType(notation, new UnionType(Arrays.stream(UserType.userTypesToDataTypes(memberTypes)).map(UnionType.Member::new).toArray(UnionType.Member[]::new)));
        }

        // windowed(type)
        if (dataType.startsWith(WINDOWED_TYPE + ROUND_BRACKET_OPEN)) {
            if (!dataType.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("Windowed type not properly closed: " + dataType);
            }
            // Parse the type in brackets separately as the type of the windowed key
            final var windowedTypeStr = dataType.substring(WINDOWED_TYPE.length() + 1, dataType.length() - 1);
            final var windowedType = parseTypeAndNotation(windowedTypeStr, DEFAULT_NOTATION).dataType();
            // Return a typed windowed object
            return new UserType(notation, new WindowedType(windowedType));
        }

        // (type1, type2, ...)
        if (dataType.startsWith(ROUND_BRACKET_OPEN)) {
            if (!dataType.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("Tuple type not properly closed: " + dataType);
            }
            // Parse the type in brackets separately as the tuple's element types
            final var elementTypesStr = dataType.substring(1, dataType.length() - 1);
            final var elementTypes = parseListOfTypesAndNotation(elementTypesStr, DEFAULT_NOTATION);
            // Return a new tuple type with parsed element types
            return new UserType(notation, new UserTupleType(elementTypes));
        }

        // tuple(type1, type2, ...)
        if (dataType.startsWith(TUPLE_TYPE + ROUND_BRACKET_OPEN)) {
            if (!dataType.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new ParseException("Tuple type not properly closed: " + dataType);
            }
            // Parse the type in brackets separately as the tuple's element types
            final var elementTypesStr = dataType.substring(TUPLE_TYPE.length() + 1, dataType.length() - 1);
            final var elementTypes = parseListOfTypesAndNotation(elementTypesStr, DEFAULT_NOTATION);
            // Return a new tuple type with parsed element types
            return new UserType(notation, new UserTupleType(elementTypes));
        }

        // Notation type with or without a specific schema
        if (ExecutionContext.INSTANCE.notationLibrary().exists(notation)) {
            final var not = ExecutionContext.INSTANCE.notationLibrary().get(notation);
            if (not != null) {
                // If the dataType (ie. schema) is empty, then return the notation with a default type
                if (dataType.isEmpty()) return new UserType(notation, not.defaultType());
                // Load the schema
                final var schema = ExecutionContext.INSTANCE.schemaLibrary().getSchema(not, dataType, false);
                final var schemaType = new DataTypeDataSchemaMapper().fromDataSchema(schema);
                if (!not.defaultType().checkAssignableFrom(schemaType).isOK()) {
                    throw new DataException("Notation does not allow for type: notation=" + not.name() + ", type=" + dataType);
                }
                return new UserType(notation, schemaType);
            }
        }

        throw new TopologyException("Unknown user type: notation=" + (notation != null ? notation : "null") + ", type=" + dataType);
    }

    // This method decomposes a user type into its components. User types are always of the form "notation:datatype".
    private static DecomposedType decompose(String composedType, String defaultNotation) {
        final var posColon = composedType.contains(NOTATION_SEPARATOR) ? composedType.indexOf(NOTATION_SEPARATOR) : composedType.length();
        final var posOpenRound = composedType.contains(ROUND_BRACKET_OPEN) ? composedType.indexOf(ROUND_BRACKET_OPEN) : composedType.length();
        final var posOpenSquare = composedType.contains(SQUARE_BRACKET_OPEN) ? composedType.indexOf(SQUARE_BRACKET_OPEN) : composedType.length();

        // Check if the user type contains a colon (user type is of the form "notation:datatype") AND that this colon
        // appears before any brackets (ie. ignore tuples, lists and other types that use brackets). If so then parse
        // it as a user type with explicit notation.
        if (posColon < posOpenRound && posColon < posOpenSquare) {
            final var parsedNotation = composedType.substring(0, composedType.indexOf(NOTATION_SEPARATOR));
            final var parsedType = composedType.substring(composedType.indexOf(NOTATION_SEPARATOR) + 1);

            // Return the parsed notation and datatype
            return new DecomposedType(parsedNotation, parsedType);
        }

        // Check if the composedType is a schemaless reference to a notation
        if (ExecutionContext.INSTANCE.notationLibrary().exists(composedType)) {
            final var not = ExecutionContext.INSTANCE.notationLibrary().get(composedType);
            return new DecomposedType(not.name(), "");
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
            case DataSchemaConstants.ANY_TYPE, DataSchemaDSL.UNKNOWN_TYPE -> DataType.UNKNOWN;
            case DataSchemaConstants.NULL_TYPE, DataSchemaDSL.NONE_TYPE -> DataNull.DATATYPE;
            case DataSchemaConstants.BOOLEAN_TYPE -> DataBoolean.DATATYPE;
            case DataSchemaConstants.BYTE_TYPE -> DataByte.DATATYPE;
            case DataSchemaConstants.SHORT_TYPE -> DataShort.DATATYPE;
            case DataSchemaConstants.INTEGER_TYPE, DataSchemaDSL.INTEGER_TYPE_ALTERNATIVE -> DataInteger.DATATYPE;
            case DataSchemaConstants.LONG_TYPE -> DataLong.DATATYPE;
            case DataSchemaConstants.FLOAT_TYPE -> DataFloat.DATATYPE;
            case DataSchemaConstants.DOUBLE_TYPE -> DataDouble.DATATYPE;
            case DataSchemaConstants.BYTES_TYPE -> DataBytes.DATATYPE;
            case DataSchemaConstants.STRING_TYPE, DataSchemaDSL.STRING_TYPE_ALTERNATIVE -> DataString.DATATYPE;
            case DataSchemaConstants.MAP_TYPE -> new MapType();
            case DataSchemaConstants.STRUCT_TYPE -> new StructType();
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
            final var ch = type.substring(index, index + 1);

            if (ch.equals(ROUND_BRACKET_OPEN) || ch.equals(SQUARE_BRACKET_OPEN)) {
                final var bracketedTerm = parseLeftMostTerm(type.substring(index));
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
            final var ch = type.substring(index, index + 1);
            if (ch.equals(openBracket)) openCount++;
            if (ch.equals(closeBracket)) openCount--;
            if (openCount == 0) {
                // Return string including both brackets
                return type.substring(0, index + 1);
            }
        }
        throw new ParseException("Error in expression: no closing bracket found: " + type);
    }
}
