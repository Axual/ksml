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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.notation.Notation;
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
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UnresolvedType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.schema.parser.DataSchemaDSL;
import io.axual.ksml.type.UserTupleType;
import io.axual.ksml.type.UserType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

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
    private static final String NAMESPACE_SEPARATOR = ".";
    private static final String ALLOWED_LITERAL_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    private static final String ALLOWED_TYPE_CHARACTERS = NAMESPACE_SEPARATOR + ALLOWED_LITERAL_CHARACTERS + NOTATION_SEPARATOR + DataSchemaDSL.UNKNOWN_TYPE;

    public Parsed<UserType> parse(String type) {
        return parse(type, false);
    }

    public Parsed<UserType> parse(String type, boolean allowUnresolved) {
        final var parsedTypes = parseListOfTypesAndNotation(type, DEFAULT_NOTATION, allowUnresolved);
        if (parsedTypes.isError()) return Parsed.error(parsedTypes.errorMessage());
        if (parsedTypes.result().length == 1) return Parsed.ok(parsedTypes.result()[0]);
        return Parsed.error("Could not parse data type: " + type);
    }

    // Parses a list of comma-separated user data types. If no comma is found, then the returned
    // list only contains one dataType.
    private Parsed<UserType[]> parseListOfTypesAndNotation(String type, String defaultNotation, boolean allowUnresolved) {
        if (type == null || type.isEmpty()) {
            return Parsed.ok(new UserType[]{UserType.UNKNOWN});
        }
        type = type.trim();

        final var leftTerm = parseLeftMostTerm(type);
        if (leftTerm.isError()) return Parsed.error(leftTerm.errorMessage());
        final var parsedLeftTerm = parseTypeAndNotation(leftTerm.result(), defaultNotation, allowUnresolved);
        if (parsedLeftTerm.isError()) return Parsed.error(parsedLeftTerm.errorMessage());

        final UserType[] remainderTypes;
        final var remainder = type.substring(leftTerm.result().length()).trim();
        if (remainder.startsWith(TYPE_SEPARATOR)) {
            final var parsedRemainderTypes = parseListOfTypesAndNotation(remainder.substring(1), defaultNotation, allowUnresolved);
            if (parsedRemainderTypes.isError()) return Parsed.error(parsedRemainderTypes.errorMessage());
            remainderTypes = parsedRemainderTypes.result();
        } else if (remainder.isEmpty()) {
            remainderTypes = new UserType[0];
        } else {
            return Parsed.error("Could not parse data type: " + type);
        }

        final var result = new UserType[remainderTypes.length + 1];
        result[0] = parsedLeftTerm.result();
        System.arraycopy(remainderTypes, 0, result, 1, remainderTypes.length);
        return Parsed.ok(result);
    }

    private record DecomposedType(String notation, String dataType, boolean explicitNotation) {
    }

    private Parsed<UserType> parseTypeAndNotation(String composedType, String defaultNotation, boolean allowUnresolved) {
        final var decomposedType = decompose(composedType, defaultNotation);
        final var notation = decomposedType.notation();
        final var dataType = decomposedType.dataType();

        final var parsers = List.<Function<DecomposedType, Optional<Parsed<UserType>>>>of(
                // Internal types
                this::parseInternalType,
                // [type]
                this::parseList,
                // list(type)
                this::parseList2,
                // enum(literal1, literal2, ...)
                this::parseEnum,
                // map(type)
                this::parseMap,
                // union(type1, type2, ...)
                this::parseUnion,
                // windowed(type)
                this::parseWindowed,
                // (type1, type2, ...)
                this::parseTuple,
                // tuple(type1, type2, ...)
                this::parseTuple2,
                // Notation with or without a schema
                this::parseNotationWithOrWithoutSchema
        );

        for (final var parser : parsers) {
            final var parseResult = parser.apply(decomposedType);
            if (parseResult.isPresent()) {
                final var result = parseResult.get();
                if (result.isOk() && result.result().dataType() instanceof UnresolvedType && !allowUnresolved)
                    return Parsed.error("Unspecified schema can only be used in the context of a topic: " + result.result());
                return parseResult.get();
            }
        }

        return Parsed.error("Unknown user type: notation=" + (notation != null ? notation : "null") + ", type=" + dataType);
    }

    private Optional<Parsed<UserType>> parseInternalType(DecomposedType type) {
        // Parse internal type
        final var internalType = parseType(type.dataType);
        if (internalType == null) return Optional.empty();

        // For basic data types, first try the parsed notation
        final var notation = ExecutionContext.INSTANCE.notationLibrary().get(type.notation);
        if (notation.defaultType() == null || notation.defaultType().isAssignableFrom(internalType).isAssignable())
            return Optional.of(Parsed.ok(new UserType(type.notation, internalType)));

        // If the notation was explicitly given, or the notation equals the default notation, then parsing failed
        if (type.explicitNotation() || DEFAULT_NOTATION.equals(type.notation))
            return Optional.of(Parsed.error("Notation " + notation.name() + " does not allow for data type " + type.dataType));

        // If no explicit notation was provided for the internal type, then try the default notation
        final var dn = ExecutionContext.INSTANCE.notationLibrary().get(DEFAULT_NOTATION);
        if (dn.defaultType() != null && dn.defaultType().isAssignableFrom(internalType).isAssignable())
            return Optional.of(Parsed.ok(new UserType(DEFAULT_NOTATION, internalType)));

        return Optional.of(Parsed.error("Both notations " + notation.name() + " and " + dn.name() + " do not allow for data type " + type.dataType));
    }

    private Optional<Parsed<UserType>> parseList(DecomposedType type) {
        // Parse "[type]"
        if (!type.dataType.startsWith(SQUARE_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(SQUARE_BRACKET_CLOSE))
            return Optional.of(Parsed.error("List type not properly closed: " + type.dataType));

        // Parse the type in brackets separately as the type of list elements
        final var valueTypeStr = type.dataType.substring(1, type.dataType.length() - 1);
        final var parsedValueType = parseTypeAndNotation(valueTypeStr, type.notation, false);
        if (parsedValueType.isError()) return Optional.of(parsedValueType);
        // Return a typed list
        return Optional.of(Parsed.ok(new UserType(type.notation, new ListType(parsedValueType.result().dataType()))));
    }

    private Optional<Parsed<UserType>> parseList2(DecomposedType type) {
        // Parse "list(type)"
        if (!type.dataType.startsWith(LIST_TYPE + ROUND_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(ROUND_BRACKET_CLOSE))
            return Optional.of(Parsed.error("List type not properly closed: " + type.dataType));

        // Parse the type in brackets separately as the type of list elements
        final var valueTypeStr = type.dataType.substring(LIST_TYPE.length() + 1, type.dataType.length() - 1);
        final var parsedValueType = parseTypeAndNotation(valueTypeStr, type.notation, false);
        if (parsedValueType.isError()) return Optional.of(parsedValueType);
        // Return a typed list
        return Optional.of(Parsed.ok(new UserType(type.notation, new ListType(parsedValueType.result().dataType()))));
    }

    private Optional<Parsed<UserType>> parseEnum(DecomposedType type) {
        // Parse "enum(literal1, literal2, ...)"
        if (!type.dataType.startsWith(ENUM_TYPE + ROUND_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(ROUND_BRACKET_CLOSE))
            return Optional.of(Parsed.error("Enum type not properly closed: " + type.dataType));

        // Parse the literals in brackets separately as possible enum values
        final var literalsStr = type.dataType.substring(ENUM_TYPE.length() + 1, type.dataType.length() - 1);
        final var literals = parseListOfLiterals(literalsStr).stream().map(EnumSchema.Symbol::new).toList();
        // Return a new enum type with those literals
        return Optional.of(Parsed.ok(new UserType(type.notation, new EnumType(new EnumSchema(literals)))));
    }

    private Optional<Parsed<UserType>> parseMap(DecomposedType type) {
        // Parse "map(type)"
        if (!type.dataType.startsWith(MAP_TYPE + ROUND_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(ROUND_BRACKET_CLOSE))
            return Optional.of(Parsed.error("Map type not properly closed: " + type.dataType));

        // Parse the type in brackets separately as the type of map elements
        final var valueTypeStr = type.dataType.substring(MAP_TYPE.length() + 1, type.dataType.length() - 1);
        final var parsedValueType = parseTypeAndNotation(valueTypeStr, type.notation, false);
        if (parsedValueType.isError()) return Optional.of(parsedValueType);
        // Return a typed map
        return Optional.of(Parsed.ok(new UserType(type.notation, new MapType(parsedValueType.result().dataType()))));
    }

    private Optional<Parsed<UserType>> parseUnion(DecomposedType type) {
        // Parse "union(type1, type2, ...)"
        if (!type.dataType.startsWith(UNION_TYPE + ROUND_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(ROUND_BRACKET_CLOSE))
            return Optional.of(Parsed.error("Union type not properly closed: " + type.dataType));

        // Parse the types in brackets separately as the union's member types
        final var memberTypesStr = type.dataType.substring(UNION_TYPE.length() + 1, type.dataType.length() - 1);
        final var parsedMemberTypes = parseListOfTypesAndNotation(memberTypesStr, type.notation, false);
        if (parsedMemberTypes.isError()) return Optional.of(Parsed.error(parsedMemberTypes.errorMessage()));
        // Return a new union type with parsed member types
        final var memberTypes = Arrays.stream(UserType.userTypesToDataTypes(parsedMemberTypes.result())).map(UnionType.Member::new).toArray(UnionType.Member[]::new);
        return Optional.of(Parsed.ok(new UserType(type.notation, new UnionType(memberTypes))));
    }

    private Optional<Parsed<UserType>> parseWindowed(DecomposedType type) {
        // Parse "windowed(type)"
        if (!type.dataType.startsWith(WINDOWED_TYPE + ROUND_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(ROUND_BRACKET_CLOSE)) {
            return Optional.of(Parsed.error("Windowed type not properly closed: " + type.dataType));
        }
        // Parse the type in brackets separately as the type of the windowed key
        final var windowedTypeStr = type.dataType.substring(WINDOWED_TYPE.length() + 1, type.dataType.length() - 1);
        // For internal types, first try the parsed notation (infectious notation downwards)
        final var parsedWindowType = parseTypeAndNotation(windowedTypeStr, type.notation, false);
        // Return a typed windowed object
        if (parsedWindowType.isOk())
            return Optional.of(Parsed.ok(new UserType(type.notation, new WindowedType(parsedWindowType.result().dataType()))));
        if (type.explicitNotation())
            return Optional.of(Parsed.error(parsedWindowType.errorMessage())); // Don't silently retry
        // Only fall back to the default notation when the notation was not explicit
        final var parsedWindowType2 = parseTypeAndNotation(windowedTypeStr, DEFAULT_NOTATION, false);
        // Return a typed windowed object
        if (parsedWindowType2.isOk())
            return Optional.of(Parsed.ok(new UserType(type.notation, new WindowedType(parsedWindowType2.result().dataType()))));

        // Return parse error
        return Optional.of(Parsed.error(parsedWindowType2.errorMessage()));
    }

    private Optional<Parsed<UserType>> parseTuple(DecomposedType type) {
        // Parse "(type1, type2, ...)"
        if (!type.dataType.startsWith(ROUND_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(ROUND_BRACKET_CLOSE))
            return Optional.of(Parsed.error("Tuple type not properly closed: " + type.dataType));

        // Parse the type in brackets separately as the tuple's element types
        final var elementTypesStr = type.dataType.substring(1, type.dataType.length() - 1);
        final var parsedElementTypes = parseListOfTypesAndNotation(elementTypesStr, type.notation, false);
        if (parsedElementTypes.isError()) return Optional.of(Parsed.error(parsedElementTypes.errorMessage()));
        // Return a new tuple type with parsed element types
        return Optional.of(Parsed.ok(new UserType(type.notation, new UserTupleType(parsedElementTypes.result()))));
    }

    private Optional<Parsed<UserType>> parseTuple2(DecomposedType type) {
        // Parse "tuple(type1, type2, ...)"
        if (!type.dataType.startsWith(TUPLE_TYPE + ROUND_BRACKET_OPEN))
            return Optional.empty();
        if (!type.dataType.endsWith(ROUND_BRACKET_CLOSE))
            return Optional.of(Parsed.error("Tuple type not properly closed: " + type.dataType));

        // Parse the type in brackets separately as the tuple's element types
        final var elementTypesStr = type.dataType.substring(TUPLE_TYPE.length() + 1, type.dataType.length() - 1);
        final var parsedElementTypes = parseListOfTypesAndNotation(elementTypesStr, type.notation, false);
        if (parsedElementTypes.isError()) return Optional.of(Parsed.error(parsedElementTypes.errorMessage()));
        // Return a new tuple type with parsed element types
        return Optional.of(Parsed.ok(new UserType(type.notation, new UserTupleType(parsedElementTypes.result()))));
    }

    private Optional<Parsed<UserType>> parseNotationWithOrWithoutSchema(DecomposedType type) {
        // Notation type with or without a specific schema
        final var notation = ExecutionContext.INSTANCE.notationLibrary().get(type.notation);

        // If the dataType (i.e., schema) is empty, then return the notation with its default type,
        // or an unresolved type if the notation supports fetching schemas from a remote registry
        if (type.dataType.isEmpty()) {
            // If the notation supports fetching remote schemas, then return an unresolved user type
            if (notation.supportsRemoteSchema())
                return Optional.of(Parsed.ok(new UserType(type.notation, UnresolvedType.INSTANCE)));
            // If the notation requires a schema, then return an error
            if (notation.schemaUsage() == Notation.SchemaUsage.SCHEMA_REQUIRED)
                return Optional.of(Parsed.error("Schema is required for notation " + notation.name() + ". Use \"" + type.notation + ":SchemaName\" to specify the schema."));
            // Return a schemaless user type
            return Optional.of(Parsed.ok(new UserType(type.notation, notation.defaultType())));
        }

        // If the notation only works schemaless, then return an error
        if (notation.schemaUsage() == Notation.SchemaUsage.SCHEMALESS_ONLY)
            return Optional.of(Parsed.error("Schema \"" + type.dataType + "\" can not be used for notation " + notation.name() + ". Use \"" + type.notation + "\" to use the notation schemaless."));

        // Try to load the schema
        final var schema = ExecutionContext.INSTANCE.schemaLibrary().getSchema(notation, type.dataType, false);
        final var schemaType = new DataTypeDataSchemaMapper().fromDataSchema(schema);
        if (notation.defaultType().isAssignableFrom(schemaType).isNotAssignable()) {
            return Optional.of(Parsed.error("Notation does not allow for type: notation=" + notation.name() + ", type=" + schemaType));
        }
        return Optional.of(Parsed.ok(new UserType(type.notation, schemaType)));
    }

    // This method decomposes a user type into its components. User types are always of the form "notation:datatype".
    private DecomposedType decompose(String composedType, String defaultNotation) {
        final var posColon = composedType.contains(NOTATION_SEPARATOR) ? composedType.indexOf(NOTATION_SEPARATOR) : composedType.length();
        final var posOpenRound = composedType.contains(ROUND_BRACKET_OPEN) ? composedType.indexOf(ROUND_BRACKET_OPEN) : composedType.length();
        final var posOpenSquare = composedType.contains(SQUARE_BRACKET_OPEN) ? composedType.indexOf(SQUARE_BRACKET_OPEN) : composedType.length();

        // Check if the user type contains a colon (the user type is of the form "notation:datatype") AND that this
        // colon appears before any brackets (i.e., ignore tuples, lists and other types that use brackets). If so, then
        // parse it as a user type with explicit notation.
        if (posColon < posOpenRound && posColon < posOpenSquare) {
            final var parsedNotation = composedType.substring(0, composedType.indexOf(NOTATION_SEPARATOR));
            final var parsedType = composedType.substring(composedType.indexOf(NOTATION_SEPARATOR) + 1);

            // Return the parsed notation and datatype
            return new DecomposedType(parsedNotation, parsedType, true);
        }

        // Check if the composedType is a reference to a schemaless notation
        if (ExecutionContext.INSTANCE.notationLibrary().exists(composedType)) {
            return new DecomposedType(composedType, "", true);
        }
        // Return the whole type string to be processed further, along with the default notation
        return new DecomposedType(defaultNotation, composedType, false);
    }

    private List<String> parseListOfLiterals(String literals) {
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

    private DataType parseType(String type) {
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

    private Parsed<String> parseLeftMostTerm(String type) {
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
                if (bracketedTerm.isError()) return Parsed.error(bracketedTerm.errorMessage());
                // Skip past the bracketed expression
                index += bracketedTerm.result().length() - 1;
            } else if (!ALLOWED_TYPE_CHARACTERS.contains(ch)) {
                return Parsed.ok(type.substring(0, index));
            }
            index++;
        }
        return Parsed.ok(type);
    }

    private Parsed<String> parseBracketedExpression(String type, String openBracket, String closeBracket) {
        var openCount = 1;
        for (var index = 1; index < type.length(); index++) {
            final var ch = type.substring(index, index + 1);
            if (ch.equals(openBracket)) openCount++;
            if (ch.equals(closeBracket)) openCount--;
            if (openCount == 0) {
                // Return string including both brackets
                return Parsed.ok(type.substring(0, index + 1));
            }
        }
        return Parsed.error("Error in expression: no closing bracket found: " + type);
    }
}
