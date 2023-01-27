package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UserTupleType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.notation.avro.AvroNotation;
import io.axual.ksml.notation.csv.CsvNotation;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.notation.soap.SOAPNotation;
import io.axual.ksml.notation.xml.XmlNotation;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.data.schema.StructSchema;

import static io.axual.ksml.data.type.UserType.UNKNOWN;

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
        UserType[] types = parseListOfTypesAndNotation(type, UserType.DEFAULT_NOTATION, true);
        if (types.length == 1) {
            return types[0];
        }
        throw new KSMLParseException("Could not parse data type: " + type);
    }

    // Parses a list of comma-separated user data types. If no comma is found, then the returned
    // list only contains one dataType.
    private static UserType[] parseListOfTypesAndNotation(String type, String defaultNotation, boolean allowOverrideNotation) {
        if (type == null || type.isEmpty()) {
            return new UserType[]{UNKNOWN};
        }
        type = type.trim();

        String leftTerm = parseLeftMostTerm(type);
        String remainder = type.substring(leftTerm.length()).trim();
        UserType leftTermType = parseTypeAndNotation(leftTerm, defaultNotation, allowOverrideNotation);
        var remainderTypes = new UserType[0];
        if (remainder.startsWith(TYPE_SEPARATOR)) {
            remainderTypes = parseListOfTypesAndNotation(remainder.substring(1), defaultNotation, false);
        } else if (!remainder.isEmpty()) {
            throw new KSMLParseException("Could not parse data type: " + type);
        }

        var result = new UserType[remainderTypes.length + 1];
        result[0] = leftTermType;
        System.arraycopy(remainderTypes, 0, result, 1, remainderTypes.length);
        return result;
    }

    private static UserType parseTypeAndNotation(String type, String defaultNotation, boolean allowOverrideNotation) {
        var resultNotation = defaultNotation;
        var typeNotation = defaultNotation;

        var posColon = type.contains(NOTATION_SEPARATOR) ? type.indexOf(NOTATION_SEPARATOR) : type.length();
        var posOpenRound = type.contains(ROUND_BRACKET_OPEN) ? type.indexOf(ROUND_BRACKET_OPEN) : type.length();
        var posOpenSquare = type.contains(SQUARE_BRACKET_OPEN) ? type.indexOf(SQUARE_BRACKET_OPEN) : type.length();

        // Extract any explicit notation from the type
        if (posColon < posOpenRound && posColon < posOpenSquare) {
            typeNotation = type.substring(0, type.indexOf(NOTATION_SEPARATOR));
            type = type.substring(type.indexOf(NOTATION_SEPARATOR) + 1);

            if (allowOverrideNotation) {
                resultNotation = typeNotation.toUpperCase();
            }
        }

        // List type
        if (type.startsWith(SQUARE_BRACKET_OPEN)) {
            if (!type.endsWith(SQUARE_BRACKET_CLOSE)) {
                throw new KSMLParseException("Error in data type: " + type);
            }
            var valueType = parseTypeAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            return new UserType(valueType.notation(), new ListType(valueType.dataType()));
        }

        // Tuple type
        if (type.startsWith(ROUND_BRACKET_OPEN)) {
            if (!type.endsWith(ROUND_BRACKET_CLOSE)) {
                throw new KSMLParseException("Error in data type: " + type);
            }
            var valueTypes = parseListOfTypesAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            return new UserType(resultNotation, new UserTupleType(valueTypes));
        }

        // enum(literal1,literal2,...)
        if (type.startsWith(ENUM_TYPE + ROUND_BRACKET_OPEN) && type.endsWith(ROUND_BRACKET_CLOSE)) {
            var literals = type.substring(ENUM_TYPE.length() + 1, type.length() - 1);
            return new UserType(resultNotation, new EnumType(parseListOfLiterals(literals)));
        }

        // union(type1,type2,...)
        if (type.startsWith(UNION_TYPE + ROUND_BRACKET_OPEN) && type.endsWith(ROUND_BRACKET_CLOSE)) {
            type = type.substring(UNION_TYPE.length() + 1, type.length() - 1);
            return new UserType(resultNotation, new UnionType(parseListOfTypesAndNotation(type, resultNotation, true)));
        }

        // windowed(type)
        if (type.startsWith(WINDOWED_TYPE + ROUND_BRACKET_OPEN) && type.endsWith(ROUND_BRACKET_CLOSE)) {
            type = type.substring(WINDOWED_TYPE.length() + 1, type.length() - 1);
            return new UserType(resultNotation, new WindowedType(parseType(type)));
        }

        // AVRO with schema
        if (typeNotation.equalsIgnoreCase(AvroNotation.NOTATION_NAME)) {
            var schema = SchemaLibrary.getSchema(type, false);
            if (!(schema instanceof StructSchema structSchema))
                throw new KSMLParseException("Schema definition is not a STRUCT: " + type);
            return new UserType(AvroNotation.NOTATION_NAME, new StructType(structSchema));
        }

        // AVRO without schema
        if (type.equalsIgnoreCase(AvroNotation.NOTATION_NAME)) {
            return new UserType(AvroNotation.NOTATION_NAME, AvroNotation.DEFAULT_TYPE);
        }

        // CSV without schema
        if (type.equalsIgnoreCase(CsvNotation.NOTATION_NAME)) {
            return new UserType(CsvNotation.NOTATION_NAME, CsvNotation.DEFAULT_TYPE);
        }

        // JSON with schema (not implemented yet)
        if (typeNotation.equalsIgnoreCase(JsonNotation.NOTATION_NAME)) {
            return new UserType(JsonNotation.NOTATION_NAME, JsonNotation.DEFAULT_TYPE);
        }

        // JSON without schema
        if (type.equalsIgnoreCase(JsonNotation.NOTATION_NAME)) {
            return new UserType(JsonNotation.NOTATION_NAME, JsonNotation.DEFAULT_TYPE);
        }

        // SOAP without schema
        if (type.equalsIgnoreCase(SOAPNotation.NOTATION_NAME)) {
            return new UserType(SOAPNotation.NOTATION_NAME, SOAPNotation.DEFAULT_TYPE);
        }

        // SOAP with schema (not implemented yet)
        if (typeNotation.equalsIgnoreCase(SOAPNotation.NOTATION_NAME)) {
            return new UserType(SOAPNotation.NOTATION_NAME, SOAPNotation.DEFAULT_TYPE);
        }

        // XML without schema
        if (type.equalsIgnoreCase(XmlNotation.NOTATION_NAME)) {
            return new UserType(XmlNotation.NOTATION_NAME, XmlNotation.DEFAULT_TYPE);
        }

        // XML with schema
        if (typeNotation.equalsIgnoreCase(XmlNotation.NOTATION_NAME)) {
            var schema = SchemaLibrary.getSchema(type, false);
            if (!(schema instanceof StructSchema structSchema))
                throw new KSMLParseException("Schema definition is not a STRUCT: " + type);
            return new UserType(XmlNotation.NOTATION_NAME, new StructType(structSchema));
        }

        return new UserType(resultNotation, parseType(type));
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
            case UNKNOWN_TYPE -> DataType.UNKNOWN;
            default -> throw new KSMLTopologyException("Can not derive dataType: " + type);
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
        throw new KSMLParseException("Error in expression: no closing bracket found: " + type);
    }
}
