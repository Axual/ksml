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
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.avro.AvroNotation;
import io.axual.ksml.notation.BinaryNotation;
import io.axual.ksml.notation.JsonNotation;
import io.axual.ksml.schema.RecordSchema;
import io.axual.ksml.schema.SchemaLibrary;
import io.axual.ksml.schema.UnionSchema;

public class UserTypeParser {
    private static final String ALLOWED_TYPE_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:.?()";
    private static final String NULLABLE_TYPE = "nullable";
    private static final String WINDOWED_TYPE = "windowed";
    private static final UserType UNKNOWN = new UserType(UserType.DEFAULT_NOTATION, DataType.UNKNOWN, null);

    private UserTypeParser() {
    }

    public static UserType parse(String type) {
        return parse(type, UserType.DEFAULT_NOTATION);
    }

    public static UserType parse(String type, String defaultNotation) {
        UserType[] types = parseListOfTypesAndNotation(type, defaultNotation, true);
        if (types.length == 1) {
            return types[0];
        }
        throw new KSMLParseException("Could not parse data dataType: " + type);
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
        if (remainder.startsWith(",")) {
            remainderTypes = parseListOfTypesAndNotation(remainder.substring(1), defaultNotation, false);
        } else if (!remainder.isEmpty()) {
            throw new KSMLParseException("Could not parse dataType: " + type);
        }

        var result = new UserType[remainderTypes.length + 1];
        result[0] = leftTermType;
        System.arraycopy(remainderTypes, 0, result, 1, remainderTypes.length);
        return result;
    }

    private static UserType parseTypeAndNotation(String type, String defaultNotation, boolean allowOverrideNotation) {
        String resultNotation = defaultNotation;
        String typeNotation = defaultNotation;

        if (type.startsWith("[")) {
            if (!type.endsWith("]")) {
                throw new KSMLParseException("Error in dataType: " + type);
            }
            UserType valueType = parseTypeAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            return new UserType(valueType.notation(), new ListType(valueType.dataType()), null);
        }

        if (type.startsWith("(")) {
            if (!type.endsWith(")")) {
                throw new KSMLParseException("Error in dataType: " + type);
            }
            UserType[] valueTypes = parseListOfTypesAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            return new UserType(resultNotation, new TupleType(dataTypesOf(valueTypes)), null);
        }

        var unbracketedType = type;
        if (type.contains("(") || type.contains("[")) {
            var roundBracket = type.indexOf("(");
            var squareBracket = type.indexOf("[");
            var firstBracket = Math.min(roundBracket > 0 ? roundBracket : type.length(), squareBracket > 0 ? squareBracket : type.length());
            unbracketedType = type.substring(0, firstBracket - 1);
        }
        if (unbracketedType.contains(":")) {
            typeNotation = unbracketedType.substring(0, type.indexOf(":"));
            type = type.substring(unbracketedType.indexOf(":") + 1);

            if (allowOverrideNotation) {
                resultNotation = typeNotation.toUpperCase();
            }
        }

        // If a dataType is nullable, return a union with [null, dataType] as result
        if (type.startsWith(NULLABLE_TYPE + "(") && type.endsWith(")")) {
            type = type.substring(NULLABLE_TYPE.length() + 1, type.length() - 1);
            var nullType = new UserType(BinaryNotation.NOTATION_NAME, DataNull.DATATYPE, null);
            var subType = parse(type);
            return new UserType(resultNotation, new UnionType(nullType, subType), new UnionSchema(nullType.schema(), subType.schema()));
        }

        // If a dataType is windowed, return a WindowedType with the parsed subtype
        if (type.startsWith(WINDOWED_TYPE + "(") && type.endsWith(")")) {
            type = type.substring(WINDOWED_TYPE.length() + 1, type.length() - 1);
            return new UserType(resultNotation, new WindowedType(parseType(type)), null);
        }

        if (typeNotation.equalsIgnoreCase(AvroNotation.NOTATION_NAME)) {
            var schema = SchemaLibrary.getSchema(type, false);
            if (!(schema instanceof RecordSchema recordSchema))
                throw new KSMLParseException("Schema definition is not a RECORD: " + type);
            return new UserType(AvroNotation.NOTATION_NAME, new RecordType(recordSchema), schema);
        }

        if (typeNotation.equalsIgnoreCase(JsonNotation.NOTATION_NAME) || type.equalsIgnoreCase(JsonNotation.NOTATION_NAME)) {
            return new UserType(JsonNotation.NOTATION_NAME, new RecordType(), null);
        }

        return new UserType(resultNotation, parseType(type), null);
    }

    private static DataType[] dataTypesOf(UserType[] userTypes) {
        var result = new DataType[userTypes.length];
        for (int index = 0; index < userTypes.length; index++) {
            result[index] = userTypes[index].dataType();
        }
        return result;
    }

    private static DataType parseType(String type) {
        return switch (type) {
            case "boolean" -> DataBoolean.DATATYPE;
            case "byte" -> DataByte.DATATYPE;
            case "bytes" -> DataBytes.DATATYPE;
            case "short" -> DataShort.DATATYPE;
            case "double" -> DataDouble.DATATYPE;
            case "float" -> DataFloat.DATATYPE;
            case "int" -> DataInteger.DATATYPE;
            case "long" -> DataLong.DATATYPE;
            case "?" -> DataType.UNKNOWN;
            case "none" -> DataNull.DATATYPE;
            case "str", "string" -> DataString.DATATYPE;
            default -> throw new KSMLTopologyException("Can not derive dataType: " + type);
        };
    }

    private static String parseLeftMostTerm(String type) {
        // Check for bracketed expression
        if (type.startsWith("[")) return parseBracketedExpression(type, "[", "]");
        if (type.startsWith("(")) return parseBracketedExpression(type, "(", ")");

        // Scan the literal at the beginning of the string until a non-literal character is found
        for (var index = 0; index < type.length(); index++) {
            var ch = type.substring(index, index + 1);
            if (!ALLOWED_TYPE_CHARACTERS.contains(ch)) {
                return type.substring(0, index);
            }
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
