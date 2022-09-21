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
import io.axual.ksml.data.object.DataNone;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.notation.AvroNotation;
import io.axual.ksml.notation.JsonNotation;
import io.axual.ksml.schema.RecordSchema;
import io.axual.ksml.schema.SchemaLibrary;

public class UserTypeParser {
    private static final String ALLOWED_TYPE_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:.?()";
    private static final String WINDOWED_TYPE = "windowed";

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
        throw new KSMLParseException("Could not parse data type: " + type);
    }

    // Parses a list of comma-separated user data types. If no comma is found, then the returned
    // list only contains one type.
    private static UserType[] parseListOfTypesAndNotation(String type, String defaultNotation, boolean allowOverrideNotation) {
        if (type == null || type.isEmpty()) {
            return new UserType[]{new UserType(DataType.UNKNOWN, defaultNotation)};
        }
        type = type.trim();

        String leftTerm = parseLeftMostTerm(type);
        String remainder = type.substring(leftTerm.length()).trim();
        UserType leftTermType = parseTypeAndNotation(leftTerm, defaultNotation, allowOverrideNotation);
        var remainderTypes = new UserType[0];
        if (remainder.startsWith(",")) {
            remainderTypes = parseListOfTypesAndNotation(remainder.substring(1), defaultNotation, false);
        } else if (!remainder.isEmpty()) {
            throw new KSMLParseException("Could not parse type: " + type);
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
                throw new KSMLParseException("Error in type: " + type);
            }
            UserType valueType = parseTypeAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            return new UserType(new ListType(valueType.type()), valueType.notation());
        }

        if (type.startsWith("(")) {
            if (!type.endsWith(")")) {
                throw new KSMLParseException("Error in type: " + type);
            }
            UserType[] valueTypes = parseListOfTypesAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            return new UserType(new TupleType(dataTypesOf(valueTypes)), resultNotation);
        }

        if (type.contains(":")) {
            typeNotation = type.substring(0, type.indexOf(":"));
            type = type.substring(type.indexOf(":") + 1);

            if (allowOverrideNotation) {
                resultNotation = typeNotation.toUpperCase();
            }
        }

        if (type.startsWith(WINDOWED_TYPE + "(") && type.endsWith(")")) {
            type = type.substring(WINDOWED_TYPE.length() + 1, type.length() - 1);
            return new UserType(new WindowedType(parseType(type)), resultNotation);
        }

        if (typeNotation.equalsIgnoreCase(AvroNotation.NOTATION_NAME)) {
            return new UserType(parseAvroType(type), AvroNotation.NOTATION_NAME);
        }

        if (typeNotation.equalsIgnoreCase(JsonNotation.NOTATION_NAME) || type.equalsIgnoreCase(JsonNotation.NOTATION_NAME)) {
            return new UserType(new RecordType(), JsonNotation.NOTATION_NAME);
        }

        return new UserType(parseType(type), resultNotation);
    }

    private static DataType[] dataTypesOf(UserType[] userTypes) {
        var result = new DataType[userTypes.length];
        for (int index = 0; index < userTypes.length; index++) {
            result[index] = userTypes[index].type();
        }
        return result;
    }

    private static DataType parseAvroType(String type) {
        var schema = SchemaLibrary.getSchema(type);

        if (schema instanceof RecordSchema rs) {
            return new RecordType(rs);
        }

        throw new KSMLParseException("Could not load schema definition: " + type);
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
            case "none" -> DataNone.DATATYPE;
            case "str", "string" -> DataString.DATATYPE;
            default -> throw new KSMLTopologyException("Can not derive type: " + type);
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
