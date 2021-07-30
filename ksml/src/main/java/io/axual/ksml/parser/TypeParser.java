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
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataListType;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.DataTypeAndNotation;
import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.notation.AvroNotation;
import io.axual.ksml.notation.BinaryNotation;
import io.axual.ksml.notation.JsonNotation;
import io.axual.ksml.schema.SchemaLibrary;

public class TypeParser {
    private static final String ALLOWED_TYPE_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:.?";

    private TypeParser() {
    }

    public static DataTypeAndNotation parse(String type) {
        return parse(type, BinaryNotation.NAME);
    }

    public static DataTypeAndNotation parse(String type, String defaultNotation) {
        DataTypeAndNotation[] types = parseListOfTypesAndNotation(type, defaultNotation, true);
        if (types.length == 1) {
            return types[0];
        }
        throw new KSMLParseException("Could not parse data type: " + type);
    }

    private static DataTypeAndNotation[] parseListOfTypesAndNotation(String type, String defaultNotation, boolean allowOverrideNotation) {
        if (type == null || type.isEmpty()) {
            return new DataTypeAndNotation[]{new DataTypeAndNotation(DataType.UNKNOWN, defaultNotation)};
        }
        type = type.trim();

        String leftTerm = parseLeftMostTerm(type);
        String remainder = type.substring(leftTerm.length()).trim();
        DataTypeAndNotation leftTermType = parseTypeAndNotation(leftTerm, defaultNotation, allowOverrideNotation);
        var remainderTypes = new DataTypeAndNotation[0];
        if (remainder.startsWith(",")) {
            remainderTypes = parseListOfTypesAndNotation(remainder.substring(1), defaultNotation, false);
        } else if (!remainder.isEmpty()) {
            throw new KSMLParseException("Could not parse type: " + type);
        }

        var result = new DataTypeAndNotation[remainderTypes.length + 1];
        result[0] = leftTermType;
        System.arraycopy(remainderTypes, 0, result, 1, remainderTypes.length);
        return result;
    }

    private static DataTypeAndNotation parseTypeAndNotation(String type, String defaultNotation, boolean allowOverrideNotation) {
        String resultNotation = defaultNotation;
        String typeNotation = defaultNotation;

        if (type.startsWith("[")) {
            if (!type.endsWith("]")) {
                throw new KSMLParseException("Error in type: " + type);
            }
            DataTypeAndNotation valueType = parseTypeAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            return new DataTypeAndNotation(new DataListType(valueType.type), resultNotation);
        }

        if (type.startsWith("(")) {
            if (!type.endsWith(")")) {
                throw new KSMLParseException("Error in type: " + type);
            }
            DataTypeAndNotation[] subTypes = parseListOfTypesAndNotation(type.substring(1, type.length() - 1), resultNotation, false);
            DataType[] valueTypes = new DataType[subTypes.length];
            for (int index = 0; index < subTypes.length; index++) {
                valueTypes[index] = subTypes[index].type;
            }
            return new DataTypeAndNotation(new TupleType(valueTypes), resultNotation);
        }

        if (type.contains(":")) {
            typeNotation = type.substring(0, type.indexOf(":"));
            type = type.substring(type.indexOf(":") + 1);

            if (allowOverrideNotation) {
                resultNotation = typeNotation.toUpperCase();
            }
        }

        if (typeNotation.equalsIgnoreCase(AvroNotation.NAME)) {
            var schema = SchemaLibrary.getSchema(type);
            if (schema == null) {
                throw new KSMLParseException("Could not load schema definition: " + type);
            }
            return new DataTypeAndNotation(new RecordType(schema), resultNotation);
        }

        if (typeNotation.equalsIgnoreCase(JsonNotation.NAME) || type.equalsIgnoreCase(JsonNotation.NAME)) {
            return new DataTypeAndNotation(new RecordType(null), JsonNotation.NAME);
        }

        return new DataTypeAndNotation(parseType(type, resultNotation), resultNotation);
    }

    private static DataType parseType(String type, String notation) {
        switch (type) {
            case "boolean":
                return DataBoolean.TYPE;
            case "byte":
                return DataByte.TYPE;
            case "bytes":
                return DataBytes.TYPE;
            case "short":
                return DataShort.TYPE;
            case "double":
                return DataDouble.TYPE;
            case "float":
                return DataFloat.TYPE;
            case "int":
                return DataInteger.TYPE;
            case "long":
                return DataLong.TYPE;
            case "?":
                return DataType.UNKNOWN;
            case "none":
                return null;
            case "str":
            case "string":
                return DataString.TYPE;
            default:
                throw new KSMLTopologyException("Can not derive type: " + type);
        }
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
