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


import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.type.AvroType;
import io.axual.ksml.type.DataType;
import io.axual.ksml.type.ListType;
import io.axual.ksml.type.StandardType;
import io.axual.ksml.type.TupleType;

public class TypeParser {
    private static final String ALLOWED_TYPE_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:.?";

    private TypeParser() {
    }

    public static DataType parse(String type) {
        DataType[] types = parseListOfTypes(type);
        if (types.length == 1) {
            return types[0];
        }
        throw new KSMLParseException("Could not parse data type: " + type);
    }

    private static DataType[] parseListOfTypes(String type) {
        if (type == null || type.isEmpty()) {
            return new DataType[]{DataType.UNKNOWN};
        }
        type = type.trim();

        String leftTerm = parseLeftMostTerm(type);
        String remainder = type.substring(leftTerm.length()).trim();
        DataType leftTermType = parseType(leftTerm);
        var remainderTypes = new DataType[0];
        if (remainder.startsWith(",")) {
            remainderTypes = parseListOfTypes(remainder.substring(1));
        } else if (!remainder.isEmpty()) {
            throw new KSMLParseException("Could not parse type: " + type);
        }

        var result = new DataType[remainderTypes.length + 1];
        result[0] = leftTermType;
        System.arraycopy(remainderTypes, 0, result, 1, remainderTypes.length);
        return result;
    }

    private static DataType parseType(String type) {
        // This method assumes no compound types are passed in, unless surrounded by brackets
        if (type.startsWith("[")) {
            if (!type.endsWith("]")) {
                throw new KSMLParseException("Error in type: " + type);
            }
            DataType valueType = parseType(type.substring(1, type.length() - 1));
            return new ListType(valueType);
        }

        if (type.startsWith("(")) {
            if (!type.endsWith(")")) {
                throw new KSMLParseException("Error in type: " + type);
            }
            DataType[] valueTypes = parseListOfTypes(type.substring(1, type.length() - 1));
            return new TupleType(valueTypes);
        }

        if (type.startsWith("avro:")) {
            var schemaName = type.substring(5);
            var schema = SchemaLoader.load(schemaName);
            if (schema == null) {
                throw new KSMLParseException("Could not load schema definition: " + schemaName);
            }
            return new AvroType(schemaName, schema);
        }

        switch (type) {
            case "boolean":
                return StandardType.BOOLEAN;
            case "double":
                return StandardType.DOUBLE;
            case "float":
                return StandardType.FLOAT;
            case "int":
                return StandardType.INTEGER;
            case "json":
                return StandardType.JSON;
            case "long":
                return StandardType.LONG;
            case "?":
            case "none":
                return null;
            case "str":
            case "string":
                return StandardType.STRING;
            case "bytes":
                return StandardType.BYTES;
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

    private static String parseBracketedExpression(String type, String openBracket, String closeBracket) {
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
