package io.axual.ksml.schema.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataSchema;

import static io.axual.ksml.data.mapper.DataSchemaConstants.DATASCHEMA_TYPE_FIELD;

public class DataSchemaTypeParser extends BaseParser<DataSchema.Type> {
    @Override
    public DataSchema.Type parse(YamlNode node) {
        if (node.isArray()) return DataSchema.Type.UNION;
        if (node.isObject()) {
            var subtype = parseString(node, DATASCHEMA_TYPE_FIELD);
            return parseType(node, subtype);
        }
        if (node.isString()) {
            return parseType(node, node.asString());
        }
        return canNotParse(node);
    }

    private DataSchema.Type parseType(YamlNode node, String type) {
        return switch (type) {
            case "null" -> DataSchema.Type.NULL;
            case "byte" -> DataSchema.Type.BYTE;
            case "short" -> DataSchema.Type.SHORT;
            case "int", "integer" -> DataSchema.Type.INTEGER;
            case "long" -> DataSchema.Type.LONG;
            case "double" -> DataSchema.Type.DOUBLE;
            case "float" -> DataSchema.Type.FLOAT;
            case "bytes" -> DataSchema.Type.BYTES;
            case "fixed" -> DataSchema.Type.FIXED;
            case "string" -> DataSchema.Type.STRING;
            case "enum" -> DataSchema.Type.ENUM;
            case "array", "list" -> DataSchema.Type.LIST;
            case "map" -> DataSchema.Type.MAP;
            case "record", "struct" -> DataSchema.Type.STRUCT;
            default -> canNotParse(node);
        };
    }

    private DataSchema.Type canNotParse(YamlNode node) {
        throw new KSMLParseException("Can not parse schema type: " + node);
    }
}
