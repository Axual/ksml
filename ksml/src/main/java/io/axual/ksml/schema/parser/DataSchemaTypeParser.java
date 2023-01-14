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

import static io.axual.ksml.dsl.DataSchemaDSL.ANY_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.BOOLEAN_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.BYTES_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.BYTE_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.DOUBLE_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.ENUM_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.FIXED_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.FLOAT_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.INTEGER_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.INTEGER_TYPE_ALTERNATIVE;
import static io.axual.ksml.dsl.DataSchemaDSL.LIST_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.LIST_TYPE_ALTERNATIVE;
import static io.axual.ksml.dsl.DataSchemaDSL.LONG_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.MAP_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.NULL_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.SHORT_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.STRING_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.STRUCT_TYPE;
import static io.axual.ksml.dsl.DataSchemaDSL.STRUCT_TYPE_ALTERNATIVE;

public class DataSchemaTypeParser extends BaseParser<DataSchema.Type> {
    @Override
    public DataSchema.Type parse(YamlNode node) {
        if (node.isArray()) return DataSchema.Type.UNION;
        if (node.isObject()) {
            var subtype = parseString(node, DATA_SCHEMA_TYPE_FIELD);
            return parseType(node, subtype);
        }
        if (node.isString()) {
            return parseType(node, node.asString());
        }
        return canNotParse(node);
    }

    private DataSchema.Type parseType(YamlNode node, String type) {
        return switch (type) {
            case ANY_TYPE -> DataSchema.Type.ANY;
            case NULL_TYPE -> DataSchema.Type.NULL;
            case BOOLEAN_TYPE -> DataSchema.Type.BOOLEAN;
            case BYTE_TYPE -> DataSchema.Type.BYTE;
            case SHORT_TYPE -> DataSchema.Type.SHORT;
            case INTEGER_TYPE, INTEGER_TYPE_ALTERNATIVE -> DataSchema.Type.INTEGER;
            case LONG_TYPE -> DataSchema.Type.LONG;
            case DOUBLE_TYPE -> DataSchema.Type.DOUBLE;
            case FLOAT_TYPE -> DataSchema.Type.FLOAT;
            case BYTES_TYPE -> DataSchema.Type.BYTES;
            case FIXED_TYPE -> DataSchema.Type.FIXED;
            case STRING_TYPE -> DataSchema.Type.STRING;
            case ENUM_TYPE -> DataSchema.Type.ENUM;
            case LIST_TYPE, LIST_TYPE_ALTERNATIVE -> DataSchema.Type.LIST;
            case MAP_TYPE -> DataSchema.Type.MAP;
            case STRUCT_TYPE, STRUCT_TYPE_ALTERNATIVE -> DataSchema.Type.STRUCT;
            default -> canNotParse(node);
        };
    }

    private DataSchema.Type canNotParse(YamlNode node) {
        throw new KSMLParseException(node, "Can not parse schema type: " + node);
    }
}
