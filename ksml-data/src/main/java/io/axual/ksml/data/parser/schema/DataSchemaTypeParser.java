package io.axual.ksml.data.parser.schema;

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

import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.DataSchema;

public class DataSchemaTypeParser extends BaseParser<DataSchema.Type> {
    @Override
    public DataSchema.Type parse(ParseNode node) {
        if (node.isArray()) return DataSchema.Type.UNION;
        if (node.isObject()) {
            var subtype = parseString(node, DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD);
            return parseType(node, subtype);
        }
        if (node.isString()) {
            return parseType(node, node.asString());
        }
        return canNotParse(node);
    }

    private DataSchema.Type parseType(ParseNode node, String type) {
        return switch (type) {
            case DataSchemaDSL.ANY_TYPE -> DataSchema.Type.ANY;
            case DataSchemaDSL.NULL_TYPE -> DataSchema.Type.NULL;
            case DataSchemaDSL.BOOLEAN_TYPE -> DataSchema.Type.BOOLEAN;
            case DataSchemaDSL.BYTE_TYPE -> DataSchema.Type.BYTE;
            case DataSchemaDSL.SHORT_TYPE -> DataSchema.Type.SHORT;
            case DataSchemaDSL.INTEGER_TYPE, DataSchemaDSL.INTEGER_TYPE_ALTERNATIVE -> DataSchema.Type.INTEGER;
            case DataSchemaDSL.LONG_TYPE -> DataSchema.Type.LONG;
            case DataSchemaDSL.DOUBLE_TYPE -> DataSchema.Type.DOUBLE;
            case DataSchemaDSL.FLOAT_TYPE -> DataSchema.Type.FLOAT;
            case DataSchemaDSL.BYTES_TYPE -> DataSchema.Type.BYTES;
            case DataSchemaDSL.FIXED_TYPE -> DataSchema.Type.FIXED;
            case DataSchemaDSL.STRING_TYPE -> DataSchema.Type.STRING;
            case DataSchemaDSL.ENUM_TYPE -> DataSchema.Type.ENUM;
            case DataSchemaDSL.LIST_TYPE, DataSchemaDSL.LIST_TYPE_ALTERNATIVE -> DataSchema.Type.LIST;
            case DataSchemaDSL.MAP_TYPE -> DataSchema.Type.MAP;
            case DataSchemaDSL.STRUCT_TYPE, DataSchemaDSL.STRUCT_TYPE_ALTERNATIVE -> DataSchema.Type.STRUCT;
            default -> canNotParse(node);
        };
    }

    private DataSchema.Type canNotParse(ParseNode node) {
        throw new ParseException(node, "Can not parse schema type: " + node);
    }
}
