package io.axual.ksml.schema.parser;

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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;

public class DataSchemaParser extends BaseParser<DataSchema> {
    @Override
    public DataSchema parse(ParseNode node) {
        if (node == null) return null;
        final var typeNode = node.isObject() ? node.get(DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD) : node;
        final var parseNode = typeNode.isObject() || typeNode.isArray() ? typeNode : node;
        final var schemaType = new DataSchemaTypeParser().parse(typeNode);
        if (schemaType == null) {
            throw new ParseException(node, "Schema type is missing");
        }
        return switch (schemaType) {
            case DataSchemaConstants.ANY_TYPE -> DataSchema.ANY_SCHEMA;
            case DataSchemaConstants.NULL_TYPE -> DataSchema.NULL_SCHEMA;
            case DataSchemaConstants.BOOLEAN_TYPE -> DataSchema.BOOLEAN_SCHEMA;
            case DataSchemaConstants.BYTE_TYPE -> DataSchema.BYTE_SCHEMA;
            case DataSchemaConstants.SHORT_TYPE -> DataSchema.SHORT_SCHEMA;
            case DataSchemaConstants.INTEGER_TYPE -> DataSchema.INTEGER_SCHEMA;
            case DataSchemaConstants.LONG_TYPE -> DataSchema.LONG_SCHEMA;
            case DataSchemaConstants.FLOAT_TYPE -> DataSchema.FLOAT_SCHEMA;
            case DataSchemaConstants.DOUBLE_TYPE -> DataSchema.DOUBLE_SCHEMA;
            case DataSchemaConstants.BYTES_TYPE -> DataSchema.BYTES_SCHEMA;
            case DataSchemaConstants.FIXED_TYPE -> new FixedSchemaParser().parse(parseNode);
            case DataSchemaConstants.STRING_TYPE -> DataSchema.STRING_SCHEMA;
            case DataSchemaConstants.ENUM_TYPE -> new EnumSchemaParser().parse(parseNode);
            case DataSchemaConstants.LIST_TYPE -> new ListSchemaParser().parse(parseNode);
            case DataSchemaConstants.MAP_TYPE -> new MapSchemaParser().parse(parseNode);
            case DataSchemaConstants.STRUCT_TYPE -> new StructSchemaParser().parse(parseNode);
            case DataSchemaConstants.UNION_TYPE -> new UnionSchemaParser().parse(parseNode);
            default -> throw new ParseException(node, "Unknown schema type: " + schemaType);
        };
    }
}
