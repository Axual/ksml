package io.axual.ksml.parser.schema;

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

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.data.schema.AnySchema;
import io.axual.ksml.data.schema.DataSchema;

import static io.axual.ksml.dsl.DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD;

public class DataSchemaParser extends BaseParser<DataSchema> {
    @Override
    public DataSchema parse(YamlNode node) {
        var typeNode = node.isObject() ? node.get(DATA_SCHEMA_TYPE_FIELD) : node;
        var parseNode = typeNode.isObject() || typeNode.isArray() ? typeNode : node;
        var schemaType = new DataSchemaTypeParser().parse(typeNode);
        return switch (schemaType) {
            case ANY -> AnySchema.INSTANCE;
            case NULL, BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BYTES, STRING ->
                    DataSchema.create(schemaType);
            case FIXED -> new FixedSchemaParser().parse(parseNode);
            case ENUM -> new EnumSchemaParser().parse(parseNode);
            case LIST -> new ListSchemaParser().parse(parseNode);
            case MAP -> new MapSchemaParser().parse(parseNode);
            case STRUCT -> new StructSchemaParser().parse(parseNode);
            case UNION -> new UnionSchemaParser().parse(parseNode);
        };
    }
}
