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

import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;

public class DataSchemaTypeParser extends BaseParser<String> {
    @Override
    public String parse(ParseNode node) {
        if (node.isArray()) return DataSchemaConstants.UNION_TYPE;
        if (node.isObject()) {
            final var subtype = parseString(node, DataSchemaDSL.DATA_SCHEMA_TYPE_FIELD);
            if (DataSchemaConstants.isType(subtype)) return subtype;
        }
        if (node.isString()) {
            final var type = node.asString();
            if (DataSchemaConstants.isType(type)) return type;
        }
        throw new ParseException(node, "Can not parse schema type: " + node);
    }
}
