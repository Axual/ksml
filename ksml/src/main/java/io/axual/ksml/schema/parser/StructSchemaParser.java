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

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.StructSchema;

import static io.axual.ksml.dsl.DataSchemaDSL.NAMED_SCHEMA_DOC_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.NAMED_SCHEMA_NAMESPACE_FIELD;
import static io.axual.ksml.dsl.DataSchemaDSL.NAMED_SCHEMA_NAME_FIELD;

public class StructSchemaParser extends BaseParser<StructSchema> {
    @Override
    public StructSchema parse(YamlNode node) {
        var namespace = node.get(NAMED_SCHEMA_NAMESPACE_FIELD).asString();
        var name = node.get(NAMED_SCHEMA_NAME_FIELD).asString();
        var doc = node.get(NAMED_SCHEMA_DOC_FIELD).asString();
        var fields = new DataFieldsParser().parse(node);
        return new StructSchema(namespace, name, doc, fields);
    }
}
