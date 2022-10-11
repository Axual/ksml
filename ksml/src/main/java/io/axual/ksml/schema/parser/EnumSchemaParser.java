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
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.EnumSchema;
import io.axual.ksml.schema.SchemaWriter;

public class EnumSchemaParser extends BaseParser<EnumSchema> {
    @Override
    public EnumSchema parse(YamlNode node) {
        return new EnumSchema(
                parseText(node, SchemaWriter.NAMEDSCHEMA_NAMESPACE_FIELD),
                parseText(node, SchemaWriter.NAMEDSCHEMA_NAME_FIELD),
                parseText(node, SchemaWriter.NAMEDSCHEMA_DOC_FIELD),
                new ListParser<>(new SymbolParser()).parse(node.get(SchemaWriter.ENUMSCHEMA_POSSIBLEVALUES_FIELD)),
                parseText(node, SchemaWriter.ENUMSCHEMA_DEFAULTVALUE_FIELD));
    }
}