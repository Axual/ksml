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

import java.util.Optional;

import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.ParseNode;

public class EnumSchemaParser extends BaseParser<EnumSchema> {
    @Override
    public EnumSchema parse(ParseNode node) {
        return new EnumSchema(
                parseString(node, DataSchemaDSL.NAMED_SCHEMA_NAMESPACE_FIELD),
                parseString(node, DataSchemaDSL.NAMED_SCHEMA_NAME_FIELD),
                parseString(node, DataSchemaDSL.NAMED_SCHEMA_DOC_FIELD),
                new ListParser<>("symbols", "symbol", new SymbolParser()).parse(node.get(DataSchemaDSL.ENUM_SCHEMA_SYMBOLS_FIELD)),
                Optional.ofNullable(parseString(node, DataSchemaDSL.ENUM_SCHEMA_DEFAULT_VALUE_FIELD)).map(Symbol::new).orElse(null));
    }
}
