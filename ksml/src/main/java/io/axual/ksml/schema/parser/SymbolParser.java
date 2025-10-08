package io.axual.ksml.schema.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;

public class SymbolParser extends BaseParser<EnumSchema.Symbol> {
    @Override
    public EnumSchema.Symbol parse(ParseNode node) {
        if (node.isObject()) {
            return new EnumSchema.Symbol(
                    parseString(node, DataSchemaDSL.ENUM_SYMBOL_NAME_FIELD),
                    parseString(node, DataSchemaDSL.ENUM_SYMBOL_DOC_FIELD),
                    parseInteger(node, DataSchemaDSL.ENUM_SYMBOL_TAG_FIELD));
        }

        if (node.isString()) {
            return new EnumSchema.Symbol(node.asString());
        }

        throw new ParseException(node, "Could not parse enum symbol");
    }
}
