package io.axual.ksml.data.parser.schema;

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

import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ListParser;
import io.axual.ksml.data.parser.MapParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.type.Symbols;

public class SymbolsParser extends BaseParser<Symbols> {
    public Symbols parse(ParseNode node) {
        // If the node is a JSON object, then parse the symbols using the "map of string to symbol metadata" method (KSML 1.1 and newer)
        if (node.isObject()) {
            final var symbols = new MapParser<>("enum-symbol", "symbol", new SymbolMetadataParser()).parse(node);
            return new Symbols(symbols);
        }

        // If the node is an array, then parse the symbols by using the "list of strings" method (KSML 1.0.x and earlier)
        if (node.isArray()) {
            final var symbols = new ListParser<>("enum-symbol", "symbol", new LegacySymbolParser()).parse(node);
            return Symbols.from(symbols);
        }

        throw new ParseException(node, "Could not parse enum symbols");
    }
}
