package io.axual.ksml.data.parser;

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

public abstract class BaseParser<T> implements Parser<T> {
    protected final ParserWithSchema<String> stringValueParser;

    protected BaseParser() {
        this.stringValueParser = null;
    }

    protected BaseParser(StringValueParser stringValueParser) {
        this.stringValueParser = stringValueParser;
    }

    protected Boolean parseBoolean(ParseNode node, String childName) {
        if (node == null) return null;
        final var child = node.get(childName);
        return child != null && child.isBoolean() ? child.asBoolean() : null;
    }

    protected Integer parseInteger(ParseNode node, String childName) {
        return node != null && node.get(childName) != null ? node.get(childName).asInt() : null;
    }

    protected String parseString(ParseNode node, String childName) {
        if (node != null) {
            ParseNode value = node.get(childName);
            if (stringValueParser != null) return stringValueParser.parse(value);
            return value != null ? value.asString() : null;
        }
        return null;
    }
}
