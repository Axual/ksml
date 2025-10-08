package io.axual.ksml.parser;

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

import io.axual.ksml.exception.ParseException;

import javax.annotation.Nullable;

public abstract class BaseParser<T> implements Parser<T> {
    @Nullable
    protected Boolean parseBoolean(ParseNode node, String childName) {
        if (node == null) return null;
        final var child = node.get(childName);
        return child != null && isValue(child, "boolean") && child.isBoolean() ? child.asBoolean() : null;
    }

    protected boolean parseBoolean(ParseNode node, String childName, boolean defaultValue) {
        final var result = parseBoolean(node, childName);
        return result != null ? result : defaultValue;
    }

    @Nullable
    protected Integer parseInteger(ParseNode node, String childName) {
        if (node == null) return null;
        final var child = node.get(childName);
        return child != null && isValue(child, "integer") && child.isInt() ? child.asInt() : null;
    }

    @Nullable
    protected String parseString(ParseNode node, String childName) {
        if (node == null) return null;
        final var child = node.get(childName);
        return child != null && isValue(child, "string") && !child.isNull() ? child.asString() : null;
    }

    private boolean isValue(ParseNode node, String expectedType) {
        if (node.isObject()) throw new ParseException(node, "Expected type " + expectedType + ", found object");
        return true;
    }
}
