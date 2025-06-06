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

import io.axual.ksml.data.schema.DataSchema;

import java.util.List;

public class StringValueParser implements ParserWithSchemas<String> {
    public interface BooleanToStringConverter {
        String interpret(boolean value);
    }

    private final BooleanToStringConverter converter;

    public StringValueParser() {
        this(null);
    }

    public StringValueParser(BooleanToStringConverter converter) {
        this.converter = converter != null
                ? converter
                : value -> value ? "true" : "false";
    }

    @Override
    public String parse(ParseNode node) {
        // This implementation catches a corner case, where Jackson parses a string as boolean, whereas it was meant
        // to be interpreted as a string literal for Python.
        if (node != null) {
            if (node.isBoolean()) return converter.interpret(node.asBoolean());
            if (node.isDouble()) return "" + node.asDouble();
            if (node.isFloat()) return "" + node.asFloat();
            if (node.isShort()) return "" + node.asShort();
            if (node.isInt()) return "" + node.asInt();
            if (node.isLong()) return "" + node.asLong();
            if (node.isString()) return node.asString();
            if (node.isArray()) {
                // Sometimes devs forget quotes around function results that contain square brackets. This makes
                // Jackson interpret it as a YAML list, instead of the string literal it was intended as. This
                // code tries to be forgiving for those situations by reconstructing the intended string.
                final var result = new StringBuilder("[");
                var first = true;
                for (final var child : node.children(null, null)) {
                    if (!first) result.append(",");
                    if (child.isString()) result.append(child.asString());
                    first = false;
                }
                return result.append("]").toString();
            }
        }
        return null;
    }

    @Override
    public List<DataSchema> schemas() {
        return List.of(DataSchema.STRING_SCHEMA);
    }
}
