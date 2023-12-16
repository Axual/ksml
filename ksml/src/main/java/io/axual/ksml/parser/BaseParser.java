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
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.execution.FatalError;

import java.time.Duration;
import java.util.function.ToLongFunction;

public abstract class BaseParser<T> implements Parser<T> {
    protected final ParserWithSchema<String> codeParser;
    protected final ParserWithSchema<String> stringValueParser;

    protected BaseParser() {
        final var codeStringParser = new StringValueParser(value -> value ? "True" : "False");
        final var codeSchema = new UnionSchema(DataSchema.booleanSchema(), DataSchema.stringSchema());
        this.codeParser = ParserWithSchema.of(codeStringParser::parse, codeSchema);
        this.stringValueParser = new StringValueParser();
    }

    protected Boolean parseBoolean(YamlNode node, String childName) {
        if (node == null) return null;
        final var child = node.get(childName);
        return child != null && child.isBoolean() ? child.asBoolean() : null;
    }

    protected Duration parseDuration(String durationStr) {
        if (durationStr == null) return null;
        durationStr = durationStr.toLowerCase().trim();
        if (durationStr.length() >= 2) {
            // Prepare a function to extract the number part from a string formatted as "1234x".
            // This function is only applied if a known unit character is found at the end of
            // the duration string.
            ToLongFunction<String> parser = ds -> Long.parseLong(ds.substring(0, ds.length() - 1));

            // If the duration ends with a unit string, then use that for the duration basis
            switch (durationStr.charAt(durationStr.length() - 1)) {
                case 'd' -> {
                    return Duration.ofDays(parser.applyAsLong(durationStr));
                }
                case 'h' -> {
                    return Duration.ofHours(parser.applyAsLong(durationStr));
                }
                case 'm' -> {
                    return Duration.ofMinutes(parser.applyAsLong(durationStr));
                }
                case 's' -> {
                    return Duration.ofSeconds(parser.applyAsLong(durationStr));
                }
                case 'w' -> {
                    return Duration.ofDays(parser.applyAsLong(durationStr) * 7);
                }
            }
        }

        try {
            // If the duration does not contain a valid unit string, assume it is a whole number in millis
            return Duration.ofMillis(Long.parseLong(durationStr));
        } catch (NumberFormatException e) {
            throw FatalError.topologyError("Illegal duration: " + durationStr);
        }
    }

    protected Integer parseInteger(YamlNode node, String childName) {
        return node != null && node.get(childName) != null ? node.get(childName).asInt() : null;
    }

    protected String parseString(YamlNode node, String childName) {
        return node != null ? stringValueParser.parse(node.get(childName)) : null;
    }
}
