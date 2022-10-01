package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import java.time.Duration;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import io.axual.ksml.exception.KSMLParseException;

public abstract class BaseParser<T> {
    public abstract T parse(YamlNode node);

    protected Boolean parseBoolean(YamlNode parent, String childName) {
        YamlNode child = parent.get(childName);
        return child != null ? child.asBoolean() : null;
    }

    protected boolean parseBoolean(YamlNode parent, String childName, boolean defaultValue) {
        var result = parseBoolean(parent, childName);
        return result != null ? result : defaultValue;
    }

    protected Duration parseDuration(YamlNode parent, String childName) {
        String durationStr = parseText(parent, childName);
        if (durationStr == null) return null;
        durationStr = durationStr.toLowerCase().trim();
        if (durationStr.length() >= 2) {
            // Prepare a function to extract the number part from a string formatted as "1234x".
            // This function is only applied if a known unit character is found at the end of
            // the duration string.
            ToLongFunction<String> parser = ds -> Long.parseLong(ds.substring(0, ds.length() - 1));

            // If the duration ends with a unit string, then use that for the duration basis
            switch (durationStr.charAt(durationStr.length() - 1)) {
                case 'd':
                    return Duration.ofDays(parser.applyAsLong(durationStr));
                case 'h':
                    return Duration.ofHours(parser.applyAsLong(durationStr));
                case 'm':
                    return Duration.ofMinutes(parser.applyAsLong(durationStr));
                case 's':
                    return Duration.ofSeconds(parser.applyAsLong(durationStr));
                case 'w':
                    return Duration.ofDays(parser.applyAsLong(durationStr) * 7);
                default:
            }
        }

        try {
            // If the duration does not contain a valid unit string, assume it is a whole number in millis
            return Duration.ofMillis(Long.parseLong(durationStr));
        } catch (NumberFormatException e) {
            throw new KSMLParseException(parent, "Illegal duration: " + durationStr);
        }
    }

    protected String[] parseMultilineText(YamlNode parent, String childName) {
        return parseTextAndTransform(parent, childName, s -> s.split("\\r?\\n"));
    }

    protected int parseInteger(YamlNode parent, String childName) {
        YamlNode child = parent.get(childName);
        return child != null ? child.asInt() : 0;
    }

    protected String parseText(YamlNode parent, String childName) {
        YamlNode child = parent.get(childName);
        return child != null ? child.asText() : null;
    }

    protected <S> S parseTextAndTransform(YamlNode parent, String childName, Function<String, S> transform) {
        String result = parseText(parent, childName);
        return result != null ? transform.apply(result) : null;
    }
}
