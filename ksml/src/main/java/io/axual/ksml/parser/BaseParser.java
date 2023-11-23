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


import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.execution.FatalError;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public abstract class BaseParser<T> {
    private String defaultName;

    protected String getDefaultName() {
        return defaultName;
    }

    public void setDefaultName(String defaultName) {
        this.defaultName = defaultName;
    }

    protected interface BooleanToStringConverter {
        String interpret(boolean value);
    }

    private final BooleanToStringConverter booleanToStringConverter;

    public BaseParser() {
        booleanToStringConverter = value -> value ? "true" : "false";
    }

    public BaseParser(BooleanToStringConverter handler) {
        this.booleanToStringConverter = handler;
    }

    public abstract T parse(YamlNode node);

    protected Boolean parseBoolean(YamlNode parent, String childName) {
        return parseBoolean(parent, childName, false);
    }

    protected Boolean parseBoolean(YamlNode parent, String childName, boolean valueIfNull) {
        YamlNode child = parent.get(childName);
        return child != null ? child.asBoolean() : valueIfNull;
    }

    protected Duration parseDuration(YamlNode parent, String childName) {
        return parseDuration(parent, childName, (Duration) null);
    }

    protected Duration parseDuration(YamlNode parent, String childName, String errorMessageIfNull) {
        final var result = parseDuration(parent, childName, (Duration) null);
        if (result == null && errorMessageIfNull != null) {
            throw FatalError.parseError(parent, errorMessageIfNull);
        }
        return result;
    }

    protected Duration parseDuration(YamlNode parent, String childName, Duration valueIfNull) {
        String durationStr = parseString(parent, childName);
        if (durationStr == null) return valueIfNull;
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
            throw new KSMLParseException(parent, "Illegal duration: " + durationStr);
        }
    }

    protected String[] parseMultilineText(YamlNode parent, String childName) {
        return parseMultilineText(parent, childName, null);
    }

    protected String[] parseMultilineText(YamlNode parent, String childName, String[] valueIfNull) {
        var result = parseTextAndTransform(parent, childName, s -> s.split("\\r?\\n"));
        return result != null ? result : valueIfNull;
    }

    protected int parseInteger(YamlNode parent, String childName) {
        return parseInteger(parent, childName, 0);
    }

    protected int parseInteger(YamlNode parent, String childName, int valueIfNull) {
        YamlNode child = parent.get(childName);
        return child != null ? child.asInt() : valueIfNull;
    }

    protected String parseString(YamlNode parent, String childName) {
        return parseString(parent, childName, null);
    }

    protected String parseString(YamlNode parent, String childName, String valueIfNull) {
        return parseStringValue(parent.get(childName), valueIfNull);
    }

    protected String parseStringValue(YamlNode node) {
        return parseStringValue(node, null);
    }

    protected String parseStringValue(YamlNode node, String valueIfNull) {
        // The following line catches a corner case, where Jackson parses a string as boolean, whereas it was meant
        // to be interpreted as a string literal for Python.
        if (node != null && node.isBoolean()) return booleanToStringConverter.interpret(node.asBoolean());
        return node != null ? node.asString() : valueIfNull;
    }

    protected <S> S parseTextAndTransform(YamlNode parent, String childName, Function<String, S> transform) {
        String result = parseString(parent, childName);
        return result != null ? transform.apply(result) : null;
    }
}
