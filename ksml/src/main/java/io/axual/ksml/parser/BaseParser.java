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

import io.axual.ksml.exception.KSMLParseException;

public abstract class BaseParser<T> {
    public abstract T parse(YamlNode node);

    protected Boolean parseBoolean(YamlNode parent, String childName) {
        YamlNode child = parent.get(childName);
        return child != null ? child.asBoolean() : null;
    }

    protected Duration parseDuration(YamlNode parent, String childName) {
        String durationStr = parseText(parent, childName);
        if (durationStr == null) return null;
        durationStr = durationStr.toLowerCase().trim();
        if (durationStr.length() >= 2) {
            // If the duration ends with a unit string, then use that for the duration basis
            switch (durationStr.charAt(durationStr.length() - 1)) {
                case 'd':
                    return Duration.ofDays(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
                case 'h':
                    return Duration.ofHours(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
                case 'm':
                    return Duration.ofMinutes(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
                case 's':
                    return Duration.ofSeconds(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
                case 'w':
                    return Duration.ofDays(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)) * 7);
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

    protected String parseText(YamlNode parent, String childName) {
        YamlNode child = parent.get(childName);
        return child != null ? child.asText() : null;
    }

    protected <S> S parseTextAndTransform(YamlNode parent, String childName, Function<String, S> transform) {
        String result = parseText(parent, childName);
        return result != null ? transform.apply(result) : null;
    }
}
