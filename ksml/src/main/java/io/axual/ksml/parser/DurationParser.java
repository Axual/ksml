package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.exception.TopologyException;
import lombok.Getter;

import java.time.Duration;
import java.util.List;
import java.util.function.ToLongFunction;

@Getter
public class DurationParser implements ParserWithSchemas<Duration> {
    private final List<DataSchema> schemas = List.of(new UnionSchema(
            new DataField(null, DataSchema.LONG_SCHEMA, "Duration in milliseconds"),
            new DataField(null, DataSchema.STRING_SCHEMA, "Duration with unit")));

    @Override
    public Duration parse(ParseNode node) {
        return parseDuration(new StringValueParser().parse(node), false);
    }

    public static Duration parseDuration(String durationStr, boolean allowFail) {
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
                default -> {
                    // Parsed below as long
                }
            }
        }

        try {
            // If the duration does not contain a valid unit string, assume it is a whole number in millis
            return Duration.ofMillis(Long.parseLong(durationStr));
        } catch (NumberFormatException e) {
            if (allowFail) return null;
            throw new TopologyException(
                    String.format(
                            "Invalid duration format: '%s'. Expected format: <number><unit> where unit is one of: s, m, h, d, w. Example: '5m' for 5 minutes",
                            durationStr));
        }
    }
}
