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
            new UnionSchema.Member(DataSchema.LONG_SCHEMA),
            new UnionSchema.Member(DataSchema.STRING_SCHEMA)));

    @Override
    public Duration parse(ParseNode node) {
        return parseDuration(new StringValueParser().parse(node), false);
    }

    public static Duration parseDuration(String durationStr, boolean allowFail) {
        if (durationStr == null) return null;
        durationStr = durationStr.toLowerCase().trim();

        try {
            final ToLongFunction<String> parser1 = ds -> Long.parseLong(ds.substring(0, ds.length() - 1).trim());
            final ToLongFunction<String> parser2 = ds -> Long.parseLong(ds.substring(0, ds.length() - 2).trim());

            // If the duration ends with "ms", then parse the remainder as a whole number of milliseconds
            if (durationStr.endsWith("ms")) {
                return Duration.ofMillis(parser2.applyAsLong(durationStr));
            }
            // If the duration ends with "s", then parse the remainder as a whole number of seconds
            if (durationStr.endsWith("s")) {
                return Duration.ofSeconds(parser1.applyAsLong(durationStr));
            }
            // If the duration ends with "m", then parse the remainder as a whole number of minutes
            if (durationStr.endsWith("m")) {
                return Duration.ofMinutes(parser1.applyAsLong(durationStr));
            }
            // If the duration ends with "h", then parse the remainder as a whole number of hours
            if (durationStr.endsWith("h")) {
                return Duration.ofHours(parser1.applyAsLong(durationStr));
            }
            // If the duration ends with "d", then parse the remainder as a whole number of days
            if (durationStr.endsWith("d")) {
                return Duration.ofDays(parser1.applyAsLong(durationStr));
            }
            // If the duration ends with "w", then parse the remainder as a whole number of weeks
            if (durationStr.endsWith("w")) {
                return Duration.ofDays(7 * parser1.applyAsLong(durationStr));
            }
            // If the duration does not contain a valid unit string, parse it as a whole number of milliseconds
            return Duration.ofMillis(Long.parseLong(durationStr));
        } catch (NumberFormatException e) {
            if (allowFail) return null;
            throw new TopologyException(
                    String.format(
                            "Invalid duration format: '%s'. Expected format: <number><unit> where unit is one of: ms, s, m, h, d, w. Example: '5m' for 5 minutes",
                            durationStr));
        }
    }
}
