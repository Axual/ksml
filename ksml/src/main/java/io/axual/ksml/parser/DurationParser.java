package io.axual.ksml.parser;

import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.parser.ParserWithSchema;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.exception.TopologyException;
import lombok.Getter;

import java.time.Duration;
import java.util.function.ToLongFunction;

@Getter
public class DurationParser implements ParserWithSchema<Duration> {
    private final DataSchema schema = new UnionSchema(DataSchema.longSchema(), DataSchema.stringSchema());

    @Override
    public Duration parse(ParseNode node) {
        return parseDuration(new StringValueParser().parse(node));
    }

    private Duration parseDuration(String durationStr) {
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
            throw new TopologyException("Illegal duration: " + durationStr);
        }
    }
}
