package io.axual.ksml.parser;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DurationParserTest {
    @Test
    void testDurationParser() {
        assertEquals(Duration.ofMillis(123), DurationParser.parseDuration("123ms", false));
        assertEquals(Duration.ofSeconds(456), DurationParser.parseDuration("456s", false));
        assertEquals(Duration.ofMinutes(789), DurationParser.parseDuration("789m", false));
        assertEquals(Duration.ofHours(123), DurationParser.parseDuration("123h", false));
        assertEquals(Duration.ofDays(456), DurationParser.parseDuration("456d", false));
        assertEquals(Duration.ofDays(7*789), DurationParser.parseDuration("789w", false));
    }
}
