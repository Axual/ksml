package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DurationParserTest {
    @Test
    void testDurationParser() {
        assertEquals(Duration.ofMillis(123), DurationParser.parseDuration("123ms", false));
        assertEquals(Duration.ofMillis(123), DurationParser.parseDuration("123       ms", false));
        assertEquals(Duration.ofSeconds(456), DurationParser.parseDuration("456s", false));
        assertEquals(Duration.ofSeconds(456), DurationParser.parseDuration("456  s", false));
        assertEquals(Duration.ofMinutes(789), DurationParser.parseDuration("789m", false));
        assertEquals(Duration.ofMinutes(789), DurationParser.parseDuration("789   m", false));
        assertEquals(Duration.ofHours(123), DurationParser.parseDuration("123h", false));
        assertEquals(Duration.ofHours(123), DurationParser.parseDuration("123 h", false));
        assertEquals(Duration.ofDays(456), DurationParser.parseDuration("456d", false));
        assertEquals(Duration.ofDays(456), DurationParser.parseDuration("456    d", false));
        assertEquals(Duration.ofDays(7*789), DurationParser.parseDuration("789w", false));
        assertEquals(Duration.ofDays(7*789), DurationParser.parseDuration("789   w", false));
    }
}
