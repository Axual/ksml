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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class DurationParserTest {
    @Test
    void testDurationParser() {
        assertThat(DurationParser.parseDuration("123ms", false)).isEqualTo(Duration.ofMillis(123));
        assertThat(DurationParser.parseDuration("123       ms", false)).isEqualTo(Duration.ofMillis(123));
        assertThat(DurationParser.parseDuration("456s", false)).isEqualTo(Duration.ofSeconds(456));
        assertThat(DurationParser.parseDuration("456  s", false)).isEqualTo(Duration.ofSeconds(456));
        assertThat(DurationParser.parseDuration("789m", false)).isEqualTo(Duration.ofMinutes(789));
        assertThat(DurationParser.parseDuration("789   m", false)).isEqualTo(Duration.ofMinutes(789));
        assertThat(DurationParser.parseDuration("123h", false)).isEqualTo(Duration.ofHours(123));
        assertThat(DurationParser.parseDuration("123 h", false)).isEqualTo(Duration.ofHours(123));
        assertThat(DurationParser.parseDuration("456d", false)).isEqualTo(Duration.ofDays(456));
        assertThat(DurationParser.parseDuration("456    d", false)).isEqualTo(Duration.ofDays(456));
        assertThat(DurationParser.parseDuration("789w", false)).isEqualTo(Duration.ofDays(7 * 789));
        assertThat(DurationParser.parseDuration("789   w", false)).isEqualTo(Duration.ofDays(7 * 789));
    }
}
