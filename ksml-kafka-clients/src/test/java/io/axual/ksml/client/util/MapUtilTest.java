package io.axual.ksml.client.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MapUtilTest {
    @Test
    @DisplayName("toStringValues converts every value to its string representation")
    void toStringValuesConverts() {
        final var input = Map.<String, Object>of("a", 1, "b", true);
        assertThat(MapUtil.toStringValues(input))
                .containsEntry("a", "1")
                .containsEntry("b", "true");
    }

    @Test
    @DisplayName("toStringValues keeps null values as null")
    void toStringValuesKeepsNull() {
        final var input = new HashMap<String, Object>();
        input.put("a", null);
        assertThat(MapUtil.toStringValues(input)).containsEntry("a", null);
    }

    @Test
    @DisplayName("merge combines both maps, with the second overriding the first")
    void mergeOverrides() {
        final var result = MapUtil.merge(Map.of("a", 1, "b", 2), Map.of("b", 3, "c", 4));
        assertThat(result)
                .containsEntry("a", 1)
                .containsEntry("b", 3)
                .containsEntry("c", 4);
    }

    @Test
    @DisplayName("merge returns a copy of the first map when the second is null")
    void mergeWithNullSecond() {
        final var first = Map.of("a", 1);
        final var result = MapUtil.merge(first, null);
        assertThat(result).isNotSameAs(first).containsExactlyEntriesOf(first);
    }
}
