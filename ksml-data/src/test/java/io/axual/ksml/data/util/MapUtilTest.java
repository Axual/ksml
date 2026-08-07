package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MapUtilTest {

    @Test
    @DisplayName("stringKeys converts all keys to their String form and keeps values")
    void stringKeysConvertsKeys() {
        final var source = new LinkedHashMap<Object, Object>();
        source.put(1, "one");
        source.put(true, "yes");
        source.put(null, "nullKey");

        final Map<String, Object> result = MapUtil.stringKeys(source);

        assertThat(result)
                .containsEntry("1", "one")
                .containsEntry("true", "yes")
                .containsEntry(null, "nullKey");
    }

    @Test
    @DisplayName("stringValues converts all values to their String form and keeps keys")
    void stringValuesConvertsValues() {
        final var source = new LinkedHashMap<String, Object>();
        source.put("int", 42);
        source.put("bool", false);
        source.put("nil", null);

        final Map<String, String> result = MapUtil.stringValues(source);

        assertThat(result)
                .containsEntry("int", "42")
                .containsEntry("bool", "false")
                .containsEntry("nil", null);
    }
}
