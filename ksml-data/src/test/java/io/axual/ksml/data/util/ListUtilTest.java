package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListUtilTest {

    @Test
    void findReturnsFirstMatch() {
        final var list = List.of("apple", "banana", "cherry");
        assertEquals("banana", ListUtil.find(list, s -> s.startsWith("b")));
    }

    @Test
    void findReturnsNullWhenNoMatch() {
        final var list = List.of("apple", "banana");
        assertNull(ListUtil.find(list, s -> s.startsWith("z")));
    }

    @Test
    void mapTransformsAllElements() {
        final var result = ListUtil.map(List.of(1, 2, 3), n -> n * 2);
        assertEquals(List.of(2, 4, 6), result);
    }

    @Test
    void filterKeepsMatchingElements() {
        final var result = ListUtil.filter(List.of(1, 2, 3, 4, 5), n -> n % 2 == 0);
        assertEquals(List.of(2, 4), result);
    }

    @Test
    void filterReturnsEmptyListWhenNoMatch() {
        final var result = ListUtil.filter(List.of(1, 3, 5), n -> n % 2 == 0);
        assertTrue(result.isEmpty());
    }

    @Test
    void anyReturnsTrueWhenMatchExists() {
        assertTrue(ListUtil.any(List.of("a", "b", "c"), s -> s.equals("b")));
    }

    @Test
    void anyReturnsFalseWhenNoMatch() {
        assertFalse(ListUtil.any(List.of("a", "b", "c"), s -> s.equals("z")));
    }

    @Test
    void anyReturnsFalseForEmptyList() {
        assertFalse(ListUtil.any(List.of(), s -> true));
    }
}
