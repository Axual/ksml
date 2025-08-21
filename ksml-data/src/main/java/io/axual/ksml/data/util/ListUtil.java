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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Utility functions for working with lists.
 */
public class ListUtil {
    /**
     * Find the first element in the list that matches the given predicate.
     *
     * @param list    the list to search
     * @param matcher predicate used to test elements
     * @param <V>     element type
     * @return the first matching element, or null if none match
     */
    public static <V> V find(List<V> list, Predicate<V> matcher) {
        for (final var entry : list) {
            if (matcher.test(entry)) return entry;
        }
        return null;
    }

    /**
     * Map the elements of a list using the provided mapper function.
     *
     * @param list   the source list
     * @param mapper function to transform elements
     * @param <T>    source element type
     * @param <R>    result element type
     * @return a new list containing mapped elements
     */
    public static <T, R> List<R> map(List<T> list, Function<T, R> mapper) {
        return list.stream().map(mapper).toList();
    }

    private ListUtil() {
        // Prevent instantiation.
    }
}
