package io.axual.ksml.data.util;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
<<<<<<< HEAD
 * Copyright (C) 2021 - 2024 Axual B.V.
=======
 * Copyright (C) 2021 - 2025 Axual B.V.
>>>>>>> main
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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility functions for working with maps.
 */
public class MapUtil {
    /**
     * Create a new map with String keys by converting the keys of the input map via toString().
     *
     * @param map the input map with arbitrary key types
     * @return a map with String keys and the same values as the input
     */
    public static Map<String, Object> stringKeys(Map<?, ?> map) {
        final var result = new HashMap<String, Object>();
        map.forEach((key, value) -> result.put(key != null ? key.toString() : null, value));
        return result;
    }

    private MapUtil() {
        // Prevent instantiation
    }
}
