package io.axual.ksml.data.tag;

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

/**
 * A key value pair used to identify a unique instance of a metric.
 */
public record ContextTag(String key, String value) {
    /**
     * Create a new metric tag instance.
     *
     * @param key   cannot be null, empty or only contains whitespace/control characters
     * @param value cannot be null, empty or only contains whitespace/control characters
     * @throws IllegalArgumentException if the key or value is null, empty or only contains whitespace/control characters
     */
    public ContextTag {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Value cannot be null or empty");
        }
    }

    @Override
    public String toString() {
        return key + "=" + (value != null ? value : "null");
    }
}
