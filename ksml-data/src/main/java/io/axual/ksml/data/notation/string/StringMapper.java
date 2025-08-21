package io.axual.ksml.data.notation.string;

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

/**
 * Bidirectional mapping between String representation and a target type T.
 * Useful for notations that serialize to textual form.
 *
 * @param <T> the target mapped type
 */
public interface StringMapper<T> {
    /**
     * Parses a value from its String representation.
     *
     * @param value the string form
     * @return the parsed value
     */
    T fromString(String value);

    /**
     * Converts a value to its String representation.
     *
     * @param value the value to convert
     * @return the string form
     */
    String toString(T value);
}
