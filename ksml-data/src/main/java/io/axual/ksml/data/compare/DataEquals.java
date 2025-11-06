package io.axual.ksml.data.compare;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

/**
 * Represents a contract for checking equality between data objects, data types and data schemas with additional
 * flexibility for parameterized comparisons using {@link EqualityFlags}.
 * <p>
 * Implementations of this interface must define the logic for comparing the current object
 * with another object for equality based on the provided flags. This allows fine-grained
 * control over which attributes or conditions are involved in the comparison.
 */
public interface DataEquals {
    Equality equals(Object obj, EqualityFlags flags);
}
