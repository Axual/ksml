package io.axual.ksml.data.schema;

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
 * Represents a container for a single value.
 * <p>
 * The {@code DataValue} class is designed as a record to hold a single data value
 * of any type. It acts as a lightweight wrapper for objects, primarily used in the
 * context of schema-related operations within the KSML framework.
 * </p>
 *
 * @param value The encapsulated value. This is a generic object and can represent any type of data.
 */
public record DataValue(Object value) {
}
