package io.axual.ksml.data.value;

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
 * Marker type used to represent a null value in the KSML value model and Serdes.
 *
 * <p>The constant {@link #NULL} is intentionally set to Java {@code null}; it exists solely
 * to have an "empty value" in places where a value of a certain type is expected.</p>
 */
public class Null {
    private Null() {
        // Prevent instantiation
    }

    /** Readable alias for DataObject null in places where a value of another type is expected. */
    public static final Null NULL = null;
}
