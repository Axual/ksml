package io.axual.ksml.data.notation;

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
 * Service Provider Interface (SPI) for creating Notation instances.
 * Implementations are discovered/configured by the hosting environment.
 */
public interface NotationProvider {
    /**
     * The logical name of the notation that this provider supplies.
     *
     * @return the notation name (eg. "avro", "jsonschema", "protobuf")
     */
    String notationName();

    /**
     * Optional vendor name when this provider is vendor-specific.
     *
     * @return the vendor name, or null if not vendor-specific
     */
    default String vendorName() {
        return null;
    }

    /**
     * Creates a new Notation instance for the given context.
     *
     * @param notationContext configuration and helpers for the notation
     * @return a new Notation
     */
    Notation createNotation(NotationContext notationContext);
}
