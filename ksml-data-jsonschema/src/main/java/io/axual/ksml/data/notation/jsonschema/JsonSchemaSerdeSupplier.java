package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema
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

import io.axual.ksml.data.notation.vendor.VendorSerdeSupplier;

/**
 * Marker interface for vendor-backed JSON Schema Serde suppliers.
 *
 * <p>Vendor modules (e.g., confluent/apicurio) implement this interface to provide
 * concrete Kafka Serde instances capable of handling JSON Schema encoded data.
 * The default {@link #notationName()} ties the supplier to the {@link JsonSchemaNotation}.
 */
public interface JsonSchemaSerdeSupplier extends VendorSerdeSupplier {
    /** The notation name handled by this supplier, i.e. {@code "jsonschema"}. */
    default String notationName() {
        return JsonSchemaNotation.NOTATION_NAME;
    }
}
