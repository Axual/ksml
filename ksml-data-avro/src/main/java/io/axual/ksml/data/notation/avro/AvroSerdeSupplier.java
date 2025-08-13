package io.axual.ksml.data.notation.avro;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - AVRO
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
 * Marker interface for vendor-specific Avro Serde suppliers.
 *
 * <p>Implementations provide concrete Kafka Serde instances for Avro under a particular vendor
 * (e.g., Confluent or Apicurio) and are used by AvroNotation via VendorNotation to obtain
 * serdes for specific DataTypes.</p>
 */
public interface AvroSerdeSupplier extends VendorSerdeSupplier {
}
