package io.axual.ksml.data.notation.jsonschema.confluent;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema Confluent
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

import io.axual.ksml.data.type.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Minimal test for {@link ConfluentJsonSchemaSerdeSupplier} default behavior.
 * Verifies the default notationName()/vendorName() and that get() returns a Serde.
 */
@DisplayName("ConfluentJsonSchemaSerdeSupplier - notation name, vendor name, and dummy get()")
class ConfluentJsonSchemaSerdeSupplierTest {
    @Test
    @DisplayName("notationName() returns 'jsonschema'; vendorName() returns 'confluent'; get() returns a serde")
    void basics() {
        var supplier = new ConfluentJsonSchemaSerdeSupplier();
        assertThat(supplier.notationName()).isEqualTo("jsonschema");
        assertThat(supplier.vendorName()).isEqualTo("confluent");

        var serde = supplier.get(new StructType(), false);
        assertThat(serde).isNotNull();
    }
}
