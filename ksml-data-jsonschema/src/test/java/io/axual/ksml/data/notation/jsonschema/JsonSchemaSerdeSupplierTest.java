package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Minimal test for {@link JsonSchemaSerdeSupplier} default behavior.
 * Verifies the default notationName() and that a dummy implementation can provide a Serde.
 */
@DisplayName("JsonSchemaSerdeSupplier - notation name and dummy get()")
class JsonSchemaSerdeSupplierTest {
    @Test
    @DisplayName("notationName() returns 'jsonschema'")
    void notationNameIsJsonSchema() {
        var supplier = new JsonSchemaSerdeSupplier() {
            @Override
            public String vendorName() { return "vendorX"; }
            @Override
            public Serde<Object> get(DataType type, boolean isKey) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                Serde<Object> raw = (Serde) Serdes.String();
                return raw;
            }
        };
        assertThat(supplier.notationName()).isEqualTo(JsonSchemaNotation.NOTATION_NAME);

        // Also verify get() can be called for a supported DataType (dummy Serde)
        var serde = supplier.get(new StructType(), false);
        assertThat(serde).isNotNull();
    }
}
