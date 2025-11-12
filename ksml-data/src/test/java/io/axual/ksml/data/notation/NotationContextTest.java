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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NotationContextTest {
    @Test
    @DisplayName("name() builds vendor_notation when vendor present; otherwise notation")
    void nameBuildsFromVendorAndNotation() {
        final var contextWithoutVendor = new NotationContext("json");
        assertThat(contextWithoutVendor.vendorName()).isNull();
        assertThat(contextWithoutVendor.notationName()).isEqualTo("json");
        assertThat(contextWithoutVendor.name()).isEqualTo("json");

        final var contextWithVendor = new NotationContext("avro", "confluent");
        assertThat(contextWithVendor.vendorName()).isEqualTo("confluent");
        assertThat(contextWithVendor.notationName()).isEqualTo("avro");
        assertThat(contextWithVendor.name()).isEqualTo("confluent_avro");
    }

    @Test
    @DisplayName("constructors populate fields; serdeConfigs default to mutable empty map and can be supplied")
    void constructorsPopulateFieldsAndSerdeConfigs() {
        final var providedConfigs = new HashMap<String, String>();
        providedConfigs.put("a", "1");
        final var providedDataMapper = new NativeDataObjectMapper();
        final var providedTypeSchemaMapper = new DataTypeDataSchemaMapper();

        final var context1 = new NotationContext("protobuf", providedDataMapper, providedTypeSchemaMapper);
        assertThat(context1.notationName()).isEqualTo("protobuf");
        assertThat(context1.vendorName()).isNull();
        assertThat(context1.nativeDataObjectMapper()).isSameAs(providedDataMapper);
        assertThat(context1.serdeConfigs()).isEmpty();
        context1.serdeConfigs().put("x", "y");
        assertThat(context1.serdeConfigs()).containsEntry("x", "y");

        final var context2 = new NotationContext("jsonschema", "apicurio", providedConfigs);
        assertThat(context2.notationName()).isEqualTo("jsonschema");
        assertThat(context2.vendorName()).isEqualTo("apicurio");
        assertThat(context2.nativeDataObjectMapper()).isNotNull();
        // The map reference is kept as-is per implementation
        assertThat(context2.serdeConfigs()).isSameAs(providedConfigs);
        assertThat(context2.serdeConfigs()).containsEntry("a", "1");

        final var context3 = new NotationContext("csv", "", (Map<String, String>) null);
        assertThat(context3.vendorName()).isEmpty();
        assertThat(context3.name()).isEqualTo("csv");
    }
}
