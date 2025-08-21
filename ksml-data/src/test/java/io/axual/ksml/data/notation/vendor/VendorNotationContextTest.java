package io.axual.ksml.data.notation.vendor;

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationContext;

import static org.assertj.core.api.Assertions.assertThat;

class VendorNotationContextTest {
    @Test
    @DisplayName("VendorNotationContext copies base fields and exposes vendor serde components")
    void vendorContextWrapsBaseContext() {
        var baseConfigs = new HashMap<String, String>();
        baseConfigs.put("x", "1");
        var baseContext = new NotationContext("avro", "confluent", new NativeDataObjectMapper(), baseConfigs);

        @SuppressWarnings("unchecked")
        var serdeMapper = (DataObjectMapper<Object>) (DataObjectMapper<?>) new io.axual.ksml.data.mapper.StringDataObjectMapper();
        var serdeSupplier = new VendorSerdeSupplier() {
            @Override
            public String vendorName() { return "confluent"; }

            @Override
            public org.apache.kafka.common.serialization.Serde<Object> get(io.axual.ksml.data.type.DataType type, boolean isKey) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                org.apache.kafka.common.serialization.Serde<Object> raw = (org.apache.kafka.common.serialization.Serde) org.apache.kafka.common.serialization.Serdes.String();
                return raw;
            }
        };

        var vendorContext = new VendorNotationContext(baseContext, serdeSupplier, serdeMapper);
        assertThat(vendorContext.notationName()).isEqualTo("avro");
        assertThat(vendorContext.vendorName()).isEqualTo("confluent");
        assertThat(vendorContext.nativeDataObjectMapper()).isSameAs(baseContext.nativeDataObjectMapper());
        assertThat(vendorContext.serdeConfigs()).isSameAs(baseContext.serdeConfigs());
        assertThat(vendorContext.serdeSupplier()).isSameAs(serdeSupplier);
        assertThat(vendorContext.serdeMapper()).isSameAs(serdeMapper);
        assertThat(vendorContext.name()).isEqualTo("confluent_avro");
    }
}
