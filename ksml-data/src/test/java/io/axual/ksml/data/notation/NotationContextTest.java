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

import static org.assertj.core.api.Assertions.assertThat;

class NotationContextTest {
    @Test
    @DisplayName("constructors populate fields; serdeConfigs default to mutable empty map and can be supplied")
    void constructorsPopulateFieldsAndSerdeConfigs() {
        final var providedConfigs = new HashMap<String, String>();
        providedConfigs.put("a", "1");
        final var providedDataMapper = new NativeDataObjectMapper();
        final var providedTypeSchemaMapper = new DataTypeDataSchemaMapper();

        final var context = new NotationContext(providedDataMapper, providedTypeSchemaMapper, providedConfigs);
        assertThat(context.nativeDataObjectMapper()).isSameAs(providedDataMapper);
        assertThat(context.serdeConfigs()).isSameAs(providedConfigs);
        context.serdeConfigs().put("x", "y");
        assertThat(context.serdeConfigs()).containsEntry("x", "y");
        // The map reference is kept as-is per implementation
        assertThat(context.serdeConfigs()).isSameAs(providedConfigs);
        assertThat(context.serdeConfigs()).containsEntry("a", "1");
    }
}
