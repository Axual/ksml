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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotationProviderTest {
    @Test
    @DisplayName("default vendorName is null; notationName returns provided name")
    void defaultVendorNameIsNull() {
        var provider = new NotationProvider() {
            @Override
            public String notationName() {
                return "avro";
            }

            @Override
            public Notation createNotation(NotationContext notationContext) {
                return null; // not needed for this test
            }
        };

        assertThat(provider.vendorName()).isNull();
        assertThat(provider.notationName()).isEqualTo("avro");
    }
}
