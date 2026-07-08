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

    @Test
    @DisplayName("name() combines vendor and notation, uses notation alone when no vendor, and is null without a notation name")
    void nameCombining() {
        final var vendored = new NotationProvider() {
            @Override
            public String notationName() {
                return "avro";
            }

            @Override
            public String vendorName() {
                return "axual";
            }

            @Override
            public Notation createNotation(NotationContext notationContext) {
                return null;
            }
        };
        assertThat(vendored.name()).isEqualTo("axual_avro");

        final var plain = new NotationProvider() {
            @Override
            public String notationName() {
                return "json";
            }

            @Override
            public String vendorName() {
                return null; // explicit: no vendor
            }

            @Override
            public Notation createNotation(NotationContext notationContext) {
                return null;
            }
        };
        assertThat(plain.vendorName()).isNull(); // documents the assumption name() relies on
        assertThat(plain.name()).isEqualTo("json");

        final var anonymous = new NotationProvider() {
            @Override
            public Notation createNotation(NotationContext notationContext) {
                return null;
            }
        };
        assertThat(anonymous.name()).isNull();
        // The no-arg createNotation() default delegates to createNotation(null)
        assertThat(anonymous.createNotation()).isNull();
    }
}
