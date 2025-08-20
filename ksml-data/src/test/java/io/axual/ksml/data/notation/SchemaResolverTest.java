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

import java.util.Map;

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.schema.DataSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaResolverTest {
    @Test
    @DisplayName("getOrThrow returns schema when present and throws SchemaException with expected message when absent")
    void getOrThrowBehavesAsDocumented() {
        // A tiny resolver backed by a map
        var resolver = new SchemaResolver<>() {
            private final Map<String, DataSchema> schemas = Map.of(
                    "user", DataSchema.STRING_SCHEMA,
                    "count", DataSchema.LONG_SCHEMA
            );

            @Override
            public DataSchema get(String referenceName) {
                return schemas.get(referenceName);
            }
        };

        assertThat(resolver.getOrThrow("user")).isEqualTo(DataSchema.STRING_SCHEMA);
        assertThat(resolver.getOrThrow("count")).isEqualTo(DataSchema.LONG_SCHEMA);

        assertThatThrownBy(() -> resolver.getOrThrow("missing"))
                .isInstanceOf(SchemaException.class)
                .hasMessageEndingWith("Unknown schema: missing");
    }
}
