package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotationTest {

    /** A minimal Notation implementation so the interface's default methods can be exercised. */
    private static Notation minimalNotation() {
        return new Notation() {
            @Override
            public Notation.SchemaUsage schemaUsage() {
                return null;
            }

            @Override
            public DataType defaultType() {
                return null;
            }

            @Override
            public String name() {
                return "test";
            }

            @Override
            public String filenameExtension() {
                return ".test";
            }

            @Override
            public Serde<Object> serde(DataType type, boolean isKey) {
                return null;
            }

            @Override
            public Notation.Converter converter() {
                return null;
            }

            @Override
            public Notation.SchemaParser schemaParser() {
                return null;
            }
        };
    }

    @Test
    @DisplayName("Remote-schema support defaults to false and fetchRemoteSchema defaults to null")
    void remoteSchemaDefaults() {
        final var notation = minimalNotation();

        assertThat(notation.supportsRemoteSchema()).isFalse();
        assertThat(notation.fetchRemoteSchema("topic", false)).isNull();
    }
}
