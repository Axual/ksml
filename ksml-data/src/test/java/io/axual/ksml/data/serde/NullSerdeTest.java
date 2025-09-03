package io.axual.ksml.data.serde;

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

import io.axual.ksml.data.exception.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NullSerdeTest {
    private static final String TOPIC = "topic";

    @Test
    @DisplayName("serializer always returns null regardless of input")
    void serializerAlwaysReturnsNull() {
        var serde = new NullSerde();
        assertThat(serde.serializer().serialize(TOPIC, null)).isNull();
        assertThat(serde.serializer().serialize(TOPIC, "ignored"))
                .as("Non-null input still yields null bytes")
                .isNull();
    }

    @Test
    @DisplayName("deserializer returns null for null/empty bytes; throws for non-empty")
    void deserializerReturnsNullOrThrows() {
        var serde = new NullSerde();
        assertThat(serde.deserializer().deserialize(TOPIC, null)).isNull();
        assertThat(serde.deserializer().deserialize(TOPIC, new byte[]{})).isNull();

        assertThatThrownBy(() -> serde.deserializer().deserialize(TOPIC, new byte[]{1}))
                .isInstanceOf(DataException.class)
                .hasMessageEndingWith("Can only deserialize empty byte arrays as DataNull");
    }
}
