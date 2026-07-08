package io.axual.ksml.definition;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.type.UserType;
import org.apache.kafka.streams.AutoOffsetReset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicDefinitionTest {

    @Test
    void toStringWithOnlyTopicOmitsOptionalFields() {
        final var definition = new TopicDefinition("orders", null, null, null, null, null);
        assertThat(definition).asString().contains("topic=orders");
        assertThat(definition.topic()).isEqualTo("orders");
    }

    @Test
    void toStringIncludesAllPopulatedFields() {
        final var key = UserType.UNKNOWN;
        final var value = UserType.UNKNOWN;
        final var definition = new TopicDefinition("orders", key, value, AutoOffsetReset.earliest(), null, null);

        assertThat(definition).asString().contains("topic=orders");
        assertThat(definition.keyType()).isEqualTo(key);
        assertThat(definition.valueType()).isEqualTo(value);
        assertThat(definition.resetPolicy()).isNotNull();
    }

    @Test
    void equalTopicDefinitionsShareHashCode() {
        final var first = new TopicDefinition("orders", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null);
        final var second = new TopicDefinition("orders", UserType.UNKNOWN, UserType.UNKNOWN, null, null, null);

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }
}
