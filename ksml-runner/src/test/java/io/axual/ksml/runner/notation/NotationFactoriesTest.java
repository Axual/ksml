package io.axual.ksml.runner.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NotationFactoriesTest {

    @Test
    @DisplayName("JSON, binary and the default notation are always registered")
    void registersMandatoryNotations() {
        final var factories = new NotationFactories(Map.of());

        // JSON and binary are mandatory in KSML and registered explicitly (not via ServiceLoader),
        // and the default notation must always be available for untyped data.
        assertThat(factories.notations())
                .containsKey(JsonNotation.NOTATION_NAME)
                .containsKey(BinaryNotation.NOTATION_NAME)
                .containsKey(UserType.DEFAULT_NOTATION);
    }

    @Test
    @DisplayName("The default notation is backed by the same binary notation instance")
    void defaultNotationIsTheBinaryNotation() {
        final var factories = new NotationFactories(Map.of());

        final var binary = factories.notations().get(BinaryNotation.NOTATION_NAME).create(null);
        final var defaultNotation = factories.notations().get(UserType.DEFAULT_NOTATION).create(null);

        // Both keys resolve to the single binary notation instance created in the constructor.
        assertThat(defaultNotation).isSameAs(binary);
    }

    @Test
    @DisplayName("Each registered factory produces a usable notation instance")
    void everyFactoryCreatesANotation() {
        final var factories = new NotationFactories(Map.of());

        assertThat(factories.notations()).isNotEmpty();
        factories.notations().forEach((name, factory) ->
                assertThat(factory.create(null))
                        .as("notation created for '%s'", name)
                        .isNotNull());
    }

    @Test
    @DisplayName("Kafka configuration is accepted and does not affect the mandatory notations")
    void acceptsKafkaConfiguration() {
        final var factories = new NotationFactories(Map.of("bootstrap.servers", "localhost:9092"));

        assertThat(factories.notations())
                .containsKey(JsonNotation.NOTATION_NAME)
                .containsKey(BinaryNotation.NOTATION_NAME);
    }
}
