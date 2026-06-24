package io.axual.ksml.runner;

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

import io.axual.ksml.runner.config.NotationConfig;
import io.axual.ksml.runner.config.NotationConfig.NotationType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KSMLRunnerNotationLookupTest {

    @Test
    void resolveFactoryNameReturnsNullForNullConfig() {
        assertThat(KSMLRunner.resolveFactoryName(null)).isNull();
    }

    @Test
    void resolveFactoryNameReturnsNullWhenTypeIsMissing() {
        final var cfg = NotationConfig.builder().type(null).build();
        assertThat(KSMLRunner.resolveFactoryName(cfg)).isNull();
    }

    @Test
    void resolveFactoryNameReturnsWireNameForValidType() {
        final var cfg = NotationConfig.builder().type(NotationType.CONFLUENT_AVRO).build();
        assertThat(KSMLRunner.resolveFactoryName(cfg)).isEqualTo("confluent_avro");
    }

    @Test
    void resolveFactoryNameMatchesJsonValueForEveryType() {
        for (final var nt : NotationType.values()) {
            final var cfg = NotationConfig.builder().type(nt).build();
            assertThat(KSMLRunner.resolveFactoryName(cfg))
                    .as("wire name for %s", nt)
                    .isEqualTo(nt.jsonValue());
        }
    }
}
