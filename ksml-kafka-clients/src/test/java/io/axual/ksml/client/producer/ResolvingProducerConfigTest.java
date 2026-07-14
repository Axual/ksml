package io.axual.ksml.client.producer;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingProducerConfigTest {
    private static final String TRANSACTIONAL_ID_PATTERN_CONFIG = "axual.transactional.id.pattern";

    @Test
    @DisplayName("A transactional id is resolved into the downstream config when a pattern is set")
    void transactionalIdIsResolved() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx");
        configs.put(TRANSACTIONAL_ID_PATTERN_CONFIG, "{tenant}-{transactional.id}");
        configs.put("tenant", "tenant");

        final var config = new ResolvingProducerConfig(configs);

        assertThat(config.downstreamConfigs())
                .containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tenant-tx")
                .doesNotContainKey(TRANSACTIONAL_ID_PATTERN_CONFIG);
    }

    @Test
    @DisplayName("A transactional id is left unchanged when no pattern is configured")
    void transactionalIdKeptWithoutPattern() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx");

        final var config = new ResolvingProducerConfig(configs);

        assertThat(config.downstreamConfigs()).containsEntry(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx");
    }

    @Test
    @DisplayName("Without a transactional id the downstream config carries no transactional id")
    void noTransactionalId() {
        final var config = new ResolvingProducerConfig(new HashMap<>());

        assertThat(config.downstreamConfigs()).doesNotContainKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }
}
