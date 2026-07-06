package io.axual.ksml.client.admin;

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

import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingDeleteTopicsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("topic name values are keyed by the unresolved topic name")
    void topicNameValuesAreUnresolved() {
        final Map<String, KafkaFuture<Void>> nameFutures = Map.of("tenant-orders", KafkaFuture.completedFuture(null));

        // Kafka requires exactly one of the id/name future maps, so the topic-id map is left null.
        final var result = new ResolvingDeleteTopicsResult(null, nameFutures, resolver);

        assertThat(result.topicNameValues()).containsOnlyKeys("orders");
    }
}
