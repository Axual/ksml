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

import io.axual.ksml.client.testutil.PrefixResolver;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResolvingCreateTopicsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();
    private final CreateTopicsResult delegate = mock(CreateTopicsResult.class);

    @Test
    @DisplayName("values are keyed by the unresolved topic name")
    void valuesAreUnresolved() {
        when(delegate.values()).thenReturn(Map.of("tenant-orders", KafkaFuture.completedFuture(null)));

        final var result = new ResolvingCreateTopicsResult(delegate, resolver);

        assertThat(result.values()).containsOnlyKeys("orders");
    }

    @Test
    @DisplayName("per-topic lookups resolve the requested topic before delegating")
    void perTopicLookupsResolve() {
        when(delegate.values()).thenReturn(Map.of());
        final KafkaFuture<Config> config = KafkaFuture.completedFuture(null);
        final KafkaFuture<Integer> numPartitions = KafkaFuture.completedFuture(3);
        final KafkaFuture<Integer> replicationFactor = KafkaFuture.completedFuture(2);
        when(delegate.config("tenant-orders")).thenReturn(config);
        when(delegate.numPartitions("tenant-orders")).thenReturn(numPartitions);
        when(delegate.replicationFactor("tenant-orders")).thenReturn(replicationFactor);

        final KafkaFuture<Void> all = KafkaFuture.completedFuture(null);
        when(delegate.all()).thenReturn(all);

        final var result = new ResolvingCreateTopicsResult(delegate, resolver);

        assertThat(result.config("orders")).isSameAs(config);
        assertThat(result.numPartitions("orders")).isSameAs(numPartitions);
        assertThat(result.replicationFactor("orders")).isSameAs(replicationFactor);
        assertThat(result.all()).isSameAs(all);
    }
}
