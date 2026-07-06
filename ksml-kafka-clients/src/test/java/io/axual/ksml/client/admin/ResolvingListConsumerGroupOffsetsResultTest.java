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

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResolvingListConsumerGroupOffsetsResultTest {
    private static final TopicPartition RESOLVED_PARTITION = new TopicPartition("tenant-orders", 0);
    private static final TopicPartition UNRESOLVED_PARTITION = new TopicPartition("orders", 0);

    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Offsets are reported under the unresolved group id and partition")
    void offsetsAreUnresolved() throws Exception {
        final Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = Map.of(
                CoordinatorKey.byGroupId("tenant-group"),
                KafkaFuture.completedFuture(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(5L))));

        final var result = new ResolvingListConsumerGroupOffsetsResult(futures, resolver, resolver);

        assertThat(result.all().get()).containsOnlyKeys("group");
        assertThat(result.partitionsToOffsetAndMetadata("group").get()).containsOnlyKeys(UNRESOLVED_PARTITION);
    }

    @Test
    @DisplayName("A failed lookup propagates the failure")
    void failurePropagates() {
        final KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>> failing = new KafkaFutureImpl<>();
        failing.completeExceptionally(new RuntimeException("boom"));
        final Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = Map.of(
                CoordinatorKey.byGroupId("tenant-group"), failing);

        final var result = new ResolvingListConsumerGroupOffsetsResult(futures, resolver, resolver);

        assertThatThrownBy(() -> result.partitionsToOffsetAndMetadata("group").get()).hasRootCauseMessage("boom");
    }
}
