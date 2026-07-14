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
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Both {@link ResolvingDeleteConsumerGroupOffsetsResult} and {@link ResolvingAlterConsumerGroupOffsetsResult}
 * resolve the requested partition before delegating to the underlying result.
 */
class ResolvingConsumerGroupOffsetsResultTest {
    private static final TopicPartition RESOLVED_PARTITION = new TopicPartition("tenant-orders", 0);
    private static final TopicPartition UNRESOLVED_PARTITION = new TopicPartition("orders", 0);

    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Deleting offsets resolves the requested partition")
    void deleteResolvesPartition() {
        final KafkaFuture<Void> future = KafkaFuture.completedFuture(null);
        final var delegate = mock(DeleteConsumerGroupOffsetsResult.class);
        when(delegate.partitionResult(RESOLVED_PARTITION)).thenReturn(future);

        final var result = new ResolvingDeleteConsumerGroupOffsetsResult(delegate, resolver);

        assertThat(result.partitionResult(UNRESOLVED_PARTITION)).isSameAs(future);
    }

    @Test
    @DisplayName("Altering offsets resolves the requested partition")
    void alterResolvesPartition() {
        final KafkaFuture<Void> future = KafkaFuture.completedFuture(null);
        final var delegate = mock(AlterConsumerGroupOffsetsResult.class);
        when(delegate.partitionResult(RESOLVED_PARTITION)).thenReturn(future);

        final var result = new ResolvingAlterConsumerGroupOffsetsResult(delegate, resolver);

        assertThat(result.partitionResult(UNRESOLVED_PARTITION)).isSameAs(future);
    }
}
