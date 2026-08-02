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

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ExtendableListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The concrete {@link ResolvingListConsumerGroupsResult} overrides {@code all()} and {@code valid()},
 * so this test exercises the plain delegation of the base class.
 */
@SuppressWarnings({"deprecation", "removal"}) // ListConsumerGroupsResult/ConsumerGroupListing deprecated in Kafka 4.1 but still extended
class ExtendableListConsumerGroupsResultTest {
    @Test
    @DisplayName("all, valid and errors delegate to the wrapped result")
    void delegatesToWrappedResult() {
        final KafkaFuture<Collection<ConsumerGroupListing>> all = KafkaFuture.completedFuture(List.of());
        final KafkaFuture<Collection<ConsumerGroupListing>> valid = KafkaFuture.completedFuture(List.of());
        final KafkaFuture<Collection<Throwable>> errors = KafkaFuture.completedFuture(List.of());
        final var delegate = mock(ListConsumerGroupsResult.class);
        when(delegate.all()).thenReturn(all);
        when(delegate.valid()).thenReturn(valid);
        when(delegate.errors()).thenReturn(errors);

        final var result = new ExtendableListConsumerGroupsResult(delegate);

        assertThat(result.all()).isSameAs(all);
        assertThat(result.valid()).isSameAs(valid);
        assertThat(result.errors()).isSameAs(errors);
    }
}
