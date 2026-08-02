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
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"deprecation", "removal"}) // ListConsumerGroupsResult/ConsumerGroupListing are deprecated in Kafka 4.1 but still wrapped by the class under test
class ResolvingListConsumerGroupsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Listed consumer groups are reported under their unresolved group ids")
    void listingsAreUnresolved() throws Exception {
        final var listing = new ConsumerGroupListing("tenant-group", Optional.empty(), false);
        final var delegate = mock(ListConsumerGroupsResult.class);
        when(delegate.all()).thenReturn(KafkaFuture.completedFuture(List.of(listing)));
        when(delegate.valid()).thenReturn(KafkaFuture.completedFuture(List.of(listing)));
        final KafkaFuture<Collection<Throwable>> errors = KafkaFuture.completedFuture(List.of());
        when(delegate.errors()).thenReturn(errors);

        final var result = new ResolvingListConsumerGroupsResult(delegate, resolver);

        assertThat(result.all().get()).extracting(ConsumerGroupListing::groupId).containsExactly("group");
        assertThat(result.valid().get()).extracting(ConsumerGroupListing::groupId).containsExactly("group");
        assertThat(result.errors()).isSameAs(errors);
    }
}
