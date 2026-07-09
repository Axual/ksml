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
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingListTopicsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Listings unresolve regular topics, keep internal topics and drop unresolvable ones")
    void listingsAreUnresolved() throws Exception {
        final Map<String, TopicListing> listings = new HashMap<>();
        listings.put("tenant-orders", new TopicListing("tenant-orders", Uuid.randomUuid(), false));
        listings.put("_internal", new TopicListing("_internal", Uuid.randomUuid(), true));
        listings.put("unprefixed", new TopicListing("unprefixed", Uuid.randomUuid(), false));

        final var result = new ResolvingListTopicsResult(KafkaFuture.completedFuture(listings), resolver);

        assertThat(result.namesToListings().get()).containsOnlyKeys("orders", "_internal");
        assertThat(result.names().get()).containsExactlyInAnyOrder("orders", "_internal");
        assertThat(result.listings().get()).extracting(TopicListing::name)
                .containsExactlyInAnyOrder("orders", "_internal");
    }
}
