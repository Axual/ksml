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

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingDescribeTopicsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Topic descriptions are keyed and named by the unresolved topic name")
    void topicDescriptionsAreUnresolved() throws Exception {
        final Map<String, KafkaFuture<TopicDescription>> nameFutures = Map.of(
                "tenant-orders", KafkaFuture.completedFuture(new TopicDescription("tenant-orders", false, List.of())));

        // Kafka requires exactly one of the id/name future maps, so the topic-id map is left null.
        final var result = new ResolvingDescribeTopicsResult(null, nameFutures, resolver);

        assertThat(result.topicNameValues()).containsOnlyKeys("orders");
        assertThat(result.topicNameValues().get("orders").get().name()).isEqualTo("orders");
    }
}
