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

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingDescribeConsumerGroupsResultTest {
    private final PrefixResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Described groups are keyed and named by the unresolved group id")
    void describedGroupsAreUnresolved() throws Exception {
        final var futures = Map.of("tenant-group",
                KafkaFuture.completedFuture(consumerGroupDescription("tenant-group")));

        final var result = new ResolvingDescribeConsumerGroupsResult(futures, resolver);

        assertThat(result.describedGroups()).containsOnlyKeys("group");
        assertThat(result.describedGroups().get("group").get().groupId()).isEqualTo("group");
    }

    @Test
    @DisplayName("all aggregates the described groups under their unresolved group ids")
    void allAggregatesUnresolvedGroups() throws Exception {
        final var futures = Map.of("tenant-group",
                KafkaFuture.completedFuture(consumerGroupDescription("tenant-group")));

        final var result = new ResolvingDescribeConsumerGroupsResult(futures, resolver);

        assertThat(result.all().get()).containsOnlyKeys("group");
    }

    private static ConsumerGroupDescription consumerGroupDescription(String groupId) {
        return new ConsumerGroupDescription(groupId, false, List.of(), "range",
                GroupType.CONSUMER, GroupState.STABLE, Node.noNode(), Set.of(),
                Optional.empty(), Optional.empty());
    }
}
