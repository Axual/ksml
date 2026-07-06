package io.axual.ksml.client.consumer;

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

import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResolvingConsumerPartitionAssignorTest {
    private static final String PREFIX = "tenant-";
    private static final String MEMBER = "member";

    private final TopicResolver topicResolver = new TopicResolver() {
        @Override
        public String resolve(String name) {
            return name == null ? null : PREFIX + name;
        }

        @Override
        public String unresolve(String name) {
            if (name == null || !name.startsWith(PREFIX)) return null;
            return name.substring(PREFIX.length());
        }
    };

    @Mock
    private ConsumerPartitionAssignor backingAssignor;

    private ResolvingConsumerPartitionAssignor assignor;

    @BeforeEach
    void setUp() {
        assignor = new ResolvingConsumerPartitionAssignor();
        assignor.configure(Map.of(
                ResolvingConsumerPartitionAssignorConfig.BACKING_ASSIGNOR_CONFIG, backingAssignor,
                ResolvingConsumerPartitionAssignorConfig.ASSIGNOR_TOPIC_RESOLVER_CONFIG, topicResolver));
    }

    @Test
    @DisplayName("Simple metadata calls are delegated to the backing assignor")
    void metadataCallsAreDelegated() {
        when(backingAssignor.name()).thenReturn("range");
        when(backingAssignor.supportedProtocols()).thenReturn(List.of(RebalanceProtocol.EAGER));

        assertThat(assignor.version()).isEqualTo((short) 1);
        assertThat(assignor.name()).isEqualTo("range");
        assertThat(assignor.supportedProtocols()).containsExactly(RebalanceProtocol.EAGER);

        assignor.subscriptionUserData(java.util.Set.of("orders"));
        verify(backingAssignor).subscriptionUserData(java.util.Set.of("orders"));
    }

    @Test
    @DisplayName("assign unresolves the cluster and subscription and resolves the resulting partitions")
    void assignUnresolvesAndResolves() {
        final var node = new Node(0, "localhost", 9092);
        final var cluster = new Cluster("cluster-id", List.of(node),
                List.of(new PartitionInfo(PREFIX + "orders", 0, node, new Node[]{node}, new Node[]{node})),
                Set.of(), Set.of());
        final var subscriptions = new GroupSubscription(Map.of(
                MEMBER, new Subscription(List.of(PREFIX + "orders"), null, List.of())));
        when(backingAssignor.assign(any(), any())).thenReturn(new GroupAssignment(Map.of(
                MEMBER, new Assignment(List.of(new TopicPartition("orders", 0))))));

        final var result = assignor.assign(cluster, subscriptions);

        // The result partitions are resolved back to the external names
        assertThat(result.groupAssignment().get(MEMBER).partitions())
                .containsExactly(new TopicPartition(PREFIX + "orders", 0));

        // The backing assignor received the unresolved (internal) cluster and subscription
        final ArgumentCaptor<Cluster> clusterCaptor = ArgumentCaptor.captor();
        final ArgumentCaptor<GroupSubscription> subscriptionCaptor = ArgumentCaptor.captor();
        verify(backingAssignor).assign(clusterCaptor.capture(), subscriptionCaptor.capture());
        assertThat(clusterCaptor.getValue().topics()).containsExactly("orders");
        assertThat(subscriptionCaptor.getValue().groupSubscription().get(MEMBER).topics()).containsExactly("orders");
    }

    @Test
    @DisplayName("onAssignment falls back to unresolving only the partitions when the user data cannot be decoded")
    void onAssignmentWithUndecodableUserData() {
        final var assignment = new Assignment(
                List.of(new TopicPartition(PREFIX + "orders", 0)),
                ByteBuffer.wrap(new byte[]{1, 2, 3}));

        assignor.onAssignment(assignment, null);

        final ArgumentCaptor<Assignment> captured = ArgumentCaptor.captor();
        verify(backingAssignor).onAssignment(captured.capture(), any());
        assertThat(captured.getValue().partitions()).containsExactly(new TopicPartition("orders", 0));
    }
}
