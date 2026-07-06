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

import io.axual.ksml.client.exception.NotSupportedException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsOptions;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link ResolvingAdmin} resolves topic and group names towards its delegate admin
 * and unresolves them again on the returned results. The delegate {@link KafkaAdminClient} created
 * by {@code Admin.create(...)} is replaced with a Mockito construction mock so no real broker is
 * contacted, and the unsupported operations are asserted to fail fast.
 */
class ResolvingAdminTest {
    private static final String UNRESOLVED_TOPIC = "orders";
    private static final String RESOLVED_TOPIC = "tenant-orders";
    private static final String UNRESOLVED_GROUP = "group";
    private static final String RESOLVED_GROUP = "tenant-group";
    private static final TopicPartition UNRESOLVED_PARTITION = new TopicPartition(UNRESOLVED_TOPIC, 0);
    private static final TopicPartition RESOLVED_PARTITION = new TopicPartition(RESOLVED_TOPIC, 0);
    private static final Map<String, Object> CONFIGS = Map.of(
            "bootstrap.servers", "localhost:9092",
            "axual.topic.pattern", "{tenant}-{topic}",
            "axual.group.id.pattern", "{tenant}-{group.id}",
            "tenant", "tenant");

    @Test
    @DisplayName("Unsupported operations fail fast with a NotSupportedException")
    void unsupportedOperationsThrow() throws Exception {
        withAdmin((admin, delegate) -> {
            // These operations fail fast before touching their arguments, so null arguments keep each
            // assertion lambda to the single invocation under test (Sonar S5778).
            assertThatThrownBy(() -> admin.createAcls(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.deleteAcls(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.describeConfigs(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.incrementalAlterConfigs(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.alterReplicaLogDirs(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.describeLogDirs(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.describeReplicaLogDirs(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.createPartitions(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.electLeaders(null, null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.alterPartitionReassignments(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.listPartitionReassignments(Optional.empty(), null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.describeClientQuotas(null, null)).isInstanceOf(NotSupportedException.class);
            assertThatThrownBy(() -> admin.alterClientQuotas(null, null)).isInstanceOf(NotSupportedException.class);
            verifyNoInteractions(delegate);
        });
    }

    @Test
    @DisplayName("Metric subscription calls are intentionally swallowed and not forwarded")
    void metricSubscriptionIsSwallowed() throws Exception {
        withAdmin((admin, delegate) -> {
            admin.registerMetricForSubscription(null);
            admin.unregisterMetricFromSubscription(null);
            verifyNoInteractions(delegate);
        });
    }

    @Test
    @DisplayName("createTopics resolves topic names and wraps the result")
    void createTopicsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(CreateTopicsResult.class);
            when(delegateResult.values()).thenReturn(Map.of());
            when(delegate.createTopics(any(), any())).thenReturn(delegateResult);

            final var result = admin.createTopics(List.of(new NewTopic(UNRESOLVED_TOPIC, 1, (short) 1)), new CreateTopicsOptions());

            assertThat(result).isInstanceOf(ResolvingCreateTopicsResult.class);
            final ArgumentCaptor<List<NewTopic>> captor = ArgumentCaptor.captor();
            verify(delegate).createTopics(captor.capture(), any());
            assertThat(captor.getValue()).extracting(NewTopic::name).containsExactly(RESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("deleteTopics resolves the topic collection and unresolves the result")
    void deleteTopicsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(DeleteTopicsResult.class);
            when(delegateResult.topicIdValues()).thenReturn(null);
            when(delegateResult.topicNameValues()).thenReturn(Map.of(RESOLVED_TOPIC, KafkaFuture.completedFuture(null)));
            when(delegate.deleteTopics(any(TopicCollection.class), any())).thenReturn(delegateResult);

            final var result = admin.deleteTopics(TopicCollection.ofTopicNames(List.of(UNRESOLVED_TOPIC)), new DeleteTopicsOptions());

            assertThat(result.topicNameValues()).containsOnlyKeys(UNRESOLVED_TOPIC);
            final ArgumentCaptor<TopicCollection> captor = ArgumentCaptor.captor();
            verify(delegate).deleteTopics(captor.capture(), any());
            assertThat(((TopicCollection.TopicNameCollection) captor.getValue()).topicNames()).containsExactly(RESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("listTopics unresolves the listed topic names")
    void listTopicsUnresolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(ListTopicsResult.class);
            when(delegateResult.namesToListings()).thenReturn(KafkaFuture.completedFuture(
                    Map.of(RESOLVED_TOPIC, new TopicListing(RESOLVED_TOPIC, Uuid.randomUuid(), false))));
            when(delegate.listTopics(any())).thenReturn(delegateResult);

            final var result = admin.listTopics(new ListTopicsOptions());

            assertThat(result.names().get()).containsExactly(UNRESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("describeTopics resolves the topic collection and unresolves the result")
    void describeTopicsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(DescribeTopicsResult.class);
            when(delegateResult.topicIdValues()).thenReturn(null);
            when(delegateResult.topicNameValues()).thenReturn(Map.of(RESOLVED_TOPIC,
                    KafkaFuture.completedFuture(new TopicDescription(RESOLVED_TOPIC, false, List.of()))));
            when(delegate.describeTopics(any(TopicCollection.class), any())).thenReturn(delegateResult);

            final var result = admin.describeTopics(TopicCollection.ofTopicNames(List.of(UNRESOLVED_TOPIC)), new DescribeTopicsOptions());

            assertThat(result.topicNameValues()).containsOnlyKeys(UNRESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("describeAcls resolves the filter and unresolves the returned bindings")
    void describeAclsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(DescribeAclsResult.class);
            when(delegateResult.values()).thenReturn(KafkaFuture.completedFuture(List.of(
                    new AclBinding(new ResourcePattern(ResourceType.TOPIC, RESOLVED_TOPIC, PatternType.LITERAL),
                            new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW)))));
            when(delegate.describeAcls(any(), any())).thenReturn(delegateResult);

            final var filter = new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, UNRESOLVED_TOPIC, PatternType.LITERAL),
                    AccessControlEntryFilter.ANY);
            final var result = admin.describeAcls(filter, new DescribeAclsOptions());

            assertThat(result.values().get()).extracting(b -> b.pattern().name()).containsExactly(UNRESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("deleteRecords resolves the partition keys and forwards the result")
    void deleteRecordsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(DeleteRecordsResult.class);
            when(delegate.deleteRecords(any(), any())).thenReturn(delegateResult);

            final var result = admin.deleteRecords(Map.of(UNRESOLVED_PARTITION, RecordsToDelete.beforeOffset(5L)), new DeleteRecordsOptions());

            assertThat(result).isSameAs(delegateResult);
            verify(delegate).deleteRecords(argThat(m -> m.containsKey(RESOLVED_PARTITION)), any());
        });
    }

    @Test
    @DisplayName("describeConsumerGroups resolves the group ids and unresolves the result")
    void describeConsumerGroupsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(DescribeConsumerGroupsResult.class);
            when(delegateResult.describedGroups()).thenReturn(Map.of(RESOLVED_GROUP,
                    KafkaFuture.completedFuture(consumerGroupDescription(RESOLVED_GROUP))));
            when(delegate.describeConsumerGroups(any(), any())).thenReturn(delegateResult);

            final var result = admin.describeConsumerGroups(List.of(UNRESOLVED_GROUP), new DescribeConsumerGroupsOptions());

            assertThat(result.describedGroups()).containsOnlyKeys(UNRESOLVED_GROUP);
            verify(delegate).describeConsumerGroups(argThat(g -> g.contains(RESOLVED_GROUP)), any());
        });
    }

    @Test
    @DisplayName("listConsumerGroups unresolves the returned group ids")
    @SuppressWarnings({"deprecation", "removal"}) // ListConsumerGroupsResult/ConsumerGroupListing deprecated in Kafka 4.1 but still wrapped
    void listConsumerGroupsUnresolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var listing = new ConsumerGroupListing(RESOLVED_GROUP, Optional.empty(), false);
            final var delegateResult = mock(ListConsumerGroupsResult.class);
            when(delegateResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(listing)));
            when(delegateResult.valid()).thenReturn(KafkaFuture.completedFuture(List.of(listing)));
            when(delegate.listConsumerGroups(any())).thenReturn(delegateResult);

            final var result = admin.listConsumerGroups(new ListConsumerGroupsOptions());

            assertThat(result.all().get()).extracting(ConsumerGroupListing::groupId).containsExactly(UNRESOLVED_GROUP);
        });
    }

    @Test
    @DisplayName("listConsumerGroupOffsets resolves the specs and unresolves the result")
    void listConsumerGroupOffsetsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(ListConsumerGroupOffsetsResult.class);
            when(delegateResult.partitionsToOffsetAndMetadata(RESOLVED_GROUP)).thenReturn(
                    KafkaFuture.completedFuture(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(1L))));
            when(delegate.listConsumerGroupOffsets(anyMap(), any())).thenReturn(delegateResult);

            final var specs = Map.of(UNRESOLVED_GROUP,
                    new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(UNRESOLVED_PARTITION)));
            final var result = admin.listConsumerGroupOffsets(specs, new ListConsumerGroupOffsetsOptions());

            assertThat(result.all().get()).containsOnlyKeys(UNRESOLVED_GROUP);
            assertThat(result.partitionsToOffsetAndMetadata(UNRESOLVED_GROUP).get())
                    .containsEntry(UNRESOLVED_PARTITION, new OffsetAndMetadata(1L));
        });
    }

    @Test
    @DisplayName("deleteConsumerGroups resolves the group ids and unresolves the result")
    void deleteConsumerGroupsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(DeleteConsumerGroupsResult.class);
            when(delegateResult.deletedGroups()).thenReturn(Map.of(RESOLVED_GROUP, KafkaFuture.completedFuture(null)));
            when(delegate.deleteConsumerGroups(any(), any())).thenReturn(delegateResult);

            final var result = admin.deleteConsumerGroups(List.of(UNRESOLVED_GROUP), new DeleteConsumerGroupsOptions());

            assertThat(result.deletedGroups()).containsOnlyKeys(UNRESOLVED_GROUP);
            verify(delegate).deleteConsumerGroups(argThat(g -> g.contains(RESOLVED_GROUP)), any());
        });
    }

    @Test
    @DisplayName("deleteConsumerGroupOffsets resolves the group id and partitions")
    void deleteConsumerGroupOffsetsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            when(delegate.deleteConsumerGroupOffsets(any(), any(), any())).thenReturn(mock(DeleteConsumerGroupOffsetsResult.class));

            final var result = admin.deleteConsumerGroupOffsets(UNRESOLVED_GROUP, Set.of(UNRESOLVED_PARTITION), new DeleteConsumerGroupOffsetsOptions());

            assertThat(result).isInstanceOf(ResolvingDeleteConsumerGroupOffsetsResult.class);
            verify(delegate).deleteConsumerGroupOffsets(eq(RESOLVED_GROUP), eq(Set.of(RESOLVED_PARTITION)), any());
        });
    }

    @Test
    @DisplayName("removeMembersFromConsumerGroup resolves the group id and forwards the result")
    void removeMembersFromConsumerGroupResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(RemoveMembersFromConsumerGroupResult.class);
            when(delegate.removeMembersFromConsumerGroup(any(), any())).thenReturn(delegateResult);

            final var result = admin.removeMembersFromConsumerGroup(UNRESOLVED_GROUP, new RemoveMembersFromConsumerGroupOptions());

            assertThat(result).isSameAs(delegateResult);
            verify(delegate).removeMembersFromConsumerGroup(eq(RESOLVED_GROUP), any());
        });
    }

    @Test
    @DisplayName("alterConsumerGroupOffsets resolves the group id and offsets")
    void alterConsumerGroupOffsetsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            when(delegate.alterConsumerGroupOffsets(any(), any(), any())).thenReturn(mock(AlterConsumerGroupOffsetsResult.class));

            final var result = admin.alterConsumerGroupOffsets(UNRESOLVED_GROUP,
                    Map.of(UNRESOLVED_PARTITION, new OffsetAndMetadata(1L)), new AlterConsumerGroupOffsetsOptions());

            assertThat(result).isInstanceOf(ResolvingAlterConsumerGroupOffsetsResult.class);
            verify(delegate).alterConsumerGroupOffsets(eq(RESOLVED_GROUP),
                    eq(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(1L))), any());
        });
    }

    @Test
    @DisplayName("listOffsets resolves the requested partitions and unresolves the result")
    void listOffsetsResolves() throws Exception {
        withAdmin((admin, delegate) -> {
            final var delegateResult = mock(ListOffsetsResult.class);
            when(delegateResult.all()).thenReturn(KafkaFuture.completedFuture(
                    Map.of(RESOLVED_PARTITION, new ListOffsetsResultInfo(1L, 2L, Optional.empty()))));
            when(delegate.listOffsets(any(), any())).thenReturn(delegateResult);

            final var result = admin.listOffsets(Map.of(UNRESOLVED_PARTITION, OffsetSpec.latest()), new ListOffsetsOptions());

            assertThat(result.all().get()).containsOnlyKeys(UNRESOLVED_PARTITION);
            verify(delegate).listOffsets(argThat(m -> m.containsKey(RESOLVED_PARTITION)), any());
        });
    }

    private void withAdmin(AdminTest test) throws Exception {
        try (MockedConstruction<KafkaAdminClient> mocked = mockConstruction(KafkaAdminClient.class)) {
            final var admin = new ResolvingAdmin(CONFIGS);
            test.accept(admin, mocked.constructed().get(0));
        }
    }

    @FunctionalInterface
    private interface AdminTest {
        void accept(ResolvingAdmin admin, Admin delegate) throws Exception;
    }

    private static ConsumerGroupDescription consumerGroupDescription(String groupId) {
        return new ConsumerGroupDescription(groupId, false, List.of(), "range",
                GroupType.CONSUMER, GroupState.STABLE, Node.noNode(), Set.of(),
                Optional.empty(), Optional.empty());
    }
}
