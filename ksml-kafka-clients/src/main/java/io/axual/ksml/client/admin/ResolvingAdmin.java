package io.axual.ksml.client.admin;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import io.axual.ksml.client.resolving.GroupResolver;
import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.util.*;

public class ResolvingAdmin extends ForwardingAdmin {
    private final TopicResolver topicResolver;
    private final GroupResolver groupResolver;

    public ResolvingAdmin(Map<String, Object> configs) {
        super(configs);
        final var config = new ResolvingClientConfig(configs);
        topicResolver = config.topicResolver;
        groupResolver = config.groupResolver;
    }

    protected void operationNotSupported(String operation) {
        throw new NotSupportedException("ResolvingAdmin " + this.getClass().getSimpleName() + " does not support " + operation);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        return new ResolvingCreateTopicsResult(super.createTopics(resolveNewTopics(newTopics), options), topicResolver);
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topicCollection, DeleteTopicsOptions options) {
        final var result = super.deleteTopics(topicResolver.resolve(topicCollection), options);
        return new ResolvingDeleteTopicsResult(result.topicIdValues(), result.topicNameValues(), topicResolver);
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        return new ResolvingListTopicsResult(super.listTopics(options).namesToListings(), topicResolver);
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topicCollection, DescribeTopicsOptions options) {
        final var result = super.describeTopics(topicResolver.resolve(topicCollection), options);
        return new ResolvingDescribeTopicsResult(result.topicIdValues(), result.topicNameValues(), topicResolver);
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        final var result = super.describeAcls(ResolverUtil.resolve(filter, topicResolver, groupResolver), options);
        if (result == null) return null;
        return new ResolvingDescribeAclsResult(result.values(), topicResolver, groupResolver);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> collection, CreateAclsOptions options) {
        operationNotSupported("createAcls");
        return null;
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> collection, DeleteAclsOptions options) {
        operationNotSupported("deleteAcls");
        return null;
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> collection, DescribeConfigsOptions options) {
        operationNotSupported("describeConfigs");
        return null;
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
        operationNotSupported("incrementalAlterConfigs");
        return null;
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> configs, AlterReplicaLogDirsOptions options) {
        operationNotSupported("alterReplicaLogDirs");
        return null;
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> collection, DescribeLogDirsOptions options) {
        operationNotSupported("describeLogDirs");
        return null;
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> collection, DescribeReplicaLogDirsOptions options) {
        operationNotSupported("describeReplicaLogDirs");
        return null;
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        operationNotSupported("createPartitions");
        return null;
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        return super.deleteRecords(topicResolver.resolve(recordsToDelete), options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        return new ResolvingDescribeConsumerGroupsResult(
                super.describeConsumerGroups(groupResolver.resolve(groupIds), options).describedGroups(),
                groupResolver);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        return new ResolvingListConsumerGroupsResult(super.listConsumerGroups(options), groupResolver);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options) {
        // Resolve the groupSpecs
        final var newGroupSpecs = new HashMap<String, ListConsumerGroupOffsetsSpec>();
        groupSpecs.forEach((groupId, spec) -> newGroupSpecs.put(groupResolver.resolve(groupId), new ListConsumerGroupOffsetsSpec().topicPartitions(topicResolver.resolveTopicPartitions(spec.topicPartitions()))));

        // Call the original API
        final var result = super.listConsumerGroupOffsets(newGroupSpecs, options);

        // Convert the result to an unresolved result
        final var newResult = new HashMap<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>>();
        newGroupSpecs.keySet().forEach(groupId -> {
            final var future = result.partitionsToOffsetAndMetadata(groupId);
            newResult.put(CoordinatorKey.byGroupId(groupId), future);
        });

        return new ResolvingListConsumerGroupOffsetsResult(newResult, topicResolver, groupResolver);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        return new ResolvingDeleteConsumerGroupsResult(
                super.deleteConsumerGroups(groupResolver.resolve(groupIds), options).deletedGroups(),
                groupResolver);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        return new ResolvingDeleteConsumerGroupOffsetsResult(
                super.deleteConsumerGroupOffsets(
                        groupResolver.resolve(groupId),
                        topicResolver.resolveTopicPartitions(partitions),
                        options),
                topicResolver);
    }

    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions, ElectLeadersOptions options) {
        operationNotSupported("electLeaders");
        return null;
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
            AlterPartitionReassignmentsOptions options) {
        operationNotSupported("alterPartitionReassignments");
        return null;
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
                                                                       ListPartitionReassignmentsOptions options) {
        operationNotSupported("listPartitionReassignments");
        return null;
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId,
                                                                               RemoveMembersFromConsumerGroupOptions options) {
        return super.removeMembersFromConsumerGroup(groupResolver.resolve(groupId), options);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId,
                                                                     Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                     AlterConsumerGroupOffsetsOptions options) {
        return new ResolvingAlterConsumerGroupOffsetsResult(
                super.alterConsumerGroupOffsets(groupResolver.resolve(groupId), topicResolver.resolve(offsets), options),
                topicResolver);
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                         ListOffsetsOptions options) {
        return new ResolvingListOffsetsResult(
                super.listOffsets(topicResolver.resolve(topicPartitionOffsets), options),
                topicResolver);
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter,
                                                           DescribeClientQuotasOptions options) {
        operationNotSupported("describeClientQuotas");
        return null;
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries,
                                                     AlterClientQuotasOptions options) {
        operationNotSupported("alterClientQuotas");
        return null;
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        // Ignore superclass exception
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        // Ignore superclass exception
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // End of public interface of AdminClient

    /// ////////////////////////////////////////////////////////////////////////////////////////////

    private Collection<NewTopic> resolveNewTopics(Collection<NewTopic> newTopics) {
        // Resolve all new topics into a new collection
        final var resolvedTopics = new ArrayList<NewTopic>();
        for (final var newTopic : newTopics) {
            final var resolvedTopic = newTopic.replicasAssignments() == null
                    ? new NewTopic(topicResolver.resolve(newTopic.name()), newTopic.numPartitions(), newTopic.replicationFactor())
                    : new NewTopic(topicResolver.resolve(newTopic.name()), newTopic.replicasAssignments());
            // Make sure that the config is added properly. Cleanup properties and timestamps are typical properties set in Streams
            resolvedTopic.configs(newTopic.configs());
            resolvedTopics.add(resolvedTopic);
        }
        return resolvedTopics;
    }
}
