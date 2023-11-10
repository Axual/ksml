package io.axual.ksml.client.admin;

/*-
 * ========================LICENSE_START=================================
 * Kafka clients for KSML
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
import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.resolving.GroupResolver;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult;
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
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ForwardingAdmin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ResolvingAdmin extends ForwardingAdmin {
    private final TopicResolver topicResolver;
    private final GroupResolver groupResolver;

    public ResolvingAdmin(Map<String, Object> configs) {
        super(configs);
        var config = new ResolvingClientConfig(configs);
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
        var result = super.deleteTopics(topicResolver.resolveTopics(topicCollection), options);
        return new ResolvingDeleteTopicsResult(result.topicIdValues(), result.topicNameValues(), topicResolver);
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        return new ResolvingListTopicsResult(super.listTopics(options).namesToListings(), topicResolver);
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topicCollection, DescribeTopicsOptions options) {
        var result = super.describeTopics(topicResolver.resolveTopics(topicCollection), options);
        return new ResolvingDescribeTopicsResult(result.topicIdValues(), result.topicNameValues(), topicResolver);
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        var result = super.describeAcls(ResolverUtil.resolve(filter, topicResolver, groupResolver), options);
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
    @Deprecated
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        operationNotSupported("alterConfigs");
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
        return super.deleteRecords(topicResolver.resolveTopics(recordsToDelete), options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
                                                               DescribeConsumerGroupsOptions options) {
        return new ResolvingDescribeConsumerGroupsResult(
                super.describeConsumerGroups(groupResolver.resolveGroups(groupIds), options).describedGroups(),
                groupResolver);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        return new ResolvingListConsumerGroupsResult(super.listConsumerGroups(options), groupResolver);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options) {
        // Resolve the groupSpecs
        var newGroupSpecs = new HashMap<String, ListConsumerGroupOffsetsSpec>();
        groupSpecs.forEach((groupId, spec) -> newGroupSpecs.put(groupResolver.resolveGroup(groupId), new ListConsumerGroupOffsetsSpec().topicPartitions(spec.topicPartitions())));

        // Resolve the options
        if (options != null) {
            var newOptions = new ListConsumerGroupOffsetsOptions().requireStable(options.requireStable());
            if (options.topicPartitions() != null) {
                var newTopicPartitions = new ArrayList<TopicPartition>(options.topicPartitions().size());
                options.topicPartitions().forEach(tp -> newTopicPartitions.add(topicResolver.resolveTopic(tp)));
                newOptions.topicPartitions(newTopicPartitions);
            }
            options = newOptions;
        }

        // Call the original API
        var result = super.listConsumerGroupOffsets(newGroupSpecs, options);
        // Convert the result to an unresolving result
        var newResult = new HashMap<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>>();
        newGroupSpecs.keySet().forEach(groupId -> {
            var future = result.partitionsToOffsetAndMetadata(groupId);
            newResult.put(CoordinatorKey.byGroupId(groupId), future);
        });
        return new ResolvingListConsumerGroupOffsetsResult(newResult, topicResolver, groupResolver);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        return new ResolvingDeleteConsumerGroupsResult(
                super.deleteConsumerGroups(groupResolver.resolveGroups(groupIds), options).deletedGroups(),
                groupResolver);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        return new ResolvingDeleteConsumerGroupOffsetsResult(
                super.deleteConsumerGroupOffsets(
                        groupResolver.resolveGroup(groupId),
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
        return super.removeMembersFromConsumerGroup(groupResolver.resolveGroup(groupId), options);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId,
                                                                     Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                     AlterConsumerGroupOffsetsOptions options) {
        return new ResolvingAlterConsumerGroupOffsetsResult(
                super.alterConsumerGroupOffsets(groupResolver.resolveGroup(groupId), topicResolver.resolveTopics(offsets), options),
                topicResolver);
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                         ListOffsetsOptions options) {
        return new ResolvingListOffsetsResult(
                super.listOffsets(topicResolver.resolveTopics(topicPartitionOffsets), options),
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

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // End of public interface of AdminClient
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private Collection<NewTopic> resolveNewTopics(Collection<NewTopic> newTopics) {
        // Resolve all new topics into a new collection
        var resolvedTopics = new ArrayList<NewTopic>();
        for (var newTopic : newTopics) {
            resolvedTopics.add(newTopic.replicasAssignments() == null
                    ? new NewTopic(topicResolver.resolveTopic(newTopic.name()), newTopic.numPartitions(), newTopic.replicationFactor())
                    : new NewTopic(topicResolver.resolveTopic(newTopic.name()), newTopic.replicasAssignments()));
        }
        return resolvedTopics;
    }
}
