package io.axual.ksml.client.consumer;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.client.resolving.TopicResolver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.state.HostInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ResolvingConsumerPartitionAssignor implements ConsumerPartitionAssignor, Configurable {
    private ConsumerPartitionAssignor backingAssignor = null;
    private TopicResolver resolver = null;

    @Override
    public void configure(Map<String, ?> configs) {
        ResolvingConsumerPartitionAssignorConfig config = new ResolvingConsumerPartitionAssignorConfig(new HashMap<>(configs));
        backingAssignor = config.getBackingAssignor();
        resolver = config.getTopicResolver();
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return backingAssignor.subscriptionUserData(topics);
    }

    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription subscriptions) {
        log.debug("Assigning: " + subscriptions.groupSubscription());
        Cluster unresolvedCluster = new Cluster(
                cluster.clusterResource().clusterId(),
                cluster.nodes(),
                unresolveClusterPartitions(cluster),
                resolver.unresolveTopics(cluster.unauthorizedTopics()),
                cluster.internalTopics(),
                cluster.controller());

        Map<String, Subscription> unresolvedSubscriptions = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> subscription : subscriptions.groupSubscription().entrySet()) {
            unresolvedSubscriptions.put(subscription.getKey(), unresolveSubscription(subscription.getValue()));
        }

        GroupSubscription unresolvedGroupSubscriptions = new GroupSubscription(unresolvedSubscriptions);

        log.debug("Assigning unresolved: " + unresolvedSubscriptions);
        GroupAssignment assignments = backingAssignor.assign(unresolvedCluster, unresolvedGroupSubscriptions);
        log.debug("Assigned unresolved: " + assignments.groupAssignment());

        Map<String, Assignment> result = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.groupAssignment().entrySet()) {
            result.put(assignmentEntry.getKey(), resolveAssignment(assignmentEntry.getValue()));
        }

        log.debug("Assigned resolved: " + result);
        return new GroupAssignment(result);
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        try {
            log.debug("Getting assignment: " + assignment);
            var unresolvedAssignment = unresolveAssignment(assignment);
            log.debug("Unresolved assignment: " + unresolvedAssignment);
            backingAssignor.onAssignment(unresolvedAssignment, metadata);
        } catch (Throwable t) {
            log.error("Error during topic partition assignment", t);
            throw t;
        }
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return backingAssignor.supportedProtocols();
    }

    @Override
    public short version() {
        return 1;
    }

    @Override
    public String name() {
        return backingAssignor.name();
    }

    private List<PartitionInfo> unresolveClusterPartitions(Cluster proxiedCluster) {
        List<PartitionInfo> result = new ArrayList<>();
        for (String topic : proxiedCluster.topics()) {
            List<PartitionInfo> partitions = proxiedCluster.availablePartitionsForTopic(topic);
            for (PartitionInfo partition : partitions) {
                result.add(new PartitionInfo(resolver.unresolveTopic(partition.topic()),
                        partition.partition(),
                        partition.leader(),
                        partition.replicas(),
                        partition.inSyncReplicas(),
                        partition.offlineReplicas()));
            }
        }
        return result;
    }

    private Assignment resolveAssignment(Assignment assignment) {
        return new Assignment(
                new ArrayList<>(resolver.resolveTopicPartitions(assignment.partitions())),
                assignment.userData()
        );
    }

    private Assignment unresolveAssignment(Assignment assignment) {
        var userData = assignment.userData();
        try {
            var info = AssignmentInfo.decode(userData);

            // Convert active tasks
            var newActiveTasks = new ArrayList<TaskId>();
            info.activeTasks().forEach(id -> newActiveTasks.add(new TaskId(id.subtopology(), id.partition(), id.topologyName())));

            // Convert standby tasks
            var newStandbyTasks = new HashMap<TaskId, Set<TopicPartition>>();
            info.standbyTasks().forEach((id, topicPartitions) -> newStandbyTasks.put(new TaskId(id.subtopology(), id.partition(), id.topologyName()), resolver.unresolveTopicPartitions(topicPartitions)));

            // Convert partitions by host
            var newPartitionsByHost = new HashMap<HostInfo, Set<TopicPartition>>();
            info.partitionsByHost().forEach((hostInfo, topicPartitions) -> newPartitionsByHost.put(hostInfo, resolver.unresolveTopicPartitions(topicPartitions)));

            // Convert standby partitions by host
            var newStandbyPartitionsByHost = new HashMap<HostInfo, Set<TopicPartition>>();
            info.standbyPartitionByHost().forEach(((hostInfo, topicPartitions) -> newStandbyPartitionsByHost.put(hostInfo, resolver.unresolveTopicPartitions(topicPartitions))));

            userData = new AssignmentInfo(
                    info.version(),
                    info.commonlySupportedVersion(),
                    newActiveTasks,
                    newStandbyTasks,
                    newPartitionsByHost,
                    newStandbyPartitionsByHost,
                    info.errCode()).encode();
        } catch (Exception e) {
            // Ignore decoding exceptions
        }

        return new Assignment(new ArrayList<>(resolver.unresolveTopicPartitions(assignment.partitions())), userData);
    }

    private Subscription unresolveSubscription(Subscription subscription) {
        return new Subscription(
                new ArrayList<>(resolver.unresolveTopics(subscription.topics())),
                subscription.userData(),
                new ArrayList<>(resolver.unresolveTopicPartitions(subscription.ownedPartitions()))
        );
    }
}
