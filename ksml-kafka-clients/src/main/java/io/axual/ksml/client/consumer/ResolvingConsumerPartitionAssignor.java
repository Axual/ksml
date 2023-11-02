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
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.PartitionInfo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ResolvingConsumerPartitionAssignor implements ConsumerPartitionAssignor, Configurable {
    private ConsumerPartitionAssignor proxiedObject = null;
    private TopicResolver resolver = null;

    @Override
    public void configure(Map<String, ?> configs) {
        ResolvingConsumerPartitionAssignorConfig config = new ResolvingConsumerPartitionAssignorConfig(new HashMap<>(configs));
        proxiedObject = config.getBackingAssignor();
        resolver = config.getTopicResolver();
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        return proxiedObject.subscriptionUserData(topics);
    }

    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription subscriptions) {
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

        GroupAssignment assignments = proxiedObject.assign(unresolvedCluster, unresolvedGroupSubscriptions);

        Map<String, Assignment> result = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.groupAssignment().entrySet()) {
            result.put(assignmentEntry.getKey(), resolveAssignment(assignmentEntry.getValue()));
        }
        return new GroupAssignment(result);
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        proxiedObject.onAssignment(unresolveAssignment(assignment), metadata);
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        return proxiedObject.supportedProtocols();
    }

    @Override
    public short version() {
        return 1;
    }

    @Override
    public String name() {
        return proxiedObject.name();
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
        return new Assignment(
                new ArrayList<>(resolver.unresolveTopicPartitions(assignment.partitions())),
                assignment.userData()
        );
    }

    private Subscription unresolveSubscription(Subscription subscription) {
        return new Subscription(
                new ArrayList<>(resolver.unresolveTopics(subscription.topics())),
                subscription.userData(),
                new ArrayList<>(resolver.unresolveTopicPartitions(subscription.ownedPartitions()))
        );
    }
}
