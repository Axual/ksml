package io.axual.ksml.client.admin;

import org.apache.kafka.clients.admin.AbortTransactionOptions;
import org.apache.kafka.clients.admin.AbortTransactionResult;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.Admin;
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
import org.apache.kafka.clients.admin.AlterUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.CreateDelegationTokenResult;
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
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeDelegationTokenResult;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsOptions;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ElectLeadersOptions;
import org.apache.kafka.clients.admin.ElectLeadersResult;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.FenceProducersResult;
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
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.RenewDelegationTokenResult;
import org.apache.kafka.clients.admin.UnregisterBrokerOptions;
import org.apache.kafka.clients.admin.UnregisterBrokerResult;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ProxyAdmin implements Admin {
    private Admin backingAdmin;

    public void initializeAdmin(Admin backingAdmin) {
        if (backingAdmin == null) {
            throw new UnsupportedOperationException("Backing admin can not be null");
        }
        if (this.backingAdmin != null) {
            throw new UnsupportedOperationException("Proxy admin already initialized");
        }
        this.backingAdmin = backingAdmin;
    }

    @Override
    public void close() {
        backingAdmin.close();
    }

    @Override
    public void close(Duration duration) {
        backingAdmin.close(duration);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
        return backingAdmin.createTopics(newTopics);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> collection, CreateTopicsOptions options) {
        return backingAdmin.createTopics(collection, options);
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topics) {
        return backingAdmin.deleteTopics(topics);
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options) {
        return backingAdmin.deleteTopics(topics, options);
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topics) {
        return backingAdmin.deleteTopics(topics);
    }

    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topicCollection, DeleteTopicsOptions options) {
        return backingAdmin.deleteTopics(topicCollection, options);
    }

    @Override
    public ListTopicsResult listTopics() {
        return backingAdmin.listTopics();
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        return backingAdmin.listTopics(options);
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames) {
        return backingAdmin.describeTopics(topicNames);
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        return backingAdmin.describeTopics(topicNames, options);
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topics) {
        return backingAdmin.describeTopics(topics);
    }

    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topicCollection, DescribeTopicsOptions options) {
        return backingAdmin.describeTopics(topicCollection, options);
    }

    @Override
    public DescribeClusterResult describeCluster() {
        return backingAdmin.describeCluster();
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        return backingAdmin.describeCluster(options);
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter) {
        return backingAdmin.describeAcls(filter);
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter aclBindingFilter, DescribeAclsOptions options) {
        return backingAdmin.describeAcls(aclBindingFilter, options);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls) {
        return backingAdmin.createAcls(acls);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> collection, CreateAclsOptions options) {
        return backingAdmin.createAcls(collection, options);
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters) {
        return backingAdmin.deleteAcls(filters);
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> collection, DeleteAclsOptions options) {
        return backingAdmin.deleteAcls(collection, options);
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources) {
        return backingAdmin.describeConfigs(resources);
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> collection, DescribeConfigsOptions options) {
        return backingAdmin.describeConfigs(collection, options);
    }

    @Override
    @Deprecated
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs) {
        return backingAdmin.alterConfigs(configs);
    }

    @Override
    @Deprecated
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        return backingAdmin.alterConfigs(configs, options);
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        return backingAdmin.incrementalAlterConfigs(configs);
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
        return backingAdmin.incrementalAlterConfigs(configs, options);
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment) {
        return backingAdmin.alterReplicaLogDirs(replicaAssignment);
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> configs, AlterReplicaLogDirsOptions options) {
        return backingAdmin.alterReplicaLogDirs(configs, options);
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers) {
        return backingAdmin.describeLogDirs(brokers);
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> collection, DescribeLogDirsOptions options) {
        return backingAdmin.describeLogDirs(collection, options);
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas) {
        return backingAdmin.describeReplicaLogDirs(replicas);
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> collection, DescribeReplicaLogDirsOptions options) {
        return backingAdmin.describeReplicaLogDirs(collection, options);
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions) {
        return backingAdmin.createPartitions(newPartitions);
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        return backingAdmin.createPartitions(newPartitions, options);
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) {
        return backingAdmin.deleteRecords(recordsToDelete);
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        return backingAdmin.deleteRecords(recordsToDelete, options);
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken() {
        return backingAdmin.createDelegationToken();
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        return backingAdmin.createDelegationToken(options);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac) {
        return backingAdmin.renewDelegationToken(hmac);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] bytes, RenewDelegationTokenOptions options) {
        return backingAdmin.renewDelegationToken(bytes, options);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac) {
        return backingAdmin.expireDelegationToken(hmac);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] bytes, ExpireDelegationTokenOptions options) {
        return backingAdmin.expireDelegationToken(bytes, options);
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken() {
        return backingAdmin.describeDelegationToken();
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        return backingAdmin.describeDelegationToken(options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        return backingAdmin.describeConsumerGroups(groupIds, options);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds) {
        return backingAdmin.describeConsumerGroups(groupIds);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        return backingAdmin.listConsumerGroups(options);
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups() {
        return backingAdmin.listConsumerGroups();
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options) {
        return backingAdmin.listConsumerGroupOffsets(groupId, options);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return backingAdmin.listConsumerGroupOffsets(groupId);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options) {
        return backingAdmin.listConsumerGroupOffsets(groupSpecs, options);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs) {
        return backingAdmin.listConsumerGroupOffsets(groupSpecs);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        return backingAdmin.deleteConsumerGroups(groupIds, options);
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds) {
        return backingAdmin.deleteConsumerGroups(groupIds);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        return backingAdmin.deleteConsumerGroupOffsets(groupId, partitions, options);
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions) {
        return backingAdmin.deleteConsumerGroupOffsets(groupId, partitions);
    }

    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions) {
        return backingAdmin.electLeaders(electionType, partitions);
    }

    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions, ElectLeadersOptions options) {
        return backingAdmin.electLeaders(electionType, partitions, options);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {
        return backingAdmin.alterPartitionReassignments(reassignments);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments, AlterPartitionReassignmentsOptions options) {
        return backingAdmin.alterPartitionReassignments(reassignments, options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments() {
        return backingAdmin.listPartitionReassignments();
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Set<TopicPartition> partitions) {
        return backingAdmin.listPartitionReassignments(partitions);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Set<TopicPartition> partitions, ListPartitionReassignmentsOptions options) {
        return backingAdmin.listPartitionReassignments(partitions, options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(ListPartitionReassignmentsOptions options) {
        return backingAdmin.listPartitionReassignments(options);
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions, ListPartitionReassignmentsOptions options) {
        return backingAdmin.listPartitionReassignments(partitions, options);
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options) {
        return backingAdmin.removeMembersFromConsumerGroup(groupId, options);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
        return backingAdmin.alterConsumerGroupOffsets(groupId, offsets);
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        return backingAdmin.alterConsumerGroupOffsets(groupId, offsets, options);
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
        return backingAdmin.listOffsets(topicPartitionOffsets);
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        return backingAdmin.listOffsets(topicPartitionOffsets, options);
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter) {
        return backingAdmin.describeClientQuotas(filter);
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter clientQuotaFilter, DescribeClientQuotasOptions options) {
        return backingAdmin.describeClientQuotas(clientQuotaFilter, options);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries) {
        return backingAdmin.alterClientQuotas(entries);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        return backingAdmin.alterClientQuotas(entries, options);
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials() {
        return backingAdmin.describeUserScramCredentials();
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users) {
        return backingAdmin.describeUserScramCredentials(users);
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
        return backingAdmin.describeUserScramCredentials(users, options);
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations) {
        return backingAdmin.alterUserScramCredentials(alterations);
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options) {
        return backingAdmin.alterUserScramCredentials(alterations, options);
    }

    @Override
    public DescribeFeaturesResult describeFeatures() {
        return backingAdmin.describeFeatures();
    }

    @Override
    public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions describeFeaturesOptions) {
        return backingAdmin.describeFeatures(describeFeaturesOptions);
    }

    @Override
    public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options) {
        return backingAdmin.updateFeatures(featureUpdates, options);
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum() {
        return backingAdmin.describeMetadataQuorum();
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
        return backingAdmin.describeMetadataQuorum(options);
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId) {
        return backingAdmin.unregisterBroker(brokerId);
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        return backingAdmin.unregisterBroker(brokerId, options);
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions) {
        return backingAdmin.describeProducers(partitions);
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions, DescribeProducersOptions options) {
        return backingAdmin.describeProducers(partitions, options);
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds) {
        return backingAdmin.describeTransactions(transactionalIds);
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        return backingAdmin.describeTransactions(transactionalIds, options);
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec) {
        return backingAdmin.abortTransaction(spec);
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        return backingAdmin.abortTransaction(spec, options);
    }

    @Override
    public ListTransactionsResult listTransactions() {
        return backingAdmin.listTransactions();
    }

    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        return backingAdmin.listTransactions(options);
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds) {
        return backingAdmin.fenceProducers(transactionalIds);
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        return backingAdmin.fenceProducers(transactionalIds, options);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return backingAdmin.metrics();
    }
}
