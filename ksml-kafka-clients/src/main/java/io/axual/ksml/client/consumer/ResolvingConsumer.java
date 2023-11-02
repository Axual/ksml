package io.axual.ksml.client.consumer;

/*-
 * ========================LICENSE_START=================================
 * axual-client-proxy
 * %%
 * Copyright (C) 2020 Axual B.V.
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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

public class ResolvingConsumer<K, V> extends ProxyConsumer<K, V> {
    private final ResolvingConsumerConfig config;

    public ResolvingConsumer(Map<String, Object> configs) {
        config = new ResolvingConsumerConfig(configs);
        initializeConsumer(new KafkaConsumer<>(config.getDownstreamConfigs()));
    }

    @Override
    public Set<TopicPartition> assignment() {
        return config.getTopicResolver().unresolveTopicPartitions(super.assignment());
    }

    @Override
    public Set<String> subscription() {
        return config.getTopicResolver().unresolveTopics(super.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        super.subscribe(
                config.getTopicResolver().resolveTopics(topics),
                convertListener(listener));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        super.subscribe(config.getTopicResolver().resolveTopics(topics));
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        Pattern resolvedPattern = config.getTopicResolver().resolveTopicPattern(pattern);
        super.subscribe(resolvedPattern, convertListener(listener));
    }

    @Override
    public void subscribe(Pattern pattern) {
        throw new UnsupportedOperationException("Subscribing to (unresolved) patterns is not supported");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        super.assign(config.getTopicResolver().resolveTopicPartitions(partitions));
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        return convertRecords(super.poll(timeout));
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return convertRecords(super.poll(timeout));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        super.commitSync(config.getTopicResolver().resolveTopics(offsets));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        super.commitSync(config.getTopicResolver().resolveTopics(offsets), timeout);
    }

    @Override
    public void commitAsync() {
        super.commitAsync();
    }

    @Override
    public void commitAsync(final OffsetCommitCallback callback) {
        if (callback == null) {
            super.commitAsync(null);
        } else {
            super.commitAsync(new ProxyOffsetCommitCallback(callback));
        }
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                            OffsetCommitCallback callback) {
        if (callback == null) {
            super.commitAsync(config.getTopicResolver().resolveTopics(offsets), null);
        } else {
            super.commitAsync(config.getTopicResolver().resolveTopics(offsets),
                    new ProxyOffsetCommitCallback(callback));
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        super.seek(config.getTopicResolver().resolveTopic(partition), offset);
    }

    @Override
    public void seek(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        Set<TopicPartition> resolvedTopicPartition = config.getTopicResolver()
                .resolveTopicPartitions(Collections.singleton(topicPartition));
        super.seek(resolvedTopicPartition.toArray(new TopicPartition[1])[0], offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        super.seekToBeginning(config.getTopicResolver().resolveTopicPartitions(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        super.seekToEnd(config.getTopicResolver().resolveTopicPartitions(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
        return super.position(config.getTopicResolver().resolveTopic(partition));
    }

    @Override
    public long position(TopicPartition topicPartition, Duration duration) {
        return super.position(config.getTopicResolver().resolveTopic(topicPartition), duration);
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return super.committed(config.getTopicResolver().resolveTopic(partition));
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return super.committed(config.getTopicResolver().resolveTopic(partition), timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return unresolveTopicPartitionOffsetAndMetadataMap(super.committed(config.getTopicResolver().resolveTopicPartitions(partitions)));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions,
                                                            Duration timeout) {
        return unresolveTopicPartitionOffsetAndMetadataMap(super.committed(config.getTopicResolver().resolveTopicPartitions(partitions), timeout));
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        ConsumerGroupMetadata groupMetadata = super.groupMetadata();
        return groupMetadata == null ? null : new ConsumerGroupMetadata(
                config.getGroupResolver().unresolveGroup(groupMetadata.groupId()),
                groupMetadata.generationId(), groupMetadata.memberId(), groupMetadata.groupInstanceId());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return convertPartitionInfo(super.partitionsFor(config.getTopicResolver().resolveTopic(topic)),
                topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return convertPartitionInfo(
                super.partitionsFor(config.getTopicResolver().resolveTopic(topic), timeout), topic);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return (convertTopicList(super.listTopics()));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return (convertTopicList(super.listTopics(timeout)));
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        super.pause(config.getTopicResolver().resolveTopicPartitions(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        super.resume(config.getTopicResolver().resolveTopicPartitions(partitions));
    }

    @Override
    public Set<TopicPartition> paused() {
        return config.getTopicResolver().unresolveTopicPartitions(super.paused());
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch) {
        return config.getTopicResolver().unresolveTopics(
                super.offsetsForTimes(
                        config.getTopicResolver().resolveTopics(timestampsToSearch)));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return config.getTopicResolver().unresolveTopics(
                super.offsetsForTimes(
                        config.getTopicResolver().resolveTopics(timestampsToSearch),
                        timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return config.getTopicResolver().unresolveTopics(
                super.beginningOffsets(
                        config.getTopicResolver().resolveTopicPartitions(partitions)));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions,
                                                      Duration timeout) {
        return config.getTopicResolver().unresolveTopics(
                super.beginningOffsets(
                        config.getTopicResolver().resolveTopicPartitions(partitions),
                        timeout));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return config.getTopicResolver().unresolveTopics(
                super.endOffsets(
                        config.getTopicResolver().resolveTopicPartitions(partitions)));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions,
                                                Duration timeout) {
        return config.getTopicResolver().unresolveTopics(
                super.endOffsets(
                        config.getTopicResolver().resolveTopicPartitions(partitions),
                        timeout));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return super.currentLag(config.getTopicResolver().resolveTopic(topicPartition));
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // End of public interface of KafkaConsumer
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private ConsumerRebalanceListener convertListener(ConsumerRebalanceListener listener) {
        return listener != null ? new ProxyConsumerRebalanceListener(listener) : null;
    }

    private ConsumerRecords<K, V> convertRecords(ConsumerRecords<K, V> records) {
        final Map<TopicPartition, List<ConsumerRecord<K, V>>> recordsByPartition = new HashMap<>();
        if (!records.isEmpty()) {
            for (TopicPartition topicPartition : records.partitions()) {
                List<ConsumerRecord<K, V>> partitionRecords = new ArrayList<>();

                for (ConsumerRecord<K, V> consumerRecord : records.records(topicPartition)) {
                    partitionRecords.add(new ConsumerRecord<>(
                            config.getTopicResolver().unresolveTopic(consumerRecord.topic()),
                            consumerRecord.partition(),
                            consumerRecord.offset(),
                            consumerRecord.timestamp(),
                            consumerRecord.timestampType(),
                            consumerRecord.serializedKeySize(),
                            consumerRecord.serializedValueSize(),
                            consumerRecord.key(),
                            consumerRecord.value(),
                            consumerRecord.headers(),
                            Optional.empty()));
                }
                recordsByPartition
                        .put(config.getTopicResolver().unresolveTopic(topicPartition), partitionRecords);
            }
        }

        return new ConsumerRecords<>(recordsByPartition);
    }

    private Map<String, List<PartitionInfo>> convertTopicList(
            Map<String, List<PartitionInfo>> topicList) {
        Map<String, List<PartitionInfo>> result = new HashMap<>(topicList.size());

        for (Map.Entry<String, List<PartitionInfo>> entry : topicList.entrySet()) {
            String topic = entry.getKey();

            List<PartitionInfo> infos = topicList.get(topic);
            List<PartitionInfo> resultInfos = new ArrayList<>(infos.size());
            for (PartitionInfo info : infos) {
                resultInfos.add(
                        new ResolvingPartitionInfo(config.getTopicResolver().unresolveTopic(info.topic()),
                                info.partition()));
            }
            result.put(config.getTopicResolver().unresolveTopic(topic), resultInfos);
        }
        return result;
    }

    private List<PartitionInfo> convertPartitionInfo(List<PartitionInfo> partitionInfoList,
                                                     String unresolvedTopic) {
        if (partitionInfoList == null) {
            return null;
        }

        List<PartitionInfo> result = new ArrayList<>(partitionInfoList.size());
        for (PartitionInfo partitionInfo : partitionInfoList) {
            result.add(new ResolvingPartitionInfo(unresolvedTopic, partitionInfo.partition()));
        }

        return result;
    }

    /**
     * Method to unresolve TopicPartition in OffsetAndMetadataMap
     */
    private Map<TopicPartition, OffsetAndMetadata> unresolveTopicPartitionOffsetAndMetadataMap(Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap) {
        return topicPartitionOffsetAndMetadataMap == null || topicPartitionOffsetAndMetadataMap.isEmpty() ? topicPartitionOffsetAndMetadataMap :
                topicPartitionOffsetAndMetadataMap.keySet()
                        .stream()
                        .collect(HashMap::new,
                                (map, topicPartition) -> map.put(new TopicPartition(config.getTopicResolver().unresolveTopic(topicPartition.topic()),
                                                topicPartition.partition()),
                                        topicPartitionOffsetAndMetadataMap.get(topicPartition)), HashMap::putAll);

    }

    private final class ProxyConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final ConsumerRebalanceListener listener;

        ProxyConsumerRebalanceListener(ConsumerRebalanceListener listener) {
            this.listener = listener;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            listener.onPartitionsRevoked(config.getTopicResolver().unresolveTopicPartitions(collection));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            listener.onPartitionsAssigned(config.getTopicResolver().unresolveTopicPartitions(collection));
        }
    }

    private final class ProxyOffsetCommitCallback implements OffsetCommitCallback {
        private final OffsetCommitCallback callback;

        ProxyOffsetCommitCallback(OffsetCommitCallback callback) {
            this.callback = callback;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
            callback.onComplete(config.getTopicResolver().unresolveTopics(offsets), e);
        }
    }
}
