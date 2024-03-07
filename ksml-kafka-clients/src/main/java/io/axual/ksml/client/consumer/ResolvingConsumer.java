package io.axual.ksml.client.consumer;

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

import io.axual.ksml.client.resolving.GroupResolver;
import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class ResolvingConsumer<K, V> extends ForwardingConsumer<K, V> {
    private final TopicResolver topicResolver;
    private final GroupResolver groupResolver;

    public ResolvingConsumer(Map<String, Object> configs) {
        var config = new ResolvingConsumerConfig(configs);
        initializeConsumer(new KafkaConsumer<>(config.downstreamConfigs()));
        topicResolver = config.topicResolver();
        groupResolver = config.groupResolver();
    }

    @Override
    public Set<TopicPartition> assignment() {
        return topicResolver.unresolveTopicPartitions(super.assignment());
    }

    @Override
    public Set<String> subscription() {
        return topicResolver.unresolve(super.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        super.subscribe(topicResolver.resolve(topics), convertListener(listener));
    }

    @Override
    public void subscribe(Collection<String> topics) {
        super.subscribe(topicResolver.resolve(topics));
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        Pattern resolvedPattern = topicResolver.resolve(pattern);
        super.subscribe(resolvedPattern, convertListener(listener));
    }

    @Override
    public void subscribe(Pattern pattern) {
        throw new UnsupportedOperationException("Subscribing to (unresolved) patterns is not supported");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        super.assign(topicResolver.resolveTopicPartitions(partitions));
    }
    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        try {
            return convertRecords(super.poll(timeout));
        } catch (NoOffsetForPartitionException e) {
            throw new NoOffsetForPartitionException(
                    topicResolver.unresolveTopicPartitions(e.partitions()));
        } catch (LogTruncationException e) {
            throw new LogTruncationException(
                    e.getMessage(),
                    topicResolver.unresolve(e.offsetOutOfRangePartitions()),
                    topicResolver.unresolve(e.divergentOffsets()));
        } catch (OffsetOutOfRangeException e) {
            throw new OffsetOutOfRangeException(
                    e.getMessage(),
                    topicResolver.unresolve(e.offsetOutOfRangePartitions()));
        }
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        try {
            return convertRecords(super.poll(timeout));
        } catch (NoOffsetForPartitionException e) {
            throw new NoOffsetForPartitionException(
                    topicResolver.unresolveTopicPartitions(e.partitions()));
        } catch (LogTruncationException e) {
            throw new LogTruncationException(
                    e.getMessage(),
                    topicResolver.unresolve(e.offsetOutOfRangePartitions()),
                    topicResolver.unresolve(e.divergentOffsets()));
        } catch (OffsetOutOfRangeException e) {
            throw new OffsetOutOfRangeException(
                    e.getMessage(),
                    topicResolver.unresolve(e.offsetOutOfRangePartitions()));
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        super.commitSync(topicResolver.resolve(offsets));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        super.commitSync(topicResolver.resolve(offsets), timeout);
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
            super.commitAsync(topicResolver.resolve(offsets), null);
        } else {
            super.commitAsync(topicResolver.resolve(offsets),
                    new ProxyOffsetCommitCallback(callback));
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        super.seek(topicResolver.resolve(partition), offset);
    }

    @Override
    public void seek(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        Set<TopicPartition> resolvedTopicPartition = topicResolver
                .resolveTopicPartitions(Collections.singleton(topicPartition));
        super.seek(resolvedTopicPartition.toArray(new TopicPartition[1])[0], offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        super.seekToBeginning(topicResolver.resolveTopicPartitions(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        super.seekToEnd(topicResolver.resolveTopicPartitions(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
        return super.position(topicResolver.resolve(partition));
    }

    @Override
    public long position(TopicPartition topicPartition, Duration duration) {
        return super.position(topicResolver.resolve(topicPartition), duration);
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return super.committed(topicResolver.resolve(partition));
    }

    /**
     * @deprecated
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return super.committed(topicResolver.resolve(partition), timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return unresolveTopicPartitionOffsetAndMetadataMap(super.committed(topicResolver.resolveTopicPartitions(partitions)));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions,
                                                            Duration timeout) {
        return unresolveTopicPartitionOffsetAndMetadataMap(super.committed(topicResolver.resolveTopicPartitions(partitions), timeout));
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        ConsumerGroupMetadata groupMetadata = super.groupMetadata();
        return groupMetadata == null ? null : new ConsumerGroupMetadata(
                groupResolver.unresolve(groupMetadata.groupId()),
                groupMetadata.generationId(), groupMetadata.memberId(), groupMetadata.groupInstanceId());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return convertPartitionInfo(super.partitionsFor(topicResolver.resolve(topic)),
                topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return convertPartitionInfo(
                super.partitionsFor(topicResolver.resolve(topic), timeout), topic);
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
        super.pause(topicResolver.resolveTopicPartitions(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        super.resume(topicResolver.resolveTopicPartitions(partitions));
    }

    @Override
    public Set<TopicPartition> paused() {
        return topicResolver.unresolveTopicPartitions(super.paused());
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch) {
        return topicResolver.unresolve(
                super.offsetsForTimes(
                        topicResolver.resolve(timestampsToSearch)));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return topicResolver.unresolve(
                super.offsetsForTimes(
                        topicResolver.resolve(timestampsToSearch),
                        timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return topicResolver.unresolve(
                super.beginningOffsets(
                        topicResolver.resolveTopicPartitions(partitions)));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions,
                                                      Duration timeout) {
        return topicResolver.unresolve(
                super.beginningOffsets(
                        topicResolver.resolveTopicPartitions(partitions),
                        timeout));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return topicResolver.unresolve(
                super.endOffsets(
                        topicResolver.resolveTopicPartitions(partitions)));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions,
                                                Duration timeout) {
        return topicResolver.unresolve(
                super.endOffsets(
                        topicResolver.resolveTopicPartitions(partitions),
                        timeout));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return super.currentLag(topicResolver.resolve(topicPartition));
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
                            topicResolver.unresolve(consumerRecord.topic()),
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
                        .put(topicResolver.unresolve(topicPartition), partitionRecords);
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
                        new ResolvingPartitionInfo(topicResolver.unresolve(info.topic()),
                                info.partition()));
            }
            result.put(topicResolver.unresolve(topic), resultInfos);
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
                                (map, topicPartition) -> map.put(new TopicPartition(topicResolver.unresolve(topicPartition.topic()),
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
            listener.onPartitionsRevoked(topicResolver.unresolveTopicPartitions(collection));
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            listener.onPartitionsAssigned(topicResolver.unresolveTopicPartitions(collection));
        }
    }

    private final class ProxyOffsetCommitCallback implements OffsetCommitCallback {
        private final OffsetCommitCallback callback;

        ProxyOffsetCommitCallback(OffsetCommitCallback callback) {
            this.callback = callback;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
            callback.onComplete(topicResolver.unresolve(offsets), e);
        }
    }
}
