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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

public class ProxyConsumer<K, V> implements Consumer<K, V> {
    private Consumer<K, V> backingConsumer;

    public void initializeConsumer(Consumer<K, V> backingConsumer) {
        if (backingConsumer == null) {
            throw new UnsupportedOperationException("Backing consumer can not be null");
        }
        if (this.backingConsumer != null) {
            throw new UnsupportedOperationException("Proxy consumer already initialized");
        }
        this.backingConsumer = backingConsumer;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return backingConsumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return backingConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        backingConsumer.subscribe(topics);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        backingConsumer.subscribe(topics, callback);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        backingConsumer.assign(partitions);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        backingConsumer.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(Pattern pattern) {
        backingConsumer.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        backingConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        return backingConsumer.poll(timeout);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return backingConsumer.poll(timeout);
    }

    @Override
    public void commitSync() {
        backingConsumer.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        backingConsumer.commitSync(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        backingConsumer.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        backingConsumer.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        backingConsumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        backingConsumer.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        backingConsumer.commitAsync(offsets, callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        backingConsumer.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        backingConsumer.seek(partition, offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        backingConsumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        backingConsumer.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return backingConsumer.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return backingConsumer.position(partition, timeout);
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        return backingConsumer.committed(partition);
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return backingConsumer.committed(partition, timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return backingConsumer.committed(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        return backingConsumer.committed(partitions, timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return backingConsumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return backingConsumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return backingConsumer.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return backingConsumer.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return backingConsumer.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return backingConsumer.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        backingConsumer.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        backingConsumer.resume(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return backingConsumer.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return backingConsumer.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return backingConsumer.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return backingConsumer.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return backingConsumer.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return backingConsumer.endOffsets(partitions, timeout);
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return backingConsumer.currentLag(topicPartition);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return backingConsumer.groupMetadata();
    }

    @Override
    public void enforceRebalance() {
        backingConsumer.enforceRebalance();
    }

    @Override
    public void enforceRebalance(String reason) {
        backingConsumer.enforceRebalance(reason);
    }

    @Override
    public void close() {
        backingConsumer.close();
    }

    @Override
    public void close(Duration timeout) {
        backingConsumer.close(timeout);
    }

    @Override
    public void wakeup() {
        backingConsumer.wakeup();
    }
}
