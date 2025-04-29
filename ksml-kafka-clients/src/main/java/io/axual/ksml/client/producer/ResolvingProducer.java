package io.axual.ksml.client.producer;

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

import io.axual.ksml.client.resolving.TopicResolver;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ResolvingProducer<K, V> extends ForwardingProducer<K, V> {
    private final ResolvingProducerConfig config;

    public ResolvingProducer(Map<String, Object> configs) {
        config = new ResolvingProducerConfig(configs);
        initializeProducer(new KafkaProducer<>(config.downstreamConfigs()));
    }

    private static RecordMetadata convertRecordMetadata(RecordMetadata input, String topic) {
        return new RecordMetadata(
                new TopicPartition(topic, input.partition()),
                input.offset(),
                0,
                input.timestamp(),
                input.serializedKeySize(),
                input.serializedValueSize());
    }

    @Deprecated
    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            newOffsets.put(config.topicResolver().resolve(entry.getKey()), entry.getValue());
        }
        super.sendOffsetsToTransaction(newOffsets, config.groupResolver().resolve(consumerGroupId));
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            newOffsets.put(config.topicResolver().resolve(entry.getKey()), entry.getValue());
        }

        final var metadata = new ConsumerGroupMetadata(
                config.groupResolver().resolve(groupMetadata.groupId()),
                groupMetadata.generationId(), groupMetadata.memberId(),
                groupMetadata.groupInstanceId());
        super.sendOffsetsToTransaction(newOffsets, metadata);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        ProducerRecord<K, V> sentRecord = convertProducerRecord(producerRecord);
        Future<RecordMetadata> future = super.send(sentRecord);
        return new ProxyFuture(future, producerRecord.topic());
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        if (callback == null) {
            return send(producerRecord);
        }

        ProducerRecord<K, V> sentRecord = convertProducerRecord(producerRecord);
        Future<RecordMetadata> future = super.send(sentRecord, new ProxyCallback(callback, producerRecord.topic()));
        return new ProxyFuture(future, producerRecord.topic());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        List<PartitionInfo> rawResult = super.partitionsFor(config.topicResolver().resolve(topic));
        List<PartitionInfo> result = new ArrayList<>(rawResult.size());
        for (PartitionInfo info : rawResult) {
            result.add(new PartitionInfo(
                    config.topicResolver().unresolve(info.topic()),
                    info.partition(),
                    info.leader(),
                    info.replicas(),
                    info.inSyncReplicas()));
        }
        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // End of public interface of KafkaProducer

    /// ////////////////////////////////////////////////////////////////////////////////////////////

    private ProducerRecord<K, V> convertProducerRecord(ProducerRecord<K, V> producerRecord) {
        final TopicResolver resolver = config.topicResolver();

        // Return a ProducerRecord with a resolved topic name
        return new ProducerRecord<>(
                resolver.resolve(producerRecord.topic()),
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),
                producerRecord.value(),
                producerRecord.headers());
    }

    private record ProxyCallback(Callback callback, String topic) implements Callback {
        @Override
        public void onCompletion(RecordMetadata input, Exception e) {
            if (input != null) {
                callback.onCompletion(convertRecordMetadata(input, topic), e);
            } else {
                callback.onCompletion(null, e);
            }
        }
    }

    private record ProxyFuture(Future<RecordMetadata> future, String topic) implements Future<RecordMetadata> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public RecordMetadata get() throws InterruptedException, ExecutionException {
            return convertRecordMetadata(future.get(), topic);
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return convertRecordMetadata(future.get(timeout, unit), topic);
        }
    }
}
