package io.axual.ksml.client.producer;

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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link ResolvingProducer} resolves topics and group ids on the way down to the
 * delegate {@link KafkaProducer} and unresolves them again on the way back. The delegate producer
 * is replaced with a Mockito construction mock so no real Kafka connection is required.
 */
class ResolvingProducerTest {
    private static final String UNRESOLVED_TOPIC = "orders";
    private static final String RESOLVED_TOPIC = "tenant-orders";
    private static final Map<String, Object> CONFIGS = Map.of(
            "axual.topic.pattern", "{tenant}-{topic}",
            "axual.group.id.pattern", "{tenant}-{group.id}",
            "tenant", "tenant");

    @Test
    @DisplayName("send resolves the record topic and unresolves the returned metadata")
    void sendResolvesTopicAndUnresolvesMetadata() throws Exception {
        try (var mocked = mockConstruction(KafkaProducer.class)) {
            final var producer = new ResolvingProducer<String, String>(CONFIGS);
            final var delegate = delegateOf(mocked);
            when(delegate.send(any())).thenReturn(resolvedMetadataFuture());

            final var metadata = producer.send(new ProducerRecord<>(UNRESOLVED_TOPIC, "k", "v")).get();

            final ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.captor();
            verify(delegate).send(sent.capture());
            assertThat(sent.getValue().topic()).isEqualTo(RESOLVED_TOPIC);
            assertThat(metadata.topic()).isEqualTo(UNRESOLVED_TOPIC);
        }
    }

    @Test
    @DisplayName("send with a null callback delegates to the single-argument send")
    void sendWithNullCallbackDelegates() throws Exception {
        try (var mocked = mockConstruction(KafkaProducer.class)) {
            final var producer = new ResolvingProducer<String, String>(CONFIGS);
            final var delegate = delegateOf(mocked);
            when(delegate.send(any())).thenReturn(resolvedMetadataFuture());

            producer.send(new ProducerRecord<>(UNRESOLVED_TOPIC, "k", "v"), null).get();

            verify(delegate).send(any());
        }
    }

    @Test
    @DisplayName("send with a callback unresolves the metadata passed to the callback")
    void sendWithCallbackUnresolvesMetadata() {
        try (var mocked = mockConstruction(KafkaProducer.class)) {
            final var producer = new ResolvingProducer<String, String>(CONFIGS);
            final var delegate = delegateOf(mocked);
            when(delegate.send(any(), any())).thenReturn(resolvedMetadataFuture());
            final var received = new RecordMetadata[1];

            producer.send(new ProducerRecord<>(UNRESOLVED_TOPIC, "k", "v"), (metadata, exception) -> received[0] = metadata);

            final ArgumentCaptor<Callback> proxyCallback = ArgumentCaptor.captor();
            verify(delegate).send(any(), proxyCallback.capture());
            proxyCallback.getValue().onCompletion(resolvedMetadata(), null);

            assertThat(received[0].topic()).isEqualTo(UNRESOLVED_TOPIC);
        }
    }

    @Test
    @DisplayName("A callback receiving no metadata is forwarded unchanged")
    void sendWithCallbackForwardsNullMetadata() {
        try (var mocked = mockConstruction(KafkaProducer.class)) {
            final var producer = new ResolvingProducer<String, String>(CONFIGS);
            final var delegate = delegateOf(mocked);
            when(delegate.send(any(), any())).thenReturn(resolvedMetadataFuture());
            final var received = new RecordMetadata[]{resolvedMetadata()};
            final var failure = new RuntimeException("boom");
            final var receivedException = new Exception[1];

            producer.send(new ProducerRecord<>(UNRESOLVED_TOPIC, "k", "v"), (metadata, exception) -> {
                received[0] = metadata;
                receivedException[0] = exception;
            });

            final ArgumentCaptor<Callback> proxyCallback = ArgumentCaptor.captor();
            verify(delegate).send(any(), proxyCallback.capture());
            proxyCallback.getValue().onCompletion(null, failure);

            assertThat(received[0]).isNull();
            assertThat(receivedException[0]).isSameAs(failure);
        }
    }

    @Test
    @DisplayName("sendOffsetsToTransaction resolves partition topics and the group id")
    void sendOffsetsToTransactionResolves() {
        try (var mocked = mockConstruction(KafkaProducer.class)) {
            final var producer = new ResolvingProducer<String, String>(CONFIGS);
            final var delegate = delegateOf(mocked);

            producer.sendOffsetsToTransaction(
                    Map.of(new TopicPartition(UNRESOLVED_TOPIC, 0), new OffsetAndMetadata(10L)),
                    groupMetadata());

            final ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsets = ArgumentCaptor.captor();
            final ArgumentCaptor<ConsumerGroupMetadata> groupMetadata = ArgumentCaptor.captor();
            verify(delegate).sendOffsetsToTransaction(offsets.capture(), groupMetadata.capture());
            assertThat(offsets.getValue()).containsKey(new TopicPartition(RESOLVED_TOPIC, 0));
            assertThat(groupMetadata.getValue().groupId()).isEqualTo("tenant-group");
        }
    }

    @Test
    @DisplayName("partitionsFor resolves the requested topic and unresolves the results")
    void partitionsForResolvesAndUnresolves() {
        try (var mocked = mockConstruction(KafkaProducer.class)) {
            final var producer = new ResolvingProducer<String, String>(CONFIGS);
            final var delegate = delegateOf(mocked);
            when(delegate.partitionsFor(RESOLVED_TOPIC)).thenReturn(
                    List.of(new PartitionInfo(RESOLVED_TOPIC, 0, Node.noNode(), new Node[0], new Node[0])));

            final var partitions = producer.partitionsFor(UNRESOLVED_TOPIC);

            assertThat(partitions).singleElement()
                    .extracting(PartitionInfo::topic)
                    .isEqualTo(UNRESOLVED_TOPIC);
        }
    }

    @Test
    @DisplayName("The returned future exposes cancellation and completion state of the delegate future")
    void proxyFutureDelegatesState() throws Exception {
        try (var mocked = mockConstruction(KafkaProducer.class)) {
            final var producer = new ResolvingProducer<String, String>(CONFIGS);
            final var delegate = delegateOf(mocked);
            when(delegate.send(any())).thenReturn(resolvedMetadataFuture());

            final var future = producer.send(new ProducerRecord<>(UNRESOLVED_TOPIC, "k", "v"));

            assertThat(future.isCancelled()).isFalse();
            assertThat(future.isDone()).isTrue();
            assertThat(future.cancel(true)).isFalse();
            assertThat(future.get(1, TimeUnit.SECONDS).topic()).isEqualTo(UNRESOLVED_TOPIC);
        }
    }

    @SuppressWarnings("removal") // The only public ConsumerGroupMetadata constructors are deprecated for removal in Kafka 4.2 with no replacement yet
    private static ConsumerGroupMetadata groupMetadata() {
        return new ConsumerGroupMetadata("group");
    }

    @SuppressWarnings("unchecked")
    private static KafkaProducer<String, String> delegateOf(MockedConstruction<KafkaProducer> mocked) {
        return (KafkaProducer<String, String>) mocked.constructed().get(0);
    }

    private static CompletableFuture<RecordMetadata> resolvedMetadataFuture() {
        return CompletableFuture.completedFuture(resolvedMetadata());
    }

    private static RecordMetadata resolvedMetadata() {
        return new RecordMetadata(new TopicPartition(RESOLVED_TOPIC, 0), 0L, 0, 123L, 4, 5);
    }
}
