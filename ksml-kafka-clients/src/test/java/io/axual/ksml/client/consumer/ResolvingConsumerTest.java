package io.axual.ksml.client.consumer;

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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link ResolvingConsumer} resolves topics and group ids towards the delegate
 * {@link KafkaConsumer} and unresolves them again for results handed back to the caller. The
 * delegate consumer is a Mockito construction mock so no real Kafka connection is required.
 */
class ResolvingConsumerTest {
    private static final String UNRESOLVED_TOPIC = "orders";
    private static final String RESOLVED_TOPIC = "tenant-orders";
    private static final TopicPartition UNRESOLVED_PARTITION = new TopicPartition(UNRESOLVED_TOPIC, 0);
    private static final TopicPartition RESOLVED_PARTITION = new TopicPartition(RESOLVED_TOPIC, 0);
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    private static final Map<String, Object> CONFIGS = Map.of(
            "axual.topic.pattern", "{tenant}-{topic}",
            "axual.group.id.pattern", "{tenant}-{group.id}",
            "tenant", "tenant");

    @Test
    @DisplayName("subscribe resolves the requested topics")
    void subscribeResolvesTopics() {
        withConsumer((consumer, delegate) -> {
            consumer.subscribe(List.of(UNRESOLVED_TOPIC));

            final ArgumentCaptor<Collection<String>> topics = ArgumentCaptor.captor();
            verify(delegate).subscribe(topics.capture());
            assertThat(topics.getValue()).containsExactly(RESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("subscribe with a listener resolves topics and unresolves rebalance callbacks")
    void subscribeWithListenerWrapsCallbacks() {
        withConsumer((consumer, delegate) -> {
            final var assigned = new ArrayList<TopicPartition>();
            final var revoked = new ArrayList<TopicPartition>();
            final ConsumerRebalanceListener callerListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    revoked.addAll(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assigned.addAll(partitions);
                }
            };

            consumer.subscribe(List.of(UNRESOLVED_TOPIC), callerListener);

            final ArgumentCaptor<ConsumerRebalanceListener> wrapped = ArgumentCaptor.captor();
            verify(delegate).subscribe(any(Collection.class), wrapped.capture());
            wrapped.getValue().onPartitionsAssigned(List.of(RESOLVED_PARTITION));
            wrapped.getValue().onPartitionsRevoked(List.of(RESOLVED_PARTITION));

            assertThat(assigned).containsExactly(UNRESOLVED_PARTITION);
            assertThat(revoked).containsExactly(UNRESOLVED_PARTITION);
        });
    }

    @Test
    @DisplayName("Subscribing to bare patterns without a listener is not supported")
    void patternSubscriptionsAreUnsupported() {
        withConsumer((consumer, delegate) -> {
            final var pattern = Pattern.compile("orders.*");
            final var subscriptionPattern = new SubscriptionPattern("orders.*");
            assertThatThrownBy(() -> consumer.subscribe(pattern)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> consumer.subscribe(subscriptionPattern)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> consumer.subscribe(subscriptionPattern, null)).isInstanceOf(UnsupportedOperationException.class);
        });
    }

    @Test
    @DisplayName("subscribe(Pattern, listener) resolves the pattern and wraps the listener")
    void patternWithListenerSubscriptionResolves() {
        withConsumer((consumer, delegate) -> {
            final var pattern = Pattern.compile("orders.*");
            final var assigned = new ArrayList<TopicPartition>();
            final ConsumerRebalanceListener callerListener = new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    throw new UnsupportedOperationException("onPartitionsRevoked not observed in this test");
                }
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assigned.addAll(partitions);
                }
            };

            consumer.subscribe(pattern, callerListener);

            final ArgumentCaptor<Pattern> patternCaptor = ArgumentCaptor.captor();
            final ArgumentCaptor<ConsumerRebalanceListener> listenerCaptor = ArgumentCaptor.captor();
            verify(delegate).subscribe(patternCaptor.capture(), listenerCaptor.capture());
            assertThat(patternCaptor.getValue().pattern()).isEqualTo("tenant-(orders.*)");
            listenerCaptor.getValue().onPartitionsAssigned(List.of(RESOLVED_PARTITION));
            assertThat(assigned).containsExactly(UNRESOLVED_PARTITION);
        });
    }

    @Test
    @DisplayName("assign, seek, pause and resume resolve their partitions")
    void partitionOperationsResolve() {
        withConsumer((consumer, delegate) -> {
            consumer.assign(List.of(UNRESOLVED_PARTITION));
            consumer.seek(UNRESOLVED_PARTITION, 5L);
            consumer.seek(UNRESOLVED_PARTITION, new OffsetAndMetadata(6L));
            consumer.seekToBeginning(List.of(UNRESOLVED_PARTITION));
            consumer.seekToEnd(List.of(UNRESOLVED_PARTITION));
            consumer.pause(List.of(UNRESOLVED_PARTITION));
            consumer.resume(List.of(UNRESOLVED_PARTITION));

            verify(delegate).assign(Set.of(RESOLVED_PARTITION));
            verify(delegate).seek(RESOLVED_PARTITION, 5L);
            verify(delegate).seek(RESOLVED_PARTITION, new OffsetAndMetadata(6L));
            verify(delegate).seekToBeginning(Set.of(RESOLVED_PARTITION));
            verify(delegate).seekToEnd(Set.of(RESOLVED_PARTITION));
            verify(delegate).pause(Set.of(RESOLVED_PARTITION));
            verify(delegate).resume(Set.of(RESOLVED_PARTITION));
        });
    }

    @Test
    @DisplayName("commitSync resolves the offset partitions")
    void commitSyncResolves() {
        withConsumer((consumer, delegate) -> {
            final var offsets = Map.of(UNRESOLVED_PARTITION, new OffsetAndMetadata(10L));

            consumer.commitSync(offsets);
            consumer.commitSync(offsets, TIMEOUT);

            verify(delegate).commitSync(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(10L)));
            verify(delegate).commitSync(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(10L)), TIMEOUT);
        });
    }

    @Test
    @DisplayName("commitAsync wraps a callback and unresolves the offsets passed to it")
    void commitAsyncUnresolvesCallbackOffsets() {
        withConsumer((consumer, delegate) -> {
            final var received = new HashMap<TopicPartition, OffsetAndMetadata>();
            final OffsetCommitCallback callerCallback = (offsets, exception) -> received.putAll(offsets);

            consumer.commitAsync(Map.of(UNRESOLVED_PARTITION, new OffsetAndMetadata(10L)), callerCallback);

            final ArgumentCaptor<OffsetCommitCallback> wrapped = ArgumentCaptor.captor();
            verify(delegate).commitAsync(any(), wrapped.capture());
            wrapped.getValue().onComplete(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(10L)), null);

            assertThat(received).containsKey(UNRESOLVED_PARTITION);
        });
    }

    @Test
    @DisplayName("commitAsync without a callback forwards a null callback")
    void commitAsyncWithoutCallback() {
        withConsumer((consumer, delegate) -> {
            consumer.commitAsync(Map.of(UNRESOLVED_PARTITION, new OffsetAndMetadata(10L)), null);
            verify(delegate).commitAsync(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(10L)), null);
        });
    }

    @Test
    @DisplayName("position and currentLag resolve the requested partition")
    void positionAndLagResolve() {
        withConsumer((consumer, delegate) -> {
            when(delegate.position(RESOLVED_PARTITION)).thenReturn(42L);

            assertThat(consumer.position(UNRESOLVED_PARTITION)).isEqualTo(42L);
            consumer.position(UNRESOLVED_PARTITION, TIMEOUT);
            consumer.currentLag(UNRESOLVED_PARTITION);

            verify(delegate).position(RESOLVED_PARTITION, TIMEOUT);
            verify(delegate).currentLag(RESOLVED_PARTITION);
        });
    }

    @Test
    @DisplayName("committed unresolves the returned partitions")
    void committedUnresolves() {
        withConsumer((consumer, delegate) -> {
            when(delegate.committed(Set.of(RESOLVED_PARTITION)))
                    .thenReturn(Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(7L)));

            final var committed = consumer.committed(Set.of(UNRESOLVED_PARTITION));

            assertThat(committed).containsEntry(UNRESOLVED_PARTITION, new OffsetAndMetadata(7L));
        });
    }

    @Test
    @DisplayName("assignment, subscription and paused unresolve the delegate results")
    void queriesUnresolve() {
        withConsumer((consumer, delegate) -> {
            when(delegate.assignment()).thenReturn(Set.of(RESOLVED_PARTITION));
            when(delegate.paused()).thenReturn(Set.of(RESOLVED_PARTITION));
            when(delegate.subscription()).thenReturn(Set.of(RESOLVED_TOPIC));

            assertThat(consumer.assignment()).containsExactly(UNRESOLVED_PARTITION);
            assertThat(consumer.paused()).containsExactly(UNRESOLVED_PARTITION);
            assertThat(consumer.subscription()).containsExactly(UNRESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("groupMetadata unresolves the group id")
    void groupMetadataUnresolves() {
        withConsumer((consumer, delegate) -> {
            when(delegate.groupMetadata()).thenReturn(groupMetadata("tenant-group"));

            assertThat(consumer.groupMetadata().groupId()).isEqualTo("group");
        });
    }

    @Test
    @DisplayName("A null group metadata is passed through as null")
    void groupMetadataNull() {
        withConsumer((consumer, delegate) -> {
            when(delegate.groupMetadata()).thenReturn(null);
            assertThat(consumer.groupMetadata()).isNull();
        });
    }

    @Test
    @DisplayName("beginningOffsets, endOffsets and offsetsForTimes resolve and unresolve partitions")
    void offsetLookupsResolveAndUnresolve() {
        withConsumer((consumer, delegate) -> {
            when(delegate.beginningOffsets(Set.of(RESOLVED_PARTITION))).thenReturn(Map.of(RESOLVED_PARTITION, 1L));
            when(delegate.endOffsets(Set.of(RESOLVED_PARTITION))).thenReturn(Map.of(RESOLVED_PARTITION, 9L));

            assertThat(consumer.beginningOffsets(Set.of(UNRESOLVED_PARTITION))).containsEntry(UNRESOLVED_PARTITION, 1L);
            assertThat(consumer.endOffsets(Set.of(UNRESOLVED_PARTITION))).containsEntry(UNRESOLVED_PARTITION, 9L);

            consumer.beginningOffsets(Set.of(UNRESOLVED_PARTITION), TIMEOUT);
            consumer.endOffsets(Set.of(UNRESOLVED_PARTITION), TIMEOUT);
            consumer.offsetsForTimes(Map.of(UNRESOLVED_PARTITION, 100L));
            verify(delegate).offsetsForTimes(Map.of(RESOLVED_PARTITION, 100L));
        });
    }

    @Test
    @DisplayName("partitionsFor resolves the topic and returns unresolved partition info")
    void partitionsForResolvesAndUnresolves() {
        withConsumer((consumer, delegate) -> {
            when(delegate.partitionsFor(RESOLVED_TOPIC)).thenReturn(
                    List.of(new PartitionInfo(RESOLVED_TOPIC, 0, null, new Node[0], new Node[0])));

            assertThat(consumer.partitionsFor(UNRESOLVED_TOPIC))
                    .singleElement()
                    .isInstanceOf(ResolvingPartitionInfo.class)
                    .extracting(PartitionInfo::topic)
                    .isEqualTo(UNRESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("listTopics unresolves the returned topic names")
    void listTopicsUnresolves() {
        withConsumer((consumer, delegate) -> {
            when(delegate.listTopics()).thenReturn(Map.of(RESOLVED_TOPIC,
                    List.of(new PartitionInfo(RESOLVED_TOPIC, 0, null, new Node[0], new Node[0]))));

            assertThat(consumer.listTopics()).containsOnlyKeys(UNRESOLVED_TOPIC);
        });
    }

    @Test
    @DisplayName("poll unresolves the topics of every returned record")
    void pollUnresolvesRecords() {
        withConsumer((consumer, delegate) -> {
            final var records = new ConsumerRecords<>(
                    Map.of(RESOLVED_PARTITION, List.<ConsumerRecord<String, String>>of(
                            new ConsumerRecord<>(RESOLVED_TOPIC, 0, 0L, "k", "v"))),
                    Map.of(RESOLVED_PARTITION, new OffsetAndMetadata(1L)));
            when(delegate.poll(TIMEOUT)).thenReturn(records);

            final var polled = consumer.poll(TIMEOUT);

            assertThat(polled.partitions()).containsExactly(UNRESOLVED_PARTITION);
            assertThat(polled.records(UNRESOLVED_PARTITION))
                    .singleElement()
                    .extracting(ConsumerRecord::topic)
                    .isEqualTo(UNRESOLVED_TOPIC);
        });
    }

    private void withConsumer(ConsumerTest test) {
        try (var mocked = mockConstruction(KafkaConsumer.class)) {
            final var consumer = new ResolvingConsumer<String, String>(CONFIGS);
            test.accept(consumer, delegateOf(mocked));
        }
    }

    @FunctionalInterface
    private interface ConsumerTest {
        void accept(ResolvingConsumer<String, String> consumer, KafkaConsumer<String, String> delegate);
    }

    @SuppressWarnings("removal") // The only public ConsumerGroupMetadata constructors are deprecated for removal in Kafka 4.2 with no replacement yet
    private static ConsumerGroupMetadata groupMetadata(String groupId) {
        return new ConsumerGroupMetadata(groupId, -1, "", Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static KafkaConsumer<String, String> delegateOf(MockedConstruction<KafkaConsumer> mocked) {
        return (KafkaConsumer<String, String>) mocked.constructed().get(0);
    }
}
