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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ForwardingConsumerTest {
    private static final TopicPartition PARTITION = new TopicPartition("t", 0);
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    @Mock
    private Consumer<String, String> delegate;

    private ForwardingConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        consumer = new ForwardingConsumer<>();
        consumer.initializeConsumer(delegate);
    }

    @Test
    @DisplayName("Initializing with a null delegate is rejected")
    void nullDelegateIsRejected() {
        final var fresh = new ForwardingConsumer<String, String>();
        assertThatThrownBy(() -> fresh.initializeConsumer(null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("can not be null");
    }

    @Test
    @DisplayName("Initializing twice is rejected")
    void doubleInitializationIsRejected() {
        assertThatThrownBy(() -> consumer.initializeConsumer(delegate))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("already initialized");
    }

    @Test
    @DisplayName("Assignment and subscription queries are forwarded")
    void queriesAreForwarded() {
        consumer.assignment();
        consumer.subscription();
        consumer.paused();

        verify(delegate).assignment();
        verify(delegate).subscription();
        verify(delegate).paused();
    }

    @Test
    @DisplayName("All subscribe overloads are forwarded")
    void subscribeIsForwarded() {
        final var topics = List.of("t");
        final ConsumerRebalanceListener listener = null;
        final var pattern = Pattern.compile("t.*");
        final var subscriptionPattern = new SubscriptionPattern("t.*");

        consumer.subscribe(topics);
        consumer.subscribe(topics, listener);
        consumer.subscribe(pattern);
        consumer.subscribe(pattern, listener);
        consumer.subscribe(subscriptionPattern);
        consumer.subscribe(subscriptionPattern, listener);
        consumer.unsubscribe();

        verify(delegate).subscribe(topics);
        verify(delegate).subscribe(topics, listener);
        verify(delegate).subscribe(pattern);
        verify(delegate).subscribe(pattern, listener);
        verify(delegate).subscribe(subscriptionPattern);
        verify(delegate).subscribe(subscriptionPattern, listener);
        verify(delegate).unsubscribe();
    }

    @Test
    @DisplayName("Assign and poll are forwarded")
    void assignAndPollAreForwarded() {
        final var partitions = List.of(PARTITION);

        consumer.assign(partitions);
        consumer.poll(TIMEOUT);

        verify(delegate).assign(partitions);
        verify(delegate).poll(TIMEOUT);
    }

    @Test
    @DisplayName("All commit overloads are forwarded")
    void commitIsForwarded() {
        final Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(PARTITION, new OffsetAndMetadata(1L));
        final OffsetCommitCallback callback = null;

        consumer.commitSync();
        consumer.commitSync(TIMEOUT);
        consumer.commitSync(offsets);
        consumer.commitSync(offsets, TIMEOUT);
        consumer.commitAsync();
        consumer.commitAsync(callback);
        consumer.commitAsync(offsets, callback);

        verify(delegate).commitSync();
        verify(delegate).commitSync(TIMEOUT);
        verify(delegate).commitSync(offsets);
        verify(delegate).commitSync(offsets, TIMEOUT);
        verify(delegate).commitAsync();
        verify(delegate).commitAsync(callback);
        verify(delegate).commitAsync(offsets, callback);
    }

    @Test
    @DisplayName("All seek and position overloads are forwarded")
    void seekAndPositionAreForwarded() {
        final var partitions = List.of(PARTITION);
        final var offsetAndMetadata = new OffsetAndMetadata(5L);

        consumer.seek(PARTITION, 5L);
        consumer.seek(PARTITION, offsetAndMetadata);
        consumer.seekToBeginning(partitions);
        consumer.seekToEnd(partitions);
        consumer.position(PARTITION);
        consumer.position(PARTITION, TIMEOUT);

        verify(delegate).seek(PARTITION, 5L);
        verify(delegate).seek(PARTITION, offsetAndMetadata);
        verify(delegate).seekToBeginning(partitions);
        verify(delegate).seekToEnd(partitions);
        verify(delegate).position(PARTITION);
        verify(delegate).position(PARTITION, TIMEOUT);
    }

    @Test
    @DisplayName("Committed and metric queries are forwarded")
    void committedAndMetricsAreForwarded() {
        final Set<TopicPartition> partitions = Set.of(PARTITION);

        consumer.committed(partitions);
        consumer.committed(partitions, TIMEOUT);
        consumer.clientInstanceId(TIMEOUT);
        consumer.metrics();
        consumer.registerMetricForSubscription(null);
        consumer.unregisterMetricFromSubscription(null);

        verify(delegate).committed(partitions);
        verify(delegate).committed(partitions, TIMEOUT);
        verify(delegate).clientInstanceId(TIMEOUT);
        verify(delegate).metrics();
        verify(delegate).registerMetricForSubscription(null);
        verify(delegate).unregisterMetricFromSubscription(null);
    }

    @Test
    @DisplayName("Topic and partition metadata queries are forwarded")
    void metadataQueriesAreForwarded() {
        consumer.partitionsFor("t");
        consumer.partitionsFor("t", TIMEOUT);
        consumer.listTopics();
        consumer.listTopics(TIMEOUT);

        verify(delegate).partitionsFor("t");
        verify(delegate).partitionsFor("t", TIMEOUT);
        verify(delegate).listTopics();
        verify(delegate).listTopics(TIMEOUT);
    }

    @Test
    @DisplayName("Pause, resume and offset lookups are forwarded")
    void pauseResumeAndOffsetsAreForwarded() {
        final var partitions = List.of(PARTITION);
        final Set<TopicPartition> partitionSet = Set.of(PARTITION);
        final Map<TopicPartition, Long> timestamps = Map.of(PARTITION, 1L);

        consumer.pause(partitions);
        consumer.resume(partitions);
        consumer.offsetsForTimes(timestamps);
        consumer.offsetsForTimes(timestamps, TIMEOUT);
        consumer.beginningOffsets(partitionSet);
        consumer.beginningOffsets(partitionSet, TIMEOUT);
        consumer.endOffsets(partitionSet);
        consumer.endOffsets(partitionSet, TIMEOUT);
        consumer.currentLag(PARTITION);

        verify(delegate).pause(partitions);
        verify(delegate).resume(partitions);
        verify(delegate).offsetsForTimes(timestamps);
        verify(delegate).offsetsForTimes(timestamps, TIMEOUT);
        verify(delegate).beginningOffsets(partitionSet);
        verify(delegate).beginningOffsets(partitionSet, TIMEOUT);
        verify(delegate).endOffsets(partitionSet);
        verify(delegate).endOffsets(partitionSet, TIMEOUT);
        verify(delegate).currentLag(PARTITION);
    }

    @Test
    @DisplayName("Group metadata, rebalance, close and wakeup are forwarded")
    @SuppressWarnings("removal") // close(Duration) is deprecated for removal but still part of the forwarded contract
    void lifecycleCallsAreForwarded() {
        final var closeOptions = CloseOptions.timeout(TIMEOUT);

        consumer.groupMetadata();
        consumer.enforceRebalance();
        consumer.enforceRebalance("reason");
        consumer.close();
        consumer.close(TIMEOUT);
        consumer.close(closeOptions);
        consumer.wakeup();

        verify(delegate).groupMetadata();
        verify(delegate).enforceRebalance();
        verify(delegate).enforceRebalance("reason");
        verify(delegate).close();
        verify(delegate).close(TIMEOUT);
        verify(delegate).close(closeOptions);
        verify(delegate).wakeup();
    }
}
