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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ForwardingProducerTest {
    @Mock
    private Producer<String, String> delegate;

    private ForwardingProducer<String, String> producer;

    @BeforeEach
    void setUp() {
        producer = new ForwardingProducer<>();
        producer.initializeProducer(delegate);
    }

    @Test
    @DisplayName("Initializing with a null delegate is rejected")
    void nullDelegateIsRejected() {
        final var fresh = new ForwardingProducer<String, String>();
        assertThatThrownBy(() -> fresh.initializeProducer(null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("can not be null");
    }

    @Test
    @DisplayName("Initializing twice is rejected")
    void doubleInitializationIsRejected() {
        assertThatThrownBy(() -> producer.initializeProducer(delegate))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("already initialized");
    }

    @Test
    @DisplayName("Transaction lifecycle calls are forwarded to the delegate")
    void transactionCallsAreForwarded() {
        producer.initTransactions();
        producer.beginTransaction();
        producer.commitTransaction();
        producer.abortTransaction();

        verify(delegate).initTransactions();
        verify(delegate).beginTransaction();
        verify(delegate).commitTransaction();
        verify(delegate).abortTransaction();
    }

    @Test
    @DisplayName("sendOffsetsToTransaction is forwarded to the delegate")
    void sendOffsetsToTransactionIsForwarded() {
        final Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(new TopicPartition("t", 0), new OffsetAndMetadata(1L));
        final var groupMetadata = groupMetadata();

        producer.sendOffsetsToTransaction(offsets, groupMetadata);

        verify(delegate).sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @SuppressWarnings("removal") // The only public ConsumerGroupMetadata constructors are deprecated for removal in Kafka 4.2 with no replacement yet
    private static ConsumerGroupMetadata groupMetadata() {
        return new ConsumerGroupMetadata("group");
    }

    @Test
    @DisplayName("Metric subscription calls are forwarded to the delegate")
    void metricSubscriptionCallsAreForwarded() {
        final var metric = (KafkaMetric) null;

        producer.registerMetricForSubscription(metric);
        producer.unregisterMetricFromSubscription(metric);

        verify(delegate).registerMetricForSubscription(metric);
        verify(delegate).unregisterMetricFromSubscription(metric);
    }

    @Test
    @DisplayName("send is forwarded to the delegate, with and without a callback")
    void sendIsForwarded() {
        final var rec = new ProducerRecord<>("t", "k", "v");
        final Callback callback = (metadata, exception) -> {
        };

        producer.send(rec);
        producer.send(rec, callback);

        verify(delegate).send(rec);
        verify(delegate).send(rec, callback);
    }

    @Test
    @DisplayName("Bookkeeping calls are forwarded to the delegate")
    void bookkeepingCallsAreForwarded() {
        producer.flush();
        producer.partitionsFor("t");
        producer.metrics();
        producer.clientInstanceId(Duration.ofSeconds(1));

        verify(delegate).flush();
        verify(delegate).partitionsFor("t");
        verify(delegate).metrics();
        verify(delegate).clientInstanceId(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("close calls are forwarded to the delegate")
    void closeCallsAreForwarded() {
        producer.close();
        producer.close(Duration.ofSeconds(1));

        verify(delegate).close();
        verify(delegate).close(Duration.ofSeconds(1));
    }
}
