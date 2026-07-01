package io.axual.ksml.runner.streams;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.client.admin.ResolvingAdmin;
import io.axual.ksml.client.consumer.ResolvingConsumer;
import io.axual.ksml.client.producer.ResolvingProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KSMLClientSupplier}. Kafka clients connect lazily, so these construct real
 * (unconnected) clients against a dummy broker address and close them immediately. The behaviour under
 * test is the (de)serializer injection and that each factory returns the resolving client variant.
 */
class KSMLClientSupplierTest {

    private static final KSMLClientSupplier SUPPLIER = new KSMLClientSupplier();

    private static Map<String, Object> baseConfig() {
        final var config = new HashMap<String, Object>();
        config.put("bootstrap.servers", "localhost:9092");
        return config;
    }

    @Test
    @DisplayName("getAdmin returns a resolving admin client")
    void getAdminReturnsResolvingAdmin() {
        final var config = baseConfig();
        try (var admin = SUPPLIER.getAdmin(config)) {
            assertThat(admin).isInstanceOf(ResolvingAdmin.class);
        }
    }

    @Test
    @DisplayName("getProducer injects the byte-array serializers and returns a resolving producer")
    void getProducerInjectsSerializers() {
        final var config = baseConfig();
        try (var producer = SUPPLIER.getProducer(config)) {
            assertThat(producer).isInstanceOf(ResolvingProducer.class);
            assertThat(config)
                    .containsEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName())
                    .containsEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        }
    }

    @Test
    @DisplayName("getConsumer injects the byte-array deserializers and returns a resolving consumer")
    void getConsumerInjectsDeserializers() {
        final var config = baseConfig();
        try (var consumer = SUPPLIER.getConsumer(config)) {
            assertThat(consumer).isInstanceOf(ResolvingConsumer.class);
            assertThat(config)
                    .containsEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName())
                    .containsEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        }
    }

    @Test
    @DisplayName("getRestoreConsumer and getGlobalConsumer delegate to getConsumer")
    void restoreAndGlobalConsumersDelegateToGetConsumer() {
        // Both delegate to getConsumer, so each must return a resolving consumer and have both
        // byte-array deserializers injected into its config (the side effect getConsumer performs).
        final var restoreConfig = baseConfig();
        try (var restore = SUPPLIER.getRestoreConsumer(restoreConfig)) {
            assertThat(restore).isInstanceOf(ResolvingConsumer.class);
            assertThat(restoreConfig)
                    .containsEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName())
                    .containsEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        }

        final var globalConfig = baseConfig();
        try (var global = SUPPLIER.getGlobalConsumer(globalConfig)) {
            assertThat(global).isInstanceOf(ResolvingConsumer.class);
            assertThat(globalConfig)
                    .containsEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName())
                    .containsEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        }
    }
}
