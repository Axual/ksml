package io.axual.ksml.runner.streams;

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

import io.axual.ksml.client.admin.ResolvingAdmin;
import io.axual.ksml.client.consumer.ResolvingConsumer;
import io.axual.ksml.client.producer.ResolvingProducer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

import java.util.Map;

public class KSMLClientSupplier implements KafkaClientSupplier {
    @Override
    public Admin getAdmin(Map<String, Object> configs) {
        return new ResolvingAdmin(configs);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> configs) {
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return new ResolvingProducer<>(configs);
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> configs) {
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        return new ResolvingConsumer<>(configs);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> configs) {
        return getConsumer(configs);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> configs) {
        return getConsumer(configs);
    }
}
