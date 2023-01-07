package io.axual.ksml.producer.factory;

/*-
 * ========================LICENSE_START=================================
 * KSML Example Producer
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.producer.config.kafka.KafkaBackendConfig;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaClientFactory implements ClientFactory {
    private final KafkaBackendConfig backendConfig;
    private final Map<String, Object> clientConfigs;
    private final NotationLibrary notationLibrary;

    public KafkaClientFactory(KafkaBackendConfig config, Map<String, Object> clientConfigs) {
        backendConfig = config;
        this.clientConfigs = new HashMap<>(clientConfigs);
        this.clientConfigs.put(BOOTSTRAP_SERVERS_CONFIG, backendConfig.getBootstrapUrl());
        this.clientConfigs.put(SCHEMA_REGISTRY_URL_CONFIG, backendConfig.getSchemaRegistryUrl());
        notationLibrary = new NotationLibrary(clientConfigs);
    }

    @Override
    public Producer<byte[], byte[]> getProducer() {
        return new KafkaProducer<>(clientConfigs);
    }

    @Override
    public Admin getAdmin() {
        return Admin.create(clientConfigs);
    }

    @Override
    public NotationLibrary getNotationLibrary() {
        return notationLibrary;
    }
}
