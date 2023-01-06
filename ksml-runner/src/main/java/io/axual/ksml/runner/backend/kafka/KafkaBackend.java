package io.axual.ksml.runner.backend.kafka;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import io.axual.ksml.KSMLTopologyGenerator;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.rest.server.StreamsQuerier;
import io.axual.ksml.runner.backend.Backend;
import io.axual.ksml.runner.config.KSMLConfig;
import lombok.extern.slf4j.Slf4j;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Slf4j
public class KafkaBackend implements Backend {
    private final KafkaStreams kafkaStreams;
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);

    public KafkaBackend(KSMLConfig ksmlConfig, KafkaBackendConfig backendConfig) {
        log.info("Constructing Kafka Backend");

        var streamsProperties = new Properties();
        if (backendConfig.getStreamsConfig() != null) {
            streamsProperties.putAll(backendConfig.getStreamsConfig());
        }

        // Explicit configs can overwrite those from the map
        streamsProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, backendConfig.getBootstrapUrl());
        streamsProperties.put(SCHEMA_REGISTRY_URL_CONFIG, backendConfig.getSchemaRegistryUrl());
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, backendConfig.getApplicationId());
        streamsProperties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, ksmlConfig.getWorkingDirectory());
        if (ksmlConfig.getApplicationServer() != null) {
            streamsProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, ksmlConfig.getApplicationServer());
        }

        // set up a stream topology generator based on the provided KSML definition
        Map<String, Object> ksmlConfigs = new HashMap<>();
        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_SOURCE_TYPE, "file");
        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_WORKING_DIRECTORY, ksmlConfig.getWorkingDirectory());
        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_CONFIG_DIRECTORY, ksmlConfig.getConfigurationDirectory());
        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_SOURCE, ksmlConfig.getDefinitions());
        ksmlConfigs.put(io.axual.ksml.KSMLConfig.NOTATION_LIBRARY, new NotationLibrary(propertiesToMap(streamsProperties)));

        var topologyGenerator = new KSMLTopologyGenerator(ksmlConfigs, streamsProperties);

        final var topology = topologyGenerator.create(new StreamsBuilder());
        kafkaStreams = new KafkaStreams(topology, streamsProperties);
    }

    private Map<String, Object> propertiesToMap(Properties props) {
        var result = new HashMap<String, Object>();
        for (var entry : props.entrySet()) {
            result.put(entry.getKey().toString(), entry.getValue());
        }
        return result;
    }

    @Override
    public State getState() {
        return convertStreamsState(kafkaStreams.state());
    }

    @Override
    public StreamsQuerier getQuerier() {
        return new StreamsQuerier() {
            @Override
            public Collection<StreamsMetadata> allMetadataForStore(String storeName) {
                return kafkaStreams.streamsMetadataForStore(storeName);
            }

            @Override
            public <K> KeyQueryMetadata queryMetadataForKey(String storeName, K key, Serializer<K> keySerializer) {
                return kafkaStreams.queryMetadataForKey(storeName, key, keySerializer);
            }

            @Override
            public <T> T store(StoreQueryParameters<T> storeQueryParameters) {
                return kafkaStreams.store(storeQueryParameters);
            }
        };
    }

    @Override
    public void close() {
        kafkaStreams.close();
    }

    @Override
    public void run() {
        log.info("Starting Kafka Backend");
        kafkaStreams.start();
        Utils.sleep(1000);
        while (!stopRunning.get()) {
            final var state = getState();
            if (state == State.STOPPED || state == State.FAILED) {
                log.info("Streams implementation has stopped, stopping Kafka Backend");
                break;
            }
            Utils.sleep(200);
        }
    }
}
