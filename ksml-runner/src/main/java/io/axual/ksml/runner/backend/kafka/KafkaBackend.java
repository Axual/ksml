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


import io.axual.ksml.KSMLConfig;
import io.axual.ksml.KSMLTopologyGenerator;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.ExecutionErrorHandler;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.rest.server.StreamsQuerier;
import io.axual.ksml.runner.backend.Backend;
import io.axual.ksml.runner.config.KSMLRunnerKSMLConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Slf4j
public class KafkaBackend implements Backend {
    private final KafkaStreams kafkaStreams;
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);

    public KafkaBackend(KSMLRunnerKSMLConfig runnerKSMLConfig, KafkaBackendConfig backendConfig) {
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
        streamsProperties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        streamsProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);

        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, runnerKSMLConfig.getWorkingDirectory());
        if (runnerKSMLConfig.getApplicationServer() != null) {
            streamsProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, runnerKSMLConfig.getApplicationServer());
        }

        // set up a stream topology generator based on the provided KSML definition
        var ksmlConfig = KSMLConfig.builder()
                .sourceType("file")
                .workingDirectory(runnerKSMLConfig.getWorkingDirectory())
                .configDirectory(runnerKSMLConfig.getConfigurationDirectory())
                .source(runnerKSMLConfig.getDefinitions())
                .notationLibrary(new NotationLibrary(propertiesToMap(streamsProperties)))
                .build();

        var topologyGenerator = new KSMLTopologyGenerator(backendConfig.getApplicationId(), ksmlConfig, streamsProperties);
        final var topology = topologyGenerator.create(new StreamsBuilder());
        kafkaStreams = new KafkaStreams(topology, streamsProperties);
        kafkaStreams.setUncaughtExceptionHandler(ExecutionContext.INSTANCE::uncaughtException);
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
