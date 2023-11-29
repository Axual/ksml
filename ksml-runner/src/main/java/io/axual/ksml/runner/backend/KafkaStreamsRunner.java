package io.axual.ksml.runner.backend;

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


import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.producer.ResolvingProducerConfig;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.ExecutionErrorHandler;
import io.axual.ksml.generator.TopologySpecification;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.rest.server.StreamsQuerier;
import io.axual.ksml.runner.config.ApplicationServerConfig;
import io.axual.ksml.runner.streams.KSMLClientSupplier;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.TopologyConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaStreamsRunner implements Runner {
    private final KafkaStreams kafkaStreams;
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);

    @Builder
    public record Config(Map<String, TopologySpecification> definitions,
                         String storageDirectory,
                         ApplicationServerConfig appServer,
                         Map<String, String> kafkaConfig,
                         NotationLibrary notationLibrary) {
    }

    public KafkaStreamsRunner(Config config) {
        log.info("Constructing Kafka Backend");

        final var streamsProps = getStreamsConfig(config.kafkaConfig, config.storageDirectory, config.appServer);

        var applicationId = config.kafkaConfig != null ? config.kafkaConfig.get(StreamsConfig.APPLICATION_ID_CONFIG) : null;
        if (applicationId == null) {
            applicationId = "ksmlApplicationId";
        }

        final var streamsConfig = new StreamsConfig(streamsProps);
        final var topologyConfig = new TopologyConfig(streamsConfig);
        final var streamsBuilder = new StreamsBuilder(topologyConfig);
        final var topologyGenerator = new TopologyGenerator(applicationId, config.notationLibrary);
        final var topology = topologyGenerator.create(streamsBuilder, config.definitions);
        kafkaStreams = new KafkaStreams(topology, mapToProperties(streamsProps));
        kafkaStreams.setUncaughtExceptionHandler(ExecutionContext.INSTANCE::uncaughtException);
    }

    private Map<String, Object> getStreamsConfig(Map<String, String> initialConfigs, String storageDirectory, ApplicationServerConfig appServer) {
        final Map<String, Object> result = initialConfigs != null ? new HashMap<>(initialConfigs) : new HashMap<>();
        // Explicit configs can overwrite those from the map
        result.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        result.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        result.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        if (result.containsKey(ResolvingClientConfig.GROUP_ID_PATTERN_CONFIG)
                || result.containsKey(ResolvingClientConfig.TOPIC_PATTERN_CONFIG)
                || result.containsKey(ResolvingProducerConfig.TRANSACTIONAL_ID_PATTERN_CONFIG)) {
            log.info("Using resolving clients for Kafka");
            result.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, KSMLClientSupplier.class.getCanonicalName());
        }

        result.put(StreamsConfig.STATE_DIR_CONFIG, storageDirectory);
        if (appServer != null && appServer.isEnabled()) {
            result.put(StreamsConfig.APPLICATION_SERVER_CONFIG, appServer.getApplicationServer());
        }

        return result;
    }

    private Properties mapToProperties(Map<String, Object> configs) {
        final var result = new Properties();
        result.putAll(configs);
        return result;
    }

    @Override
    public State getState() {
        if (kafkaStreams == null) return State.STOPPED;
        return switch (kafkaStreams.state()) {
            case CREATED, REBALANCING -> State.STARTING;
            case RUNNING -> State.STARTED;
            case PENDING_SHUTDOWN -> State.STOPPING;
            case NOT_RUNNING -> State.STOPPED;
            case PENDING_ERROR, ERROR -> State.FAILED;
        };
    }

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
    public void run() {
        log.info("Starting Kafka Streams backend");
        try {
            kafkaStreams.start();
            // Allow Kafka Streams to start up asynchronously
            Utils.sleep(1000);

            // While we need to keep running, wait and check for failure state
            while (isRunning() && !stopRunning.get()) {
                Utils.sleep(200);
            }
        } finally {
            if (isRunning()) {
                log.info("Stopping Kafka Streams");
                kafkaStreams.close();
            } else {
                log.info("Kafka Streams has stopped");
            }
        }
    }

    @Override
    public void stop() {
        stopRunning.set(true);
    }
}
