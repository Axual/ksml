package io.axual.ksml.runner.backend;

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


import io.axual.ksml.KSMLTopologyGenerator;
import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.producer.ResolvingProducerConfig;
import io.axual.ksml.client.serde.ResolvingDeserializer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.ExecutionErrorHandler;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.rest.server.StreamsQuerier;
import io.axual.ksml.runner.config.KSMLConfig;
import io.axual.ksml.runner.streams.KSMLClientSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
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
public class KafkaBackend implements Backend {
    private final KafkaStreams kafkaStreams;
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);

    public KafkaBackend(KSMLConfig ksmlConfig, Map<String, String> kafkaConfig) {
        log.info("Constructing Kafka Backend");

        HashMap<String, Object> streamsConfig = kafkaConfig != null ? new HashMap<>(kafkaConfig) : new HashMap<>();
        // Explicit configs can overwrite those from the map
        streamsConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        streamsConfig.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        streamsConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        if (streamsConfig.containsKey(ResolvingClientConfig.GROUP_ID_PATTERN_CONFIG)
                || streamsConfig.containsKey(ResolvingClientConfig.TOPIC_PATTERN_CONFIG)
                || streamsConfig.containsKey(ResolvingProducerConfig.TRANSACTIONAL_ID_PATTERN_CONFIG)) {
            log.info("Using resolving clients for Kafka");
            streamsConfig.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, KSMLClientSupplier.class.getCanonicalName());
        }

        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, ksmlConfig.getStorageDirectory());
        if (ksmlConfig.getApplicationServer() != null && ksmlConfig.getApplicationServer().isEnabled()) {
            streamsConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, ksmlConfig.getApplicationServer().getApplicationServer());
        }

        // set up a stream topology generator based on the provided KSML definition
        var ksmlConf = io.axual.ksml.KSMLConfig.builder()
                .sourceType("file")
                .configDirectory(ksmlConfig.getConfigDirectory())
                .schemaDirectory(ksmlConfig.getSchemaDirectory())
                .source(ksmlConfig.getDefinitions())
                .notationLibrary(new NotationLibrary(streamsConfig))
                .build();

        var applicationId = kafkaConfig != null ? kafkaConfig.get(StreamsConfig.APPLICATION_ID_CONFIG) : null;
        if (applicationId == null) {
            applicationId = "ksmlApplicationId";
        }

        ExecutionContext.INSTANCE.setSerdeWrapper(
                serde -> new Serdes.WrapperSerde<>(
                        new ResolvingSerializer<>(serde.serializer(), streamsConfig),
                        new ResolvingDeserializer<>(serde.deserializer(), streamsConfig)));
        var topologyGenerator = new KSMLTopologyGenerator(applicationId, ksmlConf, streamsConfig);

        var topologyConfig = new TopologyConfig(applicationId, new StreamsConfig(streamsConfig), new Properties());
        final var topology = topologyGenerator.create(new StreamsBuilder(topologyConfig));
        kafkaStreams = new KafkaStreams(topology, mapToProperties(streamsConfig));
        kafkaStreams.setUncaughtExceptionHandler(ExecutionContext.INSTANCE::uncaughtException);
    }

    private Properties mapToProperties(Map<String, Object> configs) {
        var result = new Properties();
        configs.entrySet().stream().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
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
