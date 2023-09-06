package io.axual.ksml.runner.backend.axual;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner for Axual
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


import io.axual.client.proxy.axual.producer.AxualProducerConfig;
import io.axual.client.proxy.generic.registry.ProxyChain;
import io.axual.common.config.ClientConfig;
import io.axual.common.config.SslConfig;
import io.axual.common.tools.KafkaUtil;
import io.axual.discovery.client.DiscoveryClientRegistry;
import io.axual.discovery.client.DiscoveryConfig;
import io.axual.discovery.client.DiscoveryResult;
import io.axual.discovery.client.exception.DiscoveryClientRegistrationException;
import io.axual.discovery.client.tools.DiscoveryConfigParserV2;
import io.axual.ksml.AxualNotationLibrary;
import io.axual.ksml.KSMLConfig;
import io.axual.ksml.KSMLTopologyGenerator;
import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.ExecutionErrorHandler;
import io.axual.ksml.rest.server.StreamsQuerier;
import io.axual.ksml.runner.backend.Backend;
import io.axual.ksml.runner.config.KSMLErrorHandlingConfig;
import io.axual.ksml.runner.config.KSMLRunnerKSMLConfig;
import io.axual.ksml.serde.UnknownTypeSerde;
import io.axual.streams.proxy.axual.AxualStreams;
import io.axual.streams.proxy.axual.AxualStreamsConfig;
import io.axual.streams.proxy.generic.factory.TopologyFactory;
import io.axual.streams.proxy.generic.factory.UncaughtExceptionHandlerFactory;
import io.axual.streams.proxy.wrapped.WrappedStreamsConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.*;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.axual.client.proxy.generic.registry.ProxyTypeRegistry.*;
import static io.axual.client.proxy.switching.generic.DistributorConfigs.DISTRIBUTOR_DISTANCE_CONFIG;
import static io.axual.client.proxy.switching.generic.DistributorConfigs.DISTRIBUTOR_TIMEOUT_CONFIG;
import static io.axual.common.tools.MapUtil.stringValue;

@Slf4j
public class AxualBackend implements Backend {
    private static final String DEFAULT_DISTRIBUTOR_DISTANCE = "1"; //
    private static final String DEFAULT_DISTRIBUTOR_TIMEOUT = "60000"; // 60s in ms

    private final AtomicBoolean stopRunning = new AtomicBoolean(false);
    private final AtomicBoolean clusterSwitchDetected = new AtomicBoolean(false);

    private final DiscoveryConfig discoveryConfig;
    private final ClientConfig clientConfig;
    private final KSMLRunnerKSMLConfig ksmlConfig;

    private AxualStreams axualStreams;
    private DiscoveryResult discoveryResult;
    private TopologyGenerator topologyGenerator;
    private Map<String, Object> axualConfigs;

    public AxualBackend(KSMLRunnerKSMLConfig ksmlConfig, AxualBackendConfig config) {
        log.info("Constructing Axual Backend");
        this.ksmlConfig = ksmlConfig;

        clientConfig = ClientConfig.newBuilder()
                .setTenant(config.getTenant())
                .setEnvironment(config.getEnvironment())
                .setEndpoint(config.getEndpoint())
                .setApplicationId(config.getApplicationId())
                .setApplicationVersion(config.getApplicationVersion())
                .setSslConfig(SslConfig.newBuilder()
                        .setEnableHostnameVerification(config.getSslConfig().isEnableHostnameVerification())
                        .setKeystoreLocation(config.getSslConfig().getKeystoreLocation())
                        .setKeystorePassword(config.getSslConfig().getKeystorePassword())
                        .setKeyPassword(config.getSslConfig().getKeyPassword())
                        .setTruststoreLocation(config.getSslConfig().getTruststoreLocation())
                        .setTruststorePassword(config.getSslConfig().getTruststorePassword())
                        .build())
                .build();

        discoveryConfig = DiscoveryConfigParserV2.getDiscoveryConfig(clientConfig);

        try {
            DiscoveryClientRegistry.register(discoveryConfig, this::discoveryPropertiesChanged);
            DiscoveryClientRegistry.checkProperties(discoveryConfig);
        } catch (DiscoveryClientRegistrationException e) {
            throw new KSMLTopologyException("Axual discovery service registration failed", e);
        }
    }

    @Override
    public State getState() {
        if (axualStreams == null) {
            return State.STOPPED;
        }
        return convertStreamsState(axualStreams.state());
    }

    @Override
    public StreamsQuerier getQuerier() {
        return new StreamsQuerier() {
            @Override
            public Collection<StreamsMetadata> allMetadataForStore(String storeName) {
                return axualStreams.streamsMetadataForStore(storeName);
            }

            @Override
            public <K> KeyQueryMetadata queryMetadataForKey(String storeName, K key, Serializer<K> keySerializer) {
                return axualStreams.queryMetadataForKey(storeName, key, keySerializer);
            }

            @Override
            public <T> T store(StoreQueryParameters<T> storeQueryParameters) {
                return axualStreams.store(storeQueryParameters);
            }
        };
    }

    @Override
    public void close() {
        if (axualStreams != null) {
            axualStreams.close();
        }
    }

    private void createStreams() {
        Map<String, Object> configs = KafkaUtil.getKafkaConfigs(clientConfig);
        configs.putAll(discoveryResult.getConfigs());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 262144);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        configs.put(ProducerConfig.ACKS_CONFIG, "-1");

        // set up a stream topology generator based on the provided KSML definition
        // TODO: Remove
//        Map<String, Object> ksmlConfigs = new HashMap<>();
//        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_SOURCE_TYPE, "file");
//        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_WORKING_DIRECTORY, KSMLRunnerKsmlConfig.getWorkingDirectory());
//        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_CONFIG_DIRECTORY, KSMLRunnerKsmlConfig.getConfigurationDirectory());
//        ksmlConfigs.put(io.axual.ksml.KSMLConfig.KSML_SOURCE, KSMLRunnerKsmlConfig.getDefinitions());
//        ksmlConfigs.put(io.axual.ksml.KSMLConfig.NOTATION_LIBRARY, new AxualNotationLibrary(configs));

        var errorHandlerConfig = this.ksmlConfig.getErrorHandlingConfig();
        var consumeErrorHandlerConfig = errorHandlerConfig.getConsumerErrorHandlingConfig();
        var produceErrorHandlerConfig = errorHandlerConfig.getProducerErrorHandlingConfig();
        var processErrorHandlerConfig = errorHandlerConfig.getProcessErrorHandlingConfig();

        var ksmlConfig = KSMLConfig.builder()
                .sourceType("file")
                .workingDirectory(this.ksmlConfig.getWorkingDirectory())
                .configDirectory(this.ksmlConfig.getConfigurationDirectory())
                .source(this.ksmlConfig.getDefinitions())
                .notationLibrary(new AxualNotationLibrary(configs))
                .consumeErrorHandler(
                        new ErrorHandler(
                                consumeErrorHandlerConfig.isLog(),
                                consumeErrorHandlerConfig.isLogPayload(),
                                consumeErrorHandlerConfig.getLoggerName(),
                                consumeErrorHandlerConfig.getHandler() == KSMLErrorHandlingConfig.ErrorHandlingConfig.Handler.STOP ? ErrorHandler.HandlerType.STOP_ON_FAIL : ErrorHandler.HandlerType.CONTINUE_ON_FAIL)
                )
                .produceErrorHandler(
                        new ErrorHandler(
                                produceErrorHandlerConfig.isLog(),
                                produceErrorHandlerConfig.isLogPayload(),
                                produceErrorHandlerConfig.getLoggerName(),
                                produceErrorHandlerConfig.getHandler() == KSMLErrorHandlingConfig.ErrorHandlingConfig.Handler.STOP ? ErrorHandler.HandlerType.STOP_ON_FAIL : ErrorHandler.HandlerType.CONTINUE_ON_FAIL)
                )
                .processErrorHandler(
                        new ErrorHandler(
                                processErrorHandlerConfig.isLog(),
                                processErrorHandlerConfig.isLogPayload(),
                                processErrorHandlerConfig.getLoggerName(),
                                processErrorHandlerConfig.getHandler() == KSMLErrorHandlingConfig.ErrorHandlingConfig.Handler.STOP ? ErrorHandler.HandlerType.STOP_ON_FAIL : ErrorHandler.HandlerType.CONTINUE_ON_FAIL)
                )
                .build();

        var kafkaConfigs = new Properties();
        kafkaConfigs.putAll(configs);
        kafkaConfigs.put(AxualProducerConfig.CHAIN_CONFIG, ProxyChain.newBuilder()
                .append(RESOLVING_PROXY_ID)
                .append(LINEAGE_PROXY_ID)
                .build());
        topologyGenerator = new KSMLTopologyGenerator(clientConfig.getApplicationId(), ksmlConfig, kafkaConfigs);

        configs.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000);
        configs.put(StreamsConfig.STATE_DIR_CONFIG, this.ksmlConfig.getWorkingDirectory());
        if (this.ksmlConfig.getApplicationServer() != null) {
            configs.put(StreamsConfig.APPLICATION_SERVER_CONFIG, this.ksmlConfig.getApplicationServer());
        }

        configs.put(WrappedStreamsConfig.TOPOLOGY_FACTORY_CONFIG, (TopologyFactory) this::createTopology);
        configs.put(WrappedStreamsConfig.UNCAUGHT_EXCEPTION_HANDLER_FACTORY_CONFIG, (UncaughtExceptionHandlerFactory) streams -> ExecutionContext.INSTANCE::uncaughtException);
        configs.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        configs.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, UnknownTypeSerde.class.getName());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, UnknownTypeSerde.class.getName());

        configs.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        configs.put(AxualStreamsConfig.CHAIN_CONFIG, ProxyChain.newBuilder()
                .append(RESOLVING_PROXY_ID)
                .append(LINEAGE_PROXY_ID)
                .append(HEADER_PROXY_ID)
                .build());

        configs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        axualConfigs = configs;

        log.info("Creating StreamRunnerConfig...");

        log.info("Creating AxualStreams...");

        // TODO: create sane configuration here to allow for quick restart after crashes
        //        configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "vastewaarde");
        axualStreams = new AxualStreams(configs);
    }

    private Topology createTopology(StreamsBuilder builder) {
        log.info("Creating Topology...");
        var result = topologyGenerator.create(builder);
//        log.info("Validating Topology...");
//        var adminConfigs = new HashMap<>(axualConfigs);
//        adminConfigs.put(AxualAdminConfig.CHAIN_CONFIG, ProxyChain.newBuilder()
//                .append(RESOLVING_PROXY_ID)
//                .append(LINEAGE_PROXY_ID)
//                .append(HEADER_PROXY_ID)
//                .build());
//        var producerConfigs = new HashMap<>(axualConfigs);
//        producerConfigs.put(AxualProducerConfig.CHAIN_CONFIG, ProxyChain.newBuilder()
//                .append(RESOLVING_PROXY_ID)
//                .append(LINEAGE_PROXY_ID)
//                .append(HEADER_PROXY_ID)
//                .build());
//        producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, clientConfig.getApplicationId());
//        producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
//        producerConfigs.put(ProducerConfig.RETRIES_CONFIG, 1);
//        var producer = new AxualProducer<>(producerConfigs);
//        producer.initTransactions();
//        TopologyValidator.validateTopology(
//                result,
//                clientConfig.getApplicationId(),
//                new AxualAdminClient(adminConfigs),
//                producer);
        return result;
    }

    private boolean waitForDistribution() {
        final var timeout = stringValue(discoveryResult.getConfigs(), DISTRIBUTOR_TIMEOUT_CONFIG, DEFAULT_DISTRIBUTOR_TIMEOUT);
        final var distance = stringValue(discoveryResult.getConfigs(), DISTRIBUTOR_DISTANCE_CONFIG, DEFAULT_DISTRIBUTOR_DISTANCE);
        try {
            Utils.sleep(Long.parseLong(timeout) * Long.parseLong(distance));
            return true;
        } catch (NumberFormatException e) {
            log.warn("The distribution timeout could not be calculated because of invalid values. '{}' = '{}' AND '{}' = '{}'", DISTRIBUTOR_TIMEOUT_CONFIG, timeout, DISTRIBUTOR_DISTANCE_CONFIG, distance);
            return false;
        }
    }

    @Override
    public void run() {
        log.info("Starting Axual Backend");

        createStreams();

        axualStreams.start();
        Utils.sleep(1000);

        while (!stopRunning.get()) {
            DiscoveryClientRegistry.checkProperties(discoveryConfig);
            if (clusterSwitchDetected.getAndSet(false)) {
                log.warn("Cluster switch detected, shutting down runner to reinitialize on new cluster");
                axualStreams.stop();
                while (axualStreams.state() != KafkaStreams.State.NOT_RUNNING && axualStreams.state() != KafkaStreams.State.ERROR) {
                    // Check if runner is stopped
                    Utils.sleep(50);
                }
                if (waitForDistribution()) {
                    createStreams();
                    axualStreams.start();
                    Utils.sleep(1000);
                }
            }

            final var state = getState();
            if (state == State.STOPPED || state == State.FAILED) {
                log.info("Streams implementation has stopped, stopping Axual Backend");
                break;
            }
            Utils.sleep(200);
        }
    }

    public void discoveryPropertiesChanged(DiscoveryResult newDiscoveryResult) {
        clusterSwitchDetected.set(discoveryResult != null && newDiscoveryResult != null && !discoveryResult.equals(newDiscoveryResult));
        discoveryResult = newDiscoveryResult;
    }
}
