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
import io.axual.ksml.client.resolving.ResolvingClientConfig;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.ExecutionErrorHandler;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.metric.KsmlTagEnricher;
import io.axual.ksml.metric.KsmlMetricsReporter;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.runner.config.ApplicationServerConfig;
import io.axual.ksml.runner.exception.RunnerException;
import io.axual.ksml.runner.streams.KSMLClientSupplier;
import io.axual.utils.headers.cleaning.AxualHeaderCleaningInterceptor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

@Slf4j
public class KafkaStreamsRunner implements Runner {
    @Getter
    private final KafkaStreams kafkaStreams;
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);

    @Builder
    public record Config(Map<String, TopologyDefinition> definitions,
                         String storageDirectory,
                         ApplicationServerConfig appServer,
                         Map<String, String> kafkaConfig,
                         PythonContextConfig pythonContextConfig) {
        public Config(
                final Map<String, TopologyDefinition> definitions,
                final String storageDirectory,
                final ApplicationServerConfig appServer,
                final Map<String, String> kafkaConfig,
                final PythonContextConfig pythonContextConfig) {
            this.definitions = definitions;
            this.storageDirectory = storageDirectory;
            this.appServer = appServer;

            var processedKafkaConfig = new HashMap<>(kafkaConfig);
            // Check if a resolving client is required
            if (ResolvingClientConfig.configRequiresResolving(processedKafkaConfig)) {
                log.info("Using resolving clients for producer processing");
                // Replace the deprecated configuration keys with the current ones
                ResolvingClientConfig.replaceDeprecatedConfigKeys(processedKafkaConfig);
                processedKafkaConfig.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, KSMLClientSupplier.class.getCanonicalName());
            }
            this.kafkaConfig = processedKafkaConfig;
            this.pythonContextConfig = pythonContextConfig;
        }
    }

    public KafkaStreamsRunner(Config config) {
        this(config, KafkaStreams::new);
    }

    // Package private so tests can inject a factory
    KafkaStreamsRunner(Config config, BiFunction<Topology, Properties, KafkaStreams> kafkaStreamsFactory) {
        log.info("Constructing Kafka Backend");

        final var streamsProps = getStreamsConfig(config.kafkaConfig, config.storageDirectory, config.appServer);

        var applicationId = config.kafkaConfig != null ? config.kafkaConfig.get(StreamsConfig.APPLICATION_ID_CONFIG) : null;
        if (applicationId == null) {
            applicationId = "ksmlApplicationId";
        }

        final var streamsConfig = new StreamsConfig(streamsProps);
        final var topologyConfig = new TopologyConfig(streamsConfig);
        final var streamsBuilder = new StreamsBuilder(topologyConfig);
        var optimize = streamsProps.getOrDefault(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final var topologyGenerator = new TopologyGenerator(applicationId, (String) optimize, config.pythonContextConfig());
        final var topology = topologyGenerator.create(streamsBuilder, config.definitions);
        final var topologyDesc = topology.describe();
        final var ksmlTagEnricher = KsmlTagEnricher.from(topologyDesc);

        streamsProps.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
                "io.axual.ksml.metric.KsmlMetricsReporter," +
                        "org.apache.kafka.common.metrics.JmxReporter");
        streamsProps.put(KsmlMetricsReporter.ENRICHER_INSTANCE_CONFIG, ksmlTagEnricher);

        kafkaStreams = kafkaStreamsFactory.apply(topology, mapToProperties(streamsProps));
        kafkaStreams.setStateListener(this::logStreamsStateChange);
        kafkaStreams.setUncaughtExceptionHandler(ExecutionContext.INSTANCE.errorHandling()::uncaughtException);
    }

    private void logStreamsStateChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        log.info("Pipeline processing state change. Moving from old state '{}' to new state '{}'", oldState, newState);
    }

    private static final String CLEANUP_INTERCEPTOR_CLASS_NAME = AxualHeaderCleaningInterceptor.class.getCanonicalName();

    private static void addCleanupInterceptor(String configPrefix, Map<String, Object> configs, boolean addConfigIfMissing) {
        final var configName = configPrefix + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
        if (!configs.containsKey(configName) && !addConfigIfMissing) {
            return;
        }
        final var configValue = ConfigDef.parseType(configName, configs.get(configName), ConfigDef.Type.LIST);

        var interceptorClasses = new LinkedList<String>();
        if(configValue instanceof List<?> rawInterceptors) {
            // add list items
            for(Object rawInterceptor : rawInterceptors) {
                if(rawInterceptor instanceof String interceptorClassName) {
                    interceptorClasses.add(interceptorClassName);
                }else if(rawInterceptor instanceof Class<?> clazz) {
                    interceptorClasses.add(clazz.getCanonicalName());
                }
            }

            if(!interceptorClasses.contains(CLEANUP_INTERCEPTOR_CLASS_NAME)) {
                // Add the cleanup interceptor as first interceptor
                interceptorClasses.addFirst(CLEANUP_INTERCEPTOR_CLASS_NAME);
            }
        }

        configs.put(configName, String.join(",", interceptorClasses));
    }

    private Map<String, Object> getStreamsConfig(Map<String, String> initialConfigs, String storageDirectory, ApplicationServerConfig appServer) {
        final Map<String, Object> result = initialConfigs != null ? new HashMap<>(initialConfigs) : new HashMap<>();
        // Set default value if not explicitly configured
        result.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        // Explicit configs can overwrite those from the map
        result.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        result.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);

        // Make sure that all consumers have an interceptor configuration
        addCleanupInterceptor(StreamsConfig.CONSUMER_PREFIX, result, true);
        addCleanupInterceptor(StreamsConfig.MAIN_CONSUMER_PREFIX, result, false);
        addCleanupInterceptor(StreamsConfig.RESTORE_CONSUMER_PREFIX, result, false);
        addCleanupInterceptor(StreamsConfig.GLOBAL_CONSUMER_PREFIX, result, false);

        /*
        Permutations
        Global set, specific unset

         */

        result.put(StreamsConfig.STATE_DIR_CONFIG, storageDirectory);
        if (appServer != null && appServer.enabled()) {
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
        return switch (kafkaStreams.state()) {
            case CREATED -> State.CREATED;
            case REBALANCING -> State.STARTING;
            case RUNNING -> State.STARTED;
            case PENDING_SHUTDOWN -> State.STOPPING;
            case NOT_RUNNING -> State.STOPPED;
            case PENDING_ERROR, ERROR -> State.FAILED;
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
        if (getState() == State.FAILED) {
            throw new RunnerException("Kafka Streams is in a failed state");
        }
    }

    @Override
    public void stop() {
        stopRunning.set(true);
    }
}
