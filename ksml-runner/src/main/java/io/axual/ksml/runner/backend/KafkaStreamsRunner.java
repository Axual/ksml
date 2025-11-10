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
import io.axual.ksml.metric.KsmlMetricsReporter;
import io.axual.ksml.metric.KsmlTagEnricher;
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

/**
 * Implementation of the Runner interface that manages Kafka Streams applications.
 *
 * <p>This class is responsible for:</p>
 * <ul>
 *     <li>Creating and configuring Kafka Streams applications</li>
 *     <li>Managing the lifecycle (start, run, stop) of Kafka Streams applications</li>
 *     <li>Handling state transitions and error scenarios</li>
 *     <li>Adding cleanup interceptors to consumer configurations</li>
 *     <li>Providing metrics reporting</li>
 * </ul>
 *
 * <p>The runner creates a Kafka Streams instance based on the provided configuration,
 * sets up appropriate error handlers and state listeners, and manages the application's
 * lifecycle through the Runner interface methods.</p>
 */
@Slf4j
public class KafkaStreamsRunner implements Runner {
    @Getter
    private final KafkaStreams kafkaStreams;
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);
    // Default sleep durations that can be overridden in tests
    private long startupSleepMs = 1000;
    private long pollingSleepMs = 200;

    /**
     * Configuration record for the KafkaStreamsRunner.
     *
     * @param definitions         Map of topology definitions to be used in the Kafka Streams application
     * @param definitionFilePaths Map of topology definition names to their file directories
     * @param storageDirectory    Directory where Kafka Streams will store its state
     * @param appServer           Configuration for the application server (used for interactive queries)
     * @param kafkaConfig         Kafka configuration properties
     * @param pythonContextConfig Configuration for the Python execution context
     */
    @Builder
    public record Config(Map<String, TopologyDefinition> definitions,
                         Map<String, Path> definitionFilePaths,
                         String storageDirectory,
                         ApplicationServerConfig appServer,
                         Map<String, String> kafkaConfig,
                         PythonContextConfig pythonContextConfig) {
        public Config(
                final Map<String, TopologyDefinition> definitions,
                final Map<String, Path> definitionFilePaths,
                final String storageDirectory,
                final ApplicationServerConfig appServer,
                final Map<String, String> kafkaConfig,
                final PythonContextConfig pythonContextConfig) {
            this.definitions = definitions;
            this.definitionFilePaths = definitionFilePaths != null ? definitionFilePaths : new HashMap<>();
            this.storageDirectory = storageDirectory;
            this.appServer = appServer;

            var processedKafkaConfig = new HashMap<>(kafkaConfig);
            // Check if a resolving client is required
            if (ResolvingClientConfig.configRequiresResolving(processedKafkaConfig)) {
                log.info("Using resolving Kafka clients");
                processedKafkaConfig.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, KSMLClientSupplier.class.getCanonicalName());
            }
            this.kafkaConfig = processedKafkaConfig;
            this.pythonContextConfig = pythonContextConfig;
        }
    }

    /**
     * Creates a new KafkaStreamsRunner with the provided configuration.
     *
     * @param config The configuration for the Kafka Streams application
     */
    public KafkaStreamsRunner(Config config) {
        this(config, KafkaStreams::new);
    }

    /**
     * Package private constructor that allows tests to inject a factory for creating KafkaStreams instances.
     *
     * @param config              The configuration for the Kafka Streams application
     * @param kafkaStreamsFactory Factory function for creating KafkaStreams instances
     */
    KafkaStreamsRunner(Config config, BiFunction<Topology, Properties, KafkaStreams> kafkaStreamsFactory) {
        log.info("Constructing Kafka Backend");

        final var streamsProps = getStreamsConfig(config.kafkaConfig, config.storageDirectory, config.appServer);

        final var defaultAppId = "ksmlApplicationId";
        final var applicationId = config.kafkaConfig != null ? config.kafkaConfig.getOrDefault(StreamsConfig.APPLICATION_ID_CONFIG, defaultAppId) : defaultAppId;

        final var streamsConfig = new StreamsConfig(streamsProps);
        final var topologyConfig = new TopologyConfig(streamsConfig);
        final var streamsBuilder = new StreamsBuilder(topologyConfig);
        var optimize = streamsProps.getOrDefault(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final var topologyGenerator = new TopologyGenerator(applicationId, (String) optimize, config.pythonContextConfig(), config.definitionFilePaths());
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

    /**
     * Logs state changes in the Kafka Streams application.
     * This method is used as a state listener for the Kafka Streams instance.
     *
     * @param newState The new state of the Kafka Streams application
     * @param oldState The previous state of the Kafka Streams application
     */
    private void logStreamsStateChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        log.info("Pipeline processing state change. Moving from old state '{}' to new state '{}'", oldState, newState);
    }

    private static final String CLEANUP_INTERCEPTOR_CLASS_NAME = AxualHeaderCleaningInterceptor.class.getCanonicalName();

    /**
     * Adds the AxualHeaderCleaningInterceptor to the consumer interceptor configuration.
     * This method is package-private for testability.
     *
     * <p>The cleanup interceptor is added as the first interceptor in the chain if it's not already present.</p>
     * <ul>
     *     <li>For the plain consumer ({@code CONSUMER_PREFIX}), the interceptor is always added if {@code addConfigIfMissing} is true.</li>
     *     <li>For other consumers ({@code MAIN_CONSUMER_PREFIX}, {@code RESTORE_CONSUMER_PREFIX}, {@code GLOBAL_CONSUMER_PREFIX}),
     *     the interceptor is only added if the configuration already exists.</li>
     * </ul>
     *
     * @param configPrefix       The configuration prefix for the consumer (e.g., {@code consumer.}, {@code main.consumer.}, etc.)
     * @param configs            The configuration map to modify
     * @param addConfigIfMissing Whether to add the interceptor configuration if it doesn't exist
     */
    void addCleanupInterceptor(String configPrefix, Map<String, Object> configs, boolean addConfigIfMissing) {
        final var configName = configPrefix + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
        if (!configs.containsKey(configName) && !addConfigIfMissing) {
            return;
        }

        var interceptorClasses = new LinkedList<String>();

        // Get existing interceptors if any
        final var configValue = configs.get(configName);
        if (configValue != null) {
            var parsedValue = ConfigDef.parseType(configName, configValue, ConfigDef.Type.LIST);
            if (parsedValue instanceof List<?> rawInterceptors) {
                // add list items
                for (Object rawInterceptor : rawInterceptors) {
                    if (rawInterceptor instanceof String interceptorClassName) {
                        interceptorClasses.add(interceptorClassName);
                    } else if (rawInterceptor instanceof Class<?> clazz) {
                        interceptorClasses.add(clazz.getCanonicalName());
                    }
                }
            }
        }

        // Add the cleanup interceptor if not already present
        if (!interceptorClasses.contains(CLEANUP_INTERCEPTOR_CLASS_NAME)) {
            // Add the cleanup interceptor as first interceptor
            interceptorClasses.addFirst(CLEANUP_INTERCEPTOR_CLASS_NAME);
        }

        configs.put(configName, String.join(",", interceptorClasses));
    }

    /**
     * Package-private constructor specifically for testing.
     * Creates a KafkaStreamsRunner with a dummy topology and the provided tag enricher.
     *
     * @param config              The configuration for the Kafka Streams application
     * @param kafkaStreamsFactory Factory function for creating KafkaStreams instances
     * @param tagEnricher         The tag enricher to use for metrics
     */
    KafkaStreamsRunner(Config config, BiFunction<Topology, Properties, KafkaStreams> kafkaStreamsFactory, KsmlTagEnricher tagEnricher) {
        log.info("Constructing Kafka Backend (test mode)");

        final var streamsProps = getStreamsConfig(config.kafkaConfig, config.storageDirectory, config.appServer);

        streamsProps.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
                "io.axual.ksml.metric.KsmlMetricsReporter," +
                        "org.apache.kafka.common.metrics.JmxReporter");
        streamsProps.put(KsmlMetricsReporter.ENRICHER_INSTANCE_CONFIG, tagEnricher);

        // Create a dummy topology for testing
        Topology dummyTopology = new Topology();

        kafkaStreams = kafkaStreamsFactory.apply(dummyTopology, mapToProperties(streamsProps));
        kafkaStreams.setStateListener(this::logStreamsStateChange);
        kafkaStreams.setUncaughtExceptionHandler(ExecutionContext.INSTANCE.errorHandling()::uncaughtException);
    }

    /**
     * Creates a configuration map for Kafka Streams with appropriate settings.
     * This method is package-private for testability.
     *
     * <p>The method:</p>
     * <ul>
     *     <li>Copies the initial configuration if provided</li>
     *     <li>Sets default values for optimization if not explicitly configured</li>
     *     <li>Sets exception handlers for production and deserialization errors</li>
     *     <li>Adds cleanup interceptors to all consumer configurations</li>
     *     <li>Sets the state directory for Kafka Streams</li>
     *     <li>Configures the application server if enabled</li>
     * </ul>
     *
     * @param initialConfigs   Initial configuration map (can be {@code null})
     * @param storageDirectory Directory where Kafka Streams will store its state
     * @param appServer        Configuration for the application server (used for interactive queries)
     * @return A map with the complete Kafka Streams configuration
     */
    Map<String, Object> getStreamsConfig(Map<String, String> initialConfigs, String storageDirectory, ApplicationServerConfig appServer) {
        final Map<String, Object> result = initialConfigs != null ? new HashMap<>(initialConfigs) : new HashMap<>();
        // Set default value if not explicitly configured
        result.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        // Explicit configs can overwrite those from the map
        result.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        result.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);

        // Make sure that all consumers have an interceptor configuration
        addCleanupInterceptor(StreamsConfig.CONSUMER_PREFIX, result, true);
        addCleanupInterceptor(StreamsConfig.MAIN_CONSUMER_PREFIX, result, false);
        addCleanupInterceptor(StreamsConfig.RESTORE_CONSUMER_PREFIX, result, false);
        addCleanupInterceptor(StreamsConfig.GLOBAL_CONSUMER_PREFIX, result, false);

        result.put(StreamsConfig.STATE_DIR_CONFIG, storageDirectory);
        if (appServer != null && appServer.enabled()) {
            result.put(StreamsConfig.APPLICATION_SERVER_CONFIG, appServer.getApplicationServer());
        }

        return result;
    }

    /**
     * Converts a Map to a Properties object.
     *
     * @param configs The map to convert
     * @return A Properties object containing all entries from the map
     */
    private Properties mapToProperties(Map<String, Object> configs) {
        final var result = new Properties();
        result.putAll(configs);
        return result;
    }

    /**
     * Maps the Kafka Streams state to the Runner state.
     *
     * @return The current state of the runner
     */
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

    /**
     * Starts the Kafka Streams application and monitors its state.
     * This method blocks until the application is stopped or fails.
     *
     * <p>The method:</p>
     * <ul>
     *     <li>Starts the Kafka Streams application</li>
     *     <li>Waits for a short period to allow asynchronous startup</li>
     *     <li>Continuously monitors the application state</li>
     *     <li>Closes the application when stopped</li>
     *     <li>Throws an exception if the application fails</li>
     * </ul>
     */
    @Override
    public void run() {
        log.info("Starting Kafka Streams backend");
        try {
            kafkaStreams.start();
            // Allow Kafka Streams to start up asynchronously
            Utils.sleep(startupSleepMs);

            // While we need to keep running, wait and check for failure state
            while (isRunning() && !stopRunning.get()) {
                Utils.sleep(pollingSleepMs);
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

    /**
     * Sets the sleep durations used in the run method.
     * This method is package-private and intended for testing to reduce wait times.
     *
     * @param startupSleepMs Time to sleep after starting Kafka Streams to allow asynchronous startup
     * @param pollingSleepMs Time to sleep between state checks while running
     */
    void setSleepDurations(long startupSleepMs, long pollingSleepMs) {
        this.startupSleepMs = startupSleepMs;
        this.pollingSleepMs = pollingSleepMs;
    }

    /**
     * Signals the runner to stop.
     * This method sets a flag that will cause the run method to exit its monitoring loop.
     */
    @Override
    public void stop() {
        stopRunning.set(true);
    }
}
