package io.axual.ksml.runner;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonOption;
import com.github.victools.jsonschema.module.jackson.JacksonSchemaModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;
import io.axual.ksml.client.serde.ResolvingDeserializer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider;
import io.axual.ksml.data.notation.json.JsonSchemaMapper;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.rest.server.ComponentState;
import io.axual.ksml.rest.server.KsmlQuerier;
import io.axual.ksml.rest.server.RestServer;
import io.axual.ksml.runner.backend.KafkaProducerRunner;
import io.axual.ksml.runner.backend.KafkaStreamsRunner;
import io.axual.ksml.runner.backend.Runner;
import io.axual.ksml.runner.config.ApplicationServerConfig;
import io.axual.ksml.runner.config.ErrorHandlingConfig;
import io.axual.ksml.runner.config.KSMLConfig;
import io.axual.ksml.runner.config.KSMLRunnerConfig;
import io.axual.ksml.runner.config.NotationConfig;
import io.axual.ksml.runner.config.PrometheusConfig;
import io.axual.ksml.runner.config.SchemaRegistryConfig;
import io.axual.ksml.runner.config.internal.KsmlFileOrDefinitionProvider;
import io.axual.ksml.runner.config.internal.KsmlFileOrDefinitionSubTypeResolver;
import io.axual.ksml.runner.config.internal.StringMapDefinitionPropertiesResolver;
import io.axual.ksml.runner.exception.ConfigException;
import io.axual.ksml.runner.notation.NotationFactories;
import io.axual.ksml.runner.prometheus.PrometheusExport;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import picocli.CommandLine;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static picocli.CommandLine.Option.NULL_VALUE;

@Slf4j
public class KSMLRunner {
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-runner.yaml";
    private static final String WRITE_KSML_SCHEMA_ARGUMENT = "--schema";
    private static final String WRITE_KSML_RUNNER_SCHEMA_ARGUMENT = "--runner-schema";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @CommandLine.Command(name = "KSML", description = "Load and run KSML streaming pipelines")
    public static class Arguments {
        @CommandLine.Parameters(index = "0", paramLabel = "KSML-RUNNER-CONFIG-PATH", description = "The location of the KSML Runner configuration file", defaultValue = DEFAULT_CONFIG_FILE_SHORT)
        String configFilePath;

        @CommandLine.Option(names = {WRITE_KSML_SCHEMA_ARGUMENT}, description = "Print the KSML Definition schema to the standard out, or to the file provided as argument", arity = "0..1", defaultValue = NULL_VALUE)
        String ksmlSchemaLocation = NULL_VALUE;

        @CommandLine.Option(names = {WRITE_KSML_RUNNER_SCHEMA_ARGUMENT}, description = "Print the KSML Runner configuration schema to the standard out, or to the file provided as argument", arity = "0..1", defaultValue = NULL_VALUE)
        String ksmlRunnerSchemaLocation = NULL_VALUE;

        private CommandLine.ParseResult parseResult;

        public File configFile() {
            return new File(configFilePath);
        }

        public boolean shouldPrintKsmlSchema() {
            return parseResult.hasMatchedOption(WRITE_KSML_SCHEMA_ARGUMENT);
        }

        public String ksmlSchemaLocation() {
            // Needed because value can be empty string instead of null
            return StringUtils.isNotBlank(ksmlSchemaLocation) ? ksmlSchemaLocation : null;
        }

        public boolean shouldPrintKsmlRunnerSchema() {
            return parseResult.hasMatchedOption(WRITE_KSML_RUNNER_SCHEMA_ARGUMENT);
        }

        public String ksmlRunnerSchemaLocation() {
            // Needed because value can be empty string instead of null
            return StringUtils.isNotBlank(ksmlRunnerSchemaLocation) ? ksmlRunnerSchemaLocation : null;
        }

        /**
         * Populate this class using the PicoCLI command line library
         *
         * @param args the command line arguments to parse
         * @return A new instance of the Arguments class, populated according to the specifications
         */
        public static Arguments populate(String[] args) {
            var newArgs = new Arguments();
            var cmd = new CommandLine(newArgs);
            newArgs.parseResult = cmd.parseArgs(args);
            return newArgs;
        }
    }

    public static void main(String[] args) {
        var cmd = Arguments.populate(args);

        try {
            // Load name and version from manifest
            var ksmlTitle = determineTitle();

            if (shouldExit(cmd)) {
                log.info("Exiting after schema");
                return;
            }

            log.info("Starting {}", ksmlTitle);

            // Begin loading config file
            final var configFile = cmd.configFile();
            if (!configFile.exists()) {
                log.error("Configuration file '{}' not found", configFile);
                System.exit(1);
            }

            final var config = readConfiguration(configFile);
            final var ksmlConfig = config.getKsmlConfig();
            log.info("Using directories: config: {}, schema: {}, storage: {}", ksmlConfig.configDirectory(), ksmlConfig.schemaDirectory(), ksmlConfig.storageDirectory());

            final var definitions = ksmlConfig.definitions();
            validateDefinitions(definitions);

            KsmlInfo.registerKsmlAppInfo(config.getApplicationId());

            // Start the appserver if needed
            final var restServer = createRestServer(ksmlConfig.applicationServerConfig());

            // Set up the default schema directory
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(ksmlConfig.schemaDirectory());

            // Set up all notations (defaults, configured overrides and the AVRO fallback)
            registerNotations(ksmlConfig, config.getKafkaConfigMap());

            setupErrorHandling(ksmlConfig.errorHandlingConfig());
            ExecutionContext.INSTANCE.serdeWrapper(serde -> wrapSerde(serde, config.getKafkaConfigMap()));

            final var parsedDefinitions = parseDefinitions(definitions);
            final var definitionSplit = splitDefinitions(parsedDefinitions, ksmlConfig.enableProducers(), ksmlConfig.enablePipelines());

            final var producer = createProducerRunner(definitionSplit.producers(), config, ksmlConfig);
            final var streams = createStreamsRunner(definitionSplit.pipelines(), config, ksmlConfig);

            if (producer != null || streams != null) {
                runRunners(producer, streams, restServer, ksmlConfig.prometheusConfig());
            }
        } catch (Throwable t) {
            log.error("KSML Stopping because of unhandled exception");
            throw FatalError.report(t);
        }
        // Explicit exit, need to find out which threads are actually stopping us
        System.exit(0);
    }

    /**
     * Creates and starts the REST server for state-store queries and health checks, or returns
     * {@code null} when the application server is disabled. Extracted from {@code main} for readability.
     *
     * @param appServer the application server configuration
     * @return a started {@link RestServer}, or {@code null} when disabled
     */
    static RestServer createRestServer(ApplicationServerConfig appServer) {
        if (!appServer.enabled()) {
            return null;
        }
        final var hostInfo = new HostInfo(appServer.getHost(), appServer.getPort());
        final var restServer = new RestServer(hostInfo);
        restServer.start();
        return restServer;
    }

    /**
     * Creates a {@link KafkaProducerRunner} for the given producer definitions, or {@code null} when
     * there are none. Extracted from {@code main} for readability.
     *
     * @param producerDefinitions the producer definitions to run
     * @param config              the runner configuration (for Kafka settings)
     * @param ksmlConfig          the KSML configuration (for the Python context)
     * @return a producer runner, or {@code null} when there are no producer definitions
     */
    static KafkaProducerRunner createProducerRunner(Map<String, TopologyDefinition> producerDefinitions, KSMLRunnerConfig config, KSMLConfig ksmlConfig) {
        if (producerDefinitions.isEmpty()) {
            return null;
        }
        return new KafkaProducerRunner(KafkaProducerRunner.Config.builder()
                .definitions(producerDefinitions)
                .kafkaConfig(config.getKafkaConfigMap())
                .pythonContextConfig(ksmlConfig.pythonContextConfig())
                .build());
    }

    /**
     * Creates a {@link KafkaStreamsRunner} for the given pipeline definitions, or {@code null} when
     * there are none. Extracted from {@code main} for readability.
     *
     * @param pipelineDefinitions the pipeline definitions to run
     * @param config              the runner configuration (for Kafka settings)
     * @param ksmlConfig          the KSML configuration (storage directory, app server, Python context)
     * @return a streams runner, or {@code null} when there are no pipeline definitions
     */
    static KafkaStreamsRunner createStreamsRunner(Map<String, TopologyDefinition> pipelineDefinitions, KSMLRunnerConfig config, KSMLConfig ksmlConfig) {
        if (pipelineDefinitions.isEmpty()) {
            return null;
        }
        return new KafkaStreamsRunner(KafkaStreamsRunner.Config.builder()
                .storageDirectory(ksmlConfig.storageDirectory())
                .appServer(ksmlConfig.applicationServerConfig())
                .definitions(pipelineDefinitions)
                .kafkaConfig(config.getKafkaConfigMap())
                .pythonContextConfig(ksmlConfig.pythonContextConfig())
                .build());
    }

    /**
     * Runs the producer and/or streams runners to completion: registers a shutdown hook, starts the
     * Prometheus exporter, submits the runners to an executor, wires the REST querier and the
     * cross-stop-on-failure handlers, waits for both to finish and finally cleans everything up.
     * Extracted from {@code main} to keep the orchestration in one cohesive, readable place.
     *
     * @param producer         the producer runner, or {@code null}
     * @param streams          the streams runner, or {@code null}
     * @param restServer       the REST server, or {@code null} when disabled
     * @param prometheusConfig the Prometheus exporter configuration
     * @throws Exception when the Prometheus exporter or executor shutdown fails
     */
    static void runRunners(KafkaProducerRunner producer, KafkaStreamsRunner streams, RestServer restServer, PrometheusConfig prometheusConfig) throws Exception {
        var shutdownHook = new Thread(() -> stopRunnersQuietly(producer, streams));
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try (var prometheusExport = new PrometheusExport(prometheusConfig)) {
            prometheusExport.start();
            final var executorService = Executors.newFixedThreadPool(2);

            final var producerFuture = producer == null ? CompletableFuture.completedFuture(null) : CompletableFuture.runAsync(producer, executorService);
            final var streamsFuture = streams == null ? CompletableFuture.completedFuture(null) : CompletableFuture.runAsync(streams, executorService);

            try {
                // Allow the runner(s) to start
                Utils.sleep(2000);

                if (restServer != null) {
                    // Run with the REST server
                    restServer.initGlobalQuerier(getQuerier(streams, producer));
                }

                // If one runner fails, always stop the other one too
                producerFuture.whenComplete((result, exc) -> stopOtherOnFailure(exc, "Producer failed", streams));
                streamsFuture.whenComplete((result, exc) -> stopOtherOnFailure(exc, "Stream processing failed", producer));

                // wait for all futures to finish
                CompletableFuture.allOf(producerFuture, streamsFuture).join();

                closeExecutorService(executorService);
            } finally {
                if (restServer != null) restServer.close();
            }
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }

    /**
     * Returns the wire-name string for the notation factory lookup, or {@code null} when the
     * config or its type is missing. Extracted for testability.
     */
    static String resolveFactoryName(NotationConfig notationConfig) {
        if (notationConfig == null || notationConfig.type() == null) {
            return null;
        }
        return notationConfig.type().jsonValue();
    }

    /**
     * Wraps a serde so that its serializer and deserializer resolve topic/group names using the runner's
     * Kafka configuration. Extracted from {@code main} for testability.
     *
     * @param serde       the serde to wrap
     * @param kafkaConfig the Kafka configuration used by the resolving (de)serializers
     * @return a serde whose (de)serializers resolve names according to the configuration
     */
    static <T> Serde<T> wrapSerde(Serde<T> serde, Map<String, String> kafkaConfig) {
        return new Serdes.WrapperSerde<>(
                new ResolvingSerializer<>(serde.serializer(), kafkaConfig),
                new ResolvingDeserializer<>(serde.deserializer(), kafkaConfig));
    }

    /**
     * Parses each raw KSML definition into a {@link TopologyDefinition}. Extracted from {@code main}
     * for testability.
     *
     * @param definitions the raw definitions keyed by namespace
     * @return the parsed topology definitions keyed by namespace
     */
    static Map<String, TopologyDefinition> parseDefinitions(Map<String, JsonNode> definitions) {
        final Map<String, TopologyDefinition> parsedDefinitions = new HashMap<>();
        definitions.forEach((name, definition) -> {
            final var parser = new TopologyDefinitionParser(name);
            parsedDefinitions.put(name, parser.parse(ParseNode.fromRoot(definition, name)));
        });
        return parsedDefinitions;
    }

    /**
     * Stops the producer and streams runners, swallowing any exception so a shutdown hook never fails.
     * Either runner may be {@code null}. Extracted from {@code main} for testability.
     *
     * @param producer the producer runner, or {@code null}
     * @param streams  the streams runner, or {@code null}
     */
    static void stopRunnersQuietly(Runner producer, Runner streams) {
        try {
            log.debug("In KSML shutdown hook");
            if (producer != null) producer.stop();
            if (streams != null) streams.stop();
        } catch (Exception e) {
            log.error("Could not properly shut down KSML", e);
        }
    }

    /**
     * Determines whether the runner should exit after printing a schema.
     * 
     * @param cmd the parsed command line arguments
     * @return {@code true} when the runner should exit, {@code false} otherwise
     */
    static boolean shouldExit(Arguments cmd) {
        var shouldExit = false;
        // Check if we need to output the KSML schema and then exit
        if (cmd.shouldPrintKsmlSchema()) {
            log.info("Generating KSML definitions schema");
            shouldExit = true;
            printKsmlDefinitionSchema(cmd.ksmlSchemaLocation());
        }

        // Check if we need to output the runner schema and then exit
        if (cmd.shouldPrintKsmlRunnerSchema()) {
            log.info("Generating KSML Runner Configuration Schema");
            shouldExit = true;
            printRunnerSchema(cmd.ksmlRunnerSchemaLocation());
        }
        return shouldExit;
    }

    /**
     * When a runner completes exceptionally, stops the other runner so the whole application winds down.
     * Used as the completion handler for the producer and streams futures. Extracted from {@code main}
     * for testability.
     *
     * @param exc        the exception the future completed with, or {@code null} on success
     * @param logMessage the message to log when a failure occurred
     * @param other      the other runner to stop, or {@code null}
     */
    static void stopOtherOnFailure(Throwable exc, String logMessage, Runner other) {
        if (exc != null) {
            log.info(logMessage, exc);
            // Exception, always stop the other runner too
            if (other != null) {
                other.stop();
            }
        }
    }

    /**
     * Result of splitting parsed KSML definitions into the ones containing producers and the ones
     * containing pipelines. A single definition can appear in both maps.
     */
    record DefinitionSplit(Map<String, TopologyDefinition> producers, Map<String, TopologyDefinition> pipelines) {
    }

    /**
     * Splits the parsed definitions into producer and pipeline definitions and applies the runner's
     * enable toggles. When producers or pipelines are disabled, the corresponding definitions are
     * dropped (with a warning). Extracted from {@code main} for testability.
     *
     * @param parsedDefinitions the parsed topology definitions keyed by namespace
     * @param enableProducers   whether producers are enabled for this runner
     * @param enablePipelines   whether pipelines are enabled for this runner
     * @return the producer and pipeline definitions that should actually run
     */
    static DefinitionSplit splitDefinitions(Map<String, TopologyDefinition> parsedDefinitions, boolean enableProducers, boolean enablePipelines) {
        final Map<String, TopologyDefinition> producerDefinitions = new HashMap<>();
        final Map<String, TopologyDefinition> pipelineDefinitions = new HashMap<>();
        parsedDefinitions.forEach((name, topologyDefinition) -> {
            if (!topologyDefinition.producers().isEmpty()) producerDefinitions.put(name, topologyDefinition);
            if (!topologyDefinition.pipelines().isEmpty()) pipelineDefinitions.put(name, topologyDefinition);
        });

        if (!enableProducers && !producerDefinitions.isEmpty()) {
            log.warn("Producers are disabled for this runner. The supplied producer specifications will be ignored.");
            producerDefinitions.clear();
        }
        if (!enablePipelines && !pipelineDefinitions.isEmpty()) {
            log.warn("Pipelines are disabled for this runner. The supplied pipeline specifications will be ignored.");
            pipelineDefinitions.clear();
        }
        return new DefinitionSplit(producerDefinitions, pipelineDefinitions);
    }

    /**
     * Validates that the runner has at least one KSML definition to run. Extracted from {@code main}
     * for testability.
     *
     * @param definitions the resolved KSML definitions keyed by namespace
     * @throws ConfigException when no definitions are configured
     */
    static void validateDefinitions(Map<String, ?> definitions) {
        if (definitions == null || definitions.isEmpty()) {
            throw new ConfigException("definitions", definitions, "No KSML definitions found in configuration");
        }
    }

    /**
     * Registers all notations in the {@link ExecutionContext}: first the built-in defaults provided by
     * {@link NotationFactories}, then the configured notation overrides, and finally a Confluent AVRO
     * fallback when no notations are configured at all. Extracted from {@code main} for testability.
     *
     * @param ksmlConfig     the KSML configuration holding notation overrides and schema registries
     * @param kafkaConfigMap the effective Kafka configuration passed to the notation factories
     */
    static void registerNotations(KSMLConfig ksmlConfig, Map<String, String> kafkaConfigMap) {
        // Set up all default notations and register them in the NotationLibrary
        final var notationFactories = new NotationFactories(kafkaConfigMap);
        for (final var notation : notationFactories.notations().entrySet()) {
            ExecutionContext.INSTANCE.notationLibrary().register(notation.getKey(), notation.getValue().create(null));
        }

        // Set up all notation overrides from the KSML config
        for (final var notationEntry : ksmlConfig.notations().entrySet()) {
            final var notationStr = notationEntry.getKey() != null ? notationEntry.getKey() : "undefined";

            final var notationConfig = notationEntry.getValue();
            // Resolve the schema-registry and notation configuration up front so a missing schema
            // registry is reported even when the notation type itself is incomplete.
            final var notationConfigs = mergeNotationConfigs(notationConfig, ksmlConfig.schemaRegistries());
            final var factoryName = resolveFactoryName(notationConfig);
            if (notationConfig != null && factoryName != null) {
                final var factory = notationFactories.notations().get(factoryName);
                if (factory == null) {
                    throw FatalError.report(new ConfigException("Unknown notation type: " + factoryName));
                }
                ExecutionContext.INSTANCE.notationLibrary().register(notationStr, factory.create(notationConfigs));
            } else {
                log.warn("Notation configuration incomplete: notation={}, serde={}", notationStr, factoryName);
            }
        }

        // Ensure typical defaults are used for AVRO
        // WARNING: Defaults for notations will be deprecated in the future. Make sure you explicitly configure
        // notations with multiple implementations (like AVRO) in your ksml-runner.yaml.
        if (ksmlConfig.notations().isEmpty()) {
            final var dataMapper = new DataObjectFlattener();
            final var dataTypeMapper = new DataTypeFlattener();
            final var defaultAvro = new ConfluentAvroNotationProvider().createNotation(new NotationContext(dataMapper, dataTypeMapper, kafkaConfigMap));
            ExecutionContext.INSTANCE.notationLibrary().register(AvroNotation.NOTATION_NAME, defaultAvro);
            log.warn("No notations configured. Loading default Avro notation with Confluent implementation. If you use AVRO in your KSML definition, please explicitly configure notations in the ksml-runner.yaml.");
        }
    }

    /**
     * Merges the configuration that a notation factory should receive: first the referenced schema
     * registry configuration (when present), then the notation's own configuration overrides.
     * Extracted from {@code main} for testability.
     *
     * @param notationConfig   the notation configuration entry
     * @param schemaRegistries the configured schema registries, keyed by name
     * @return the merged configuration map passed to the notation factory
     */
    static Map<String, String> mergeNotationConfigs(NotationConfig notationConfig, Map<String, SchemaRegistryConfig> schemaRegistries) {
        final var notationConfigs = new HashMap<String, String>();
        final var schemaRegistryName = notationConfig.schemaRegistry();
        if (schemaRegistryName != null) {
            final var srConfigs = schemaRegistries.get(schemaRegistryName);
            if (srConfigs != null && srConfigs.config() != null) {
                notationConfigs.putAll(srConfigs.config());
            } else {
                log.warn("No schema registry configuration found for schema registry: {}", schemaRegistryName);
            }
        }
        if (notationConfig.config() != null) notationConfigs.putAll(notationConfig.config());
        return notationConfigs;
    }

    /**
     * Registers the consume, produce and process error handlers in the {@link ExecutionContext}.
     * A {@code null} configuration leaves the existing handlers untouched. Extracted from {@code main}
     * for testability.
     *
     * @param errorHandling the error handling configuration, or {@code null}
     */
    static void setupErrorHandling(ErrorHandlingConfig errorHandling) {
        if (errorHandling != null) {
            ExecutionContext.INSTANCE.errorHandling().setConsumeHandler(getErrorHandler(errorHandling.consumerErrorHandlingConfig()));
            ExecutionContext.INSTANCE.errorHandling().setProduceHandler(getErrorHandler(errorHandling.producerErrorHandlingConfig()));
            ExecutionContext.INSTANCE.errorHandling().setProcessHandler(getErrorHandler(errorHandling.processErrorHandlingConfig()));
        }
    }

    static void closeExecutorService(final ExecutorService executorService) throws ExecutionException {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
            throw new ExecutionException("Exception caught while shutting down", e);
        }
    }

    protected static KsmlQuerier getQuerier(KafkaStreamsRunner streamsRunner, KafkaProducerRunner producerRunner) {
        return new KsmlQuerier() {
            @Override
            public Collection<StreamsMetadata> allMetadataForStore(String storeName) {
                if (streamsRunner == null) {
                    return List.of();
                }
                return streamsRunner.kafkaStreams().streamsMetadataForStore(storeName);
            }

            @Override
            public <K> KeyQueryMetadata queryMetadataForKey(String storeName, K key, Serializer<K> keySerializer) {
                if (streamsRunner == null) {
                    return null;
                }
                return streamsRunner.kafkaStreams().queryMetadataForKey(storeName, key, keySerializer);
            }

            @Override
            public <T> T store(StoreQueryParameters<T> storeQueryParameters) {
                if (streamsRunner == null) {
                    return null;
                }
                return streamsRunner.kafkaStreams().store(storeQueryParameters);
            }

            @Override
            public ComponentState getStreamRunnerState() {
                if (streamsRunner == null) {
                    return ComponentState.NOT_APPLICABLE;
                }
                return stateConverter(streamsRunner.getState());
            }

            @Override
            public ComponentState getProducerState() {
                if (producerRunner == null) {
                    return ComponentState.NOT_APPLICABLE;
                }
                return stateConverter(producerRunner.getState());
            }

            ComponentState stateConverter(Runner.State state) {
                return switch (state) {
                    case CREATED -> ComponentState.CREATED;
                    case STARTING -> ComponentState.STARTING;
                    case STARTED -> ComponentState.STARTED;
                    case STOPPING -> ComponentState.STOPPING;
                    case STOPPED -> ComponentState.STOPPED;
                    case FAILED -> ComponentState.FAILED;
                };
            }
        };
    }

    private static String determineTitle() {
        var titleBuilder = new StringBuilder()
                .append(KsmlInfo.APP_NAME);
        if (!KsmlInfo.APP_VERSION.isBlank()) {
            titleBuilder.append(" ").append(KsmlInfo.APP_VERSION);
        }
        if (!KsmlInfo.BUILD_TIME.isBlank()) {
            titleBuilder.append(" (")
                    .append(KsmlInfo.BUILD_TIME)
                    .append(")");
        }
        return titleBuilder.toString();
    }

    @SuppressWarnings("java:S106")
    private static void printRunnerSchema(String filename) {
        final var moduleJackson = new JacksonSchemaModule(
                JacksonOption.RESPECT_JSONPROPERTY_REQUIRED,
                JacksonOption.ALWAYS_REF_SUBTYPES,
                JacksonOption.FLATTENED_ENUMS_FROM_JSONVALUE);
        final var moduleJakarta = new JakartaValidationModule(
                JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS
        );

        final var configBuilder = new SchemaGeneratorConfigBuilder(
                SchemaVersion.DRAFT_2019_09, OptionPreset.PLAIN_JSON)
                .with(moduleJackson)
                .with(moduleJakarta)
                .with(Option.MAP_VALUES_AS_ADDITIONAL_PROPERTIES, Option.DEFINITIONS_FOR_ALL_OBJECTS, Option.FORBIDDEN_ADDITIONAL_PROPERTIES_BY_DEFAULT);

        // Instantiate add providers/resolved to allow the KsmlFileOrDefinition and StringMap to be processed with correct Schema types
        final var stringMapResolver = new StringMapDefinitionPropertiesResolver();
        configBuilder.forFields()
                .withAdditionalPropertiesResolver(stringMapResolver)
                .withDefaultResolver(KSMLRunner::resolveDefaultValue);
        configBuilder.forTypesInGeneral()
                // Add support for the KsmlFileOrDefinition
                .withCustomDefinitionProvider(stringMapResolver)
                .withCustomDefinitionProvider(new KsmlFileOrDefinitionProvider())
                .withSubtypeResolver(new KsmlFileOrDefinitionSubTypeResolver())
        ;

        // Construct the real schema
        final var config = configBuilder.build();
        final var generator = new SchemaGenerator(config);
        final var jsonSchema = generator.generateSchema(KSMLRunnerConfig.class);
        final var schema = jsonSchema.toPrettyString();

        try {
            if (filename != null) {
                final var writer = new PrintWriter(filename);
                writer.println(schema);
                writer.close();
                log.info("KSML JSON schema written to file: {}", filename);
            } else {
                System.out.println(schema);
            }
        } catch (Exception e) {
            log.atError()
                    .setMessage("""
                            Error writing KSML Runner JSON schema to file: {}
                            Error: {}
                            """)
                    .addArgument(filename)
                    .addArgument(e::getMessage)
                    .log();
        }
    }

    @SuppressWarnings("java:S106")
    /**
     * Resolves the schema {@code default} for a field from its {@link JsonProperty#defaultValue()}.
     * <p>
     * victools does not read {@code @JsonProperty(defaultValue=...)} on its own, so emit it here.
     * The value is converted to the field's type so the schema 'default' is correctly typed
     * (e.g. boolean false rather than the string "false") for autocomplete tooling.
     *
     * @param field the field whose default value should be resolved
     * @return the typed default value, or {@code null} when the field has no {@code defaultValue}
     */
    private static Object resolveDefaultValue(FieldScope field) {
        final var jsonProperty = field.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
        if (jsonProperty == null || jsonProperty.defaultValue().isEmpty()) {
            return null;
        }
        final var defaultValue = jsonProperty.defaultValue();
        final var type = field.getType().getErasedType();
        if (type == boolean.class || type == Boolean.class) return Boolean.valueOf(defaultValue);
        if (type == int.class || type == Integer.class) return Integer.valueOf(defaultValue);
        if (type == long.class || type == Long.class) return Long.valueOf(defaultValue);
        return defaultValue;
    }

    @SuppressWarnings("java:S106")
    private static void printKsmlDefinitionSchema(String filename) {
        // Check if the runner was started with "--schema". If so, then we output the JSON schema to validate the
        // KSML definitions with on stdout and exit
        final var parser = new TopologyDefinitionParser("dummy");
        final var schema = new JsonSchemaMapper(true).fromDataSchema(parser.schema());

        if (filename != null) {
            try {
                final var writer = new PrintWriter(filename);
                writer.println(schema);
                writer.close();
                log.info("KSML JSON schema written to file: {}", filename);
            } catch (Exception e) {
                log.atError()
                        .setMessage("""
                                Error writing KSML JSON schema to file: {}
                                Error: {}
                                """)
                        .addArgument(filename)
                        .addArgument(e::getMessage)
                        .log();
            }
        } else {
            System.out.println(schema);
        }
    }

    static KSMLRunnerConfig readConfiguration(File configFile) {
        final var mapper = new ObjectMapper(new YAMLFactory());
        try {
            final var config = mapper.readValue(configFile, KSMLRunnerConfig.class);
            if (config != null) return config;
        } catch (JacksonException e) {
            log.error("Configuration exception", e);
        }
        throw new ConfigException("No configuration found");
    }

    static ErrorHandler getErrorHandler(ErrorHandlingConfig.ErrorTypeHandlingConfig config) {
        final var handlerType = switch (config.handler()) {
            case CONTINUE -> ErrorHandler.HandlerType.CONTINUE_ON_FAIL;
            case STOP -> ErrorHandler.HandlerType.STOP_ON_FAIL;
            case RETRY -> ErrorHandler.HandlerType.RETRY_ON_FAIL;
            // An explicit "handler:" (null) in the config overrides the Handler.STOP field default,
            // fall back to the documented default rather than throwing a NullPointerException here.
            case null -> ErrorHandler.HandlerType.STOP_ON_FAIL;
        };
        return new ErrorHandler(
                config.log(),
                config.loggerName(),
                config.logPayload(),
                handlerType);
    }
}
