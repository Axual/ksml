package io.axual.ksml.runner;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jackson.JacksonOption;
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
import io.axual.ksml.runner.config.ErrorHandlingConfig;
import io.axual.ksml.runner.config.KSMLRunnerConfig;
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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
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

            boolean shouldExit = false;
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

            if (shouldExit) {
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
            if (definitions == null || definitions.isEmpty()) {
                throw new ConfigException("definitions", definitions, "No KSML definitions found in configuration");
            }

            KsmlInfo.registerKsmlAppInfo(config.getApplicationId());

            // Start the appserver if needed
            final var appServer = ksmlConfig.applicationServerConfig();
            RestServer restServer = null;
            // Start rest server to provide service endpoints
            if (appServer.enabled()) {
                HostInfo hostInfo = new HostInfo(appServer.getHost(), appServer.getPort());
                restServer = new RestServer(hostInfo);
                restServer.start();
            }

            // Set up the default schema directory
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(ksmlConfig.schemaDirectory());

            // Set up all default notations and register them in the NotationLibrary
            final var notationFactories = new NotationFactories(config.getKafkaConfigMap());
            for (final var notation : notationFactories.notations().entrySet()) {
                ExecutionContext.INSTANCE.notationLibrary().register(notation.getKey(), notation.getValue().create(null));
            }

            // Set up all notation overrides from the KSML config
            for (final var notationEntry : ksmlConfig.notations().entrySet()) {
                final var notationStr = notationEntry.getKey() != null ? notationEntry.getKey() : "undefined";

                final var notationConfigs = new HashMap<String, String>();
                final var schemaRegistryName = notationEntry.getValue().schemaRegistry();
                if (schemaRegistryName != null) {
                    final var srConfigs = ksmlConfig.schemaRegistries().get(schemaRegistryName);
                    if (srConfigs != null && srConfigs.config() != null) {
                        notationConfigs.putAll(srConfigs.config());
                    } else {
                        log.warn("No schema registry configuration found for schema registry: {}", schemaRegistryName);
                    }
                }

                final var notationConfig = notationEntry.getValue();
                final var factoryName = notationConfig != null ? notationConfig.type() : "unknown";
                if (notationConfig != null && factoryName != null) {
                    final var factory = notationFactories.notations().get(factoryName);
                    if (factory == null) {
                        throw FatalError.report(new ConfigException("Unknown notation type: " + factoryName));
                    }
                    if (notationConfig.config() != null) notationConfigs.putAll(notationConfig.config());
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
                final var defaultAvro = new ConfluentAvroNotationProvider().createNotation(new NotationContext(AvroNotation.NOTATION_NAME, null, dataMapper, dataTypeMapper, config.getKafkaConfigMap()));
                ExecutionContext.INSTANCE.notationLibrary().register(AvroNotation.NOTATION_NAME, defaultAvro);
                log.warn("No notations configured. Loading default Avro notation with Confluent implementation. If you use AVRO in your KSML definition, please explicitly configure notations in the ksml-runner.yaml.");
            }

            final var errorHandling = ksmlConfig.errorHandlingConfig();
            if (errorHandling != null) {
                ExecutionContext.INSTANCE.errorHandling().setConsumeHandler(getErrorHandler(errorHandling.consumerErrorHandlingConfig()));
                ExecutionContext.INSTANCE.errorHandling().setProduceHandler(getErrorHandler(errorHandling.producerErrorHandlingConfig()));
                ExecutionContext.INSTANCE.errorHandling().setProcessHandler(getErrorHandler(errorHandling.processErrorHandlingConfig()));
            }
            ExecutionContext.INSTANCE.serdeWrapper(
                    serde -> new Serdes.WrapperSerde<>(
                            new ResolvingSerializer<>(serde.serializer(), config.getKafkaConfigMap()),
                            new ResolvingDeserializer<>(serde.deserializer(), config.getKafkaConfigMap())));

            final Map<String, TopologyDefinition> producerDefinitions = new HashMap<>();
            final Map<String, TopologyDefinition> pipelineDefinitions = new HashMap<>();
            definitions.forEach((name, definition) -> {
                final var parser = new TopologyDefinitionParser(name);
                final var topologyDefinition = parser.parse(ParseNode.fromRoot(definition, name));
                if (!topologyDefinition.producers().isEmpty()) producerDefinitions.put(name, topologyDefinition);
                if (!topologyDefinition.pipelines().isEmpty()) pipelineDefinitions.put(name, topologyDefinition);
            });

            if (!ksmlConfig.enableProducers() && !producerDefinitions.isEmpty()) {
                log.warn("Producers are disabled for this runner. The supplied producer specifications will be ignored.");
                producerDefinitions.clear();
            }
            if (!ksmlConfig.enablePipelines() && !pipelineDefinitions.isEmpty()) {
                log.warn("Pipelines are disabled for this runner. The supplied pipeline specifications will be ignored.");
                pipelineDefinitions.clear();
            }

            final var producer = producerDefinitions.isEmpty() ? null : new KafkaProducerRunner(
                    KafkaProducerRunner.Config.builder()
                            .definitions(producerDefinitions)
                            .kafkaConfig(config.getKafkaConfigMap())
                            .pythonContextConfig(ksmlConfig.pythonContextConfig())
                            .build()
            );
            final var streams = pipelineDefinitions.isEmpty() ? null : new KafkaStreamsRunner(KafkaStreamsRunner.Config.builder()
                    .storageDirectory(ksmlConfig.storageDirectory())
                    .appServer(ksmlConfig.applicationServerConfig())
                    .definitions(pipelineDefinitions)
                    .kafkaConfig(config.getKafkaConfigMap())
                    .pythonContextConfig(ksmlConfig.pythonContextConfig())
                    .build());

            if (producer != null || streams != null) {
                var shutdownHook = new Thread(() -> {
                    try {
                        log.debug("In KSML shutdown hook");
                        if (producer != null) producer.stop();
                        if (streams != null) streams.stop();
                    } catch (Exception e) {
                        log.error("Could not properly shut down KSML", e);
                    }
                });

                Runtime.getRuntime().addShutdownHook(shutdownHook);

                try (var prometheusExport = new PrometheusExport(config.getKsmlConfig().prometheusConfig())) {
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

                        producerFuture.whenComplete((result, exc) -> {
                            if (exc != null) {
                                log.info("Producer failed", exc);
                                // Exception, always stop streams too
                                if (streams != null) {
                                    streams.stop();
                                }
                            }
                        });
                        streamsFuture.whenComplete((result, exc) -> {
                            if (exc != null) {
                                log.info("Stream processing failed", exc);
                                // Exception, always stop producer too
                                if (producer != null) {
                                    producer.stop();
                                }
                            }
                        });

                        final var allFutures = CompletableFuture.allOf(producerFuture, streamsFuture);
                        // wait for all futures to finish
                        allFutures.join();

                        closeExecutorService(executorService);
                    } finally {
                        if (restServer != null) restServer.close();
                    }
                } finally {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                }
            }
        } catch (Throwable t) {
            log.error("KSML Stopping because of unhandled exception");
            throw FatalError.report(t);
        }
        // Explicit exit, need to find out which threads are actually stopping us
        System.exit(0);
    }

    private static void closeExecutorService(final ExecutorService executorService) throws ExecutionException {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
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

    private static void printRunnerSchema(String filename) {
        JacksonModule moduleJackson = new JacksonModule(
                JacksonOption.RESPECT_JSONPROPERTY_REQUIRED,
                JacksonOption.ALWAYS_REF_SUBTYPES,
                JacksonOption.FLATTENED_ENUMS_FROM_JSONPROPERTY);
        JakartaValidationModule moduleJakarta = new JakartaValidationModule(
                JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS
        );

        SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(
                SchemaVersion.DRAFT_2019_09, OptionPreset.PLAIN_JSON)
                .with(moduleJackson)
                .with(moduleJakarta)
                .with(Option.MAP_VALUES_AS_ADDITIONAL_PROPERTIES, Option.DEFINITIONS_FOR_ALL_OBJECTS, Option.FORBIDDEN_ADDITIONAL_PROPERTIES_BY_DEFAULT);

        // Instantiate add providers/resolved to allow the KsmlFileOrDefinition and StringMap to be processed with correct Schema types
        final var stringMapResolver = new StringMapDefinitionPropertiesResolver();
        configBuilder.forFields()
                .withAdditionalPropertiesResolver(stringMapResolver);
        configBuilder.forTypesInGeneral()
                // Add support for the KsmlFileOrDefinition
                .withCustomDefinitionProvider(stringMapResolver)
                .withCustomDefinitionProvider(new KsmlFileOrDefinitionProvider())
                .withSubtypeResolver(new KsmlFileOrDefinitionSubTypeResolver())
        ;

        // Construct the real schema
        SchemaGeneratorConfig config = configBuilder.build();
        SchemaGenerator generator = new SchemaGenerator(config);
        JsonNode jsonSchema = generator.generateSchema(KSMLRunnerConfig.class);
        String schema = jsonSchema.toPrettyString();

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
                // Ignore
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

    private static KSMLRunnerConfig readConfiguration(File configFile) {
        final var mapper = new ObjectMapper(new YAMLFactory());
        try {
            final var config = mapper.readValue(configFile, KSMLRunnerConfig.class);
            if (config != null) return config;
        } catch (IOException e) {
            log.error("Configuration exception", e);
        }
        throw new ConfigException("No configuration found");
    }

    private static ErrorHandler getErrorHandler(ErrorHandlingConfig.ErrorTypeHandlingConfig config) {
        final var handlerType = switch (config.handler()) {
            case CONTINUE -> ErrorHandler.HandlerType.CONTINUE_ON_FAIL;
            case STOP -> ErrorHandler.HandlerType.STOP_ON_FAIL;
            case RETRY -> ErrorHandler.HandlerType.RETRY_ON_FAIL;
        };
        return new ErrorHandler(
                config.log(),
                config.loggerName(),
                config.logPayload(),
                handlerType);
    }
}
