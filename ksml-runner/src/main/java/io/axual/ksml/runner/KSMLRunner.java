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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.state.HostInfo;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.axual.ksml.client.serde.ResolvingDeserializer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.mapper.DataTypeSchemaMapper;
import io.axual.ksml.data.mapper.KafkaStreamsDataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationLibrary;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.AvroSchemaLoader;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.csv.CsvDataObjectConverter;
import io.axual.ksml.data.notation.csv.CsvNotation;
import io.axual.ksml.data.notation.csv.CsvSchemaLoader;
import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.notation.json.JsonSchemaLoader;
import io.axual.ksml.data.notation.json.JsonSchemaMapper;
import io.axual.ksml.data.notation.soap.SOAPDataObjectConverter;
import io.axual.ksml.data.notation.soap.SOAPNotation;
import io.axual.ksml.data.notation.xml.XmlDataObjectConverter;
import io.axual.ksml.data.notation.xml.XmlNotation;
import io.axual.ksml.data.notation.xml.XmlSchemaLoader;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.schema.KafkaStreamsSchemaMapper;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.rest.server.RestServer;
import io.axual.ksml.runner.backend.KafkaProducerRunner;
import io.axual.ksml.runner.backend.KafkaStreamsRunner;
import io.axual.ksml.runner.backend.Runner;
import io.axual.ksml.runner.config.KSMLErrorHandlingConfig;
import io.axual.ksml.runner.config.KSMLRunnerConfig;
import io.axual.ksml.runner.exception.ConfigException;
import io.axual.ksml.runner.prometheus.PrometheusExport;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KSMLRunner {
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-runner.yaml";
    private static final String WRITE_KSML_SCHEMA_ARGUMENT = "--schema";

    public static void main(String[] args) {
        try {
            // Load name and version from manifest
            var ksmlTitle = determineTitle();

            // Check if we need to output the schema and then exit
            checkForSchemaOutput(args);

            log.info("Starting {}", ksmlTitle);

            // Begin loading config file
            final var configFile = new File(args.length == 0 ? DEFAULT_CONFIG_FILE_SHORT : args[0]);
            if (!configFile.exists()) {
                log.error("Configuration file '{}' not found", configFile);
                System.exit(1);
            }

            final var config = readConfiguration(configFile);
            final var ksmlConfig = config.getKsmlConfig();
            log.info("Using directories: config: {}, schema: {}, storage: {}", ksmlConfig.getConfigDirectory(), ksmlConfig.getSchemaDirectory(), ksmlConfig.getStorageDirectory());
            final var definitions = ksmlConfig.getDefinitions();
            if (definitions == null || definitions.isEmpty()) {
                throw new ConfigException("definitions", definitions, "No KSML definitions found in configuration");
            }

            KsmlInfo.registerKsmlAppInfo(config.getApplicationId());

            // Ensure that Kafka Streams specific types are correctly handled by the KSML data library
            DataTypeSchemaMapper.SUPPLIER(KafkaStreamsSchemaMapper::new);
            NativeDataObjectMapper.SUPPLIER(KafkaStreamsDataObjectMapper::new);

            // Set up the notation library with all known notations and type override classes
            final var jsonNotation = new JsonNotation();
            NotationLibrary.register(AvroNotation.NOTATION_NAME, new AvroNotation(config.getKafkaConfig()), null);
            NotationLibrary.register(BinaryNotation.NOTATION_NAME, new BinaryNotation(jsonNotation::serde), null);
            NotationLibrary.register(CsvNotation.NOTATION_NAME, new CsvNotation(), new CsvDataObjectConverter());
            NotationLibrary.register(JsonNotation.NOTATION_NAME, jsonNotation, new JsonDataObjectConverter());
            NotationLibrary.register(SOAPNotation.NOTATION_NAME, new SOAPNotation(), new SOAPDataObjectConverter());
            NotationLibrary.register(XmlNotation.NOTATION_NAME, new XmlNotation(), new XmlDataObjectConverter());

            // Register schema loaders
            final var schemaDirectory = ksmlConfig.getSchemaDirectory();
            SchemaLibrary.registerLoader(AvroNotation.NOTATION_NAME, new AvroSchemaLoader(schemaDirectory));
            SchemaLibrary.registerLoader(CsvNotation.NOTATION_NAME, new CsvSchemaLoader(schemaDirectory));
            SchemaLibrary.registerLoader(JsonNotation.NOTATION_NAME, new JsonSchemaLoader(schemaDirectory));
            SchemaLibrary.registerLoader(XmlNotation.NOTATION_NAME, new XmlSchemaLoader(schemaDirectory));

            final var errorHandling = ksmlConfig.getErrorHandlingConfig();
            if (errorHandling != null) {
                ExecutionContext.INSTANCE.setConsumeHandler(getErrorHandler(errorHandling.getConsume(), "ConsumeError"));
                ExecutionContext.INSTANCE.setProduceHandler(getErrorHandler(errorHandling.getProduce(), "ProduceError"));
                ExecutionContext.INSTANCE.setProcessHandler(getErrorHandler(errorHandling.getProcess(), "ProcessError"));
            }
            ExecutionContext.INSTANCE.setSerdeWrapper(
                    serde -> new Serdes.WrapperSerde<>(
                            new ResolvingSerializer<>(serde.serializer(), config.getKafkaConfig()),
                            new ResolvingDeserializer<>(serde.deserializer(), config.getKafkaConfig())));

            final Map<String, TopologyDefinition> producerSpecs = new HashMap<>();
            final Map<String, TopologyDefinition> pipelineSpecs = new HashMap<>();
            definitions.forEach((name, definition) -> {
                final var parser = new TopologyDefinitionParser(name);
                final var specification = parser.parse(ParseNode.fromRoot(definition, name));
                if (!specification.producers().isEmpty()) producerSpecs.put(name, specification);
                if (!specification.pipelines().isEmpty()) pipelineSpecs.put(name, specification);
            });

            final var producer = producerSpecs.isEmpty() ? null : new KafkaProducerRunner(KafkaProducerRunner.Config.builder()
                    .definitions(producerSpecs)
                    .kafkaConfig(config.getKafkaConfig())
                    .build());
            final var streams = pipelineSpecs.isEmpty() ? null : new KafkaStreamsRunner(KafkaStreamsRunner.Config.builder()
                    .storageDirectory(ksmlConfig.getStorageDirectory())
                    .appServer(ksmlConfig.getApplicationServerConfig())
                    .definitions(pipelineSpecs)
                    .kafkaConfig(config.getKafkaConfig())
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
                try (var prometheusExport = new PrometheusExport(config.getKsmlConfig().getPrometheusConfig())) {
                    prometheusExport.start();
                    final var executorService = Executors.newFixedThreadPool(2);
                    final var producerFuture = producer == null ? null : executorService.submit(producer);
                    final var streamsFuture = streams == null ? null : executorService.submit(streams);
                    RestServer restServer = null;

                    try {
                        // Allow the runner(s) to start
                        Utils.sleep(2000);

                        final var appServer = ksmlConfig.getApplicationServerConfig();
                        if (streamsFuture != null && appServer != null && appServer.isEnabled()) {
                            // Run with the REST server
                            HostInfo hostInfo = new HostInfo(appServer.getHost(), appServer.getPort());
                            restServer = new RestServer(hostInfo);
                            restServer.start(streams.getQuerier());
                        }

                        while (producerFuture == null || !producerFuture.isDone() || streamsFuture == null || !streamsFuture.isDone()) {
                            final var producerError = producer != null && producer.getState() == Runner.State.FAILED;
                            final var streamsError = streams != null && streams.getState() == Runner.State.FAILED;

                            // If either runner has an error, stop all runners
                            if (producerError || streamsError) {
                                if (producer != null) producer.stop();
                                if (streams != null) streams.stop();
                                if (producer != null) {
                                    try {
                                        producerFuture.get(5, TimeUnit.SECONDS);
                                    } catch (TimeoutException | ExecutionException | InterruptedException e) {
                                        // Ignore
                                    }
                                }
                                if (streams != null) {
                                    try {
                                        streamsFuture.get(5, TimeUnit.SECONDS);
                                    } catch (TimeoutException | ExecutionException | InterruptedException e) {
                                        // Ignore
                                    }
                                }
                                executorService.shutdown();
                                try {
                                    if (!executorService.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                                        executorService.shutdownNow();
                                    }
                                } catch (InterruptedException e) {
                                    executorService.shutdownNow();
                                    throw new ExecutionException("Exception caught while shutting down", e);
                                }
                                break;
                            }
                        }
                    } finally {
                        if (restServer != null) restServer.close();
                    }
                } finally {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                }
            }
        } catch (Throwable t) {
            log.error("Unhandled exception", t);
            throw FatalError.reportAndExit(t);
        }
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

    private static void checkForSchemaOutput(String[] args) {
        // Check if the runner was started with "--schema". If so, then we output the JSON schema to validate the
        // KSML definitions with on stdout and exit
        if (args.length >= 1 && WRITE_KSML_SCHEMA_ARGUMENT.equals(args[0])) {
            final var parser = new TopologyDefinitionParser("dummy");
            final var schema = new JsonSchemaMapper().fromDataSchema(parser.schema());

            final var filename = args.length >= 2 ? args[1] : null;
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
            System.exit(0);
        }
    }

    private static KSMLRunnerConfig readConfiguration(File configFile) {
        final var mapper = new ObjectMapper(new YAMLFactory());
        try {
            final var config = mapper.readValue(configFile, KSMLRunnerConfig.class);
            if (config != null) {
                if (config.getKsmlConfig() == null) {
                    throw new ConfigException("Section \"ksml\" is missing in configuration");
                }
                if (config.getKafkaConfig() == null) {
                    throw new ConfigException("Section \"kafka\" is missing in configuration");
                }
                return config;
            }
        } catch (IOException e) {
            log.error("Configuration exception", e);
        }
        throw new ConfigException("No configuration found");
    }

    private static ErrorHandler getErrorHandler(KSMLErrorHandlingConfig.ErrorHandlingConfig config, String
            loggerName) {
        if (config == null) return new ErrorHandler(true, false, loggerName, ErrorHandler.HandlerType.STOP_ON_FAIL);
        final var handlerType = switch (config.getHandler()) {
            case CONTINUE -> ErrorHandler.HandlerType.CONTINUE_ON_FAIL;
            case STOP -> ErrorHandler.HandlerType.STOP_ON_FAIL;
        };
        return new ErrorHandler(
                config.isLog(),
                config.isLogPayload(),
                config.getLoggerName() != null ? config.getLoggerName() : loggerName,
                handlerType);
    }
}
