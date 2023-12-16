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
import io.axual.ksml.client.serde.ResolvingDeserializer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.notation.avro.AvroNotation;
import io.axual.ksml.notation.avro.AvroSchemaLoader;
import io.axual.ksml.notation.binary.BinaryNotation;
import io.axual.ksml.notation.csv.CsvDataObjectConverter;
import io.axual.ksml.notation.csv.CsvNotation;
import io.axual.ksml.notation.csv.CsvSchemaLoader;
import io.axual.ksml.notation.json.JsonDataObjectConverter;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.notation.json.JsonSchemaLoader;
import io.axual.ksml.notation.json.JsonSchemaMapper;
import io.axual.ksml.notation.soap.SOAPDataObjectConverter;
import io.axual.ksml.notation.soap.SOAPNotation;
import io.axual.ksml.notation.xml.XmlDataObjectConverter;
import io.axual.ksml.notation.xml.XmlNotation;
import io.axual.ksml.notation.xml.XmlSchemaLoader;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.rest.server.RestServer;
import io.axual.ksml.runner.backend.KafkaProducerRunner;
import io.axual.ksml.runner.backend.KafkaStreamsRunner;
import io.axual.ksml.runner.backend.Runner;
import io.axual.ksml.runner.config.KSMLErrorHandlingConfig;
import io.axual.ksml.runner.config.KSMLRunnerConfig;
import io.axual.ksml.runner.exception.ConfigException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.state.HostInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

@Slf4j
public class KSMLRunner {
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-runner.yaml";

    public static void main(String[] args) {
        try {
            // Load name and version from manifest
            var executableName = "KSML Runner";
            var executableVersion = "";
            try {
                ClassLoader cl = KSMLRunner.class.getClassLoader();

                try (InputStream url = cl.getResourceAsStream("META-INF/MANIFEST.MF")) {
                    Manifest manifest = new Manifest(url);
                    Attributes attr = manifest.getMainAttributes();
                    String attrName = attr.getValue("Implementation-Title");
                    if (attrName != null) {
                        executableName = attrName;
                    }

                    String attrVersion = attr.getValue("Implementation-Version");
                    if (attrVersion != null) {
                        executableVersion = attrVersion;
                    }
                }
            } catch (IOException e) {
                log.info("Could not load manifest file, using default values");
            }

            log.info("Starting {} {}", executableName, executableVersion);
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

            // Set up the notation library with all known notations
            NotationLibrary.register(AvroNotation.NOTATION_NAME, new AvroNotation(config.getKafkaConfig()), null);
            NotationLibrary.register(BinaryNotation.NOTATION_NAME, new BinaryNotation(), null);
            NotationLibrary.register(CsvNotation.NOTATION_NAME, new CsvNotation(), new CsvDataObjectConverter());
            NotationLibrary.register(JsonNotation.NOTATION_NAME, new JsonNotation(), new JsonDataObjectConverter());
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
                final var schema = new JsonSchemaMapper().fromDataSchema((DataSchema) parser.schema());
                try {
                    final var writer = new PrintWriter(config.getKsmlConfig().getConfigDirectory() + "/ksml.json");
                    writer.println(schema);
                    writer.close();
                } catch (Exception e) {
                    // Ignore
                }
                log.info("Schema: {}", schema);
                final var specification = parser.parse(YamlNode.fromRoot(definition, name));
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
                try {
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
                                    throw FatalError.reportAndExit(new KSMLExecutionException("Exception caught while shutting down", e));
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
            System.exit(2);
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
