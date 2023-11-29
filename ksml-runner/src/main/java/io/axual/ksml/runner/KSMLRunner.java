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
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.rest.server.RestServer;
import io.axual.ksml.runner.backend.KafkaProducerRunner;
import io.axual.ksml.runner.backend.KafkaStreamsRunner;
import io.axual.ksml.runner.backend.Runner;
import io.axual.ksml.runner.config.KSMLRunnerConfig;
import io.axual.ksml.runner.exception.ConfigException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.HostInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

@Slf4j
public class KSMLRunner {
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-runner.yaml";

    public static void main(String[] args) {
        // Load name and version from manifest
        String name = "KSML Runner";
        String version = "";
        try {
            ClassLoader cl = KSMLRunner.class.getClassLoader();

            try (InputStream url = cl.getResourceAsStream("META-INF/MANIFEST.MF")) {
                Manifest manifest = new Manifest(url);
                Attributes attr = manifest.getMainAttributes();
                String attrName = attr.getValue("Implementation-Title");
                if (attrName != null) {
                    name = attrName;
                }

                String attrVersion = attr.getValue("Implementation-Version");
                if (attrVersion != null) {
                    version = attrVersion;
                }
            }
        } catch (IOException e) {
            log.info("Could not load manifest file, using default values");
        }

        log.info("Starting {} {}", name, version);
        final var configFile = new File(args.length == 0 ? DEFAULT_CONFIG_FILE_SHORT : args[0]);
        if (!configFile.exists()) {
            log.error("Configuration file '{}' not found", configFile);
            System.exit(1);
        }

        final var config = readConfiguration(configFile);
        final var definitions = config.getKsmlConfig().getDefinitions();
        if (definitions == null || definitions.isEmpty()) {
            throw new ConfigException("definitions", definitions, "No KSML definitions found in configuration");
        }

        try (final var kafkaProducerBackend = new KafkaProducerRunner(config.getKsmlConfig(), config.getKafkaConfig());
             final var kafkaStreamsBackend = new KafkaStreamsRunner(config.getKsmlConfig(), config.getKafkaConfig())) {
            var shutdownHook = new Thread(() -> {
                try {
                    log.debug("In KSML shutdown hook");
                    kafkaStreamsBackend.close();
                    kafkaProducerBackend.close();
                } catch (Exception e) {
                    log.error("Could not properly close the KSML backend", e);
                }
            });

            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                final var appServer = config.getKsmlConfig().getApplicationServerConfig();
                if (appServer != null && appServer.isEnabled()) {
                    // Run with the REST server
                    HostInfo hostInfo = new HostInfo(appServer.getHost(), appServer.getPort());
                    try (RestServer restServer = new RestServer(hostInfo)) {
                        restServer.start(kafkaStreamsBackend.getQuerier());
                        run(kafkaStreamsBackend);
                    }
                } else {
                    // Run without the REST server
                    run(kafkaStreamsBackend);
                }
            } finally {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            }
        } catch (Exception e) {
            log.error("An exception occurred while running KSML", e);
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
            //Ignore
        }
        throw new ConfigException("No configuration found");
    }

    private static void run(Runner runner) {
        var executorService = Executors.newFixedThreadPool(5);
        Future<?> backendFuture = executorService.submit(runner);

        try {
            backendFuture.get();
            // Future, check exit state of backend
        } catch (ExecutionException | InterruptedException e) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e2) {
                executorService.shutdownNow();
                throw FatalError.reportAndExit(new KSMLExecutionException("Exception caught", e2));
            }
            throw FatalError.reportAndExit(new KSMLExecutionException("Exception caught", e));
        }
    }
}
