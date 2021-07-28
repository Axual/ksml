package io.axual.ksml.runner;

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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.axual.ksml.runner.backend.Backend;
import io.axual.ksml.runner.config.KSMLRunnerConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KSMLRunner {
    private static final String DEFAULT_CONFIG_FILE_SHORT = "ksml-runner.yml";

    public static void main(String[] args) {
        final var configPath = new File(args.length == 0 ? DEFAULT_CONFIG_FILE_SHORT : args[0]);
        if (!configPath.exists()) {
            log.error("Configuration file '{}' not found", configPath);
            System.exit(1);
        }
        Backend backend;
        try {
            final var mapper = new ObjectMapper(new YAMLFactory());
            final KSMLRunnerConfig config = mapper.readValue(configPath, KSMLRunnerConfig.class);
            config.validate();
            log.info("Using backed of type {}", config.getBackend().getType());
            backend = config.getConfiguredBackend();

        } catch (IOException e) {
            log.error("An exception occurred while reading the configuration", e);
            System.exit(2);
            // Return to uninitialized variable errors, should not be executed because of the exit;
            return;
        }

        var executorService = Executors.newFixedThreadPool(5);
        Future<?> backendFuture = executorService.submit(backend);

        try {
            backendFuture.get();
            // Future, check exit state of backend
        } catch (ExecutionException | InterruptedException e) {
            log.error("Caught exception while waiting for finish", e);
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }

    }
}
