package io.axual.ksml.integration.testutil;

/*-
 * ========================LICENSE_START=================================
 * KSML Integration Tests
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.runner.KSMLRunner;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for running KSMLRunner directly in tests instead of using Docker containers.
 * This allows for faster test execution and easier debugging.
 */
@Slf4j
public class KSMLRunnerTestUtil {

    /**
     * A ProcessBuilder-based implementation that runs KSMLRunner in a separate JVM process.
     * This provides complete isolation and avoids classpath conflicts.
     */
    public static class KSMLRunnerWrapper {
        private final Path configPath;
        private final Map<String, String> systemProperties;
        private Process ksmlProcess;
        private volatile boolean isRunning = false;

        public KSMLRunnerWrapper(Path configPath) {
            this(configPath, new HashMap<>());
        }

        public KSMLRunnerWrapper(Path configPath, Map<String, String> systemProperties) {
            this.configPath = configPath;
            this.systemProperties = systemProperties;
        }

        /**
         * Starts the KSMLRunner in a separate JVM process.
         */
        public void start() {
            if (isRunning) {
                return;
            }

            try {
                // Build the command to run KSMLRunner
                List<String> command = new ArrayList<>();

                // Java executable
                String javaHome = System.getProperty("java.home");
                command.add(javaHome + "/bin/java");

                // JVM options and system properties
                systemProperties.forEach((key, value) -> command.add("-D" + key + "=" + value));

                // Classpath - use the same classpath as the current JVM
                String classpath = System.getProperty("java.class.path");
                command.add("-cp");
                command.add(classpath);

                // Main class
                command.add(KSMLRunner.class.getName());

                // Configuration file (relative to working directory)
                command.add(configPath.getFileName().toString());

                log.info("Starting KSMLRunner process with command: {}", String.join(" ", command));

                // Create ProcessBuilder with working directory
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.directory(configPath.getParent().toFile());

                // Inherit IO so we can see logs
                processBuilder.inheritIO();

                // Start the process
                ksmlProcess = processBuilder.start();
                isRunning = true;

                log.info("KSMLRunner process started with PID: {}", ksmlProcess.pid());

            } catch (IOException e) {
                log.error("Failed to start KSMLRunner process", e);
                throw new RuntimeException("Failed to start KSMLRunner process", e);
            }
        }

        /**
         * Stops the KSMLRunner process.
         */
        public void stop() {
            if (!isRunning || ksmlProcess == null) {
                return;
            }

            log.info("Stopping KSMLRunner process...");

            // Try graceful shutdown first
            ksmlProcess.destroy();

            try {
                // Wait up to 10 seconds for graceful shutdown
                boolean exited = ksmlProcess.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
                if (!exited) {
                    log.warn("KSMLRunner didn't exit gracefully, forcing termination");
                    ksmlProcess.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for KSMLRunner to stop");
                ksmlProcess.destroyForcibly();
            }

            isRunning = false;
            log.info("KSMLRunner process stopped");
        }

        /**
         * Checks if the KSMLRunner process is still running.
         */
        public boolean isRunning() {
            return isRunning && ksmlProcess != null && ksmlProcess.isAlive();
        }

        /**
         * Waits for the KSMLRunner to be ready (basic wait implementation).
         */
        public void waitForReady(long timeoutMillis) throws InterruptedException {
            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < timeoutMillis) {
                if (!isRunning()) {
                    throw new RuntimeException("KSMLRunner process stopped unexpectedly");
                }

                // Wait for a reasonable initialization time (5 seconds)
                if (System.currentTimeMillis() - startTime > 5000) {
                    log.info("KSMLRunner process should be ready now");
                    // Give it a bit more time to ensure everything is running
                    Thread.sleep(2000);
                    return;
                }

                Thread.sleep(1000);
            }

            throw new RuntimeException("KSMLRunner did not become ready within " + (timeoutMillis / 1000) + " seconds");
        }
    }

    /**
     * Prepares the test environment by copying necessary files to a temporary directory
     * and updating the configuration to work with the local setup.
     *
     * @param tempDir The temporary directory to use for test files
     * @param resourceBasePath The base path in test resources where KSML files are located
     * @param kafkaBootstrapServers The Kafka bootstrap servers to use
     * @return The path to the prepared ksml-runner.yaml config file
     */
    public static Path prepareTestEnvironment(Path tempDir, String resourceBasePath, String kafkaBootstrapServers) throws IOException {
        // Create state directory
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Copy all KSML files from resources to temp directory
        String[] filesToCopy = {"ksml-runner.yaml", "producer-xml.yaml", "processor-xml.yaml", "SensorData.xsd"};

        for (String fileName : filesToCopy) {
            Path sourcePath = Path.of(KSMLRunnerTestUtil.class.getResource(resourceBasePath + "/" + fileName).getPath());
            Path targetPath = tempDir.resolve(fileName);
            Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Copied {} to {}", fileName, targetPath);
        }

        // Update ksml-runner.yaml with local paths and Kafka configuration
        Path configPath = tempDir.resolve("ksml-runner.yaml");
        String configContent = Files.readString(configPath);

        // Keep relative paths since KSMLRunner will run from temp directory
        // configContent = configContent.replace("producer: producer-xml.yaml",
        //                                     "producer: " + tempDir.resolve("producer-xml.yaml"));
        // configContent = configContent.replace("processor: processor-xml.yaml",
        //                                     "processor: " + tempDir.resolve("processor-xml.yaml"));
        configContent = configContent.replace("storageDirectory: /ksml/state",
                                            "storageDirectory: " + stateDir);

        // Update Kafka bootstrap servers
        configContent = configContent.replace("bootstrap.servers: broker:9093",
                                            "bootstrap.servers: " + kafkaBootstrapServers);

        Files.writeString(configPath, configContent);
        log.info("Prepared test environment in {}", tempDir);

        return configPath;
    }

    /**
     * Prepares a custom test environment where you can specify custom KSML configurations.
     *
     * @param tempDir The temporary directory to use for test files
     * @param ksmlFiles Map of filename to file content for KSML files
     * @param kafkaBootstrapServers The Kafka bootstrap servers to use
     * @param applicationId The Kafka Streams application ID
     * @return The path to the prepared ksml-runner.yaml config file
     */
    public static Path prepareCustomTestEnvironment(
            Path tempDir,
            Map<String, String> ksmlFiles,
            String kafkaBootstrapServers,
            String applicationId) throws IOException {

        // Create state directory
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Write all KSML files to temp directory
        for (Map.Entry<String, String> entry : ksmlFiles.entrySet()) {
            Path filePath = tempDir.resolve(entry.getKey());
            Files.writeString(filePath, entry.getValue());
            log.debug("Created {} in temp directory", entry.getKey());
        }

        // Create ksml-runner.yaml configuration
        String runnerConfig = String.format("""
            ksml:
              definitions:
                producer: %s
                processor: %s
              storageDirectory: %s
              createStorageDirectory: true

            kafka:
              bootstrap.servers: %s
              application.id: %s
              security.protocol: PLAINTEXT
              acks: all
            """,
            tempDir.resolve("producer.yaml"),
            tempDir.resolve("processor.yaml"),
            stateDir,
            kafkaBootstrapServers,
            applicationId
        );

        Path configPath = tempDir.resolve("ksml-runner.yaml");
        Files.writeString(configPath, runnerConfig);

        log.info("Prepared custom test environment in {}", tempDir);
        return configPath;
    }
}