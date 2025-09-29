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
import java.time.Duration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.SoftAssertions;
import io.axual.ksml.runner.KSMLRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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

                // Wait for a reasonable initialization time (3 seconds)
                if (System.currentTimeMillis() - startTime > 3000) {
                    log.info("KSMLRunner process should be ready now");
                    // Give it a bit more time to ensure everything is running
                    Thread.sleep(1000);
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
     * @param filesToCopy Array of filenames to copy from the resource directory
     * @param kafkaBootstrapServers The Kafka bootstrap servers to use
     * @return The path to the prepared ksml-runner.yaml config file
     */
    public static Path prepareTestEnvironment(Path tempDir, String resourceBasePath, String[] filesToCopy, String kafkaBootstrapServers) throws IOException {
        // Create state directory
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Copy all KSML files from resources to temp directory
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
     * Polls a Kafka consumer with retry logic to handle consumer group rebalancing delays.
     * This method addresses the issue where a single poll might return empty results
     * due to consumer group coordination/rebalancing taking 3-5 seconds.
     *
     * @param consumer The Kafka consumer to poll from
     * @param timeout The maximum duration to wait for records
     * @param <K> The key type
     * @param <V> The value type
     * @return ConsumerRecords containing the polled records, or empty if timeout reached
     */
    public static <K, V> ConsumerRecords<K, V> pollWithRetry(
            KafkaConsumer<K, V> consumer,
            Duration timeout) {

        long endTime = System.currentTimeMillis() + timeout.toMillis();
        ConsumerRecords<K, V> records = ConsumerRecords.empty();

        // Keep polling until we get records or timeout
        while (records.isEmpty() && System.currentTimeMillis() < endTime) {
            long remainingTime = endTime - System.currentTimeMillis();
            if (remainingTime <= 0) {
                break;
            }

            // Poll with shorter intervals to be more responsive
            Duration pollTimeout = Duration.ofMillis(Math.min(500, remainingTime));
            records = consumer.poll(pollTimeout);

            if (!records.isEmpty()) {
                log.debug("Received {} records after {} ms",
                        records.count(),
                        timeout.toMillis() - (endTime - System.currentTimeMillis()));
                break;
            }
        }

        if (records.isEmpty()) {
            log.warn("No records received after polling for {} ms", timeout.toMillis());
        }

        return records;
    }

    /**
     * Validates JSON sensor data structure using Jackson ObjectMapper and SoftAssertions.
     * This follows the same pattern used in JsonSchemaMapperTest for consistent JSON validation.
     *
     * @param jsonValue The JSON string to validate
     * @param softly SoftAssertions instance to collect all validation failures
     * @return JsonNode for additional custom validations
     */
    public static JsonNode validateSensorJsonStructure(String jsonValue, SoftAssertions softly) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(jsonValue);

            // Validate required sensor data fields exist and have correct types
            softAssertJsonField(softly, node, "/name", "name field");
            softAssertJsonField(softly, node, "/timestamp", "timestamp field");
            softAssertJsonField(softly, node, "/value", "value field");
            softAssertJsonField(softly, node, "/type", "type field");
            softAssertJsonField(softly, node, "/unit", "unit field");

            return node;

        } catch (JsonProcessingException e) {
            softly.fail("Invalid JSON structure: " + e.getMessage());
            return null;
        }
    }

    /**
     * Validates JSON sensor data structure for processed messages (with additional fields).
     */
    public static JsonNode validateProcessedSensorJsonStructure(String jsonValue, SoftAssertions softly) {
        JsonNode node = validateSensorJsonStructure(jsonValue, softly);
        if (node != null) {
            // Additional fields for processed messages
            softAssertJsonField(softly, node, "/processed_at", "processed_at field");
            softAssertJsonField(softly, node, "/sensor_id", "sensor_id field");
        }
        return node;
    }

    /**
     * Soft assertion for JSON field existence and non-null value.
     * Follows the same pattern as JsonSchemaMapperTest.
     */
    private static void softAssertJsonField(SoftAssertions softly, JsonNode rootNode, String jsonPointer, String fieldDescription) {
        softly.assertThatObject(rootNode.at(jsonPointer))
                .as("JSON path '%s' (%s) should exist and not be null", jsonPointer, fieldDescription)
                .returns(false, JsonNode::isMissingNode)
                .returns(false, JsonNode::isNull);
    }

    /**
     * Validates that JSON contains one of the expected enum values.
     * Uses JsonNode path access instead of string contains for better validation.
     */
    public static void softAssertJsonEnumField(SoftAssertions softly, JsonNode rootNode, String jsonPointer, String fieldDescription, String... validValues) {
        JsonNode fieldNode = rootNode.at(jsonPointer);
        softly.assertThatObject(fieldNode)
                .as("JSON path '%s' (%s) should exist and not be null", jsonPointer, fieldDescription)
                .returns(false, JsonNode::isMissingNode)
                .returns(false, JsonNode::isNull)
                .returns(true, JsonNode::isTextual);

        if (fieldNode.isTextual()) {
            String actualValue = fieldNode.asText();
            softly.assertThat(actualValue)
                    .as("%s should be one of the valid enum values", fieldDescription)
                    .isIn(validValues);
        }
    }
}