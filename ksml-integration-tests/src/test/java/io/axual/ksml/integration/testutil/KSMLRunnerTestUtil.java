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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
     * Configuration for schema registry setup.
     */
    public record SchemaRegistryConfig(String oldApicurioRegistryUrl, String newApicurioRegistryUrl) {
        public String getOldApicurioRegistryUrl() { return oldApicurioRegistryUrl; }
        public String getNewApicurioRegistryUrl() { return newApicurioRegistryUrl; }
    }

    /**
     * Prepares the test environment for schema registry tests by copying necessary files
     * and updating configurations.
     *
     * @param tempDir The temporary directory to use for test files
     * @param resourceBasePath The base path in test resources where KSML files are located
     * @param commonFiles Array of common filenames to copy from the resource directory
     * @param kafkaBootstrapServers The Kafka bootstrap servers to use
     * @param schemaRegistrySubdir Subdirectory containing schema registry specific ksml-runner.yaml
     * @param schemaRegistryConfig Configuration for schema registry URL replacement
     * @return The path to the prepared ksml-runner.yaml config file
     */
    public static Path prepareTestEnvironment(Path tempDir, String resourceBasePath, String[] commonFiles,
                                            String kafkaBootstrapServers, String schemaRegistrySubdir,
                                            SchemaRegistryConfig schemaRegistryConfig) throws IOException {
        // Create state directory
        Path stateDir = tempDir.resolve("state");
        Files.createDirectories(stateDir);

        // Copy common KSML files from resources to temp directory
        for (String fileName : commonFiles) {
            final var resource = KSMLRunnerTestUtil.class.getResource(resourceBasePath + "/" + fileName);
            if (resource == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceBasePath + "/" + fileName);
            }
            Path sourcePath = Path.of(resource.getPath());
            Path targetPath = tempDir.resolve(fileName);
            Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Copied {} to {}", fileName, targetPath);
        }

        // Handle ksml-runner.yaml - either from subdirectory or common files
        Path configPath = tempDir.resolve("ksml-runner.yaml");
        if (schemaRegistrySubdir != null) {
            // Copy schema registry specific ksml-runner.yaml from subdirectory
            final var schemaRegistryResource = KSMLRunnerTestUtil.class.getResource(
                resourceBasePath + "/" + schemaRegistrySubdir + "/ksml-runner.yaml");
            if (schemaRegistryResource == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceBasePath + "/" + schemaRegistrySubdir + "/ksml-runner.yaml");
            }
            Path schemaRegistryRunnerSource = Path.of(schemaRegistryResource.getPath());
            Files.copy(schemaRegistryRunnerSource, configPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Copied schema registry specific ksml-runner.yaml from {}", schemaRegistrySubdir);
        } else if (!Files.exists(configPath)) {
            // If no subdirectory and ksml-runner.yaml wasn't in commonFiles, look for it in resourceBasePath
            final var defaultRunnerResource = KSMLRunnerTestUtil.class.getResource(
                resourceBasePath + "/ksml-runner.yaml");
            if (defaultRunnerResource == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceBasePath + "/ksml-runner.yaml");
            }
            Path defaultRunnerSource = Path.of(defaultRunnerResource.getPath());
            Files.copy(defaultRunnerSource, configPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Copied default ksml-runner.yaml from resourceBasePath");
        }

        // Update ksml-runner.yaml with local paths and configuration
        String configContent = Files.readString(configPath);

        // Update storage directory
        configContent = configContent.replace("storageDirectory: /ksml/state",
                                            "storageDirectory: " + stateDir);

        // Update Kafka bootstrap servers
        configContent = configContent.replace("bootstrap.servers: broker:9093",
                                            "bootstrap.servers: " + kafkaBootstrapServers);

        // Update schema registry URL if provided
        if (schemaRegistryConfig != null) {
            configContent = configContent.replace(schemaRegistryConfig.getOldApicurioRegistryUrl(),
                                                schemaRegistryConfig.getNewApicurioRegistryUrl());
        }

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
     * Waits for messages to be produced to a Kafka topic by checking partition offsets using AdminClient.
     * This is more efficient than fixed sleep times and provides actual verification that messages exist.
     *
     * @param kafkaBootstrapServers The Kafka bootstrap servers
     * @param topicName The topic to check for messages
     * @param minMessages Minimum number of messages to wait for
     * @param timeout Maximum time to wait
     * @throws Exception if timeout is reached or AdminClient operations fail
     */
    public static void waitForTopicMessages(String kafkaBootstrapServers, String topicName,
                                          int minMessages, Duration timeout) throws Exception {
        log.info("Waiting for at least {} messages in topic {} (timeout: {}s)",
                minMessages, topicName, timeout.toSeconds());

        Properties adminProps = new Properties();
        adminProps.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        try (org.apache.kafka.clients.admin.AdminClient adminClient = org.apache.kafka.clients.admin.AdminClient.create(adminProps)) {
            long startTime = System.currentTimeMillis();
            long timeoutMs = timeout.toMillis();

            while (System.currentTimeMillis() - startTime < timeoutMs) {
                try {
                    // Get partition info for the topic
                    org.apache.kafka.clients.admin.DescribeTopicsResult topicsResult =
                        adminClient.describeTopics(java.util.Collections.singletonList(topicName));
                    org.apache.kafka.clients.admin.TopicDescription topicDesc =
                        topicsResult.topicNameValues().get(topicName).get();

                    // Get high watermarks (message counts) for all partitions
                    Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> partitionOffsetSpecs = new HashMap<>();
                    for (TopicPartitionInfo partition : topicDesc.partitions()) {
                        org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition(topicName, partition.partition());
                        partitionOffsetSpecs.put(tp, org.apache.kafka.clients.admin.OffsetSpec.latest());
                    }

                    org.apache.kafka.clients.admin.ListOffsetsResult offsetsResult = adminClient.listOffsets(partitionOffsetSpecs);

                    long totalMessages = 0;
                    for (Map.Entry<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> entry :
                         offsetsResult.all().get().entrySet()) {
                        totalMessages += entry.getValue().offset();
                    }

                    if (totalMessages >= minMessages) {
                        long elapsedMs = System.currentTimeMillis() - startTime;
                        log.info("Found {} messages in topic {} after {}ms (expected >= {})",
                                totalMessages, topicName, elapsedMs, minMessages);
                        return;
                    }

                    log.debug("Found {} messages in topic {}, waiting for {} more...",
                             totalMessages, topicName, minMessages - totalMessages);

                } catch (Exception e) {
                    log.debug("Error checking topic messages: {}", e.getMessage());
                }

                Thread.sleep(1000); // Check every second
            }

            throw new RuntimeException("Timeout waiting for " + minMessages + " messages in topic " + topicName +
                                     " after " + timeout.toSeconds() + " seconds");
        }
    }

    /**
     * Checks if the schema registry is ready by making an HTTP GET request to the APIs endpoint.
     *
     * @param registryUrl The base URL of the schema registry (e.g., {@code http://localhost:8081})
     * @return true if the registry is ready, false otherwise
     */
    public static boolean isSchemaRegistryReady(String registryUrl) {
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build()) {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(registryUrl + "/apis"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() >= 200 && response.statusCode() < 300;

        } catch (Exception e) {
            log.debug("Schema registry not ready: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Registers a JSON schema with Apicurio Schema Registry using the Confluent-compatible API.
     *
     * @param registryUrl The base URL of the schema registry (e.g., {@code http://localhost:8081})
     * @param subjectName The subject name for the schema (e.g., {@code "topic-name-value"})
     * @param schemaContent The JSON schema content as a string
     * @throws IOException if the HTTP request fails
     * @throws InterruptedException if the request is interrupted
     */
    public static void registerJsonSchema(String registryUrl, String subjectName, String schemaContent)
            throws IOException, InterruptedException {

        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build()) {

            // Create the JSON payload using ObjectMapper for proper JSON construction
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> payload = new HashMap<>();
            payload.put("schema", schemaContent);
            payload.put("schemaType", "JSON");

            String jsonPayload = mapper.writeValueAsString(payload);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(registryUrl + "/apis/ccompat/v7/subjects/" + subjectName + "/versions?normalize=true"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                log.info("Successfully registered JSON schema for subject: {}", subjectName);
                log.debug("Response: {}", response.body());
            } else {
                String errorMsg = String.format("Failed to register schema for subject %s. Status: %d, Response: %s",
                        subjectName, response.statusCode(), response.body());
                log.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
    }

    /**
     * Waits for the schema registry to be ready by polling the readiness endpoint.
     *
     * @param registryUrl The base URL of the schema registry
     * @param timeout Maximum time to wait for the registry to be ready
     * @throws InterruptedException if interrupted while waiting
     */
    public static void waitForSchemaRegistryReady(String registryUrl, Duration timeout) throws InterruptedException {
        log.info("Waiting for schema registry to be ready at: {}", registryUrl);

        long startTime = System.currentTimeMillis();
        long timeoutMs = timeout.toMillis();

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (isSchemaRegistryReady(registryUrl)) {
                log.info("Schema registry is ready");
                return;
            }

            Thread.sleep(2000); // Check every 2 seconds
        }

        throw new RuntimeException("Schema registry did not become ready within " + timeout.toSeconds() + " seconds");
    }
}