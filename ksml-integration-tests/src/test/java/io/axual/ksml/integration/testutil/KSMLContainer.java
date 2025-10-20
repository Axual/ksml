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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil.KSMLRunnerWrapper;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil.SchemaRegistryConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * TestContainers-compatible wrapper for KSML that provides a fluent API for configuring
 * and running KSML integration tests. This class encapsulates the complexity of setting up
 * KSML with various configurations (Kafka, schema registries, files, etc.) and provides
 * a clean, declarative API similar to other TestContainers modules.
 *
 * <p>Example usage:
 * <pre>
 * &#64;Container
 * static KSMLContainer ksml = new KSMLContainer()
 *     .withKsmlFiles("/path/to/resources", "producer.yaml", "processor.yaml")
 *     .withKafka(kafka)
 *     .withApicurioRegistry(schemaRegistry)
 *     .dependsOn(kafka, schemaRegistry);
 * </pre>
 */
@Slf4j
public class KSMLContainer implements Startable {

    @FunctionalInterface
    public interface SetupCallback {
        void setup(KSMLContainer container) throws Exception;
    }

    private final List<String> ksmlFiles = new ArrayList<>();
    private String resourceBasePath;
    private String schemaRegistrySubdirectory;
    private KafkaContainer kafka;
    private GenericContainer<?> schemaRegistry;
    private SchemaRegistryType schemaRegistryType;
    private Path tempDirectory;
    private KSMLRunnerWrapper runnerWrapper;
    private final Map<String, String> systemProperties = new HashMap<>();
    private final List<Startable> dependencies = new ArrayList<>();
    private final List<String> topicsToCreate = new ArrayList<>();
    private int topicPartitionCount = 1; // Default to 1 partition
    private SetupCallback setupCallback;

    public enum SchemaRegistryType {
        APICURIO_AVRO("apicurio_avro",
                     "apicurio.registry.url: http://schema-registry:8081/apis/registry/v2",
                     "apicurio.registry.url: http://localhost:%d/apis/registry/v2"),
        CONFLUENT_AVRO("confluent_avro",
                      "schema.registry.url: http://schema-registry:8081/apis/ccompat/v7",
                      "schema.registry.url: http://localhost:%d/apis/ccompat/v7"),
        APICURIO_JSON("apicurio_jsonschema",
                     "apicurio.registry.url: http://schema-registry:8081/apis/registry/v2",
                     "apicurio.registry.url: http://localhost:%d/apis/registry/v2");

        final String subdirectory;
        final String oldUrlPattern;
        final String newUrlPattern;

        SchemaRegistryType(String subdirectory, String oldUrlPattern, String newUrlPattern) {
            this.subdirectory = subdirectory;
            this.oldUrlPattern = oldUrlPattern;
            this.newUrlPattern = newUrlPattern;
        }
    }

    /**
     * Specifies the KSML files to use for this test.
     *
     * @param resourceBasePath Base path in test resources (e.g., "/docs-examples/beginner-tutorial/avro")
     * @param files List of file names to copy (e.g., "producer.yaml", "processor.yaml")
     * @return this container for method chaining
     */
    public KSMLContainer withKsmlFiles(String resourceBasePath, String... files) {
        this.resourceBasePath = resourceBasePath;
        this.ksmlFiles.clear();
        Collections.addAll(this.ksmlFiles, files);
        return this;
    }

    /**
     * Configures this KSML container to use the specified Kafka container.
     *
     * @param kafka The Kafka TestContainer to connect to
     * @return this container for method chaining
     */
    public KSMLContainer withKafka(KafkaContainer kafka) {
        this.kafka = kafka;
        return this;
    }

    /**
     * Configures this KSML container to use Apicurio Schema Registry with AVRO format.
     *
     * @param schemaRegistry The Apicurio schema registry TestContainer
     * @return this container for method chaining
     */
    public KSMLContainer withApicurioAvroRegistry(GenericContainer<?> schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
        this.schemaRegistryType = SchemaRegistryType.APICURIO_AVRO;
        return this;
    }

    /**
     * Configures this KSML container to use Apicurio Schema Registry with JSON format.
     *
     * @param schemaRegistry The Apicurio schema registry TestContainer
     * @return this container for method chaining
     */
    public KSMLContainer withApicurioJsonRegistry(GenericContainer<?> schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
        this.schemaRegistryType = SchemaRegistryType.APICURIO_JSON;
        return this;
    }

    /**
     * Configures this KSML container to use Confluent-compatible Schema Registry with AVRO format.
     *
     * @param schemaRegistry The Confluent schema registry TestContainer
     * @return this container for method chaining
     */
    public KSMLContainer withConfluentAvroRegistry(GenericContainer<?> schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
        this.schemaRegistryType = SchemaRegistryType.CONFLUENT_AVRO;
        return this;
    }

    /**
     * Specifies Kafka topics that should be created before KSML starts.
     * This ensures topics exist when KSML tries to access them.
     *
     * @param topics Topic names to create
     * @return this container for method chaining
     */
    public KSMLContainer withTopics(String... topics) {
        Collections.addAll(this.topicsToCreate, topics);
        return this;
    }

    /**
     * Specifies the number of partitions to use when creating topics.
     * This applies to all topics created via {@link #withTopics(String...)}.
     * Default is 1 partition if not specified.
     *
     * @param partitionCount Number of partitions for topics
     * @return this container for method chaining
     */
    public KSMLContainer withPartitions(int partitionCount) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("Partition count must be at least 1");
        }
        this.topicPartitionCount = partitionCount;
        return this;
    }

    /**
     * Specifies a setup callback that will be executed before KSML starts.
     * This is useful for tasks like schema registration that must happen
     * before KSML begins processing.
     *
     * @param callback Setup callback to execute
     * @return this container for method chaining
     */
    public KSMLContainer withSetupCallback(SetupCallback callback) {
        this.setupCallback = callback;
        return this;
    }

    /**
     * Declares that this KSML container depends on other containers.
     * This is used by TestContainers to manage startup order.
     *
     * @param dependencies Containers that must start before KSML
     * @return this container for method chaining
     */
    public KSMLContainer dependsOn(Startable... dependencies) {
        Collections.addAll(this.dependencies, dependencies);
        return this;
    }

    @Override
    public void start() {
        try {
            // Ensure dependencies are started first
            for (Startable dependency : dependencies) {
                log.debug("Starting dependency: {}", dependency.getClass().getSimpleName());
                dependency.start();
            }

            // Validate configuration
            validateConfiguration();

            // Create Kafka topics before starting KSML
            if (!topicsToCreate.isEmpty()) {
                createKafkaTopics();
            }

            // Execute setup callback before starting KSML
            if (setupCallback != null) {
                log.info("Executing setup callback before KSML start");
                setupCallback.setup(this);
            }

            // Ensure temp directory exists
            if (tempDirectory == null) {
                tempDirectory = Files.createTempDirectory("ksml-test-");
                log.debug("Created temporary directory: {}", tempDirectory);
            }

            // Prepare test environment using existing utility
            SchemaRegistryConfig registryConfig = null;
            if (schemaRegistry != null && schemaRegistryType != null) {
                final int mappedPort = schemaRegistry.getMappedPort(8081);
                final String newUrl = String.format(schemaRegistryType.newUrlPattern, mappedPort);
                registryConfig = new SchemaRegistryConfig(
                    schemaRegistryType.oldUrlPattern,
                    newUrl
                );
                schemaRegistrySubdirectory = schemaRegistryType.subdirectory;
            }

            final Path configPath = KSMLRunnerTestUtil.prepareTestEnvironment(
                tempDirectory,
                resourceBasePath,
                ksmlFiles.toArray(new String[0]),
                kafka.getBootstrapServers(),
                schemaRegistrySubdirectory,
                registryConfig
            );

            log.info("Starting KSML with config: {}", configPath);
            if (schemaRegistry != null) {
                log.info("Schema Registry: {}",
                    String.format(schemaRegistryType.newUrlPattern, schemaRegistry.getMappedPort(8081)));
            }

            // Create and start KSML runner
            runnerWrapper = new KSMLRunnerWrapper(configPath, systemProperties);
            runnerWrapper.start();

            // Wait for KSML to be ready
            runnerWrapper.waitForReady(30000);

            log.info("KSML container started successfully");

        } catch (Exception e) {
            log.error("Failed to start KSML container", e);
            throw new RuntimeException("Failed to start KSML container", e);
        }
    }

    @Override
    public void stop() {
        if (runnerWrapper != null) {
            log.info("Stopping KSML container");
            runnerWrapper.stop();
            runnerWrapper = null;
        }
    }

    public boolean isRunning() {
        return runnerWrapper != null && runnerWrapper.isRunning();
    }

    private void validateConfiguration() {
        if (resourceBasePath == null || ksmlFiles.isEmpty()) {
            throw new IllegalStateException("KSML files not configured. Call withKsmlFiles() first.");
        }
        if (kafka == null) {
            throw new IllegalStateException("Kafka container not configured. Call withKafka() first.");
        }
    }

    private void createKafkaTopics() {
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

            try (AdminClient adminClient = AdminClient.create(props)) {
                List<NewTopic> topics = new ArrayList<>();
                for (String topicName : topicsToCreate) {
                    topics.add(new NewTopic(topicName, topicPartitionCount, (short) 1));
                }

                adminClient.createTopics(topics).all().get();
                log.info("Created Kafka topics: {} with {} partitions", topicsToCreate, topicPartitionCount);
            }
        } catch (Exception e) {
            log.error("Failed to create Kafka topics", e);
            throw new RuntimeException("Failed to create Kafka topics", e);
        }
    }
}
