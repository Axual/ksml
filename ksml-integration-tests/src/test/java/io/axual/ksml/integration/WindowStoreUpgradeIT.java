package io.axual.ksml.integration;

/*-
 * ========================LICENSE_START=================================
 * KSML Integration Tests
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

import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil;
import io.axual.ksml.integration.testutil.KSMLRunnerTestUtil.KSMLRunnerWrapper;
import io.axual.ksml.integration.testutil.SharedKsmlInfra;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Window store state written by KSML 1.x reads correctly after the upgrade, from local state and after a restore
 * from the changelog (AXPD-12445, design D7). KSML 1.x runs as a container and writes two window stores from Python;
 * this version then runs on the same state directory and topics and reads them back from Python, once with the local
 * state and once without it.
 * <p>
 * KSML 1.x is stopped with SIGTERM, as in production. Its shutdown hook doesn't wait for Kafka Streams to close, so
 * the local state is what RocksDB had written to disk at that moment. KSML 1.1.0 (Kafka Streams 4.0) has written the
 * stores and a checkpoint on commit by then; KSML 1.3.2 (Kafka Streams 4.3) has not written them to disk.
 */
@Slf4j
class WindowStoreUpgradeIT {
    private static final String KSML_1X_IMAGE = "registry.axual.io/opensource/images/axual/ksml:";
    private static final String RESOURCES = "/window-store-upgrade";
    private static final String RUNNER_CONFIG = "ksml-runner.yaml";
    private static final String DEFINITION = "window-store-upgrade.yaml";
    private static final String APPLICATION_ID = "io.ksml.window.store.upgrade.test";
    private static final String TOPIC_PREFIX = "window_upgrade_";
    private static final Duration OUTPUT_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration PROCESSING_TIMEOUT = Duration.ofMinutes(3);
    // Enough records for a commit of KSML 1.1.0 to write a checkpoint (more than 10000 changelog records)
    private static final int FILLER_RECORDS = 10_100;
    private static final String EXPECTED = "timestamped=one unset=one";

    @TempDir
    Path workDirectory;

    // Each KSML 1.x version gets its own topics and application id, because the broker is shared
    private String suffix;
    private KSMLRunnerWrapper runner;
    private KafkaConsumer<String, String> outputConsumer;

    @AfterEach
    void cleanUp() {
        if (runner != null) runner.stop();
        if (outputConsumer != null) outputConsumer.close();
    }

    @ParameterizedTest(name = "from KSML {0}")
    @ValueSource(strings = {"1.1.0", "1.3.2"})
    void windowStoreStateFromKsml1ReadsCorrectly(String ksml1Version) throws Exception {
        suffix = ksml1Version.replace(".", "");
        final var bootstrapServers = SharedKsmlInfra.kafka().getBootstrapServers();
        createTopics(bootstrapServers);
        outputConsumer = outputConsumer(bootstrapServers);
        final long windowStart = System.currentTimeMillis() / 60_000 * 60_000;

        // KSML 1.x writes the stores and reads them back
        copyResources();
        final var stateDirectory = workDirectory.resolve("state");
        Files.createDirectories(stateDirectory, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx")));
        try (var ksml1 = new GenericContainer<>(DockerImageName.parse(KSML_1X_IMAGE + ksml1Version))
                .withNetwork(SharedKsmlInfra.network())
                .withFileSystemBind(workDirectory.toString(), "/ksml", BindMode.READ_WRITE)
                .withWorkingDirectory("/ksml")
                .withCommand("/ksml/" + RUNNER_CONFIG)
                .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("ksml-" + ksml1Version))
                .waitingFor(Wait.forLogMessage(".*to new state 'RUNNING'.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)))) {
            ksml1.start();
            final var records = new ArrayList<String[]>();
            records.add(new String[]{"a", "put:" + windowStart + ":one"});
            for (int i = 0; i < FILLER_RECORDS; i++) {
                records.add(new String[]{"filler" + i, "put:" + windowStart + ":" + i});
            }
            produce(bootstrapServers, records);
            waitForPuts(records.size());
            // Several commits (commit.interval.ms is 1000)
            Thread.sleep(5_000);
            produceGet(bootstrapServers, windowStart);
            assertThat(readGetResult()).as("read by KSML " + ksml1Version).isEqualTo(EXPECTED);
            ksml1.getDockerClient().stopContainerCmd(ksml1.getContainerId()).withTimeout(60).exec();
        }

        // This version reads the local state written by KSML 1.x
        final var localStateResult = runThisVersionAndGet(bootstrapServers, windowStart);

        // This version restores the stores from the changelog topics
        deleteRecursively(stateDirectory.resolve(applicationId()));
        final var restoredResult = runThisVersionAndGet(bootstrapServers, windowStart);

        log.info("Window store upgrade from KSML {}: local state: {}, restored from changelog: {}",
                ksml1Version, localStateResult, restoredResult);
        final var softly = new SoftAssertions();
        softly.assertThat(localStateResult).as("read from the local state of KSML " + ksml1Version).isEqualTo(EXPECTED);
        softly.assertThat(restoredResult).as("read after a restore from the changelog").isEqualTo(EXPECTED);
        softly.assertAll();
    }

    /** Runs this version, reads key {@code a} from both stores and stops; {@code null} when there is no result. */
    private String runThisVersionAndGet(String bootstrapServers, long windowStart) throws Exception {
        KSMLRunnerTestUtil.prepareTestEnvironment(workDirectory, RESOURCES, new String[]{RUNNER_CONFIG, DEFINITION}, bootstrapServers, null, null);
        renameForVersion();
        runner = new KSMLRunnerWrapper(workDirectory.resolve(RUNNER_CONFIG), Map.of());
        runner.start();
        runner.waitForReady(30_000);
        produceGet(bootstrapServers, windowStart);
        final var result = readGetResult();
        runner.stop();
        runner = null;
        return result;
    }

    private String applicationId() {
        return APPLICATION_ID + "." + suffix;
    }

    private String topic(String name) {
        return TOPIC_PREFIX + suffix + "_" + name;
    }

    private void copyResources() throws IOException {
        for (final var file : List.of(RUNNER_CONFIG, DEFINITION)) {
            try (var in = getClass().getResourceAsStream(RESOURCES + "/" + file)) {
                assertThat(in).as(file).isNotNull();
                Files.write(workDirectory.resolve(file), in.readAllBytes());
            }
        }
        renameForVersion();
    }

    /** Gives the copied resources the topic names and application id of this KSML 1.x version. */
    private void renameForVersion() throws IOException {
        final var config = workDirectory.resolve(RUNNER_CONFIG);
        Files.writeString(config, Files.readString(config).replace(APPLICATION_ID, applicationId()));
        final var definition = workDirectory.resolve(DEFINITION);
        Files.writeString(definition, Files.readString(definition).replace(TOPIC_PREFIX, TOPIC_PREFIX + suffix + "_"));
    }

    private void createTopics(String bootstrapServers) throws Exception {
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            final var existing = admin.listTopics().names().get();
            final var topics = Stream.of(topic("input"), topic("output"))
                    .filter(topic -> !existing.contains(topic))
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            admin.createTopics(topics).all().get();
        }
    }

    private void produceGet(String bootstrapServers, long windowStart) {
        produce(bootstrapServers, List.<String[]>of(new String[]{"a", "get:" + windowStart}));
    }

    /** Produces key/value pairs to the input topic, in order. */
    private void produce(String bootstrapServers, List<String[]> records) {
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            records.forEach(rec -> producer.send(new ProducerRecord<>(topic("input"), rec[0], rec[1])));
            producer.flush();
        }
    }

    private KafkaConsumer<String, String> outputConsumer(String bootstrapServers) {
        final var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        consumer.assign(List.of(new TopicPartition(topic("output"), 0)));
        return consumer;
    }

    /** Waits until the output topic has the results of the given number of puts. */
    private void waitForPuts(int count) {
        final long deadline = System.currentTimeMillis() + PROCESSING_TIMEOUT.toMillis();
        int puts = 0;
        while (puts < count && System.currentTimeMillis() < deadline) {
            puts += outputConsumer.poll(Duration.ofMillis(1000)).count();
        }
        assertThat(puts).as("records processed by KSML 1.x").isEqualTo(count);
    }

    /** Reads the output topic up to the next result of a get, skipping the results of puts; {@code null} on timeout. */
    private String readGetResult() {
        final long deadline = System.currentTimeMillis() + OUTPUT_TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (final var rec : outputConsumer.poll(Duration.ofMillis(500))) {
                if (!"put".equals(rec.value())) return rec.value();
            }
        }
        return null;
    }

    private static void deleteRecursively(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            for (final var path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
