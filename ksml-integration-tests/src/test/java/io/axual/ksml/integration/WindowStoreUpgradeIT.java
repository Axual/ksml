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
import io.axual.ksml.store.StoreUpgrade;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Window store state written by KSML 1.x reads correctly after the upgrade, from local state and after a restore
 * from the changelog. KSML 1.x runs as a container and writes two window stores from Python;
 * this version then runs on the same state directory and topics and reads them back from Python, once with the local
 * state and once without it.
 * <p>
 * KSML 1.x is stopped with SIGTERM, as in production. Its shutdown hook doesn't wait for Kafka Streams to close, so
 * the local state is what RocksDB had written to disk at that moment. KSML 1.1.0 (Kafka Streams 4.0) has written the
 * stores and a checkpoint on commit by then; KSML 1.3.2 (Kafka Streams 4.3) has not written them to disk.
 * <p>
 * A store that leaves out {@code logging} had no changelog in KSML 1.x. This version stops on its local state until
 * the definition sets {@code logging}, and with {@code logging: false} reads the state that KSML 1.x left.
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
        final var stateDirectory = runKsml1(ksml1Version, bootstrapServers, windowStart, UnaryOperator.identity(), List.of("window_timestamped", "window_unset"));
        // The store without timestamped was a plain store in KSML 1.x, which Kafka Streams converts itself
        final var plainStoreDirectory = stateDirectory.resolve(applicationId()).resolve("0_0").resolve("window_unset");
        final var plainDatabasesBefore = rocksDbIdentities(plainStoreDirectory);

        // This version reads the local state written by KSML 1.x
        final var localStateResult = runThisVersionAndGet(bootstrapServers, windowStart);
        final var plainDatabasesAfter = rocksDbIdentities(plainStoreDirectory);

        // This version restores the stores from the changelog topics
        deleteRecursively(stateDirectory.resolve(applicationId()));
        final var restoredResult = runThisVersionAndGet(bootstrapServers, windowStart);

        // A second start of this version keeps the local state: the RocksDB databases are the same ones
        final var storeDirectory = stateDirectory.resolve(applicationId()).resolve("0_0").resolve("window_timestamped");
        final var databasesBefore = rocksDbIdentities(storeDirectory);
        final var restartResult = runThisVersionAndGet(bootstrapServers, windowStart);
        final var databasesAfter = rocksDbIdentities(storeDirectory);

        log.info("Window store upgrade from KSML {}: local state: {}, restored from changelog: {}, after a restart: {}",
                ksml1Version, localStateResult, restoredResult, restartResult);
        final var softly = new SoftAssertions();
        softly.assertThat(localStateResult).as("read from the local state of KSML " + ksml1Version).isEqualTo(EXPECTED);
        softly.assertThat(plainDatabasesAfter).as("RocksDB databases of the plain store after the upgrade").isEqualTo(plainDatabasesBefore);
        softly.assertThat(restoredResult).as("read after a restore from the changelog").isEqualTo(EXPECTED);
        softly.assertThat(restartResult).as("read after a restart of this version").isEqualTo(EXPECTED);
        softly.assertThat(databasesAfter).as("RocksDB databases after a restart of this version").isNotEmpty().isEqualTo(databasesBefore);
        softly.assertAll();
    }

    @Test
    void unloggedStoreFromKsml1StopsUntilLoggingIsSet() throws Exception {
        suffix = "110unlogged";
        final var bootstrapServers = SharedKsmlInfra.kafka().getBootstrapServers();
        createTopics(bootstrapServers);
        outputConsumer = outputConsumer(bootstrapServers);
        final long windowStart = System.currentTimeMillis() / 60_000 * 60_000;
        final UnaryOperator<String> unlogged = definition -> withPlainStoreLogging(definition, "");

        // KSML 1.1.0 writes the plain store without a changelog; the logged store makes it write its local state
        final var stateDirectory = runKsml1("1.1.0", bootstrapServers, windowStart, unlogged, List.of("window_timestamped"));
        final var applicationDirectory = stateDirectory.resolve(applicationId());
        final var plainStoreDirectory = applicationDirectory.resolve("0_0").resolve("window_unset");
        final var timestampedStoreDirectory = applicationDirectory.resolve("0_0").resolve("window_timestamped");
        final var plainDatabases = rocksDbIdentities(plainStoreDirectory);
        final var timestampedDatabases = rocksDbIdentities(timestampedStoreDirectory);

        // This version stops before Kafka Streams starts, and changes nothing
        prepareThisVersion(bootstrapServers, unlogged);
        runner = new KSMLRunnerWrapper(workDirectory.resolve(RUNNER_CONFIG), Map.of());
        runner.start();
        final var exitCode = runner.waitForExit(Duration.ofSeconds(60));
        // Does nothing when KSML stopped by itself; kills it when it did not, so the next run gets the state directory
        runner.stop();
        runner = null;
        // Taken before the run with logging: false below, which writes the marker and rebuilds the timestamped store
        final var plainDatabasesAfterStop = rocksDbIdentities(plainStoreDirectory);
        final var timestampedDatabasesAfterStop = rocksDbIdentities(timestampedStoreDirectory);
        final var markerAfterStop = Files.exists(applicationDirectory.resolve(StoreUpgrade.MARKER_FILE));

        // With logging: false it reads what KSML 1.1.0 left on disk, as there is no changelog
        final var result = runThisVersionAndGet(bootstrapServers, windowStart, definition -> withPlainStoreLogging(definition, "    logging: false\n"));

        final var softly = new SoftAssertions();
        // KSMLRunner returns 1 for every failure, so the checks below show that this was the stop that changes nothing
        softly.assertThat(exitCode).as("exit code of this version on the unlogged store").contains(1);
        softly.assertThat(plainDatabases).as("RocksDB databases of the unlogged store, written by KSML 1.1.0").isNotEmpty();
        softly.assertThat(timestampedDatabases).as("RocksDB databases of the timestamped store, written by KSML 1.1.0").isNotEmpty();
        softly.assertThat(plainDatabasesAfterStop).as("RocksDB databases of the unlogged store after the stop").isEqualTo(plainDatabases);
        softly.assertThat(timestampedDatabasesAfterStop).as("RocksDB databases of the timestamped store after the stop").isEqualTo(timestampedDatabases);
        softly.assertThat(markerAfterStop).as("marker file written after the stop").isFalse();
        softly.assertThat(result).as("read with logging: false").isEqualTo(EXPECTED);
        softly.assertAll();
    }

    /** Runs KSML 1.x, which writes both stores and reads them back, and stops it; returns the state directory. */
    private Path runKsml1(String ksml1Version, String bootstrapServers, long windowStart, UnaryOperator<String> definitionChange,
                          List<String> loggedStores) throws Exception {
        copyResources();
        changeDefinition(definitionChange);
        final var stateDirectory = workDirectory.resolve("state");
        Files.createDirectories(stateDirectory, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx")));
        // On Linux, the image user (1024) can't read the work directory, and the test can't read or delete the state
        // that it writes. So KSML 1.x runs as the user of the test, in group 0 like the image user, as the parity
        // Docker tests do with PARITY_USER. The image sets no HOME, and an unknown user gets "/".
        final var hostUser = Files.getAttribute(workDirectory, "unix:uid") + ":0";
        try (var ksml1 = new GenericContainer<>(DockerImageName.parse(KSML_1X_IMAGE + ksml1Version))
                .withCreateContainerCmdModifier(cmd -> cmd.withUser(hostUser))
                .withEnv("HOME", "/home/ksml")
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
            waitForState(bootstrapServers, ksml1Version, stateDirectory, records.size(), loggedStores);
            produceGet(bootstrapServers, windowStart);
            assertThat(readGetResult()).as("read by KSML " + ksml1Version).isEqualTo(EXPECTED);
            ksml1.getDockerClient().stopContainerCmd(ksml1.getContainerId()).withTimeout(60).exec();
        }
        return stateDirectory;
    }

    /** Replaces the logging line of the plain store {@code window_unset}. */
    private static String withPlainStoreLogging(String definition, String logging) {
        final var start = definition.indexOf("  window_unset:");
        return definition.substring(0, start) + definition.substring(start).replaceFirst("    logging: true\n", logging);
    }

    private void prepareThisVersion(String bootstrapServers, UnaryOperator<String> definitionChange) throws Exception {
        KSMLRunnerTestUtil.prepareTestEnvironment(workDirectory, RESOURCES, new String[]{RUNNER_CONFIG, DEFINITION}, bootstrapServers, null, null);
        renameForVersion();
        changeDefinition(definitionChange);
    }

    private void changeDefinition(UnaryOperator<String> change) throws IOException {
        final var definition = workDirectory.resolve(DEFINITION);
        Files.writeString(definition, change.apply(Files.readString(definition)));
    }

    /** Runs this version, reads key {@code a} from both stores and stops; {@code null} when there is no result. */
    private String runThisVersionAndGet(String bootstrapServers, long windowStart) throws Exception {
        return runThisVersionAndGet(bootstrapServers, windowStart, UnaryOperator.identity());
    }

    private String runThisVersionAndGet(String bootstrapServers, long windowStart, UnaryOperator<String> definitionChange) throws Exception {
        prepareThisVersion(bootstrapServers, definitionChange);
        final var membersBefore = groupMembers(bootstrapServers);
        runner = new KSMLRunnerWrapper(workDirectory.resolve(RUNNER_CONFIG), Map.of());
        runner.start();
        runner.waitForReady(30_000);
        waitForInputAssignment(bootstrapServers, membersBefore);
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

    /**
     * Waits until KSML 1.x has written every put to the changelogs of the logged stores. KSML 1.1.0 also writes its
     * local state and a checkpoint on commit; KSML 1.3.2 does not write them before it stops.
     */
    private void waitForState(String bootstrapServers, String ksml1Version, Path stateDirectory, int puts, List<String> loggedStores) {
        final var checkpoint = stateDirectory.resolve(applicationId()).resolve("0_0").resolve(".checkpoint");
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            Awaitility.await().atMost(PROCESSING_TIMEOUT).pollInterval(Duration.ofSeconds(1)).until(() -> {
                for (final var store : loggedStores) {
                    if (changelogEndOffset(admin, store) < puts) return false;
                }
                return !ksml1Version.equals("1.1.0") || Files.exists(checkpoint);
            });
        }
    }

    /** The member ids in the consumer group of the application, which KSML 1.x or an earlier run may have left. */
    private Set<String> groupMembers(String bootstrapServers) throws Exception {
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            return admin.describeConsumerGroups(List.of(applicationId())).all().get().get(applicationId()).members().stream()
                    .map(MemberDescription::consumerId)
                    .collect(Collectors.toSet());
        } catch (ExecutionException _) {
            // The group does not exist yet
            return Set.of();
        }
    }

    /**
     * Waits until the consumer group is stable without the members of earlier runs, and a new member has the input
     * partition. Kafka Streams does not leave the group when it stops, so an earlier member stays until its session
     * times out. While it has the partition, a get is not read in time, and when it drops out, the group rebalances,
     * which a stop of this version must not interrupt.
     */
    private void waitForInputAssignment(String bootstrapServers, Set<String> membersBefore) {
        final var input = new TopicPartition(topic("input"), 0);
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            Awaitility.await().atMost(PROCESSING_TIMEOUT).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().until(() -> {
                final var group = admin.describeConsumerGroups(List.of(applicationId())).all().get().get(applicationId());
                return group.groupState() == GroupState.STABLE
                        && group.members().stream().noneMatch(member -> membersBefore.contains(member.consumerId()))
                        && group.members().stream().anyMatch(member -> member.assignment().topicPartitions().contains(input));
            });
        }
    }

    /** The end offset of the changelog of a store, or 0 while the changelog topic does not exist. */
    private long changelogEndOffset(AdminClient admin, String store) throws InterruptedException {
        final var partition = new TopicPartition(applicationId() + "-" + store + "-changelog", 0);
        try {
            return admin.listOffsets(Map.of(partition, OffsetSpec.latest())).all().get().get(partition).offset();
        } catch (ExecutionException _) {
            return 0;
        }
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

    /** The IDENTITY files of the RocksDB databases of a store, which RocksDB creates with a new id for a new database. */
    private static List<String> rocksDbIdentities(Path storeDirectory) throws IOException {
        if (!Files.isDirectory(storeDirectory)) return List.of();
        try (var paths = Files.walk(storeDirectory)) {
            final var identities = new ArrayList<String>();
            for (final var path : paths.filter(path -> path.getFileName().toString().equals("IDENTITY")).sorted().toList()) {
                identities.add(storeDirectory.relativize(path) + "=" + Files.readString(path).trim());
            }
            return identities;
        }
    }

    private static void deleteRecursively(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            for (final var path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
