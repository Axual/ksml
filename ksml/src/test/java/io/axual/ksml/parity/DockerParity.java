package io.axual.ksml.parity;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.awaitility.core.ConditionTimeoutException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.awaitility.Awaitility.await;

/**
 * Runs a parity case on real Kafka with Docker Compose ({@code parity-docker/docker-compose.yml}). The KSML side
 * runs the case definition in the KSML runner image, the Kafka Streams side runs {@link ReferenceApp} on the same
 * image. Each side has its own broker, so both use the same topic names and application id.
 * <p>
 * All input is written before the apps start, record caches are off and the commit interval is short, so the
 * output of a single-partition topic is deterministic. Stores cannot be read here; the comparison covers the
 * output topics, the internal topics on each broker, and whether each app keeps running.
 */
@Slf4j
final class DockerParity {
    private static final String IMAGE = System.getProperty("parity.image", "axual/ksml:local");
    private static final Path COMPOSE_FILE = Paths.get("..", "parity-docker", "docker-compose.yml").toAbsolutePath().normalize();
    private static final Path RUN_DIR = Paths.get("target", "parity-docker-run").toAbsolutePath();
    private static final Path LOG_DIR = Paths.get("target", "parity-report-docker").toAbsolutePath();
    private static final String KSML_BOOTSTRAP = "localhost:19092";
    private static final String REF_BOOTSTRAP = "localhost:29092";
    private static final long START_TIMEOUT_MS = 150_000;
    private static final long DONE_TIMEOUT_MS = 90_000;
    private static final long QUIET_MS = 4_000;
    private static boolean brokersUp = false;

    private DockerParity() {
    }

    /**
     * The Kafka Streams settings used on both sides. The KSML runner gets the same values in its runner config.
     */
    static Properties streamsConfig(String appId, String bootstrap, String stateDir, Map<String, String> caseConfig) {
        final var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.putAll(sharedStreamsConfig());
        // The KSML runner uses "all" unless told otherwise (KafkaStreamsRunner.getStreamsConfig)
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.putAll(caseConfig);
        return props;
    }

    private static Map<String, String> sharedStreamsConfig() {
        final var result = new TreeMap<String, String>();
        result.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        result.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        result.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "1000");
        result.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return result;
    }

    static synchronized ParityComparison run(ParityCase parityCase) {
        final var ref = new SideResult("kafka-streams");
        final var ksml = new SideResult("ksml");
        final var appId = "parity-" + parityCase.id.toLowerCase() + "-" + (System.currentTimeMillis() % 1_000_000);
        final var env = new HashMap<String, String>();
        try {
            ensureBrokers();
            final var caller = callingTest();
            env.put("REF_CLASS", caller[0]);
            env.put("REF_METHOD", caller[1]);
            env.put("APP_ID", appId);
            resetTopics(parityCase);
            prepareRunDir(parityCase, appId);

            final var restartAt = parityCase.restartAfterStep >= 0 ? parityCase.restartAfterStep : parityCase.steps.size();
            produce(parityCase, 0, restartAt);
            compose(env, "up", "-d", "--force-recreate", "ksml", "reference");
            waitUntilDone(env, parityCase, appId, ref, ksml);
            if (parityCase.restartAfterStep >= 0) {
                if (streamsProtocol(parityCase)) {
                    ksml.notes.add("streams group before the restart: " + streamsGroupSummary(KSML_BOOTSTRAP, appId));
                    ref.notes.add("streams group before the restart: " + streamsGroupSummary(REF_BOOTSTRAP, appId));
                }
                compose(env, "stop", "-t", "20", "ksml", "reference");
                ksml.notes.add("both apps were stopped and started again after input step " + restartAt + " (state folders kept)");
                produce(parityCase, restartAt, parityCase.steps.size());
                compose(env, "start", "ksml", "reference");
                waitUntilDone(env, parityCase, appId, ref, ksml);
                if (streamsProtocol(parityCase)) {
                    ksml.notes.add("streams group after the restart: " + streamsGroupSummary(KSML_BOOTSTRAP, appId));
                    ref.notes.add("streams group after the restart: " + streamsGroupSummary(REF_BOOTSTRAP, appId));
                }
            } else if (streamsProtocol(parityCase)) {
                ksml.notes.add("streams group: " + streamsGroupSummary(KSML_BOOTSTRAP, appId));
                ref.notes.add("streams group: " + streamsGroupSummary(REF_BOOTSTRAP, appId));
            }
            collect(parityCase, KSML_BOOTSTRAP, ksml);
            collect(parityCase, REF_BOOTSTRAP, ref);
            checkApp(env, "ksml", ksml);
            checkApp(env, "reference", ref);
        } catch (Exception e) {
            ksml.harnessError = ParityRunner.describe(e);
        } finally {
            saveLogs(parityCase, env);
            try {
                compose(env, "rm", "-s", "-f", "ksml", "reference");
            } catch (Exception e) {
                log.warn("Could not remove the app containers", e);
            }
        }
        return ParityComparison.compare(parityCase, "docker", ref, ksml);
    }

    private static void ensureBrokers() throws IOException, InterruptedException {
        if (brokersUp) return;
        compose(Map.of(), "up", "-d", "--wait", "broker-ksml", "broker-ref");
        brokersUp = true;
    }

    private static String[] callingTest() {
        for (final var frame : Thread.currentThread().getStackTrace()) {
            if (frame.getClassName().endsWith("ParityTest")) return new String[]{frame.getClassName(), frame.getMethodName()};
        }
        throw new IllegalStateException("Docker mode must be started from a *ParityTest method");
    }

    private static String compose(Map<String, String> env, String... args) throws IOException, InterruptedException {
        final var command = new ArrayList<>(List.of("docker", "compose", "-f", COMPOSE_FILE.toString(), "--profile", "apps"));
        command.addAll(List.of(args));
        final var builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().put("KSML_IMAGE", IMAGE);
        builder.environment().put("PARITY_RUN_DIR", RUN_DIR.toString());
        builder.environment().putAll(env);
        final var process = builder.start();
        final var output = new String(process.getInputStream().readAllBytes(), UTF_8);
        if (!process.waitFor(5, TimeUnit.MINUTES) || process.exitValue() != 0) {
            throw new IOException("docker compose " + String.join(" ", args) + " failed: " + output);
        }
        return output;
    }

    private static Admin admin(String bootstrap) {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    private static void resetTopics(ParityCase parityCase) throws Exception {
        final var topics = new ArrayList<String>();
        topics.addAll(parityCase.inputs.keySet());
        topics.addAll(parityCase.outputs.keySet());
        for (final var bootstrap : List.of(KSML_BOOTSTRAP, REF_BOOTSTRAP)) {
            try (final var admin = admin(bootstrap)) {
                final var existing = admin.listTopics().names().get();
                if (!existing.isEmpty()) admin.deleteTopics(existing).all().get();
                waitUntil(Duration.ofSeconds(60), Duration.ofMillis(500), () -> admin.listTopics().names().get().isEmpty());
                // A topic that is still being deleted cannot be created yet, so retry with the topics that are missing
                await().atMost(Duration.ofSeconds(15)).pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(500)).ignoreExceptions()
                        .until(() -> {
                            final var present = admin.listTopics().names().get();
                            final var missing = topics.stream()
                                    .filter(t -> !present.contains(t))
                                    .map(t -> new NewTopic(t, parityCase.partitions.getOrDefault(t, 1), (short) 1)
                                            .configs(Map.of("retention.ms", "-1")))
                                    .toList();
                            if (!missing.isEmpty()) admin.createTopics(missing).all().get();
                            return admin.listTopics().names().get().containsAll(topics);
                        });
                // Wait until every partition of the new topics has a leader, so the first writes are not lost
                waitUntil(Duration.ofSeconds(30), Duration.ofMillis(300), () -> admin.describeTopics(topics).allTopicNames().get().values().stream()
                        .allMatch(d -> d.partitions().stream().allMatch(p -> p.leader() != null && !p.leader().isEmpty())));
            }
        }
    }

    private static void prepareRunDir(ParityCase parityCase, String appId) throws Exception {
        deleteRecursively(RUN_DIR);
        // The containers run as another user, so they must be able to write their state here
        for (final var dir : List.of("ksml", "ref", "extra")) {
            Files.createDirectories(RUN_DIR.resolve(dir));
            Files.setPosixFilePermissions(RUN_DIR.resolve(dir), PosixFilePermissions.fromString("rwxrwxrwx"));
        }
        final var definition = Files.readString(Paths.get(ClassLoader.getSystemResource(parityCase.ksmlResource).toURI()));
        Files.writeString(RUN_DIR.resolve("ksml").resolve("definition.yaml"), definition);
        final var config = new StringBuilder()
                .append("ksml:\n")
                .append("  definitions:\n")
                .append("    parity: definition.yaml\n")
                .append("  storageDirectory: /ksml/state\n")
                .append("  createStorageDirectory: true\n")
                .append("kafka:\n")
                .append("  bootstrap.servers: broker-ksml:9093\n")
                .append("  application.id: ").append(appId).append('\n');
        sharedStreamsConfig().forEach((k, v) -> config.append("  ").append(k).append(": \"").append(v).append("\"\n"));
        parityCase.config.forEach((k, v) -> config.append("  ").append(k).append(": \"").append(v).append("\"\n"));
        Files.writeString(RUN_DIR.resolve("ksml").resolve("ksml-runner.yaml"), config.toString());
        // The reference app calls the test method, which needs these on its class path
        for (final var entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            final var name = Paths.get(entry).getFileName().toString();
            if (name.startsWith("assertj-core") || name.startsWith("junit-jupiter-api") || name.startsWith("opentest4j")
                    || name.startsWith("junit-platform-commons") || name.startsWith("apiguardian")
                    || name.startsWith("awaitility") || name.startsWith("hamcrest")) {
                Files.copy(Paths.get(entry), RUN_DIR.resolve("extra").resolve(name));
            }
        }
    }

    private static void produce(ParityCase parityCase, int from, int to) throws Exception {
        if (from >= to) return;
        final var expected = new HashMap<String, Long>();
        for (var i = 0; i < to; i++) {
            if (parityCase.steps.get(i) instanceof ParityCase.Pipe pipe) expected.merge(pipe.topic(), 1L, Long::sum);
        }
        for (final var bootstrap : List.of(KSML_BOOTSTRAP, REF_BOOTSTRAP)) {
            final var props = Map.<String, Object>of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ProducerConfig.ACKS_CONFIG, "all",
                    ProducerConfig.LINGER_MS_CONFIG, 0,
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
            try (final var producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
                for (var i = from; i < to; i++) {
                    if (parityCase.steps.get(i) instanceof ParityCase.Pipe pipe) {
                        final var format = parityCase.inputs.getOrDefault(pipe.topic(), new ParityCase.TopicFormat(Format.STRING, Format.JSON));
                        final var headers = new RecordHeaders();
                        pipe.headers().forEach((k, v) -> headers.add(k, v.getBytes(UTF_8)));
                        // Wait for each write, so the order is kept and a failed write is not missed
                        producer.send(new ProducerRecord<>(pipe.topic(), null, pipe.timestamp(),
                                format.key().encode(pipe.key()), format.value().encode(pipe.value()), headers)).get();
                    }
                }
            }
            try (final var admin = admin(bootstrap)) {
                for (final var topic : expected.entrySet()) {
                    final var written = endOffsets(admin, Set.of(topic.getKey())).values().stream().mapToLong(Long::longValue).sum();
                    if (written != topic.getValue()) {
                        throw new IllegalStateException("topic " + topic.getKey() + " on " + bootstrap + " has " + written
                                + " records, expected " + topic.getValue());
                    }
                }
            }
        }
    }

    /**
     * Waits until both apps have joined their group, have committed all input, and their output is quiet.
     */
    private static void waitUntilDone(Map<String, String> env, ParityCase parityCase, String appId, SideResult ref, SideResult ksml) throws Exception {
        final var sides = Map.of(KSML_BOOTSTRAP, ksml, REF_BOOTSTRAP, ref);
        final var services = Map.of(KSML_BOOTSTRAP, "ksml", REF_BOOTSTRAP, "reference");
        for (final var entry : sides.entrySet()) {
            try (final var admin = admin(entry.getKey())) {
                final var progress = new Progress(System.currentTimeMillis());
                // isDone always ends by itself through the start and done timeouts; atMost is only a safety net
                await().atMost(Duration.ofMillis(START_TIMEOUT_MS + DONE_TIMEOUT_MS + 60_000))
                        .pollDelay(Duration.ZERO).pollInterval(Duration.ofSeconds(1))
                        .until(() -> isDone(env, parityCase, appId, admin, services.get(entry.getKey()), entry.getValue(), progress));
            }
        }
    }

    /**
     * What {@link #isDone} remembers between two polls of one side.
     */
    private static final class Progress {
        private final long start;
        private long lastOutput = -1L;
        private long quietSince;
        private long startedAt = -1L;

        private Progress(long start) {
            this.start = start;
            this.quietSince = start;
        }
    }

    /**
     * One poll of one side: true when the app is done, has stopped or failed, or did not start or finish in time.
     */
    private static boolean isDone(Map<String, String> env, ParityCase parityCase, String appId, Admin admin, String service,
                                  SideResult side, Progress progress) throws Exception {
        final var now = System.currentTimeMillis();
        final var stable = isStable(admin, appId, streamsProtocol(parityCase));
        if (stable && progress.startedAt < 0) progress.startedAt = now;
        final var committed = stable
                ? committedOffsets(admin, appId, streamsProtocol(parityCase))
                : Map.<TopicPartition, OffsetAndMetadata>of();
        // Input topics and the internal topics the app reads (repartition, foreign-key subscription)
        final var readTopics = new HashSet<>(parityCase.inputs.keySet());
        readTopics.removeAll(parityCase.globalTopics);
        committed.keySet().forEach(tp -> readTopics.add(tp.topic()));
        admin.listTopics().names().get().stream()
                .filter(t -> t.startsWith(appId + "-") && (t.endsWith("-repartition") || t.endsWith("-topic")))
                .forEach(readTopics::add);
        final var readEnds = endOffsets(admin, readTopics);
        final var allConsumed = readEnds.entrySet().stream().allMatch(e -> e.getValue() == 0
                || (committed.get(e.getKey()) != null && committed.get(e.getKey()).offset() >= e.getValue()));
        final var output = endOffsets(admin, parityCase.outputs.keySet()).values().stream().mapToLong(Long::longValue).sum();
        if (output != progress.lastOutput) {
            progress.lastOutput = output;
            progress.quietSince = now;
        }
        if (progress.startedAt > 0 && allConsumed && now - progress.quietSince >= QUIET_MS) return true;
        if (!compose(env, "ps", "--format", "{{.State}}", service).contains("running")) return true;
        if (streamsFailed(compose(env, "logs", "--no-color", "--tail", "300", service))) return true;
        if (progress.startedAt < 0 && now - progress.start > START_TIMEOUT_MS) {
            side.notes.add("the app did not join its consumer group within " + START_TIMEOUT_MS / 1000 + "s");
            return true;
        }
        if (progress.startedAt > 0 && now - progress.startedAt > DONE_TIMEOUT_MS) {
            side.notes.add("the app did not consume all input within " + DONE_TIMEOUT_MS / 1000 + "s");
            return true;
        }
        return false;
    }

    /**
     * Polls until the condition is true or the time is up. A timeout is not an error here: the next step shows what
     * is missing.
     */
    private static void waitUntil(Duration atMost, Duration interval, Callable<Boolean> condition) {
        try {
            await().atMost(atMost).pollDelay(Duration.ZERO).pollInterval(interval).ignoreExceptions().until(condition);
        } catch (ConditionTimeoutException _) {
            // Go on
        }
    }

    /**
     * True when the log shows that Kafka Streams went to its ERROR state, even if the process is still running.
     */
    private static boolean streamsFailed(String logs) {
        return logs.contains("to new state 'ERROR'") || logs.contains("REFERENCE STATE PENDING_ERROR -> ERROR");
    }

    /**
     * True when the case runs both apps with the streams rebalance protocol, so the group is a streams group.
     */
    private static boolean streamsProtocol(ParityCase parityCase) {
        return "streams".equalsIgnoreCase(parityCase.config.get("group.protocol"));
    }

    private static boolean isStable(Admin admin, String appId, boolean streamsGroup) throws InterruptedException {
        try {
            if (streamsGroup) {
                final var group = admin.describeStreamsGroups(List.of(appId)).all().get().get(appId);
                return group.groupState() == GroupState.STABLE && !group.members().isEmpty();
            }
            final var group = admin.describeConsumerGroups(List.of(appId)).all().get().get(appId);
            return group.groupState() == GroupState.STABLE && !group.members().isEmpty();
        } catch (ExecutionException _) {
            // Kafka 4.x reports a group that has not been created yet as an error
            return false;
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> committedOffsets(Admin admin, String appId, boolean streamsGroup) throws Exception {
        if (streamsGroup) {
            return admin.listStreamsGroupOffsets(Map.of(appId, new ListStreamsGroupOffsetsSpec())).partitionsToOffsetAndMetadata(appId).get();
        }
        return admin.listConsumerGroupOffsets(appId).partitionsToOffsetAndMetadata().get();
    }

    /**
     * Streams groups only: the group epoch and, per member, the instance id and member epoch. Used as evidence for
     * static membership across a restart.
     */
    private static String streamsGroupSummary(String bootstrap, String appId) {
        try (final var admin = admin(bootstrap)) {
            final var group = admin.describeStreamsGroups(List.of(appId)).all().get().get(appId);
            final var members = group.members().stream()
                    .map(m -> "instanceId=" + m.instanceId().orElse("-") + " memberEpoch=" + m.memberEpoch())
                    .sorted().toList();
            return "state=" + group.groupState() + " groupEpoch=" + group.groupEpoch() + " members=" + members;
        } catch (Exception e) {
            return "could not describe the streams group: " + ParityRunner.describe(e);
        }
    }

    private static Map<TopicPartition, Long> endOffsets(Admin admin, Set<String> topics) throws Exception {
        final var description = admin.describeTopics(topics).allTopicNames().get();
        final var request = new HashMap<TopicPartition, OffsetSpec>();
        description.forEach((topic, d) -> d.partitions().forEach(p -> request.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest())));
        return admin.listOffsets(request).all().get().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private static void collect(ParityCase parityCase, String bootstrap, SideResult side) throws Exception {
        final Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (final var consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
             final var admin = admin(bootstrap)) {
            for (final var output : parityCase.outputs.entrySet()) {
                final var partitions = consumer.partitionsFor(output.getKey()).stream()
                        .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
                final var multiPartition = partitions.size() > 1;
                consumer.assign(partitions);
                consumer.seekToBeginning(partitions);
                final var ends = consumer.endOffsets(partitions);
                final var byPartition = new TreeMap<Integer, List<SideResult.Rec>>();
                final var deadline = System.currentTimeMillis() + 30_000;
                while (partitions.stream().anyMatch(p -> consumer.position(p) < ends.get(p)) && System.currentTimeMillis() < deadline) {
                    for (final var rec : consumer.poll(Duration.ofMillis(500))) {
                        final var headers = new TreeMap<String, String>();
                        rec.headers().forEach(h -> headers.put(h.key(), h.value() == null ? null : new String(h.value(), UTF_8)));
                        final var format = output.getValue();
                        byPartition.computeIfAbsent(rec.partition(), p -> new ArrayList<>()).add(new SideResult.Rec(
                                format.key().decode(rec.key()), format.value().decode(rec.value()), rec.timestamp(),
                                headers, format.value().raw(rec.value()), multiPartition ? rec.partition() : -1));
                    }
                }
                final var records = new ArrayList<SideResult.Rec>();
                byPartition.values().forEach(records::addAll);
                side.records.put(output.getKey(), records);
            }
            side.producedTopics.addAll(admin.listTopics().names().get());
        }
    }

    private static void checkApp(Map<String, String> env, String service, SideResult side) throws Exception {
        final var state = compose(env, "ps", "-a", "--format", "{{.Service}} {{.State}} {{.ExitCode}}", service).trim();
        final var logs = compose(env, "logs", "--no-color", service);
        final var errorLines = logs.lines()
                .filter(l -> l.contains(" ERROR ") || l.contains("REFERENCE ERROR") || l.contains("Exception:"))
                .filter(l -> !l.contains("GET_TELEMETRY_SUBSCRIPTIONS"))
                .map(l -> l.length() > 400 ? l.substring(0, 400) + "..." : l)
                .distinct().limit(5).toList();
        if (!state.contains("running")) {
            side.processingError = "app stopped (" + state + "): " + (errorLines.isEmpty() ? "no error in the log" : errorLines.getFirst());
        } else if (streamsFailed(logs)) {
            side.processingError = "Kafka Streams went to state ERROR: " + (errorLines.isEmpty() ? "no error in the log" : errorLines.getFirst());
            side.notes.add("the process was still running after Kafka Streams went to state ERROR");
        } else {
            errorLines.forEach(l -> side.notes.add("error in the log while still running: " + l));
        }
    }

    private static void saveLogs(ParityCase parityCase, Map<String, String> env) {
        try {
            Files.createDirectories(LOG_DIR);
            Files.writeString(LOG_DIR.resolve(parityCase.id + "-ksml.log"), compose(env, "logs", "--no-color", "ksml"));
            Files.writeString(LOG_DIR.resolve(parityCase.id + "-reference.log"), compose(env, "logs", "--no-color", "reference"));
        } catch (Exception e) {
            log.warn("Could not save the app logs for {}", parityCase.id, e);
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (final var paths = Files.walk(dir)) {
            for (final var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }
}
