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

import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.definition.parser.TopologyDefinitionParser;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.python.PythonContextConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.test.TestRecord;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Builds one side of a parity case and runs the case input through it on a fresh {@link TopologyTestDriver}.
 */
final class ParityRunner {
    static final String APP_ID = "parity";
    private static final ParityCase.TopicFormat DEFAULT_FORMAT = new ParityCase.TopicFormat(Format.STRING, Format.JSON);

    private ParityRunner() {
    }

    /**
     * A builder that sees the case's settings, like the KSML runner's builder does (settings such as
     * {@code dsl.store.format} are read when the topology is built, not when it runs).
     */
    static StreamsBuilder streamsBuilder(ParityCase parityCase, String optimization) {
        if (parityCase.config.isEmpty()) return new StreamsBuilder();
        final var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);
        props.putAll(parityCase.config);
        return new StreamsBuilder(new TopologyConfig(new StreamsConfig(props)));
    }

    static SideResult runReference(ParityCase parityCase, String optimization) {
        final var result = new SideResult("kafka-streams");
        result.printFile = parityCase.referencePrintFile;
        final Topology topology;
        try {
            final var builder = streamsBuilder(parityCase, optimization);
            parityCase.reference.accept(builder);
            final var props = new Properties();
            props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);
            props.putAll(parityCase.config);
            topology = builder.build(props);
        } catch (Throwable e) {
            result.buildError = describe(e);
            return result;
        }
        return execute(parityCase, optimization, topology, result);
    }

    static SideResult runKsml(ParityCase parityCase, String optimization) {
        final var result = new SideResult("ksml");
        result.printFile = parityCase.ksmlPrintFile;
        final Topology topology;
        try {
            final var uri = ClassLoader.getSystemResource(parityCase.ksmlResource).toURI();
            final var definition = YAMLObjectMapper.INSTANCE.readValue(Files.readString(Paths.get(uri)), JsonNode.class);
            final Map<String, TopologyDefinition> definitions = Map.of("definition",
                    new TopologyDefinitionParser("parity").parse(ParseNode.fromRoot(definition, parityCase.id)));
            final var generator = new TopologyGenerator(APP_ID, optimization, PythonContextConfig.builder().build());
            topology = generator.create(streamsBuilder(parityCase, optimization), definitions);
        } catch (Throwable e) {
            result.buildError = describe(e);
            return result;
        }
        return execute(parityCase, optimization, topology, result);
    }

    private static SideResult execute(ParityCase parityCase, String optimization, Topology topology, SideResult result) {
        result.topologyDescription = topology.describe().toString();
        result.shape = shape(topology.describe());
        Path stateDir = null;
        try {
            if (result.printFile != null) Files.deleteIfExists(Paths.get(result.printFile));
            stateDir = Files.createTempDirectory("parity-" + parityCase.id);
            final var props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);
            props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
            props.putAll(parityCase.config);
            try (final var driver = new TopologyTestDriver(topology, props, Instant.EPOCH)) {
                pipeAll(parityCase, driver, result);
                parityCase.outputs.forEach((topic, format) -> result.records.put(topic, read(driver, topic, format)));
                result.producedTopics.addAll(driver.producedTopicNames());
                for (final var store : parityCase.stores) {
                    result.stores.put(store, readStore(driver, store));
                }
            }
            if (result.printFile != null) {
                final var file = Paths.get(result.printFile);
                result.printed.addAll(Files.exists(file) ? Files.readAllLines(file) : List.of("<no file written>"));
            }
        } catch (Throwable e) {
            result.driverError = describe(e);
        } finally {
            delete(stateDir);
        }
        return result;
    }

    private static void pipeAll(ParityCase parityCase, TopologyTestDriver driver, SideResult result) {
        final var inputs = new HashMap<String, TestInputTopic<byte[], byte[]>>();
        var index = 0;
        for (final var step : parityCase.steps) {
            try {
                switch (step) {
                    case ParityCase.Pipe pipe -> {
                        final var format = parityCase.inputs.getOrDefault(pipe.topic(), DEFAULT_FORMAT);
                        final var topic = inputs.computeIfAbsent(pipe.topic(),
                                t -> driver.createInputTopic(t, new ByteArraySerializer(), new ByteArraySerializer()));
                        final var headers = new RecordHeaders();
                        pipe.headers().forEach((k, v) -> headers.add(k, v.getBytes(UTF_8)));
                        topic.pipeInput(new TestRecord<>(format.key().encode(pipe.key()), format.value().encode(pipe.value()),
                                headers, Instant.ofEpochMilli(pipe.timestamp())));
                    }
                    case ParityCase.WallClock wallClock -> driver.advanceWallClockTime(Duration.ofMillis(wallClock.millis()));
                }
            } catch (Throwable e) {
                result.processingError = "input #" + index + ": " + describe(e);
                return;
            }
            index++;
        }
    }

    private static List<SideResult.Rec> read(TopologyTestDriver driver, String topic, ParityCase.TopicFormat format) {
        final var result = new ArrayList<SideResult.Rec>();
        final var output = driver.createOutputTopic(topic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        for (final var rec : output.readRecordsToList()) {
            final var headers = new TreeMap<String, String>();
            for (final var header : rec.headers()) {
                headers.put(header.key(), header.value() == null ? null : new String(header.value(), UTF_8));
            }
            result.add(new SideResult.Rec(format.key().decode(rec.key()), format.value().decode(rec.value()),
                    rec.timestamp(), headers, format.value().raw(rec.value()), -1));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<String> readStore(TopologyTestDriver driver, String name) {
        final var entries = new ArrayList<String>();
        try {
            if (!driver.getAllStateStores().containsKey(name)) return List.of("<no store named " + name + ">");
            final var keyValueStore = driver.getKeyValueStore(name);
            final var windowStore = keyValueStore == null ? driver.getWindowStore(name) : null;
            final var sessionStore = keyValueStore == null && windowStore == null ? driver.getSessionStore(name) : null;
            KeyValueIterator<?, ?> iterator = null;
            if (keyValueStore != null) iterator = keyValueStore.all();
            else if (windowStore != null) iterator = windowStore.all();
            else if (sessionStore != null) iterator = sessionStore.findSessions(0L, Long.MAX_VALUE);
            if (iterator == null) return List.of("<unsupported store type " + driver.getAllStateStores().get(name).getClass().getSimpleName() + ">");
            try (final var it = (KeyValueIterator<Object, Object>) iterator) {
                while (it.hasNext()) {
                    final KeyValue<Object, Object> entry = it.next();
                    entries.add(Normalizer.render(storeValue(entry.key)) + " -> " + Normalizer.render(storeValue(entry.value)));
                }
            }
        } catch (Throwable e) {
            entries.add("<error reading store: " + describe(e) + ">");
        }
        entries.sort(Comparator.naturalOrder());
        return entries;
    }

    /**
     * The reference stores JSON as text, KSML stores it as a struct. Compare both as parsed JSON.
     */
    private static Object storeValue(Object value) {
        final var normalized = Normalizer.normalize(value);
        if (normalized instanceof String text && (text.startsWith("{") || text.startsWith("["))) return Normalizer.parseJson(text);
        return normalized;
    }

    private static SideResult.Shape shape(TopologyDescription description) {
        final var stores = new TreeSet<String>();
        final var internalTopics = new TreeSet<String>();
        for (final var subTopology : description.subtopologies()) {
            for (final var node : subTopology.nodes()) {
                if (node instanceof TopologyDescription.Processor processor) stores.addAll(processor.stores());
                if (node instanceof TopologyDescription.Source source && source.topicSet() != null) {
                    source.topicSet().stream().filter(ParityRunner::isInternalTopic).forEach(internalTopics::add);
                }
                if (node instanceof TopologyDescription.Sink sink && sink.topic() != null && isInternalTopic(sink.topic())) {
                    internalTopics.add(sink.topic());
                }
            }
        }
        return new SideResult.Shape(description.subtopologies().size(), internalTopics.size(), description.globalStores().size(), stores);
    }

    private static boolean isInternalTopic(String topic) {
        return topic.endsWith("-repartition") || topic.endsWith("-topic");
    }

    static String describe(Throwable e) {
        final var parts = new ArrayList<String>();
        for (var cause = e; cause != null && parts.size() < 8; cause = cause.getCause() == cause ? null : cause.getCause()) {
            final var message = String.valueOf(cause.getMessage());
            if (parts.stream().noneMatch(p -> p.contains(message))) parts.add(cause.getClass().getSimpleName() + ": " + message);
        }
        final var message = String.join(" -> ", parts).replace('\n', ' ');
        return message.length() > 1500 ? message.substring(0, 1500) + "..." : message;
    }

    private static void delete(Path dir) {
        if (dir == null) return;
        try (final var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException _) {
            // Best effort, the directory is in the temp folder
        }
    }
}
