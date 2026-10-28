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

import org.apache.kafka.streams.StreamsBuilder;

import org.junit.jupiter.api.Assumptions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One parity case: a plain Kafka Streams topology (the reference) and a KSML definition that should behave
 * the same. Both get the same input records on their own {@code TopologyTestDriver}, and the output topics,
 * named stores and topology shape are compared.
 * <p>
 * A case with {@link #knownDifference(String, String)} documents a difference that was found. Its test passes
 * while the difference is still there, and fails once KSML behaves like Kafka Streams, so the marker can be
 * removed.
 */
public final class ParityCase {
    /**
     * System property that picks how {@link #run()} works: {@code ttd} (default) runs both sides on a
     * {@code TopologyTestDriver}, {@code docker} runs both sides on real brokers with Docker Compose, and
     * {@code capture} only stores the case in {@link #captured} (used by {@link ReferenceApp}).
     */
    public static final String MODE_PROPERTY = "parity.mode";
    static ParityCase captured;

    sealed interface Step permits Pipe, WallClock {
    }

    record Pipe(String topic, String key, String value, long timestamp, Map<String, String> headers) implements Step {
    }

    record WallClock(long millis) implements Step {
    }

    record TopicFormat(Format key, Format value) {
    }

    final String id;
    final String title;
    Consumer<StreamsBuilder> reference;
    String ksmlResource;
    final Map<String, TopicFormat> inputs = new LinkedHashMap<>();
    final Map<String, TopicFormat> outputs = new LinkedHashMap<>();
    final List<String> stores = new ArrayList<>();
    final List<Step> steps = new ArrayList<>();
    List<String> optimizations = List.of("none", "all");
    boolean compareHeaders = false;
    boolean compareShape = false;
    boolean compareChangelogs = false;
    boolean compareFinalValues = false;
    final Set<String> globalTopics = new HashSet<>();
    String findingId;
    String findingText;
    String referencePrintFile;
    String ksmlPrintFile;
    final Map<String, Integer> partitions = new LinkedHashMap<>();
    final Map<String, String> config = new LinkedHashMap<>();
    int restartAfterStep = -1;
    boolean dockerOnly = false;
    String dockerSkipReason;

    private ParityCase(String id, String title) {
        this.id = id;
        this.title = title;
    }

    public static ParityCase of(String id, String title) {
        return new ParityCase(id, title);
    }

    public ParityCase input(String topic, Format key, Format value) {
        inputs.put(topic, new TopicFormat(key, value));
        return this;
    }

    public ParityCase input(String topic) {
        return input(topic, Format.STRING, Format.JSON);
    }

    public ParityCase output(String topic, Format key, Format value) {
        outputs.put(topic, new TopicFormat(key, value));
        return this;
    }

    public ParityCase output(String topic) {
        return output(topic, Format.STRING, Format.JSON);
    }

    public ParityCase store(String name) {
        stores.add(name);
        return this;
    }

    public ParityCase reference(Consumer<StreamsBuilder> reference) {
        this.reference = reference;
        return this;
    }

    public ParityCase ksml(String resource) {
        this.ksmlResource = resource;
        return this;
    }

    public ParityCase pipe(String topic, String key, String value, long timestamp) {
        steps.add(new Pipe(topic, key, value, timestamp, Map.of()));
        return this;
    }

    public ParityCase pipe(String topic, String key, String value, long timestamp, Map<String, String> headers) {
        steps.add(new Pipe(topic, key, value, timestamp, headers));
        return this;
    }

    public ParityCase advanceWallClock(long millis) {
        steps.add(new WallClock(millis));
        return this;
    }

    public ParityCase optimizations(String... modes) {
        this.optimizations = List.of(modes);
        return this;
    }

    public ParityCase compareHeaders() {
        this.compareHeaders = true;
        return this;
    }

    public ParityCase compareShape() {
        this.compareShape = true;
        return this;
    }

    public ParityCase compareChangelogs() {
        this.compareChangelogs = true;
        return this;
    }

    /**
     * Compares only the last value per key and partition. Use this when the input has several partitions,
     * because then the order between partitions is not fixed.
     */
    public ParityCase compareFinalValues() {
        this.compareFinalValues = true;
        return this;
    }

    /**
     * Marks an input topic that is read as a global table. A global consumer commits no offsets.
     */
    public ParityCase global(String topic) {
        globalTopics.add(topic);
        return this;
    }

    /**
     * Compares the files that a {@code print} operation writes on each side.
     */
    public ParityCase printFiles(String referenceFile, String ksmlFile) {
        this.referencePrintFile = referenceFile;
        this.ksmlPrintFile = ksmlFile;
        return this;
    }

    /**
     * Number of partitions for a topic in Docker mode. The test driver always uses one partition.
     */
    public ParityCase partitions(String topic, int count) {
        partitions.put(topic, count);
        return this;
    }

    /**
     * Sets a Kafka Streams setting on both sides: the reference topology and driver, and the KSML runner config.
     */
    public ParityCase config(String key, String value) {
        config.put(key, value);
        return this;
    }

    /**
     * Docker mode only: stop both apps after this many input steps, then start them again and send the rest.
     */
    public ParityCase restartAfter(int steps) {
        this.restartAfterStep = steps;
        return this;
    }

    public ParityCase dockerOnly() {
        this.dockerOnly = true;
        return this;
    }

    public ParityCase skipInDocker(String reason) {
        this.dockerSkipReason = reason;
        return this;
    }

    public ParityCase knownDifference(String findingId, String findingText) {
        this.findingId = findingId;
        this.findingText = findingText;
        return this;
    }

    /**
     * Runs the case for every optimization setting, writes the report and checks the outcome.
     */
    public List<ParityComparison> run() {
        final var mode = System.getProperty(MODE_PROPERTY, "ttd");
        if (mode.equals("capture")) {
            captured = this;
            return List.of();
        }
        if (mode.equals("docker")) {
            Assumptions.assumeTrue(dockerSkipReason == null, dockerSkipReason);
            final var comparison = DockerParity.run(this);
            ParityReport.write(this, comparison);
            return List.of(comparison);
        }
        Assumptions.assumeFalse(dockerOnly, "this case only runs in Docker mode");
        final var comparisons = new ArrayList<ParityComparison>();
        for (final var optimization : optimizations) {
            final var ref = ParityRunner.runReference(this, optimization);
            final var ksml = ParityRunner.runKsml(this, optimization);
            final var comparison = ParityComparison.compare(this, optimization, ref, ksml);
            ParityReport.write(this, comparison);
            comparisons.add(comparison);
        }
        if (findingId == null) {
            for (final var comparison : comparisons) {
                assertThat(comparison.differences())
                        .as("%s [optimization=%s] %s%n%s", id, comparison.optimization(), title, comparison.details())
                        .isEmpty();
            }
        } else {
            assertThat(comparisons.stream().anyMatch(c -> !c.differences().isEmpty()))
                    .as("%s: known difference %s is gone, KSML now behaves like Kafka Streams. Remove the marker.%n%s",
                            id, findingId, comparisons.getFirst().details())
                    .isTrue();
        }
        return comparisons;
    }
}
