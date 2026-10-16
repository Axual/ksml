package io.axual.ksml.generator;

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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Builds a multi-subtopology topology out of several independent source/sink chains and checks that
 * {@link TopologyAnalyzer#analyze} buckets each topic correctly: a plain input/output pair, a genuine
 * intermediate topic (output of one subtopology, input of the next), and two intermediate topics that
 * get reclassified as internal because their names end in "-repartition"/"-changelog".
 */
class TopologyAnalyzerTest {
    private static final String APPLICATION_ID = "test-app";

    @Test
    @DisplayName("analyze classifies input, output, intermediate and internal topics correctly")
    void analyzeClassifiesTopicsCorrectly() {
        final var builder = new StreamsBuilder();
        final var consumed = Consumed.with(Serdes.String(), Serdes.String());
        final var produced = Produced.with(Serdes.String(), Serdes.String());

        // A standalone input/output pair, unrelated to any other subtopology
        builder.stream("another-input", consumed).to("another-output", produced);

        // A genuine intermediate topic: output of one subtopology, input of the next
        builder.stream("raw-input", consumed).to("real-intermediate", produced);
        builder.stream("real-intermediate", consumed).to("final-output", produced);

        // An intermediate topic reclassified as internal due to its "-repartition" suffix
        builder.stream("raw-input-2", consumed).to("mid-repartition", produced);
        builder.stream("mid-repartition", consumed).to("final-output-2", produced);

        // An intermediate topic reclassified as internal due to its "-changelog" suffix
        builder.stream("raw-input-3", consumed).to("mid-changelog", produced);
        builder.stream("mid-changelog", consumed).to("final-output-3", produced);

        final var topology = builder.build();
        final var analysis = TopologyAnalyzer.analyze(topology, APPLICATION_ID);

        assertThat(analysis.inputTopics()).containsExactlyInAnyOrder("another-input", "raw-input", "raw-input-2", "raw-input-3");
        assertThat(analysis.outputTopics()).containsExactlyInAnyOrder("another-output", "final-output", "final-output-2", "final-output-3");
        assertThat(analysis.intermediateTopics()).containsExactly("real-intermediate");
        assertThat(analysis.internalTopics()).containsExactlyInAnyOrder(
                APPLICATION_ID + "-mid-repartition",
                APPLICATION_ID + "-mid-changelog");
    }
}
