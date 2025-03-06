package io.axual.ksml.generator;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class TopologyAnalyzer {
    public record TopologyAnalysis(Set<String> inputTopics, Set<String> intermediateTopics,
                                   Set<String> outputTopics, Set<String> internalTopics) {
    }

    public static TopologyAnalysis analyze(Topology topology, String applicationId) {
        // Derive all input, intermediate and output topics
        final var inputTopics = new TreeSet<String>();
        final var intermediateTopics = new TreeSet<String>();
        final var outputTopics = new TreeSet<String>();
        final var internalTopics = new TreeSet<String>();

        analyzeTopology(topology, inputTopics, outputTopics);

        // Intermediate topics are both input and output topics, so sort them into a separate set
        for (final var topic : inputTopics) {
            if (outputTopics.contains(topic)) {
                intermediateTopics.add(topic);
                outputTopics.remove(topic);
            }
        }
        inputTopics.removeAll(intermediateTopics);

        // Internal topics end in "changelog" or "repartition", so sort them into a separate set
        for (final var topic : intermediateTopics) {
            if (topic.endsWith("-changelog") || topic.endsWith("-repartition")) {
                internalTopics.add(topic);
            }
        }
        intermediateTopics.removeAll(internalTopics);

        // Return the built topology and its context
        return new TopologyAnalysis(
                inputTopics,
                intermediateTopics,
                outputTopics,
                internalTopics.stream().map(topic -> applicationId + "-" + topic).collect(Collectors.toSet()));
    }

    private static void analyzeTopology(Topology topology, Set<String> inputTopics, Set<String> outputTopics) {
        final var description = topology.describe();
        for (int index = 0; index < description.subtopologies().size(); index++) {
            for (var subTopology : description.subtopologies()) {
                for (var node : subTopology.nodes()) {
                    if (node instanceof TopologyDescription.Source sourceNode) {
                        inputTopics.addAll(sourceNode.topicSet());
                        if (sourceNode.topicPattern() != null)
                            inputTopics.add(sourceNode.topicPattern().pattern());
                    }
                    if (node instanceof TopologyDescription.Processor processorNode) {
                        // Ignore store names here
                    }
                    if (node instanceof TopologyDescription.Sink sinkNode && sinkNode.topic() != null) {
                        outputTopics.add(sinkNode.topic());
                    }
                }
            }
        }
    }
}
