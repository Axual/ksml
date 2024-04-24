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

import io.axual.ksml.definition.StateStoreDefinition;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.*;

public class TopologyAnalyzer {
    public record TopologyAnalysis(Set<String> inputTopics, Set<String> intermediateTopics,
                                   Set<String> outputTopics, Map<String, StateStoreDefinition> stores) {
    }

    public static TopologyAnalysis analyze(Topology topology, String applicationId, Collection<String> knownTopics) {
        // Derive all input, intermediate and output topics
        final var topologyInputs = new HashSet<String>();
        final var topologyOutputs = new HashSet<String>();
        final var inputTopics = new HashSet<String>();
        final var outputTopics = new HashSet<String>();
        final var intermediateTopics = new HashSet<String>();

        analyzeTopology(topology, topologyInputs, topologyOutputs);

        for (String topic : topologyInputs) {
            if (knownTopics.contains(topic)) {
                inputTopics.add(topic);
            } else {
                intermediateTopics.add(applicationId + "-" + topic);
            }
        }

        for (String topic : topologyOutputs) {
            if (knownTopics.contains(topic)) {
                outputTopics.add(topic);
            } else {
                intermediateTopics.add(applicationId + "-" + topic);
            }
        }

        // Return the built topology and its context
        return new TopologyAnalysis(
                inputTopics,
                intermediateTopics,
                outputTopics,
                new HashMap<>());
//                context.getStoreDefinitions());
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
//                        stores.addAll(processorNode.stores());
                    }
                    if (node instanceof TopologyDescription.Sink sinkNode && sinkNode.topic() != null) {
                        outputTopics.add(sinkNode.topic());
                    }
                }
            }
        }
    }
}
