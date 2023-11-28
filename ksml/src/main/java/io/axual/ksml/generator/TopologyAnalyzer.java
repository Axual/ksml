package io.axual.ksml.generator;

import io.axual.ksml.definition.StateStoreDefinition;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
