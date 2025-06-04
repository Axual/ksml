package io.axual.ksml.metric;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.streams.TopologyDescription;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;

/**
 * Translates Kafka Streams metrics into enriched KSML metrics by appending contextual tags
 * such as namespace, pipeline, and operation name. his allows latency metrics to be mapped back to
 * specific KSML processing elements.
 */
@Slf4j
public final class KsmlTagEnricher {
    public static final String STREAM_PROCESSOR_GROUP = "stream-processor-node-metrics";
    private static final Set<String> INTERESTING_GROUPS = Set.of(STREAM_PROCESSOR_GROUP);
    private static final String NODE_LATENCY_PREFIX = "record-e2e-latency-";
    // Regex Explanation:
    // Matches node names formatted as: <namespace>_pipelines_<pipeline>[_via_step<step>]_operation
    //
    // Examples:
    //   - "my_ns_pipelines_flow1_via_step2_transform"
    //     → namespace=my_ns, pipeline=flow1, step=2, operation=transform
    //
    //   - "my_ns_pipelines_flow2_sink"
    //     → namespace=my_ns, pipeline=flow2, step=null, operation=sink
    static final Pattern NODE_PATTERN = Pattern.compile(
            "^(?<ns>(?:(?!_pipelines_).)+)_pipelines_"                     // namespace (possessive with negative lookahead)
                    + "(?<pipeline>(?:(?!_via_step\\d|_[^_]+$).)+)"  // pipeline (possessive, stops before *via*step or final _op)
                    + "(?:_via_step(?<step>\\d+))?"                        // optional step
                    + "_(?<op>[^_]+)$"                                     // operation (no underscores)
    );

    private final Map<String, OperationMeta> nodeToOperation;

    KsmlTagEnricher(Map<String, OperationMeta> node2OperationMap) {
        this.nodeToOperation = Map.copyOf(node2OperationMap);
    }

    record OperationMeta(String namespace, String pipeline, String operation, String step) {
    }

    public static KsmlTagEnricher from(TopologyDescription desc) {
        if (desc == null) {
            throw new IllegalArgumentException("TopologyDescription cannot be null");
        }

        Map<String, OperationMeta> node2Operation = new HashMap<>();

        for (TopologyDescription.Subtopology st : desc.subtopologies()) {
            processSubtopology(st, node2Operation);
        }

        return new KsmlTagEnricher(node2Operation);
    }

    private static void processSubtopology(TopologyDescription.Subtopology subtopology,
                                           Map<String, OperationMeta> node2Operation) {
        for (TopologyDescription.Node node : subtopology.nodes()) {
            processNode(node, node2Operation);
        }
    }

    private static void processNode(TopologyDescription.Node node,
                                    Map<String, OperationMeta> node2Operation) {
        String nodeName = node.name();

        if (!isValidNodeName(nodeName)) {
            log.warn("Skipping invalid node name: {}", nodeName);
            return;
        }

        extractAndStoreOperationMeta(nodeName, node2Operation);
    }

    private static boolean isValidNodeName(String nodeName) {
        return nodeName != null && nodeName.length() <= 500;
    }

    private static void extractAndStoreOperationMeta(String nodeName,
                                                     Map<String, OperationMeta> node2Operation) {
        try {
            Optional<OperationMeta> operationMeta = parseNodeName(nodeName);
            operationMeta.ifPresent(meta -> node2Operation.put(nodeName, meta));
        } catch (Exception e) {
            log.warn("Failed to process node name: {}", nodeName, e);
        }
    }

    private static Optional<OperationMeta> parseNodeName(String nodeName) {
        Matcher m = NODE_PATTERN.matcher(nodeName);

        if (!m.matches()) {
            return Optional.empty();
        }

        String namespace = m.group("ns");
        String pipeline = m.group("pipeline");
        String operation = m.group("op");
        String step = m.group("step");

        if (namespace == null || pipeline == null || operation == null) {
            log.debug("Incomplete pattern match for node: {}", nodeName);
            return Optional.empty();
        }

        return Optional.of(new OperationMeta(namespace, pipeline, operation, step));
    }

    /**
     * Checks whether the given Kafka metric should be processed by this enricher.
     * Only latency metrics for stream processor nodes are considered interesting.
     */
    public boolean isInteresting(@NotNull KafkaMetric km) {
        var metricName = km.metricName();
        var group = metricName.group();
        if (group == null || !INTERESTING_GROUPS.contains(group)) return false;
        return isNodeLatency(metricName.name());
    }

    // 10-20 × faster than allocating a Matcher for every metric change with a regex like:
    // Pattern.compile("record-e2e-latency-(avg|max|min)")
    private static boolean isNodeLatency(String name) {
        return LatencyStat.of(name).isPresent();
    }

    private enum LatencyStat { AVG, MIN, MAX;
        static Optional<LatencyStat> of(String metricName) {
            if (!metricName.startsWith(NODE_LATENCY_PREFIX)) return Optional.empty();
            return switch (metricName.substring(NODE_LATENCY_PREFIX.length())) {
                case "avg" -> Optional.of(AVG);
                case "min" -> Optional.of(MIN);
                case "max" -> Optional.of(MAX);
                default    -> Optional.empty();
            };
        }
    }

    /**
     * Converts a Kafka metric name into a KSML-enriched metric name,
     * adding tags for namespace, pipeline, operation, etc.
     * Only applies to e2e latency metrics for stream processor nodes.
     *
     * @param kafkaName the original Kafka metric name
     * @return enriched KSML metric name, or null if enrichment is not applicable
     */
    public MetricName toKsmlMetricName(
            @NotNull org.apache.kafka.common.MetricName kafkaName) {

        if (!STREAM_PROCESSOR_GROUP.equals(kafkaName.group())) return null;

        String nodeId = kafkaName.tags().get("processor-node-id");
        if (nodeId == null) {
            log.warn("Missing processor-node-id tag in metric: {}", kafkaName);
            return null;
        }

        OperationMeta op = nodeToOperation.get(nodeId);
        if (op == null) {
            log.trace("No operation mapping found for node: {}", nodeId);
            return null;
        }

        MetricTags tags = new MetricTags()
                .append("namespace", op.namespace)
                .append("pipeline", op.pipeline)
                .append("operation_name", op.operation)
                .append("processor_node_id",  nodeId)
                .append("unit", "ms")
                .appendIfPresent("step", op.step);

        if (isNodeLatency(kafkaName.name())) {
            String which = kafkaName.name()
                    .substring(NODE_LATENCY_PREFIX.length());
            return new MetricName("record_e2e_latency_" + which + "_ms", tags);
        }
        return null; // not a metric we care to expose
    }
}
