package io.axual.ksml.definition.parser;

import io.axual.ksml.exception.TopologyException;
import org.apache.kafka.streams.Topology;

public class OffsetResetPolicyParser {
    public static Topology.AutoOffsetReset parseResetPolicy(String resetPolicy) {
        if (resetPolicy == null || resetPolicy.isEmpty()) return null;
        if (Topology.AutoOffsetReset.EARLIEST.name().equalsIgnoreCase(resetPolicy))
            return Topology.AutoOffsetReset.EARLIEST;
        if (Topology.AutoOffsetReset.LATEST.name().equalsIgnoreCase(resetPolicy))
            return Topology.AutoOffsetReset.LATEST;
        throw new TopologyException("Unknown offset reset policy: " + resetPolicy);
    }
}
