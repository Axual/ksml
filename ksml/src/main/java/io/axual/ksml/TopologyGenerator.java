package io.axual.ksml;

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


import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.generator.TopologyAnalyzer;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.operation.StoreOperation;
import io.axual.ksml.operation.StreamOperation;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TopologyGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyGenerator.class);
    private final String applicationId;
    private final Properties optimization;

    public TopologyGenerator(String applicationId) {
        this(applicationId, null);
    }

    public TopologyGenerator(String applicationId, String optimization) {
        // Parse configuration
        this.applicationId = applicationId;
        this.optimization = new Properties();
        if (optimization != null) {
            this.optimization.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);
        }
    }

    public Topology create(StreamsBuilder streamsBuilder, Map<String, TopologyDefinition> definitions) {
        if (definitions.isEmpty()) return null;

        final var stores = new TreeMap<String, StateStoreDefinition>();

        definitions.forEach((name, definition) -> {
            final var context = new TopologyBuildContext(streamsBuilder, definition);
            generate(definition, context);
            stores.putAll(definition.stateStores());
        });

        var topology = streamsBuilder.build(optimization);
        var analysis = TopologyAnalyzer.analyze(topology, applicationId);

        StringBuilder summary = new StringBuilder("\n\n");
        summary.append(topology != null ? topology.describe() : "null");
        summary.append("\n");

        appendTopics(summary, "Input topics", analysis.inputTopics());
        appendTopics(summary, "Intermediate topics", analysis.intermediateTopics());
        appendTopics(summary, "Output topics", analysis.outputTopics());
        appendTopics(summary, "Internal topics", analysis.internalTopics());

        appendStores(summary, "Registered state stores", stores);

        LOG.info("\n{}", summary);

        return topology;
    }

    public void appendTopics(StringBuilder builder, String description, Set<String> topics) {
        if (!topics.isEmpty()) {
            builder.append(description).append(":\n  ");
            builder.append(String.join("\n  ", topics));
            builder.append("\n");
        }
    }

    public void appendStores(StringBuilder builder, String description, Map<String, StateStoreDefinition> stores) {
        if (!stores.isEmpty()) {
            builder.append(description).append(":\n");
        }
        for (final var storeEntry : stores.entrySet()) {
            builder
                    .append("  ")
                    .append(storeEntry.getKey())
                    .append(" (")
                    .append(storeEntry.getValue().type())
                    .append("): key=")
                    .append(storeEntry.getValue().keyType())
                    .append(", value=")
                    .append(storeEntry.getValue().valueType())
                    .append(", url_path=/state/")
                    .append(switch (storeEntry.getValue().type()) {
                        case KEYVALUE_STORE -> "keyValue";
                        case SESSION_STORE -> "session";
                        case WINDOW_STORE -> "window";
                    })
                    .append("/")
                    .append(storeEntry.getKey())
                    .append("/")
                    .append("\n");
        }
    }

    private String getPrefix(String source) {
        // The source contains the full path to the source YAML file. We generate a prefix for
        // naming Kafka Streams Processor nodes by just taking the filename (eg. everything after
        // the last slash in the file path) and removing the file extension if it exists.
        while (source.contains("/")) {
            source = source.substring(source.indexOf("/") + 1);
        }
        if (source.contains(".")) {
            source = source.substring(0, source.lastIndexOf("."));
        }
        if (!source.isEmpty()) source += "_";
        return source;
    }

    private void generate(TopologyDefinition specification, TopologyBuildContext context) {
        // Preload the function into the Python context
        specification.functions().forEach((name, func) -> context.createUserFunction(func));

        // Figure out which state stores to create manually. Mechanism:
        // 1. run through all pipelines and scan for StoreOperations, don't create the stores referenced
        // 2. run through all stores and create the remaining ones manually
        final var kafkaStreamsCreatedStores = new HashSet<String>();

        // Ensure that local state store in tables are registered with the StreamBuilder
        specification.topics().forEach((name, def) -> {
            if (def instanceof TableDefinition tableDef) {
                context.getStreamWrapper(tableDef);
                if (tableDef.store() != null) {
                    // Register the state store and mark as already created (by Kafka Streams framework, not by user)
                    specification.register(tableDef.store().name(), tableDef.store());
                    kafkaStreamsCreatedStores.add(tableDef.store().name());
                }
            }
            if (def instanceof GlobalTableDefinition globalTableDef) {
                context.getStreamWrapper(globalTableDef);
                if (globalTableDef.store() != null) {
                    // Register the state store and mark as already created (by Kafka Streams framework, not by user)
                    specification.register(globalTableDef.store().name(), globalTableDef.store());
                    kafkaStreamsCreatedStores.add(globalTableDef.store().name());
                }
            }
        });

        // Filter out all state stores that Kafka Streams will set up later as part of the topology
        specification.pipelines().forEach((name, pipeline) -> pipeline.chain().forEach(operation -> {
            if (operation instanceof StoreOperation storeOperation && storeOperation.store() != null)
                kafkaStreamsCreatedStores.add(storeOperation.store().name());
        }));

        // Create all not-automatically-created stores
        specification.stateStores().forEach((name, store) -> {
            if (!kafkaStreamsCreatedStores.contains(name)) {
                context.createUserStateStore(store);
            }
        });

        // For each pipeline, generate the topology
        specification.pipelines().forEach((name, pipeline) -> {
            StringBuilder tsBuilder = new StringBuilder();
            // Use a cursor to keep track of where we are in the topology
            StreamWrapper cursor = context.getStreamWrapper(pipeline.source());
            tsBuilder.append("%n  %s".formatted(cursor));
            // For each operation, advance the cursor by applying the operation
            for (StreamOperation operation : pipeline.chain()) {
                tsBuilder.append("%n    ==> %s".formatted(operation));
                cursor = cursor.apply(operation, context);
                tsBuilder.append("%n  %s".formatted(cursor));
            }
            // Finally, at the end, apply the sink operation
            if (pipeline.sink() != null) {
                tsBuilder.append("%n    ==> %s".formatted(pipeline.sink()));
                cursor.apply(pipeline.sink(), context);
            }
            LOG.info("""
                    Generating Kafka Streams topology for pipeline {}:
                    {}
                    """, name, tsBuilder);

        });
    }
}
