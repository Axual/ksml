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


import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.generator.TopologyAnalyzer;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.generator.TopologySpecification;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.operation.StoreOperation;
import io.axual.ksml.operation.StreamOperation;
import io.axual.ksml.operation.ToOperation;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TopologyGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyGenerator.class);
    private final String applicationId;
    private final NotationLibrary notationLibrary;

    public TopologyGenerator(String applicationId, NotationLibrary notationLibrary) {
        // Parse configuration
        this.applicationId = applicationId;
        this.notationLibrary = notationLibrary;
    }

    public Topology create(StreamsBuilder streamsBuilder, Map<String, TopologySpecification> definitions) {
        if (definitions.isEmpty()) return null;

        final var knownTopics = new HashSet<String>();

        definitions.forEach((name, definition) -> {
            final var context = new TopologyBuildContext(streamsBuilder, definition, notationLibrary, name);
            generate(definition, context);
        });

        var topology = streamsBuilder.build();
        var analysis = TopologyAnalyzer.analyze(topology, applicationId, knownTopics);

        StringBuilder summary = new StringBuilder("\n\n");
        summary.append(topology != null ? topology.describe() : "null");
        summary.append("\n");

        appendTopics(summary, "Input topics", analysis.inputTopics());
        appendTopics(summary, "Intermediate topics", analysis.intermediateTopics());
        appendTopics(summary, "Output topics", analysis.outputTopics());

        appendStores(summary, "Registered state stores", analysis.stores());

        LOG.info("\n{}\n", summary);

        return topology;
    }

    public void appendTopics(StringBuilder builder, String description, Set<String> topics) {
        if (topics.size() > 0) {
            builder.append(description).append(":\n  ");
            builder.append(String.join("\n  ", topics));
            builder.append("\n");
        }
    }

    public void appendStores(StringBuilder builder, String description, Map<String, StateStoreDefinition> stores) {
        if (stores.size() > 0) {
            builder.append(description).append(":\n");
        }
        for (var entry : stores.entrySet()) {
            builder
                    .append("  ")
                    .append(entry.getKey())
                    .append(" (")
                    .append(entry.getValue().type())
                    .append("): key=")
                    .append(entry.getValue().keyType())
                    .append(", value=")
                    .append(entry.getValue().valueType())
                    .append(", url_path=/state/")
                    .append(switch (entry.getValue().type()) {
                        case KEYVALUE_STORE -> "keyValue";
                        case SESSION_STORE -> "session";
                        case WINDOW_STORE -> "window";
                    })
                    .append("/")
                    .append(entry.getKey())
                    .append("/");
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

    private void generate(TopologySpecification specification, TopologyBuildContext context) {
        // Register all topics
        final var knownTopics = new HashSet<String>();
        specification.topics().forEach((name, def) -> knownTopics.add(def.topic()));

        // Add source and target topics to the set of known topics
        specification.pipelines().forEach((name, def) -> {
            if (def.source() != null) knownTopics.add(def.source().topic());
            if (def.sink() instanceof ToOperation toOperation) {
                knownTopics.add(toOperation.target.topic());
            }
        });

        // Preload the function into the Python context
        specification.functions().forEach((name, func) -> {
            context.createUserFunction(func);
        });

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
        });

        // Filter out all state stores that Kafka Streams will set up later as part of the topology
        specification.pipelines().forEach((name, pipeline) -> {
            pipeline.chain().forEach(operation -> {
                if (operation instanceof StoreOperation storeOperation && storeOperation.store() != null)
                    kafkaStreamsCreatedStores.add(storeOperation.store().name());
            });
        });

        // Create all not-automatically-created stores
        specification.stateStores().forEach((name, store) -> {
            if (!kafkaStreamsCreatedStores.contains(name)) {
                context.createUserStateStore(store);
            }
        });

        // For each pipeline, generate the topology
        specification.pipelines().forEach((name, pipeline) -> {
            // Use a cursor to keep track of where we are in the topology
            StreamWrapper cursor = context.getStreamWrapper(pipeline.source());
            LOG.info("Generating Kafka Streams topology:");
            LOG.info("  {}", cursor);
            // For each operation, advance the cursor by applying the operation
            for (StreamOperation operation : pipeline.chain()) {
                LOG.info("    ==> {}", operation);
                cursor = cursor.apply(operation, context);
                LOG.info("  {}", cursor);
            }
            // Finally, at the end, apply the sink operation
            if (pipeline.sink() != null) {
                LOG.info("    ==> {}", pipeline.sink());
                cursor.apply(pipeline.sink(), context);
            }
        });
    }
}
