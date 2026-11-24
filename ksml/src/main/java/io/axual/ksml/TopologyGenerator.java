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
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyAnalyzer;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.operation.DualStoreOperation;
import io.axual.ksml.operation.LeftJoinWithStreamOperation;
import io.axual.ksml.operation.OuterJoinWithStreamOperation;
import io.axual.ksml.operation.StoreOperation;
import io.axual.ksml.operation.OperationChain;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.store.StoreUpgrade.SharedJoinStore;
import io.axual.ksml.stream.StreamWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * Generates a Kafka Streams topology from KSML definitions. Each definition gets its own Python context, which the
 * topology uses while it runs. Close the generator when the topology is not used anymore, to free these contexts.
 */
@Slf4j
public class TopologyGenerator implements AutoCloseable {
    private static final String UNDEFINED = "undefined";
    private final String applicationId;
    private final Properties optimization;
    private final PythonContextConfig pythonContextConfig;
    private final boolean headersStoreFormat;
    private final List<StateStoreDefinition> userStateStores = new ArrayList<>();
    private final List<StateStoreDefinition> upgradeCheckedStores = new ArrayList<>();
    private final List<SharedJoinStore> sharedJoinStores = new ArrayList<>();
    // The contexts of all topologies that this generator created, closed by close()
    private final List<TopologyBuildContext> buildContexts = new ArrayList<>();

    public TopologyGenerator(String applicationId) {
        this(applicationId, null, PythonContextConfig.builder().build());
    }

    public TopologyGenerator(String applicationId, String optimization, PythonContextConfig pythonContextConfig) {
        this(applicationId, optimization, pythonContextConfig, false);
    }

    /**
     * @param headersStoreFormat whether declared stores are built in the headers-aware format, as the Kafka Streams
     *                           {@code dsl.store.format: HEADERS} config asks for
     */
    public TopologyGenerator(String applicationId, String optimization, PythonContextConfig pythonContextConfig, boolean headersStoreFormat) {
        // Parse configuration
        this.applicationId = applicationId;
        this.optimization = new Properties();
        if (optimization != null) {
            this.optimization.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, optimization);
        }
        this.pythonContextConfig = pythonContextConfig != null
                ? pythonContextConfig
                : PythonContextConfig.builder().build();
        this.headersStoreFormat = headersStoreFormat;
    }

    /** The stores that the last {@link #create} built for Python functions, rather than for DSL operations. */
    public List<StateStoreDefinition> userStateStores() {
        return List.copyOf(userStateStores);
    }

    /**
     * The declared stores that the last {@link #create} built, without the stores whose KSML 1.x local state has a
     * changelog whatever {@code logging} says: Kafka Streams always logged the window stores of stream-stream joins,
     * and a table restores from its input topic with topology optimization, as a global table always does.
     */
    public List<StateStoreDefinition> upgradeCheckedStores() {
        return List.copyOf(upgradeCheckedStores);
    }

    /**
     * The shared stores of the stream-stream left and outer joins that the last {@link #create} built. Kafka Streams
     * creates them itself, so they are not declared stores, but their logging depends on the join stores.
     */
    public List<SharedJoinStore> sharedJoinStores() {
        return List.copyOf(sharedJoinStores);
    }

    /** Closes the Python contexts of the topologies that this generator created. */
    @Override
    public void close() {
        buildContexts.forEach(TopologyBuildContext::close);
        buildContexts.clear();
    }

    public Topology create(StreamsBuilder streamsBuilder, Map<String, TopologyDefinition> definitions) {
        userStateStores.clear();
        upgradeCheckedStores.clear();
        sharedJoinStores.clear();
        if (definitions.isEmpty()) return null;

        final var stores = new TreeMap<String, StateStoreDefinition>();

        definitions.forEach((_, definition) -> {
            // Log the start of the processor
            log.info("Starting processor definition: name={}, version={}, namespace={}",
                    definition.name() != null ? definition.name() : UNDEFINED,
                    definition.version() != null ? definition.version() : UNDEFINED,
                    definition.namespace() != null ? definition.namespace() : UNDEFINED);

            final var context = new TopologyBuildContext(streamsBuilder, definition, pythonContextConfig, headersStoreFormat);
            buildContexts.add(context);
            generate(definition, context);
            stores.putAll(definition.stateStores());
            // Stores whose KSML 1.x state has a changelog whatever logging says, so the upgrade check leaves them out
            final var restorableStores = new HashSet<String>();
            collectStreamJoinStores(definition, restorableStores);
            collectSourceTopicStores(definition, restorableStores);
            definition.stateStores().forEach((name, store) -> {
                if (!restorableStores.contains(name)) upgradeCheckedStores.add(store);
            });
            collectSharedJoinStores(definition, sharedJoinStores);
        });

        final var topology = build(streamsBuilder);
        final var analysis = TopologyAnalyzer.analyze(topology, applicationId);

        StringBuilder summary = new StringBuilder("\n\n");
        summary.append(topology != null ? topology.describe() : "null");
        summary.append("\n");

        appendTopics(summary, "Input topics", analysis.inputTopics());
        appendTopics(summary, "Intermediate topics", analysis.intermediateTopics());
        appendTopics(summary, "Output topics", analysis.outputTopics());
        appendTopics(summary, "Internal topics", analysis.internalTopics());

        appendStores(summary, "Registered state stores", stores);

        log.info("\n{}", summary);

        return topology;
    }

    private Topology build(StreamsBuilder streamsBuilder) {
        try {
            return streamsBuilder.build(optimization);
        } catch (org.apache.kafka.streams.errors.TopologyException e) {
            // Without optimization, each cogroup adds its own repartition, which clashes with other uses
            if (e.getMessage() != null && e.getMessage().contains("-repartition-filter is already added")) {
                throw new TopologyException("A grouped stream that needs a repartition, for example after groupBy, can not be used in a cogroup and also in another operation when topology.optimization is not all. Use a separate groupBy for each. " + e.getMessage());
            }
            throw e;
        }
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

    private void generate(TopologyDefinition definition, TopologyBuildContext context) {
        // Preload the function into the Python context
        definition.functions().forEach((_, func) -> context.createUserFunction(func));

        // Figure out which state stores to create manually. Mechanism:
        // 1. run through all pipelines and scan for StoreOperations, don't create the stores referenced
        // 2. run through all stores and create the remaining ones manually
        final var kafkaStreamsCreatedStores = new HashSet<String>();

        // Ensure that local state store in tables are registered with the StreamBuilder
        registerFrameworkManagedStores(definition, context, kafkaStreamsCreatedStores);

        // Filter out all state stores that Kafka Streams will set up later as part of the topology
        collectPipelineManagedStores(definition, kafkaStreamsCreatedStores);

        // Create all not-automatically-created stores
        definition.stateStores().forEach((name, store) -> {
            if (!kafkaStreamsCreatedStores.contains(name)) {
                context.createUserStateStore(store);
                userStateStores.add(store);
            }
        });

        // For each pipeline, generate the topology
        definition.pipelines().forEach((name, pipeline) -> {
            try {
                StringBuilder tsBuilder = new StringBuilder();
                // Use a cursor to keep track of where we are in the topology
                StreamWrapper cursor = context.getStreamWrapper(pipeline.source());
                tsBuilder.append("%n  %s".formatted(cursor));
                // For each operation, advance the cursor by applying the operation
                cursor = OperationChain.apply(cursor, pipeline.chain(), context, (operation, result) -> {
                    tsBuilder.append("%n    ==> %s".formatted(operation));
                    tsBuilder.append("%n  %s".formatted(result));
                });
                // Finally, at the end, apply the sink operation
                if (pipeline.sink() != null) {
                    tsBuilder.append("%n    ==> %s".formatted(pipeline.sink()));
                    cursor.apply(pipeline.sink(), context);
                }
                log.info("""
                        Generating Kafka Streams topology for pipeline {}:
                        {}
                        """, name, tsBuilder);

            } catch (Exception e) {
                throw new TopologyException("Error in topology \"" + name + "\": " + e.getMessage(), e);
            }
        });
    }

    /**
     * Registers the state store backing any {@link TableDefinition} or {@link GlobalTableDefinition}
     * in {@code definition}'s topics with {@code definition} itself, and adds its name to
     * {@code kafkaStreamsCreatedStores} since Kafka Streams creates it automatically as part of the
     * table/globalTable, rather than KSML having to create it manually.
     */
    private void registerFrameworkManagedStores(TopologyDefinition definition, TopologyBuildContext context, Set<String> kafkaStreamsCreatedStores) {
        definition.topics().forEach((_, def) -> {
            if (def instanceof TableDefinition tableDef) {
                context.getStreamWrapper(tableDef);
                if (tableDef.store() != null) {
                    // Register the state store and mark as already created (by Kafka Streams framework, not by user)
                    definition.register(tableDef.store().name(), tableDef.store());
                    kafkaStreamsCreatedStores.add(tableDef.store().name());
                }
            }
            if (def instanceof GlobalTableDefinition globalTableDef) {
                context.getStreamWrapper(globalTableDef);
                if (globalTableDef.store() != null) {
                    // Register the state store and mark as already created (by Kafka Streams framework, not by user)
                    definition.register(globalTableDef.store().name(), globalTableDef.store());
                    kafkaStreamsCreatedStores.add(globalTableDef.store().name());
                }
            }
        });
    }

    /**
     * Adds the name of any state store referenced by a {@link StoreOperation} or
     * {@link DualStoreOperation} in any of {@code definition}'s pipeline operation chains to
     * {@code kafkaStreamsCreatedStores}, since Kafka Streams sets those up itself as part of
     * building the topology, rather than KSML having to create them manually.
     */
    private void collectPipelineManagedStores(TopologyDefinition definition, Set<String> kafkaStreamsCreatedStores) {
        definition.pipelines().forEach((_, pipeline) -> pipeline.chain().forEach(operation -> {
            if (operation instanceof StoreOperation storeOperation && storeOperation.store() != null)
                kafkaStreamsCreatedStores.add(storeOperation.store().name());
            if (operation instanceof DualStoreOperation dualStoreOperation)
                addDualStoreNames(dualStoreOperation, kafkaStreamsCreatedStores);
        }));
    }

    // The window stores of stream-stream joins: Kafka Streams always logged them, so their KSML 1.x state has a
    // changelog. The shared store of a left or outer join is checked apart, see collectSharedJoinStores
    private static void collectStreamJoinStores(TopologyDefinition definition, Set<String> streamJoinStores) {
        definition.pipelines().forEach((_, pipeline) -> pipeline.chain().forEach(operation -> {
            if (operation instanceof DualStoreOperation dualStoreOperation)
                addDualStoreNames(dualStoreOperation, streamJoinStores);
        }));
    }

    // The shared stores of left and outer stream-stream joins, with the names that Kafka Streams gives them
    // (OuterStreamJoinStoreFactory: the name of the thisStore supplier, a suffix for the join type, and "-store")
    private static void collectSharedJoinStores(TopologyDefinition definition, List<SharedJoinStore> sharedJoinStores) {
        definition.pipelines().forEach((_, pipeline) -> pipeline.chain().forEach(operation -> {
            final var suffix = switch (operation) {
                case OuterJoinWithStreamOperation _ -> "-outer-shared-join-store";
                case LeftJoinWithStreamOperation _ -> "-left-shared-join-store";
                default -> null;
            };
            if (suffix != null && operation instanceof DualStoreOperation join && join.thisStore() != null && join.otherStore() != null)
                sharedJoinStores.add(new SharedJoinStore(join.thisStore().name() + suffix, join.thisStore(), join.otherStore()));
        }));
    }

    // The stores of tables that restore from their input topic. With reuse.ktable.source.topics, which is part of
    // topology.optimization: all (the KSML default, also in 1.x), Kafka Streams uses the input topic of a table as the
    // changelog of its store, whatever logging says. A global table always restores from its input topic.
    private void collectSourceTopicStores(TopologyDefinition definition, Set<String> sourceTopicStores) {
        final var reuseTableSourceTopics = reusesTableSourceTopics();
        definition.topics().forEach((_, topic) -> {
            if (topic instanceof TableDefinition table && table.store() != null && reuseTableSourceTopics)
                sourceTopicStores.add(table.store().name());
            if (topic instanceof GlobalTableDefinition globalTable && globalTable.store() != null)
                sourceTopicStores.add(globalTable.store().name());
        });
    }

    // Parsed as Kafka Streams does when it builds the topology
    private boolean reusesTableSourceTopics() {
        final var config = optimization.getProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG);
        return config != null && StreamsConfig.verifyTopologyOptimizationConfigs(config).contains(StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS);
    }

    private static void addDualStoreNames(DualStoreOperation operation, Set<String> names) {
        if (operation.thisStore() != null) names.add(operation.thisStore().name());
        if (operation.otherStore() != null) names.add(operation.otherStore().name());
    }
}
