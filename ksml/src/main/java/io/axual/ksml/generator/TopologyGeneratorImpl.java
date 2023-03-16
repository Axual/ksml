package io.axual.ksml.generator;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.KSMLConfig;
import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.definition.parser.*;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.operation.StreamOperation;
import io.axual.ksml.parser.MapParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.parser.topology.TopologyParseContext;
import io.axual.ksml.stream.StreamWrapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static io.axual.ksml.dsl.KSMLDSL.*;

/**
 * Generate a Kafka Streams topology from a KSML configuration, using a Python interpreter.
 *
 * @see KSMLConfig
 */
public class TopologyGeneratorImpl {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyGeneratorImpl.class);
    private final KSMLConfig config;

    public TopologyGeneratorImpl(KSMLConfig config) {
        this.config = config;
        ExecutionContext.INSTANCE.setConsumeHandler(config.consumeErrorHandler());
        ExecutionContext.INSTANCE.setProduceHandler(config.produceErrorHandler());
        ExecutionContext.INSTANCE.setProcessHandler(config.processErrorHandler());
    }

    private List<YAMLDefinition> readKSMLDefinitions() {
        try {
            switch (config.sourceType()) {
                case "file" -> {
                    // Parse source from file
                    LOG.info("Reading KSML from source file(s): {}", config.source());
                    return YAMLReader.readYAML(YAMLObjectMapper.INSTANCE, config.configDirectory(), config.source());
                }
                case "content" -> {
                    // Parse YAML content directly from string
                    LOG.info("Reading KSML from content string: {}", config.source());
                    return Collections.singletonList(new YAMLDefinition("content", YAMLObjectMapper.INSTANCE.readValue((String) config.source(), JsonNode.class)));
                }
                default -> throw new KSMLParseException(null, "Unknown KSML source dataType: " + config.sourceType());
            }
        } catch (IOException e) {
            LOG.info("Can not read YAML: {}", e.getMessage());
        }

        return new ArrayList<>();
    }

    public Topology create(String applicationId, StreamsBuilder builder) {
        List<YAMLDefinition> definitions = readKSMLDefinitions();
        GeneratedTopology generatorResult = null;
        for (YAMLDefinition definition : definitions) {
            generatorResult = generate(applicationId, builder, YamlNode.fromRoot(definition.root(), "ksml"), getPrefix(definition.source()));
        }

        if (generatorResult != null) {
            StringBuilder summary = new StringBuilder("\n\n");
            summary.append(generatorResult.topology() != null ? generatorResult.topology().describe() : "null");
            summary.append("\n");

            appendTopics(summary, "Input topics", generatorResult.inputTopics());
            appendTopics(summary, "Intermediate topics", generatorResult.intermediateTopics());
            appendTopics(summary, "Output topics", generatorResult.outputTopics());

            appendStores(summary, "Registered state stores", generatorResult.stores());

            LOG.info("\n{}\n", summary);
        }

        return builder.build();
    }

    public void appendTopics(StringBuilder builder, String description, Set<String> topics) {
        if (topics.size() > 0) {
            builder.append(description).append(":\n  ");
            builder.append(String.join("\n  ", topics));
            builder.append("\n");
        }
    }

    public void appendStores(StringBuilder builder, String description, Map<String, TopologyParseContext.StoreDescriptor> stores) {
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
                    .append(", retention=")
                    .append(entry.getValue().store().retention() != null ? entry.getValue().store().retention() : "null")
                    .append(", caching=")
                    .append(entry.getValue().store().caching() != null ? entry.getValue().store().caching() : "null")
                    .append(", url_path=/state/")
                    .append(switch (entry.getValue().type()) {
                        case KEYVALUE_STORE -> "keyvalue";
                        case SESSION_STORE -> "session";
                        case WINDOW_STORE -> "windowed";
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
        if (!source.isEmpty()) return source + "_";
        return source;
    }

    private GeneratedTopology generate(String applicationId, StreamsBuilder builder, YamlNode node, String
            namePrefix) {
        if (node == null) return null;

        // Set up the parse context, which will gather toplevel information on the streams topology
        TopologyParseContext context = new TopologyParseContext(builder, config.notationLibrary(), namePrefix);

        // Parse all defined streams
        Map<String, BaseStreamDefinition> streamDefinitions = new HashMap<>();
        new MapParser<>("stream definition", new StreamDefinitionParser()).parse(node.get(STREAMS_DEFINITION)).forEach(context::registerStreamDefinition);
        new MapParser<>("table definition", new TableDefinitionParser(context::registerStore)).parse(node.get(TABLES_DEFINITION)).forEach(context::registerStreamDefinition);
        new MapParser<>("globalTable definition", new GlobalTableDefinitionParser()).parse(node.get(GLOBALTABLES_DEFINITION)).forEach(context::registerStreamDefinition);

        // Parse all defined functions
        new MapParser<>("function definition", new TypedFunctionDefinitionParser()).parse(node.get(FUNCTIONS_DEFINITION)).forEach(context::registerFunction);

        // Parse all defined stores
        new MapParser<>("store definition", new StoreDefinitionParser()).parse(node.get(STORES_DEFINITION)).forEach(context::registerStore);

        // Parse all defined pipelines
        final MapParser<PipelineDefinition> mapParser = new MapParser<>("pipeline definition", new PipelineDefinitionParser(context));
        final Map<String, PipelineDefinition> pipelineDefinitionMap = mapParser.parse(node.get(PIPELINES_DEFINITION));
        pipelineDefinitionMap.forEach((name, pipeline) -> {
            BaseStreamDefinition definition = pipeline.source();
            StreamWrapper cursor = context.getStreamWrapper(definition);
            LOG.info("Activating Kafka Streams topology:");
            LOG.info("  {}", cursor);
            for (StreamOperation operation : pipeline.chain()) {
                LOG.info("    ==> {}", operation);
                cursor = cursor.apply(operation);
                LOG.info("  {}", cursor);
            }
            if (pipeline.sink() != null) {
                LOG.info("    ==> {}", pipeline.sink());
                cursor.apply(pipeline.sink());
            }
        });

        // Build the result topology
        var topology = context.build();

        // Derive all input, intermediate and output topics
        var knownTopics = context.getRegisteredTopics();
        var topologyInputs = new HashSet<String>();
        var topologyOutputs = new HashSet<String>();
        var inputTopics = new HashSet<String>();
        var outputTopics = new HashSet<String>();
        var intermediateTopics = new HashSet<String>();

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
        return new GeneratedTopology(
                topology,
                inputTopics,
                intermediateTopics,
                outputTopics,
                context.stores());
    }

    private static void analyzeTopology(Topology topology, Set<String> inputTopics, Set<String> outputTopics) {
        var description = topology.describe();
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
