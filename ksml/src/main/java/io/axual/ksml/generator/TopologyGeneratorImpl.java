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

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.KSMLConfig;
import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.definition.parser.GlobalTableDefinitionParser;
import io.axual.ksml.definition.parser.PipelineDefinitionParser;
import io.axual.ksml.definition.parser.StoreDefinitionParser;
import io.axual.ksml.definition.parser.StreamDefinitionParser;
import io.axual.ksml.definition.parser.TableDefinitionParser;
import io.axual.ksml.definition.parser.TypedFunctionDefinitionParser;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.MapParser;
import io.axual.ksml.parser.StreamOperation;
import io.axual.ksml.parser.TopologyParseContext;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.stream.StreamWrapper;

import static io.axual.ksml.dsl.KSMLDSL.FUNCTIONS_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.GLOBALTABLES_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINES_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.STORES_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.STREAMS_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.TABLES_DEFINITION;

/**
 * Generate a Kafka Streams topology from a KSML configuration, using a Python interpreter.
 *
 * @see KSMLConfig
 */
public class TopologyGeneratorImpl {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyGeneratorImpl.class);
    private final KSMLConfig config;

    public record Result(Topology topology,
                         Map<String, TopologyParseContext.StoreDescriptor> stores) {
    }

    public TopologyGeneratorImpl(KSMLConfig config) {
        this.config = config;
    }

    private List<YAMLDefinition> readKSMLDefinitions() {
        try {
            switch (config.sourceType) {
                case "file" -> {
                    // Parse source from file
                    LOG.info("Reading KSML from source file(s): {}", config.source);
                    return YAMLReader.readYAML(YAMLObjectMapper.INSTANCE, config.configDirectory, config.source);
                }
                case "content" -> {
                    // Parse YAML content directly from string
                    LOG.info("Reading KSML from content string: {}", config.source);
                    return Collections.singletonList(new YAMLDefinition("content", YAMLObjectMapper.INSTANCE.readValue((String) config.source, JsonNode.class)));
                }
                default ->
                        throw new KSMLParseException(null, "Unknown KSML source dataType: " + config.sourceType);
            }
        } catch (IOException e) {
            LOG.info("Can not read YAML: {}", e.getMessage());
        }

        return new ArrayList<>();
    }

    public Topology create(StreamsBuilder builder) {
        List<YAMLDefinition> definitions = readKSMLDefinitions();
        for (YAMLDefinition definition : definitions) {
            var generatorResult = generate(builder, YamlNode.fromRoot(definition.root(), "ksml"), getPrefix(definition.source()));
            if (generatorResult != null) {
                LOG.info("\n\n{}", generatorResult.topology() != null ? generatorResult.topology().describe() : "null");
                if (generatorResult.stores.size() > 0) {
                    StringBuilder storeOutput = new StringBuilder("Registered state stores:\n");
                    for (var entry : generatorResult.stores().entrySet()) {
                        storeOutput
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
                    LOG.info("\n{}\n", storeOutput);
                }
            }
        }
        return builder.build();
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

    private Result generate(StreamsBuilder builder, YamlNode node, String namePrefix) {
        if (node == null) return null;

        // Set up the parse context, which will gather toplevel information on the streams topology
        TopologyParseContext context = new TopologyParseContext(builder, config.notationLibrary, namePrefix);

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

        // Return the built topology
        return new Result(context.build(), context.stores());
    }
}
