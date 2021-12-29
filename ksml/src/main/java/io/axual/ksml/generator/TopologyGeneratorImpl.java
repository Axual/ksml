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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.axual.ksml.KSMLConfig;
import io.axual.ksml.definition.BaseStreamDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.definition.StoreDefinition;
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
import lombok.AllArgsConstructor;
import lombok.Getter;

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

    @AllArgsConstructor
    @Getter
    public static class Result {
        private Topology topology;
        private Map<String, TopologyParseContext.StoreDescriptor> stores;
    }

    private class KSMLDefinition {
        public final String source;
        public final JsonNode root;

        public KSMLDefinition(String source, JsonNode root) {
            this.source = source;
            this.root = root;
        }
    }

    public TopologyGeneratorImpl(KSMLConfig config) {
        this.config = config;
    }

    private List<KSMLDefinition> readKSMLDefinitions() {
        ObjectMapper mapper = new ObjectMapper(
                new YAMLFactory()
                        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                        .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                        .enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE));
        try {
            switch (config.sourceType) {
                case "file":
                    // Parse source from file
                    LOG.info("Reading KSML from source file(s): {}", config.source);
                    return readYAMLsFromFile(mapper, config.workingDirectory, config.source);
                case "content":
                    // Parse YAML content directly from string
                    LOG.info("Reading KSML from content string: {}", config.source);
                    return Collections.singletonList(new KSMLDefinition("content", mapper.readValue((String) config.source, JsonNode.class)));
                default:
                    throw new KSMLParseException(null, "Unknown KSML source type: " + config.sourceType);
            }
        } catch (IOException e) {
            LOG.error("Can not read YAML!", e);
        }

        return new ArrayList<>();
    }

    private List<KSMLDefinition> readYAMLsFromFile(ObjectMapper mapper, String baseDir, Object source) throws IOException {
        if (source instanceof String) {
            String fullSourcePath = Path.of(baseDir, (String) source).toString();
            return Collections.singletonList(new KSMLDefinition(fullSourcePath, mapper.readValue(new File(fullSourcePath), JsonNode.class)));
        }
        if (source instanceof Collection) {
            List<KSMLDefinition> result = new ArrayList<>();
            for (Object element : (Collection<?>) source) {
                result.addAll(readYAMLsFromFile(mapper, baseDir, element));
            }
            return result;
        }
        return new ArrayList<>();
    }

    public Topology create(StreamsBuilder builder) {
        List<KSMLDefinition> definitions = readKSMLDefinitions();
        for (KSMLDefinition definition : definitions) {
            var generatorResult = generate(builder, YamlNode.fromRoot(definition.root, "ksml"));
            if (generatorResult != null) {
                LOG.info("\n{}", generatorResult.getTopology() != null ? generatorResult.getTopology().describe() : "null");
                if (generatorResult.getStores().size() > 0) {
                    LOG.info("Registered state stores:");
                    for (var entry : generatorResult.getStores().entrySet()) {
                        LOG.info("  {}: retention={}, key={}, value={}, caching={}", entry.getKey(), entry.getValue().storeRetention, entry.getValue().keyType, entry.getValue().valueType, entry.getValue().cachingEnabled);
                    }
                }
            }
        }
        return builder.build();
    }

    private Result generate(StreamsBuilder builder, YamlNode node) {
        if (node == null) return null;

        // Parse all defined streams
        Map<String, BaseStreamDefinition> streamDefinitions = new HashMap<>();
        new MapParser<>(new StreamDefinitionParser()).parse(node.get(STREAMS_DEFINITION)).forEach(streamDefinitions::putIfAbsent);
        new MapParser<>(new TableDefinitionParser()).parse(node.get(TABLES_DEFINITION)).forEach(streamDefinitions::putIfAbsent);
        new MapParser<>(new GlobalTableDefinitionParser()).parse(node.get(GLOBALTABLES_DEFINITION)).forEach(streamDefinitions::putIfAbsent);

        // Parse all defined functions
        Map<String, FunctionDefinition> functions = new MapParser<>(new TypedFunctionDefinitionParser())
                .parse(node.get(FUNCTIONS_DEFINITION));

        // Parse all defined stores
        Map<String, StoreDefinition> stores = new MapParser<>(new StoreDefinitionParser()).parse(node.get(STORES_DEFINITION));

        // Parse all defined pipelines
        TopologyParseContext context = new TopologyParseContext(builder, config.notationLibrary, streamDefinitions, functions, stores);
        final MapParser<PipelineDefinition> mapParser = new MapParser<>(new PipelineDefinitionParser(context));
        final Map<String, PipelineDefinition> pipelineDefinitionMap = mapParser.parse(node.get(PIPELINES_DEFINITION));
        pipelineDefinitionMap.forEach((name, pipeline) -> {
            BaseStreamDefinition definition = pipeline.source;
            StreamWrapper cursor = context.getStreamWrapper(definition);
            LOG.info("Activating Kafka Streams topology:");
            LOG.info("{}", cursor);
            for (StreamOperation operation : pipeline.chain) {
                LOG.info("    ==> {}", operation);
                cursor = cursor.apply(operation);
                LOG.info("{}", cursor);
            }
            if (pipeline.sink != null) {
                LOG.info("    ==> {}", pipeline.sink);
                cursor.apply(pipeline.sink);
            }
        });

        // Return the built topology
        return new Result(context.build(), context.stores());
    }
}
