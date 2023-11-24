package io.axual.ksml.datagenerator.parser;

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


import io.axual.ksml.KSMLConfig;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.datagenerator.config.GeneratorConfig;
import io.axual.ksml.datagenerator.definition.ProducerDefinition;
import io.axual.ksml.definition.parser.StreamDefinitionParser;
import io.axual.ksml.generator.YAMLDefinition;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.generator.YAMLReader;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.notation.avro.AvroNotation;
import io.axual.ksml.notation.avro.AvroSchemaLoader;
import io.axual.ksml.notation.csv.CsvNotation;
import io.axual.ksml.notation.csv.CsvSchemaLoader;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.notation.json.JsonSchemaLoader;
import io.axual.ksml.notation.xml.XmlNotation;
import io.axual.ksml.notation.xml.XmlSchemaLoader;
import io.axual.ksml.parser.MapParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.axual.ksml.datagenerator.dsl.ProducerDSL.PRODUCERS_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.FUNCTIONS_DEFINITION;
import static io.axual.ksml.dsl.KSMLDSL.STREAMS_DEFINITION;

/**
 * Generate a Kafka Streams topology from a KSML configuration, using a Python interpreter.
 *
 * @see KSMLConfig
 */
public class ProducerDefinitionFileParser {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerDefinitionFileParser.class);
    private final GeneratorConfig config;

    public ProducerDefinitionFileParser(GeneratorConfig config) {
        this.config = config;
    }

    private List<YAMLDefinition> readDefinitions() {
        try {
            // Parse source from file
            LOG.info("Reading Producer Definition from source file(s): {}", config.getDefinitions());
            return YAMLReader.readYAML(YAMLObjectMapper.INSTANCE, config.getConfigDirectory(), config.getDefinitions());
        } catch (IOException e) {
            LOG.info("Can not read YAML: {}", e.getMessage());
        }

        return new ArrayList<>();
    }

    public Map<String, ProducerDefinition> create(NotationLibrary notationLibrary, PythonContext pythonContext) {
        // Register schema loaders
        SchemaLibrary.registerLoader(AvroNotation.NOTATION_NAME, new AvroSchemaLoader(config.getSchemaDirectory()));
        SchemaLibrary.registerLoader(CsvNotation.NOTATION_NAME, new CsvSchemaLoader(config.getSchemaDirectory()));
        SchemaLibrary.registerLoader(JsonNotation.NOTATION_NAME, new JsonSchemaLoader(config.getSchemaDirectory()));
        SchemaLibrary.registerLoader(XmlNotation.NOTATION_NAME, new XmlSchemaLoader(config.getSchemaDirectory()));

        List<YAMLDefinition> definitions = readDefinitions();
        Map<String, ProducerDefinition> producers = new TreeMap<>();
        for (YAMLDefinition definition : definitions) {
            producers.putAll(generate(YamlNode.fromRoot(definition.root(), "definition"), notationLibrary, pythonContext));
        }

        StringBuilder output = new StringBuilder("\n\nRegistered producers:\n");
        for (var entry : producers.entrySet()) {
            var producer = entry.getValue();
            var keyType = producer.target().keyType.toString();
            var valueType = producer.target().valueType.toString();
            output
                    .append("  ")
                    .append(entry.getKey())
                    .append(": output (")
                    .append(keyType)
                    .append(", ")
                    .append(valueType)
                    .append(") to ")
                    .append(producer.target().topic)
                    .append(" every ")
                    .append(producer.interval().toMillis())
                    .append("ms\n");
        }
        LOG.info("\n\n{}", output);

        return producers;
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

    private Map<String, ProducerDefinition> generate(YamlNode node, NotationLibrary notationLibrary, PythonContext pythonContext) {
        if (node == null) return null;

        // Set up the parse context, which will gather toplevel information on the streams topology
        var context = new ProducerParseContext(notationLibrary, pythonContext);

        // Parse all defined streams
        new MapParser<>("stream definition", new StreamDefinitionParser()).parse(node.get(STREAMS_DEFINITION)).forEach(context::registerStreamDefinition);

        // Parse all defined functions
        new MapParser<>("function definition", new TypedFunctionDefinitionParser()).parse(node.get(FUNCTIONS_DEFINITION)).forEach(context::registerFunction);
        // Generate all the function code in the Python context
        for (var function : context.getFunctionDefinitions().entrySet()) {
            PythonFunction.fromNamed(pythonContext, function.getKey(), function.getValue());
        }

        // Parse all defined message producers
        return new HashMap<>(new MapParser<>("producer definition", new ProducerDefinitionParser(context)).parse(node.get(PRODUCERS_DEFINITION)));
    }
}
