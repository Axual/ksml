package io.axual.ksml.definition.parser;

import io.axual.ksml.generator.TopologySpecification;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.MapParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINES_DEFINITION;

public class TopologySpecificationParser extends BaseParser<TopologySpecification> {

    @Override
    public TopologySpecification parse(YamlNode node) {
        // Set up an index for the topology resources
        final var result = new TopologySpecification();

        // If there is nothing to parse, return immediately
        if (node == null) return result;

        // Parse all defined streams
        new MapParser<>("stream definition", new StreamDefinitionParser()).parse(node.get(STREAMS_DEFINITION)).forEach(result::registerStreamDefinition);
        new MapParser<>("table definition", new TableDefinitionParser()).parse(node.get(TABLES_DEFINITION)).forEach(result::registerStreamDefinition);
        new MapParser<>("globalTable definition", new GlobalTableDefinitionParser()).parse(node.get(GLOBALTABLES_DEFINITION)).forEach(result::registerStreamDefinition);

        // Parse all defined state stores
        new MapParser<>("state store definition", new StateStoreDefinitionParser()).parse(node.get(STORES_DEFINITION)).forEach(result::registerStateStore);

        // Parse all defined functions
        new MapParser<>("function definition", new TypedFunctionDefinitionParser()).parse(node.get(FUNCTIONS_DEFINITION)).forEach(result::registerFunction);

        // Parse all defined pipelines
        new MapParser<>("pipeline definition", new PipelineDefinitionParser()).parse(node.get(PIPELINES_DEFINITION)).forEach(result::registerPipeline);

        return result;
    }
}
