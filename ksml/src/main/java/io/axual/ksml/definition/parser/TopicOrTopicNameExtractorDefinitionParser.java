package io.axual.ksml.definition.parser;

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopicOrTopicNameExtractorDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.CombinedParser;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.parser.YamlNode;
import org.apache.commons.collections4.ListUtils;

public class TopicOrTopicNameExtractorDefinitionParser extends ContextAwareParser<TopicOrTopicNameExtractorDefinition> {
    final StructParser<CombinedParser.Combination<TopicDefinition, FunctionDefinition>> topicParser;
    final StructParser<CombinedParser.Combination<FunctionDefinition, FunctionDefinition>> tneParser;
    final StructSchema schema;

    public TopicOrTopicNameExtractorDefinitionParser(TopologyResources resources) {
        super(resources);
        // This parser parses the "to:" tag as a reference to a topic, or as an inline definition of topic and partitioner
        topicParser = lookupField(
                "topic",
                KSMLDSL.Operations.TO,
                false,
                "Reference to a pre-defined topic, or an inline definition of a topic and an optional stream partitioner",
                name -> resources.topic(name) != null ? new CombinedParser.Combination<>(resources.topic(name), null) : null,
                new CombinedParser<>(
                        new TopicDefinitionParser(),
                        functionField(KSMLDSL.Operations.To.PARTITIONER, "Stream partitioner", new StreamPartitionerDefinitionParser())));

        // This parser parses the "to:" tag as a reference to a TNE, or as an inline definition of TNE and partitioner
        tneParser = lookupField(
                "topic name extractor",
                KSMLDSL.Operations.TO,
                false,
                "Reference to a pre-defined topic name extractor, or an inline definition of a topic name extractor and an optional stream partitioner",
                name -> resources.function(name) != null ? new CombinedParser.Combination<>(resources.function(name), null) : null,
                new CombinedParser<>(
                        functionField(KSMLDSL.Operations.To.TOPIC_NAME_EXTRACTOR, "Topic name extractor", new TopicNameExtractorDefinitionParser()),
                        functionField(KSMLDSL.Operations.To.PARTITIONER, "Stream partitioner", new StreamPartitionerDefinitionParser())));

        // Set up the schema to the outside world as an object with 1 field, which can be either the topic or TNE variant
        schema = structSchema((String) null, null, ListUtils.union(topicParser.fields(), tneParser.fields()));
    }

    @Override
    protected StructParser<TopicOrTopicNameExtractorDefinition> parser() {
        return new StructParser<>() {
            @Override
            public TopicOrTopicNameExtractorDefinition parse(YamlNode node) {
                // First try to parse as a topic
                final var topicAndPartitioner = topicParser.parse(node);
                if (topicAndPartitioner != null && topicAndPartitioner.first() != null) {
                    return new TopicOrTopicNameExtractorDefinition(topicAndPartitioner.first(), null, topicAndPartitioner.second());
                }
                // Then try to parse as a topic name extractor
                final var tneAndPartitioner = tneParser.parse(node);
                if (tneAndPartitioner != null && tneAndPartitioner.first() != null) {
                    return new TopicOrTopicNameExtractorDefinition(null, tneAndPartitioner.first(), tneAndPartitioner.second());
                }

                // If the "to:" tag exists, then we raise an error, since we could not parse it properly
                if (node.get(KSMLDSL.Operations.TO) != null) {
                    throw FatalError.topologyError("Could not properly parse the pipeline TO: tag");
                }

                // No TO tag found, so return null
                return null;
            }

            @Override
            public StructSchema schema() {
                return schema;
            }
        };
    }
}
