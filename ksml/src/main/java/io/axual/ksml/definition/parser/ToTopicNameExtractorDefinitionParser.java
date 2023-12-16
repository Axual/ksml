package io.axual.ksml.definition.parser;

import io.axual.ksml.definition.ToTopicNameExtractorDefinition;
import io.axual.ksml.definition.TopicNameExtractorDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StructParser;

public class ToTopicNameExtractorDefinitionParser extends ContextAwareParser<ToTopicNameExtractorDefinition> {
    public ToTopicNameExtractorDefinitionParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    protected StructParser<ToTopicNameExtractorDefinition> parser() {
        return structParser(
                ToTopicNameExtractorDefinition.class,
                "Writes out pipeline messages to a topic as given by a topic name extractor",
                lookupField(
                        "topic name extractor",
                        KSMLDSL.Operations.To.TOPIC_NAME_EXTRACTOR,
                        false,
                        "Reference to a pre-defined topic name extractor, or an inline definition of a topic name extractor and an optional stream partitioner",
                        resources()::function,
                        new TopicNameExtractorDefinitionParser()),
                new StreamPartitionerDefinitionParser(),
                (tne, partitioner) -> new ToTopicNameExtractorDefinition(new TopicNameExtractorDefinition(tne), partitioner));
    }
}
