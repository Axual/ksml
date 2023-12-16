package io.axual.ksml.definition.parser;

import io.axual.ksml.definition.ToTopicDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StructParser;

public class ToTopicDefinitionParser extends ContextAwareParser<ToTopicDefinition> {
    public ToTopicDefinitionParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    protected StructParser<ToTopicDefinition> parser() {
        return structParser(
                ToTopicDefinition.class,
                "Writes out pipeline messages to a topic",
                topicField(
                        KSMLDSL.Operations.To.TOPIC,
                        false,
                        "Reference to a pre-defined topic, or an inline definition of a topic and an optional stream partitioner",
                        new TopicDefinitionParser(false)),
                new StreamPartitionerDefinitionParser(),
                ToTopicDefinition::new);
    }
}
