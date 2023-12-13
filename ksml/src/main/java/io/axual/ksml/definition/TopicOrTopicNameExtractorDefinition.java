package io.axual.ksml.definition;

public record TopicOrTopicNameExtractorDefinition(TopicDefinition topic, FunctionDefinition topicNameExtractor, FunctionDefinition partitioner) {
}
