package io.axual.ksml.definition;

public record TypedRef(TopicType type, Ref<? extends TopicDefinition> ref) {
    public enum TopicType {
        STREAM,
        TABLE,
        GLOBALTABLE
    }
}
