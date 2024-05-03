package io.axual.ksml.definition;

public abstract class AbstractDefinition implements Definition {
    @Override
    public String toString() {
        return definitionType();
    }
}
