package io.axual.ksml.definition;

public interface Definition {
    String DEFINITION_LITERAL = "Definition";

    default String definitionType() {
        var type = getClass().getSimpleName();
        if (DEFINITION_LITERAL.equals(type.substring(type.length() - DEFINITION_LITERAL.length()))) {
            type = type.substring(0, type.length() - DEFINITION_LITERAL.length());
        }
        return type;
    }
}
