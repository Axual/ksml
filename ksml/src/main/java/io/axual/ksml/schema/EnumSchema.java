package io.axual.ksml.schema;

import java.util.List;

public class EnumSchema extends NamedSchema {
    private final List<String> values;
    private final String defaultValue;

    public EnumSchema(String namespace, String name, String doc, List<String> values) {
        this(namespace, name, doc, values, null);
    }

    public EnumSchema(String namespace, String name, String doc, List<String> values, String defaultValue) {
        super(Type.ENUM, namespace, name, doc);
        this.values = values;
        this.defaultValue = defaultValue;
    }

    public List<String> values() {
        return values;
    }

    public String defaultValue() {
        return defaultValue;
    }
}
